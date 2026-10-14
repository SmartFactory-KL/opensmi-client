# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""The remote interface for a server-side `Server`."""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import uuid
from asyncio import Task
from collections.abc import Awaitable, Iterable
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, Never, Self

import structlog
from asyncua import ua
from asyncua.client.client import Client
from asyncua.client.ua_client import UaClientState
from asyncua.common.node import Node
from asyncua.common.subscription import DataChangeNotif
from asyncua.crypto.cert_gen import generate_private_key, generate_self_signed_app_certificate
from asyncua.crypto.security_policies import SECURITY_POLICY_TYPE_MAP
from asyncua.ua import SecurityPolicyType, UaError
from asyncua.ua.uaerrors import BadUserAccessDenied
from cryptography import x509
from cryptography.hazmat._oid import ExtendedKeyUsageOID
from cryptography.hazmat.primitives import serialization
from opensmi.core import Signal
from opensmi.core.base_server import BaseServer
from opensmi.core.subscription_manager import SubscriptionManager, UaDataChangeSubscriber
from opensmi.core.ua_node_util import get_child_without_ns, get_children_browse_names
from typing_extensions import override

from opensmi.client.browse import DEFAULT_BROWSE_RULE, BrowseFeature, BrowseRule, evaluate_browse_features
from opensmi.client.common import gather
from opensmi.client.remote_machine import RemoteMachine
from opensmi.client.remote_ua_object import RemoteUaObject


class RemoteServerStatus(Enum):
    """Status of the remote server."""

    DISCONNECTED = "disconnected"
    """The client is not connected to a server."""
    CONNECTING = "connecting"
    """The client is trying to connect to a server."""
    BROWSING = "browsing"
    """The client is connected, but is busy browsing the server namespace."""
    CONNECTED = "connected"
    """The client is connected and ready to be used."""
    RECONNECTING = "reconnecting"
    """The client lost connection, but is trying to reconnect to the server."""
    DISCONNECTING = "disconnecting"
    """The client is currently disconnecting."""


class RemoteServer(BaseServer[RemoteUaObject], UaDataChangeSubscriber):
    """The remote interface for a server-side `Server`.

    After connecting to a server for the first time, it's address space needs to be browsed. If
    your application only requires certain parts of the address space, i.e. only components, you can
    skip unnecessary browsing by setting the ``browse_*`` options of the constructor accordingly. This
    will speed up the connection speed.
    """

    _browse_rules: tuple[BrowseRule, ...]

    def __init__(
        self,
        url: str,
        *,
        username: str,
        password: str,
        application_uri: str | None = None,
        timeout: float = 10.0,
        watchdog_interval: float = 1.0,
        auto_reconnect: bool = True,
        reconnect_max_delay: float = 30.0,
        reconnect_request_timeout: float = 60.0,
        security_policy: SecurityPolicyType = SecurityPolicyType.NoSecurity,
        browse_rules: Iterable[BrowseRule] | None = None,
    ) -> None:
        """Create a new RemoteServer. Does not connect, call `connect` (or use ``async with``) for that.

        :param url: URL of the OPC UA server to connect to, e.g. opc.tcp://localhost:4843/
        :param username: Username for authenticating with the server.
        :param password: Password for authenticating with the server.
        :param application_uri: Globally unique application URI identifying this client (also used in the certificate
            when a security policy is set). A random ``urn:uuid:...`` is generated if ``None``.
        :param timeout: Timeout in seconds for connection tries.
        :param watchdog_interval: Interval in seconds between connection liveness checks.
        :param auto_reconnect: Whether to automatically reconnect when the connection to the server is lost.
        :param reconnect_max_delay: How many seconds maximum to wait before reconnect (Exponential backoff).
        :param reconnect_request_timeout: How long requests block waiting for the connection to be ready.
        :param security_policy: OPC UA security policy used for the connection. Default: No security policy.
        :param browse_rules: The browse rules for automatic browsing. Browse everything by default (``None``),
            an empty iterable means browse nothing.
        """
        super().__init__()
        self._ua_client = Client(
            url=url,
            timeout=timeout,
            watchdog_intervall=watchdog_interval,
            auto_reconnect=auto_reconnect,
            reconnect_max_delay=reconnect_max_delay,
            reconnect_request_timeout=reconnect_request_timeout,
        )

        self._ua_client.set_user(username)
        self._username: str = username

        self._ua_client.set_password(password)

        if application_uri is None:
            application_uri = f"urn:uuid:{uuid.uuid4()}"

        self._ua_client.application_uri = application_uri
        self._application_uri: str = application_uri

        self._ua_client.product_uri = "urn:opensmi:client"
        self._ua_client_state_subscription = self._ua_client.uaclient.subscribe_state()

        self._security_policy = security_policy

        if browse_rules is None:
            self._browse_rules = (DEFAULT_BROWSE_RULE,)
        else:
            self._browse_rules = tuple(browse_rules)

        self._status = RemoteServerStatus.DISCONNECTED
        self._suppress_connected = True
        """Used to suppress `CONNECTED` before `BROWSING` state from ua client."""
        self._disconnect_requested = False
        self._connect_task: asyncio.Task | None = None

        self.subscription_manager = SubscriptionManager(ua_client=self._ua_client)

        self._machines: dict[str, RemoteMachine] = {}

        self.logger = structlog.getLogger(__name__, url=url, username=username)

        self._state_update_task: Task[Never] | None = None

        self.custom_type_definitions: dict[str, type] = {}

        # signals
        self.status_changed = Signal[RemoteServerStatus]()
        """Fired each time the status of the remote server is changed."""
        self.time_updated = Signal[datetime.datetime]()
        """Fired each time the remote server time updates. Can be used for checking if the connection is alive."""

        self.init_exceptions: list[Exception] = []
        """EXPERIMENTAL! List of collected exceptions that where not raised during initialization."""

    @property
    def browse_rules(self) -> tuple[BrowseRule, ...]:
        """Browse rules for automatically recursively browsing the remote server."""
        return self._browse_rules

    async def _set_status(self, status: RemoteServerStatus) -> None:
        if self._status == status:
            return

        self.logger.info("New status", status=status.name)
        self._status = status
        await self.status_changed.send(status)

    async def connect(
        self,
        *,
        max_retries: int | None = 3,
        min_delay_seconds: float = 0.5,
        max_delay_seconds: float = 30.0,
    ) -> Self:
        """Connect to the remote server, retrying if necessary (with Exponential backoff).

        :param max_retries: How many times to retry connecting to the remote server. ``None`` means unlimited.
        :param min_delay_seconds: How many seconds to wait minimum before next reconnection attempt.
        :param max_delay_seconds: How many seconds to wait maximum before next reconnection attempt.

        :raises ConnectionError: if connection could not be established after ``max_retries``.
        """
        if self._connect_task is not None:
            msg = "connect() is already in progress!"
            raise RuntimeError(msg)

        self._disconnect_requested = False
        self._connect_task = asyncio.create_task(
            self._connect_and_retry(
                max_retries=max_retries, min_delay_seconds=min_delay_seconds, max_delay_seconds=max_delay_seconds
            ),
            name="client_connect_task",
        )
        try:
            await self._connect_task
        except asyncio.CancelledError:
            if self._disconnect_requested:
                msg = "connect() aborted by disconnect()"
                raise ConnectionAbortedError(msg) from None
            raise
        finally:
            self._connect_task = None
        return self  # allows chaining

    async def _connect_and_retry(
        self,
        *,
        max_retries: int | None = 3,
        min_delay_seconds: float = 0.5,
        max_delay_seconds: float = 30.0,
    ) -> None:
        attempt = 0
        last_exc: Exception
        delay = min_delay_seconds
        await self._cancel_state_update_task()
        self._state_update_task = asyncio.create_task(self._status_update_loop(), name="client_state_update_task")
        while True:
            try:
                await self._connect()
                break
            except ConnectionRefusedError as err:
                last_exc = err
                self.logger.warning("Could not connect, ConnectionRefusedError!")
            except BadUserAccessDenied as err:  # most users are only allowed 1 simultaneous login
                # TODO(CaHa): how to differentiate wrong password?
                last_exc = err
                self.logger.warning("Could not login, BadUserAccessDenied!")
            except UaError as err:
                last_exc = err
                self.logger.warning(f"Could not connect: {err}")

            attempt += 1
            if max_retries is not None and attempt > max_retries:  # give up after optional max_retries argument
                msg = f"Reached maximum number of attempts ({max_retries}), giving up! Last error: {last_exc}"
                raise ConnectionError(msg) from last_exc

            msg = f"Trying to connect again in {delay} seconds"
            if max_retries is not None:
                msg += f" (Attempt {attempt}/{max_retries})"
            else:
                msg += f" (Attempt {attempt})"
            self.logger.warning(msg)

            await asyncio.sleep(delay)
            delay = min(max_delay_seconds, delay * 2)

    async def disconnect(self) -> None:
        """Disconnect from the remote server."""
        self.logger.info("Disconnecting...")
        await self._cancel_state_update_task()
        await self._set_status(RemoteServerStatus.DISCONNECTING)
        await self.subscription_manager.shutdown()

        aborted = False
        if self._connect_task is not None and not self._connect_task.done():
            aborted = True
            self._disconnect_requested = True
            self._connect_task.cancel()
            await asyncio.wait((self._connect_task,))
        try:
            await self._ua_client.disconnect()
        except Exception:
            if not aborted:
                raise
            self.logger.debug("Ignoring exception while closing aborted connection", exc_info=True)
        finally:
            await self._set_status(RemoteServerStatus.DISCONNECTED)

    async def _cancel_state_update_task(self) -> None:
        if self._state_update_task is None:
            return
        self.logger.debug("Canceling state update task...")
        self._state_update_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._state_update_task
        self._state_update_task = None

    async def _set_security(self):
        if self._security_policy == SecurityPolicyType.NoSecurity:
            return  # Default, we don't need to anything

        # TODO also load from file?
        key = generate_private_key()
        key_bytes = key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )

        cert = generate_self_signed_app_certificate(
            private_key=key,
            common_name="OpenSMI Python Client",
            names={},
            subject_alt_names=[x509.UniformResourceIdentifier(self._application_uri)],
            extended=[ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH],
            days=3650,
        )
        cert_bytes = cert.public_bytes(serialization.Encoding.DER)

        policy, mode, _ = SECURITY_POLICY_TYPE_MAP[self._security_policy]
        self.logger.info(f"Setting security to {policy=}, {mode=}")
        await self._ua_client.set_security(policy=policy, certificate=cert_bytes, private_key=key_bytes, mode=mode)

    async def _connect(self) -> None:
        self._suppress_connected = True
        await self._set_security()
        await self._write_ua_client_state(self.ua_client.uaclient.state)
        await self._ua_client.connect()

        try:
            self.custom_type_definitions = await self._ua_client.load_data_type_definitions()
        except:
            self.logger.warning("Could not not load custom data type definitions!")

        # iterate through server namespace array to figure out correct namespace indexes
        for ns_idx, namespace in enumerate(await self.ua_client.get_namespace_array()):
            self._namespace_map[namespace] = ns_idx

        self._machines.clear()
        self._ua_objects.clear()
        await self.subscription_manager.clear_subscriptions()
        await self.subscription_manager.init()

        if self.browse_rules:
            await self._set_status(RemoteServerStatus.BROWSING)
            await self._browse_machines()  # Iterate through all modules (v1-v3) / machines (v4)

        await self._setup_subscriptions()
        self.logger.info("Monitoring items", no_of_items=self.subscription_manager.no_of_monitored_items)
        self._suppress_connected = False
        await self._set_status(RemoteServerStatus.CONNECTED)

    @property
    def ua_client(self) -> Client:
        """Return the internal OPC UA client. Make sure you know what you are doing."""
        return self._ua_client

    async def _setup_subscriptions(self) -> None:
        current_time_node = self._ua_client.get_node(
            ua.NodeId(Identifier=ua.Int32(ua.object_ids.ObjectIds.Server_ServerStatus_CurrentTime))
        )
        await self.subscription_manager.subscribe_data_change(self, current_time_node)

    @override
    async def ua_on_data_change(self, node: Node, val: Any, data: DataChangeNotif) -> None:
        await self.time_updated.send(val)

    @property
    def path(self) -> PurePosixPath:
        """Always ``/``."""
        return PurePosixPath("/")

    async def _browse_machines(self) -> None:
        """Iterate through all machines."""
        ua_machines = self._ua_client.get_node(
            ua.NodeId(
                Identifier=ua.Int32(1001),  # Machines
                NamespaceIndex=self.ua_get_namespace_index("http://opcfoundation.org/UA/Machinery/"),
            ),
        )

        result = await get_children_browse_names(ua_machines)
        if not len(result):
            self.logger.warning(
                "No standard 'Machines' object found! Fallback to browsing.",
                ua_node_id=ua_machines.nodeid.to_string(),
            )
            # raises if 'Machines' node does not exist
            ua_machines = await get_child_without_ns(self._ua_client.get_objects_node(), browse_name="Machines")
            result = await get_children_browse_names(ua_machines)

        tasks: list[Awaitable] = []
        for child, browse_name in result.items():
            name = browse_name.Name

            browse_features = evaluate_browse_features(self.browse_rules, self.path / name)
            if BrowseFeature.MACHINES in browse_features:
                machine = RemoteMachine(
                    name=name,
                    ua_node=child,
                    server=self,  # pyright: ignore[reportArgumentType]
                    browse_rules=self.browse_rules,
                )
                tasks.append(machine.init())
                self._machines[name] = machine
        self.init_exceptions = await gather(tasks)

    @property
    def status(self) -> RemoteServerStatus:
        """Return the status of the client."""
        return self._status

    @property
    def connected(self) -> bool:
        """Whether we are currently connected to the remote module."""
        return self.ua_client.uaclient.state == UaClientState.CONNECTED

    @property
    def machines(self) -> dict[str, RemoteMachine]:
        """Provide all available machines in dictionary form using the name of the machine as the key."""
        return self._machines

    @property
    def username(self) -> str:
        """Username used to connect to the remote server."""
        return self._username

    async def _write_ua_client_state(self, ua_client_state: UaClientState) -> None:
        match ua_client_state:
            case UaClientState.CONNECTED:
                if not self._suppress_connected:
                    await self._set_status(RemoteServerStatus.CONNECTED)
            case UaClientState.CONNECTING | UaClientState.SOCKET_OPEN | UaClientState.CHANNEL_OPEN:
                await self._set_status(RemoteServerStatus.CONNECTING)
            case UaClientState.DISCONNECTED:
                await self._set_status(RemoteServerStatus.DISCONNECTED)
            case UaClientState.DISCONNECTING:
                await self._set_status(RemoteServerStatus.DISCONNECTING)
            case UaClientState.RECONNECTING:
                await self._set_status(RemoteServerStatus.RECONNECTING)

    async def _status_update_loop(self) -> Never:
        while True:
            async with self._ua_client_state_subscription as sub:
                ua_client_state = await sub.next_change()
                await self._write_ua_client_state(ua_client_state)

    async def __aenter__(self) -> Self:
        """Enter the asynchronous context manager."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        """Exit the asynchronous context manager and shut down the server."""
        await self.disconnect()
