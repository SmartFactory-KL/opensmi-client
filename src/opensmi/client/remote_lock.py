# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""The remote interface for a server-side `Lock`."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from asyncua import ua
from asyncua.common.node import Node
from asyncua.common.subscription import DataChangeNotif
from asyncua.ua.uaerrors import UaError
from opensmi.core import Signal
from opensmi.core.subscription_manager import UaDataChangeSubscriber
from opensmi.core.ua_node_util import get_children_browse_names
from typing_extensions import override

from opensmi.client.common import gather
from opensmi.client.dto import RemoteLockDTO
from opensmi.client.remote_server import RemoteServer
from opensmi.client.remote_ua_object import RemoteUaObject

if TYPE_CHECKING:
    from opensmi.client.remote_component import BaseRemoteComponent


@dataclass(slots=True, frozen=True, kw_only=True)
class RemoteLockUaNodes:
    """Data class for all `RemoteLock` OPC UA nodes except the main node."""

    Locked: ua.NodeId
    LockingUser: ua.NodeId
    LockingClient: ua.NodeId
    RemainingLockTime: ua.NodeId

    # OPC UA methods
    InitLock: ua.NodeId
    BreakLock: ua.NodeId
    ExitLock: ua.NodeId
    RenewLock: ua.NodeId


class RemoteLock(RemoteUaObject["BaseRemoteComponent"], UaDataChangeSubscriber):
    """The remote interface for a server-side `Lock`."""

    _ua_nodes: RemoteLockUaNodes
    _max_inactive_lock_time_ms: int
    _renew_retry_seconds: float
    _renew_threshold: float

    def __init__(
        self,
        *,
        parent: BaseRemoteComponent,
        server: RemoteServer,
        ua_node: Node | None = None,
        max_inactive_lock_time: int = 60_000,
        renew_threshold: float = 0.5,
        renew_retry_seconds: float = 1.0,
    ) -> None:
        """Create a new instance. Must be asynchronously initialized with ``init()`` before use.

        :param max_inactive_lock_time: Default ``MaxInactiveLockTime`` in milliseconds. Will be overridden
            if the server provides this information.
        :param renew_threshold: Fraction of ``MaxInactiveLockTime`` below which a renewal is requested (0 < x < 1).
        :param renew_retry_seconds: Minimum seconds between two `renew_due` events while still below the
            threshold (> 0).
        """
        super().__init__(name="Lock", ua_node=ua_node, parent=parent, server=server)

        self._locking_user = ""
        self._locking_client = ""
        self._locked = False
        self._locked_since: datetime | None = None
        self._remaining_lock_time_ms: int | None = None
        self._last_renew_request: float | None = None
        # validated by property setters:
        self.max_inactive_lock_time_ms = max_inactive_lock_time
        self.renew_retry_seconds = renew_retry_seconds
        self.renew_threshold = renew_threshold

        self.locked_changed = Signal[RemoteLock, bool]()
        """Callback when the locked status changed. Payload is locked status."""
        self.locking_user_changed = Signal[RemoteLock, str]()
        """Callback when a lock user changed. Payload is the new user name."""
        self.renew_due = Signal[RemoteLock, float]()
        """Callback when a lock renew is due. Payload contains the remaining lock time in milliseconds."""

    def dto(self) -> RemoteLockDTO | None:
        """Return the data transfer object."""
        if not self.is_initialized:
            return None
        return RemoteLockDTO(
            name=self.name,
            path=self.path,
            ua_node_id=self.ua_node.nodeid.to_string(),
            ua_type=self.ua_type,
            is_locked=self._locked,
            locking_user=self.locking_user,
            locking_client=self.locking_client,
            locked_since=self.locked_since,
            remaining_lock_time_ms=self.remaining_lock_time_ms,
            max_inactive_lock_time_ms=self.max_inactive_lock_time_ms,
        )

    @property
    def locked(self) -> bool:
        """Whether the module is locked."""
        return self._locked

    @property
    def locking_user(self) -> str:
        """Empty when module is unlocked, otherwise name of the locking user."""
        return self._locking_user

    @property
    def locked_since(self) -> datetime | None:
        """Return the time when the current user locked the asset. Is ``None`` when Lock is unlocked."""
        return self._locked_since

    @property
    def locking_client(self) -> str:
        """Empty when module is unlocked, otherwise hostname of the locking user."""
        return self._locking_client

    @property
    def locked_by_us(self) -> bool:
        """Whether the lock is owned by us."""
        return self.locked and self.locking_user == self.server.username

    @property
    def remaining_lock_time_ms(self) -> int | None:
        """Return the remaining lock time in milliseconds. Read-only property.

        Check this regularly to know when to renew the lock with `renew_lock`.
        """
        if not self.locked:
            return None
        return self._remaining_lock_time_ms

    @property
    def max_inactive_lock_time_ms(self) -> int:
        """Return the maximum (inactive) lock time in milliseconds.

        Only read once from server during initialization, assumed to be constant.
        If the server did not provide this information, the default value from a corresponding argument
        to `__init__` will be used.
        """
        return self._max_inactive_lock_time_ms

    @max_inactive_lock_time_ms.setter
    def max_inactive_lock_time_ms(self, value: int) -> None:
        value = int(value)
        if not value > 0:
            msg = f"max_inactive_lock_time_ms must be a number > 0, got {value!r}"
            raise ValueError(msg)
        self._max_inactive_lock_time_ms = value
        self._last_renew_request = None  # re-evaluate against the new maximum

    @property
    def renew_threshold(self) -> float:
        """Fraction of ``MaxInactiveLockTime`` below which a renewal is requested (0 < x < 1)."""
        return self._renew_threshold

    @renew_threshold.setter
    def renew_threshold(self, value: float) -> None:
        if not 0 < value < 1:
            msg = f"renew_threshold must be a number with 0 < x < 1, got {value!r}"
            raise ValueError(msg)
        self._renew_threshold = float(value)
        self._last_renew_request = None  # re-evaluate with the new threshold

    @property
    def renew_retry_seconds(self) -> float:
        """Minimum seconds between two ``renew_due`` events while still below the threshold (> 0)."""
        return self._renew_retry_seconds

    @renew_retry_seconds.setter
    def renew_retry_seconds(self, value: float) -> None:
        if not value > 0:
            msg = f"renew_retry_seconds must be a number > 0, got {value!r}"
            raise ValueError(msg)
        self._renew_retry_seconds = float(value)

    # -------------------------------------------------------------------------------------------------------

    async def init_lock(self) -> None:
        """Occupy the lock, will fail when already locked."""
        await self.ua_node.call_method(self._ua_nodes.InitLock)

    async def break_lock(self) -> None:
        """Occupy the lock, will fail when already locked by higher priority user."""
        await self.ua_node.call_method(self._ua_nodes.BreakLock)

    async def exit_lock(self) -> None:
        """Free the lock, will fail when we are not the locking user."""
        await self.ua_node.call_method(self._ua_nodes.ExitLock)

    async def renew_lock(self) -> None:
        """Renew the lock, will fail when we are not the locking user."""
        await self.ua_node.call_method(self._ua_nodes.RenewLock)

    async def _ua_read_max_inactive_lock_time(self) -> None:
        try:
            ua_max_inactive_lock_time = self.server.ua_client.get_node(
                ua.NodeId(
                    Identifier=ua.Int32(6387),  # /Objects/Server/ServerCapabilities/MaxInactiveLockTime
                    NamespaceIndex=self.server.ua_get_namespace_index("http://opcfoundation.org/UA/DI/"),
                )
            )

            max_inactive_lock_time = await ua_max_inactive_lock_time.read_value()
            if isinstance(max_inactive_lock_time, (float, int)):
                self.max_inactive_lock_time_ms = int(max_inactive_lock_time)
                self.logger.info(
                    "Using server MaxInactiveLockTime",
                    max_inactive_lock_time_ms=self.max_inactive_lock_time_ms,
                )
            else:
                self.logger.warning(
                    "Could not read valid MaxInactiveLockTime, using default",
                    reason="MaxInactiveLockTime was not a float nor an int.",
                    max_inactive_lock_time=max_inactive_lock_time,
                )
        except UaError as err:
            self.logger.warning(
                "Could not read MaxInactiveLockTime, using default",
                reason=err,
                default=self.max_inactive_lock_time_ms,
            )
        except Exception as err:
            self.logger.warning(
                "Invalid MaxInactiveLockTime from server, using default",
                reason=err,
                default=self.max_inactive_lock_time_ms,
            )

    async def _browse(self) -> None:
        ua_locked: Node | None = None
        ua_locking_user: Node | None = None
        ua_locking_client: Node | None = None
        ua_remaining_lock_time: Node | None = None

        ua_init_lock: Node | None = None
        ua_break_lock: Node | None = None
        ua_exit_lock: Node | None = None
        ua_renew_lock: Node | None = None

        browse_result = await get_children_browse_names(self.ua_node)
        for child, browse_name in browse_result.items():
            match browse_name.Name:
                case "Locked":
                    ua_locked = child
                case "LockingUser":
                    ua_locking_user = child
                case "LockingClient":
                    ua_locking_client = child
                case "RemainingLockTime":
                    ua_remaining_lock_time = child
                case "InitLock":
                    ua_init_lock = child
                case "BreakLock":
                    ua_break_lock = child
                case "ExitLock":
                    ua_exit_lock = child
                case "RenewLock":
                    ua_renew_lock = child

        assert ua_locked is not None
        assert ua_locking_user is not None
        assert ua_locking_client is not None
        assert ua_remaining_lock_time is not None
        assert ua_init_lock is not None
        assert ua_break_lock is not None
        assert ua_exit_lock is not None
        assert ua_renew_lock is not None

        self._ua_nodes = RemoteLockUaNodes(
            Locked=ua_locked.nodeid,
            LockingUser=ua_locking_user.nodeid,
            LockingClient=ua_locking_client.nodeid,
            RemainingLockTime=ua_remaining_lock_time.nodeid,
            InitLock=ua_init_lock.nodeid,
            BreakLock=ua_break_lock.nodeid,
            ExitLock=ua_exit_lock.nodeid,
            RenewLock=ua_renew_lock.nodeid,
        )

    @override
    async def _init(self) -> None:
        await super()._init()

        self.init_exceptions.extend(await gather((self._browse(), self._ua_read_max_inactive_lock_time())))

        assert self._ua_nodes
        nodes_to_subscribe = [
            self.server.ua_client.get_node(self._ua_nodes.Locked),
            self.server.ua_client.get_node(self._ua_nodes.LockingUser),
            self.server.ua_client.get_node(self._ua_nodes.LockingClient),
            self.server.ua_client.get_node(self._ua_nodes.RemainingLockTime),
        ]
        await self.server.subscription_manager.subscribe_data_change(handler=self, nodes=nodes_to_subscribe)

    @property
    def ua_nodes(self) -> RemoteLockUaNodes:
        """Return all raw OPC UA nodes related to this `RemoteLock`."""
        return self._ua_nodes

    async def _check_renew_due(self) -> None:
        max_inactive_lock_time_ms = self.max_inactive_lock_time_ms
        remaining_lock_time_ms = self._remaining_lock_time_ms

        if not self.locked_by_us or remaining_lock_time_ms is None:
            self._last_renew_request = None
            return

        if remaining_lock_time_ms > max_inactive_lock_time_ms:
            self.logger.warning(
                "Remaining lock time exceeds configured maximum, raising maximum",
                remaining_lock_time_ms=remaining_lock_time_ms,
                max_inactive_lock_time_ms=max_inactive_lock_time_ms,
            )
            self.max_inactive_lock_time_ms = remaining_lock_time_ms  # setter validates, resets re-arm state
            max_inactive_lock_time_ms = self.max_inactive_lock_time_ms

        if remaining_lock_time_ms >= max_inactive_lock_time_ms * self.renew_threshold:
            self._last_renew_request = None  # re-arm
            return

        now = time.monotonic()
        if self._last_renew_request is not None and now - self._last_renew_request < self.renew_retry_seconds:
            return

        self._last_renew_request = now
        await self.renew_due.send(self, remaining_lock_time_ms)

    @override
    async def ua_on_data_change(self, node: Node, val: Any, data: DataChangeNotif) -> None:
        match node.nodeid:
            case self._ua_nodes.Locked:
                self._locked = val
                self._locked_since = data.monitored_item.Value.ServerTimestamp if val else None
                await self.locked_changed.send(self, self._locked)
            case self._ua_nodes.LockingUser:
                self._locking_user = val
                await self.locking_user_changed.send(self, self._locking_user)
            case self._ua_nodes.LockingClient:
                self._locking_client = val
            case self._ua_nodes.RemainingLockTime:
                self._remaining_lock_time_ms = val
                await self._check_renew_due()
