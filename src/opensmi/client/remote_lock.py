# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""The remote interface for a server-side `Lock`."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from asyncua import ua
from asyncua.common.node import Node
from asyncua.common.subscription import DataChangeNotif
from opensmi.core import Signal
from opensmi.core.errors import OpenSmiRuntimeError
from opensmi.core.subscription_manager import UaDataChangeSubscriber
from opensmi.core.ua_node_util import get_children_browse_names
from typing_extensions import override

from opensmi.client.dto import RemoteLockDTO
from opensmi.client.remote_server import RemoteServer
from opensmi.client.remote_ua_object import RemoteUaObject

if TYPE_CHECKING:
    from opensmi.client.remote_component import BaseRemoteComponent


@dataclass(slots=True)
class _RemoteLockUaNodes:
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

    _ua_nodes: _RemoteLockUaNodes

    def __init__(
        self,
        *,
        parent: BaseRemoteComponent,
        server: RemoteServer,
        ua_node: Node | None = None,
        max_inactive_lock_time: int | None = None,
    ) -> None:
        """Create a new instance. Must be asynchronously initialized with ``init()`` before use.

        :param max_inactive_lock_time: Default ``MaxInactiveLockTime`` in milliseconds. Will be overridden
            if the server provides this information.
        """
        super().__init__(name="Lock", ua_node=ua_node, parent=parent, server=server)

        self._locking_user = ""
        self._locking_client = ""
        self._locked = False
        self._locked_since: datetime | None = None
        self._remaining_lock_time_ms: int | None = None
        self._max_inactive_lock_time_ms: int | None = max_inactive_lock_time

        self.locked_changed = Signal[RemoteLock, bool]()
        self.locking_user_changed = Signal[RemoteLock, str]()

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
    def max_inactive_lock_time_ms(self) -> int | None:
        """Return the maximum (inactive) lock time in milliseconds. Read-only property.

        Is ``None`` when the server did not provide this value. Only read during initialization, assumed to be constant.
        """
        return self._max_inactive_lock_time_ms

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
        ua_max_inactive_lock_time = self.server.ua_client.get_node(
            ua.NodeId(
                Identifier=ua.Int32(6387),  # /Objects/Server/ServerCapabilities/MaxInactiveLockTime
                NamespaceIndex=self.server.ua_get_namespace_index("http://opcfoundation.org/UA/DI/"),
            )
        )

        max_inactive_lock_time = await ua_max_inactive_lock_time.read_value()
        if isinstance(max_inactive_lock_time, int):
            self._max_inactive_lock_time_ms = max_inactive_lock_time
        else:
            self.logger.warning("Could not read RemainingLockTime, using default")

    @override
    async def _init(self) -> None:
        await super()._init()

        ua_locked: Node | None = None
        ua_locking_user: Node | None = None
        ua_locking_client: Node | None = None
        ua_remaining_lock_time: Node | None = None

        ua_init_lock: Node | None = None
        ua_break_lock: Node | None = None
        ua_exit_lock: Node | None = None
        ua_renew_lock: Node | None = None

        browse_result, _ = await asyncio.gather(
            get_children_browse_names(self.ua_node), self._ua_read_max_inactive_lock_time(), return_exceptions=True
        )

        if isinstance(browse_result, BaseException):
            msg = f"Could not browse {self.path} children!"
            raise OpenSmiRuntimeError(msg)

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

        self._ua_nodes = _RemoteLockUaNodes(
            Locked=ua_locked.nodeid,
            LockingUser=ua_locking_user.nodeid,
            LockingClient=ua_locking_client.nodeid,
            RemainingLockTime=ua_remaining_lock_time.nodeid,
            InitLock=ua_init_lock.nodeid,
            BreakLock=ua_break_lock.nodeid,
            ExitLock=ua_exit_lock.nodeid,
            RenewLock=ua_renew_lock.nodeid,
        )

        nodes_to_subscribe = [ua_locked, ua_locking_user, ua_locking_client, ua_remaining_lock_time]
        await self.server.subscription_manager.subscribe_data_change(handler=self, nodes=nodes_to_subscribe)

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
