# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""The remote interface for a server-side `Lock`."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from asyncua import ua
from asyncua.common.node import Node
from asyncua.common.subscription import DataChangeNotif
from open_smi_common.signal import Signal
from open_smi_common.subscription_manager import UaDataChangeSubscriber
from open_smi_common.ua_node_util import get_children_browse_names
from typing_extensions import override

from open_smi_client.dto import RemoteLockDTO
from open_smi_client.remote_server import RemoteServer
from open_smi_client.remote_ua_object import RemoteUaObject

if TYPE_CHECKING:
    from open_smi_client.remote_component import BaseRemoteComponent


@dataclass(slots=True)
class _RemoteLockUaNodes:
    """Data class for all `RemoteLock` OPC UA nodes."""

    Locked: ua.NodeId
    LockingUser: ua.NodeId
    LockingClient: ua.NodeId

    # OPC UA methods
    InitLock: ua.NodeId
    BreakLock: ua.NodeId
    ExitLock: ua.NodeId
    RenewLock: ua.NodeId


class RemoteLock(RemoteUaObject["BaseRemoteComponent"], UaDataChangeSubscriber):
    """The remote interface for a server-side `Lock`."""

    _ua_nodes: _RemoteLockUaNodes

    def __init__(self, *, parent: BaseRemoteComponent, server: RemoteServer) -> None:
        """Create a new instance. Must be asynchronously initialized with ``init()`` before use."""
        super().__init__(name="Lock", ua_node=None, parent=parent, server=server)

        self._locking_user = ""
        self._locking_client = ""
        self._locked = False
        self._locked_since: datetime | None = None

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

    @override
    async def _init(self) -> None:
        await super()._init()

        ua_locked: Node | None = None
        ua_locking_user: Node | None = None
        ua_locking_client: Node | None = None

        ua_init_lock: Node | None = None
        ua_break_lock: Node | None = None
        ua_exit_lock: Node | None = None
        ua_renew_lock: Node | None = None

        for child, browse_name in (await get_children_browse_names(self.ua_node)).items():
            match browse_name.Name:
                case "Locked":
                    ua_locked = child
                case "LockingUser":
                    ua_locking_user = child
                case "LockingClient":
                    ua_locking_client = child
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
        assert ua_init_lock is not None
        assert ua_break_lock is not None
        assert ua_exit_lock is not None
        assert ua_renew_lock is not None

        self._ua_nodes = _RemoteLockUaNodes(
            Locked=ua_locked.nodeid,
            LockingUser=ua_locking_user.nodeid,
            LockingClient=ua_locking_client.nodeid,
            InitLock=ua_init_lock.nodeid,
            BreakLock=ua_break_lock.nodeid,
            ExitLock=ua_exit_lock.nodeid,
            RenewLock=ua_renew_lock.nodeid,
        )

        nodes_to_subscribe = [ua_locked, ua_locking_user, ua_locking_client]
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
