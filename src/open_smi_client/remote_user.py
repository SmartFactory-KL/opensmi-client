# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""The remote interface for a server-side `User`."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from open_smi_common.signal import Signal
from open_smi_common.subscription_manager import UaDataChangeSubscriber
from open_smi_common.ua_node_util import get_children_browse_names
from typing_extensions import override

from open_smi_client.dto import RemoteUserDTO
from open_smi_client.remote_ua_object import RemoteUaObject

if TYPE_CHECKING:
    from asyncua import ua
    from asyncua.common.node import Node
    from asyncua.common.subscription import DataChangeNotif

    from open_smi_client.remote_machine import RemoteMachine
    from open_smi_client.remote_server import RemoteServer


@dataclass
class RemoteUserData:
    """The remote interface for a server-side `User`."""

    name: str = ""
    """ Static property containing the name of the user. """
    allow_multiple: bool = False
    """ Static property indicating whether multiple logins for this user are allowed. """
    user_level: str = ""
    """ Static property indicating the priority of the user (for breaking locks). """
    max_access_level: int = 0
    """ Static property indicating the maximum access level the user has.
     (Refer to minimum access level required to execute certain skills.) """
    is_present: bool = False
    """ Dynamic property indicating whether the user is currently present on the remote server. """


class RemoteUser(RemoteUaObject["RemoteMachine"], UaDataChangeSubscriber):
    """The remote interface for a server-side `User`."""

    def __init__(
        self,
        *,
        name: str | ua.LocalizedText,
        ua_node: Node,
        server: RemoteServer,
        parent: RemoteMachine,
        **kwargs,
    ) -> None:
        """Create a new instance."""
        super().__init__(name=name, ua_node=ua_node, parent=parent, server=server, **kwargs)

        self.data = RemoteUserData(name=self.name)
        self.user_presence_changed = Signal[RemoteUser, bool]()

    @override
    async def _init(self) -> None:
        await super()._init()

        for child, browse_name in (await get_children_browse_names(self.ua_node)).items():
            match browse_name.Name:
                case "AllowMultiple":
                    self.data.allow_multiple = await child.read_value()
                case "UserLevel":
                    self.data.user_level = await child.read_value()
                case "MaxAccessLevel":
                    self.data.max_access_level = await child.read_value()
                case "IsPresent":
                    self.data.is_present = await child.read_value()
                    await self.server.subscription_manager.subscribe_data_change(self, child)

    def dto(self) -> RemoteUserDTO:
        """Return the data transfer object."""
        return RemoteUserDTO(
            name=self.data.name,
            path=self.path,
            ua_node_id=self.ua_node.nodeid.to_string(),
            ua_type=self.ua_type,
            allow_multiple=self.data.allow_multiple,
            current_access_level=int(self.data.user_level),
            max_access_level=self.data.max_access_level,
            is_present=self.data.is_present,
        )

    @override
    def __str__(self) -> str:
        return f"User '{self.name}' (access-level: {self.data.max_access_level}, present: {self.data.is_present})"

    @override
    async def ua_on_data_change(self, node: Node, val: Any, data: DataChangeNotif) -> None:
        # there is currently only one subscription, so we don't need to check which node it is
        self.data.is_present = val

        await self.user_presence_changed.send(self, val)
