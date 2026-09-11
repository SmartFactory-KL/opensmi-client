# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""The remote interface for a server-side `BaseMachine`."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Iterable
from typing import TYPE_CHECKING

from asyncua import ua
from asyncua.common.node import Node
from opensmi.core.ua_node_util import get_children_browse_names
from typing_extensions import override

from opensmi.client.browse import BrowseFeature, BrowseRule
from opensmi.client.dto import RemoteMachineDTO
from opensmi.client.remote_component import BaseRemoteComponent
from opensmi.client.remote_user import RemoteUser

if TYPE_CHECKING:
    from opensmi.client.remote_lock import RemoteLock
    from opensmi.client.remote_server import RemoteServer


class RemoteMachine(BaseRemoteComponent):
    """The remote interface for a server-side `BaseMachine`."""

    def __init__(
        self,
        *,
        name: str,
        ua_node: Node,
        server: RemoteServer,
        browse_rules: Iterable[BrowseRule] | None = None,
    ) -> None:
        """Create a new instance. Must be asynchronously initialized with ``init()`` before use."""
        super().__init__(
            name=name,
            ua_node=ua_node,
            server=server,
            browse_rules=browse_rules,
            parent=None,  # pyright: ignore[reportArgumentType]
        )

        self._users: list[RemoteUser] = []

    @property
    def lock(self) -> RemoteLock:
        """Return the remote lock instance. Is mandatory for machines."""
        assert self._lock is not None
        return self._lock

    def dto(self) -> RemoteMachineDTO:
        """Return the data transfer object."""
        return RemoteMachineDTO(
            name=self.name,
            path=self.path,
            ua_node_id=self.ua_node.nodeid.to_string(),
            ua_type=self.ua_type,
            #
            current_state=self.current_state,
            lock=self.lock.dto() if self.lock is not None else None,
            #
            attributes=self.attributes.dto(),
            identification=self.identification.dto(),
            monitoring=self.monitoring.dto(),
            parameter_set=self.parameter_set.dto(),
            #
            skill_set=[s.dto() for s in self.skill_set.values()],
            method_set=[m.dto() for m in self.method_set.values()],
            components=[c.dto() for c in self.components.values()],
            resources=[r.dto() for r in self.resources.values()],
            # machine specific:
            users=[u.dto() for u in self._users],
        )

    @property
    def users(self) -> list[RemoteUser]:
        """Users provided by this remote machine. Might be empty if not browsed or not supported by the server."""
        return self._users

    async def _browse_users(self, ua_node: Node) -> None:
        tasks: list[Awaitable] = []
        users_browsed = await get_children_browse_names(ua_node)
        for user_node, browse_name in users_browsed.items():
            user = RemoteUser(name=browse_name.Name, ua_node=user_node, parent=self, server=self.server)
            tasks.append(user.init())
            tasks.append(user.ua_read_type())
            self._users.append(user)

        await asyncio.gather(*tasks, return_exceptions=True)

    @override
    async def _init_machinery_building_blocks_handle_node(
        self,
        ua_node: Node,
        browse_name: ua.QualifiedName,
        evaluated_browse_features: BrowseFeature,
    ) -> bool:

        if browse_name.Name == "Users":
            if BrowseFeature.USERS in evaluated_browse_features:
                await self._browse_users(ua_node)
            else:
                self.logger.info("Skipped browsing users...")
            return True

        return False
