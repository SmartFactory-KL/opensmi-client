# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""The remote interface for a server-side `BaseSkill`."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from asyncua import ua
from asyncua.common.methods import call_method
from asyncua.common.node import Node
from asyncua.common.subscription import DataChangeNotif
from asyncua.ua import LocalizedText
from opensmi.core import Signal, SkillState
from opensmi.core.errors import SkillHaltedError, SkillNotSuspendableError
from opensmi.core.subscription_manager import UaDataChangeSubscriber
from opensmi.core.ua_node_util import get_children_browse_names, get_type_definition
from typing_extensions import override

from opensmi.client.browse import BrowseRule
from opensmi.client.dto import RemoteSkillDTO
from opensmi.client.remote_callable import RemoteCallable

if TYPE_CHECKING:
    from opensmi.client.remote_component import BaseRemoteComponent
    from opensmi.client.remote_server import RemoteServer


@dataclass(slots=True)
class _RemoteSkillUaNodes:
    """Data class for all `RemoteSkill` OPC UA nodes."""

    state_machine: Node
    current_state: Node
    halt: ua.NodeId
    reset: ua.NodeId
    start: ua.NodeId
    suspend: ua.NodeId | None = None


def _parse_state(val: str | LocalizedText) -> SkillState:
    try:
        state_str: str = val.Text  # type: ignore
    except AttributeError:
        # some servers ignore the node-set datatype specification and use normal strings instead...
        state_str = str(val)

    # assume the worst/safest state
    new_state = SkillState.HALTED if not state_str else SkillState[state_str.upper()]
    return new_state


class RemoteSkill(RemoteCallable, UaDataChangeSubscriber):
    """The remote interface for a server-side `BaseSkill`."""

    _ua_nodes: _RemoteSkillUaNodes

    def __init__(
        self,
        *,
        name: str,
        ua_node: Node,
        server: RemoteServer,
        parent: BaseRemoteComponent,
        browse_rules: Iterable[BrowseRule] | None,
    ) -> None:
        """Create a new instance. Must be asynchronously initialized with ``init()`` before use."""
        super().__init__(name=name, ua_node=ua_node, server=server, parent=parent, browse_rules=browse_rules)

        self._current_state = SkillState.HALTED
        self.state_changed = Signal[RemoteSkill, SkillState]()

    def dto(self) -> RemoteSkillDTO:
        """Return the data transfer object."""
        return RemoteSkillDTO(
            name=self.name,
            path=self.path,
            ua_node_id=self.ua_node.nodeid.to_string(),
            ua_type=self.ua_type,
            min_access_level=int(self.min_access_level) if self.min_access_level is not None else None,
            parameter_set=self.parameter_set.dto(),
            monitoring=self.monitoring.dto(),
            final_result_data=self.final_result_data.dto(),
            ua_depends_on_node_ids=[node_id.to_string() for node_id in self._depends_on],
            ua_dependency_of_node_ids=[node_id.to_string() for node_id in self._dependency_of],
            current_state=self.current_state.name,
            is_suspendable=self.suspendable,
        )

    async def _browse_state_machine(self, ua_node: Node) -> None:
        ua_current_state: Node | None = None
        ua_halt: Node | None = None
        ua_reset: Node | None = None
        ua_start: Node | None = None
        ua_suspend: Node | None = None

        for child, browse_name in (await get_children_browse_names(ua_node)).items():
            match browse_name.Name:
                case "CurrentState":
                    ua_current_state = child
                    await self.server.subscription_manager.subscribe_data_change(handler=self, nodes=[ua_current_state])
                case "Halt":
                    ua_halt = child
                case "Reset":
                    ua_reset = child
                case "Start":
                    ua_start = child
                case "Suspend":
                    ua_suspend = child

        assert ua_current_state is not None
        assert ua_halt is not None
        assert ua_reset is not None
        assert ua_start is not None

        self._ua_nodes = _RemoteSkillUaNodes(
            state_machine=ua_node,
            current_state=ua_current_state,
            halt=ua_halt.nodeid,
            reset=ua_reset.nodeid,
            start=ua_start.nodeid,
            suspend=ua_suspend.nodeid if ua_suspend is not None else None,
        )

        self.logger.debug("Completed of state machine browsing!")

    @override
    def _init_handle_node(self, ua_node: Node, browse_name: ua.QualifiedName) -> list[Awaitable]:
        match browse_name.Name:
            case "StateMachine":
                self._ua_state_machine_node = ua_node
                return [self._browse_state_machine(ua_node)]
            case _:
                self.logger.warning("Found unsupported UA node in skill", ua_node=ua_node, browse_name=browse_name)
        return []

    @property
    def suspendable(self) -> bool:
        """Indicates whether the skill is suspendable."""
        return self._ua_nodes.suspend is not None

    async def start(self) -> None:
        """Call the start method on the remote skill. User is responsible for handling any errors."""
        await call_method(self._ua_nodes.state_machine, self._ua_nodes.start)

    async def reset(self) -> None:
        """Call the reset method on the remote skill. User is responsible for handling any errors."""
        await call_method(self._ua_nodes.state_machine, self._ua_nodes.reset)

    async def halt(self) -> None:
        """Call the halt method on the remote skill. User is responsible for handling any errors."""
        await call_method(self._ua_nodes.state_machine, self._ua_nodes.halt)

    async def suspend(self) -> None:
        """Call the suspend method on the remote skill. User is responsible for handling any errors."""
        if self._ua_nodes.suspend is not None:
            await call_method(self._ua_nodes.state_machine, self._ua_nodes.suspend)
        else:
            msg = f"Skill '{self.name}' is not suspendable!"
            raise SkillNotSuspendableError(msg) from None

    async def wait_for_state(self, target_state: SkillState) -> None:
        """Block until the skill either reached the given state or was halted. Given timeout is in seconds."""
        start_state = self.current_state

        while True:
            if self.current_state == target_state:
                break
            if self.current_state == SkillState.HALTED and start_state != SkillState.HALTED:
                msg = f"Skill '{self.name}' halted while waiting for state '{target_state.name}'!"
                raise SkillHaltedError(msg)
            await asyncio.sleep(0.1)

    async def _handle_state_change(self, val: str | LocalizedText) -> None:
        try:
            new_state = _parse_state(val)
            if new_state == self._current_state:
                return

            self._current_state = new_state
            await self.state_changed.send(self, self._current_state)
        except KeyError:
            self.logger.warning("Received unknown state value, ignoring!", state=val)

    @override
    async def ua_on_data_change(self, node: Node, val: Any, data: DataChangeNotif) -> None:  # type: ignore
        """Handle current state data change notifications from OPC UA."""
        await self._handle_state_change(val)  # we only subscribed to exactly one node, the current state

    @property
    def current_state(self) -> SkillState:
        """Cached current state of the remote skill."""
        return self._current_state

    async def read_current_state(self) -> SkillState:
        """Read the current state of the remote skill."""
        ua_value = await self._ua_nodes.current_state.read_value()
        return _parse_state(ua_value)

    @override
    async def ua_read_type(self) -> str:
        """Read the OPC UA type, resolved from node ID browse name."""
        # self.ua_node is of type SkillElementType "SkillExecution"
        parent_node = await self.ua_node.get_parent()
        assert parent_node is not None
        self._ua_type = await get_type_definition(parent_node)
        return self._ua_type
