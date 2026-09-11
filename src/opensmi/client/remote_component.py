# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""The remote interface for a server-side `BaseMachineryItem`."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from asyncua import ua
from asyncua.common.node import Node
from asyncua.common.subscription import DataChangeNotif
from opensmi.core import Signal
from opensmi.core.protocols import UaEvent
from opensmi.core.subscription_manager import UaDataChangeSubscriber, UaEventSubscriber
from opensmi.core.ua_node_util import get_children_browse_names, get_type_definition
from typing_extensions import deprecated, override

from opensmi.client.browse import BrowseFeature, BrowseRule, evaluate_browse_features
from opensmi.client.dto import RemoteComponentDTO
from opensmi.client.remote_method import RemoteMethod
from opensmi.client.remote_skill import RemoteSkill
from opensmi.client.remote_ua_object import RemoteUaObject
from opensmi.client.remote_variable_container import (
    Attributes,
    Identification,
    Monitoring,
    ParameterSet,
)

if TYPE_CHECKING:
    from opensmi.client.remote_lock import RemoteLock
    from opensmi.client.remote_resource import RemoteResource
    from opensmi.client.remote_server import RemoteServer


@dataclass(slots=True, frozen=True, kw_only=True)
class NotificationEvent:
    """OPC UA notification event data."""

    message: str
    """Human-readable message."""
    severity: int
    """Severity level of the message: 0 - 1000."""
    error_code: str
    """Machine-readable error code. Might be empty"""


class BaseRemoteComponent(RemoteUaObject["BaseRemoteComponent"], UaDataChangeSubscriber, UaEventSubscriber):
    """The remote interface for a server-side `BaseMachineryItem`."""

    _ua_skill_set_node: Node
    _ua_state_machine_node: Node
    _ua_components: Node

    def __init__(
        self,
        *,
        name: str | ua.LocalizedText,
        ua_node: Node,
        parent: BaseRemoteComponent,
        server: RemoteServer,
        browse_rules: Iterable[BrowseRule] | None,
        ua_type: str | None = None,
        **kwargs,
    ) -> None:
        """*Cooperative* constructor."""
        super().__init__(
            name=name,
            ua_node=ua_node,
            parent=parent,
            server=server,
            browse_rules=browse_rules,
            ua_type=ua_type,
            **kwargs,
        )

        self.skill_set: dict[str, RemoteSkill] = {}
        self.feasibility_check_set: dict[str, RemoteSkill] = {}
        self.precondition_check_set: dict[str, RemoteSkill] = {}
        self.components: dict[str, RemoteComponent] = {}
        self.method_set: dict[str, RemoteMethod] = {}
        self.resources: dict[str, RemoteResource] = {}

        self._current_state: str = "Unknown"
        self._ua_current_state: Node | None = None

        self._lock: RemoteLock | None = None

        self.identification = Identification(parent=self, server=server)
        self.monitoring = Monitoring(parent=self, server=server)
        self.attributes = Attributes(parent=self, server=server)
        self.parameter_set = ParameterSet(parent=self, server=server)

        self.status_changed = Signal[BaseRemoteComponent, str]()
        self.notification_received = Signal[BaseRemoteComponent, NotificationEvent]()

    @property
    def current_state(self) -> str:
        """(Cached) Current state of the remote component (may not be up-to-date due to subscription delay)."""
        return self._current_state

    async def read_current_state(self) -> str:
        """Read the current state of the remote component.

        Will be ``Unknown`` when there is no OPC UA current state node.
        """
        if self._ua_current_state is not None:
            return await self._ua_current_state.read_value()
        return "Unknown"

    @property
    def name(self) -> str:
        """Return the name of the remote module."""
        return self._name

    @name.setter
    def name(self, value: str | ua.LocalizedText) -> None:
        if isinstance(value, ua.LocalizedText):
            self._name = str(value.Text)
        else:
            self._name = value

    @property
    def type(self) -> str:
        """Return human-readable component type (mainly for UI purposes)."""
        return self._ua_type

    async def _browse_skill(self, ua_node: Node, name: str) -> None:
        self.logger.info("Processing skill...", name=name)
        tasks: list[Awaitable] = []
        for child, browse_name in (await get_children_browse_names(ua_node)).items():
            match browse_name.Name:
                case "SkillExecution":
                    skill = RemoteSkill(
                        name=name, ua_node=child, server=self.server, parent=self, browse_rules=self.browse_rules
                    )
                    tasks.append(skill.init())
                    tasks.append(skill.ua_read_type())
                    self.skill_set[name] = skill
                case "FeasibilityCheck":
                    feasibility_check = RemoteSkill(
                        name=name, ua_node=child, server=self.server, parent=self, browse_rules=self.browse_rules
                    )
                    tasks.append(feasibility_check.init())
                    tasks.append(feasibility_check.ua_read_type())
                    self.feasibility_check_set[name] = feasibility_check
                case "PreconditionCheck":
                    precondition_check = RemoteSkill(
                        name=name, ua_node=child, server=self.server, parent=self, browse_rules=self.browse_rules
                    )
                    tasks.append(precondition_check.init())
                    tasks.append(precondition_check.ua_read_type())
                    self.precondition_check_set[name] = precondition_check
                case _:
                    self.logger.warning(
                        "Found unsupported node in skill node!", ua_node=str(child.nodeid), browse_name=browse_name
                    )

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _browse_skill_set(self, ua_node: Node) -> None:
        """Browse all skills in given ``ua_node``."""
        self._ua_skill_set_node = ua_node

        tasks: list[Awaitable] = []
        for child, browse_name in (await get_children_browse_names(ua_node)).items():
            tasks.append(self._browse_skill(ua_node=child, name=browse_name.Name))

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _browse_lock(self, ua_node: Node) -> None:
        """Browse a single lock in given ``ua_node``."""
        from opensmi.client.remote_lock import RemoteLock

        lock = RemoteLock(server=self.server, parent=self)
        lock.ua_node = ua_node
        await asyncio.gather(lock.init(), lock.ua_read_type(), return_exceptions=True)
        self.lock = lock

    async def _browse_component(self, ua_node: Node, *, name: str) -> None:
        """Browse a single component in given ``ua_node`` with ``name``."""
        try:
            # TODO(CaHa): enable something like this via setup before browsing
            # class_map = {
            #     "PortType": RemotePort,
            #     "StorageType": RemoteStorage,
            #     "StorageSlotType": RemoteStorageSlot
            # }
            # type_definition = await get_type_definition(ua_component_node)
            #
            # component_class = class_map.get(type_definition, RemoteComponent)
            ua_type = await get_type_definition(ua_node)
            component = RemoteComponent(
                name=name,
                ua_node=ua_node,
                parent=self,
                server=self.server,
                browse_rules=self.browse_rules,
                ua_type=ua_type,
            )
            await component.init()

            self.components[name] = component
        except Exception:
            self.logger.warning(
                "Failed to initialize component",
                component_name=name,
                ua_node=ua_node.nodeid.to_string(),
            )

    async def _browse_components(self, ua_node: Node) -> None:
        """Browse all components in given ``ua_node``."""
        children = await get_children_browse_names(ua_node)
        await asyncio.gather(
            *(self._browse_component(child, name=browse_name.Name) for child, browse_name in children.items())
        )

    async def _browse_resource(self, ua_node: Node, *, name: str) -> None:
        """Browse a single resource in given ``ua_node`` with ``name``."""
        from opensmi.client.remote_resource import RemoteResource  # prevent circular import

        try:
            resource = RemoteResource(
                name=name, ua_node=ua_node, server=self.server, parent=self, browse_rules=self.browse_rules
            )
            await resource.init()
            await resource.ua_read_type()
            self.resources[name] = resource
        except Exception:
            self.logger.warning(
                "Failed to initialize resource",
                resource_name=name,
                ua_node=ua_node.nodeid.to_string(),
            )

    async def _browse_resources(self, ua_node: Node) -> None:
        """Browse all resources in given ``ua_node``."""
        children = await get_children_browse_names(ua_node)
        await asyncio.gather(
            *(self._browse_resource(child, name=browse_name.Name) for child, browse_name in children.items())
        )

    async def _init_machinery_building_blocks_handle_node(
        self,
        ua_node: Node,
        browse_name: ua.QualifiedName,
        evaluated_browse_features: BrowseFeature,
    ) -> bool:
        """Handle child node found during browsing our machinery building blocks OPC UA node.

        :return: ``True`` if handled, ``False`` otherwise.
        """
        return False

    async def _browse_machinery_building_blocks(self, ua_node: Node, evaluated_browse_features: BrowseFeature) -> None:
        for child, browse_name in (await get_children_browse_names(ua_node)).items():
            match browse_name.Name:
                case "MachineryItemState":
                    self._ua_current_state = await child.get_child("CurrentState")  # no namespace index
                    await self.server.subscription_manager.subscribe_data_change(
                        handler=self, nodes=self._ua_current_state
                    )
                case _:
                    if not await self._init_machinery_building_blocks_handle_node(
                        child, browse_name, evaluated_browse_features
                    ):
                        self.logger.warning("Found unsupported machinery building block", browse_name=browse_name)

    async def _browse_method(self, ua_node: Node, *, name: str) -> None:
        """Browse a single method in given ``ua_node`` with ``name``."""
        try:
            method = RemoteMethod(
                name=name, ua_node=ua_node, server=self.server, parent=self, browse_rules=self.browse_rules
            )
            await method.init()
            await method.ua_read_type()
            self.method_set[name] = method
        except Exception:
            self.logger.warning(
                "Failed to initialize method",
                method_name=name,
                ua_node=ua_node.nodeid.to_string(),
            )

    async def _browse_methods(self, ua_node: Node) -> None:
        """Browse all methods in given ``ua_node``."""
        children = await get_children_browse_names(ua_node)
        await asyncio.gather(
            *(self._browse_method(child, name=browse_name.Name) for child, browse_name in children.items())
        )

    @override
    async def _init(self) -> None:  # noqa: C901
        """Set up all subscriptions for all subcomponents and itself."""
        await super()._init()

        evaluated_browse_features = evaluate_browse_features(self.browse_rules, self.path)
        self.logger.info("Browsing Component", evaluated_browse_features=evaluated_browse_features)

        tasks: list[Awaitable] = []
        for child, browse_name in (await get_children_browse_names(self.ua_node)).items():
            match browse_name.Name:
                case "Lock":
                    if BrowseFeature.LOCK in evaluated_browse_features:
                        tasks.append(self._browse_lock(child))
                    else:
                        self.logger.info("Skipped browsing lock...")
                case "SkillSet":
                    if BrowseFeature.SKILL_SET in evaluated_browse_features:
                        tasks.append(self._browse_skill_set(child))
                    else:
                        self.logger.info("Skipped browsing skill set...")
                case "Notification":
                    tasks.append(self.server.subscription_manager.subscribe_event(self, child))
                case "Identification":
                    if BrowseFeature.IDENTIFICATION in evaluated_browse_features:
                        self.identification.ua_node = child
                        tasks.append(self.identification.init())
                    else:
                        self.logger.info("Skipped browsing identification...")
                case "Attributes":
                    if BrowseFeature.ATTRIBUTES in evaluated_browse_features:
                        self.attributes.ua_node = child
                        tasks.append(self.attributes.init())
                    else:
                        self.logger.info("Skipped browsing attributes...")
                case "Monitoring":
                    if BrowseFeature.MONITORING in evaluated_browse_features:
                        self.monitoring.ua_node = child
                        tasks.append(self.monitoring.init())
                    else:
                        self.logger.info("Skipped browsing monitoring...")
                case "ParameterSet":
                    if BrowseFeature.PARAMETER_SET in evaluated_browse_features:
                        self.parameter_set.ua_node = child
                        tasks.append(self.parameter_set.init())
                    else:
                        self.logger.info("Skipped browsing parameter set...")
                case "Components":
                    if BrowseFeature.COMPONENTS in evaluated_browse_features:
                        tasks.append(self._browse_components(child))
                    else:
                        self.logger.info("Skipped browsing sub-components...")
                case "Resources":
                    if BrowseFeature.RESOURCES in evaluated_browse_features:
                        tasks.append(self._browse_resources(child))
                    else:
                        self.logger.info("Skipped browsing resources...")
                case "MachineryBuildingBlocks":
                    tasks.append(self._browse_machinery_building_blocks(child, evaluated_browse_features))
                case "MethodSet":
                    if BrowseFeature.METHOD_SET in evaluated_browse_features:
                        tasks.append(self._browse_methods(child))
                    else:
                        self.logger.info("Skipped browsing method set...")
                case _:
                    self.logger.warning("Found unhandled child node!", node_id=child.nodeid.to_string())

        tasks.append(self.ua_read_type())
        await asyncio.gather(*tasks, return_exceptions=True)

    @override
    async def ua_on_event(self, ua_event: UaEvent) -> None:
        """Handle "message" event notifications."""
        if ua_event.Message.Text is None:
            self.logger.warning("Received OPC UA event with no message text!", ua_event=ua_event)
            return
        # TODO remove split when work-around no longer required
        text_splits: list[str] = ua_event.Message.Text.split("|||", maxsplit=1)
        text = text_splits[0]
        try:
            error_code = ua_event.ErrorCode  # TODO(CaHa): Fix reading the error code from events
        except AttributeError:
            try:
                error_code = text_splits[1]
            except IndexError:
                error_code = ""

        self.logger.debug("Received OPC UA event", text=text, severity=ua_event.Severity, error_code=error_code)

        notification = NotificationEvent(message=text, severity=ua_event.Severity, error_code=error_code)
        await self.notification_received.send(self, notification)

    @override
    async def ua_on_data_change(self, node: Node, val: Any, data: DataChangeNotif) -> None:
        # we only subscribed to one node
        try:
            self._current_state = val.Text
        except AttributeError:
            # some servers use normal strings instead
            self._current_state = val if val is not None else ""

        await self.status_changed.send(self, self._current_state)

    @deprecated("Please use parameters.read_all() instead.")
    async def read_parameters(self) -> Mapping[str, Any]:
        """Read all parameter set variables."""
        return await self.parameter_set.read_all()

    @deprecated("Please use parameters.write_all() instead.")
    async def write_parameters(self, parameters: Mapping[str, Any]) -> None:
        """Write given parameter set variables."""
        await self.parameter_set.write_all(parameters)

    @deprecated("Please use monitoring.read_all() instead.")
    async def read_monitoring(self) -> Mapping[str, Any]:
        """Read all parameter set variables."""
        return await self.monitoring.read_all()


class RemoteComponent(BaseRemoteComponent):
    """The remote interface for a server-side `BaseComponent`."""

    @property
    def lock(self) -> RemoteLock | None:
        """Return the remote lock instance if it exists. Is optional for components."""
        return self._lock

    def dto(self) -> RemoteComponentDTO:
        """Return the data transfer object."""
        return RemoteComponentDTO(
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
        )
