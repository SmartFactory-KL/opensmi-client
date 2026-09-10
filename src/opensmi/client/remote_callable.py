# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""Base class for `RemoteMethod` and `RemoteSkill`."""

from __future__ import annotations

import asyncio
from abc import abstractmethod
from collections.abc import Awaitable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from asyncua import ua
from asyncua.common.node import Node
from asyncua.ua import NodeId
from opensmi.core.ua_node_util import get_children_browse_names
from typing_extensions import deprecated, override

from opensmi.client.browse import BrowseFeature, BrowseRule, evaluate_browse_features
from opensmi.client.remote_ua_object import RemoteUaObject
from opensmi.client.remote_variable_container import (
    FinalResultData,
    Monitoring,
    ParameterSet,
)

if TYPE_CHECKING:
    from opensmi.client.remote_component import BaseRemoteComponent
    from opensmi.client.remote_server import RemoteServer


class RemoteCallable(RemoteUaObject["BaseRemoteComponent"]):
    """Base class all `RemoteMethod` and `RemoteSkill`."""

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

        self._min_access_level: int | None = None
        self.final_result_data = FinalResultData(parent=self, server=server)
        self.monitoring = Monitoring(parent=self, server=server)
        self.parameter_set = ParameterSet(parent=self, server=server)

        self._depends_on: list[NodeId] = []
        self._dependency_of: list[NodeId] = []

    @override
    async def _init(self) -> None:
        await super()._init()

        evaluated_browse_features = evaluate_browse_features(self.browse_rules, self.path)

        tasks: list[Awaitable] = []
        for child, browse_name in (await get_children_browse_names(self.ua_node)).items():
            match browse_name.Name:
                case "MinAccessLevel":
                    self._min_access_level = await child.read_value()
                case "Monitoring":
                    if BrowseFeature.MONITORING in evaluated_browse_features:
                        self.monitoring.ua_node = child
                        tasks.append(self.monitoring.init())
                    else:
                        self.logger.info("Skipping browsing monitoring...")
                case "ParameterSet":
                    if BrowseFeature.PARAMETER_SET in evaluated_browse_features:
                        self.parameter_set.ua_node = child
                        tasks.append(self.parameter_set.init())
                    else:
                        self.logger.info("Skipping browsing parameter set...")
                case "FinalResultData":
                    if BrowseFeature.FINAL_RESULT_DATA in evaluated_browse_features:
                        self.final_result_data.ua_node = child
                        tasks.append(self.final_result_data.init())
                    else:
                        self.logger.info("Skipping browsing final result data...")
                case "Requirements":
                    tasks.append(self._browse_dependencies(child))
                case _:
                    tasks.extend(self._init_handle_node(child, browse_name))

        await asyncio.gather(*tasks, return_exceptions=True)

    @abstractmethod
    def _init_handle_node(self, ua_node: Node, browse_name: ua.QualifiedName) -> list[Awaitable]:
        """Handle child node found during browsing our OPC UA node.

        :return: ``True`` if handled, ``False`` otherwise.
        """
        raise NotImplementedError

    @property
    def min_access_level(self) -> int | None:
        """Minimum access level required to interact with the skill/method.

        Is ``None`` when the remote server does not provide this information.
        """
        return self._min_access_level

    @property
    def ua_depends_on(self) -> Sequence[NodeId]:
        """Return all browsed OPC UA node IDs of which we depend on."""
        return self._depends_on

    @property
    def ua_dependency_of(self) -> Sequence[NodeId]:
        """Return all browsed OPC UA node IDs of which we are a dependency of."""
        return self._dependency_of

    async def _browse_dependencies(self, ua_node: Node) -> None:
        ua_ref_depends_on = ua.FourByteNodeId(ua.Int32(ua.object_ids.ObjectIds.Requires))

        for reference in await ua_node.get_references(refs=ua_ref_depends_on):
            if reference.IsForward:
                self._depends_on.append(reference.NodeId)
            else:
                self._dependency_of.append(reference.NodeId)

    @deprecated("Please use final_result_data.read_all() instead.")
    async def read_results(self) -> Mapping[str, Any]:
        """Read all final result data variables."""
        return await self.final_result_data.read_all()

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
