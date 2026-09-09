# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""The remote interface for a server-side `BaseMethod`."""

from __future__ import annotations

from collections.abc import Awaitable, Mapping
from typing import Any

from asyncua import ua
from asyncua.common.node import Node
from typing_extensions import override

from open_smi_client.dto import RemoteCallableDTO
from open_smi_client.remote_callable import RemoteCallable


class RemoteMethod(RemoteCallable):
    """The remote interface for a server-side `BaseMethod`."""

    _ua_call: ua.NodeId

    def dto(self) -> RemoteCallableDTO:
        """Return the data transfer object."""
        return RemoteCallableDTO(
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
        )

    @override
    def _init_handle_node(self, ua_node: Node, browse_name: ua.QualifiedName) -> list[Awaitable]:
        match browse_name.Name:
            case "Call":
                assert isinstance(ua_node.nodeid, ua.NodeId)
                self._ua_call = ua_node.nodeid
            case _:
                self.logger.warning("Found Unsupported UA node in method", ua_node=ua_node, browse_name=browse_name)
        return []

    async def call(self) -> Mapping[str, Any]:
        """Call the remote method and return the final result data.

        User is responsible for handling any errors.
        """
        await self.ua_node.call_method(self._ua_call)
        return await self.final_result_data.read_all()
