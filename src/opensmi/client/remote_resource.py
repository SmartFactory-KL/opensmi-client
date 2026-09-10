# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""The remote interface for a server-side `Resource`."""

from __future__ import annotations

from opensmi.client.dto import RemoteResourceDTO
from opensmi.client.remote_component import BaseRemoteComponent


class RemoteResource(BaseRemoteComponent):
    """The remote interface for a server-side `Resource`."""

    def dto(self) -> RemoteResourceDTO:
        """Return the data transfer object."""
        return RemoteResourceDTO(
            name=self.name,
            path=self.path,
            ua_node_id=self.ua_node.nodeid.to_string(),
            ua_type=self.ua_type,
            attributes=self.attributes.dto(),
            identification=self.identification.dto(),
            monitoring=self.monitoring.dto(),
        )
