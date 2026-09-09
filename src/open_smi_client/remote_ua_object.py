# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""Base class for remote UA objects."""

from abc import abstractmethod
from collections.abc import AsyncGenerator, Iterable
from typing import TYPE_CHECKING, Generic, TypeVar

from asyncua import ua
from asyncua.common.node import Node
from open_smi_common.base_ua_object import BaseUaObject
from open_smi_common.lifecycle_mixin import LifecycleMixin, lifecycle
from open_smi_common.ua_node_util import get_type_definition

from open_smi_client.browse import DEFAULT_BROWSE_RULE, BrowseRule

if TYPE_CHECKING:
    from open_smi_client.remote_server import RemoteServer

_ParentType = TypeVar("_ParentType", bound="RemoteUaObject")


class RemoteUaObject(BaseUaObject["RemoteServer"], LifecycleMixin, Generic[_ParentType]):
    """Base class for remote UA objects."""

    _browse_rules: tuple[BrowseRule, ...]

    def __init__(
        self,
        *,
        name: str | ua.LocalizedText,
        ua_node: Node | None,
        parent: _ParentType,
        server: "RemoteServer",
        browse_rules: Iterable[BrowseRule] | None = None,
        ua_type: str | None = None,
        **kwargs,
    ) -> None:
        """Create a new instance. Must be asynchronously initialized with ``init()`` before use."""
        if isinstance(name, ua.LocalizedText):
            name = str(name.Text)
        self._parent = parent
        super().__init__(name=name, server=server, **kwargs)
        self._logger = self.logger.bind(path=self.path)  # pyright: ignore[reportAttributeAccessIssue]

        if ua_node:  # lock & remote variable container don't have these from the get-go
            self.ua_node = ua_node

        self._ua_type: str = ua_type or "UNKNOWN"

        if browse_rules is None:
            self._browse_rules = (DEFAULT_BROWSE_RULE,)
        else:
            self._browse_rules = tuple(browse_rules)

    @property
    def browse_rules(self) -> tuple[BrowseRule, ...]:
        """Browse rules for automatically recursively browsing the remote server."""
        return self._browse_rules

    @abstractmethod
    async def _init(self) -> None:
        pass

    async def _shutdown(self) -> None:
        pass

    @lifecycle
    async def lifecycle_object(self) -> AsyncGenerator[None]:
        """Lifecycle of the object."""
        await self._init()
        yield
        await self._shutdown()

    @property
    def parent(self) -> _ParentType:
        """Return the parent remote UA object."""
        return self._parent

    async def ua_read_type(self) -> str:
        """Read the OPC UA type, resolved from node ID browse name."""
        self._ua_type = await get_type_definition(self.ua_node)
        return self._ua_type

    @property
    def ua_type(self) -> str:
        """OPC UA type, resolved from node ID browse name."""
        return self._ua_type
