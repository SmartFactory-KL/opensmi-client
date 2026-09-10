# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""Various containers for `RemoteVariable`. The containers themselves are `RemoteUaObject`."""

import asyncio
from collections.abc import Awaitable, Iterator, Mapping
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from asyncua import ua
from asyncua.common.node import Node
from opensmi.core.ua_node_util import get_children_browse_names
from typing_extensions import override

from opensmi.client.dto import RemoteVariableDTO
from opensmi.client.remote_ua_object import RemoteUaObject
from opensmi.client.remote_variable import RemoteVariable

if TYPE_CHECKING:
    from opensmi.client.remote_server import RemoteServer

_ParentType = TypeVar("_ParentType", bound="RemoteUaObject")


class RemoteVariableContainer(RemoteUaObject[_ParentType], Generic[_ParentType]):
    """Container for `RemoteVariable`. Is itself an `RemoteUaObject`."""

    def __init__(
        self,
        *,
        name: str | ua.LocalizedText,
        ua_node: Node | None,
        parent: _ParentType,
        server: "RemoteServer",
        **kwargs,
    ) -> None:
        """Create a new instance. Must be asynchronously initialized with ``init()`` before use."""
        super().__init__(name=name, ua_node=ua_node, parent=parent, server=server, **kwargs)
        self._variables: dict[str, RemoteVariable] = {}

    async def _browse_variable(self, ua_node: Node, name: str) -> None:
        try:
            var = RemoteVariable(name=name, ua_node=ua_node, server=self.server, parent=self)
            await var.init()
            await var.ua_read_type()
            self._variables[name] = var
        except Exception:
            self.logger.warning(
                "Failed to initialize variable",
                variable_name=name,
                ua_node=ua_node.nodeid.to_string(),
            )

    @override
    async def _init(self) -> None:
        """Discover our remote variables."""
        tasks: list[Awaitable] = []
        for child, browse_name in (await get_children_browse_names(self.ua_node)).items():
            existing_var = self.server.get_ua_object_optional(child.nodeid)
            if isinstance(existing_var, RemoteVariable):
                self.logger.info("Found existing variable", variable_name=existing_var.name)
                self._variables[existing_var.name] = existing_var  # pyright: ignore[reportArgumentType]
            else:
                tasks.append(self._browse_variable(child, browse_name.Name))

        await asyncio.gather(*tasks, return_exceptions=True)

    async def read_all(self) -> Mapping[str, Any]:
        """Read all contained variables."""
        return {variable.name: await variable.read_value() for variable in self}

    async def write_all(self, values: Mapping[str, Any]) -> None:
        """Write all given values to the corresponding variables."""
        for name, value in values.items():
            await self[name].write_value(value)

    def dto(self) -> list[RemoteVariableDTO]:
        """Return the data transfer object."""
        if not self.is_initialized:
            return []
        return [var.dto() for var in self]

    #
    # Pythonic container interface
    #

    def __getitem__(self, name: str) -> RemoteVariable:
        """Get a `RemoteVariable` by the given ``name``.

        :raises KeyError: If no variable with given name exists.
        """
        child = self._variables.get(name)
        if child is not None:
            return child
        msg = f"No RemoteVariable named '{name}' contained in {self.path}!"
        raise KeyError(msg)

    def __contains__(self, key: object) -> bool:
        """Whether a variable with given ``name`` exists in this container."""
        return key in self._variables

    def __len__(self) -> int:
        """Return the number of contained `RemoteVariable`."""
        return len(self._variables)

    def __iter__(self) -> Iterator[RemoteVariable]:
        """Iterate over all contained `RemoteVariable`."""
        return iter(self._variables.values())


class Attributes(RemoteVariableContainer, Generic[_ParentType]):
    """Container of attribute `RemoteVariable` for `RemoteComponent`, etc."""

    def __init__(self, *, parent: _ParentType, server: "RemoteServer") -> None:
        """Create a new instance. Must be asynchronously initialized with ``init()`` before use."""
        super().__init__(name="Attributes", ua_node=None, parent=parent, server=server)


class Identification(RemoteVariableContainer, Generic[_ParentType]):
    """Container of identification `RemoteVariable` for `RemoteComponent`, `RemoteMachine`, etc."""

    def __init__(self, *, parent: _ParentType, server: "RemoteServer") -> None:
        """Create a new instance. Must be asynchronously initialized with ``init()`` before use."""
        super().__init__(name="Identification", ua_node=None, parent=parent, server=server)


class Monitoring(RemoteVariableContainer, Generic[_ParentType]):
    """Container of monitoring `RemoteVariable` for `RemoteComponent`, `RemoteSkill`, etc."""

    def __init__(self, *, parent: _ParentType, server: "RemoteServer") -> None:
        """Create a new instance. Must be asynchronously initialized with ``init()`` before use."""
        super().__init__(name="Monitoring", ua_node=None, parent=parent, server=server)


class ParameterSet(RemoteVariableContainer, Generic[_ParentType]):
    """Container of parameter `RemoteVariable` for `RemoteComponent`, `RemoteSkill`, etc."""

    def __init__(self, *, parent: _ParentType, server: "RemoteServer") -> None:
        """Create a new instance. Must be asynchronously initialized with ``init()`` before use."""
        super().__init__(name="ParameterSet", ua_node=None, parent=parent, server=server)


class FinalResultData(RemoteVariableContainer, Generic[_ParentType]):
    """Container for final result data of `RemoteSkill` and `RemoteMethod`."""

    def __init__(self, *, parent: _ParentType, server: "RemoteServer") -> None:
        """Create a new instance. Must be asynchronously initialized with ``init()`` before use."""
        super().__init__(name="FinalResultData", ua_node=None, parent=parent, server=server)
