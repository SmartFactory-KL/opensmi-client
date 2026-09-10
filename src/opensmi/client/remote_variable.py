# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""The remote interface for a server-side `UaVariable`."""

from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

import structlog
from asyncua import ua
from asyncua.common.node import Node
from asyncua.common.subscription import DataChangeNotif
from asyncua.ua import (
    DataValue,
    EnumDefinition,
    NodeId,
    NodeIdType,
    UaStatusCodeError,
    Variant,
)
from asyncua.ua.uaerrors import BadOutOfRange, BadUserAccessDenied
from opensmi.core import Signal
from opensmi.core.subscription_manager import UaDataChangeSubscriber
from opensmi.core.ua_node_util import get_properties, write_value
from typing_extensions import override

from opensmi.client.dto import RangeDTO, RemoteVariableDTO
from opensmi.client.remote_ua_object import RemoteUaObject

if TYPE_CHECKING:
    from opensmi.client.remote_server import RemoteServer
    from opensmi.client.remote_variable_container import RemoteVariableContainer

_LOGGER = structlog.getLogger("open_smi.RemoteVariable")


# def ua_value_to_simple(value: Any) -> bool | bytes | int | float | str | None:
#     """Convert given ``value`` (from OPC UA typically) to a simple JavaScript-friendly data type."""
#     if value is None:
#         return None
#     if isinstance(
#         value,
#         (
#             bool,
#             bytes,
#             int,
#             float,
#             str,
#         ),
#     ):
#         return value
#     if isinstance(value, LocalizedText):
#         return value.Text
#     if isinstance(value, NodeId):
#         return value.to_string() if not value.is_null() else None
#     _LOGGER.warning("Unsupported value type, fallback to string", type=type(value))
#     return str(value)
#
#
# def ua_value_to_dto(value: Any) -> Any | dict[str, Any]:
#     if dataclasses.is_dataclass(value):  # handle generic dataclass types used by asyncua
#         return dataclasses.asdict(value)  # type: ignore
#     return ua_value_to_simple(value)


@dataclasses.dataclass
class TimestampedValue:
    """A single value of `RemoteVariable` combined with the timestamp."""

    value: Any
    timestamp: datetime | None


class RemoteVariable(RemoteUaObject["RemoteVariableContainer"], UaDataChangeSubscriber):
    """The remote interface for a server-side `UaVariable`."""

    def __init__(
        self,
        *,
        name: str,
        ua_node: Node,
        server: RemoteServer,
        parent: RemoteVariableContainer,
    ) -> None:
        """Create a new instance. Must be asynchronously initialized with ``init()`` before use."""
        super().__init__(name=name, ua_node=ua_node, server=server, parent=parent)

        self._valid_values: dict[int, str] | dict[str, str] = {}
        self._is_writable: bool = False
        self._ua_value: ua.Variant = ua.Variant()
        self._timestamp: datetime | None = None
        self._ua_data_type: str = "UNKNOWN"
        self._ua_value_rank: int = 0
        self.unit: ua.EUInformation | None = None
        """ Unit of this variable. """
        self.range: ua.Range | None = None
        """ Range of this variable. """
        self.children: dict[str, RemoteVariable] = {}

        self.value_changed = Signal[RemoteVariable, ua.Variant]()

    @override
    async def _init(self) -> None:
        await super()._init()

        # Read all attributes we need at the same time
        (
            data_type_data_value,
            value_data_value,
            self._ua_value_rank_value,
            raw_user_access_level,
        ) = await self.ua_node.read_attributes(
            attrs=[
                ua.attribute_ids.AttributeIds.DataType,
                ua.attribute_ids.AttributeIds.Value,
                ua.attribute_ids.AttributeIds.ValueRank,
                ua.attribute_ids.AttributeIds.UserAccessLevel,
            ]
        )
        assert raw_user_access_level.Value is not None
        user_access_level: set[ua.AccessLevel] = ua.AccessLevel.parse_bitfield(raw_user_access_level.Value.Value)
        self._is_writable = ua.AccessLevel.CurrentWrite in user_access_level

        node_type_node_id: NodeId | None = data_type_data_value.Value.Value  # type: ignore

        if node_type_node_id is not None and node_type_node_id.NamespaceIndex == 0:
            try:
                self._ua_data_type = ua.object_ids.ObjectIdNames[node_type_node_id.Identifier]
            except KeyError:
                _LOGGER.warning("Could not determine UA data type", node_type_node_id=node_type_node_id)

        # Get valid values if data type is an enum
        self._valid_values = {}
        if node_type_node_id is not None:
            if isinstance(value_data_value.Value.Value, NodeId):  # type: ignore
                self._valid_values[""] = "<None>"  # pyright: ignore[reportArgumentType]
                for ref in await self.ua_node.get_references(
                    ua.FourByteNodeId(ua.Int32(ua.object_ids.ObjectIds.Utilizes))
                ):
                    self._valid_values[ref.NodeId.to_string()] = ref.DisplayName.Text
            elif (
                node_type_node_id.NamespaceIndex != 0
            ):  # not OPC UA namespace -> custom type  # TODO what about standard enums?
                (data_type_definition_data_value,) = await self.server.ua_client.uaclient.read_attributes(
                    [node_type_node_id], ua.attribute_ids.AttributeIds.DataTypeDefinition
                )
                variant = data_type_definition_data_value.Value
                assert variant is not None
                dtype_definition = variant.Value
                if isinstance(dtype_definition, EnumDefinition):
                    self._ua_data_type = "Enumeration"  # close enough
                    for field in dtype_definition.Fields:
                        self._valid_values[field.Value] = field.Name
        else:
            self.logger.warning("Node has no valid data type!")

        # handle properties
        properties = await get_properties(self.ua_node, logger=self.logger)
        self._unit = properties.unit
        self._range = properties.range

        await self.update_from_data_value(value_data_value)

        await self.server.subscription_manager.subscribe_data_change(self, self.ua_node)

    @property
    def is_writable(self) -> bool:
        """Whether this variable is writable or read-only.

        Note however, that this is separate from the current access rights of the user.
        """
        return self._is_writable

    def _process_value(self, value: Variant | None) -> Any:
        if value is None or (isinstance(value.Value, NodeId) and value.Value.is_null()):
            return None

        if len(self.valid_values) > 0 and isinstance(value.Value, (str, int)):
            try:
                return self.valid_values[value.Value]  # pyright: ignore[reportArgumentType]
            except KeyError:
                self._logger.warning("Tried to match non-existent definition", value=value.Value)
                return f"INVALID ENUM VALUE: {value.Value}"
        else:
            return value.Value

    @property
    def value(self) -> Any:
        """Cached Value of this variable."""
        return self._process_value(self._ua_value)

    @property
    def raw_value(self) -> Variant:
        """Return the raw value as an OPC UA Variant."""
        return self._ua_value

    @property
    def is_value_node_id(self) -> bool:
        """Return ``True`` if the value of this RemoteVariable is a not null Node ID."""
        return isinstance(self._ua_value.Value, NodeId) and not self._ua_value.Value.is_null()

    async def read_value(self, *, update_cache: bool = False) -> Any:
        """Read the actual value from the remote server, not the cached local one."""
        data_value = await self.ua_node.read_data_value()
        if update_cache:
            await self.update_from_data_value(data_value)
        return self._process_value(data_value.Value)

    async def read_raw_value(self) -> Variant:
        """Read the actual raw value from the remote server."""
        data_value = await self.ua_node.read_data_value()
        return data_value.Value  # pyright: ignore[reportReturnType]

    async def read_resolved_value(self) -> Any:
        """Read the resolved value of this variable. Fall back to normal value if there is nothing to resolve.

        For example, if this variable value is a node id, resolve it to the display name of the target node.
        If the display name is not readable, fall back to the browse name. If that is also not readable, return the
        last part of the string identifier of the node.
        """
        if self.is_value_node_id:
            target_node = self.server.ua_client.get_node(self._ua_value.Value)
            try:
                display_name = await target_node.read_display_name()
                return display_name.Text
            except ua.UaError:
                self._logger.warning("Could not read the display name!", node_id=target_node.nodeid.to_string())
                try:
                    browse_name = await target_node.read_browse_name()
                    return browse_name.Name
                except ua.UaError:
                    self._logger.warning("Could not read the browse name!", node_id=target_node.nodeid.to_string())
                    if target_node.nodeid.NodeIdType == NodeIdType.String:
                        identifier = cast(str, target_node.nodeid.Identifier)
                        return identifier.split(".")[-1]

        return await self.read_value()  # fallback

    @property
    def valid_values(self) -> dict[int, str] | dict[str, str]:
        """Valid values of this remote variable in dictionary form.

        Used for e.g. selection dropdown in front-end.
        """
        return self._valid_values

    @property
    def timestamp(self) -> datetime | None:
        """Timestamp of last value change."""
        return self._timestamp

    @property
    def ua_data_type(self) -> str:
        """Internal OPC UA data type."""
        return self._ua_data_type

    @property
    def ua_value_rank(self) -> int:
        """Internal OPC UA value rank (-1 for Scalar, 0 for 1 or more, >=1 exact dimension)."""
        return self._ua_value_rank

    def dto(self) -> RemoteVariableDTO:
        """Return the data transfer object."""
        return RemoteVariableDTO(
            name=self.name,
            path=self.path,
            ua_node_id=self.ua_node.nodeid.to_string(),
            ua_type=self.ua_type,
            value=self.value,
            timestamp=self.timestamp,
            is_writable=self.is_writable,
            valid_values=self.valid_values,
            unit=self.unit.DisplayName.Text if self.unit else None,
            range=RangeDTO(low=self.range.Low, high=self.range.High) if self.range else None,
            ua_data_type=self.ua_data_type,
            ua_value_rank=self.ua_value_rank,
        )

    async def read_history(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        num_values: int = 0,
        return_bounds: bool = True,
    ) -> list[TimestampedValue]:
        """Provide requested (default=all) historic values of this variable including their timestamps of the server.

        :param start_time: Start timestamp in UTC for the first data point of the period if specified.
        :param end_time: End timestamp in UTC for the last data point of the period if specified.
        :param num_values: Maximum number of values returned. 0 = No maximum.
        :param return_bounds: Include the bounding values?
        """
        return [
            TimestampedValue(value=entry.Value.Value, timestamp=entry.ServerTimestamp)
            for entry in await self.ua_node.read_raw_history(
                starttime=start_time, endtime=end_time, numvalues=num_values, return_bounds=return_bounds
            )
            if entry.Value is not None
        ]

    async def update_from_data_value(self, value: DataValue | DataChangeNotif | None) -> None:
        """Update both value and timestamp from given data value or subscription callback data."""
        if value is None:
            return
        if isinstance(value, DataChangeNotif):
            value = value.monitored_item.Value

        assert isinstance(value, DataValue)
        assert value.Value is not None

        if self._ua_value == value.Value:
            return

        self._ua_value = value.Value
        self._timestamp = value.ServerTimestamp

        await self.value_changed.send(self, self._ua_value)

    async def write_value(self, value: Any) -> None:
        """Try to directly write the given value to the server.

        Might fail if variable is not writable or user has currently no rights to write the variable.
        """
        if not self.is_writable:
            msg = f"RemoteVariable '{self.name}' is not writable!"
            raise RuntimeError(msg)

        try:
            await write_value(self.ua_node, value)
        except (BadUserAccessDenied, BadOutOfRange):
            raise
        except UaStatusCodeError:
            try:
                index = list(self.valid_values.values()).index(value)
                # enums always start with 0 -> key should be the index...
                # but we are not only dealing with enums
                key = list(self.valid_values.keys())[index]
                await write_value(self.ua_node, key)
            except Exception as err:
                msg = f"{value} is not a valid value (valid are: {self.valid_values}!"
                raise RuntimeError(msg) from err

    @override
    async def ua_on_data_change(self, node: Node, val: Any, data: DataChangeNotif) -> None:
        await self.update_from_data_value(data)

    def __str__(self) -> str:
        """Return string representation of this variable."""
        ret_string = str(self.value)
        if self.unit is not None:
            ret_string += " " + (self.unit.DisplayName.Text or "<None>")
        if self.range is not None:
            ret_string += f" (Low: {self.range.Low}; High: {self.range.High})"

        return ret_string
