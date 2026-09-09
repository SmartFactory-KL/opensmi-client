# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""Data transfer objects."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any


@dataclass(slots=True, frozen=True, kw_only=True)
class RemoteUaObjectDTO:
    """Data transfer object class for all data transfer objects."""

    name: str
    path: PurePosixPath
    ua_node_id: str
    """OPC UA node ID as string, e.g. ``ns=1;s=DummyMachine``"""
    ua_type: str


@dataclass(slots=True, frozen=True, kw_only=True)
class RemoteUserDTO(RemoteUaObjectDTO):
    """Data transfer object for a `RemoteUser`."""

    allow_multiple: bool
    current_access_level: int
    max_access_level: int
    is_present: bool


@dataclass(slots=True, frozen=True, kw_only=True)
class RangeDTO:
    """Data transfer object for a range."""

    low: int | float
    """Minimum value of the range."""
    high: int | float
    """Maximum value of the range."""


@dataclass(slots=True, frozen=True, kw_only=True)
class RemoteVariableDTO(RemoteUaObjectDTO):
    """Data transfer object for a `RemoteVariable`."""

    value: Any
    """Value of the variable."""
    timestamp: datetime | None
    """Timestamp of the last value change. Exists most of the time."""
    is_writable: bool
    """Whether the variable is writable or read-only."""
    valid_values: dict[int, str] | dict[str, str] | None
    """Optional. Mapping of value (int) / node id (str) -> human-readable name. Restricts ``value``."""
    unit: str | None
    """Optional. Symbol of a unit, e.g. 'm' for meters."""
    range: RangeDTO | None
    """Optional. Low/High. Restricts range of valid values for ``value``."""
    ua_data_type: str
    ua_value_rank: int
    """OPC UA value rank (-1 for Scalar, 0 for "1 or more", >=1 exact dimension)."""


@dataclass(slots=True, frozen=True, kw_only=True)
class RemoteCallableDTO(RemoteUaObjectDTO):
    """Data transfer object class for `RemoteCallable`."""

    min_access_level: int | None
    parameter_set: list[RemoteVariableDTO]
    monitoring: list[RemoteVariableDTO]
    final_result_data: list[RemoteVariableDTO]
    ua_depends_on_node_ids: list[str]
    ua_dependency_of_node_ids: list[str]


@dataclass(slots=True, frozen=True, kw_only=True)
class RemoteSkillDTO(RemoteCallableDTO):
    """Data transfer object for a `RemoteSkill`."""

    current_state: str
    is_suspendable: bool


@dataclass(slots=True, frozen=True, kw_only=True)
class RemoteResourceDTO(RemoteUaObjectDTO):
    """Data transfer object for a `RemoteResource`."""

    attributes: list[RemoteVariableDTO]
    identification: list[RemoteVariableDTO]
    monitoring: list[RemoteVariableDTO]


@dataclass(slots=True, frozen=True, kw_only=True)
class RemoteLockDTO(RemoteUaObjectDTO):
    """Data transfer object for a `RemoteLock`."""

    is_locked: bool
    """Whether the lock is locked or not."""
    locking_user: str
    """Username of the locking user."""
    locking_client: str
    """Client info of the locking user."""
    locked_since: datetime | None
    """Timestamp the lock is locked since. Is ``None`` when unlocked."""


@dataclass(slots=True, frozen=True, kw_only=True)
class RemoteComponentDTO(RemoteUaObjectDTO):
    """Data transfer object for a `RemoteComponent`."""

    current_state: str
    lock: RemoteLockDTO | None

    attributes: list[RemoteVariableDTO]
    identification: list[RemoteVariableDTO]
    monitoring: list[RemoteVariableDTO]
    parameter_set: list[RemoteVariableDTO]

    skill_set: list[RemoteSkillDTO]
    method_set: list[RemoteCallableDTO]
    components: list["RemoteComponentDTO"]
    resources: list[RemoteResourceDTO]


@dataclass(slots=True, frozen=True, kw_only=True)
class RemoteMachineDTO(RemoteComponentDTO):
    """Data transfer object for a `RemoteMachine`."""

    users: list[RemoteUserDTO]
