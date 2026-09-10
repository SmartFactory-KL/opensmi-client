# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""Client implementation of Skill-based Production using OPC-UA."""

from importlib import metadata

from .auto_lock import AutoLock as AutoLock
from .remote_component import BaseRemoteComponent as BaseRemoteComponent
from .remote_component import NotificationEvent as NotificationEvent
from .remote_component import RemoteComponent as RemoteComponent
from .remote_lock import RemoteLock as RemoteLock
from .remote_machine import RemoteMachine as RemoteMachine
from .remote_method import RemoteMethod as RemoteMethod
from .remote_resource import RemoteResource as RemoteResource
from .remote_server import RemoteServer as RemoteServer
from .remote_server import RemoteServerStatus as RemoteServerStatus
from .remote_skill import RemoteSkill as RemoteSkill
from .remote_user import RemoteUser as RemoteUser
from .remote_variable import RemoteVariable as RemoteVariable

__version__ = metadata.version("OpenSMI-Client")
