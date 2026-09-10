# SPDX-FileCopyrightText: 2026 The Authors
#
# SPDX-License-Identifier: MIT

import asyncio
import json
from dataclasses import asdict
from datetime import datetime
from pathlib import PurePosixPath

from rich.pretty import pprint

from opensmi.client import RemoteServer


def _json_serialize(obj: object) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, PurePosixPath):
        return str(obj)

    msg = f"Object of type {type(obj).__name__} is not JSON serializable"
    raise TypeError(msg)


async def main() -> None:
    # remember: do not commit the actual password to version control...
    async with RemoteServer("opc.tcp://localhost:4843/", username="visitor", password="visitor") as server:
        for machine in server.machines.values():
            dto = machine.dto()
            pprint(dto)
            print(json.dumps(asdict(dto), default=_json_serialize, indent=4))


if __name__ == "__main__":
    asyncio.run(main())
