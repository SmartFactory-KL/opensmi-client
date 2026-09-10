# SPDX-FileCopyrightText: 2026 The Authors
#
# SPDX-License-Identifier: MIT

import asyncio

from opensmi.core import LogFormat, setup_logging
from rich.pretty import pprint

from opensmi.client import RemoteServer
from opensmi.client.browse import BrowseFeature, browse_when, match_all, match_name

setup_logging(log_levels={"sf": "INFO"}, log_format=LogFormat.KEY_VALUE)


async def main() -> None:
    browse_rules = [
        browse_when(
            match_all,  # matches everything
            BrowseFeature.MACHINES,
            BrowseFeature.MONITORING,
            # BrowseFeature.IDENTIFICATION,
        ),
        browse_when(
            match_name("DummyMachine"),
            BrowseFeature.COMPONENTS,
            BrowseFeature.SKILL_SET,
        ),
    ]

    # remember: do not commit the actual password to version control...
    async with RemoteServer(
        "opc.tcp://localhost:4843/", username="visitor", password="visitor", browse_rules=browse_rules
    ) as server:
        for machine in server.machines.values():
            dto = machine.dto()
            pprint(dto)

        pprint(browse_rules)


if __name__ == "__main__":
    asyncio.run(main())
