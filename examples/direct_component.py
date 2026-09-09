# SPDX-FileCopyrightText: 2026 The Authors
#
# SPDX-License-Identifier: MIT

import asyncio

from open_smi_common import LogFormat, setup_logging
from rich.pretty import pprint

from open_smi_client import RemoteComponent, RemoteServer
from open_smi_client.browse import DEFAULT_BROWSE_RULE

setup_logging(log_levels={"sf": "INFO"}, log_format=LogFormat.KEY_VALUE)


async def main() -> None:
    # remember: do not commit the actual password to version control...
    async with RemoteServer(
        "opc.tcp://localhost:4843/",
        username="visitor",
        password="visitor",
        browse_rules=(),  # browse nothing automatically
    ) as server:
        ua_node = server.ua_client.get_node("ns=1;s=DummyMachine.Components.DummyComponent")
        comp = await RemoteComponent(
            name="DummyComponent",
            ua_node=ua_node,
            parent=None,  # pyright: ignore[reportArgumentType]
            server=server,
            browse_rules=(DEFAULT_BROWSE_RULE,),  # browse everything, can be specified as usual
        ).init()

        pprint(comp.dto())


if __name__ == "__main__":
    asyncio.run(main())
