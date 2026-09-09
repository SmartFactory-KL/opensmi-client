# SPDX-FileCopyrightText: 2026 The Authors
#
# SPDX-License-Identifier: MIT

import asyncio

from rich import print

from open_smi_client import RemoteServer, RemoteVariable


async def main() -> None:
    # remember: do not commit the actual password to version control...
    async with RemoteServer("opc.tcp://localhost:4843/", username="visitor", password="visitor") as server:
        machine = server.machines["DummyMachine"]

        variable: RemoteVariable = machine.components["DummyComponent"].monitoring["RandomValue"]
        print(variable)

        history = await variable.read_history(num_values=10)  # limit to the last 10
        for entry in history:
            print(entry)


if __name__ == "__main__":
    asyncio.run(main())
