# SPDX-FileCopyrightText: 2026 The Authors
#
# SPDX-License-Identifier: MIT

import asyncio

from asyncua import ua

from open_smi_client import RemoteServer, RemoteVariable


async def main():
    async def on_value_changed(var: RemoteVariable, value: ua.Variant) -> None:
        print(f"new value: {var.path} = {value.Value}")

    # remember: do not commit the actual password to version control...
    async with RemoteServer("opc.tcp://localhost:4843/", username="visitor", password="visitor") as server:
        machine = server.machines["DummyMachine"]

        for var in machine.components["DummyComponent"].monitoring:
            var.value_changed.connect(on_value_changed)

        await asyncio.sleep(120)


if __name__ == "__main__":
    asyncio.run(main())
