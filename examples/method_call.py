# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: CC0-1.0

"""This example connects to the local dummy server using the "production" config, started separately (run_dummy.py).

First, it sets the module from the cold start up and then executes a method to show how it works.
"""

import asyncio

from opensmi.client import RemoteServer


async def main() -> None:

    # remember: do not commit the actual password to version control...
    async with RemoteServer("opc.tcp://localhost:4843/", username="operator", password="operator") as server:
        machine = server.machines["DummyMachine"]

        # Step 1: Lock the module
        assert machine.lock is not None
        await machine.lock.init_lock()

        # Step 2: Profit!
        method = machine.method_set["DummyMethod"]
        print("parameters before:", await method.parameter_set.read_all())
        await method.parameter_set.write_all({"a": 5, "b": 42})
        await method.call()
        print("results:", await method.final_result_data.read_all())


if __name__ == "__main__":
    asyncio.run(main())
