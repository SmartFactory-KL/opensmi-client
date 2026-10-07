# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: CC0-1.0

import asyncio
import contextlib

from opensmi.client import AutoLock, RemoteServer


async def main() -> None:

    # remember: do not commit the actual password to version control...
    async with RemoteServer("opc.tcp://localhost:4843/", username="orchestrator", password="orchestrator") as server:
        machine = server.machines["DummyMachine"]
        auto_lock = AutoLock(machine.lock)
        await auto_lock.lock()

        event = asyncio.Event()
        print("Press Ctrl+C to exit!")
        await event.wait()


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
