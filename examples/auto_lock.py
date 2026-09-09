# SPDX-FileCopyrightText: 2026 The Authors
#
# SPDX-License-Identifier: MIT

import asyncio
import contextlib

from open_smi.client import AutoLock, RemoteServer


async def main() -> None:

    # remember: do not commit the actual password to version control...
    async with RemoteServer("opc.tcp://localhost:4843/", username="orchestrator", password="orchestrator") as server:
        machine = server.machines["DummyMachine"]
        lock = machine.lock
        assert lock is not None

        auto_lock = AutoLock(lock)
        await auto_lock.lock()

        event = asyncio.Event()
        print("Press Ctrl+C to exit!")
        await event.wait()


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
