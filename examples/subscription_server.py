# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: CC0-1.0

"""This example connects to the local dummy server, started separately (e.g. run_dummy.py).
Change the URL and credentials to connect to any other Skill node set v3/v4 compatible server.

The example sets up some example subscriptions, waits until Ctrl+C is pressed and then finally disconnects.
"""

import asyncio
import contextlib
import datetime

from opensmi.client import RemoteServer, RemoteServerStatus


async def main() -> None:
    time_update_count = 0

    async def on_status_change(status: RemoteServerStatus) -> None:
        print(f"new status: {status.name}")

    async def on_server_time_update(current_server_time: datetime.datetime) -> None:
        nonlocal time_update_count
        print(f"({time_update_count:03d}) received server time: {current_server_time}")
        time_update_count += 1

    # remember: do not commit the actual password to version control...
    async with RemoteServer("opc.tcp://localhost:4843/", username="visitor", password="visitor") as server:
        server.status_changed.connect(on_status_change)
        server.time_updated.connect(on_server_time_update)

        event = asyncio.Event()
        print("Press Ctrl+C to exit!")
        await event.wait()


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
