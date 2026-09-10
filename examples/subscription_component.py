# SPDX-FileCopyrightText: 2026 The Authors
#
# SPDX-License-Identifier: MIT

import asyncio

from opensmi.client import BaseRemoteComponent, NotificationEvent, RemoteServer


async def main():
    # remember: do not commit the actual password to version control...
    async with RemoteServer("opc.tcp://localhost:4843/", username="visitor", password="visitor") as server:
        machine = server.machines["DummyMachine"]

        async def on_new_message(component: BaseRemoteComponent, notification: NotificationEvent) -> None:
            print(f"new message: {component.name=} {notification=}")

        async def on_status_change(component: BaseRemoteComponent, status: str) -> None:
            print(f"new status: {component.name=} '{status}' !")

        # subscribe to events by connecting async functions/methods to signals:
        machine.notification_received.connect(on_new_message)
        machine.status_changed.connect(on_status_change)
        machine.components["DummyComponent"].notification_received.connect(on_new_message)

        await asyncio.sleep(120)


if __name__ == "__main__":
    asyncio.run(main())
