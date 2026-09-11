# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""Connect to a separately started local quickstart server and call a skill."""

import asyncio

from opensmi.core import SkillState

from opensmi.client import RemoteServer


async def main() -> None:

    # remember: do not commit the actual password to version control...
    async with RemoteServer("opc.tcp://localhost:4841/", username="operator", password="operator") as server:
        machine = server.machines["ExampleMachine"]

        await machine.lock.init_lock()

        skill = machine.skill_set["ExampleSkill"]
        # reset the Skill if necessary
        if await skill.read_current_state() != SkillState.READY:
            await skill.reset()
            await skill.wait_for_state(SkillState.READY)

        # bulk write the parameters
        await skill.parameter_set.write_all({"x": 5, "y": 10})

        # start the skill and wait for completion
        await skill.start()
        await skill.wait_for_state(SkillState.COMPLETED)

        # retrieve the results before resetting
        print("results:", await skill.final_result_data.read_all())

        await skill.reset()
        await skill.wait_for_state(SkillState.READY)


if __name__ == "__main__":
    asyncio.run(main())
