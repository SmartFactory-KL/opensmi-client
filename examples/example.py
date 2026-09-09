# SPDX-FileCopyrightText: 2026 The Authors
#
# SPDX-License-Identifier: MIT

"""This example connects to the local dummy server using the "production" config, started separately (run_dummy.py).

First, it sets the module from the cold start up and then executes a skill to show how it works.
"""

import asyncio

from open_smi_common import SkillState

from open_smi_client import RemoteServer


async def main():

    # remember: do not commit the actual password to version control...
    async with RemoteServer("opc.tcp://localhost:4843/", username="operator", password="operator") as server:
        machine = server.machines["DummyMachine"]

        for skill_name, skill in machine.skill_set.items():  # iterate through available skills as demonstration
            print(f"Skill '{skill_name}' is in state '{skill.current_state.name}'")

        # Step 1: Lock the module
        assert machine.lock is not None
        await machine.lock.init_lock()

        try:
            # Step 2: Reset & then start the StartupSkill
            startup_skill = machine.skill_set["StartupSkill"]

            await startup_skill.reset()
            await startup_skill.wait_for_state(SkillState.READY)
            await startup_skill.start()
            await startup_skill.wait_for_state(SkillState.RUNNING)
        except:
            pass  # TODO check if the module is actually ready

        # Step ?: Module should be ready now

        # Step 5: Profit!
        skill = machine.skill_set["DummySkillWithoutGateRequirement"]
        print("parameters before:", await skill.parameter_set.read_all())
        await skill.parameter_set.write_all({"x": 5, "y": 42})
        await skill.start()
        await skill.wait_for_state(SkillState.COMPLETED)
        print("results:", await skill.final_result_data.read_all())
        await skill.reset()  # reset after getting the results!
        await skill.wait_for_state(SkillState.READY)

        try:
            await skill.wait_for_state(SkillState.RUNNING, 1)  # will not happen :)
        except asyncio.exceptions.TimeoutError:
            print("As expected, the skill didn't start on its own...")


if __name__ == "__main__":
    asyncio.run(main())
