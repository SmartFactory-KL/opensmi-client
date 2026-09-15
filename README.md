# OpenSMI Client

**OpenSMI Client** is the client half of the OpenSMI framework: a Python library for connecting to **any** SMI-compliant
OPC UA server — generically discovering its machines, components and skills, and
interacting with them without needing a generated, server-specific client.

Since SMI defines a standardized information model, **OpenSMI Client** doesn't need to know your server's specific
machine/skill types ahead of time — it browses the address space at runtime and exposes what it finds through a
consistent Python API.

Browsing is optional, though: if you already know the relevant node IDs (e.g. from the server's configuration, or cached
from a previous run), you can point the client directly at them and skip discovery entirely, or mix both approaches —
browse once to locate a machine, then operate on it and its sub-objects using known nodes from there on.

## Requirements

- Python 3.11+
- An SMI-compliant OPC UA server to connect to (e.g. one built with **OpenSMI-Server**)

## Installation

```bash
pip install opensmi-client
```

## Quick Start

```python
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
            await skill.reset(wait_for=SkillState.READY)

        # bulk write the parameters
        await skill.parameter_set.write_all({"x": 5, "y": 10})

        # start the skill and wait for completion
        await skill.start(wait_for=SkillState.COMPLETED)

        # retrieve the results before resetting
        print("results:", await skill.final_result_data.read_all())

        await skill.reset(wait_for=SkillState.READY)


if __name__ == "__main__":
    asyncio.run(main())
```

This connects to the `ExampleMachine` from the server quick start, locks it, drives its
`ExampleSkill` through a full parameter-write → start → completion → read-result → reset cycle, and prints the
computed sum.

## Project Status

The client API is stable and follows [semantic versioning](https://semver.org/). Changes to the underlying OPC UA
information model are incorporated as needed but kept backwards compatible, and do not change the Python API —
code written against **OpenSMI Client** should keep working across information-model revisions.

## License

[MIT](LICENSES/MIT.txt)

---

*This text was drafted with AI assistance (Claude, Anthropic) and reviewed/edited by the author before
publication.*
