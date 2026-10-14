# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""Common helpers for the client."""

import asyncio
from collections.abc import Awaitable, Iterable


async def gather(
    tasks: Iterable[Awaitable],
) -> list[Exception]:
    """Run given ``tasks`` and return all failures.

    ``ConnectionError``, ``TimeoutError`` and any ``BaseException`` will be raised.
    """
    results = await asyncio.gather(*tasks, return_exceptions=True)
    failures: list[Exception] = []
    for result in results:
        if isinstance(result, (ConnectionError, TimeoutError)):
            raise result
        if isinstance(result, Exception):
            failures.append(result)
        elif isinstance(result, BaseException):  # CancelledError, KeyboardInterrupt, SystemExit
            raise result

    return failures
