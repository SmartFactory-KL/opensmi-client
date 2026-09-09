# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""Mechanism that automatically locks given `RemoteLock` instance."""

import asyncio

import structlog
from asyncua.ua.uaerrors import BadLocked
from open_smi_common import AsyncTaskMixin

from open_smi_client.remote_lock import RemoteLock


class AutoLock(AsyncTaskMixin):
    """Mechanism that automatically locks given `RemoteLock` instance.

    Will try to lock again after the `RemoteLock` is lost of any reason (i.e. connection lost,
    kicked by user with higher priority).
    """

    def __init__(self, lock: RemoteLock, *, break_lock: bool = False, interval: float = 1.0):
        """Create a new AutoLock instance.

        :param lock: The `RemoteLock` instance to automatically lock.
        :param break_lock: Whether to use `RemoteLock.break_lock()` instead of `RemoteLock.init_lock()`.
        :param interval: How many seconds between two consecutive lock attempts.
        """
        self._lock: RemoteLock = lock
        self._break_lock: bool = break_lock
        self._interval: float = interval
        self.logger = structlog.getLogger("open_smi.AutoLock", lock_path=str(self._lock.path))

        self._lock.locking_user_changed.connect(self.on_locking_user_changed)

    async def lock(self, *, blocking: bool = True) -> None:
        """Block until we own our `RemoteLock`."""
        if blocking:
            await self._lock_once()
        else:
            self._create_task(self._lock_once(), name="AutoLock")

    async def _lock_once(self) -> None:
        method_name = "break" if self._break_lock else "init"
        while not self._lock.locked_by_us:
            try:
                self.logger.info(
                    f"Trying to {method_name} the lock... ",
                    locking_user=self._lock.locking_user,
                    locking_client=self._lock.locking_client,
                )
                if self._break_lock:
                    await self._lock.break_lock()
                    break

                await self._lock.init_lock()
                break
            except BadLocked:
                pass
            except Exception:  # unknown problem
                self.logger.exception(f"Cannot {method_name} lock!")
            await asyncio.sleep(self._interval)

        self.logger.info("Successfully locked!")

    async def on_locking_user_changed(self, _lock: RemoteLock, username: str) -> None:
        """Schedule lock task because the lock changed."""
        self.logger.debug("Received new locking user", username=username)
        if not username or username == self._lock.server.username:
            return
        self._create_task(self._lock_once(), name="AutoLock")
