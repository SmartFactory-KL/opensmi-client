# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""Mechanism that automatically locks given `RemoteLock` instance."""

import asyncio
import contextlib

import structlog
from asyncua.ua.uaerrors import BadLocked

from opensmi.client.remote_lock import RemoteLock


class AutoLock:
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
        self.logger = structlog.getLogger("opensmi.AutoLock", lock_path=str(self._lock.path))
        self._lock_task: asyncio.Task[None] | None = None
        self._active = False

    async def lock(self, *, blocking: bool = True) -> None:
        """Start automatic locking.

        :param blocking: Whether to block until we own our lock.
        """
        if not self._active:
            self._active = True
            self._lock.locking_user_changed.connect(self.on_locking_user_changed)

        if blocking:
            await self._lock_once()
        else:
            self._schedule_lock()

    async def unlock(self) -> None:
        """Stop automatic locking and release the lock if we hold it."""
        if not self._active:
            return
        self._active = False
        self._lock.locking_user_changed.disconnect(self.on_locking_user_changed)

        if self._lock_task:
            self._lock_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._lock_task
            self._lock_task = None

        if self._lock.locked_by_us:
            try:
                await self._lock.exit_lock()
            except Exception as err:
                self.logger.warning("Could not exit lock", reason=err)

    def _schedule_lock(self) -> None:
        if not self._active:
            return
        if self._lock_task is not None and not self._lock_task.done():
            return
        self._lock_task = asyncio.create_task(self._lock_once(), name="AutoLock")

    async def _lock_once(self) -> None:
        method_name = "break" if self._break_lock else "init"
        while self._active and not self._lock.locked_by_us:
            try:
                self.logger.info(
                    f"Trying to {method_name} the lock... ",
                    locking_user=self._lock.locking_user,
                    locking_client=self._lock.locking_client,
                )
                if self._break_lock:
                    await self._lock.break_lock()
                else:
                    await self._lock.init_lock()
                self.logger.info("Successfully locked!")
                break
            except BadLocked:
                pass
            except Exception:  # unknown problem
                self.logger.exception(f"Cannot {method_name} lock!")
            await asyncio.sleep(self._interval)

    async def on_locking_user_changed(self, _lock: RemoteLock, username: str) -> None:
        """Schedule lock task because the locking user changed."""
        self.logger.debug("Received new locking user", username=username)
        if username != self._lock.server.username:
            self._schedule_lock()

    @property
    def is_active(self) -> bool:
        """Return whether the auto lock is active or not."""
        return self._active
