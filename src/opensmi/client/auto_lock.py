# SPDX-FileCopyrightText: 2026 OpenSMI Contributors
#
# SPDX-License-Identifier: MIT

"""Mechanism that automatically locks given `RemoteLock` instance."""

import asyncio
import contextlib

import structlog

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
        self._renew_task: asyncio.Task[None] | None = None
        self._active = False

    async def lock(self, *, blocking: bool = True) -> None:
        """Start automatic locking.

        :param blocking: Whether to block until we own our lock.
        """
        if not self._active:
            self._active = True
            self._lock.locking_user_changed.connect(self.on_locking_user_changed)
            self._lock.renew_due.connect(self.on_renew_due)

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
        self._lock.renew_due.disconnect(self.on_renew_due)

        # cancel tasks
        for task in (self._lock_task, self._renew_task):
            if not task:
                continue
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._lock_task = None
        self._renew_task = None

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
        self._lock_task = asyncio.create_task(self._lock_once(), name="AutoLock_Lock")

    async def _lock_once(self) -> None:
        method_name = "break" if self._break_lock else "init"
        while self._active and not self._lock.locked_by_us:
            try:
                if self._break_lock:
                    await self._lock.break_lock()
                else:
                    await self._lock.init_lock()
                self.logger.info("Successfully locked!")
                break
            except Exception as err:
                self.logger.warning(
                    f"Could not {method_name} lock!",
                    reason=err,
                    locking_user=self._lock.locking_user,
                    locking_client=self._lock.locking_client,
                )
            await asyncio.sleep(self._interval)

    async def on_locking_user_changed(self, _lock: RemoteLock, username: str) -> None:
        """Schedule lock task because the locking user changed."""
        self.logger.debug("Received new locking user", username=username)
        if username != self._lock.server.username:
            self._schedule_lock()

    async def _renew(self) -> None:
        try:
            async with asyncio.timeout(5):
                await self._lock.renew_lock()
        except Exception as err:
            self.logger.warning("Could not renew lock", reason=err)

    async def on_renew_due(self, _lock: RemoteLock, remaining_ms: float) -> None:
        """Schedule lock renew task because we don't want to lose the lock."""
        if not self._active or (self._renew_task and not self._renew_task.done()):
            return
        self.logger.debug("Lock renewal requested", remaining_ms=remaining_ms)
        self._renew_task = asyncio.create_task(self._renew(), name="AutoLock_Renew")

    @property
    def is_active(self) -> bool:
        """Return whether the auto lock is active or not."""
        return self._active
