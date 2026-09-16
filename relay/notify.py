"""Waking a waiting worker the moment something is published.

A worker holding a poll open used to learn about a message by the server
asking the database again a second later. Postgres can say so instead:
`pg_notify` on publish, `LISTEN` here, and the wait ends when there is a reason
for it to end rather than on a timer.

One connection does the listening for the whole process. A connection per
waiting worker would spend the database's connections on doing nothing.
Waiters hold an asyncio event, and a notification sets the events registered
for the channel it names.

It is a hint, not the delivery. A missed notification - the listener
reconnecting, an instance restarting - costs latency and nothing else, because
a waiter still reads the log before it waits and again after. That is why
losing this connection is not worth failing a request over.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Iterable

from psycopg import AsyncConnection

log = logging.getLogger("relay.notify")

CHANNEL = "relay_published"


def key(workspace: str, channel: str) -> str:
    """What a publisher announces and a waiter listens for. Both halves are
    needed: two workspaces may each have a channel called jobs."""
    return f"{workspace}/{channel}"


class Notifier:
    def __init__(self, dsn: str, *, retry: float = 2.0):
        self.dsn = dsn
        self.retry = retry
        self.live = False
        self._waiters: dict[str, set[asyncio.Event]] = {}
        self._task: asyncio.Task[None] | None = None
        self._starting = asyncio.Lock()

    async def ensure(self) -> None:
        """Start listening, once per process, on first use."""
        if self._task is not None and not self._task.done():
            return
        async with self._starting:
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self._listen(), name="relay-notify")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except BaseException:                 # noqa: BLE001 - shutting down
                pass
            self._task = None
        self.live = False

    async def _listen(self) -> None:
        while True:
            try:
                conn = await AsyncConnection.connect(self.dsn, autocommit=True)
                async with conn:
                    await conn.execute(f"LISTEN {CHANNEL}")
                    self.live = True
                    log.info("listening for published messages")
                    async for note in conn.notifies():
                        self._wake(note.payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:              # noqa: BLE001 - reported, then retried
                log.warning("notification listener dropped (%s); retrying", type(exc).__name__)
            finally:
                self.live = False
            await asyncio.sleep(self.retry)

    def _wake(self, payload: str) -> None:
        for event in self._waiters.get(payload, ()):
            event.set()

    async def wait(self, keys: Iterable[str], timeout: float) -> bool:
        """Sleep until one of `keys` is published, or the timeout runs out.
        True when a notification did the waking, which is only ever a hint."""
        if timeout <= 0:
            return False
        await self.ensure()
        event = asyncio.Event()
        watched = list(keys)
        for k in watched:
            self._waiters.setdefault(k, set()).add(event)
        try:
            await asyncio.wait_for(event.wait(), timeout)
            return True
        except (asyncio.TimeoutError, TimeoutError):
            return False
        finally:
            for k in watched:
                holders = self._waiters.get(k)
                if holders is not None:
                    holders.discard(event)
                    if not holders:
                        self._waiters.pop(k, None)
