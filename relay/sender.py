"""The server's own delivery loop, for places that cannot come and collect.

A worker holds a request open and is answered. A Telegram chat cannot do that,
so the server goes to it: this loop watches every registered bot, sends what is
waiting, and moves the bot's cursor once it has gone.

It is woken by a publish, the same notification that wakes a waiting worker, so
an alert leaves for a phone about as quickly as it reaches a worker. The timer
is only there for a notification that never arrives.
"""
from __future__ import annotations

import asyncio
import logging

from relay import telegram
from relay.notify import Notifier
from relay.pgstore import PgStore

log = logging.getLogger("relay.sender")


class BotSender:
    def __init__(self, store: PgStore, notifier: Notifier | None = None, *,
                 api: str | None = None, idle: float = 30.0, batch: int = 20):
        self.store = store
        self.notifier = notifier
        self.api = api
        self.idle = idle
        self.batch = batch
        self._task: asyncio.Task[None] | None = None
        self._nudge = asyncio.Event()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run(), name="relay-bot-sender")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except BaseException:                    # noqa: BLE001 - shutting down
                pass
            self._task = None

    def nudge(self) -> None:
        """Something was published; look now rather than at the next tick."""
        self._nudge.set()

    async def run(self) -> None:
        while True:
            try:
                sent = await self.once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:                 # noqa: BLE001 - reported, then retried
                log.warning("bot delivery round failed (%s)", type(exc).__name__)
                sent = 0
            # More to send means keep going; otherwise wait to be woken.
            if sent:
                continue
            self._nudge.clear()
            try:
                await asyncio.wait_for(self._nudge.wait(), timeout=self.idle)
            except (asyncio.TimeoutError, TimeoutError):
                pass

    async def once(self) -> int:
        """One pass over the bots that have something waiting."""
        sent = 0
        for bot in await self.store.bots_with_work(limit=self.batch):
            sent += await self.deliver(bot)
        return sent

    async def deliver(self, bot: dict) -> int:
        scoped = self.store.ws(bot["workspace"])
        waiting = await scoped.messages_after(bot["channel"], int(bot["cursor"]), limit=20)
        sent = 0
        for message in waiting:
            text = telegram.render(message["body"], channel=bot["channel"])
            try:
                await telegram.send(bot["bot_token"], bot["chat_id"], text, api=self.api)
            except telegram.TelegramError as exc:
                if exc.retry_after:
                    log.info("telegram asked to wait %.0fs", exc.retry_after)
                    await asyncio.sleep(min(exc.retry_after, 60))
                    return sent
                if not exc.permanent:
                    log.warning("could not send to %s: %s", bot["name"], exc)
                    return sent
                # About this message or this chat. Skipping it is what keeps
                # one bad message from holding up every later one for ever.
                log.error("dropping message %d for %s: %s", message["seq"], bot["name"], exc)
            # The cursor moves for a message that was sent, and for one that
            # never can be. It never moves for one that might still succeed.
            await self.store.bot_ack(bot["workspace"], bot["channel"], bot["name"],
                                     int(message["seq"]))
            sent += 1
        return sent
