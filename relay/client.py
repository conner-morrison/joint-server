"""Worker-side client. Needs only `websockets`.

    client = RelayClient("https://relay.example.com", token)

    @client.on("jobs")
    async def on_job(msg):
        ...
        await client.publish("results", {"job": msg.body["id"], "ok": True})

    await client.run()

Handlers run one at a time, in order, and a message is acknowledged once its
handler returns, so a worker that dies mid-message receives it again when it
next connects. A handler that raises is logged and acknowledged anyway: one
bad message must not wedge the channel for ever.

Publishing works while disconnected. The message waits in memory, is sent on
reconnect, and the server drops a resend it has already stored.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

log = logging.getLogger("relay.client")

AUTH_CLOSE = 4401


@dataclass(frozen=True)
class Message:
    channel: str
    seq: int
    sender: str             # a worker id, or "@server"
    body: Any
    ts: float


Handler = Callable[[Message], Awaitable[None]]


class AuthError(RuntimeError):
    """The server rejected the token. Reconnecting will not help."""


class PublishError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


class RelayClient:
    def __init__(self, url: str, token: str, *, outbox_max: int = 1000, max_backoff: float = 30.0):
        base = url.rstrip("/")
        if base.startswith("http"):
            base = "ws" + base[len("http"):]            # http -> ws, https -> wss
        self.url = base if base.endswith("/ws") else base + "/ws"
        self.token = token
        self.outbox_max = outbox_max
        self.max_backoff = max_backoff
        self.worker_id = ""
        self.channels: set[str] = set()
        self.connected = asyncio.Event()
        self._handlers: dict[str, Handler] = {}
        self._fallback: Handler | None = None
        self._outbox: dict[str, str] = {}               # client id -> publish frame, until the server confirms
        self._waiters: dict[str, asyncio.Future[int]] = {}
        self._acks: dict[str, int] = {}                 # channel -> seq not yet sent to the server
        self._handled: dict[str, int] = {}              # channel -> highest seq handled by this process
        self._inbox: asyncio.Queue[Message] = asyncio.Queue()
        self._ws: Any = None
        self._stopped = asyncio.Event()

    # --- api -----------------------------------------------------------------
    def on(self, channel: str | None = None) -> Callable[[Handler], Handler]:
        """Register a handler for one channel, or with no argument for every
        channel that has no handler of its own."""
        def register(fn: Handler) -> Handler:
            if channel is None:
                self._fallback = fn
            else:
                self._handlers[channel] = fn
            return fn
        return register

    async def publish(self, channel: str, body: Any, *, timeout: float | None = None) -> int:
        """Post to a channel and return the seq the server stored it under.

        Waits through disconnects. If `timeout` expires the outcome is unknown
        (the server may have stored it) and the message is not resent."""
        if len(self._outbox) >= self.outbox_max:
            raise PublishError("outbox_full", f"{len(self._outbox)} messages are still waiting for the server")
        cid = uuid.uuid4().hex
        text = json.dumps({"type": "publish", "id": cid, "channel": channel, "body": body})
        fut: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        self._outbox[cid] = text
        self._waiters[cid] = fut
        await self._send(text)
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            self._outbox.pop(cid, None)
            self._waiters.pop(cid, None)

    async def run(self) -> None:
        """Connect and keep reconnecting until close(), or until the token is rejected."""
        import websockets
        from websockets.exceptions import ConnectionClosed

        handling = asyncio.create_task(self._handle_loop(), name="relay-handlers")
        backoff = 1.0
        try:
            while not self._stopped.is_set():
                try:
                    async with websockets.connect(
                        self.url, additional_headers={"Authorization": f"Bearer {self.token}"},
                        ping_interval=20, ping_timeout=20,
                    ) as ws:
                        self._ws = ws
                        backoff = 1.0
                        with contextlib.suppress(ConnectionClosed):
                            async for raw in ws:
                                await self._dispatch(raw)
                        if ws.close_code == AUTH_CLOSE:
                            raise AuthError(ws.close_reason or "token rejected")
                        if not self._stopped.is_set():
                            log.warning("relay closed the connection (%s %s)", ws.close_code, ws.close_reason)
                except (AuthError, asyncio.CancelledError):
                    raise
                except Exception as exc:
                    log.warning("relay unreachable (%s); retrying in %.0fs", exc, backoff)
                finally:
                    self._ws = None
                    self.connected.clear()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stopped.wait(), backoff)
                backoff = min(self.max_backoff, backoff * 2)
        finally:
            handling.cancel()
            await asyncio.gather(handling, return_exceptions=True)

    async def close(self) -> None:
        self._stopped.set()
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()

    # --- io ------------------------------------------------------------------
    async def _send(self, text: str) -> bool:
        ws = self._ws
        if ws is None:
            return False
        try:
            await ws.send(text)
            return True
        except Exception:
            return False

    async def _flush(self) -> None:
        for text in list(self._outbox.values()):
            if not await self._send(text):
                return
        await self._flush_acks()

    async def _flush_acks(self) -> None:
        for channel, seq in list(self._acks.items()):
            if not await self._send(json.dumps({"type": "ack", "channel": channel, "seq": seq})):
                return
            if self._acks.get(channel) == seq:
                del self._acks[channel]

    def _settle(self, cid: Any, *, result: int = 0, error: Exception | None = None) -> bool:
        if not isinstance(cid, str):
            return False
        self._outbox.pop(cid, None)
        fut = self._waiters.pop(cid, None)
        if fut is None:
            return False
        if not fut.done():
            if error is not None:
                fut.set_exception(error)
            else:
                fut.set_result(result)
        return True

    async def _dispatch(self, raw: str | bytes) -> None:
        try:
            frame = json.loads(raw)
        except ValueError:
            log.warning("undecodable frame from the relay")
            return
        kind = frame.get("type")
        if kind == "message":
            # Handlers run in their own task. If they ran here, a handler that
            # publishes would wait for a confirmation this loop could never read.
            self._inbox.put_nowait(Message(frame["channel"], frame["seq"], frame["sender"],
                                           frame.get("body"), frame.get("ts", 0.0)))
        elif kind == "published":
            self._settle(frame.get("id"), result=frame["seq"])
        elif kind == "error":
            err = PublishError(frame.get("code", "error"), frame.get("message", ""))
            if not self._settle(frame.get("id"), error=err):
                log.warning("relay error: %s", err)
        elif kind == "welcome":
            self.worker_id = frame.get("worker_id", "")
            self.channels = set(frame.get("channels", []))
            log.info("connected to %s as %s; channels: %s", self.url, self.worker_id,
                     ", ".join(sorted(self.channels)) or "none")
            await self._flush()
            self.connected.set()
        elif kind == "joined":
            self.channels.add(frame["channel"])
            log.info("joined channel %s", frame["channel"])
        elif kind == "left":
            self.channels.discard(frame["channel"])
            log.info("left channel %s", frame["channel"])

    async def _handle_loop(self) -> None:
        while True:
            msg = await self._inbox.get()
            # After a reconnect the server resends everything unacknowledged,
            # which can include messages this process already handled.
            if msg.seq > self._handled.get(msg.channel, 0):
                handler = self._handlers.get(msg.channel, self._fallback)
                if handler is None:
                    log.warning("no handler for channel %r; message %d dropped", msg.channel, msg.seq)
                else:
                    try:
                        await handler(msg)
                    except Exception:
                        log.exception("handler for %r failed on message %d; acknowledging it anyway",
                                      msg.channel, msg.seq)
                self._handled[msg.channel] = msg.seq
            self._acks[msg.channel] = max(msg.seq, self._acks.get(msg.channel, 0))
            await self._flush_acks()
