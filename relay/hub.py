"""Routes messages between workers that share a channel.

Delivery is pull-based. Publishing only writes the message to the store and
wakes the connected members; each connection's delivery loop then reads from
the store whatever it has not sent yet. The store is the single source of
truth, so a message published in the middle of a reconnect replay can be
neither skipped nor sent out of order, and a server restart loses nothing that
a publisher was told had been stored.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Any

from relay.store import Store

log = logging.getLogger("relay.hub")

SERVER_SENDER = "@server"       # can never collide with a worker id, see store.NAME_RE
AUTH_CLOSE = 4401               # close code telling a client to stop retrying: its token is no good
REPLACED_CLOSE = 4000           # the same worker connected again from somewhere else
BATCH = 200


class RelayError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(eq=False)
class Session:
    worker_id: str
    ws: Any
    connected_at: float = field(default_factory=time.time)
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    # channel -> highest seq sent on this socket. Lets the delivery loop run
    # ahead of the acknowledged cursor without resending what is in flight.
    sent: dict[str, int] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def send(self, frame: dict[str, Any]) -> None:
        text = json.dumps(frame, separators=(",", ":"), ensure_ascii=False)
        async with self.lock:           # the read loop and the delivery loop both write
            await self.ws.send_text(text)


class Hub:
    def __init__(self, store: Store, *, max_body_bytes: int = 256_000):
        self.store = store
        self.max_body_bytes = max_body_bytes
        self.sessions: dict[str, Session] = {}
        self._watchers: set[asyncio.Queue[dict[str, Any]]] = set()

    def online(self, worker_id: str) -> bool:
        return worker_id in self.sessions

    # --- console event stream ----------------------------------------------------
    def watch(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1000)
        self._watchers.add(queue)
        return queue

    def unwatch(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._watchers.discard(queue)

    def emit(self, kind: str, **data: Any) -> None:
        """Fan an event out to every open console. Call on the event loop thread."""
        event = {"kind": kind, "ts": time.time(), **data}
        for queue in list(self._watchers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # A console this far behind cannot catch up event by event.
                # Swap its backlog for one instruction to reload everything.
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait({"kind": "resync", "ts": time.time()})

    # --- connection lifecycle ------------------------------------------------
    async def serve(self, worker_id: str, ws: Any) -> None:
        old = self.sessions.get(worker_id)
        if old is not None:
            log.warning("%s connected again; closing its previous connection", worker_id)
            with contextlib.suppress(Exception):
                await old.ws.close(code=REPLACED_CLOSE, reason="replaced by a newer connection")
        session = Session(worker_id, ws)
        self.sessions[worker_id] = session
        self.store.touch_worker(worker_id)
        self.emit("worker", worker_id=worker_id, online=True)
        log.info("worker connected: %s", worker_id)

        tasks: list[asyncio.Task[None]] = []
        try:
            await session.send({"type": "welcome", "worker_id": worker_id,
                                "channels": sorted(self.store.channels_of(worker_id))})
            tasks = [asyncio.create_task(self._read_loop(session)),
                     asyncio.create_task(self._delivery_loop(session))]
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if not task.cancelled() and (exc := task.exception()) is not None:
                    log.debug("session %s ended: %r", worker_id, exc)
        except Exception as exc:
            log.debug("session %s ended: %r", worker_id, exc)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # A replaced session finishes after its successor registered; it
            # must not evict the connection that is still live.
            if self.sessions.get(worker_id) is session:
                del self.sessions[worker_id]
                self.store.touch_worker(worker_id)
                self.emit("worker", worker_id=worker_id, online=False)
            log.info("worker disconnected: %s", worker_id)

    async def notify(self, worker_id: str, frame: dict[str, Any]) -> None:
        """Tell a connected worker its memberships changed, and re-run its delivery."""
        session = self.sessions.get(worker_id)
        if session is None:
            return
        with contextlib.suppress(Exception):
            await session.send(frame)
        session.wake.set()

    async def kick(self, worker_id: str, reason: str) -> None:
        session = self.sessions.get(worker_id)
        if session is not None:
            with contextlib.suppress(Exception):
                await session.ws.close(code=AUTH_CLOSE, reason=reason)

    # --- inbound -------------------------------------------------------------
    async def _read_loop(self, s: Session) -> None:
        while True:
            msg = await s.ws.receive()
            if msg["type"] == "websocket.disconnect":
                return
            raw = msg.get("text")
            if raw is None:
                raw = (msg.get("bytes") or b"").decode("utf-8", "replace")
            try:
                frame = json.loads(raw)
                if not isinstance(frame, dict):
                    raise ValueError("a frame must be a JSON object")
            except ValueError as exc:
                await s.send({"type": "error", "code": "bad_frame", "message": str(exc)})
                continue

            kind = frame.get("type")
            if kind == "publish":
                await self._on_publish(s, frame)
            elif kind == "ack":
                channel, seq = frame.get("channel"), frame.get("seq")
                if isinstance(channel, str) and isinstance(seq, int) and not isinstance(seq, bool):
                    self.store.ack(channel, s.worker_id, seq)
            elif kind == "ping":
                await s.send({"type": "pong", "id": frame.get("id")})
            else:
                await s.send({"type": "error", "id": frame.get("id"), "code": "unknown_type",
                              "message": f"unknown frame type {kind!r}"})

    async def _on_publish(self, s: Session, frame: dict[str, Any]) -> None:
        cid = frame.get("id")
        if not isinstance(cid, str) or not cid or len(cid) > 128:
            cid = None
        channel = frame.get("channel")
        try:
            seq, duplicate = self.publish(channel, sender=s.worker_id, body=frame.get("body"), client_id=cid)
        except RelayError as exc:
            await s.send({"type": "error", "id": cid, "code": exc.code, "message": str(exc)})
            return
        await s.send({"type": "published", "id": cid, "channel": channel, "seq": seq, "duplicate": duplicate})

    def publish(self, channel: Any, *, sender: str, body: Any, client_id: str | None = None) -> tuple[int, bool]:
        """Store a message and wake the members who should receive it.

        Touches asyncio events, so call it on the event loop thread: from an
        `async def` route, never from a plain `def` one."""
        if not isinstance(channel, str):
            raise RelayError("bad_frame", "channel must be a string")
        if sender == SERVER_SENDER:
            if not self.store.channel_exists(channel):
                raise RelayError("no_channel", f"channel {channel!r} does not exist")
        elif not self.store.is_member(channel, sender):
            raise RelayError("not_member", f"{sender} is not a member of channel {channel!r}")

        encoded = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        if (size := len(encoded.encode())) > self.max_body_bytes:
            raise RelayError("too_large", f"body is {size} bytes; the limit is {self.max_body_bytes}")
        now = time.time()
        try:
            seq, duplicate = self.store.append(channel, sender, encoded, client_id, ts=now)
        except sqlite3.IntegrityError:
            raise RelayError("no_channel", f"channel {channel!r} does not exist") from None

        if not duplicate:
            for worker_id in self.store.members_of(channel):
                if worker_id != sender and (session := self.sessions.get(worker_id)) is not None:
                    session.wake.set()
            self.emit("message", channel=channel, seq=seq, sender=sender, body=body, ts=now)
        return seq, duplicate

    # --- outbound ------------------------------------------------------------
    async def _delivery_loop(self, s: Session) -> None:
        while True:
            s.wake.clear()              # before reading, so a publish during the read is not missed
            if not await self._deliver(s):
                await s.wake.wait()

    async def _deliver(self, s: Session) -> bool:
        """Send everything this socket has not sent yet. True if a batch filled
        up, meaning there may be more to send right away."""
        cursors = self.store.channels_of(s.worker_id)       # re-read: memberships change live
        for channel in list(s.sent):
            if channel not in cursors:
                del s.sent[channel]
        more = False
        for channel, cursor in cursors.items():
            after = max(cursor, s.sent.get(channel, 0))
            rows = self.store.messages_after(channel, after, limit=BATCH, exclude_sender=s.worker_id)
            for row in rows:
                await s.send({"type": "message", **row})
                s.sent[channel] = row["seq"]
            more = more or len(rows) == BATCH
        return more
