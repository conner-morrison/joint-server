"""Relay storage on Postgres, for deployments with no disk of their own.

The same model as `relay.store`: workers, channels, memberships each with a
cursor, and one global increasing `seq` over the message log. What differs is
that several processes read and write it at once, because a serverless
deployment runs as many copies of the application as it likes. Nothing here
keeps state in memory, so any instance can serve any request.

Tokens are stored as SHA-256 hashes, as before: a copy of the database hands
out no working credentials.
"""
from __future__ import annotations

import json
import secrets
import time
from typing import Any

from psycopg import AsyncConnection, errors
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from relay.store import NAME_RE, _hash, check_name  # one definition of a valid name

SCHEMA = """
CREATE TABLE IF NOT EXISTS workers (
    worker_id   TEXT PRIMARY KEY,
    token_hash  TEXT NOT NULL UNIQUE,
    label       TEXT NOT NULL DEFAULT '',
    created_at  DOUBLE PRECISION NOT NULL,
    last_seen   DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS channels (
    name        TEXT PRIMARY KEY,
    description TEXT NOT NULL DEFAULT '',
    created_at  DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS members (
    channel     TEXT NOT NULL REFERENCES channels(name) ON DELETE CASCADE,
    worker_id   TEXT NOT NULL REFERENCES workers(worker_id) ON DELETE CASCADE,
    cursor      BIGINT NOT NULL,
    joined_at   DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (channel, worker_id)
);
CREATE INDEX IF NOT EXISTS members_worker ON members(worker_id);

CREATE TABLE IF NOT EXISTS messages (
    seq         BIGSERIAL PRIMARY KEY,
    channel     TEXT NOT NULL REFERENCES channels(name) ON DELETE CASCADE,
    sender      TEXT NOT NULL,              -- a worker id, or @server; not a foreign key so history survives
    client_id   TEXT,                       -- the publisher's id for this message, used to drop resends
    body        JSONB NOT NULL,
    ts          DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_channel_seq ON messages(channel, seq);
CREATE INDEX IF NOT EXISTS messages_ts ON messages(ts);
CREATE UNIQUE INDEX IF NOT EXISTS messages_dedupe ON messages(sender, client_id) WHERE client_id IS NOT NULL;
"""

__all__ = ["PgStore", "SCHEMA", "NAME_RE", "check_name"]


def _message(row: dict[str, Any]) -> dict[str, Any]:
    return {"seq": row["seq"], "channel": row["channel"], "sender": row["sender"],
            "body": row["body"], "ts": row["ts"]}


class PgStore:
    """Async because every call is a network round trip; a blocking driver
    would stall the event loop for the whole of it."""

    def __init__(self, dsn: str, *, min_size: int = 0, max_size: int = 4):
        # Opened lazily: a serverless instance that only serves a static file
        # should not pay for a database connection.
        self.pool = AsyncConnectionPool(dsn, min_size=min_size, max_size=max_size,
                                        open=False, kwargs={"row_factory": dict_row})
        self._ready = False

    async def open(self) -> None:
        await self.pool.open()

    async def close(self) -> None:
        await self.pool.close()

    async def setup(self) -> None:
        """Create the schema if it is not there. Safe to call concurrently:
        two instances starting at once both run CREATE TABLE IF NOT EXISTS,
        and Postgres may raise a duplicate-object error rather than serialise
        them, which is not a failure."""
        async with self.pool.connection() as conn:
            try:
                await conn.execute(SCHEMA)
            except errors.DuplicateTable:
                pass
        self._ready = True

    async def _all(self, sql: str, args: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        async with self.pool.connection() as conn:
            cur = await conn.execute(sql, args)
            return await cur.fetchall()

    async def _one(self, sql: str, args: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        async with self.pool.connection() as conn:
            cur = await conn.execute(sql, args)
            return await cur.fetchone()

    async def _run(self, sql: str, args: tuple[Any, ...] = ()) -> int:
        async with self.pool.connection() as conn:
            cur = await conn.execute(sql, args)
            return cur.rowcount

    # --- workers ---------------------------------------------------------
    async def add_worker(self, worker_id: str, label: str = "") -> str:
        check_name("worker", worker_id)
        token = secrets.token_urlsafe(32)
        try:
            await self._run("INSERT INTO workers(worker_id, token_hash, label, created_at) VALUES (%s, %s, %s, %s)",
                            (worker_id, _hash(token), label, time.time()))
        except errors.UniqueViolation:
            raise ValueError(f"worker {worker_id!r} already exists") from None
        return token

    async def rotate_token(self, worker_id: str) -> str | None:
        token = secrets.token_urlsafe(32)
        changed = await self._run("UPDATE workers SET token_hash = %s WHERE worker_id = %s",
                                  (_hash(token), worker_id))
        return token if changed else None

    async def remove_worker(self, worker_id: str) -> bool:
        return await self._run("DELETE FROM workers WHERE worker_id = %s", (worker_id,)) > 0

    async def worker_exists(self, worker_id: str) -> bool:
        return await self._one("SELECT 1 FROM workers WHERE worker_id = %s", (worker_id,)) is not None

    async def worker_for_token(self, token: str) -> str | None:
        if not token:
            return None
        row = await self._one("SELECT worker_id FROM workers WHERE token_hash = %s", (_hash(token),))
        return row["worker_id"] if row else None

    async def touch_worker(self, worker_id: str) -> None:
        await self._run("UPDATE workers SET last_seen = %s WHERE worker_id = %s", (time.time(), worker_id))

    async def list_workers(self) -> list[dict[str, Any]]:
        rows = await self._all("SELECT worker_id, label, created_at, last_seen FROM workers ORDER BY worker_id")
        channels: dict[str, list[str]] = {}
        for m in await self._all("SELECT worker_id, channel FROM members ORDER BY channel"):
            channels.setdefault(m["worker_id"], []).append(m["channel"])
        return [{**r, "channels": channels.get(r["worker_id"], [])} for r in rows]

    # --- channels --------------------------------------------------------
    async def add_channel(self, name: str, description: str = "") -> None:
        check_name("channel", name)
        try:
            await self._run("INSERT INTO channels(name, description, created_at) VALUES (%s, %s, %s)",
                            (name, description, time.time()))
        except errors.UniqueViolation:
            raise ValueError(f"channel {name!r} already exists") from None

    async def remove_channel(self, name: str) -> bool:
        return await self._run("DELETE FROM channels WHERE name = %s", (name,)) > 0

    async def channel_exists(self, name: str) -> bool:
        return await self._one("SELECT 1 FROM channels WHERE name = %s", (name,)) is not None

    async def list_channels(self) -> list[dict[str, Any]]:
        rows = await self._all("""
            SELECT c.name, c.description, c.created_at,
                   (SELECT COUNT(*) FROM messages m WHERE m.channel = c.name) AS messages,
                   (SELECT MAX(seq) FROM messages m WHERE m.channel = c.name) AS last_seq
            FROM channels c ORDER BY c.name""")
        members: dict[str, list[str]] = {}
        for m in await self._all("SELECT channel, worker_id FROM members ORDER BY worker_id"):
            members.setdefault(m["channel"], []).append(m["worker_id"])
        return [{**r, "members": members.get(r["name"], [])} for r in rows]

    # --- membership ------------------------------------------------------
    async def head(self) -> int:
        """The last seq ever issued. Read from the sequence, not MAX(seq), so
        that pruning every message does not rewind it and hand a new member
        messages it should never have seen."""
        row = await self._one(
            "SELECT CASE WHEN is_called THEN last_value ELSE 0 END AS head FROM messages_seq_seq")
        return int(row["head"]) if row else 0

    async def join(self, channel: str, worker_id: str, *, from_start: bool = False) -> bool:
        if not await self.channel_exists(channel):
            raise LookupError(f"no channel {channel!r}")
        if not await self.worker_exists(worker_id):
            raise LookupError(f"no worker {worker_id!r}")
        cursor = 0 if from_start else await self.head()
        return await self._run("""
            INSERT INTO members(channel, worker_id, cursor, joined_at) VALUES (%s, %s, %s, %s)
            ON CONFLICT (channel, worker_id) DO NOTHING""",
            (channel, worker_id, cursor, time.time())) > 0

    async def leave(self, channel: str, worker_id: str) -> bool:
        return await self._run("DELETE FROM members WHERE channel = %s AND worker_id = %s",
                               (channel, worker_id)) > 0

    async def is_member(self, channel: str, worker_id: str) -> bool:
        return await self._one("SELECT 1 FROM members WHERE channel = %s AND worker_id = %s",
                               (channel, worker_id)) is not None

    async def members_of(self, channel: str) -> list[str]:
        return [r["worker_id"] for r in await self._all(
            "SELECT worker_id FROM members WHERE channel = %s ORDER BY worker_id", (channel,))]

    async def channels_of(self, worker_id: str) -> dict[str, int]:
        """channel -> acknowledged cursor, for every channel the worker is in."""
        return {r["channel"]: int(r["cursor"]) for r in await self._all(
            "SELECT channel, cursor FROM members WHERE worker_id = %s ORDER BY channel", (worker_id,))}

    async def ack(self, channel: str, worker_id: str, seq: int) -> None:
        """Advance a cursor. It never moves backwards, and never past the last
        stored message, so a bad ack cannot make a worker skip future ones."""
        await self._run("""
            UPDATE members
               SET cursor = GREATEST(cursor, LEAST(%s, (SELECT COALESCE(MAX(seq), 0) FROM messages)))
             WHERE channel = %s AND worker_id = %s""", (seq, channel, worker_id))

    # --- messages --------------------------------------------------------
    async def append(self, channel: str, sender: str, body: Any, client_id: str | None = None, *,
                     ts: float | None = None) -> tuple[int, bool]:
        """Store a message. Returns (seq, duplicate).

        A publisher that lost its connection before hearing back resends with
        the same client_id; that resend returns the original seq instead of
        storing the message twice."""
        async with self.pool.connection() as conn:
            try:
                cur = await conn.execute(
                    "INSERT INTO messages(channel, sender, client_id, body, ts) "
                    "VALUES (%s, %s, %s, %s, %s) RETURNING seq",
                    (channel, sender, client_id, json.dumps(body), time.time() if ts is None else ts))
                row = await cur.fetchone()
                return int(row["seq"]), False                    # type: ignore[index]
            except errors.UniqueViolation:
                if client_id is None:
                    raise
                # A failed INSERT poisons the transaction; the lookup needs a clean one.
                await conn.rollback()
                cur = await conn.execute("SELECT seq FROM messages WHERE sender = %s AND client_id = %s",
                                         (sender, client_id))
                row = await cur.fetchone()
                if row is None:
                    raise
                return int(row["seq"]), True

    async def messages_after(self, channel: str, after: int, *, limit: int = 200,
                             exclude_sender: str | None = None) -> list[dict[str, Any]]:
        rows = await self._all("""
            SELECT seq, channel, sender, body, ts FROM messages
             WHERE channel = %s AND seq > %s AND sender <> %s
             ORDER BY seq LIMIT %s""", (channel, after, exclude_sender or "", limit))
        return [_message(r) for r in rows]

    async def messages_before(self, channel: str, before: int, *, limit: int = 100) -> list[dict[str, Any]]:
        """The newest `limit` messages below `before`, oldest first."""
        rows = await self._all("""
            SELECT seq, channel, sender, body, ts FROM messages
             WHERE channel = %s AND seq < %s
             ORDER BY seq DESC LIMIT %s""", (channel, before, limit))
        return [_message(r) for r in reversed(rows)]

    async def prune(self, before_ts: float) -> int:
        return await self._run("DELETE FROM messages WHERE ts < %s", (before_ts,))
