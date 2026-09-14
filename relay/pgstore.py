"""Relay storage on Postgres, for deployments with no disk of their own.

The same model as `relay.store` - workers, channels, memberships each with a
cursor, one increasing `seq` over the message log - with two differences.

Several processes read and write it at once, because a serverless deployment
runs as many copies of the application as it likes, so nothing is kept in
memory and any instance can serve any request.

And one deployment holds many **workspaces**. A workspace is a relay of its
own: its own channels, workers and messages, reached at its own path and
opened with its own password. `PgStore` owns the workspaces; `store.ws(slug)`
returns the relay inside one, whose methods are the ones `relay.store` has.
Nothing in a workspace can see anything in another, because every statement
carries the slug.

Passwords and tokens are stored as hashes: a copy of the database hands out no
working credentials.
"""
from __future__ import annotations

import asyncio
import json
import secrets
import time
from typing import Any

from psycopg import AsyncConnection, errors
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from relay.store import NAME_RE, _hash, check_name  # one definition of a valid name

SCHEMA = """
CREATE TABLE IF NOT EXISTS workspaces (
    slug          TEXT PRIMARY KEY,          -- what appears in the address
    name          TEXT NOT NULL DEFAULT '',  -- what the person typed
    password_hash TEXT NOT NULL,
    created_at    DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS workers (
    workspace   TEXT NOT NULL REFERENCES workspaces(slug) ON DELETE CASCADE,
    worker_id   TEXT NOT NULL,
    token_hash  TEXT NOT NULL UNIQUE,        -- unique everywhere: a token names its workspace too
    label       TEXT NOT NULL DEFAULT '',
    created_at  DOUBLE PRECISION NOT NULL,
    last_seen   DOUBLE PRECISION,
    PRIMARY KEY (workspace, worker_id)
);

-- A worker that turned up unannounced. It has chosen an id and a token and
-- is waiting for a person to say yes; until then the token opens nothing.
CREATE TABLE IF NOT EXISTS pending_workers (
    workspace    TEXT NOT NULL REFERENCES workspaces(slug) ON DELETE CASCADE,
    worker_id    TEXT NOT NULL,
    token_hash   TEXT NOT NULL,
    label        TEXT NOT NULL DEFAULT '',
    requested_at DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (workspace, worker_id)
);
CREATE INDEX IF NOT EXISTS pending_token ON pending_workers(token_hash);

CREATE TABLE IF NOT EXISTS channels (
    workspace   TEXT NOT NULL REFERENCES workspaces(slug) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (workspace, name)
);

CREATE TABLE IF NOT EXISTS members (
    workspace   TEXT NOT NULL,
    channel     TEXT NOT NULL,
    worker_id   TEXT NOT NULL,
    cursor      BIGINT NOT NULL,
    joined_at   DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (workspace, channel, worker_id),
    FOREIGN KEY (workspace, channel) REFERENCES channels(workspace, name) ON DELETE CASCADE,
    FOREIGN KEY (workspace, worker_id) REFERENCES workers(workspace, worker_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS members_worker ON members(workspace, worker_id);

CREATE TABLE IF NOT EXISTS messages (
    seq         BIGSERIAL PRIMARY KEY,
    workspace   TEXT NOT NULL,
    channel     TEXT NOT NULL,
    sender      TEXT NOT NULL,              -- a worker id, or @server; not a foreign key so history survives
    client_id   TEXT,                       -- the publisher's id for this message, used to drop resends
    body        JSONB NOT NULL,
    ts          DOUBLE PRECISION NOT NULL,
    -- Deleting a channel takes its messages, as it always has.
    FOREIGN KEY (workspace, channel) REFERENCES channels(workspace, name) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS messages_channel_seq ON messages(workspace, channel, seq);
CREATE INDEX IF NOT EXISTS messages_ts ON messages(ts);
CREATE UNIQUE INDEX IF NOT EXISTS messages_dedupe
    ON messages(workspace, sender, client_id) WHERE client_id IS NOT NULL;
"""


__all__ = ["PgStore", "WorkspaceStore", "SCHEMA", "NAME_RE", "check_name", "slugify"]


def slugify(name: str) -> str:
    """The address a workspace lives at, from the name someone typed. Matches
    what the console derives, so both agree on where a workspace is."""
    return "".join(c for c in "".join(name.strip().lower().split())
                   if c.isascii() and (c.isalnum() or c in "_.-"))


def _message(row: dict[str, Any]) -> dict[str, Any]:
    return {"seq": row["seq"], "channel": row["channel"], "sender": row["sender"],
            "body": row["body"], "ts": row["ts"]}


class PgStore:
    """The deployment: a pool, and the workspaces in it.

    Async because every call is a network round trip; a blocking driver would
    stall the event loop for the whole of it.
    """

    def __init__(self, dsn: str, *, min_size: int = 0, max_size: int = 3):
        # Opened on first use, not at startup. An instance that only serves the
        # console should not pay for a connection, and a database that is
        # unreachable should fail the request that needed it with something
        # readable rather than killing the process before it can answer at all.
        self.pool = AsyncConnectionPool(dsn, min_size=min_size, max_size=max_size,
                                        open=False, kwargs={"row_factory": dict_row})
        self._ready = False
        self._opening = asyncio.Lock()

    async def open(self) -> None:
        await self.ready()

    async def ready(self) -> None:
        """Connect and make sure the schema is there, once per process."""
        if self._ready:
            return
        async with self._opening:
            if self._ready:
                return
            await self.pool.open(wait=True, timeout=15)
            await self.setup()
            self._ready = True

    async def close(self) -> None:
        if self.pool.closed:
            return
        await self.pool.close()

    async def setup(self) -> None:
        """Create the schema if it is not there. Two instances starting at once
        both run CREATE TABLE IF NOT EXISTS, and Postgres may raise a duplicate
        object error rather than serialise them, which is not a failure."""
        async with self.pool.connection() as conn:
            try:
                await conn.execute(SCHEMA)
            except (errors.DuplicateTable, errors.DuplicateObject, errors.UniqueViolation):
                pass

    async def _all(self, sql: str, args: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        await self.ready()
        async with self.pool.connection() as conn:
            cur = await conn.execute(sql, args)
            return await cur.fetchall()

    async def _one(self, sql: str, args: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        await self.ready()
        async with self.pool.connection() as conn:
            cur = await conn.execute(sql, args)
            return await cur.fetchone()

    async def _run(self, sql: str, args: tuple[Any, ...] = ()) -> int:
        await self.ready()
        async with self.pool.connection() as conn:
            cur = await conn.execute(sql, args)
            return cur.rowcount

    # --- workspaces ------------------------------------------------------
    def ws(self, slug: str) -> WorkspaceStore:
        """The relay inside one workspace. Does not check that it exists:
        callers authenticate first, which settles that."""
        return WorkspaceStore(self, slug)

    async def create_workspace(self, slug: str, password: str, name: str = "") -> None:
        check_name("workspace", slug)
        if not password:
            raise ValueError("a workspace needs a password")
        try:
            await self._run(
                "INSERT INTO workspaces(slug, name, password_hash, created_at) VALUES (%s, %s, %s, %s)",
                (slug, name or slug, _hash(password), time.time()))
        except errors.UniqueViolation:
            raise ValueError(f"workspace {slug!r} already exists") from None

    async def find_workspace(self, slug: str) -> dict[str, Any] | None:
        """Slug, name and when it was made. Never the password hash."""
        return await self._one(
            "SELECT slug, name, created_at FROM workspaces WHERE slug = %s", (slug,))

    async def workspace_exists(self, slug: str) -> bool:
        return await self._one("SELECT 1 FROM workspaces WHERE slug = %s", (slug,)) is not None

    async def check_password(self, slug: str, password: str) -> bool:
        """Whether this password opens this workspace. Compared as hashes and
        in constant time, so neither the password nor whether the workspace
        exists can be read off the time it takes."""
        row = await self._one("SELECT password_hash FROM workspaces WHERE slug = %s", (slug,))
        # Still compare when there is no such workspace, so a missing one and a
        # wrong password cost the same.
        return secrets.compare_digest(row["password_hash"] if row else "x" * 64, _hash(password)) and row is not None

    async def set_password(self, slug: str, password: str) -> bool:
        if not password:
            raise ValueError("a workspace needs a password")
        return await self._run("UPDATE workspaces SET password_hash = %s WHERE slug = %s",
                               (_hash(password), slug)) > 0

    async def remove_workspace(self, slug: str) -> bool:
        return await self._run("DELETE FROM workspaces WHERE slug = %s", (slug,)) > 0

    async def list_workspaces(self) -> list[dict[str, Any]]:
        """Slugs and names only. Never the password hashes."""
        return await self._all("SELECT slug, name, created_at FROM workspaces ORDER BY slug")

    async def workspace_for_token(self, token: str) -> tuple[str, str] | None:
        """(workspace, worker_id) for a worker token. Tokens are unique across
        the deployment, so a worker's token says which workspace it is in and
        the address never has to be trusted for that."""
        if not token:
            return None
        row = await self._one("SELECT workspace, worker_id FROM workers WHERE token_hash = %s", (_hash(token),))
        return (row["workspace"], row["worker_id"]) if row else None

    async def prune(self, before_ts: float) -> int:
        """Old messages, across every workspace."""
        return await self._run("DELETE FROM messages WHERE ts < %s", (before_ts,))


class WorkspaceStore:
    """One workspace's relay. Every statement carries the slug, so nothing here
    can reach into another workspace even if asked to."""

    def __init__(self, store: PgStore, slug: str):
        self.store = store
        self.slug = slug

    # --- workers ---------------------------------------------------------
    async def add_worker(self, worker_id: str, label: str = "") -> str:
        check_name("worker", worker_id)
        token = secrets.token_urlsafe(32)
        try:
            await self.store._run(
                "INSERT INTO workers(workspace, worker_id, token_hash, label, created_at) "
                "VALUES (%s, %s, %s, %s, %s)",
                (self.slug, worker_id, _hash(token), label, time.time()))
        except errors.UniqueViolation:
            raise ValueError(f"worker {worker_id!r} already exists") from None
        return token

    async def rotate_token(self, worker_id: str) -> str | None:
        token = secrets.token_urlsafe(32)
        changed = await self.store._run(
            "UPDATE workers SET token_hash = %s WHERE workspace = %s AND worker_id = %s",
            (_hash(token), self.slug, worker_id))
        return token if changed else None

    async def remove_worker(self, worker_id: str) -> bool:
        return await self.store._run("DELETE FROM workers WHERE workspace = %s AND worker_id = %s",
                                     (self.slug, worker_id)) > 0

    async def worker_exists(self, worker_id: str) -> bool:
        return await self.store._one("SELECT 1 FROM workers WHERE workspace = %s AND worker_id = %s",
                                     (self.slug, worker_id)) is not None

    async def worker_for_token(self, token: str) -> str | None:
        """The worker this token belongs to, but only if it is in this
        workspace: a token from another one is no token at all here."""
        found = await self.store.workspace_for_token(token)
        return found[1] if found and found[0] == self.slug else None

    async def touch_worker(self, worker_id: str) -> None:
        await self.store._run("UPDATE workers SET last_seen = %s WHERE workspace = %s AND worker_id = %s",
                              (time.time(), self.slug, worker_id))

    async def list_workers(self) -> list[dict[str, Any]]:
        rows = await self.store._all(
            "SELECT worker_id, label, created_at, last_seen FROM workers WHERE workspace = %s ORDER BY worker_id",
            (self.slug,))
        channels: dict[str, list[str]] = {}
        for m in await self.store._all(
                "SELECT worker_id, channel FROM members WHERE workspace = %s ORDER BY channel", (self.slug,)):
            channels.setdefault(m["worker_id"], []).append(m["channel"])
        return [{**r, "channels": channels.get(r["worker_id"], [])} for r in rows]

    # --- enrolment -------------------------------------------------------
    # A worker arrives with no credentials anyone recognises. Rather than a
    # person going to the server to mint a token and carrying it back, the
    # worker proposes an id and a token it made itself, and a person approves
    # it here. Nothing it sent works until they do.
    async def request_worker(self, worker_id: str, token: str, label: str = "") -> str:
        """Ask to join. Returns "pending". Raises if the id is taken by a
        registered worker, which stops a newcomer claiming an existing name."""
        check_name("worker", worker_id)
        if not token:
            raise ValueError("a token is required")
        if await self.worker_exists(worker_id):
            raise ValueError(f"worker {worker_id!r} is already registered")
        # A repeat from the same worker replaces its own request: one that lost
        # its token can ask again rather than being stuck pending for ever.
        await self.store._run("""
            INSERT INTO pending_workers(workspace, worker_id, token_hash, label, requested_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (workspace, worker_id)
            DO UPDATE SET token_hash = EXCLUDED.token_hash,
                          label = EXCLUDED.label,
                          requested_at = EXCLUDED.requested_at""",
            (self.slug, worker_id, _hash(token), label, time.time()))
        return "pending"

    async def is_pending(self, token: str) -> str | None:
        """The worker id this token is waiting as, if it is waiting."""
        if not token:
            return None
        row = await self.store._one(
            "SELECT worker_id FROM pending_workers WHERE workspace = %s AND token_hash = %s",
            (self.slug, _hash(token)))
        return row["worker_id"] if row else None

    async def list_pending(self) -> list[dict[str, Any]]:
        """What is waiting for a person. The fingerprint is the start of the
        token's hash: the worker can print the same thing, so whoever approves
        can check they are approving the machine in front of them."""
        rows = await self.store._all(
            "SELECT worker_id, token_hash, label, requested_at FROM pending_workers "
            "WHERE workspace = %s ORDER BY requested_at", (self.slug,))
        return [{"worker_id": r["worker_id"], "label": r["label"],
                 "requested_at": r["requested_at"], "fingerprint": r["token_hash"][:12]}
                for r in rows]

    async def approve_worker(self, worker_id: str) -> bool:
        """Turn a request into a worker, keeping the token it proposed, so the
        work it was already trying to do simply starts succeeding."""
        row = await self.store._one(
            "SELECT token_hash, label FROM pending_workers WHERE workspace = %s AND worker_id = %s",
            (self.slug, worker_id))
        if row is None:
            return False
        try:
            await self.store._run(
                "INSERT INTO workers(workspace, worker_id, token_hash, label, created_at) "
                "VALUES (%s, %s, %s, %s, %s)",
                (self.slug, worker_id, row["token_hash"], row["label"], time.time()))
        except errors.UniqueViolation:
            # Approved twice, or the id was taken meanwhile. Either way the
            # request is spent.
            await self.reject_worker(worker_id)
            return False
        await self.reject_worker(worker_id)
        return True

    async def reject_worker(self, worker_id: str) -> bool:
        return await self.store._run(
            "DELETE FROM pending_workers WHERE workspace = %s AND worker_id = %s",
            (self.slug, worker_id)) > 0

    # --- channels --------------------------------------------------------
    async def add_channel(self, name: str, description: str = "") -> None:
        check_name("channel", name)
        try:
            await self.store._run(
                "INSERT INTO channels(workspace, name, description, created_at) VALUES (%s, %s, %s, %s)",
                (self.slug, name, description, time.time()))
        except errors.UniqueViolation:
            raise ValueError(f"channel {name!r} already exists") from None

    async def remove_channel(self, name: str) -> bool:
        return await self.store._run("DELETE FROM channels WHERE workspace = %s AND name = %s",
                                     (self.slug, name)) > 0

    async def channel_exists(self, name: str) -> bool:
        return await self.store._one("SELECT 1 FROM channels WHERE workspace = %s AND name = %s",
                                     (self.slug, name)) is not None

    async def list_channels(self) -> list[dict[str, Any]]:
        rows = await self.store._all("""
            SELECT c.name, c.description, c.created_at,
                   (SELECT COUNT(*) FROM messages m WHERE m.workspace = c.workspace AND m.channel = c.name) AS messages,
                   (SELECT MAX(seq) FROM messages m WHERE m.workspace = c.workspace AND m.channel = c.name) AS last_seq
              FROM channels c WHERE c.workspace = %s ORDER BY c.name""", (self.slug,))
        members: dict[str, list[str]] = {}
        for m in await self.store._all(
                "SELECT channel, worker_id FROM members WHERE workspace = %s ORDER BY worker_id", (self.slug,)):
            members.setdefault(m["channel"], []).append(m["worker_id"])
        return [{**r, "members": members.get(r["name"], [])} for r in rows]

    # --- membership ------------------------------------------------------
    async def head(self) -> int:
        """The last seq ever issued, deployment-wide. Read from the sequence
        rather than MAX(seq), so pruning does not rewind it and hand a new
        member messages it should never see."""
        row = await self.store._one(
            "SELECT CASE WHEN is_called THEN last_value ELSE 0 END AS head FROM messages_seq_seq")
        return int(row["head"]) if row else 0

    async def join(self, channel: str, worker_id: str, *, from_start: bool = False) -> bool:
        if not await self.channel_exists(channel):
            raise LookupError(f"no channel {channel!r}")
        if not await self.worker_exists(worker_id):
            raise LookupError(f"no worker {worker_id!r}")
        cursor = 0 if from_start else await self.head()
        return await self.store._run("""
            INSERT INTO members(workspace, channel, worker_id, cursor, joined_at) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (workspace, channel, worker_id) DO NOTHING""",
            (self.slug, channel, worker_id, cursor, time.time())) > 0

    async def leave(self, channel: str, worker_id: str) -> bool:
        return await self.store._run(
            "DELETE FROM members WHERE workspace = %s AND channel = %s AND worker_id = %s",
            (self.slug, channel, worker_id)) > 0

    async def is_member(self, channel: str, worker_id: str) -> bool:
        return await self.store._one(
            "SELECT 1 FROM members WHERE workspace = %s AND channel = %s AND worker_id = %s",
            (self.slug, channel, worker_id)) is not None

    async def members_of(self, channel: str) -> list[str]:
        return [r["worker_id"] for r in await self.store._all(
            "SELECT worker_id FROM members WHERE workspace = %s AND channel = %s ORDER BY worker_id",
            (self.slug, channel))]

    async def channels_of(self, worker_id: str) -> dict[str, int]:
        """channel -> acknowledged cursor, for every channel the worker is in."""
        return {r["channel"]: int(r["cursor"]) for r in await self.store._all(
            "SELECT channel, cursor FROM members WHERE workspace = %s AND worker_id = %s ORDER BY channel",
            (self.slug, worker_id))}

    async def ack(self, channel: str, worker_id: str, seq: int) -> None:
        """Advance a cursor. It never moves backwards, and never past the last
        stored message, so a bad ack cannot make a worker skip future ones."""
        await self.store._run("""
            UPDATE members
               SET cursor = GREATEST(cursor, LEAST(%s, (SELECT COALESCE(MAX(seq), 0) FROM messages)))
             WHERE workspace = %s AND channel = %s AND worker_id = %s""",
            (seq, self.slug, channel, worker_id))

    # --- messages --------------------------------------------------------
    async def append(self, channel: str, sender: str, body: Any, client_id: str | None = None, *,
                     ts: float | None = None) -> tuple[int, bool]:
        """Store a message. Returns (seq, duplicate).

        A publisher that lost its connection before hearing back resends with
        the same client_id; that resend returns the original seq instead of
        storing the message twice."""
        await self.store.ready()
        async with self.store.pool.connection() as conn:
            try:
                cur = await conn.execute(
                    "INSERT INTO messages(workspace, channel, sender, client_id, body, ts) "
                    "VALUES (%s, %s, %s, %s, %s, %s) RETURNING seq",
                    (self.slug, channel, sender, client_id, json.dumps(body),
                     time.time() if ts is None else ts))
                row = await cur.fetchone()
                return int(row["seq"]), False                    # type: ignore[index]
            except errors.UniqueViolation:
                if client_id is None:
                    raise
                # A failed INSERT poisons the transaction; the lookup needs a clean one.
                await conn.rollback()
                cur = await conn.execute(
                    "SELECT seq FROM messages WHERE workspace = %s AND sender = %s AND client_id = %s",
                    (self.slug, sender, client_id))
                row = await cur.fetchone()
                if row is None:
                    raise
                return int(row["seq"]), True

    async def messages_after(self, channel: str, after: int, *, limit: int = 200,
                             exclude_sender: str | None = None) -> list[dict[str, Any]]:
        rows = await self.store._all("""
            SELECT seq, channel, sender, body, ts FROM messages
             WHERE workspace = %s AND channel = %s AND seq > %s AND sender <> %s
             ORDER BY seq LIMIT %s""", (self.slug, channel, after, exclude_sender or "", limit))
        return [_message(r) for r in rows]

    async def messages_before(self, channel: str, before: int, *, limit: int = 100) -> list[dict[str, Any]]:
        """The newest `limit` messages below `before`, oldest first."""
        rows = await self.store._all("""
            SELECT seq, channel, sender, body, ts FROM messages
             WHERE workspace = %s AND channel = %s AND seq < %s
             ORDER BY seq DESC LIMIT %s""", (self.slug, channel, before, limit))
        return [_message(r) for r in reversed(rows)]

    async def waiting_for(self, worker_id: str, *, limit: int = 200) -> list[dict[str, Any]]:
        """Everything above this worker's cursors, across all its channels,
        oldest first. One query, because a poll should be one round trip."""
        rows = await self.store._all("""
            SELECT m.seq, m.channel, m.sender, m.body, m.ts
              FROM messages m
              JOIN members mem
                ON mem.workspace = m.workspace AND mem.channel = m.channel
             WHERE m.workspace = %s AND mem.worker_id = %s
               AND m.seq > mem.cursor AND m.sender <> %s
             ORDER BY m.seq LIMIT %s""", (self.slug, worker_id, worker_id, limit))
        return [_message(r) for r in rows]
