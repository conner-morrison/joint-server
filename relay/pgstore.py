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
import logging
import os
import re
import secrets
import time
from typing import Any

from psycopg import AsyncConnection, errors
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from relay.notify import CHANNEL as NOTIFY_CHANNEL, key as notify_key
from relay.store import NAME_RE, _hash, check_name  # one definition of a valid name

log = logging.getLogger("relay.pgstore")

SCHEMA = """
CREATE TABLE IF NOT EXISTS workspaces (
    slug          TEXT PRIMARY KEY,          -- what appears in the address
    name          TEXT NOT NULL DEFAULT '',  -- what the person typed
    password_hash TEXT NOT NULL,
    created_at    DOUBLE PRECISION NOT NULL
);

-- A worker is known by an id this server gave it. The id is its name to the
-- server and its way in at once, so it is random and long: everything else
-- about a worker can be edited, and none of it is what lets it speak.
--
-- `name` is what a person calls it, and need not be unique - two machines may
-- both reasonably be called "gmail". `token_hash` is only ever set on a worker
-- registered before ids existed, and is how that worker still gets in.
CREATE TABLE IF NOT EXISTS workers (
    workspace   TEXT NOT NULL REFERENCES workspaces(slug) ON DELETE CASCADE,
    worker_id   TEXT NOT NULL,
    name        TEXT NOT NULL DEFAULT '',
    token_hash  TEXT UNIQUE,
    label       TEXT NOT NULL DEFAULT '',
    created_at  DOUBLE PRECISION NOT NULL,
    last_seen   DOUBLE PRECISION,
    PRIMARY KEY (workspace, worker_id)
);

-- A worker asking to join. It has nothing to offer but what it is called:
-- there is no credential for it to invent, and none for a person to carry
-- anywhere. The ticket is the one secret involved, it belongs to the request
-- rather than to the worker, and it is only good for asking what came of it.
-- `granted_id` is the answer, kept until the worker has collected it so that a
-- lost reply costs a retry rather than a registration.
CREATE TABLE IF NOT EXISTS registrations (
    workspace    TEXT NOT NULL REFERENCES workspaces(slug) ON DELETE CASCADE,
    ref          TEXT NOT NULL,
    name         TEXT NOT NULL,
    ticket_hash  TEXT NOT NULL,
    label        TEXT NOT NULL DEFAULT '',
    granted_id   TEXT,
    requested_at DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (workspace, ref)
);
CREATE INDEX IF NOT EXISTS registrations_ticket ON registrations(ticket_hash);

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

-- Somewhere the server itself delivers to. Not a worker: nothing connects, so
-- there is nobody to approve and no token to hand out. A bot is registered
-- once for the workspace and then added to channels, exactly as a worker is.
--
-- Its token is kept as it is, because sending requires it in full. It is a
-- credential belonging to a service, not to a person, and whoever can read
-- this table can already read every message it would forward.
CREATE TABLE IF NOT EXISTS bots (
    workspace   TEXT NOT NULL REFERENCES workspaces(slug) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    kind        TEXT NOT NULL DEFAULT 'telegram',
    chat_id     TEXT NOT NULL,
    bot_token   TEXT NOT NULL,
    label       TEXT NOT NULL DEFAULT '',     -- what Telegram calls the bot
    created_at  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (workspace, name)
);

-- Which channels a bot is in, and how far it has been sent. The same shape as
-- a worker's membership, because it means the same thing.
CREATE TABLE IF NOT EXISTS bot_members (
    workspace   TEXT NOT NULL,
    channel     TEXT NOT NULL,
    bot         TEXT NOT NULL,
    cursor      BIGINT NOT NULL,
    joined_at   DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (workspace, channel, bot),
    FOREIGN KEY (workspace, channel) REFERENCES channels(workspace, name) ON DELETE CASCADE,
    FOREIGN KEY (workspace, bot) REFERENCES bots(workspace, name) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS bot_members_pending ON bot_members(workspace, channel, cursor);

-- One message, asked for again by a person, for one member.
--
-- A cursor is a single mark: moving it back to reach one old message would
-- resend everything after it as well. This says "that one, once more" without
-- disturbing where the member had got to.
CREATE TABLE IF NOT EXISTS redeliveries (
    workspace   TEXT NOT NULL,
    channel     TEXT NOT NULL,
    worker_id   TEXT NOT NULL,
    seq         BIGINT NOT NULL,
    asked_at    DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (workspace, channel, worker_id, seq)
);

-- Jobs to be let through no further. The key is the same id a publisher
-- gives a job, so muting is the dedupe rule pointed at something that has not
-- arrived yet: the job is refused at the door rather than stored, announced
-- and then explained away.
CREATE TABLE IF NOT EXISTS muted (
    workspace  TEXT NOT NULL REFERENCES workspaces(slug) ON DELETE CASCADE,
    key        TEXT NOT NULL,
    url        TEXT NOT NULL DEFAULT '',
    note       TEXT NOT NULL DEFAULT '',
    muted_at   DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (workspace, key)
);

-- Something the server has to tell one worker about itself, rather than about
-- the world: that it has been put into a channel, or taken out of one. It is
-- not a message in a channel, because it is nobody else's business and because
-- a worker taken out of a channel can no longer be told anything through it.
--
-- Tied to the workspace and nothing else on purpose: a notice about leaving a
-- channel has to outlive the membership it describes, and one about a deleted
-- channel has to outlive the channel.
CREATE TABLE IF NOT EXISTS notices (
    id          BIGSERIAL PRIMARY KEY,
    workspace   TEXT NOT NULL REFERENCES workspaces(slug) ON DELETE CASCADE,
    worker_id   TEXT NOT NULL,
    kind        TEXT NOT NULL,              -- joined, or left
    channel     TEXT NOT NULL,
    at          DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS notices_worker ON notices(workspace, worker_id, id);

CREATE TABLE IF NOT EXISTS messages (
    seq         BIGSERIAL PRIMARY KEY,
    workspace   TEXT NOT NULL,
    channel     TEXT NOT NULL,
    sender      TEXT NOT NULL,              -- a worker id, or @server; not a foreign key so history survives
    client_id   TEXT,                       -- the publisher's id for this message, used to drop resends
    job_key     TEXT,                       -- the job this is about, read from its own link
                                            -- (its index is made alongside the column, in a
                                            -- migration: an older table has neither)
    body        JSONB NOT NULL,
    ts          DOUBLE PRECISION NOT NULL,
    -- Deleting a channel takes its messages, as it always has.
    FOREIGN KEY (workspace, channel) REFERENCES channels(workspace, name) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS messages_channel_seq ON messages(workspace, channel, seq);
CREATE INDEX IF NOT EXISTS messages_ts ON messages(ts);
-- One message per id in a workspace, whoever published it. The same job can
-- reach two different workers - a mail parser and a webhook, say - and it is
-- still one job; scoping this to the sender would let each of them store its
-- own copy.
--
-- The cost is that publishers in a workspace share one namespace of ids, so an
-- id must mean the same thing to all of them. A job's own URL does; a counter
-- that starts at one does not.
CREATE UNIQUE INDEX IF NOT EXISTS messages_dedupe_workspace
    ON messages(workspace, client_id) WHERE client_id IS NOT NULL;
"""


__all__ = ["PgStore", "WorkspaceStore", "SCHEMA", "NAME_RE", "check_name", "slugify",
           "ONLINE_WINDOW"]

# How many messages a channel keeps. The oldest go when the newest arrives,
# so a channel is the last hundred things that happened rather than everything
# that ever did. 0 keeps them all.
CHANNEL_MAX = int(os.environ.get("RELAY_CHANNEL_MAX", "100"))

# How recently a worker must have been in touch to count as online.
#
# Nothing stays connected here: a worker holds one request at a time and opens
# the next when it returns, so being online means having been heard from
# lately rather than holding a socket. A worker waiting the full 25 seconds
# touches this at least that often, and this leaves room for two of those to
# be missed before it is called gone.
ONLINE_WINDOW = float(os.environ.get("RELAY_ONLINE_WINDOW", "75"))


# Upwork names a job in its own links: ~021987abc. Two links to one job differ
# in their tracking, never in that. This is the rule a publisher uses to name a
# job, and muting has to agree with it exactly or it would mute nothing.
UPWORK_JOB = re.compile(r"~[0-9a-z]+", re.I)


WORKER_NAME_RE = re.compile(r"^[^\x00-\x1f]{1,64}$")


def check_worker_name(name: str) -> str:
    """What a worker calls itself, tidied. A name is shown to people and is
    nothing else: it does not have to be unique, and it never has to be typed
    into an address, so it may contain whatever reads well."""
    said = " ".join((name or "").split())
    if not said or not WORKER_NAME_RE.match(said):
        raise ValueError("a worker needs a name of 1 to 64 characters")
    return said


def new_worker_id() -> str:
    """The id this server gives a worker when it is registered.

    It identifies the worker and lets it in, both, which is why it is random
    and not counted: a guessable id would be a way into a workspace. Shaped so
    it is safe in an address, because it travels in one.
    """
    return "w-" + secrets.token_hex(16)


UPWORK_JOB_URL = re.compile(r"upwork\.com/jobs/(~[0-9a-z]+)", re.I)
# Where a publisher puts the job's own link. Looked at first, so a job
# mentioned inside a description does not decide what the message is about.
JOB_LINK_KEYS = ("upworkUrl", "upwork_url", "job_url", "jobUrl", "jobLink", "url", "link")


def job_of(body: Any) -> str | None:
    """The job a message is about, from the job's own link.

    The publisher's id is what normally keeps a repeat from being stored, but
    that only works for a publisher that sends one. Reading the link instead
    means two workers watching the same board - or one worker reading the same
    job out of two emails - post a job once between them, whatever either of
    them chose to call it.
    """
    def found(value: Any) -> str | None:
        if isinstance(value, str):
            seen = UPWORK_JOB_URL.search(value)
            return f"job:{seen.group(1).lower()}" if seen else None
        if isinstance(value, dict):
            for item in value.values():
                got = found(item)
                if got:
                    return got
        elif isinstance(value, (list, tuple)):
            for item in value:
                got = found(item)
                if got:
                    return got
        return None

    if isinstance(body, dict):
        for key in JOB_LINK_KEYS:
            got = found(body.get(key))
            if got:
                return got
        for key in ("links", "urls"):
            got = found(body.get(key))
            if got:
                return got
    # Nothing under a name this server knows. A job written as labelled text
    # carries its link in the text, so the whole body is worth one look.
    return found(body if isinstance(body, str) else json.dumps(body, default=str))


def job_key(url_or_key: str) -> str:
    """The id a job is known by, from a link or from an id already formed."""
    said = (url_or_key or "").strip()
    if not said:
        return ""
    if not said.startswith(("http://", "https://")):
        return said                      # already an id
    found = UPWORK_JOB.search(said)
    return f"job:{found.group(0)}" if found else f"job:{said}"


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
        await self._widen_dedupe()
        await self._retire_tokens()
        await self._read_job_links()

    async def _widen_dedupe(self) -> None:
        """Move an existing database from per-sender dedupe to per-workspace.

        `CREATE INDEX IF NOT EXISTS` does nothing when an index of that name
        exists, whatever its definition, so a database made before this change
        keeps the old rule until it is replaced by name. Done in its own
        transaction, and never at the cost of starting: a deployment that
        cannot widen its dedupe should still run.
        """
        try:
            async with self.pool.connection() as conn:
                old = await (await conn.execute(
                    "SELECT 1 FROM pg_indexes WHERE indexname = 'messages_dedupe'")).fetchone()
                if old is None:
                    return
                # Two messages already sharing an id, from different senders,
                # would make the new index impossible. They are history and not
                # ours to delete, so say so and leave the old rule in place.
                clash = await (await conn.execute("""
                    SELECT workspace, client_id, COUNT(*) AS n FROM messages
                     WHERE client_id IS NOT NULL
                     GROUP BY workspace, client_id HAVING COUNT(*) > 1 LIMIT 1""")).fetchone()
                if clash is not None:
                    log.warning(
                        "keeping per-sender dedupe: id %r in workspace %r is already on %d "
                        "messages. Remove or re-id them to dedupe across publishers.",
                        clash["client_id"], clash["workspace"], clash["n"])
                    return
                await conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS messages_dedupe_workspace "
                    "ON messages(workspace, client_id) WHERE client_id IS NOT NULL")
                await conn.execute("DROP INDEX IF EXISTS messages_dedupe")
                log.info("dedupe now covers every publisher in a workspace")
        except Exception as exc:                 # noqa: BLE001 - reported, never fatal
            log.warning("could not widen dedupe (%s); the old rule still applies",
                        type(exc).__name__)

    async def _read_job_links(self) -> None:
        """Give the messages already stored the job key they would get now.

        Without this, a job posted before this existed is not recognised when
        it is posted again, and the first repeat of every job already in a
        channel gets through. Reading them once at startup costs one pass over
        a few hundred rows and saves every one of those.

        Never at the cost of starting.
        """
        # The column and its index first, in their own transaction: a table made
        # before this existed has neither, and nothing below may be allowed to
        # roll back the one thing that stops every write failing.
        try:
            async with self.pool.connection() as conn:
                await conn.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS job_key TEXT")
                await conn.execute("CREATE INDEX IF NOT EXISTS messages_job "
                                   "ON messages(workspace, channel, job_key)")
        except Exception as exc:                 # noqa: BLE001 - reported, never fatal
            log.warning("could not add the job column (%s); jobs will not be deduplicated "
                        "by their link", type(exc).__name__)
            return
        try:
            async with self.pool.connection() as conn:
                missing = await (await conn.execute(
                    "SELECT seq, body FROM messages WHERE job_key IS NULL")).fetchall()
                read = [(job_of(row["body"]), row["seq"]) for row in missing]
                found = [pair for pair in read if pair[0]]
                for key, seq in found:
                    await conn.execute("UPDATE messages SET job_key = %s WHERE seq = %s",
                                       (key, seq))
                if found:
                    log.info("read the job link of %d stored messages", len(found))
        except Exception as exc:                 # noqa: BLE001 - reported, never fatal
            log.warning("could not read job links from stored messages (%s); a job posted "
                        "before now may get through once more", type(exc).__name__)

    async def _retire_tokens(self) -> None:
        """Move an existing database to workers identified by an id.

        A worker used to arrive with an id and a token it chose itself. Now it
        arrives with a name and the server gives it an id. The workers already
        registered keep their token, because the alternative is a pipeline that
        stops at deploy time and three machines to go and edit before it runs
        again; their name becomes the id they were registered under, which is
        what a person had been reading all along.

        Requests still waiting under the old flow cannot be honoured - their
        whole premise was a token the worker brought - so that table goes.

        Never at the cost of starting: a deployment that cannot migrate should
        still serve the workers it has.
        """
        try:
            async with self.pool.connection() as conn:
                await conn.execute(
                    "ALTER TABLE workers ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT ''")
                # Written before ids existed: the id was the name.
                await conn.execute("UPDATE workers SET name = worker_id WHERE name = ''")
                # A worker registered from now on has no token at all.
                await conn.execute("ALTER TABLE workers ALTER COLUMN token_hash DROP NOT NULL")
                await conn.execute("DROP TABLE IF EXISTS pending_workers")
        except Exception as exc:                 # noqa: BLE001 - reported, never fatal
            log.warning("could not move workers onto ids (%s); they keep their tokens",
                        type(exc).__name__)

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

    # --- bot delivery, across every workspace ----------------------------
    async def bots_with_work(self, limit: int = 50) -> list[dict[str, Any]]:
        """Bot memberships that have something waiting. Asked deployment-wide,
        because the sender is one loop for the whole server rather than one
        per workspace."""
        return await self._all("""
            SELECT m.workspace, m.channel, m.bot AS name, m.cursor,
                   b.kind, b.chat_id, b.bot_token
              FROM bot_members m JOIN bots b
                ON b.workspace = m.workspace AND b.name = m.bot
             WHERE EXISTS (SELECT 1 FROM messages g
                            WHERE g.workspace = m.workspace AND g.channel = m.channel
                              AND g.seq > m.cursor)
             ORDER BY m.cursor
             LIMIT %s""", (limit,))

    async def bot_ack(self, workspace: str, channel: str, name: str, seq: int) -> None:
        """Advance a bot's cursor on one channel. Never backwards, so a
        redelivery cannot undo what was already sent."""
        await self._run("""
            UPDATE bot_members SET cursor = GREATEST(cursor, %s)
             WHERE workspace = %s AND channel = %s AND bot = %s""",
            (seq, workspace, channel, name))

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
    async def add_worker(self, name: str, label: str = "") -> dict[str, Any]:
        """Register a worker outright, without waiting to be asked. The id is
        the server's to give, here as everywhere."""
        name = check_worker_name(name)
        worker_id = new_worker_id()
        await self.store._run(
            "INSERT INTO workers(workspace, worker_id, name, label, created_at) "
            "VALUES (%s, %s, %s, %s, %s)", (self.slug, worker_id, name, label, time.time()))
        return {"worker_id": worker_id, "name": name}

    async def rename_worker(self, worker_id: str, name: str) -> str | None:
        """A name is only what people call it, so it can be corrected without
        anything else changing: the id goes on working."""
        name = check_worker_name(name)
        changed = await self.store._run(
            "UPDATE workers SET name = %s WHERE workspace = %s AND worker_id = %s",
            (name, self.slug, worker_id))
        return name if changed else None

    async def reissue_id(self, worker_id: str) -> dict[str, Any] | None:
        """A new id for a worker whose id got out, keeping everything else: the
        same name, the same channels, the same place in each of them.

        Done as a new row the memberships are moved onto, because the id is
        what they are keyed by. The old id stops working the moment this
        returns, which is the point of asking for it.
        """
        row = await self.store._one(
            "SELECT name, label, created_at FROM workers WHERE workspace = %s AND worker_id = %s",
            (self.slug, worker_id))
        if row is None:
            return None
        fresh = new_worker_id()
        async with self.store.pool.connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO workers(workspace, worker_id, name, label, created_at) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    (self.slug, fresh, row["name"], row["label"], row["created_at"]))
                await conn.execute(
                    "UPDATE members SET worker_id = %s WHERE workspace = %s AND worker_id = %s",
                    (fresh, self.slug, worker_id))
                await conn.execute(
                    "UPDATE redeliveries SET worker_id = %s WHERE workspace = %s AND worker_id = %s",
                    (fresh, self.slug, worker_id))
                await conn.execute("DELETE FROM workers WHERE workspace = %s AND worker_id = %s",
                                   (self.slug, worker_id))
        return {"worker_id": fresh, "name": row["name"]}

    async def remove_worker(self, worker_id: str) -> bool:
        return await self.store._run("DELETE FROM workers WHERE workspace = %s AND worker_id = %s",
                                     (self.slug, worker_id)) > 0

    async def worker_exists(self, worker_id: str) -> bool:
        return await self.store._one("SELECT 1 FROM workers WHERE workspace = %s AND worker_id = %s",
                                     (self.slug, worker_id)) is not None

    async def name_of(self, worker_id: str) -> str:
        """What to call a worker when showing it to a person. Falls back to the
        id, which is all there is to say about a worker that has no name."""
        row = await self.store._one(
            "SELECT name FROM workers WHERE workspace = %s AND worker_id = %s",
            (self.slug, worker_id))
        return str(row["name"] or worker_id) if row else worker_id

    async def authenticate(self, presented: str) -> str | None:
        """The worker behind what arrived in the Authorization header.

        Normally the id itself: it is the credential now, so a worker needs
        nothing else to carry. A worker registered before ids existed is found
        by its token instead, so a pipeline that was working did not stop the
        day this changed. Scoped to this workspace, because the address already
        says which workspace is being asked.
        """
        if not presented:
            return None
        row = await self.store._one(
            "SELECT worker_id FROM workers "
            " WHERE workspace = %s AND worker_id = %s AND token_hash IS NULL",
            (self.slug, presented))
        if row is not None:
            return str(row["worker_id"])
        row = await self.store._one(
            "SELECT worker_id FROM workers WHERE workspace = %s AND token_hash = %s",
            (self.slug, _hash(presented)))
        return str(row["worker_id"]) if row else None

    async def touch_worker(self, worker_id: str) -> None:
        await self.store._run("UPDATE workers SET last_seen = %s WHERE workspace = %s AND worker_id = %s",
                              (time.time(), self.slug, worker_id))

    async def list_workers(self) -> list[dict[str, Any]]:
        """Name first: it is what a person reads. The id comes too, because it
        is what the worker was given and what it is identified by here."""
        rows = await self.store._all(
            "SELECT worker_id, name, label, created_at, last_seen, "
            "       (token_hash IS NOT NULL) AS legacy, "
            "       (last_seen IS NOT NULL AND last_seen > %s) AS online "
            "  FROM workers WHERE workspace = %s ORDER BY name, worker_id",
            (time.time() - ONLINE_WINDOW, self.slug))
        channels: dict[str, list[str]] = {}
        for m in await self.store._all(
                "SELECT worker_id, channel FROM members WHERE workspace = %s ORDER BY channel", (self.slug,)):
            channels.setdefault(m["worker_id"], []).append(m["channel"])
        return [{**r, "name": r["name"] or r["worker_id"],
                 "channels": channels.get(r["worker_id"], [])} for r in rows]

    # --- registering -----------------------------------------------------
    # A worker arrives knowing nothing and holding nothing. It says what it is
    # called; a person here says yes; the server gives it an id and that id is
    # how it speaks from then on. Nobody copies a secret from one machine to
    # another at any point, because there is never one to copy.
    async def request_worker(self, name: str, label: str = "") -> dict[str, Any]:
        """Ask to join. Returns the ticket to come back with, which is good for
        one thing: asking what came of this request."""
        name = check_worker_name(name)
        ticket = secrets.token_urlsafe(32)
        ref = secrets.token_hex(8)
        await self.store._run(
            "INSERT INTO registrations(workspace, ref, name, ticket_hash, label, requested_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (self.slug, ref, name, _hash(ticket), label, time.time()))
        # An answered request is kept only so a worker that missed the answer
        # can ask again. After a week it has either collected it or gone.
        await self.store._run(
            "DELETE FROM registrations WHERE workspace = %s AND granted_id IS NOT NULL "
            "  AND requested_at < %s", (self.slug, time.time() - 7 * 86400))
        return {"ref": ref, "ticket": ticket, "name": name}

    async def registration(self, ticket: str) -> dict[str, Any] | None:
        """What came of a request, for the worker that made it."""
        if not ticket:
            return None
        return await self.store._one(
            "SELECT ref, name, label, granted_id, requested_at FROM registrations "
            " WHERE workspace = %s AND ticket_hash = %s", (self.slug, _hash(ticket)))

    async def is_pending(self, ticket: str) -> str | None:
        """The name a ticket is waiting as, if it is still waiting."""
        found = await self.registration(ticket)
        return None if found is None or found["granted_id"] else str(found["name"])

    async def list_pending(self) -> list[dict[str, Any]]:
        """What is waiting for a person. The fingerprint is the start of the
        ticket's hash, which the worker prints too, so whoever approves can
        check they are approving the machine in front of them rather than
        whatever else happened to ask at the same moment."""
        rows = await self.store._all(
            "SELECT ref, name, label, ticket_hash, requested_at FROM registrations "
            " WHERE workspace = %s AND granted_id IS NULL ORDER BY requested_at", (self.slug,))
        return [{"ref": r["ref"], "name": r["name"], "label": r["label"],
                 "requested_at": r["requested_at"], "fingerprint": r["ticket_hash"][:12]}
                for r in rows]

    async def approve_worker(self, ref: str) -> dict[str, Any] | None:
        """Say yes: the worker is registered and given its id. Approving twice
        gives the same id rather than a second worker."""
        row = await self.store._one(
            "SELECT name, label, granted_id FROM registrations WHERE workspace = %s AND ref = %s",
            (self.slug, ref))
        if row is None:
            return None
        if row["granted_id"]:
            return {"worker_id": str(row["granted_id"]), "name": str(row["name"])}
        worker_id = new_worker_id()
        await self.store._run(
            "INSERT INTO workers(workspace, worker_id, name, label, created_at) "
            "VALUES (%s, %s, %s, %s, %s)",
            (self.slug, worker_id, row["name"], row["label"], time.time()))
        await self.store._run(
            "UPDATE registrations SET granted_id = %s WHERE workspace = %s AND ref = %s",
            (worker_id, self.slug, ref))
        return {"worker_id": worker_id, "name": str(row["name"])}

    async def reject_worker(self, ref: str) -> bool:
        return await self.store._run(
            "DELETE FROM registrations WHERE workspace = %s AND ref = %s", (self.slug, ref)) > 0

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
        # Each member as both of its names: the one a person reads, and the id
        # every request about that member is made with.
        members: dict[str, list[dict[str, Any]]] = {}
        for m in await self.store._all("""
                SELECT m.channel, m.worker_id, COALESCE(NULLIF(w.name, ''), m.worker_id) AS name
                  FROM members m LEFT JOIN workers w
                    ON w.workspace = m.workspace AND w.worker_id = m.worker_id
                 WHERE m.workspace = %s ORDER BY name, m.worker_id""", (self.slug,)):
            members.setdefault(m["channel"], []).append(
                {"worker_id": m["worker_id"], "name": m["name"]})
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
        joined = await self.store._run("""
            INSERT INTO members(workspace, channel, worker_id, cursor, joined_at) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (workspace, channel, worker_id) DO NOTHING""",
            (self.slug, channel, worker_id, cursor, time.time())) > 0
        if joined:
            await self.tell_worker(worker_id, "joined", channel)
        return joined

    async def leave(self, channel: str, worker_id: str) -> bool:
        left = await self.store._run(
            "DELETE FROM members WHERE workspace = %s AND channel = %s AND worker_id = %s",
            (self.slug, channel, worker_id)) > 0
        if left:
            await self.tell_worker(worker_id, "left", channel)
        return left

    async def tell_worker(self, worker_id: str, kind: str, channel: str) -> None:
        """Leave a worker something to find the next time it asks.

        A worker is told what it was put into and taken out of because it
        cannot see either: being added to a channel is the moment it may start
        publishing there, and being taken out is the moment its work stops
        reaching anyone. Without this, both look exactly like a quiet day.
        """
        await self.store._run(
            "INSERT INTO notices(workspace, worker_id, kind, channel, at) VALUES (%s, %s, %s, %s, %s)",
            (self.slug, worker_id, kind, channel, time.time()))

    async def notices_for(self, worker_id: str, limit: int = 50) -> list[dict[str, Any]]:
        rows = await self.store._all(
            "SELECT id, kind, channel, at FROM notices "
            " WHERE workspace = %s AND worker_id = %s ORDER BY id LIMIT %s",
            (self.slug, worker_id, limit))
        said = {"joined": "added to", "left": "removed from"}
        return [{"id": r["id"], "kind": r["kind"], "channel": r["channel"], "at": r["at"],
                 "message": f"{said.get(r['kind'], r['kind'])} #{r['channel']}"} for r in rows]

    async def notices_taken(self, ids: list[int]) -> None:
        """Spent once handed over. A notice says something happened, not that
        anything is owed; keeping it until the worker acknowledged would repeat
        it on every poll until then."""
        if not ids:
            return
        await self.store._run("DELETE FROM notices WHERE workspace = %s AND id = ANY(%s)",
                              (self.slug, ids))

    async def is_member(self, channel: str, worker_id: str) -> bool:
        return await self.store._one(
            "SELECT 1 FROM members WHERE workspace = %s AND channel = %s AND worker_id = %s",
            (self.slug, channel, worker_id)) is not None

    async def members_of(self, channel: str) -> list[dict[str, Any]]:
        """Who is in a channel: the id each one is keyed by, and the name to
        show for it. Both, because a person reads the name and every request
        about a member is made with the id."""
        return [{"worker_id": r["worker_id"], "name": r["name"] or r["worker_id"]}
                for r in await self.store._all("""
            SELECT m.worker_id, COALESCE(w.name, '') AS name
              FROM members m LEFT JOIN workers w
                ON w.workspace = m.workspace AND w.worker_id = m.worker_id
             WHERE m.workspace = %s AND m.channel = %s
             ORDER BY w.name, m.worker_id""", (self.slug, channel))]

    async def channels_of(self, worker_id: str) -> dict[str, int]:
        """channel -> acknowledged cursor, for every channel the worker is in."""
        return {r["channel"]: int(r["cursor"]) for r in await self.store._all(
            "SELECT channel, cursor FROM members WHERE workspace = %s AND worker_id = %s ORDER BY channel",
            (self.slug, worker_id))}

    async def skip_to_now(self, channel: str, name: str, *, bot: bool = False) -> int:
        """Give up on what a member never collected and start it from here.

        A backlog is kept because a worker that was away should miss nothing.
        That is the right default and the wrong answer for a worker that was
        away for a week: the news it would receive is no longer news. Returns
        how many were passed over.
        """
        head = await self.head()
        table, column = ("bot_members", "bot") if bot else ("members", "worker_id")
        row = await self.store._one(
            f"SELECT cursor FROM {table} WHERE workspace = %s AND channel = %s AND {column} = %s",
            (self.slug, channel, name))
        if row is None:
            raise LookupError(f"{name!r} is not in channel {channel!r}")
        skipped = await self.store._one(
            "SELECT COUNT(*) AS n FROM messages WHERE workspace = %s AND channel = %s AND seq > %s",
            (self.slug, channel, int(row["cursor"])))
        await self.store._run(
            f"UPDATE {table} SET cursor = GREATEST(cursor, %s) "
            f"WHERE workspace = %s AND channel = %s AND {column} = %s",
            (head, self.slug, channel, name))
        return int(skipped["n"]) if skipped else 0

    async def resend_to(self, channel: str, name: str, *, count: int = 1,
                        bot: bool = False) -> int:
        """Put a member's place back by `count` messages, so it is sent them
        again.

        A cursor never moves backwards on its own: a worker's own
        acknowledgement must not be able to rewind it, or a confused worker
        would read the same message for ever. Moving it back deliberately is a
        different act, and the only way to answer "did that actually arrive".
        Returns how many will be sent again.
        """
        table, column = ("bot_members", "bot") if bot else ("members", "worker_id")
        row = await self.store._one(
            f"SELECT cursor FROM {table} WHERE workspace = %s AND channel = %s AND {column} = %s",
            (self.slug, channel, name))
        if row is None:
            raise LookupError(f"{name!r} is not in channel {channel!r}")
        # The last `count` messages at or below where it had got to. Anything
        # above the cursor is already coming, and does not need resending.
        rows = await self.store._all("""
            SELECT seq FROM messages
             WHERE workspace = %s AND channel = %s AND seq <= %s
             ORDER BY seq DESC LIMIT %s""",
            (self.slug, channel, int(row["cursor"]), max(1, count)))
        if not rows:
            return 0
        back = int(rows[-1]["seq"]) - 1
        await self.store._run(
            f"UPDATE {table} SET cursor = %s "
            f"WHERE workspace = %s AND channel = %s AND {column} = %s",
            (back, self.slug, channel, name))
        return len(rows)

    async def delivery_of(self, channel: str) -> list[dict[str, Any]]:
        """Each member of a channel and how far behind it is.

        Whether something reached a worker is otherwise unanswerable from
        outside: a worker that is not a member receives nothing and says
        nothing, and one that is offline is indistinguishable from one that is
        up to date. A count of what is still waiting separates them.
        """
        workers = await self.store._all("""
            SELECT m.worker_id, COALESCE(NULLIF(w.name, ''), m.worker_id) AS name,
                   m.cursor,
                   (SELECT COUNT(*) FROM messages g
                     WHERE g.workspace = m.workspace AND g.channel = m.channel
                       AND g.seq > m.cursor AND g.sender <> m.worker_id) AS waiting
              FROM members m LEFT JOIN workers w
                ON w.workspace = m.workspace AND w.worker_id = m.worker_id
             WHERE m.workspace = %s AND m.channel = %s
             ORDER BY w.name, m.worker_id""", (self.slug, channel))
        bots = await self.store._all("""
            SELECT b.bot AS worker_id, b.bot AS name, b.cursor,
                   (SELECT COUNT(*) FROM messages g
                     WHERE g.workspace = b.workspace AND g.channel = b.channel
                       AND g.seq > b.cursor) AS waiting
              FROM bot_members b
             WHERE b.workspace = %s AND b.channel = %s
             ORDER BY b.bot""", (self.slug, channel))
        return ([{**r, "kind": "worker"} for r in workers]
                + [{**r, "kind": "bot"} for r in bots])

    async def ack(self, channel: str, worker_id: str, seq: int) -> None:
        """Advance a cursor. It never moves backwards, and never past the last
        stored message, so a bad ack cannot make a worker skip future ones."""
        await self.store._run("""
            UPDATE members
               SET cursor = GREATEST(cursor, LEAST(%s, (SELECT COALESCE(MAX(seq), 0) FROM messages)))
             WHERE workspace = %s AND channel = %s AND worker_id = %s""",
            (seq, self.slug, channel, worker_id))

    # --- bots ------------------------------------------------------------
    # Registered once for the workspace, then added to channels like a worker.
    async def add_bot(self, name: str, chat_id: str, bot_token: str, label: str = "") -> None:
        check_name("bot", name)
        # Pasted values arrive with the spaces and newlines they were copied
        # with. Untrimmed they are different strings, which is how a second
        # registration of one chat slips past the check below.
        chat_id, bot_token = chat_id.strip(), bot_token.strip()
        if not chat_id or not bot_token:
            raise ValueError("a bot needs a chat id and a token")
        # Two registrations sending to one chat is two copies of every alert,
        # and the chat is what decides that - not the token, because a second
        # token for the same bot, or a second bot in the same chat, arrives
        # just as twice. A bot is workspace-wide and joins as many channels as
        # it likes, so there is never a reason for two pointing at one chat.
        twin = await self.store._one(
            "SELECT name FROM bots WHERE workspace = %s AND chat_id = %s",
            (self.slug, chat_id))
        if twin is not None:
            raise ValueError(
                f"bot {twin['name']!r} already sends to that chat. Add it to more "
                "channels instead: a second one here would send everything twice")
        try:
            await self.store._run(
                "INSERT INTO bots(workspace, name, kind, chat_id, bot_token, label, created_at) "
                "VALUES (%s, %s, 'telegram', %s, %s, %s, %s)",
                (self.slug, name, chat_id, bot_token, label, time.time()))
        except errors.UniqueViolation:
            raise ValueError(f"bot {name!r} already exists") from None

    async def list_bots(self) -> list[dict[str, Any]]:
        """Never the token. It is needed to send and nowhere else, so it does
        not travel to a console that only wants to list what is registered."""
        rows = await self.store._all(
            "SELECT name, kind, chat_id, label, created_at FROM bots WHERE workspace = %s "
            "ORDER BY name", (self.slug,))
        channels: dict[str, list[str]] = {}
        for m in await self.store._all(
                "SELECT bot, channel FROM bot_members WHERE workspace = %s ORDER BY channel",
                (self.slug,)):
            channels.setdefault(m["bot"], []).append(m["channel"])
        return [{**r, "channels": channels.get(r["name"], [])} for r in rows]

    async def bot_for_chat(self, chat_id: str) -> str | None:
        """The bot already sending to a chat, if there is one. One chat, one
        bot: a second is two copies of every alert on the same phone."""
        row = await self.store._one("SELECT name FROM bots WHERE workspace = %s AND chat_id = %s",
                                    (self.slug, chat_id.strip()))
        return None if row is None else str(row["name"])

    async def bot_exists(self, name: str) -> bool:
        return await self.store._one("SELECT 1 FROM bots WHERE workspace = %s AND name = %s",
                                     (self.slug, name)) is not None

    async def remove_bot(self, name: str) -> bool:
        return await self.store._run("DELETE FROM bots WHERE workspace = %s AND name = %s",
                                     (self.slug, name)) > 0

    async def join_bot(self, channel: str, bot: str, *, from_start: bool = False) -> bool:
        """Add a bot to a channel. Starts at the head, like any new member:
        joining is not a request for what the channel already carried."""
        if not await self.channel_exists(channel):
            raise LookupError(f"no channel {channel!r}")
        if not await self.bot_exists(bot):
            raise LookupError(f"no bot {bot!r}")
        cursor = 0 if from_start else await self.head()
        return await self.store._run("""
            INSERT INTO bot_members(workspace, channel, bot, cursor, joined_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (workspace, channel, bot) DO NOTHING""",
            (self.slug, channel, bot, cursor, time.time())) > 0

    async def leave_bot(self, channel: str, bot: str) -> bool:
        return await self.store._run(
            "DELETE FROM bot_members WHERE workspace = %s AND channel = %s AND bot = %s",
            (self.slug, channel, bot)) > 0

    async def bots_of(self, channel: str) -> list[dict[str, Any]]:
        return await self.store._all("""
            SELECT b.name, b.chat_id, b.label
              FROM bot_members m JOIN bots b
                ON b.workspace = m.workspace AND b.name = m.bot
             WHERE m.workspace = %s AND m.channel = %s
             ORDER BY b.name""", (self.slug, channel))

    # --- messages --------------------------------------------------------
    # --- muted ------------------------------------------------------------
    async def mute(self, url_or_key: str, note: str = "") -> str:
        key = job_key(url_or_key)
        if not key:
            raise ValueError("give the job's link, or its id")
        await self.store._run("""
            INSERT INTO muted(workspace, key, url, note, muted_at) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (workspace, key) DO UPDATE SET note = EXCLUDED.note""",
            (self.slug, key, url_or_key.strip(), note, time.time()))
        return key

    async def unmute(self, key: str) -> bool:
        return await self.store._run("DELETE FROM muted WHERE workspace = %s AND key = %s",
                                     (self.slug, job_key(key))) > 0

    async def muted(self) -> list[dict[str, Any]]:
        return await self.store._all(
            "SELECT key, url, note, muted_at FROM muted WHERE workspace = %s ORDER BY muted_at DESC",
            (self.slug,))

    async def is_muted(self, client_id: str | None) -> bool:
        if not client_id:
            return False
        return await self.store._one("SELECT 1 FROM muted WHERE workspace = %s AND key = %s",
                                     (self.slug, client_id)) is not None

    async def append(self, channel: str, sender: str, body: Any, client_id: str | None = None, *,
                     ts: float | None = None) -> tuple[int, bool]:
        """Store a message. Returns (seq, duplicate).

        A publisher that lost its connection before hearing back resends with
        the same client_id; that resend returns the original seq instead of
        storing the message twice.

        A job already in this channel is a duplicate too, however it arrived.
        The id a publisher chooses only works for a publisher that chooses one,
        and two workers watching the same board choose differently - so the
        job's own link decides, and the channel keeps the first telling of it.
        """
        await self.store.ready()
        key = job_of(body)
        async with self.store.pool.connection() as conn:
            try:
                if key is not None:
                    # Inside the transaction that would store it, so the answer
                    # cannot be made stale by something committing meanwhile.
                    seen = await (await conn.execute(
                        "SELECT seq FROM messages "
                        " WHERE workspace = %s AND channel = %s AND job_key = %s "
                        " ORDER BY seq LIMIT 1", (self.slug, channel, key))).fetchone()
                    if seen is not None:
                        return int(seen["seq"]), True
                cur = await conn.execute(
                    "INSERT INTO messages(workspace, channel, sender, client_id, job_key, body, ts) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING seq",
                    (self.slug, channel, sender, client_id, key, json.dumps(body),
                     time.time() if ts is None else ts))
                row = await cur.fetchone()
                await self._trim(conn, channel)
                # Announced in the same transaction as the insert, so nobody is
                # ever told about a message that then fails to commit.
                await conn.execute("SELECT pg_notify(%s, %s)",
                                   (NOTIFY_CHANNEL, notify_key(self.slug, channel)))
                return int(row["seq"]), False                    # type: ignore[index]
            except errors.UniqueViolation:
                if client_id is None:
                    raise
                # A failed INSERT poisons the transaction; the lookup needs a clean one.
                await conn.rollback()
                # Found by id alone: the message that claimed it may have come
                # from another publisher, which is the point.
                cur = await conn.execute(
                    "SELECT seq FROM messages WHERE workspace = %s AND client_id = %s",
                    (self.slug, client_id))
                row = await cur.fetchone()
                if row is None:
                    raise
                return int(row["seq"]), True

    async def _trim(self, conn: Any, channel: str) -> None:
        """Drop the oldest so the channel keeps only its newest CHANNEL_MAX.

        In the same transaction as the message that pushed it over, so the
        channel is never briefly longer than it is allowed to be. A member
        that had not reached the dropped messages will not see them: the cap
        is a statement that they are no longer worth delivering, and a cursor
        below them simply has nothing there to find.
        """
        if CHANNEL_MAX <= 0:
            return
        cutoff = await (await conn.execute(
            "SELECT seq FROM messages WHERE workspace = %s AND channel = %s "
            " ORDER BY seq DESC OFFSET %s LIMIT 1",
            (self.slug, channel, CHANNEL_MAX))).fetchone()
        if cutoff is None:
            return
        await conn.execute(
            "DELETE FROM messages WHERE workspace = %s AND channel = %s AND seq <= %s",
            (self.slug, channel, cutoff["seq"]))
        # A request to send one of them again can no longer be honoured.
        await conn.execute(
            "DELETE FROM redeliveries WHERE workspace = %s AND channel = %s AND seq <= %s",
            (self.slug, channel, cutoff["seq"]))

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
        oldest first, plus anything asked for again by hand. One query,
        because a poll should be one round trip."""
        rows = await self.store._all("""
            SELECT m.seq, m.channel, m.sender, m.body, m.ts
              FROM messages m
              JOIN members mem
                ON mem.workspace = m.workspace AND mem.channel = m.channel
             WHERE m.workspace = %s AND mem.worker_id = %s AND m.sender <> %s
               AND (m.seq > mem.cursor
                    OR EXISTS (SELECT 1 FROM redeliveries r
                                WHERE r.workspace = m.workspace AND r.channel = m.channel
                                  AND r.worker_id = mem.worker_id AND r.seq = m.seq))
             ORDER BY m.seq LIMIT %s""", (self.slug, worker_id, worker_id, limit))
        return [_message(r) for r in rows]

    async def bot_member(self, channel: str, bot: str) -> dict[str, Any] | None:
        """One bot's place in a channel, with what is needed to send to it."""
        return await self.store._one("""
            SELECT m.bot AS name, m.channel, b.kind, b.chat_id, b.bot_token
              FROM bot_members m JOIN bots b
                ON b.workspace = m.workspace AND b.name = m.bot
             WHERE m.workspace = %s AND m.channel = %s AND m.bot = %s""",
            (self.slug, channel, bot))

    async def message_at(self, channel: str, seq: int) -> dict[str, Any] | None:
        """One message, by where it sits in its channel."""
        row = await self.store._one(
            "SELECT seq, channel, sender, body, ts FROM messages "
            " WHERE workspace = %s AND channel = %s AND seq = %s", (self.slug, channel, seq))
        return _message(row) if row else None

    async def send_again(self, channel: str, worker_id: str, seq: int) -> bool:
        """Ask for one message to be delivered to one member again."""
        if not await self.is_member(channel, worker_id):
            raise LookupError(f"{worker_id!r} is not in channel {channel!r}")
        if await self.store._one(
                "SELECT 1 FROM messages WHERE workspace = %s AND channel = %s AND seq = %s",
                (self.slug, channel, seq)) is None:
            raise LookupError(f"no message {seq} in channel {channel!r}")
        async with self.store.pool.connection() as conn:
            await conn.execute("""
                INSERT INTO redeliveries(workspace, channel, worker_id, seq, asked_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (workspace, channel, worker_id, seq) DO NOTHING""",
                (self.slug, channel, worker_id, seq, time.time()))
            # Announced like a publish, so a worker holding a poll has it now
            # rather than whenever its wait happens to run out.
            await conn.execute("SELECT pg_notify(%s, %s)",
                               (NOTIFY_CHANNEL, notify_key(self.slug, channel)))
        return True

    async def redelivered(self, worker_id: str, seqs: list[int]) -> None:
        """Spent once handed over. A message asked for by hand is asked for
        again by hand if it needs to be: keeping the row until the worker
        acknowledges would resend it on every poll until then."""
        if not seqs:
            return
        await self.store._run(
            "DELETE FROM redeliveries WHERE workspace = %s AND worker_id = %s AND seq = ANY(%s)",
            (self.slug, worker_id, seqs))
