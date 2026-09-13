"""Relay storage: workers, channels, memberships, and the message log.

SQLite with WAL. Every message gets a global, increasing `seq`. Each membership
keeps a `cursor`: the highest seq that worker has acknowledged on that channel.
Redelivery after a disconnect is then just "everything above the cursor".

Tokens are stored as SHA-256 hashes, so a copy of the database does not hand
out working credentials.
"""
from __future__ import annotations

import hashlib
import json
import re
import secrets
import sqlite3
import threading
import time
from typing import Any

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS workers (
    worker_id   TEXT PRIMARY KEY,
    token_hash  TEXT NOT NULL UNIQUE,
    label       TEXT NOT NULL DEFAULT '',
    created_at  REAL NOT NULL,
    last_seen   REAL
);

CREATE TABLE IF NOT EXISTS channels (
    name        TEXT PRIMARY KEY,
    description TEXT NOT NULL DEFAULT '',
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS members (
    channel     TEXT NOT NULL REFERENCES channels(name) ON DELETE CASCADE,
    worker_id   TEXT NOT NULL REFERENCES workers(worker_id) ON DELETE CASCADE,
    cursor      INTEGER NOT NULL,
    joined_at   REAL NOT NULL,
    PRIMARY KEY (channel, worker_id)
);
CREATE INDEX IF NOT EXISTS members_worker ON members(worker_id);

CREATE TABLE IF NOT EXISTS messages (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    channel     TEXT NOT NULL REFERENCES channels(name) ON DELETE CASCADE,
    sender      TEXT NOT NULL,              -- a worker id, or @server; not a foreign key so history survives
    client_id   TEXT,                       -- the publisher's id for this message, used to drop resends
    body        TEXT NOT NULL,              -- JSON
    ts          REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_channel_seq ON messages(channel, seq);
CREATE INDEX IF NOT EXISTS messages_ts ON messages(ts);
CREATE UNIQUE INDEX IF NOT EXISTS messages_dedupe ON messages(sender, client_id) WHERE client_id IS NOT NULL;
"""

# Must start with a letter or digit, which keeps "@server" free for the server itself.
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


def check_name(kind: str, name: str) -> str:
    if not isinstance(name, str) or not NAME_RE.match(name):
        raise ValueError(f"invalid {kind} name {name!r}: use 1-64 letters, digits, '_', '.' or '-', "
                         "starting with a letter or digit")
    return name


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _message(row: sqlite3.Row) -> dict[str, Any]:
    return {"seq": row["seq"], "channel": row["channel"], "sender": row["sender"],
            "body": json.loads(row["body"]), "ts": row["ts"]}


class Store:
    def __init__(self, path: str = "relay.db"):
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _run(self, sql: str, args: tuple[Any, ...] = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._conn.execute(sql, args)

    def _one(self, sql: str, args: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(sql, args).fetchone()

    def _all(self, sql: str, args: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, args).fetchall()

    # --- workers ---------------------------------------------------------
    def add_worker(self, worker_id: str, label: str = "") -> str:
        """Register a worker and return its token. Only the hash is kept, so
        this is the one time the token can be read."""
        check_name("worker", worker_id)
        token = secrets.token_urlsafe(32)
        try:
            self._run("INSERT INTO workers(worker_id, token_hash, label, created_at) VALUES (?, ?, ?, ?)",
                      (worker_id, _hash(token), label, time.time()))
        except sqlite3.IntegrityError:
            raise ValueError(f"worker {worker_id!r} already exists") from None
        return token

    def rotate_token(self, worker_id: str) -> str | None:
        token = secrets.token_urlsafe(32)
        cur = self._run("UPDATE workers SET token_hash = ? WHERE worker_id = ?", (_hash(token), worker_id))
        return token if cur.rowcount else None

    def remove_worker(self, worker_id: str) -> bool:
        return self._run("DELETE FROM workers WHERE worker_id = ?", (worker_id,)).rowcount > 0

    def worker_exists(self, worker_id: str) -> bool:
        return self._one("SELECT 1 FROM workers WHERE worker_id = ?", (worker_id,)) is not None

    def worker_for_token(self, token: str) -> str | None:
        row = self._one("SELECT worker_id FROM workers WHERE token_hash = ?", (_hash(token),))
        return row["worker_id"] if row else None

    def touch_worker(self, worker_id: str) -> None:
        self._run("UPDATE workers SET last_seen = ? WHERE worker_id = ?", (time.time(), worker_id))

    def list_workers(self) -> list[dict[str, Any]]:
        rows = self._all("SELECT worker_id, label, created_at, last_seen FROM workers ORDER BY worker_id")
        channels: dict[str, list[str]] = {}
        for m in self._all("SELECT worker_id, channel FROM members ORDER BY channel"):
            channels.setdefault(m["worker_id"], []).append(m["channel"])
        return [{**dict(r), "channels": channels.get(r["worker_id"], [])} for r in rows]

    # --- channels --------------------------------------------------------
    def add_channel(self, name: str, description: str = "") -> None:
        check_name("channel", name)
        try:
            self._run("INSERT INTO channels(name, description, created_at) VALUES (?, ?, ?)",
                      (name, description, time.time()))
        except sqlite3.IntegrityError:
            raise ValueError(f"channel {name!r} already exists") from None

    def remove_channel(self, name: str) -> bool:
        return self._run("DELETE FROM channels WHERE name = ?", (name,)).rowcount > 0

    def channel_exists(self, name: str) -> bool:
        return self._one("SELECT 1 FROM channels WHERE name = ?", (name,)) is not None

    def list_channels(self) -> list[dict[str, Any]]:
        rows = self._all("""
            SELECT c.name, c.description, c.created_at,
                   (SELECT COUNT(*) FROM messages m WHERE m.channel = c.name) AS messages,
                   (SELECT MAX(seq) FROM messages m WHERE m.channel = c.name) AS last_seq
            FROM channels c ORDER BY c.name""")
        members: dict[str, list[str]] = {}
        for m in self._all("SELECT channel, worker_id FROM members ORDER BY worker_id"):
            members.setdefault(m["channel"], []).append(m["worker_id"])
        return [{**dict(r), "members": members.get(r["name"], [])} for r in rows]

    # --- membership ------------------------------------------------------
    def head(self) -> int:
        """The last seq ever issued. Read from sqlite_sequence rather than
        MAX(seq) so pruning every message does not rewind it."""
        row = self._one("SELECT seq FROM sqlite_sequence WHERE name = 'messages'")
        return int(row["seq"]) if row else 0

    def join(self, channel: str, worker_id: str, *, from_start: bool = False) -> bool:
        """Add a worker to a channel. Returns False if it was already a member.

        A new member starts at the current head: it receives what is posted
        from now on. `from_start` also hands it whatever the channel still
        retains."""
        if not self.channel_exists(channel):
            raise LookupError(f"no channel {channel!r}")
        if not self.worker_exists(worker_id):
            raise LookupError(f"no worker {worker_id!r}")
        cursor = 0 if from_start else self.head()
        cur = self._run("INSERT OR IGNORE INTO members(channel, worker_id, cursor, joined_at) VALUES (?, ?, ?, ?)",
                        (channel, worker_id, cursor, time.time()))
        return cur.rowcount > 0

    def leave(self, channel: str, worker_id: str) -> bool:
        return self._run("DELETE FROM members WHERE channel = ? AND worker_id = ?",
                         (channel, worker_id)).rowcount > 0

    def is_member(self, channel: str, worker_id: str) -> bool:
        return self._one("SELECT 1 FROM members WHERE channel = ? AND worker_id = ?",
                         (channel, worker_id)) is not None

    def members_of(self, channel: str) -> list[str]:
        return [r["worker_id"] for r in
                self._all("SELECT worker_id FROM members WHERE channel = ? ORDER BY worker_id", (channel,))]

    def channels_of(self, worker_id: str) -> dict[str, int]:
        """channel -> acknowledged cursor, for every channel the worker is in."""
        return {r["channel"]: int(r["cursor"]) for r in
                self._all("SELECT channel, cursor FROM members WHERE worker_id = ? ORDER BY channel", (worker_id,))}

    def ack(self, channel: str, worker_id: str, seq: int) -> None:
        """Advance a cursor. It never moves backwards, and never past the last
        stored message, so a bad ack cannot make a worker skip future ones."""
        self._run("""
            UPDATE members
               SET cursor = MAX(cursor, MIN(?, (SELECT COALESCE(MAX(seq), 0) FROM messages)))
             WHERE channel = ? AND worker_id = ?""", (seq, channel, worker_id))

    # --- messages --------------------------------------------------------
    def append(self, channel: str, sender: str, body_json: str, client_id: str | None = None, *,
               ts: float | None = None) -> tuple[int, bool]:
        """Store a message. Returns (seq, duplicate).

        A publisher that lost its connection before hearing back resends with
        the same client_id; that resend returns the original seq instead of
        storing the message twice."""
        with self._lock:
            try:
                cur = self._conn.execute(
                    "INSERT INTO messages(channel, sender, client_id, body, ts) VALUES (?, ?, ?, ?, ?)",
                    (channel, sender, client_id, body_json, time.time() if ts is None else ts))
                return int(cur.lastrowid or 0), False
            except sqlite3.IntegrityError:
                if client_id is None:
                    raise
                row = self._conn.execute("SELECT seq FROM messages WHERE sender = ? AND client_id = ?",
                                         (sender, client_id)).fetchone()
                if row is None:
                    raise                   # not a resend: the channel vanished underneath us
                return int(row["seq"]), True

    def messages_after(self, channel: str, after: int, *, limit: int = 200,
                       exclude_sender: str | None = None) -> list[dict[str, Any]]:
        rows = self._all("""
            SELECT seq, channel, sender, body, ts FROM messages
             WHERE channel = ? AND seq > ? AND sender != ?
             ORDER BY seq LIMIT ?""", (channel, after, exclude_sender or "", limit))
        return [_message(r) for r in rows]

    def messages_before(self, channel: str, before: int, *, limit: int = 100) -> list[dict[str, Any]]:
        """The newest `limit` messages below `before`, oldest first."""
        rows = self._all("""
            SELECT seq, channel, sender, body, ts FROM messages
             WHERE channel = ? AND seq < ?
             ORDER BY seq DESC LIMIT ?""", (channel, before, limit))
        return [_message(r) for r in reversed(rows)]

    def prune(self, before_ts: float) -> int:
        return self._run("DELETE FROM messages WHERE ts < ?", (before_ts,)).rowcount
