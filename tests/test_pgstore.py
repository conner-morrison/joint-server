"""PgStore against a real Postgres.

An embedded server is started once for the module; each test gets a clean
schema. Nothing here is mocked: the behaviour that matters (dedupe, cursors,
head surviving a prune) is enforced by the database, so a fake would be
testing itself.
"""
from __future__ import annotations

import os
import tempfile
import unittest

from relay.pgstore import PgStore, slugify

try:
    import pgserver
except ImportError:                                   # pragma: no cover
    pgserver = None

_server = None
_tmp = None


def setUpModule() -> None:
    global _server, _tmp
    if pgserver is None:
        raise unittest.SkipTest("pgserver is not installed")
    _tmp = tempfile.TemporaryDirectory()
    _server = pgserver.get_server(_tmp.name)


def tearDownModule() -> None:
    if _server is not None:
        _server.cleanup()
    if _tmp is not None:
        _tmp.cleanup()


class PgStoreTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        assert _server is not None
        _server.psql("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        self.db = PgStore(_server.get_uri(), min_size=1, max_size=4)
        await self.db.open()
        await self.db.setup()
        await self.db.create_workspace("acme", "hunter2", "Acme")
        self.store = self.db.ws("acme")
        await self.store.add_channel("jobs", "work to be picked up")
        # The server gives out the ids; a test learns them the way a worker
        # does, by being told when it is registered.
        self.ids = {}
        for who in ("alice", "bob", "carol"):
            self.ids[who] = (await self.store.add_worker(who))["worker_id"]
        self.alice, self.bob, self.carol = (self.ids[w] for w in ("alice", "bob", "carol"))
        await self.store.join("jobs", self.alice)
        await self.store.join("jobs", self.bob)

    async def asyncTearDown(self) -> None:
        await self.db.close()

    async def test_a_worker_from_before_ids_still_gets_in_with_its_token(self) -> None:
        """The reason the token column survives. Workers registered under the
        old model are running machines, and some of them are code nobody here
        can edit; breaking them at deploy time to tidy a column would be the
        wrong trade. Their name is the id they were registered under, which is
        what a person had been reading all along.
        """
        from relay.store import _hash

        await self.db._run(
            "INSERT INTO workers(workspace, worker_id, name, token_hash, label, created_at) "
            "VALUES ('acme', 'gmail', 'gmail', %s, '', 1.0)", (_hash("an-old-token"),))
        # Reached by the token it has always used.
        self.assertEqual(await self.store.authenticate("an-old-token"), "gmail")
        # And not by its id, which is a name and was never a secret.
        self.assertIsNone(await self.store.authenticate("gmail"))
        listed = {w["worker_id"]: w for w in await self.store.list_workers()}
        self.assertEqual(listed["gmail"]["name"], "gmail")
        self.assertTrue(listed["gmail"]["legacy"], "the console should be able to say so")
        self.assertFalse(listed[self.alice]["legacy"])

    async def test_a_registered_worker_holds_no_token(self) -> None:
        """There is no token any more. A worker is reached by the id it was
        given, and nothing else about it is a secret."""
        row = await self.db._one(
            "SELECT token_hash, name FROM workers WHERE worker_id = %s", (self.alice,))
        self.assertIsNone(row["token_hash"])
        self.assertEqual(row["name"], "alice")

    async def test_names_are_validated(self) -> None:
        for bad in ("", "@server", "-nope", "a" * 65, "has space"):
            with self.assertRaises(ValueError):
                await self.store.add_channel(bad)

    async def test_a_duplicate_channel_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            await self.store.add_channel("jobs")

    async def test_two_workers_may_share_a_name(self) -> None:
        """A name is what a person calls a worker, and two machines may both
        reasonably be called the same thing. The id is what tells them apart,
        and the server gives a different one to each."""
        again = await self.store.add_worker("alice")
        self.assertNotEqual(again["worker_id"], self.alice)
        self.assertEqual(again["name"], "alice")
        names = [w["name"] for w in await self.store.list_workers()]
        self.assertEqual(names.count("alice"), 2)

    async def test_join_starts_at_head_unless_from_start(self) -> None:
        await self.store.append("jobs", self.alice, {"n": 1})
        await self.store.append("jobs", self.alice, {"n": 2})
        await self.store.join("jobs", self.carol)
        self.assertEqual((await self.store.channels_of(self.carol))["jobs"], await self.store.head())
        self.assertEqual(await self.store.messages_after("jobs", (await self.store.channels_of(self.carol))["jobs"]), [])

        await self.store.leave("jobs", self.carol)
        await self.store.join("jobs", self.carol, from_start=True)
        got = await self.store.messages_after("jobs", (await self.store.channels_of(self.carol))["jobs"])
        self.assertEqual([m["body"] for m in got], [{"n": 1}, {"n": 2}])

    async def test_resend_with_same_client_id_is_stored_once(self) -> None:
        first, dup1 = await self.store.append("jobs", self.alice, {"n": 1}, "c1")
        again, dup2 = await self.store.append("jobs", self.alice, {"n": 1}, "c1")
        self.assertEqual((first, dup1, again, dup2), (first, False, first, True))
        self.assertEqual(len(await self.store.messages_after("jobs", 0)), 1)

    async def test_the_same_id_is_one_message_whoever_sends_it(self) -> None:
        """The same job can reach two different workers - a mail parser and a
        webhook - and it is still one job. Scoping this to the sender would let
        each of them store its own copy."""
        a, first = await self.store.append("jobs", self.alice, 1, "shared")
        b, again = await self.store.append("jobs", self.bob, 2, "shared")
        self.assertEqual(a, b, "the second sender is told which message already has that id")
        self.assertEqual((first, again), (False, True))
        self.assertEqual(len(await self.store.messages_after("jobs", 0)), 1)

    async def test_ack_never_moves_back_or_past_the_log(self) -> None:
        seq, _ = await self.store.append("jobs", self.bob, {"n": 1})
        await self.store.ack("jobs", self.alice, seq)
        self.assertEqual((await self.store.channels_of(self.alice))["jobs"], seq)
        await self.store.ack("jobs", self.alice, 0)
        self.assertEqual((await self.store.channels_of(self.alice))["jobs"], seq)
        await self.store.ack("jobs", self.alice, seq + 1000)
        self.assertEqual((await self.store.channels_of(self.alice))["jobs"], seq)

    async def test_messages_exclude_their_sender(self) -> None:
        await self.store.append("jobs", self.alice, {"from": self.alice})
        for_bob = await self.store.messages_after("jobs", 0, exclude_sender=self.bob)
        for_alice = await self.store.messages_after("jobs", 0, exclude_sender=self.alice)
        self.assertEqual(len(for_bob), 1)
        self.assertEqual(for_alice, [])

    async def test_head_survives_a_prune(self) -> None:
        """Pruning must not rewind the head, or a new member would start below
        messages that still exist and be handed old ones."""
        await self.store.append("jobs", self.alice, {"n": 1}, ts=1.0)
        top, _ = await self.store.append("jobs", self.alice, {"n": 2}, ts=1.0)
        self.assertEqual(await self.db.prune(2.0), 2)
        self.assertEqual(await self.store.head(), top)

    async def test_bodies_keep_their_shape(self) -> None:
        for body in ({"a": [1, 2, {"b": None}]}, [1, "two"], "text", 42, 3.5, True, None):
            seq, _ = await self.store.append("jobs", self.alice, body)
            got = await self.store.messages_before("jobs", seq + 1, limit=1)
            self.assertEqual(got[0]["body"], body, f"{body!r} did not survive the round trip")

    async def test_a_channel_keeps_only_its_newest(self) -> None:
        """A channel is the last hundred things that happened, not everything
        that ever did. The oldest go as the newest arrive, in order."""
        from relay import pgstore
        was, pgstore.CHANNEL_MAX = pgstore.CHANNEL_MAX, 5
        try:
            for n in range(8):
                await self.store.append("jobs", self.alice, {"n": n})
            kept = await self.store.messages_after("jobs", 0)
            self.assertEqual([m["body"]["n"] for m in kept], [3, 4, 5, 6, 7])
        finally:
            pgstore.CHANNEL_MAX = was

    async def test_the_cap_is_per_channel(self) -> None:
        """One busy channel does not evict another's history."""
        from relay import pgstore
        was, pgstore.CHANNEL_MAX = pgstore.CHANNEL_MAX, 3
        try:
            await self.store.add_channel("quiet")
            await self.store.append("quiet", self.alice, {"kept": True})
            for n in range(6):
                await self.store.append("jobs", self.alice, {"n": n})
            quiet = await self.store.messages_after("quiet", 0)
            self.assertEqual([m["body"] for m in quiet], [{"kept": True}])
            self.assertEqual(len(await self.store.messages_after("jobs", 0)), 3)
        finally:
            pgstore.CHANNEL_MAX = was

    async def test_the_head_survives_the_cap(self) -> None:
        """Dropping the oldest must not rewind the sequence, or a new member
        would start below messages that still exist and be handed old ones."""
        from relay import pgstore
        was, pgstore.CHANNEL_MAX = pgstore.CHANNEL_MAX, 2
        try:
            for n in range(5):
                await self.store.append("jobs", self.alice, {"n": n})
            head = await self.store.head()
            await self.store.join("jobs", self.carol)
            self.assertEqual((await self.store.channels_of(self.carol))["jobs"], head)
            self.assertEqual(await self.store.messages_after("jobs", head), [])
        finally:
            pgstore.CHANNEL_MAX = was

    async def test_removing_a_channel_takes_its_messages_and_members(self) -> None:
        await self.store.append("jobs", self.alice, {"n": 1})
        self.assertTrue(await self.store.remove_channel("jobs"))
        self.assertEqual(await self.store.channels_of(self.alice), {})
        self.assertEqual(await self.store.messages_after("jobs", 0), [])

    async def test_removing_a_worker_takes_its_memberships(self) -> None:
        self.assertTrue(await self.store.remove_worker(self.alice))
        self.assertEqual([m["worker_id"] for m in await self.store.members_of("jobs")],
                         [self.bob])

    async def test_listings_report_members_and_counts(self) -> None:
        await self.store.append("jobs", self.alice, {"n": 1})
        channels = await self.store.list_channels()
        self.assertEqual(channels[0]["name"], "jobs")
        self.assertEqual(channels[0]["messages"], 1)
        # Both names: the one to read, and the one to act with.
        self.assertEqual([(m["name"], m["worker_id"]) for m in channels[0]["members"]],
                         [("alice", self.alice), ("bob", self.bob)])
        workers = {w["worker_id"]: w for w in await self.store.list_workers()}
        self.assertEqual(workers[self.alice]["channels"], ["jobs"])
        self.assertEqual(workers[self.carol]["channels"], [])

    async def test_join_requires_both_to_exist(self) -> None:
        with self.assertRaises(LookupError):
            await self.store.join("nope", self.alice)
        with self.assertRaises(LookupError):
            await self.store.join("jobs", "nobody")


class MigrationTest(unittest.IsolatedAsyncioTestCase):
    """Starting against a database written by the previous version.

    This runs on the deployment's live data at the next deploy, so it is worth
    knowing it works before then rather than afterwards.
    """

    async def asyncSetUp(self) -> None:
        assert _server is not None
        _server.psql("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        # The schema as it was: a worker chose its own id and token, and a
        # request waiting for approval carried the token it had chosen.
        _server.psql("""
            CREATE TABLE workspaces (
                slug TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '',
                password_hash TEXT NOT NULL, created_at DOUBLE PRECISION NOT NULL);
            CREATE TABLE workers (
                workspace TEXT NOT NULL REFERENCES workspaces(slug) ON DELETE CASCADE,
                worker_id TEXT NOT NULL, token_hash TEXT NOT NULL UNIQUE,
                label TEXT NOT NULL DEFAULT '', created_at DOUBLE PRECISION NOT NULL,
                last_seen DOUBLE PRECISION, PRIMARY KEY (workspace, worker_id));
            CREATE TABLE pending_workers (
                workspace TEXT NOT NULL REFERENCES workspaces(slug) ON DELETE CASCADE,
                worker_id TEXT NOT NULL, token_hash TEXT NOT NULL,
                label TEXT NOT NULL DEFAULT '', requested_at DOUBLE PRECISION NOT NULL,
                PRIMARY KEY (workspace, worker_id));
            CREATE TABLE channels (
                workspace TEXT NOT NULL REFERENCES workspaces(slug) ON DELETE CASCADE,
                name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '',
                created_at DOUBLE PRECISION NOT NULL, PRIMARY KEY (workspace, name));
            CREATE TABLE messages (
                seq BIGSERIAL PRIMARY KEY, workspace TEXT NOT NULL, channel TEXT NOT NULL,
                sender TEXT NOT NULL, client_id TEXT, body JSONB NOT NULL,
                ts DOUBLE PRECISION NOT NULL);
            INSERT INTO workspaces VALUES ('upwork', 'Upwork', 'x', 1.0);
            INSERT INTO workers VALUES ('upwork', 'gmail', 'hash-of-a-token', '', 1.0, NULL);
            INSERT INTO pending_workers VALUES ('upwork', 'hopeful', 'hash-2', '', 1.0);
            INSERT INTO channels VALUES ('upwork', 'jobs', '', 1.0);
            INSERT INTO messages(workspace, channel, sender, body, ts) VALUES
              ('upwork', 'jobs', '@server',
               '{"title":"A job","upworkUrl":"https://www.upwork.com/jobs/~0221"}', 1.0);
        """)
        self.db = PgStore(_server.get_uri(), min_size=1, max_size=4)
        await self.db.open()
        await self.db.setup()

    async def asyncTearDown(self) -> None:
        await self.db.close()

    async def test_an_existing_worker_keeps_its_id_and_gains_a_name(self) -> None:
        row = await self.db._one(
            "SELECT worker_id, name, token_hash FROM workers WHERE workspace = 'upwork'")
        self.assertEqual((row["worker_id"], row["name"], row["token_hash"]),
                         ("gmail", "gmail", "hash-of-a-token"))

    async def test_a_worker_registered_now_needs_no_token(self) -> None:
        """The column has to stop being required, or nothing can be registered
        under the new model at all."""
        made = await self.db.ws("upwork").add_worker("Office PC")
        row = await self.db._one("SELECT token_hash FROM workers WHERE worker_id = %s",
                                 (made["worker_id"],))
        self.assertIsNone(row["token_hash"])

    async def test_a_table_made_before_the_job_column_still_starts(self) -> None:
        """What a column added to the schema costs. CREATE TABLE IF NOT EXISTS
        adds nothing to a table that is already there, so anything in the
        schema that depends on a new column fails on exactly the databases that
        matter - the ones with data in them - and takes the whole schema, and
        the deployment, down with it. The column is added by migration, and the
        setUp here has a messages table so that is actually exercised."""
        row = await self.db._one(
            "SELECT column_name FROM information_schema.columns "
            " WHERE table_name = 'messages' AND column_name = 'job_key'")
        self.assertIsNotNone(row, "the job column was never added")

    async def test_jobs_stored_before_the_column_are_read(self) -> None:
        """Otherwise the first repeat of every job already in a channel gets
        through, which is the whole of what this was meant to stop."""
        seq, duplicate = await self.db.ws("upwork").append(
            "jobs", "@server",
            {"title": "The same job again", "upworkUrl": "https://www.upwork.com/jobs/~0221"})
        self.assertTrue(duplicate)
        self.assertEqual(seq, 1)

    async def test_requests_from_the_old_flow_are_gone(self) -> None:
        """Their whole premise was a token the worker brought, so they cannot be
        honoured. Leaving them in a console that can no longer act on them would
        be worse than clearing them."""
        gone = await self.db._one(
            "SELECT 1 FROM information_schema.tables WHERE table_name = 'pending_workers'")
        self.assertIsNone(gone)
        self.assertEqual(await self.db.ws("upwork").list_pending(), [])


class WorkspaceTest(unittest.IsolatedAsyncioTestCase):
    """Two workspaces on one deployment must not be able to see each other."""

    async def asyncSetUp(self) -> None:
        assert _server is not None
        _server.psql("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        self.db = PgStore(_server.get_uri(), min_size=1, max_size=4)
        await self.db.open()
        await self.db.setup()
        await self.db.create_workspace("acme", "acme-pass", "Acme")
        await self.db.create_workspace("upwork", "upwork-pass", "Upwork")
        self.acme, self.upwork = self.db.ws("acme"), self.db.ws("upwork")

    async def asyncTearDown(self) -> None:
        await self.db.close()

    async def test_password_opens_only_its_own_workspace(self) -> None:
        self.assertTrue(await self.db.check_password("acme", "acme-pass"))
        self.assertFalse(await self.db.check_password("acme", "upwork-pass"))
        self.assertFalse(await self.db.check_password("acme", ""))
        self.assertFalse(await self.db.check_password("nosuch", "acme-pass"))

    async def test_password_is_not_stored(self) -> None:
        row = await self.db._one("SELECT password_hash FROM workspaces WHERE slug = 'acme'")
        self.assertNotIn("acme-pass", row["password_hash"])

    async def test_slugs_are_unique_and_validated(self) -> None:
        with self.assertRaises(ValueError):
            await self.db.create_workspace("acme", "other")
        for bad in ("", "has space", "-nope", "a" * 65):
            with self.assertRaises(ValueError):
                await self.db.create_workspace(bad, "pass")
        with self.assertRaises(ValueError):
            await self.db.create_workspace("fine", "")

    async def test_the_same_names_can_exist_in_both(self) -> None:
        """Two workspaces each with a 'jobs' channel and a 'scout' worker is
        ordinary, not a conflict."""
        for ws in (self.acme, self.upwork):
            await ws.add_channel("jobs")
            scout = await ws.add_worker("scout")
            await ws.join("jobs", scout["worker_id"])
        for ws in (self.acme, self.upwork):
            self.assertEqual([m["name"] for m in await ws.members_of("jobs")], ["scout"])

    async def test_messages_do_not_cross(self) -> None:
        scouts = {}
        for name, ws in (("acme", self.acme), ("upwork", self.upwork)):
            await ws.add_channel("jobs")
            scouts[name] = (await ws.add_worker("scout"))["worker_id"]
            await ws.join("jobs", scouts[name], from_start=True)
        await self.acme.append("jobs", "@server", {"secret": "acme only"})

        self.assertEqual(len(await self.acme.messages_after("jobs", 0)), 1)
        self.assertEqual(await self.upwork.messages_after("jobs", 0), [])
        self.assertEqual(await self.upwork.waiting_for(scouts["upwork"]), [])
        self.assertEqual(len(await self.acme.waiting_for(scouts["acme"])), 1)

    async def test_an_id_opens_only_its_own_workspace(self) -> None:
        """An id is a worker's whole credential, so it must mean nothing in a
        workspace that did not issue it."""
        await self.acme.add_channel("jobs")
        scout = await self.acme.add_worker("scout")
        self.assertEqual(await self.acme.authenticate(scout["worker_id"]), scout["worker_id"])
        self.assertIsNone(await self.upwork.authenticate(scout["worker_id"]))
        self.assertIsNone(await self.acme.authenticate("w-" + "0" * 32))
        self.assertIsNone(await self.acme.authenticate(""))

    async def test_the_same_dedupe_id_is_independent_per_workspace(self) -> None:
        for ws in (self.acme, self.upwork):
            await ws.add_channel("jobs")
        a, dup_a = await self.acme.append("jobs", "@server", 1, "gmail-1:0")
        b, dup_b = await self.upwork.append("jobs", "@server", 1, "gmail-1:0")
        self.assertNotEqual(a, b)
        self.assertFalse(dup_a or dup_b)
        again, dup = await self.acme.append("jobs", "@server", 1, "gmail-1:0")
        self.assertEqual((again, dup), (a, True))

    async def test_deleting_a_workspace_takes_everything_in_it(self) -> None:
        await self.acme.add_channel("jobs")
        scout = await self.acme.add_worker("scout")
        await self.acme.join("jobs", scout["worker_id"])
        await self.acme.append("jobs", "@server", {"n": 1})
        await self.upwork.add_channel("jobs")
        await self.upwork.append("jobs", "@server", {"n": 2})

        self.assertTrue(await self.db.remove_workspace("acme"))
        self.assertEqual(await self.db._all("SELECT 1 FROM messages WHERE workspace = 'acme'"), [])
        self.assertEqual(await self.db._all("SELECT 1 FROM workers WHERE workspace = 'acme'"), [])
        self.assertEqual(await self.db._all("SELECT 1 FROM members WHERE workspace = 'acme'"), [])
        # The other workspace is untouched.
        self.assertEqual(len(await self.upwork.messages_after("jobs", 0)), 1)

    async def test_waiting_for_spans_channels_in_order(self) -> None:
        await self.acme.add_channel("jobs")
        await self.acme.add_channel("results")
        scout = (await self.acme.add_worker("scout"))["worker_id"]
        await self.acme.join("jobs", scout)
        await self.acme.join("results", scout)
        await self.acme.append("jobs", "@server", {"n": 1})
        await self.acme.append("results", "@server", {"n": 2})
        await self.acme.append("jobs", "@server", {"n": 3})
        waiting = await self.acme.waiting_for(scout)
        self.assertEqual([m["body"]["n"] for m in waiting], [1, 2, 3])
        self.assertEqual([m["seq"] for m in waiting], sorted(m["seq"] for m in waiting))

    async def test_a_poll_does_not_return_what_was_acked(self) -> None:
        await self.acme.add_channel("jobs")
        scout = (await self.acme.add_worker("scout"))["worker_id"]
        await self.acme.join("jobs", scout)
        seq, _ = await self.acme.append("jobs", "@server", {"n": 1})
        self.assertEqual(len(await self.acme.waiting_for(scout)), 1)
        await self.acme.ack("jobs", scout, seq)
        self.assertEqual(await self.acme.waiting_for(scout), [])

    async def test_slugify_matches_what_the_console_derives(self) -> None:
        self.assertEqual(slugify("My Acme Jobs"), "myacmejobs")
        self.assertEqual(slugify("  Upwork  "), "upwork")
        self.assertEqual(slugify("a/b?c"), "abc")
        self.assertEqual(slugify("Rele-vant_1.0"), "rele-vant_1.0")


class WidenDedupeTest(unittest.IsolatedAsyncioTestCase):
    """Moving a database made before dedupe covered every publisher.

    `CREATE INDEX IF NOT EXISTS` does nothing when an index of that name
    exists, whatever its definition, so the old rule survives a deploy unless
    it is replaced by name.
    """

    async def asyncSetUp(self) -> None:
        assert _server is not None
        _server.psql("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        self.db = PgStore(_server.get_uri(), min_size=1, max_size=4)
        await self.db.open()
        async with self.db.pool.connection() as conn:       # as an older deploy left it
            await conn.execute("DROP INDEX IF EXISTS messages_dedupe_workspace")
            await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS messages_dedupe "
                               "ON messages(workspace, sender, client_id) "
                               "WHERE client_id IS NOT NULL")
        await self.db.create_workspace("acme", "p", "Acme")
        self.ws = self.db.ws("acme")
        await self.ws.add_channel("jobs")

    async def asyncTearDown(self) -> None:
        await self.db.close()

    async def dedupe_indexes(self) -> list[str]:
        rows = await self.db._all(
            "SELECT indexname FROM pg_indexes WHERE tablename = 'messages'")
        return sorted(r["indexname"] for r in rows if "dedupe" in r["indexname"])

    async def test_a_restart_widens_it(self) -> None:
        self.assertEqual(await self.dedupe_indexes(), ["messages_dedupe"])
        await self.db.setup()
        self.assertEqual(await self.dedupe_indexes(), ["messages_dedupe_workspace"])
        await self.ws.append("jobs", "one", 1, "job:~123")
        seq, dup = await self.ws.append("jobs", "two", 1, "job:~123")
        self.assertTrue(dup, "a second publisher with that id is the same message")

    async def test_history_that_cannot_be_widened_is_left_alone(self) -> None:
        """Two senders already share an id. Those messages are history and not
        this code's to delete, so the old rule stays and the deployment runs."""
        await self.ws.append("jobs", "one", 1, "job:~123")
        await self.ws.append("jobs", "two", 1, "job:~123")
        await self.db.setup()
        self.assertEqual(await self.dedupe_indexes(), ["messages_dedupe"])
        # Still serving, still deduplicating per sender.
        _, dup = await self.ws.append("jobs", "one", 1, "job:~123")
        self.assertTrue(dup)
