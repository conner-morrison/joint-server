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
        self.tokens = {w: await self.store.add_worker(w) for w in ("alice", "bob", "carol")}
        await self.store.join("jobs", "alice")
        await self.store.join("jobs", "bob")

    async def asyncTearDown(self) -> None:
        await self.db.close()

    async def test_token_resolves_and_is_not_stored(self) -> None:
        self.assertEqual(await self.store.worker_for_token(self.tokens["alice"]), "alice")
        self.assertIsNone(await self.store.worker_for_token("nonsense"))
        self.assertIsNone(await self.store.worker_for_token(""))
        row = await self.db._one("SELECT token_hash FROM workers WHERE worker_id = 'alice'")
        self.assertNotIn(self.tokens["alice"], row["token_hash"])

    async def test_names_are_validated(self) -> None:
        for bad in ("", "@server", "-nope", "a" * 65, "has space"):
            with self.assertRaises(ValueError):
                await self.store.add_channel(bad)

    async def test_duplicates_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            await self.store.add_channel("jobs")
        with self.assertRaises(ValueError):
            await self.store.add_worker("alice")

    async def test_join_starts_at_head_unless_from_start(self) -> None:
        await self.store.append("jobs", "alice", {"n": 1})
        await self.store.append("jobs", "alice", {"n": 2})
        await self.store.join("jobs", "carol")
        self.assertEqual((await self.store.channels_of("carol"))["jobs"], await self.store.head())
        self.assertEqual(await self.store.messages_after("jobs", (await self.store.channels_of("carol"))["jobs"]), [])

        await self.store.leave("jobs", "carol")
        await self.store.join("jobs", "carol", from_start=True)
        got = await self.store.messages_after("jobs", (await self.store.channels_of("carol"))["jobs"])
        self.assertEqual([m["body"] for m in got], [{"n": 1}, {"n": 2}])

    async def test_resend_with_same_client_id_is_stored_once(self) -> None:
        first, dup1 = await self.store.append("jobs", "alice", {"n": 1}, "c1")
        again, dup2 = await self.store.append("jobs", "alice", {"n": 1}, "c1")
        self.assertEqual((first, dup1, again, dup2), (first, False, first, True))
        self.assertEqual(len(await self.store.messages_after("jobs", 0)), 1)

    async def test_the_same_id_from_a_different_sender_is_not_a_resend(self) -> None:
        a, _ = await self.store.append("jobs", "alice", 1, "shared")
        b, dup = await self.store.append("jobs", "bob", 1, "shared")
        self.assertNotEqual(a, b)
        self.assertFalse(dup)

    async def test_ack_never_moves_back_or_past_the_log(self) -> None:
        seq, _ = await self.store.append("jobs", "bob", {"n": 1})
        await self.store.ack("jobs", "alice", seq)
        self.assertEqual((await self.store.channels_of("alice"))["jobs"], seq)
        await self.store.ack("jobs", "alice", 0)
        self.assertEqual((await self.store.channels_of("alice"))["jobs"], seq)
        await self.store.ack("jobs", "alice", seq + 1000)
        self.assertEqual((await self.store.channels_of("alice"))["jobs"], seq)

    async def test_messages_exclude_their_sender(self) -> None:
        await self.store.append("jobs", "alice", {"from": "alice"})
        for_bob = await self.store.messages_after("jobs", 0, exclude_sender="bob")
        for_alice = await self.store.messages_after("jobs", 0, exclude_sender="alice")
        self.assertEqual(len(for_bob), 1)
        self.assertEqual(for_alice, [])

    async def test_head_survives_a_prune(self) -> None:
        """Pruning must not rewind the head, or a new member would start below
        messages that still exist and be handed old ones."""
        await self.store.append("jobs", "alice", {"n": 1}, ts=1.0)
        top, _ = await self.store.append("jobs", "alice", {"n": 2}, ts=1.0)
        self.assertEqual(await self.db.prune(2.0), 2)
        self.assertEqual(await self.store.head(), top)

    async def test_bodies_keep_their_shape(self) -> None:
        for body in ({"a": [1, 2, {"b": None}]}, [1, "two"], "text", 42, 3.5, True, None):
            seq, _ = await self.store.append("jobs", "alice", body)
            got = await self.store.messages_before("jobs", seq + 1, limit=1)
            self.assertEqual(got[0]["body"], body, f"{body!r} did not survive the round trip")

    async def test_removing_a_channel_takes_its_messages_and_members(self) -> None:
        await self.store.append("jobs", "alice", {"n": 1})
        self.assertTrue(await self.store.remove_channel("jobs"))
        self.assertEqual(await self.store.channels_of("alice"), {})
        self.assertEqual(await self.store.messages_after("jobs", 0), [])

    async def test_removing_a_worker_takes_its_memberships(self) -> None:
        self.assertTrue(await self.store.remove_worker("alice"))
        self.assertEqual(await self.store.members_of("jobs"), ["bob"])

    async def test_listings_report_members_and_counts(self) -> None:
        await self.store.append("jobs", "alice", {"n": 1})
        channels = await self.store.list_channels()
        self.assertEqual(channels[0]["name"], "jobs")
        self.assertEqual(channels[0]["messages"], 1)
        self.assertEqual(channels[0]["members"], ["alice", "bob"])
        workers = {w["worker_id"]: w for w in await self.store.list_workers()}
        self.assertEqual(workers["alice"]["channels"], ["jobs"])
        self.assertEqual(workers["carol"]["channels"], [])

    async def test_join_requires_both_to_exist(self) -> None:
        with self.assertRaises(LookupError):
            await self.store.join("nope", "alice")
        with self.assertRaises(LookupError):
            await self.store.join("jobs", "nobody")


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
            await ws.add_worker("scout")
            await ws.join("jobs", "scout")
        self.assertEqual(await self.acme.members_of("jobs"), ["scout"])
        self.assertEqual(await self.upwork.members_of("jobs"), ["scout"])

    async def test_messages_do_not_cross(self) -> None:
        for ws in (self.acme, self.upwork):
            await ws.add_channel("jobs")
            await ws.add_worker("scout")
            await ws.join("jobs", "scout", from_start=True)
        await self.acme.append("jobs", "@server", {"secret": "acme only"})

        self.assertEqual(len(await self.acme.messages_after("jobs", 0)), 1)
        self.assertEqual(await self.upwork.messages_after("jobs", 0), [])
        self.assertEqual(await self.upwork.waiting_for("scout"), [])
        self.assertEqual(len(await self.acme.waiting_for("scout")), 1)

    async def test_a_token_belongs_to_one_workspace(self) -> None:
        await self.acme.add_channel("jobs")
        token = await self.acme.add_worker("scout")
        self.assertEqual(await self.db.workspace_for_token(token), ("acme", "scout"))
        self.assertEqual(await self.acme.worker_for_token(token), "scout")
        # The same token presented to another workspace is not a token at all.
        self.assertIsNone(await self.upwork.worker_for_token(token))

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
        await self.acme.add_worker("scout")
        await self.acme.join("jobs", "scout")
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
        await self.acme.add_worker("scout")
        await self.acme.join("jobs", "scout")
        await self.acme.join("results", "scout")
        await self.acme.append("jobs", "@server", {"n": 1})
        await self.acme.append("results", "@server", {"n": 2})
        await self.acme.append("jobs", "@server", {"n": 3})
        waiting = await self.acme.waiting_for("scout")
        self.assertEqual([m["body"]["n"] for m in waiting], [1, 2, 3])
        self.assertEqual([m["seq"] for m in waiting], sorted(m["seq"] for m in waiting))

    async def test_a_poll_does_not_return_what_was_acked(self) -> None:
        await self.acme.add_channel("jobs")
        await self.acme.add_worker("scout")
        await self.acme.join("jobs", "scout")
        seq, _ = await self.acme.append("jobs", "@server", {"n": 1})
        self.assertEqual(len(await self.acme.waiting_for("scout")), 1)
        await self.acme.ack("jobs", "scout", seq)
        self.assertEqual(await self.acme.waiting_for("scout"), [])

    async def test_slugify_matches_what_the_console_derives(self) -> None:
        self.assertEqual(slugify("My Acme Jobs"), "myacmejobs")
        self.assertEqual(slugify("  Upwork  "), "upwork")
        self.assertEqual(slugify("a/b?c"), "abc")
        self.assertEqual(slugify("Rele-vant_1.0"), "rele-vant_1.0")
