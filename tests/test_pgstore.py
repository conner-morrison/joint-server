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

from relay.pgstore import PgStore

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
        self.store = PgStore(_server.get_uri(), min_size=1, max_size=4)
        await self.store.open()
        await self.store.setup()
        await self.store.add_channel("jobs", "work to be picked up")
        self.tokens = {w: await self.store.add_worker(w) for w in ("alice", "bob", "carol")}
        await self.store.join("jobs", "alice")
        await self.store.join("jobs", "bob")

    async def asyncTearDown(self) -> None:
        await self.store.close()

    async def test_token_resolves_and_is_not_stored(self) -> None:
        self.assertEqual(await self.store.worker_for_token(self.tokens["alice"]), "alice")
        self.assertIsNone(await self.store.worker_for_token("nonsense"))
        self.assertIsNone(await self.store.worker_for_token(""))
        row = await self.store._one("SELECT token_hash FROM workers WHERE worker_id = 'alice'")
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
        self.assertEqual(await self.store.prune(2.0), 2)
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
