"""The multi-workspace relay, over HTTP, against a real Postgres.

A live server on a free port, driven with real requests. The enrolment path is
the reason this file is long: a worker that nobody has heard of must be able to
arrive, be refused in a way it can act on, ask, wait, and then carry on without
anyone touching it again.
"""
from __future__ import annotations

import asyncio
import json
import socket
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from typing import Any

import uvicorn
from http.server import BaseHTTPRequestHandler, HTTPServer

from relay import telegram as telegram_module
from relay.notify import Notifier
from relay.pgstore import PgStore
from relay.sender import BotSender
from relay.serverless import create_app

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


class LiveApp:
    def __init__(self, dsn: str):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}"
        app = create_app(PgStore(dsn, min_size=1, max_size=8), Notifier(dsn))
        config = uvicorn.Config(app, host="127.0.0.1", port=self.port,
                                log_level="warning", timeout_graceful_shutdown=2)
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> None:
        self.thread.start()
        deadline = time.time() + 15
        while not self.server.started:
            if time.time() > deadline:
                raise RuntimeError("server did not start")
            time.sleep(0.02)

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(10)


class ServerlessTest(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        assert _server is not None
        cls.app = LiveApp(_server.get_uri())
        cls.app.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.app.stop()

    def setUp(self) -> None:
        assert _server is not None
        _server.psql("TRUNCATE workspaces CASCADE;")

    async def call(self, method: str, path: str, body: Any = None, token: str | None = None
                   ) -> tuple[int, Any]:
        def go() -> tuple[int, Any]:
            headers = {"Content-Type": "application/json"}
            if token is not None:
                headers["Authorization"] = f"Bearer {token}"
            data = json.dumps(body).encode() if body is not None else None
            req = urllib.request.Request(self.app.url + path, data=data, method=method, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=40) as r:
                    return r.status, json.loads(r.read() or b"null")
            except urllib.error.HTTPError as e:
                with e:
                    return e.code, json.loads(e.read() or b"null")
        return await asyncio.to_thread(go)

    async def workspace(self, name: str = "Acme Jobs", password: str = "hunter2") -> str:
        status, body = await self.call("POST", "/api/workspaces", {"name": name, "password": password})
        self.assertEqual(status, 201, body)
        return body["slug"]

    # --- workspaces -------------------------------------------------------
    async def test_a_workspace_is_created_at_a_slug_of_its_name(self) -> None:
        status, body = await self.call("POST", "/api/workspaces", {"name": "My Acme Jobs", "password": "p"})
        self.assertEqual((status, body["slug"], body["name"]), (201, "myacmejobs", "My Acme Jobs"))

    async def test_a_slug_is_claimed_once(self) -> None:
        await self.workspace("Acme")
        status, _ = await self.call("POST", "/api/workspaces", {"name": "acme", "password": "other"})
        self.assertEqual(status, 409)

    async def test_a_name_with_nothing_usable_in_it_is_refused(self) -> None:
        status, _ = await self.call("POST", "/api/workspaces", {"name": "???", "password": "p"})
        self.assertEqual(status, 400)

    async def test_the_password_opens_only_its_own_workspace(self) -> None:
        await self.workspace("Acme", "acme-pass")
        await self.workspace("Upwork", "upwork-pass")
        ok, _ = await self.call("GET", "/acme/api/channels", token="acme-pass")
        wrong, _ = await self.call("GET", "/acme/api/channels", token="upwork-pass")
        none, _ = await self.call("GET", "/acme/api/channels")
        self.assertEqual((ok, wrong, none), (200, 401, 401))

    async def test_an_unknown_workspace_is_404(self) -> None:
        status, _ = await self.call("GET", "/nosuch/api/channels", token="p")
        self.assertEqual(status, 404)

    async def test_the_workspaces_here_are_listed_by_name(self) -> None:
        await self.workspace("Acme", "acme-pass")
        await self.workspace("Upwork", "upwork-pass")
        status, listed = await self.call("GET", "/api/workspaces")
        self.assertEqual(status, 200)
        self.assertEqual([w["slug"] for w in listed], ["acme", "upwork"])
        self.assertEqual([w["name"] for w in listed], ["Acme", "Upwork"])

    async def test_the_listing_never_carries_a_password(self) -> None:
        """Names are public here; what opens them is not."""
        await self.workspace("Acme", "swordfish")
        _, listed = await self.call("GET", "/api/workspaces")
        blob = json.dumps(listed)
        self.assertNotIn("swordfish", blob)
        self.assertNotIn("password", blob)

    async def test_an_address_says_what_workspace_lives_there(self) -> None:
        """A workspace URL opened on a new device has to be able to greet the
        visitor by name before anyone has a password to offer."""
        await self.workspace("Acme Jobs", "p")
        status, body = await self.call("GET", "/api/workspaces/acmejobs")
        self.assertEqual((status, body["exists"], body["name"]), (200, True, "Acme Jobs"))

        status, body = await self.call("GET", "/api/workspaces/nosuch")
        self.assertEqual((status, body["exists"], body["name"]), (200, False, ""))

    # --- enrolment --------------------------------------------------------
    async def test_an_unknown_worker_is_told_how_to_ask(self) -> None:
        ws = await self.workspace()
        status, body = await self.call("POST", f"/{ws}/publish",
                                       {"channel": "jobs", "body": 1}, token="a-token-nobody-knows")
        self.assertEqual(status, 401)
        self.assertEqual(body["status"], "unregistered")
        self.assertEqual(body["enrol"], f"/{ws}/enrol")

    async def test_reading_also_tells_an_unknown_worker_how_to_ask(self) -> None:
        ws = await self.workspace()
        status, body = await self.call("GET", f"/{ws}/messages", token="nobody-knows-this")
        self.assertEqual((status, body["status"]), (401, "unregistered"))

    async def test_a_request_waits_and_the_token_does_not_work_yet(self) -> None:
        ws = await self.workspace()
        status, body = await self.call("POST", f"/{ws}/enrol",
                                       {"worker_id": "scout-1", "token": "scout-token", "label": "Office PC"})
        self.assertEqual((status, body["status"]), (202, "pending"))

        # Still refused, but now with something it can wait on rather than retry.
        status, body = await self.call("POST", f"/{ws}/publish",
                                       {"channel": "jobs", "body": 1}, token="scout-token")
        self.assertEqual((status, body["status"], body["worker_id"]), (403, "pending", "scout-1"))

    async def test_a_person_sees_the_request_with_a_fingerprint(self) -> None:
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/enrol", {"worker_id": "scout-1", "token": "scout-token"})
        status, pending = await self.call("GET", f"/{ws}/api/pending", token="hunter2")
        self.assertEqual(status, 200)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["worker_id"], "scout-1")
        # Something the worker can print too, so a person can compare the two.
        self.assertEqual(len(pending[0]["fingerprint"]), 12)
        self.assertNotIn("scout-token", json.dumps(pending))

    async def test_approval_makes_the_token_it_already_had_start_working(self) -> None:
        """The whole point: nobody carries a credential anywhere."""
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/api/channels", {"name": "jobs"}, token="hunter2")
        await self.call("POST", f"/{ws}/enrol", {"worker_id": "scout-1", "token": "scout-token"})

        status, _ = await self.call("POST", f"/{ws}/api/pending/scout-1", token="hunter2")
        self.assertEqual(status, 200)
        status, _ = await self.call("PUT", f"/{ws}/api/channels/jobs/members/scout-1",
                                    {"from_start": False}, token="hunter2")
        self.assertEqual(status, 200)

        status, body = await self.call("POST", f"/{ws}/publish",
                                       {"channel": "jobs", "body": {"n": 1}}, token="scout-token")
        self.assertEqual((status, body["worker_id"]), (201, "scout-1"))

    async def test_a_rejected_request_leaves_nothing_behind(self) -> None:
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/enrol", {"worker_id": "scout-1", "token": "scout-token"})
        status, _ = await self.call("DELETE", f"/{ws}/api/pending/scout-1", token="hunter2")
        self.assertEqual(status, 200)
        _, pending = await self.call("GET", f"/{ws}/api/pending", token="hunter2")
        self.assertEqual(pending, [])
        status, body = await self.call("GET", f"/{ws}/messages", token="scout-token")
        self.assertEqual((status, body["status"]), (401, "unregistered"))

    async def test_a_newcomer_cannot_claim_a_registered_name(self) -> None:
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/api/workers", {"worker_id": "scout-1"}, token="hunter2")
        status, _ = await self.call("POST", f"/{ws}/enrol",
                                    {"worker_id": "scout-1", "token": "an-impostor"})
        self.assertEqual(status, 409)

    async def test_enrolling_again_with_the_same_token_is_harmless(self) -> None:
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/enrol", {"worker_id": "scout-1", "token": "t"})
        await self.call("POST", f"/{ws}/api/pending/scout-1", token="hunter2")
        status, body = await self.call("POST", f"/{ws}/enrol", {"worker_id": "scout-1", "token": "t"})
        self.assertEqual((status, body["status"]), (202, "registered"))

    async def test_a_request_is_confined_to_its_workspace(self) -> None:
        acme = await self.workspace("Acme", "acme-pass")
        upwork = await self.workspace("Upwork", "upwork-pass")
        await self.call("POST", f"/{acme}/enrol", {"worker_id": "scout-1", "token": "shared-token"})
        await self.call("POST", f"/{acme}/api/pending/scout-1", token="acme-pass")

        _, pending = await self.call("GET", f"/{upwork}/api/pending", token="upwork-pass")
        self.assertEqual(pending, [])
        # Approved in Acme, unknown in Upwork.
        status, body = await self.call("GET", f"/{upwork}/messages", token="shared-token")
        self.assertEqual((status, body["status"]), (401, "unregistered"))

    async def test_two_workers_cannot_share_one_token(self) -> None:
        """A token is the whole of a worker's identity: it is all that arrives
        with a request. Two workers holding one would be one worker to this
        server, sharing channels and a cursor, so whichever acknowledged first
        would quietly consume the other's messages."""
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/enrol", {"worker_id": "first", "token": "shared"})
        await self.call("POST", f"/{ws}/api/pending/first", token="hunter2")

        status, body = await self.call("POST", f"/{ws}/enrol",
                                       {"worker_id": "second", "token": "shared"})
        self.assertEqual(status, 409)
        self.assertIn("first", body["detail"])
        self.assertIn("own", body["detail"])

    async def test_a_clash_at_approval_keeps_the_request_and_says_why(self) -> None:
        """The token can be taken between asking and being approved. Silently
        discarding the request then reads as "nothing waiting", which says
        nothing about what to do."""
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/enrol", {"worker_id": "second", "token": "shared"})
        # The same token is registered to somebody else meanwhile.
        await self.call("POST", f"/{ws}/enrol", {"worker_id": "first", "token": "shared2"})
        await self.call("POST", f"/{ws}/api/pending/first", token="hunter2")
        await self.call("DELETE", f"/{ws}/api/workers/first", token="hunter2")
        await self.call("POST", f"/{ws}/api/workers", {"worker_id": "holder"}, token="hunter2")
        _, rows = await self.call("GET", f"/{ws}/api/pending", token="hunter2")
        self.assertEqual([r["worker_id"] for r in rows], ["second"])

        # Approving the waiting one works, since nothing else holds its token.
        status, _ = await self.call("POST", f"/{ws}/api/pending/second", token="hunter2")
        self.assertEqual(status, 200)

    async def test_a_worker_is_online_while_it_keeps_in_touch(self) -> None:
        """Nothing stays connected here: a worker holds one request and opens
        the next when it returns. Online therefore means heard from lately,
        and a worker that has never called is not online however healthy it
        is elsewhere."""
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/enrol", {"worker_id": "quiet", "token": "quiet-token"})
        await self.call("POST", f"/{ws}/api/pending/quiet", token="hunter2")
        await self.call("POST", f"/{ws}/enrol", {"worker_id": "busy", "token": "busy-token"})
        await self.call("POST", f"/{ws}/api/pending/busy", token="hunter2")

        _, before = await self.call("GET", f"/{ws}/api/workers", token="hunter2")
        self.assertEqual({w["worker_id"]: w["online"] for w in before},
                         {"busy": False, "quiet": False})

        # Asking for messages is being in touch.
        await self.call("GET", f"/{ws}/messages", token="busy-token")
        _, after = await self.call("GET", f"/{ws}/api/workers", token="hunter2")
        seen = {w["worker_id"]: w["online"] for w in after}
        self.assertTrue(seen["busy"], "a worker that just polled should be online")
        self.assertFalse(seen["quiet"], "a worker that never called should not be")

    # --- delivery ---------------------------------------------------------
    async def test_a_worker_polls_acks_and_does_not_see_it_again(self) -> None:
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/api/channels", {"name": "jobs"}, token="hunter2")
        await self.call("POST", f"/{ws}/enrol", {"worker_id": "mailer", "token": "mailer-token"})
        await self.call("POST", f"/{ws}/api/pending/mailer", token="hunter2")
        await self.call("PUT", f"/{ws}/api/channels/jobs/members/mailer", {}, token="hunter2")
        await self.call("POST", f"/{ws}/api/channels/jobs/messages", {"body": {"n": 1}}, token="hunter2")

        status, body = await self.call("GET", f"/{ws}/messages", token="mailer-token")
        self.assertEqual(status, 200)
        self.assertEqual([m["body"] for m in body["messages"]], [{"n": 1}])

        seq = body["messages"][0]["seq"]
        await self.call("POST", f"/{ws}/ack", {"channel": "jobs", "seq": seq}, token="mailer-token")
        _, body = await self.call("GET", f"/{ws}/messages", token="mailer-token")
        self.assertEqual(body["messages"], [])

    async def test_an_unacked_message_comes_back(self) -> None:
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/api/channels", {"name": "jobs"}, token="hunter2")
        await self.call("POST", f"/{ws}/enrol", {"worker_id": "mailer", "token": "t"})
        await self.call("POST", f"/{ws}/api/pending/mailer", token="hunter2")
        await self.call("PUT", f"/{ws}/api/channels/jobs/members/mailer", {}, token="hunter2")
        await self.call("POST", f"/{ws}/api/channels/jobs/messages", {"body": 1}, token="hunter2")
        first, _ = await self.call("GET", f"/{ws}/messages", token="t")
        second, body = await self.call("GET", f"/{ws}/messages", token="t")
        self.assertEqual(first, 200)
        self.assertEqual(len(body["messages"]), 1)

    async def test_a_wait_returns_as_soon_as_something_arrives(self) -> None:
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/api/channels", {"name": "jobs"}, token="hunter2")
        await self.call("POST", f"/{ws}/enrol", {"worker_id": "mailer", "token": "t"})
        await self.call("POST", f"/{ws}/api/pending/mailer", token="hunter2")
        await self.call("PUT", f"/{ws}/api/channels/jobs/members/mailer", {}, token="hunter2")

        async def post_soon() -> None:
            await asyncio.sleep(1.0)
            await self.call("POST", f"/{ws}/api/channels/jobs/messages", {"body": "late"}, token="hunter2")

        started = time.monotonic()
        poster = asyncio.create_task(post_soon())
        status, body = await self.call("GET", f"/{ws}/messages?wait=20", token="t")
        took = time.monotonic() - started
        await poster
        self.assertEqual(status, 200)
        self.assertEqual([m["body"] for m in body["messages"]], ["late"])
        # Published at t=1. The publish itself wakes the waiting request, so
        # this returns just after that rather than on the next check.
        self.assertLess(took, 1.6, f"woken {took - 1:.2f}s after the message was published")

    async def test_a_publish_does_not_wake_another_workspace(self) -> None:
        """Two workspaces can both have a channel called jobs. A worker waiting
        in one must not be woken by the other, or a busy neighbour would keep
        it querying for messages that are not its own."""
        acme = await self.workspace("Acme", "acme-pass")
        other = await self.workspace("Upwork", "upwork-pass")
        for ws, password in ((acme, "acme-pass"), (other, "upwork-pass")):
            await self.call("POST", f"/{ws}/api/channels", {"name": "jobs"}, token=password)
            await self.call("POST", f"/{ws}/enrol", {"worker_id": "scout", "token": f"{ws}-token"})
            await self.call("POST", f"/{ws}/api/pending/scout", token=password)
            await self.call("PUT", f"/{ws}/api/channels/jobs/members/scout", {}, token=password)

        async def post_elsewhere() -> None:
            await asyncio.sleep(0.5)
            await self.call("POST", f"/{other}/api/channels/jobs/messages",
                            {"body": "not yours"}, token="upwork-pass")

        poster = asyncio.create_task(post_elsewhere())
        status, body = await self.call("GET", f"/{acme}/messages?wait=2", token=f"{acme}-token")
        await poster
        self.assertEqual((status, body["messages"]), (200, []))

    async def test_one_worker_publishing_wakes_every_other_member(self) -> None:
        """What a channel is for. A publish reaches each other member at once,
        each with its own copy: this is a channel, not a queue, so two workers
        waiting on it do not race for the same message and neither takes it
        away from the other. The publisher is not told its own news.
        """
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/api/channels", {"name": "github"}, token="hunter2")
        for who in ("reporter", "builder", "notifier"):
            await self.call("POST", f"/{ws}/enrol", {"worker_id": who, "token": f"{who}-token"})
            await self.call("POST", f"/{ws}/api/pending/{who}", token="hunter2")
            await self.call("PUT", f"/{ws}/api/channels/github/members/{who}", {}, token="hunter2")

        async def publish_soon() -> None:
            await asyncio.sleep(0.5)
            await self.call("POST", f"/{ws}/publish",
                            {"channel": "github", "body": {"event": "push", "ref": "main"}},
                            token="reporter-token")

        started = time.monotonic()
        poster = asyncio.create_task(publish_soon())
        # Both other members wait at the same time.
        listeners = [self.call("GET", f"/{ws}/messages?wait=20", token=f"{who}-token")
                     for who in ("builder", "notifier")]
        answers = await asyncio.gather(*listeners)
        woken = time.monotonic() - started
        await poster

        for status, body in answers:
            self.assertEqual(status, 200)
            self.assertEqual([m["body"] for m in body["messages"]],
                             [{"event": "push", "ref": "main"}],
                             f"{body['worker_id']} did not get its own copy")
            self.assertEqual(body["messages"][0]["sender"], "reporter")
        self.assertLess(woken - 0.5, 1.0, f"woken {woken - 0.5:.2f}s after the publish")

        # The one that published is not told about its own message.
        status, mine = await self.call("GET", f"/{ws}/messages?wait=1", token="reporter-token")
        self.assertEqual((status, mine["messages"]), (200, []))

    async def test_delivery_says_who_is_behind(self) -> None:
        """Whether something reached a worker is otherwise unanswerable from
        outside. A worker that never joined receives nothing and says nothing,
        and one that is asleep looks exactly like one that is up to date."""
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/api/channels", {"name": "github"}, token="hunter2")
        for who in ("reporter", "listener", "stranger"):
            await self.call("POST", f"/{ws}/enrol", {"worker_id": who, "token": f"{who}-token"})
            await self.call("POST", f"/{ws}/api/pending/{who}", token="hunter2")
        for who in ("reporter", "listener"):
            await self.call("PUT", f"/{ws}/api/channels/github/members/{who}", {}, token="hunter2")

        await self.call("POST", f"/{ws}/publish", {"channel": "github", "body": {"n": 1}},
                        token="reporter-token")
        status, rows = await self.call("GET", f"/{ws}/api/channels/github/delivery", token="hunter2")
        self.assertEqual(status, 200)
        behind = {r["name"]: r["waiting"] for r in rows}
        # The listener has it waiting; the sender is not owed its own message;
        # the one that never joined is not there at all, which is the answer.
        self.assertEqual(behind, {"listener": 1, "reporter": 0})

        # Once it collects and acknowledges, it is caught up.
        _, got = await self.call("GET", f"/{ws}/messages", token="listener-token")
        await self.call("POST", f"/{ws}/ack",
                        {"channel": "github", "seq": got["messages"][0]["seq"]},
                        token="listener-token")
        _, rows = await self.call("GET", f"/{ws}/api/channels/github/delivery", token="hunter2")
        self.assertEqual({r["name"]: r["waiting"] for r in rows}, {"listener": 0, "reporter": 0})

    async def test_skipping_a_backlog_starts_a_member_from_now(self) -> None:
        """A worker away long enough has a stack of stale news to read before
        it reaches anything current. Skipping passes over it without touching
        the channel: the messages stay, and everyone else still gets them."""
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/api/channels", {"name": "github"}, token="hunter2")
        for who in ("reporter", "away", "other"):
            await self.call("POST", f"/{ws}/enrol", {"worker_id": who, "token": f"{who}-token"})
            await self.call("POST", f"/{ws}/api/pending/{who}", token="hunter2")
            await self.call("PUT", f"/{ws}/api/channels/github/members/{who}", {}, token="hunter2")

        for n in range(8):
            await self.call("POST", f"/{ws}/publish", {"channel": "github", "body": {"n": n}},
                            token="reporter-token")

        status, done = await self.call("POST", f"/{ws}/api/channels/github/skip/away",
                                       token="hunter2")
        self.assertEqual((status, done["skipped"]), (200, 8))

        # Nothing waiting for it, and nothing waiting is nothing delivered.
        status, got = await self.call("GET", f"/{ws}/messages?wait=1", token="away-token")
        self.assertEqual((status, got["messages"]), (200, []))

        # What is posted next does reach it.
        await self.call("POST", f"/{ws}/publish", {"channel": "github", "body": {"n": "new"}},
                        token="reporter-token")
        _, got = await self.call("GET", f"/{ws}/messages?wait=5", token="away-token")
        self.assertEqual([m["body"] for m in got["messages"]], [{"n": "new"}])

        # The other member was not skipped, and the channel still holds all nine.
        _, rows = await self.call("GET", f"/{ws}/api/channels/github/delivery", token="hunter2")
        self.assertEqual({r["name"]: r["waiting"] for r in rows}["other"], 9)
        _, history = await self.call("GET", f"/{ws}/api/channels/github/messages", token="hunter2")
        self.assertEqual(len(history), 9)

    async def test_resending_gives_a_member_the_last_message_again(self) -> None:
        """Answering "did that actually arrive". Nothing is republished: the
        member's place is put back, so the relay sends it what it already had,
        and nobody else is touched."""
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/api/channels", {"name": "github"}, token="hunter2")
        for who in ("reporter", "listener", "other"):
            await self.call("POST", f"/{ws}/enrol", {"worker_id": who, "token": f"{who}-token"})
            await self.call("POST", f"/{ws}/api/pending/{who}", token="hunter2")
            await self.call("PUT", f"/{ws}/api/channels/github/members/{who}", {}, token="hunter2")

        for body in ({"n": 1}, {"jobId": "e5c5f515", "status": "done"}):
            await self.call("POST", f"/{ws}/publish", {"channel": "github", "body": body},
                            token="reporter-token")
        _, got = await self.call("GET", f"/{ws}/messages?wait=5", token="listener-token")
        await self.call("POST", f"/{ws}/ack",
                        {"channel": "github", "seq": got["messages"][-1]["seq"]},
                        token="listener-token")
        _, nothing = await self.call("GET", f"/{ws}/messages?wait=1", token="listener-token")
        self.assertEqual(nothing["messages"], [])

        status, again = await self.call("POST", f"/{ws}/api/channels/github/resend/listener",
                                        token="hunter2")
        self.assertEqual((status, again["resending"]), (200, 1))

        # The last one, once, and it is the same message rather than a copy.
        _, back = await self.call("GET", f"/{ws}/messages?wait=5", token="listener-token")
        self.assertEqual([m["body"] for m in back["messages"]],
                         [{"jobId": "e5c5f515", "status": "done"}])
        self.assertEqual(back["messages"][0]["seq"], got["messages"][-1]["seq"])

        # The channel still holds two, and the other member is unaffected.
        _, history = await self.call("GET", f"/{ws}/api/channels/github/messages", token="hunter2")
        self.assertEqual(len(history), 2)
        _, rows = await self.call("GET", f"/{ws}/api/channels/github/delivery", token="hunter2")
        self.assertEqual({r["name"]: r["waiting"] for r in rows}["other"], 2)

    async def test_a_wait_with_nothing_to_say_ends_empty(self) -> None:
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/enrol", {"worker_id": "idle", "token": "t"})
        await self.call("POST", f"/{ws}/api/pending/idle", token="hunter2")
        started = time.monotonic()
        status, body = await self.call("GET", f"/{ws}/messages?wait=2", token="t")
        self.assertEqual((status, body["messages"]), (200, []))
        self.assertGreaterEqual(time.monotonic() - started, 1.5)

    async def test_a_publisher_cannot_reach_a_channel_it_is_not_in(self) -> None:
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/api/channels", {"name": "secret"}, token="hunter2")
        await self.call("POST", f"/{ws}/enrol", {"worker_id": "scout", "token": "t"})
        await self.call("POST", f"/{ws}/api/pending/scout", token="hunter2")
        missing, _ = await self.call("POST", f"/{ws}/publish", {"channel": "nope", "body": 1}, token="t")
        not_member, _ = await self.call("POST", f"/{ws}/publish", {"channel": "secret", "body": 1}, token="t")
        self.assertEqual((missing, not_member), (403, 403))

    async def test_a_retried_publish_is_stored_once(self) -> None:
        ws = await self.workspace()
        await self.call("POST", f"/{ws}/api/channels", {"name": "jobs"}, token="hunter2")
        await self.call("POST", f"/{ws}/enrol", {"worker_id": "bot", "token": "t"})
        await self.call("POST", f"/{ws}/api/pending/bot", token="hunter2")
        await self.call("PUT", f"/{ws}/api/channels/jobs/members/bot", {}, token="hunter2")
        msg = {"channel": "jobs", "id": "gmail-1:0", "body": {"n": 1}}
        _, first = await self.call("POST", f"/{ws}/publish", msg, token="t")
        _, retry = await self.call("POST", f"/{ws}/publish", msg, token="t")
        self.assertFalse(first["duplicate"])
        self.assertTrue(retry["duplicate"])
        self.assertEqual(first["seq"], retry["seq"])


class UnconfiguredTest(unittest.IsolatedAsyncioTestCase):
    """A deployment with no database must still start.

    Refusing to start is the one failure a platform cannot explain: the
    browser is told the function crashed, which says nothing about what to
    fix. So the app comes up, serves its console, and every endpoint says
    what is missing.
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.app = LiveApp.__new__(LiveApp)
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            cls.app.port = s.getsockname()[1]
        cls.app.url = f"http://127.0.0.1:{cls.app.port}"
        config = uvicorn.Config(create_app(None), host="127.0.0.1", port=cls.app.port,
                                log_level="warning", timeout_graceful_shutdown=2)
        cls.app.server = uvicorn.Server(config)
        cls.app.thread = threading.Thread(target=cls.app.server.run, daemon=True)
        cls.app.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.app.stop()

    async def call(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        return await ServerlessTest.call(self, method, path, body, token="anything")  # type: ignore[arg-type]

    async def test_the_console_is_still_served(self) -> None:
        """A deployment with no database still starts and still serves its
        console, which is the only thing able to say what is missing."""
        status, body = await self.call("GET", "/api/workspaces")
        self.assertEqual((status, body["status"]), (503, "unconfigured"))
        self.assertIn("DATABASE_URL", body["message"])

    async def test_every_endpoint_says_what_is_missing(self) -> None:
        for method, path, payload in (
            ("POST", "/api/workspaces", {"name": "Acme", "password": "p"}),
            ("GET", "/acme/api/channels", None),
            ("POST", "/acme/enrol", {"worker_id": "w", "token": "t"}),
            ("GET", "/acme/messages", None),
        ):
            status, body = await self.call(method, path, payload)
            self.assertEqual(status, 503, f"{method} {path}")
            self.assertEqual(body["status"], "unconfigured", f"{method} {path}")


class RedactTest(unittest.TestCase):
    def test_a_connection_string_never_reaches_the_reader(self) -> None:
        from relay.serverless import redact
        said = redact('could not connect to "postgresql://someone:sup3rsecret@host/db" after 30s')
        self.assertNotIn("sup3rsecret", said)
        self.assertNotIn("someone", said)
        self.assertIn("30s", said)


class UnreachableDatabaseTest(unittest.IsolatedAsyncioTestCase):
    """A database that is configured but not answering.

    With no endpoint left to ask about the database's health, the ordinary
    requests have to carry that news themselves. An unhandled driver error
    would reach the caller as a bare 500, which says nothing about which part
    is unwell, and would put the connection string in the server's logs.
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.app = LiveApp.__new__(LiveApp)
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            cls.app.port = s.getsockname()[1]
        cls.app.url = f"http://127.0.0.1:{cls.app.port}"
        store = PgStore("postgresql://someone:sup3rsecret@127.0.0.1:1/nope", max_size=1)
        store.pool.timeout = 2                       # fail fast rather than wait
        config = uvicorn.Config(create_app(store), host="127.0.0.1", port=cls.app.port,
                                log_level="critical", timeout_graceful_shutdown=2)
        cls.app.server = uvicorn.Server(config)
        cls.app.thread = threading.Thread(target=cls.app.server.run, daemon=True)
        cls.app.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.app.stop()

    async def call(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        return await ServerlessTest.call(self, method, path, body, token="anything")  # type: ignore[arg-type]

    async def test_a_request_says_the_database_is_unreachable(self) -> None:
        status, body = await self.call("GET", "/api/workspaces")
        self.assertEqual(status, 503)
        self.assertEqual(body["status"], "database_unreachable")

    async def test_the_connection_string_is_not_in_the_answer(self) -> None:
        _, body = await self.call("GET", "/api/workspaces")
        said = json.dumps(body)
        self.assertNotIn("sup3rsecret", said)
        self.assertNotIn("someone", said)


class FakeTelegram(threading.Thread):
    """Stands in for api.telegram.org, and records what it was asked to send."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.sent: list[dict[str, Any]] = []
        self.reject: set[str] = set()          # chat ids to refuse permanently
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}"
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                payload = json.loads(self.rfile.read(length) or b"{}")
                if self.path.endswith("/getMe"):
                    if "bad" in self.path:
                        return self.reply(401, {"ok": False, "description": "Unauthorized"})
                    return self.reply(200, {"ok": True, "result": {"username": "test_bot"}})
                if str(payload.get("chat_id")) in outer.reject:
                    return self.reply(400, {"ok": False, "description": "chat not found"})
                outer.sent.append(payload)
                self.reply(200, {"ok": True, "result": {"message_id": len(outer.sent)}})

            def reply(self, code: int, body: Any) -> None:
                raw = json.dumps(body).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *args: Any) -> None:
                pass

        self.server = HTTPServer(("127.0.0.1", self.port), Handler)

    def run(self) -> None:
        self.server.serve_forever()

    def stop(self) -> None:
        self.server.shutdown()


class BotDeliveryTest(unittest.IsolatedAsyncioTestCase):
    """A Telegram chat registered on a channel, delivered to by the server.

    Nothing connects and nothing is approved: registering is administration,
    like adding a member, and the server does the sending.
    """

    @classmethod
    def setUpClass(cls) -> None:
        assert _server is not None
        cls.telegram = FakeTelegram()
        cls.telegram.start()
        cls.store = PgStore(_server.get_uri(), min_size=1, max_size=8)
        cls.notifier = Notifier(_server.get_uri())
        cls.sender = BotSender(cls.store, cls.notifier, api=cls.telegram.url, idle=0.2)
        cls.app = LiveApp.__new__(LiveApp)
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            cls.app.port = s.getsockname()[1]
        cls.app.url = f"http://127.0.0.1:{cls.app.port}"
        app = create_app(cls.store, cls.notifier, cls.sender)
        # The route checks the token against Telegram; point it at the stand-in.
        telegram_module.API = cls.telegram.url
        config = uvicorn.Config(app, host="127.0.0.1", port=cls.app.port,
                                log_level="warning", timeout_graceful_shutdown=2)
        cls.app.server = uvicorn.Server(config)
        cls.app.thread = threading.Thread(target=cls.app.server.run, daemon=True)
        cls.app.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.app.stop()
        cls.telegram.stop()
        telegram_module.API = "https://api.telegram.org"

    def setUp(self) -> None:
        assert _server is not None
        _server.psql("TRUNCATE workspaces CASCADE;")
        self.telegram.sent.clear()
        self.telegram.reject.clear()

    async def call(self, method: str, path: str, body: Any = None, token: str | None = None
                   ) -> tuple[int, Any]:
        return await ServerlessTest.call(self, method, path, body, token)  # type: ignore[arg-type]

    async def ready(self) -> str:
        status, body = await self.call("POST", "/api/workspaces",
                                       {"name": "Acme", "password": "p"})
        self.assertEqual(status, 201, body)
        await self.call("POST", "/acme/api/channels", {"name": "jobs"}, token="p")
        return "acme"

    async def register(self, name: str, chat_id: str, token: str = "1:abc",
                       channel: str = "jobs", join: bool = True) -> None:
        """Register a bot and put it in a channel. Creating it sends a welcome,
        which is cleared so a test counts only what it published."""
        status, body = await self.call("POST", "/acme/api/bots",
                                       {"name": name, "chat_id": chat_id, "token": token},
                                       token="p")
        assert status == 201, body
        if join:
            status, _ = await self.call("PUT", f"/acme/api/channels/{channel}/bots/{name}",
                                        {"from_start": False}, token="p")
            assert status == 200
        self.telegram.sent.clear()

    async def eventually(self, count: int, within: float = 8.0) -> None:
        deadline = time.monotonic() + within
        while time.monotonic() < deadline:
            if len(self.telegram.sent) >= count:
                return
            await asyncio.sleep(0.05)
        self.fail(f"expected {count} sent, saw {len(self.telegram.sent)}")

    async def test_a_bot_is_registered_without_anyone_approving_it(self) -> None:
        await self.ready()
        status, body = await self.call("POST", "/acme/api/bots",
                                       {"name": "phone", "chat_id": "555", "token": "1:abc"},
                                       token="p")
        self.assertEqual((status, body["name"]), (201, "phone"))
        # Creating it proved it, by sending to it.
        self.assertEqual(len(self.telegram.sent), 1)
        self.assertIn("Connected", self.telegram.sent[0]["text"])
        # Nothing is waiting: a bot is not a worker.
        _, pending = await self.call("GET", "/acme/api/pending", token="p")
        self.assertEqual(pending, [])

    async def test_the_server_sends_what_arrives(self) -> None:
        await self.ready()
        await self.register("phone", "555", "1:abc")
        await self.call("POST", "/acme/api/channels/jobs/messages",
                        {"body": {"type": "job", "title": "Build a scraper", "budget": "$500"}},
                        token="p")
        await self.eventually(1)
        said = self.telegram.sent[0]
        self.assertEqual(said["chat_id"], "555")
        self.assertIn("Build a scraper", said["text"])
        self.assertIn("$500", said["text"])

    async def test_nothing_is_sent_twice(self) -> None:
        await self.ready()
        await self.register("phone", "555", "1:abc")
        for n in range(3):
            await self.call("POST", "/acme/api/channels/jobs/messages", {"body": {"n": n}},
                            token="p")
        await self.eventually(3)
        await asyncio.sleep(1.0)              # let the loop go round again
        self.assertEqual(len(self.telegram.sent), 3)

    async def test_history_before_registering_is_not_sent(self) -> None:
        """Registering is joining, and joining a channel is not a request for
        everything it ever carried."""
        await self.ready()
        await self.call("POST", "/acme/api/channels/jobs/messages", {"body": "old"}, token="p")
        await self.register("phone", "555", "1:abc")
        await self.call("POST", "/acme/api/channels/jobs/messages", {"body": "new"}, token="p")
        await self.eventually(1)
        await asyncio.sleep(0.6)
        self.assertEqual(len(self.telegram.sent), 1)
        self.assertIn("new", self.telegram.sent[0]["text"])

    async def test_a_removed_bot_stops_receiving(self) -> None:
        await self.ready()
        await self.register("phone", "555", "1:abc")
        status, _ = await self.call("DELETE", "/acme/api/channels/jobs/bots/phone", token="p")
        self.assertEqual(status, 200)
        await self.call("POST", "/acme/api/channels/jobs/messages", {"body": "after"}, token="p")
        await asyncio.sleep(1.5)
        self.assertEqual(self.telegram.sent, [])
        _, bots = await self.call("GET", "/acme/api/channels/jobs/bots", token="p")
        self.assertEqual(bots, [])

    async def test_a_listing_never_carries_the_token(self) -> None:
        await self.ready()
        await self.register("phone", "555", "sup3rsecret")
        _, bots = await self.call("GET", "/acme/api/channels/jobs/bots", token="p")
        self.assertEqual(bots[0]["name"], "phone")
        self.assertNotIn("sup3rsecret", json.dumps(bots))

    async def test_one_undeliverable_message_does_not_block_the_rest(self) -> None:
        """A chat that refuses a message refuses it for ever. Retrying would
        stop every later alert behind one that can never go."""
        await self.ready()
        await self.register("gone", "404", "1:abc")
        await self.register("phone", "555", "1:abc")
        self.telegram.reject.add("404")
        await self.call("POST", "/acme/api/channels/jobs/messages", {"body": "one"}, token="p")
        await self.call("POST", "/acme/api/channels/jobs/messages", {"body": "two"}, token="p")
        await self.eventually(2)
        self.assertEqual({s["chat_id"] for s in self.telegram.sent}, {"555"})

    async def test_a_bad_pair_is_refused_and_nothing_is_created(self) -> None:
        await self.ready()
        status, body = await self.call(
            "POST", "/acme/api/bots",
            {"name": "phone", "chat_id": "555", "token": "bad"}, token="p")
        self.assertEqual((status, body["status"]), (400, "invalid_bot"))
        self.assertIn("Telegram", body["message"])
        # Nothing was stored, so a bad pair leaves no half-made bot behind.
        _, bots = await self.call("GET", "/acme/api/bots", token="p")
        self.assertEqual(bots, [])

    async def test_a_good_token_with_a_wrong_chat_is_refused(self) -> None:
        """The failure worth catching. A bad token is obvious; a chat id that
        is one digit out looks fine and then silently delivers to nobody, so
        creating a bot sends to it rather than only asking whether the token
        is real."""
        await self.ready()
        self.telegram.reject.add("999")
        status, body = await self.call("POST", "/acme/api/bots",
                                       {"name": "typo", "chat_id": "999", "token": "1:abc"},
                                       token="p")
        self.assertEqual((status, body["status"]), (400, "invalid_bot"))
        _, bots = await self.call("GET", "/acme/api/bots", token="p")
        self.assertEqual(bots, [])

    async def test_bots_belong_to_their_channel(self) -> None:
        await self.ready()
        await self.call("POST", "/acme/api/channels", {"name": "quiet"}, token="p")
        await self.register("phone", "555", "1:abc")
        await self.call("POST", "/acme/api/channels/quiet/messages", {"body": "elsewhere"},
                        token="p")
        await asyncio.sleep(1.2)
        self.assertEqual(self.telegram.sent, [])
