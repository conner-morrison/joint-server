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

from relay.pgstore import PgStore
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
        app = create_app(PgStore(dsn, min_size=1, max_size=8))
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

    async def test_health_and_readiness_agree_when_connected(self) -> None:
        for path in ("/healthz", "/readyz"):
            status, body = await self.call("GET", path)
            self.assertEqual((status, body["ok"], body["database"]), (200, True, "connected"), path)

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
        self.assertLess(took, 10, "the poll should return when the message lands, not at the deadline")

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

    async def test_health_answers_200_even_with_no_database(self) -> None:
        """A platform kills a deployment whose health check fails. Failing this
        one over a missing setting takes down the page that would have named
        the setting, so the deployment disappears for the reason it was trying
        to report."""
        status, body = await self.call("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual((body["ok"], body["database"]), (False, "unconfigured"))
        self.assertIn("DATABASE_URL", body["message"])

    async def test_readiness_is_the_one_that_fails(self) -> None:
        status, body = await self.call("GET", "/readyz")
        self.assertEqual((status, body["ok"]), (503, False))

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
