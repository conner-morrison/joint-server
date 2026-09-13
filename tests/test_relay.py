"""End-to-end tests against a real uvicorn server on a free local port."""
from __future__ import annotations

import asyncio
import json
import os
import socket
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from typing import Any

import uvicorn
import websockets

from relay.app import create_app
from relay.client import AuthError, Message, PublishError, RelayClient
from relay.store import Store

ADMIN = "admin-test-token"


class StoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store(":memory:")
        self.store.add_channel("jobs")
        self.token = self.store.add_worker("alice")
        self.store.add_worker("bob")

    def tearDown(self) -> None:
        self.store.close()

    def test_token_resolves_and_is_not_stored(self) -> None:
        self.assertEqual(self.store.worker_for_token(self.token), "alice")
        self.assertIsNone(self.store.worker_for_token("nope"))
        raw = self.store._all("SELECT token_hash FROM workers")
        self.assertNotIn(self.token, [r["token_hash"] for r in raw])

    def test_names_are_validated(self) -> None:
        for bad in ("", "@server", "has space", "x" * 65):
            with self.assertRaises(ValueError):
                self.store.add_worker(bad)
        with self.assertRaises(ValueError):
            self.store.add_worker("alice")

    def test_join_starts_at_head_unless_from_start(self) -> None:
        self.store.join("jobs", "alice")
        self.store.append("jobs", "alice", '{"n":1}')
        self.store.join("jobs", "bob")
        self.assertEqual(self.store.channels_of("bob"), {"jobs": 1})
        self.store.leave("jobs", "bob")
        self.store.join("jobs", "bob", from_start=True)
        self.assertEqual(self.store.channels_of("bob"), {"jobs": 0})

    def test_ack_never_moves_back_or_past_the_log(self) -> None:
        self.store.join("jobs", "bob")
        self.store.append("jobs", "alice", "1")
        self.store.append("jobs", "alice", "2")
        self.store.ack("jobs", "bob", 99)
        self.assertEqual(self.store.channels_of("bob")["jobs"], 2)
        self.store.ack("jobs", "bob", 1)
        self.assertEqual(self.store.channels_of("bob")["jobs"], 2)

    def test_resend_with_same_client_id_is_stored_once(self) -> None:
        self.assertEqual(self.store.append("jobs", "alice", "1", "c1"), (1, False))
        self.assertEqual(self.store.append("jobs", "alice", "1", "c1"), (1, True))
        self.assertEqual(len(self.store.messages_after("jobs", 0)), 1)

    def test_prune_keeps_head(self) -> None:
        self.store.append("jobs", "alice", "1")
        self.assertEqual(self.store.prune(time.time() + 1), 1)
        self.assertEqual(self.store.head(), 1)


class LiveServer:
    def __init__(self, store: Store, **app_kw: Any):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}"
        config = uvicorn.Config(create_app(store, ADMIN, **app_kw), host="127.0.0.1", port=self.port,
                                log_level="warning", timeout_graceful_shutdown=2)
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> None:
        self.thread.start()
        deadline = time.time() + 10
        while not self.server.started:
            if time.time() > deadline:
                raise RuntimeError("server did not start")
            time.sleep(0.02)

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(10)


class RelayTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(os.path.join(self.tmp.name, "relay.db"))
        self.store.add_channel("jobs")
        self.tokens = {w: self.store.add_worker(w) for w in ("alice", "bob", "carol")}
        self.store.join("jobs", "alice")
        self.store.join("jobs", "bob")
        self.srv = LiveServer(self.store, cors_origins=["https://console.example.com", "https://relay-*.vercel.app"])
        self.srv.start()
        self.ws_url = f"ws://127.0.0.1:{self.srv.port}/ws"
        self.running: list[tuple[RelayClient, asyncio.Task[None]]] = []

    async def asyncTearDown(self) -> None:
        for client, task in self.running:
            await client.close()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def tearDown(self) -> None:
        self.srv.stop()
        self.store.close()
        self.tmp.cleanup()

    # --- helpers -------------------------------------------------------------
    def client(self, worker: str) -> tuple[RelayClient, asyncio.Queue[Message]]:
        client = RelayClient(self.srv.url, self.tokens[worker])
        got: asyncio.Queue[Message] = asyncio.Queue()

        @client.on()
        async def collect(msg: Message) -> None:
            await got.put(msg)

        return client, got

    async def connect(self, worker: str) -> tuple[RelayClient, asyncio.Queue[Message]]:
        client, got = self.client(worker)
        self.running.append((client, asyncio.create_task(client.run())))
        await asyncio.wait_for(client.connected.wait(), 5)
        return client, got

    async def raw(self, worker: str) -> Any:
        ws = await websockets.connect(self.ws_url, additional_headers={"Authorization": f"Bearer {self.tokens[worker]}"})
        welcome = json.loads(await asyncio.wait_for(ws.recv(), 5))
        self.assertEqual(welcome["type"], "welcome")
        return ws

    async def api(self, method: str, path: str, body: Any = None, token: str | None = ADMIN) -> tuple[int, Any]:
        def call() -> tuple[int, Any]:
            headers = {"Content-Type": "application/json"}
            if token:
                headers["Authorization"] = f"Bearer {token}"
            data = json.dumps(body).encode() if body is not None else None
            req = urllib.request.Request(self.srv.url + path, data=data, method=method, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=5) as r:
                    return r.status, json.loads(r.read() or b"null")
            except urllib.error.HTTPError as e:
                with e:
                    return e.code, json.loads(e.read() or b"null")
        return await asyncio.to_thread(call)

    async def nothing_arrives(self, q: asyncio.Queue[Message], wait: float = 0.3) -> None:
        await asyncio.sleep(wait)
        self.assertTrue(q.empty(), f"unexpected: {q.get_nowait() if not q.empty() else ''}")

    # --- tests ---------------------------------------------------------------
    async def test_members_receive_but_sender_and_outsiders_do_not(self) -> None:
        alice, alice_q = await self.connect("alice")
        bob, bob_q = await self.connect("bob")
        _, carol_q = await self.connect("carol")

        seq = await alice.publish("jobs", {"id": 1})
        msg = await asyncio.wait_for(bob_q.get(), 5)
        self.assertEqual((msg.channel, msg.seq, msg.sender, msg.body), ("jobs", seq, "alice", {"id": 1}))

        await bob.publish("jobs", {"reply": 1})
        msg = await asyncio.wait_for(alice_q.get(), 5)
        self.assertEqual(msg.sender, "bob")
        self.assertTrue(alice_q.empty())
        await self.nothing_arrives(carol_q)

    async def test_non_member_cannot_publish(self) -> None:
        carol, _ = await self.connect("carol")
        with self.assertRaises(PublishError) as cm:
            await carol.publish("jobs", {"sneaky": True}, timeout=5)
        self.assertEqual(cm.exception.code, "not_member")

    async def test_offline_member_gets_backlog_in_order(self) -> None:
        alice, _ = await self.connect("alice")
        for n in range(3):
            await alice.publish("jobs", {"n": n})
        _, bob_q = await self.connect("bob")
        got = [(await asyncio.wait_for(bob_q.get(), 5)).body["n"] for _ in range(3)]
        self.assertEqual(got, [0, 1, 2])

    async def test_unacked_message_is_redelivered_and_acked_one_is_not(self) -> None:
        alice, _ = await self.connect("alice")
        seq = await alice.publish("jobs", {"important": True})

        ws = await self.raw("bob")
        first = json.loads(await asyncio.wait_for(ws.recv(), 5))
        self.assertEqual(first["seq"], seq)
        await ws.close()                                        # no ack

        ws = await self.raw("bob")
        again = json.loads(await asyncio.wait_for(ws.recv(), 5))
        self.assertEqual(again["seq"], seq)
        await ws.send(json.dumps({"type": "ack", "channel": "jobs", "seq": seq}))
        await ws.close()

        ws = await self.raw("bob")
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(ws.recv(), 0.3)
        await ws.close()

    async def test_resent_publish_is_stored_once(self) -> None:
        ws = await self.raw("alice")
        frame = json.dumps({"type": "publish", "id": "same-id", "channel": "jobs", "body": 1})
        await ws.send(frame)
        await ws.send(frame)
        a = json.loads(await asyncio.wait_for(ws.recv(), 5))
        b = json.loads(await asyncio.wait_for(ws.recv(), 5))
        await ws.close()
        self.assertEqual(a["seq"], b["seq"])
        self.assertEqual((a["duplicate"], b["duplicate"]), (False, True))
        self.assertEqual(len(self.store.messages_after("jobs", 0)), 1)

    async def test_publish_while_offline_is_sent_on_connect(self) -> None:
        _, bob_q = await self.connect("bob")
        alice, _ = self.client("alice")
        pending = asyncio.create_task(alice.publish("jobs", {"queued": True}))
        await asyncio.sleep(0.1)
        self.assertFalse(pending.done())
        self.running.append((alice, asyncio.create_task(alice.run())))
        seq = await asyncio.wait_for(pending, 5)
        msg = await asyncio.wait_for(bob_q.get(), 5)
        self.assertEqual((msg.seq, msg.body), (seq, {"queued": True}))

    async def test_handler_can_publish_a_reply(self) -> None:
        alice, alice_q = await self.connect("alice")
        bob, _ = self.client("bob")

        @bob.on("jobs")
        async def answer(msg: Message) -> None:
            await bob.publish("jobs", {"done": msg.body["id"]})

        self.running.append((bob, asyncio.create_task(bob.run())))
        await asyncio.wait_for(bob.connected.wait(), 5)
        await alice.publish("jobs", {"id": 7})
        reply = await asyncio.wait_for(alice_q.get(), 5)
        self.assertEqual(reply.body, {"done": 7})

    async def test_bad_token_stops_the_client(self) -> None:
        client = RelayClient(self.srv.url, "not-a-token")
        with self.assertRaises(AuthError):
            await asyncio.wait_for(client.run(), 5)

    async def test_admin_api_requires_token(self) -> None:
        status, _ = await self.api("GET", "/api/channels", token=None)
        self.assertEqual(status, 401)
        status, _ = await self.api("GET", "/api/channels", token="wrong")
        self.assertEqual(status, 401)

    async def test_admin_manages_channels_and_members_live(self) -> None:
        alice, alice_q = await self.connect("alice")

        self.assertEqual((await self.api("POST", "/api/channels", {"name": "results"}))[0], 201)
        status, body = await self.api("PUT", "/api/channels/results/members/alice")
        self.assertEqual((status, body["joined"]), (200, True))
        for _ in range(50):
            if "results" in alice.channels:
                break
            await asyncio.sleep(0.02)
        self.assertIn("results", alice.channels)

        status, body = await self.api("POST", "/api/channels/results/messages", {"body": {"hello": "all"}})
        self.assertEqual(status, 201)
        msg = await asyncio.wait_for(alice_q.get(), 5)
        self.assertEqual((msg.channel, msg.sender, msg.body), ("results", "@server", {"hello": "all"}))

        status, history = await self.api("GET", "/api/channels/results/messages")
        self.assertEqual([m["seq"] for m in history], [body["seq"]])

        status, workers = await self.api("GET", "/api/workers")
        self.assertTrue(next(w for w in workers if w["worker_id"] == "alice")["online"])

    async def test_posting_with_the_same_id_is_stored_once(self) -> None:
        """A sender that cannot tell whether its POST arrived retries with the
        same id; the retry must not post the message a second time."""
        alice, alice_q = await self.connect("alice")
        first = await self.api("POST", "/api/channels/jobs/messages",
                               {"id": "gmail-42:0", "body": {"job": "one"}})
        self.assertEqual((first[0], first[1]["duplicate"]), (201, False))

        retry = await self.api("POST", "/api/channels/jobs/messages",
                               {"id": "gmail-42:0", "body": {"job": "one"}})
        self.assertEqual((retry[0], retry[1]["duplicate"]), (201, True))
        self.assertEqual(retry[1]["seq"], first[1]["seq"])

        msg = await asyncio.wait_for(alice_q.get(), 5)
        self.assertEqual(msg.body, {"job": "one"})
        await self.nothing_arrives(alice_q)

        _, history = await self.api("GET", "/api/channels/jobs/messages")
        self.assertEqual([m["seq"] for m in history], [first[1]["seq"]])

    async def test_posts_without_an_id_are_never_deduplicated(self) -> None:
        a = await self.api("POST", "/api/channels/jobs/messages", {"body": {"n": 1}})
        b = await self.api("POST", "/api/channels/jobs/messages", {"body": {"n": 1}})
        self.assertNotEqual(a[1]["seq"], b[1]["seq"])
        self.assertFalse(b[1]["duplicate"])

    async def test_removed_member_stops_receiving(self) -> None:
        alice, _ = await self.connect("alice")
        bob, bob_q = await self.connect("bob")
        self.assertEqual((await self.api("DELETE", "/api/channels/jobs/members/bob"))[0], 200)
        await alice.publish("jobs", {"after": "removal"})
        await self.nothing_arrives(bob_q)
        self.assertNotIn("jobs", bob.channels)

    async def test_history_pages_backwards_from_newest(self) -> None:
        for n in range(5):
            await self.api("POST", "/api/channels/jobs/messages", {"body": n})
        _, newest = await self.api("GET", "/api/channels/jobs/messages?limit=2")
        self.assertEqual([m["body"] for m in newest], [3, 4])
        _, older = await self.api("GET", f"/api/channels/jobs/messages?before={newest[0]['seq']}&limit=2")
        self.assertEqual([m["body"] for m in older], [1, 2])
        _, oldest = await self.api("GET", "/api/channels/jobs/messages?after=0&limit=2")
        self.assertEqual([m["body"] for m in oldest], [0, 1])

    async def test_cors_allows_configured_origins_only(self) -> None:
        def preflight(origin: str) -> str | None:
            req = urllib.request.Request(self.srv.url + "/api/workers", method="OPTIONS", headers={
                "Origin": origin, "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "authorization"})
            try:
                with urllib.request.urlopen(req, timeout=5) as r:
                    return r.headers.get("access-control-allow-origin")
            except urllib.error.HTTPError as e:
                with e:
                    return e.headers.get("access-control-allow-origin")

        for origin in ("https://console.example.com", "https://relay-git-main-me.vercel.app"):
            self.assertEqual(await asyncio.to_thread(preflight, origin), origin)
        for origin in ("https://evil.example.com", "https://relay-x.vercel.app.evil.com"):
            self.assertIsNone(await asyncio.to_thread(preflight, origin))

    async def test_stream_reports_presence_and_messages(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.srv.port)
        writer.write(f"GET /api/stream HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {ADMIN}\r\n\r\n".encode())
        await writer.drain()
        buf = b""

        async def until(needle: str) -> None:
            nonlocal buf
            while needle.encode() not in buf:
                chunk = await asyncio.wait_for(reader.read(4096), 5)
                self.assertTrue(chunk, "stream closed")
                buf += chunk
            buf = buf[buf.index(needle.encode()) + len(needle):]

        try:
            await until('"kind":"hello"')
            alice, _ = await self.connect("alice")
            await until('"kind":"worker","ts"')
            await until('"worker_id":"alice","online":true')
            await self.api("POST", "/api/channels", {"name": "results"})
            await until('"kind":"changed"')
            seq = await alice.publish("jobs", {"id": 9})
            await until(f'"kind":"message","ts"')
            await until(f'"seq":{seq},"sender":"alice","body":{{"id":9}}')
        finally:
            writer.close()

    async def test_stream_requires_admin_token(self) -> None:
        status, _ = await self.api("GET", "/api/stream", token="wrong")
        self.assertEqual(status, 401)

    async def test_removed_worker_is_disconnected(self) -> None:
        client, _ = self.client("carol")
        task = asyncio.create_task(client.run())
        await asyncio.wait_for(client.connected.wait(), 5)
        self.assertEqual((await self.api("DELETE", "/api/workers/carol"))[0], 200)
        with self.assertRaises(AuthError):
            await asyncio.wait_for(task, 5)


if __name__ == "__main__":
    unittest.main()
