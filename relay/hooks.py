"""Calling somewhere when a channel gets something.

A worker that can hold a request open is told by being answered. Something
woken by an HTTP call - a scheduled task, a function, anything with an address
and no way to wait - needs the server to go to it instead.

It is a trigger and not a delivery. A trigger keeps a cursor like any other
member, and a call that is answered moves it to the head of the channel: ten
items arriving in a burst is one call saying there is work, not ten calls each
carrying one. What the work is stays in the channel, to be collected by
whatever the call woke up.
"""
from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import os
import socket
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urlsplit

from relay.pgstore import PgStore

log = logging.getLogger("relay.hooks")

TIMEOUT = float(os.environ.get("RELAY_HOOK_TIMEOUT", "15"))
RETRY = float(os.environ.get("RELAY_HOOK_RETRY", "60"))
# A moment between being woken and calling. Ten leads arriving together then
# cost one call rather than ten, because the cursor jumps to the head of the
# channel when the call is answered.
SETTLE = float(os.environ.get("RELAY_HOOK_SETTLE", "2"))
# Off everywhere but a test: a relay anyone may create a workspace on must not
# be a way to make requests inside the network it runs in.
ALLOW_LOCAL = os.environ.get("RELAY_HOOK_ALLOW_LOCAL", "") == "yes"


def check_url(url: str, *, allow_local: bool = False) -> str:
    """Somewhere this server is willing to call.

    Anyone may create a workspace here, so a trigger is an address someone
    else chose. Left unchecked it would make this server a way to reach
    whatever it can reach and nobody else can - the database beside it, a
    cloud instance's metadata, anything listening on localhost.

    The name is resolved now and again when the call is made, so this is a
    guard rather than a proof; it stops the obvious thing, which is what it is
    for.
    """
    said = (url or "").strip()
    parsed = urlsplit(said)
    local = allow_local or ALLOW_LOCAL
    if parsed.scheme != "https" and not (local and parsed.scheme == "http"):
        raise ValueError("a trigger must be an https:// address")
    if not parsed.hostname:
        raise ValueError("a trigger needs a host")
    try:
        found = socket.getaddrinfo(parsed.hostname, None)
    except OSError as exc:
        raise ValueError(f"that address does not resolve: {exc}") from None
    for info in found:
        ip = ipaddress.ip_address(info[4][0])
        if ip.is_loopback and local:
            continue                     # a test's own listener, and nothing else
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            raise ValueError("a trigger cannot point inside the network this server runs in")
    return said


def _call(url: str, method: str, headers: dict[str, str], payload: bytes | None,
          timeout: float) -> tuple[int, str]:
    request = urllib.request.Request(url, data=payload, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as answer:
            return answer.status, ""
    except urllib.error.HTTPError as exc:
        with exc:
            said = exc.read()[:300].decode("utf-8", "replace")
        return exc.code, said
    except Exception as exc:                     # noqa: BLE001 - reported, then retried
        return 0, f"{type(exc).__name__}: {exc}"


class HookSender:
    def __init__(self, store: PgStore, *, timeout: float = TIMEOUT, idle: float = 30.0,
                 batch: int = 20, allow_local: bool = ALLOW_LOCAL,
                 settle: float = SETTLE):
        self.store = store
        self.allow_local = allow_local
        self.timeout = timeout
        self.idle = idle
        self.batch = batch
        self.settle = settle
        self._task: asyncio.Task[None] | None = None
        self._nudge = asyncio.Event()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run(), name="relay-hook-sender")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except BaseException:                # noqa: BLE001 - shutting down
                pass
            self._task = None

    def nudge(self) -> None:
        self._nudge.set()

    async def run(self) -> None:
        while True:
            try:
                fired = await self.once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:             # noqa: BLE001 - reported, then retried
                log.warning("trigger round failed (%s)", type(exc).__name__)
                fired = 0
            if fired:
                continue
            self._nudge.clear()
            try:
                await asyncio.wait_for(self._nudge.wait(), timeout=self.idle)
                # Woken by a publish. A moment's pause before calling turns a
                # burst into one call: whatever else arrives while waiting is
                # covered by the same one.
                await asyncio.sleep(self.settle)
            except (asyncio.TimeoutError, TimeoutError):
                pass

    async def once(self) -> int:
        fired = 0
        for hook in await self.store.hooks_with_work(limit=self.batch, retry=RETRY):
            fired += await self.fire(hook)
        return fired

    async def test(self, hook: dict[str, Any]) -> dict[str, Any]:
        """Call it now and say what came back. Nothing is moved: a test that
        marked the channel as seen would swallow the work it was testing."""
        headers = {"Content-Type": "application/json", **(hook.get("headers") or {})}
        said = hook.get("body")
        if said is None:
            said = {"relay": {"workspace": hook["workspace"], "channel": hook["channel"],
                              "waiting": 0, "seq": 0, "trigger": hook["name"], "test": True}}
        payload = json.dumps(said).encode() if hook["method"] != "GET" else None
        try:
            url = check_url(hook["url"], allow_local=self.allow_local)
        except ValueError as exc:
            return {"ok": False, "status": 0, "error": str(exc)}
        status, error = await asyncio.to_thread(
            _call, url, hook["method"], headers, payload, self.timeout)
        return {"ok": 200 <= status < 300, "status": status, "error": error}

    async def fire(self, hook: dict[str, Any]) -> int:
        head = int(hook["head"] or 0)
        headers = {"Content-Type": "application/json", **(hook.get("headers") or {})}
        # What the publisher sent stays in the channel. This says only that
        # there is something there, because a trigger that carried the work
        # would have to be trusted with it.
        said = hook.get("body")
        if said is None:
            said = {"relay": {"workspace": hook["workspace"], "channel": hook["channel"],
                              "waiting": int(hook["waiting"] or 0), "seq": head,
                              "trigger": hook["name"]}}
        payload = json.dumps(said).encode() if hook["method"] != "GET" else None
        try:
            url = check_url(hook["url"], allow_local=self.allow_local)
        except ValueError as exc:
            await self.store.hook_failed(hook["workspace"], hook["channel"], hook["name"],
                                         0, str(exc))
            return 0
        status, error = await asyncio.to_thread(
            _call, url, hook["method"], headers, payload, self.timeout)
        if 200 <= status < 300:
            await self.store.hook_fired(hook["workspace"], hook["channel"], hook["name"],
                                        head, status)
            return 1
        log.warning("trigger %s on #%s answered %s", hook["name"], hook["channel"], status or "-")
        await self.store.hook_failed(hook["workspace"], hook["channel"], hook["name"],
                                     status, error)
        return 0
