"""HTTP and websocket surface.

Workers connect to /ws with their own token and can do only what their channel
memberships allow. Everything under /api needs the admin token, including
/api/stream, the live event feed the console reads.

Routes that change state are `async def` on purpose. They notify connected
workers and consoles through asyncio primitives, which must be touched from
the event loop thread, and FastAPI runs plain `def` routes in a thread pool.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import secrets
import time
from typing import Any, AsyncIterator, Sequence

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from relay.hub import AUTH_CLOSE, SERVER_SENDER, Hub, RelayError
from relay.store import Store

log = logging.getLogger("relay.app")


class WorkerIn(BaseModel):
    worker_id: str
    label: str = ""


class ChannelIn(BaseModel):
    name: str
    description: str = ""


class JoinIn(BaseModel):
    from_start: bool = False


class PostIn(BaseModel):
    body: Any = None


def _bearer(value: str | None) -> str:
    if value and value[:7].lower() == "bearer ":
        return value[7:].strip()
    return ""


def _sse(event: dict[str, Any]) -> str:
    return "data: " + json.dumps(event, separators=(",", ":"), ensure_ascii=False) + "\n\n"


def cors_rules(origins: Sequence[str]) -> tuple[list[str], str | None]:
    """Split origins into exact matches and one regex for wildcard entries, so
    `https://myconsole-*.vercel.app` covers every Vercel preview deployment."""
    cleaned = [o.strip().rstrip("/") for o in origins if o.strip()]
    exact = [o for o in cleaned if "*" not in o]
    patterns = [re.escape(o).replace(r"\*", "[^/]*") for o in cleaned if "*" in o]
    return exact, ("^(?:" + "|".join(patterns) + ")$") if patterns else None


async def _prune_loop(store: Store, retention_days: float, every: float = 3600.0) -> None:
    while True:
        removed = store.prune(time.time() - retention_days * 86400)
        if removed:
            log.info("pruned %d messages older than %g days", removed, retention_days)
        await asyncio.sleep(every)


def create_app(store: Store, admin_token: str, *, retention_days: float = 7.0,
               max_body_bytes: int = 256_000, cors_origins: Sequence[str] = (),
               keepalive_s: float = 15.0) -> FastAPI:
    if not admin_token:
        raise ValueError("an admin token is required")
    hub = Hub(store, max_body_bytes=max_body_bytes)

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        task = asyncio.create_task(_prune_loop(store, retention_days)) if retention_days > 0 else None
        try:
            yield
        finally:
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    app = FastAPI(title="relay", lifespan=lifespan)
    app.state.hub = hub
    app.state.store = store

    exact, pattern = cors_rules(cors_origins)
    if exact or pattern:
        # Bearer tokens, not cookies, so no credentials mode and no CSRF surface.
        app.add_middleware(CORSMiddleware, allow_origins=exact, allow_origin_regex=pattern,
                           allow_methods=["GET", "POST", "PUT", "DELETE"],
                           allow_headers=["Authorization", "Content-Type"], max_age=600)

    def require_admin(authorization: str | None = Header(default=None)) -> None:
        if not secrets.compare_digest(_bearer(authorization).encode(), admin_token.encode()):
            raise HTTPException(401, "admin token required")

    @app.get("/healthz")
    def healthz() -> dict[str, Any]:
        return {"ok": True, "online": len(hub.sessions)}

    @app.websocket("/ws")
    async def worker_socket(ws: WebSocket) -> None:
        # Query string as a fallback for clients that cannot set headers (browsers).
        token = _bearer(ws.headers.get("authorization")) or ws.query_params.get("token", "")
        worker_id = store.worker_for_token(token) if token else None
        await ws.accept()
        if worker_id is None:
            # Accept-then-close rather than a 403, so the client gets a close
            # code it can recognise and stops retrying.
            await ws.close(code=AUTH_CLOSE, reason="invalid token")
            return
        await hub.serve(worker_id, ws)

    api = APIRouter(prefix="/api", dependencies=[Depends(require_admin)])

    # --- live events -----------------------------------------------------------
    @api.get("/stream")
    async def stream() -> StreamingResponse:
        """Server-sent events: `hello` (who is online), `worker` (online changes),
        `message`, `changed` (workers or channels were edited), `resync`."""
        async def events() -> AsyncIterator[str]:
            queue = hub.watch()
            try:
                yield _sse({"kind": "hello", "ts": time.time(), "online": sorted(hub.sessions)})
                while True:
                    try:
                        event = await asyncio.wait_for(queue.get(), keepalive_s)
                    except asyncio.TimeoutError:
                        yield ": keep-alive\n\n"        # also flushes proxies and detects dead clients
                        continue
                    yield _sse(event)
            finally:
                hub.unwatch(queue)

        return StreamingResponse(events(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    # --- workers -------------------------------------------------------------
    @api.get("/workers")
    def list_workers() -> list[dict[str, Any]]:
        return [{**w, "online": hub.online(w["worker_id"])} for w in store.list_workers()]

    @api.post("/workers", status_code=201)
    async def add_worker(body: WorkerIn) -> dict[str, str]:
        try:
            token = store.add_worker(body.worker_id, body.label)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None
        hub.emit("changed", scope="workers")
        return {"worker_id": body.worker_id, "token": token}

    @api.post("/workers/{worker_id}/token")
    async def rotate_token(worker_id: str) -> dict[str, str]:
        token = store.rotate_token(worker_id)
        if token is None:
            raise HTTPException(404, f"no worker {worker_id!r}")
        await hub.kick(worker_id, "token rotated")
        return {"worker_id": worker_id, "token": token}

    @api.delete("/workers/{worker_id}")
    async def remove_worker(worker_id: str) -> dict[str, str]:
        if not store.remove_worker(worker_id):
            raise HTTPException(404, f"no worker {worker_id!r}")
        await hub.kick(worker_id, "worker removed")
        hub.emit("changed", scope="workers")
        return {"removed": worker_id}

    # --- channels ------------------------------------------------------------
    @api.get("/channels")
    def list_channels() -> list[dict[str, Any]]:
        return [{**c, "online": [w for w in c["members"] if hub.online(w)]} for c in store.list_channels()]

    @api.post("/channels", status_code=201)
    async def add_channel(body: ChannelIn) -> dict[str, str]:
        try:
            store.add_channel(body.name, body.description)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None
        hub.emit("changed", scope="channels")
        return {"name": body.name}

    @api.delete("/channels/{name}")
    async def remove_channel(name: str) -> dict[str, str]:
        members = store.members_of(name)
        if not store.remove_channel(name):
            raise HTTPException(404, f"no channel {name!r}")
        for worker_id in members:
            await hub.notify(worker_id, {"type": "left", "channel": name})
        hub.emit("changed", scope="channels")
        return {"removed": name}

    @api.put("/channels/{name}/members/{worker_id}")
    async def join(name: str, worker_id: str, body: JoinIn | None = None) -> dict[str, Any]:
        try:
            created = store.join(name, worker_id, from_start=bool(body and body.from_start))
        except LookupError as exc:
            raise HTTPException(404, str(exc.args[0])) from None
        if created:
            await hub.notify(worker_id, {"type": "joined", "channel": name})
            hub.emit("changed", scope="channels")
        return {"channel": name, "worker_id": worker_id, "joined": created}

    @api.delete("/channels/{name}/members/{worker_id}")
    async def leave(name: str, worker_id: str) -> dict[str, Any]:
        if not store.leave(name, worker_id):
            raise HTTPException(404, f"{worker_id!r} is not a member of {name!r}")
        await hub.notify(worker_id, {"type": "left", "channel": name})
        hub.emit("changed", scope="channels")
        return {"channel": name, "worker_id": worker_id, "left": True}

    # --- messages ------------------------------------------------------------
    @api.get("/channels/{name}/messages")
    def history(name: str, after: int | None = None, before: int | None = None,
                limit: int = 100) -> list[dict[str, Any]]:
        """With `after`: oldest first from there. Otherwise the newest `limit`
        messages, below `before` when it is given. Always in ascending order."""
        if not store.channel_exists(name):
            raise HTTPException(404, f"no channel {name!r}")
        limit = min(max(limit, 1), 1000)
        if after is not None:
            return store.messages_after(name, after, limit=limit)
        return store.messages_before(name, before if before is not None else store.head() + 1, limit=limit)

    @api.post("/channels/{name}/messages", status_code=201)
    async def post_as_server(name: str, body: PostIn) -> dict[str, int]:
        try:
            seq, _ = hub.publish(name, sender=SERVER_SENDER, body=body.body)
        except RelayError as exc:
            raise HTTPException(404 if exc.code == "no_channel" else 400, str(exc)) from None
        return {"seq": seq}

    app.include_router(api)
    return app
