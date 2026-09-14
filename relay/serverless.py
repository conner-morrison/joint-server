"""The relay as one deployment holding many workspaces.

A workspace lives at its own path: /acme/api/... is Acme's relay, opened with
Acme's password. Everything is read from and written to Postgres, so any
instance can serve any request and none of them remember anything.

There are no websockets here. A function does not outlive its request, so a
worker asks for its messages and the request waits, briefly, for some to
arrive. See DESIGN-serverless.md.

Enrolment is the part worth reading twice. A worker that turns up with a token
nobody knows is not turned away: it is told how to ask. It proposes an id and
the token it made, that lands in front of a person, and when they approve it
the token it has been using all along starts working.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import time
from typing import Any, AsyncIterator

from fastapi import Depends, FastAPI, Header, HTTPException, Path, Request
from fastapi.responses import JSONResponse
from psycopg import Error as PgError
from pydantic import BaseModel

from relay.pgstore import PgStore, WorkspaceStore, check_name, slugify

SERVER_SENDER = "@server"
MAX_BODY_BYTES = int(os.environ.get("RELAY_MAX_BODY_BYTES", "256000"))
POLL_MAX_WAIT = float(os.environ.get("RELAY_POLL_MAX_WAIT", "25"))
POLL_INTERVAL = float(os.environ.get("RELAY_POLL_INTERVAL", "1.0"))


class WorkspaceIn(BaseModel):
    name: str
    password: str


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
    id: str | None = None


class PublishIn(BaseModel):
    channel: str
    body: Any = None
    id: str | None = None


class EnrolIn(BaseModel):
    worker_id: str
    token: str
    label: str = ""


class AckIn(BaseModel):
    channel: str
    seq: int


def bearer(value: str | None) -> str:
    if value and value[:7].lower() == "bearer ":
        return value[7:].strip()
    return ""


def too_large(body: Any) -> bool:
    return len(json.dumps(body, separators=(",", ":")).encode()) > MAX_BODY_BYTES


def redact(text: str) -> str:
    """Connection errors quote the connection string, which carries a
    password. Keep the part that says what went wrong, drop the credentials."""
    return re.sub(r"(?i)(postgres(?:ql)?://)[^\s\"\']*", r"\1...", text)


def create_app(store: PgStore | None) -> FastAPI:
    """`store` is None when the deployment has no database configured. The app
    still starts: it serves the console and says what is missing, because a
    process that refuses to start can only report a crash."""

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        # Nothing is opened here. A database that is down should fail the
        # request that needed it, not stop the console from loading.
        try:
            yield
        finally:
            if store is not None:
                await store.close()

    app = FastAPI(title="relay", lifespan=lifespan)
    app.state.store = store

    def db() -> PgStore:
        if store is None:
            raise HTTPException(503, {
                "status": "unconfigured",
                "message": "This deployment has no database. Set DATABASE_URL to a Postgres "
                           "connection string in the project's environment variables and redeploy.",
            })
        return store

    # --- who is asking ---------------------------------------------------
    async def workspace(ws: str = Path(..., description="workspace slug")) -> str:
        if not await db().workspace_exists(ws):
            raise HTTPException(404, f"no workspace {ws!r}")
        return ws

    async def admin(ws: str = Depends(workspace),
                    authorization: str | None = Header(default=None)) -> WorkspaceStore:
        """The workspace's own password. It opens that workspace and no other."""
        if not await db().check_password(ws, bearer(authorization)):
            raise HTTPException(401, "workspace password required")
        return store.ws(ws)

    async def worker(ws: str = Depends(workspace),
                     authorization: str | None = Header(default=None)) -> tuple[WorkspaceStore, str]:
        """A worker's own token.

        An unknown token is not simply refused: if it belongs to a request
        already waiting, the answer says so, and otherwise the answer says how
        to ask. A worker can therefore be pointed at a workspace and left to
        sort itself out.
        """
        token = bearer(authorization)
        scoped = db().ws(ws)
        worker_id = await scoped.worker_for_token(token)
        if worker_id is not None:
            return scoped, worker_id

        waiting = await scoped.is_pending(token)
        if waiting is not None:
            raise HTTPException(403, {"status": "pending", "worker_id": waiting,
                                      "message": f"{waiting} is waiting to be approved in this workspace"})
        raise HTTPException(401, {"status": "unregistered", "enrol": f"/{ws}/enrol",
                                  "message": "unknown token: POST a worker_id and token to the enrol path"})

    # --- the deployment --------------------------------------------------
    @app.post("/api/workspaces", status_code=201)
    async def create_workspace(body: WorkspaceIn) -> dict[str, Any]:
        """Anyone may make one. What they get is a workspace of their own, not
        a way into anybody else's."""
        slug = slugify(body.name)
        try:
            check_name("workspace", slug)
            await db().create_workspace(slug, body.password, body.name.strip())
        except ValueError as exc:
            raise HTTPException(409 if "exists" in str(exc) else 400, str(exc)) from None
        return {"slug": slug, "name": body.name.strip()}

    @app.get("/api/workspaces")
    async def list_workspaces() -> list[dict[str, Any]]:
        """The workspaces here, by name. Names only: what is inside one needs
        that workspace's password, and this says nothing about it."""
        return await db().list_workspaces()

    @app.get("/api/workspaces/{ws}")
    async def workspace_exists(ws: str) -> dict[str, Any]:
        """Whether a workspace is here, and what it is called, so its address
        can greet a visitor by name. Nothing about what is inside it."""
        found = await db().find_workspace(ws)
        return {"slug": ws, "exists": found is not None, "name": found["name"] if found else ""}

    # --- enrolment -------------------------------------------------------
    @app.post("/{ws}/enrol", status_code=202)
    async def enrol(body: EnrolIn, ws: str = Depends(workspace)) -> dict[str, Any]:
        scoped = db().ws(ws)
        # Already approved and using this very token: nothing to do, carry on.
        if await scoped.worker_for_token(body.token) == body.worker_id:
            return {"status": "registered", "worker_id": body.worker_id}
        try:
            await scoped.request_worker(body.worker_id, body.token, body.label)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None
        return {"status": "pending", "worker_id": body.worker_id,
                "message": "waiting for someone to approve this worker in the console"}

    @app.post("/{ws}/publish", status_code=201)
    async def publish(body: PublishIn, who: tuple[WorkspaceStore, str] = Depends(worker)) -> dict[str, Any]:
        scoped, worker_id = who
        if not await scoped.is_member(body.channel, worker_id):
            # A channel it is not in and a channel that does not exist look the
            # same, so a token cannot be used to find out what exists.
            raise HTTPException(403, f"{worker_id} is not a member of channel {body.channel!r}")
        if too_large(body.body):
            raise HTTPException(413, f"body is larger than {MAX_BODY_BYTES} bytes")
        seq, duplicate = await scoped.append(body.channel, worker_id, body.body, body.id)
        await scoped.touch_worker(worker_id)
        return {"seq": seq, "duplicate": duplicate, "worker_id": worker_id}

    @app.get("/{ws}/messages")
    async def messages(wait: float = 0.0, limit: int = 200,
                       who: tuple[WorkspaceStore, str] = Depends(worker)) -> dict[str, Any]:
        """Everything above this worker's cursors. With `wait`, the request
        stays open until something arrives or the time runs out, so a worker
        with nothing to do is not asking over and over."""
        scoped, worker_id = who
        await scoped.touch_worker(worker_id)
        deadline = time.monotonic() + min(max(wait, 0.0), POLL_MAX_WAIT)
        while True:
            found = await scoped.waiting_for(worker_id, limit=min(max(limit, 1), 1000))
            if found or time.monotonic() >= deadline:
                return {"messages": found, "worker_id": worker_id}
            await asyncio.sleep(POLL_INTERVAL)

    @app.post("/{ws}/ack")
    async def ack(body: AckIn, who: tuple[WorkspaceStore, str] = Depends(worker)) -> dict[str, Any]:
        scoped, worker_id = who
        await scoped.ack(body.channel, worker_id, body.seq)
        return {"ok": True}

    # --- the console's api -----------------------------------------------
    @app.get("/{ws}/api/workers")
    async def list_workers(scoped: WorkspaceStore = Depends(admin)) -> list[dict[str, Any]]:
        return await scoped.list_workers()

    @app.post("/{ws}/api/workers", status_code=201)
    async def add_worker(body: WorkerIn, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        try:
            token = await scoped.add_worker(body.worker_id, body.label)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None
        return {"worker_id": body.worker_id, "token": token}

    @app.post("/{ws}/api/workers/{worker_id}/token")
    async def new_token(worker_id: str, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        token = await scoped.rotate_token(worker_id)
        if token is None:
            raise HTTPException(404, f"no worker {worker_id!r}")
        return {"worker_id": worker_id, "token": token}

    @app.delete("/{ws}/api/workers/{worker_id}")
    async def remove_worker(worker_id: str, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        if not await scoped.remove_worker(worker_id):
            raise HTTPException(404, f"no worker {worker_id!r}")
        return {"removed": True}

    @app.get("/{ws}/api/pending")
    async def list_pending(scoped: WorkspaceStore = Depends(admin)) -> list[dict[str, Any]]:
        return await scoped.list_pending()

    @app.post("/{ws}/api/pending/{worker_id}")
    async def approve(worker_id: str, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        if not await scoped.approve_worker(worker_id):
            raise HTTPException(404, f"nothing waiting as {worker_id!r}")
        return {"worker_id": worker_id, "approved": True}

    @app.delete("/{ws}/api/pending/{worker_id}")
    async def reject(worker_id: str, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        if not await scoped.reject_worker(worker_id):
            raise HTTPException(404, f"nothing waiting as {worker_id!r}")
        return {"worker_id": worker_id, "rejected": True}

    @app.get("/{ws}/api/channels")
    async def list_channels(scoped: WorkspaceStore = Depends(admin)) -> list[dict[str, Any]]:
        return await scoped.list_channels()

    @app.post("/{ws}/api/channels", status_code=201)
    async def add_channel(body: ChannelIn, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        try:
            await scoped.add_channel(body.name, body.description)
        except ValueError as exc:
            raise HTTPException(409 if "exists" in str(exc) else 400, str(exc)) from None
        return {"name": body.name}

    @app.delete("/{ws}/api/channels/{name}")
    async def remove_channel(name: str, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        if not await scoped.remove_channel(name):
            raise HTTPException(404, f"no channel {name!r}")
        return {"removed": True}

    @app.put("/{ws}/api/channels/{name}/members/{worker_id}")
    async def add_member(name: str, worker_id: str, body: JoinIn | None = None,
                         scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        try:
            joined = await scoped.join(name, worker_id, from_start=bool(body and body.from_start))
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from None
        return {"joined": joined}

    @app.delete("/{ws}/api/channels/{name}/members/{worker_id}")
    async def remove_member(name: str, worker_id: str,
                            scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        return {"left": await scoped.leave(name, worker_id)}

    @app.get("/{ws}/api/channels/{name}/messages")
    async def history(name: str, after: int | None = None, before: int | None = None, limit: int = 100,
                      scoped: WorkspaceStore = Depends(admin)) -> list[dict[str, Any]]:
        if not await scoped.channel_exists(name):
            raise HTTPException(404, f"no channel {name!r}")
        limit = min(max(limit, 1), 1000)
        if after is not None:
            return await scoped.messages_after(name, after, limit=limit)
        head = await scoped.head()
        return await scoped.messages_before(name, before if before is not None else head + 1, limit=limit)

    @app.post("/{ws}/api/channels/{name}/messages", status_code=201)
    async def post_as_server(name: str, body: PostIn,
                             scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        if not await scoped.channel_exists(name):
            raise HTTPException(404, f"no channel {name!r}")
        if too_large(body.body):
            raise HTTPException(413, f"body is larger than {MAX_BODY_BYTES} bytes")
        seq, duplicate = await scoped.append(name, SERVER_SENDER, body.body, body.id)
        return {"seq": seq, "duplicate": duplicate}

    async def database_down(_: Request, exc: Exception) -> JSONResponse:
        """The database refused or never answered. Without this the request
        fails as an unhandled error, which reaches the caller as a bare 500 and
        says nothing about which part is unwell."""
        return JSONResponse({"status": "database_unreachable",
                             "message": redact(str(exc))[:300]}, 503)

    app.add_exception_handler(PgError, database_down)

    @app.exception_handler(HTTPException)
    async def structured_errors(_: Request, exc: HTTPException) -> JSONResponse:
        """Enrolment answers carry a shape a worker acts on, not just prose."""
        detail = exc.detail
        payload = detail if isinstance(detail, dict) else {"detail": detail}
        return JSONResponse(payload, status_code=exc.status_code)

    return app
