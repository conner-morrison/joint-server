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

from fastapi import Depends, FastAPI, Header, HTTPException, Path, Request, Response
from fastapi.responses import JSONResponse
from psycopg import Error as PgError
from pydantic import BaseModel, field_validator

from relay import telegram
from relay.notify import Notifier, key as notify_key
from relay.sender import BotSender
from relay.pgstore import PgStore, WorkspaceStore, check_name, slugify

SERVER_SENDER = "@server"
MAX_BODY_BYTES = int(os.environ.get("RELAY_MAX_BODY_BYTES", "256000"))
POLL_MAX_WAIT = float(os.environ.get("RELAY_POLL_MAX_WAIT", "25"))
POLL_INTERVAL = float(os.environ.get("RELAY_POLL_INTERVAL", "5.0"))


class WorkspaceIn(BaseModel):
    name: str
    password: str


class WorkerIn(BaseModel):
    name: str
    label: str = ""


class ChannelIn(BaseModel):
    name: str
    description: str = ""


class JoinIn(BaseModel):
    from_start: bool = False


class BotIn(BaseModel):
    name: str
    chat_id: str
    token: str

    @field_validator("name", "chat_id", "token")
    @classmethod
    def trimmed(cls, value: str) -> str:
        """A chat id and a token are pasted, and paste brings its spaces and
        newlines with it. Untrimmed, " 555 " and "555" are two chats as far
        as the check for a second registration is concerned, and a token with
        a trailing newline is a URL Telegram never sees."""
        return value.strip()


class PostIn(BaseModel):
    body: Any = None
    id: str | None = None


class PublishIn(BaseModel):
    channel: str
    body: Any = None
    id: str | None = None


class EnrolIn(BaseModel):
    """What a worker brings: what it is called, and nothing else. There is no
    credential for it to invent and none for a person to carry anywhere."""
    name: str
    label: str = ""


class RenameIn(BaseModel):
    name: str


class MuteIn(BaseModel):
    url: str
    note: str = ""


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


def create_app(store: PgStore | None, notifier: Notifier | None = None,
               sender: BotSender | None = None) -> FastAPI:
    """`store` is None when the deployment has no database configured. The app
    still starts: it serves the console and says what is missing, because a
    process that refuses to start can only report a crash."""

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if sender is not None:
            sender.start()
        # Nothing is opened here. A database that is down should fail the
        # request that needed it, not stop the console from loading.
        try:
            yield
        finally:
            if sender is not None:
                await sender.close()
            if notifier is not None:
                await notifier.close()
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
        """A worker's own id, which is what it was given when it was registered.

        What arrives is not simply refused when it is unknown: if it is the
        ticket of a request still waiting, the answer says so, and otherwise the
        answer says how to ask. A worker can therefore be pointed at a workspace
        and left to sort itself out.
        """
        presented = bearer(authorization)
        scoped = db().ws(ws)
        worker_id = await scoped.authenticate(presented)
        if worker_id is not None:
            return scoped, worker_id

        waiting = await scoped.is_pending(presented)
        if waiting is not None:
            raise HTTPException(403, {"status": "pending", "name": waiting,
                                      "message": f"{waiting} is waiting to be approved in this workspace"})
        raise HTTPException(401, {"status": "unregistered", "enrol": f"/{ws}/enrol",
                                  "message": "unknown id: POST a name to the enrol path to ask to join"})

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
        """Ask to join, with a name.

        What comes back is a ticket, which is good for one thing: asking what
        came of this request. The id a worker ends up using is the server's to
        give, and it is given once a person has said yes.
        """
        scoped = db().ws(ws)
        try:
            asked = await scoped.request_worker(body.name, body.label)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None
        return {"status": "pending", "name": asked["name"], "ticket": asked["ticket"],
                "poll": f"/{ws}/enrol/{asked['ticket']}",
                "message": "waiting for someone to approve this worker in the console"}

    @app.get("/{ws}/enrol/{ticket}")
    async def enrolled(ticket: str, response: Response, wait: float = 0.0,
                       ws: str = Depends(workspace)) -> dict[str, Any]:
        """What came of a request. The worker asks until it is answered.

        With `wait`, the request is held open until it is answered or the time
        runs out, the same way a worker waits for messages. The server cannot
        call a worker - a worker has no address, which is the whole point of it
        dialling out - so a worker learns it was approved by asking. Holding the
        question open is what makes the answer arrive when the person clicks
        rather than whenever the worker next had a reason to speak.

        Answered once and then kept for a week, so a worker that lost the reply
        on the way can ask again instead of registering all over again.
        """
        scoped = db().ws(ws)
        deadline = time.monotonic() + min(max(wait, 0.0), POLL_MAX_WAIT)
        while True:
            found = await scoped.registration(ticket)
            if found is None:
                raise HTTPException(404, {"status": "unknown",
                                          "message": "no such request: it was declined, or it has "
                                                     "expired. Ask again by name."})
            if found["granted_id"] or time.monotonic() >= deadline:
                break
            # Approval is a person clicking, so a second is soon enough and
            # costs one small query. There is nothing to notify on here.
            await asyncio.sleep(min(1.0, max(deadline - time.monotonic(), 0.05)))
        if not found["granted_id"]:
            response.status_code = 202
            return {"status": "pending", "name": found["name"],
                    "message": "waiting for someone to approve this worker in the console"}
        return {"status": "registered", "worker_id": found["granted_id"], "name": found["name"],
                "message": f"registered as {found['name']}. Send this id as the bearer token "
                           "from now on, and keep it: it is how this server knows you."}

    @app.post("/{ws}/publish", status_code=201)
    async def publish(body: PublishIn,
                      who: tuple[WorkspaceStore, str] = Depends(worker)) -> Any:
        scoped, worker_id = who
        if not await scoped.is_member(body.channel, worker_id):
            # A channel it is not in and a channel that does not exist look the
            # same, so a token cannot be used to find out what exists.
            raise HTTPException(403, f"{worker_id} is not a member of channel {body.channel!r}")
        if too_large(body.body):
            raise HTTPException(413, f"body is larger than {MAX_BODY_BYTES} bytes")
        # Refused at the door: a muted job is never stored, so nothing is
        # announced and nothing has to be explained away afterwards. The
        # publisher is told it was accepted, because there is nothing for it
        # to do differently.
        if await scoped.is_muted(body.id):
            await scoped.touch_worker(worker_id)
            # 200, not the route's 201: nothing was created.
            return JSONResponse({"seq": None, "muted": True, "worker_id": worker_id}, 200)
        seq, duplicate = await scoped.append(body.channel, worker_id, body.body, body.id)
        await scoped.touch_worker(worker_id)
        if sender is not None and not duplicate:
            sender.nudge()
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
        # What this worker would be woken for: its own channels, in its own
        # workspace. A publish elsewhere is not its business.
        watching = [notify_key(scoped.slug, channel)
                    for channel in await scoped.channels_of(worker_id)]
        while True:
            found = await scoped.waiting_for(worker_id, limit=min(max(limit, 1), 1000))
            left = deadline - time.monotonic()
            if found or left <= 0:
                await scoped.redelivered(worker_id, [int(m["seq"]) for m in found])
                return {"messages": found, "worker_id": worker_id}
            # Woken by the publish itself. The timeout is a safety net for a
            # notification that never arrives, not the thing doing the work,
            # which is why it can afford to be long.
            if notifier is not None:
                await notifier.wait(watching, timeout=min(left, POLL_INTERVAL))
            else:
                await asyncio.sleep(min(left, POLL_INTERVAL))

    @app.post("/{ws}/ack")
    async def ack(body: AckIn, who: tuple[WorkspaceStore, str] = Depends(worker)) -> dict[str, Any]:
        scoped, worker_id = who
        await scoped.ack(body.channel, worker_id, body.seq)
        await scoped.touch_worker(worker_id)
        return {"ok": True}

    # --- the console's api -----------------------------------------------
    @app.get("/{ws}/api/workers")
    async def list_workers(scoped: WorkspaceStore = Depends(admin)) -> list[dict[str, Any]]:
        return await scoped.list_workers()

    @app.post("/{ws}/api/workers", status_code=201)
    async def add_worker(body: WorkerIn, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        """Register a worker without waiting for it to ask. The id comes back
        once; it is what the worker authenticates with."""
        try:
            return await scoped.add_worker(body.name, body.label)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None

    @app.post("/{ws}/api/workers/{worker_id}/id")
    async def new_id(worker_id: str, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        """A new id for a worker whose id got out. It keeps its name, its
        channels and its place in each of them; the old id stops working."""
        fresh = await scoped.reissue_id(worker_id)
        if fresh is None:
            raise HTTPException(404, f"no worker {worker_id!r}")
        return fresh

    @app.post("/{ws}/api/workers/{worker_id}/name")
    async def rename_worker(worker_id: str, body: RenameIn,
                            scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        """A name is only what people call it, so correcting one changes
        nothing else."""
        try:
            name = await scoped.rename_worker(worker_id, body.name)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None
        if name is None:
            raise HTTPException(404, f"no worker {worker_id!r}")
        return {"worker_id": worker_id, "name": name}

    @app.delete("/{ws}/api/workers/{worker_id}")
    async def remove_worker(worker_id: str, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        if not await scoped.remove_worker(worker_id):
            raise HTTPException(404, f"no worker {worker_id!r}")
        return {"removed": True}

    @app.get("/{ws}/api/pending")
    async def list_pending(scoped: WorkspaceStore = Depends(admin)) -> list[dict[str, Any]]:
        return await scoped.list_pending()

    @app.post("/{ws}/api/pending/{ref}")
    async def approve(ref: str, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        """Say yes. The worker is registered and given its id, which it collects
        the next time it asks what came of its request."""
        approved = await scoped.approve_worker(ref)
        if approved is None:
            raise HTTPException(404, "nothing is waiting under that request")
        return {**approved, "approved": True}

    @app.delete("/{ws}/api/pending/{ref}")
    async def reject(ref: str, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        if not await scoped.reject_worker(ref):
            raise HTTPException(404, "nothing is waiting under that request")
        return {"rejected": True}

    @app.get("/{ws}/api/bots")
    async def list_bots(scoped: WorkspaceStore = Depends(admin)) -> list[dict[str, Any]]:
        return await scoped.list_bots()

    @app.post("/{ws}/api/bots", status_code=201)
    async def add_bot(body: BotIn, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        """Proved before it is stored, by sending to it.

        Asking Telegram whether the token is real would leave the chat id
        untested, and a wrong chat id fails silently later, when nobody is
        looking. Sending is the only check that covers both, and it arrives as
        the message that says the bot is working.
        """
        # Asked before the welcome is sent, so a registration that is going to
        # be refused does not put a message in the chat saying it worked.
        twin = await scoped.bot_for_chat(body.chat_id)
        if twin is not None:
            raise HTTPException(409, f"bot {twin!r} already sends to that chat. Add it to "
                                     "more channels instead: a second one here would send "
                                     "everything twice")
        try:
            who = await telegram.check(body.token)
            await telegram.send(body.token, body.chat_id, telegram.WELCOME)
        except telegram.TelegramError as exc:
            raise HTTPException(400, {
                "status": "invalid_bot",
                "message": f"Telegram would not send with that ID and token: {exc}",
            }) from None
        try:
            await scoped.add_bot(body.name, body.chat_id, body.token,
                                 label=who.get("username") or "")
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None
        return {"name": body.name, "chat_id": body.chat_id,
                "label": who.get("username") or "", "channels": []}

    @app.delete("/{ws}/api/bots/{bot}")
    async def remove_bot(bot: str, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        if not await scoped.remove_bot(bot):
            raise HTTPException(404, f"no bot {bot!r}")
        return {"removed": True}

    @app.get("/{ws}/api/muted")
    async def list_muted(scoped: WorkspaceStore = Depends(admin)) -> list[dict[str, Any]]:
        return await scoped.muted()

    @app.post("/{ws}/api/muted", status_code=201)
    async def add_muted(body: MuteIn, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        """Silence one job before it arrives, by its link."""
        try:
            key = await scoped.mute(body.url, body.note)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from None
        return {"key": key, "muted": True}

    @app.delete("/{ws}/api/muted/{key:path}")
    async def remove_muted(key: str, scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        if not await scoped.unmute(key):
            raise HTTPException(404, f"{key!r} is not muted")
        return {"unmuted": True}

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

    @app.post("/{ws}/api/channels/{name}/skip/{who}")
    async def skip_backlog(name: str, who: str, bot: bool = False,
                           scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        """Pass over what a member never collected, and start it from now."""
        try:
            skipped = await scoped.skip_to_now(name, who, bot=bot)
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from None
        return {"skipped": skipped}

    @app.post("/{ws}/api/channels/{name}/send/{who}")
    async def send_one(name: str, who: str, seq: int, bot: bool = False,
                       scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        """Hand one message to one member again, whatever it has already had.

        A chat is sent to there and then rather than queued, because nothing is
        coming to collect it, and its place in the channel is left alone: this
        is for looking at one message again, not for rewinding what the chat
        has already been told.
        """
        if bot:
            if sender is None:
                raise HTTPException(503, "this deployment is not sending to chats")
            try:
                parts = await sender.send_one(scoped.slug, name, who, seq)
            except LookupError as exc:
                raise HTTPException(404, str(exc)) from None
            except telegram.TelegramError as exc:
                raise HTTPException(502, f"Telegram would not take it: {exc}") from None
            return {"sent": True, "seq": seq, "bot": who, "messages": parts}
        try:
            await scoped.send_again(name, who, seq)
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from None
        return {"sent": True, "seq": seq, "worker_id": who}

    @app.post("/{ws}/api/channels/{name}/resend/{who}")
    async def resend(name: str, who: str, count: int = 1, bot: bool = False,
                     scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        """Send a member the last thing again, to see whether it arrives."""
        try:
            again = await scoped.resend_to(name, who, count=min(max(count, 1), 100), bot=bot)
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from None
        if again and sender is not None:
            sender.nudge()
        return {"resending": again}

    @app.get("/{ws}/api/channels/{name}/delivery")
    async def delivery(name: str, scoped: WorkspaceStore = Depends(admin)
                       ) -> list[dict[str, Any]]:
        """Who is behind on this channel, and by how much."""
        if not await scoped.channel_exists(name):
            raise HTTPException(404, f"no channel {name!r}")
        return await scoped.delivery_of(name)

    @app.get("/{ws}/api/channels/{name}/bots")
    async def channel_bots(name: str, scoped: WorkspaceStore = Depends(admin)
                           ) -> list[dict[str, Any]]:
        return await scoped.bots_of(name)

    @app.put("/{ws}/api/channels/{name}/bots/{bot}")
    async def add_bot_member(name: str, bot: str, body: JoinIn | None = None,
                             scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        try:
            joined = await scoped.join_bot(name, bot, from_start=bool(body and body.from_start))
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from None
        if joined and sender is not None:
            sender.nudge()
        return {"joined": joined}

    @app.delete("/{ws}/api/channels/{name}/bots/{bot}")
    async def remove_bot_member(name: str, bot: str,
                                scoped: WorkspaceStore = Depends(admin)) -> dict[str, Any]:
        return {"left": await scoped.leave_bot(name, bot)}

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
                             scoped: WorkspaceStore = Depends(admin)) -> Any:
        if not await scoped.channel_exists(name):
            raise HTTPException(404, f"no channel {name!r}")
        if too_large(body.body):
            raise HTTPException(413, f"body is larger than {MAX_BODY_BYTES} bytes")
        if await scoped.is_muted(body.id):
            return JSONResponse({"seq": None, "muted": True}, 200)
        seq, duplicate = await scoped.append(name, SERVER_SENDER, body.body, body.id)
        if sender is not None and not duplicate:
            sender.nudge()
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
