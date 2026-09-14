"""Vercel entrypoint: the relay, serving its own console.

Vercel loads `app` from this file and routes every request to it. One
deployment holds many workspaces, so the address is the whole story:

    /                       the console
    /acme                   the console, opened on the Acme workspace
    /acme/api/channels      Acme's relay, opened with Acme's password
    /acme/publish           a worker in Acme posting

Because the console is served by the relay, they share an origin: there is no
CORS, and nothing has to be told where the relay is.
"""
from __future__ import annotations

import os

from fastapi import Response
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from relay.pgstore import PgStore
from relay.serverless import create_app

# Vercel's own POSTGRES_URL is accepted, so a database added through the
# marketplace needs no second variable.
DSN = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL", "")

UI = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ui")

# A missing database must not stop the app from starting. Raising here would
# reach the browser as "this function crashed", which says nothing about what
# to fix; the app starts, serves the console, and the console says what is wrong.
# A serverless host runs many copies of this, each with its own pool, against
# one database that has a finite connection limit. A managed pooler (Neon,
# Supabase) absorbs that; a plain Postgres (Railway, a VPS) does not, so the
# pool is kept small and can be tuned without a code change.
POOL_MAX = int(os.environ.get("RELAY_POOL_MAX", "3"))

app = create_app(PgStore(DSN, max_size=POOL_MAX) if DSN else None)

# StaticFiles raises when its directory is absent, which would be a crash at
# import for a missing folder. The console being unavailable is worth saying
# out loud, not worth taking the relay down for.
if os.path.isdir(UI):
    app.mount("/static", StaticFiles(directory=UI), name="static")


# Anything the relay itself would have answered. Serving the console here
# instead would turn "this endpoint does not exist" into a 200 carrying a web
# page, which a caller can only discover by failing to parse it.
API_PARTS = ("/api/", "/publish", "/messages", "/ack", "/enrol")


@app.get("/{path:path}", include_in_schema=False)
async def console(path: str = "") -> Response:
    whole = "/" + path
    if any(part in whole for part in API_PARTS) or whole.endswith(API_PARTS):
        return JSONResponse({"detail": f"no such endpoint: {whole}"}, status_code=404)
    candidate = os.path.normpath(os.path.join(UI, path))
    if path and candidate.startswith(UI + os.sep) and os.path.isfile(candidate):
        return FileResponse(candidate)
    index = os.path.join(UI, "index.html")
    if not os.path.isfile(index):
        return JSONResponse({"detail": "the console is not part of this deployment; the API is here"}, 404)
    return FileResponse(index)
