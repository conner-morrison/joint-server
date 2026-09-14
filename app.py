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
# to fix; the app starts, serves the console, and /healthz says what is wrong.
app = create_app(PgStore(DSN) if DSN else None)

# Files first, then the console for anything else: /acme is a workspace the
# console opens, not a file, and only the browser needs to know the difference.
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
    return FileResponse(os.path.join(UI, "index.html"))
