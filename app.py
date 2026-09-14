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

from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from relay.pgstore import PgStore
from relay.serverless import create_app

DSN = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL", "")
if not DSN:
    raise RuntimeError("set DATABASE_URL to a Postgres connection string")

UI = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ui")

app = create_app(PgStore(DSN))

# Files first, then the console for anything else: /acme is a workspace the
# console opens, not a file, and only the browser needs to know the difference.
app.mount("/static", StaticFiles(directory=UI), name="static")


@app.get("/{path:path}", include_in_schema=False)
async def console(path: str = "") -> FileResponse:
    candidate = os.path.normpath(os.path.join(UI, path))
    if path and candidate.startswith(UI + os.sep) and os.path.isfile(candidate):
        return FileResponse(candidate)
    return FileResponse(os.path.join(UI, "index.html"))
