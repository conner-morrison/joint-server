# Relay on Vercel

The relay was one always-on process with a SQLite file and websockets held
open. Vercel gives neither: a function handles one request and dies, with no
disk that survives and no memory shared between instances. This is what
replaces each part, and why.

## What stays

Vercel runs ASGI applications, so FastAPI and every admin route survive. The
domain model survives too: channels, members, a per-member cursor, a global
message sequence, dedupe by publisher id. That model was never tied to SQLite
or to websockets.

## What changes

| | process relay | serverless relay |
|---|---|---|
| storage | SQLite file on disk | Postgres (Neon), reached over the network |
| delivery | server pushes over a held websocket | worker long-polls `GET /messages` |
| routing | `Hub` in memory, one process | none: every instance reads the same database |
| console | separate Vercel site, CORS | same origin, no CORS, no server-URL field |

## Why long-polling rather than websockets

Vercel's websocket support is Node-only and still bounded by the function
lifetime, so a worker's connection would drop every few minutes. Worse, each
instance has its own memory, so the in-memory routing that makes push work has
nowhere to live: a publish arriving at instance A cannot reach a socket held by
instance B without going through the database anyway.

A long poll holds one request open for up to `wait` seconds, checking the
database as it goes, and returns as soon as anything is there. Hobby functions
may run 300s, so a 30s hold is uncontroversial. A worker sees a message within
about a second of it being stored, which is the same order as push for this
workload, and the code is a loop instead of a connection lifecycle.

## Delivery guarantees, kept

The cursor is what preserves them, and it already lives in the database.

- **Stored before confirmed.** `POST /publish` returns after the insert commits.
- **At least once.** A poll returns everything above the worker's cursor; the
  cursor advances only when the worker acks. A worker that dies mid-handler
  sees the message again.
- **In order per channel.** `seq` is a sequence; a poll returns ascending.
- **Resends dropped.** `UNIQUE (sender, client_id)` still does this.
- **New members start now.** Joining sets the cursor to the current head.

## Costs, stated plainly

- **Latency** becomes the poll's own round trip rather than a push: about a
  second, against tens of milliseconds.
- **A poll is billed** while it waits. Vercel bills active CPU, and a waiting
  poll is idle, but each database check is real work. Workers should hold long
  polls, not spin on short ones.
- **Neon's free tier suspends** an idle database; the first query after that
  takes noticeably longer. It wakes on its own.

## Endpoints

Worker token:

| method | path | does |
|---|---|---|
| `POST` | `/publish` | publish to a channel the worker is in |
| `GET` | `/messages?wait=30` | everything above this worker's cursors; waits for it |
| `POST` | `/ack` | advance a cursor, once the handler is done with a message |

Admin token: `/api/*` exactly as before. The console is served from the same
deployment, so it calls them on its own origin.

## The database

Any Postgres works: what the deployment needs is a connection string in
`DATABASE_URL` (Vercel's own `POSTGRES_URL` is accepted too). Nothing here is
tied to a particular provider.

What differs between them is **connections**. A serverless host runs many
copies of the application at once, each with its own pool, against one
database with a finite limit.

- **Neon, Supabase** publish a *pooled* connection string, which absorbs that.
  Use it: on Neon the host contains `-pooler`.
- **Railway, a VPS, anything plain** hand you a direct connection to Postgres,
  whose `max_connections` defaults to about 100. Keep `RELAY_POOL_MAX` small
  (3 is the default here) and it is fine at this scale.

Two things catch people out on Railway. Its `DATABASE_URL` points at
`postgres.railway.internal`, which only resolves **inside** Railway: from
Vercel use the public one, `DATABASE_PUBLIC_URL`, whose host ends in
`proxy.rlwy.net`. And a Railway database is always on, so unlike Neon's free
tier nothing sleeps and no query pays to wake it.
