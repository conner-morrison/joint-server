# relay

One server on the internet, any number of local workers. The server creates
**channels** and decides which workers belong to each. A member posts to a
channel, and every other member receives the post: right away if it is
connected, or when it next connects if it is not.

```
  worker A ─┐                              ┌─ worker C
            │        websocket + token     │
  worker B ─┼──►  relay server (SQLite)  ◄─┼─ worker D
            │                              │
            │   channel "jobs":    A B C   │
            │   channel "results": C D     │
            └──────────────────────────────┘
```

A worker can only post to, and receive from, the channels it is a member of.
The server is the delivery medium. It does not interpret message bodies:
a body is any JSON.

## Quick start

```bash
pip install -e ".[server,client]"

export RELAY_ADMIN_TOKEN=$(python3 -c "import secrets;print(secrets.token_urlsafe(24))")
python -m relay channel add jobs --description "work to be picked up"
python -m relay worker add scout-1      # prints a token, shown once
python -m relay worker add builder-1
python -m relay join jobs scout-1 builder-1
python -m relay serve --port 8700
```

In two more terminals, using the tokens printed above:

```bash
python examples/worker.py --url http://127.0.0.1:8700 --token <scout-1 token>   --channel jobs
python examples/worker.py --url http://127.0.0.1:8700 --token <builder-1 token> --channel jobs
```

Type `{"url": "https://example.com/job/1"}` into one terminal. It appears in the other.

## Writing a worker

```python
import asyncio
from relay import RelayClient

client = RelayClient("https://relay.example.com", token)

@client.on("jobs")
async def on_job(msg):
    # msg.channel, msg.seq, msg.sender, msg.body, msg.ts
    result = await do_the_work(msg.body)
    await client.publish("results", {"job": msg.body["url"], "result": result})

@client.on()                            # any channel without its own handler
async def everything_else(msg):
    print(msg.channel, msg.body)

asyncio.run(client.run())
```

`run()` reconnects on its own with backoff. It returns only after `close()`,
and raises `AuthError` if the token is rejected, because retrying cannot help.
`publish()` returns the message's sequence number once the server has stored
it. It raises `PublishError` if the worker is not a member of that channel.

## Delivery guarantees

- **Stored before it is confirmed.** `publish()` returns only after the message
  is in the database, so a confirmed message survives a server restart.
- **At least once.** Each member has a cursor on each channel, which is the
  last message it acknowledged. The client acknowledges a message after its
  handler returns. On reconnect the server resends everything after the
  cursor. The client skips resends it already handled in the same process, so
  in practice only a worker that restarts mid-message sees one twice. Make
  handlers idempotent where that matters.
- **In order per channel.** Messages arrive in sequence order, and the handlers
  run one at a time.
- **Resends from the publisher are dropped.** A publish that was sent but not
  confirmed before the connection dropped is resent on reconnect. The server
  recognises the message id and does not store it twice.
- **A failing handler does not block the channel.** The exception is logged
  and the message is acknowledged, so one bad message cannot stall the channel.
- **New members start now.** Joining a channel delivers messages posted from
  that point on. Use `--from-start` (or `{"from_start": true}`) to also get
  what the channel still holds.
- **Retention.** Messages older than `--retention-days` (default 7, `0` = keep
  for ever) are deleted, including ones an offline worker has not received yet.

## Admin HTTP API

Every call needs `Authorization: Bearer $RELAY_ADMIN_TOKEN`.

| method | path | does |
|---|---|---|
| `GET` | `/api/workers` | workers, their channels, online state |
| `POST` | `/api/workers` `{"worker_id", "label"}` | register; returns the token once |
| `POST` | `/api/workers/{id}/token` | new token; disconnects the worker |
| `DELETE` | `/api/workers/{id}` | remove; disconnects the worker |
| `GET` | `/api/channels` | channels, members, who is online, message counts |
| `POST` | `/api/channels` `{"name", "description"}` | create a channel |
| `DELETE` | `/api/channels/{name}` | delete a channel and its messages |
| `PUT` | `/api/channels/{name}/members/{id}` `{"from_start"}` | add a member |
| `DELETE` | `/api/channels/{name}/members/{id}` | remove a member |
| `GET` | `/api/channels/{name}/messages?limit=100` | newest messages; `before=<seq>` pages back, `after=<seq>` reads forward |
| `POST` | `/api/channels/{name}/messages` `{"body", "id"}` | post as `@server`; an `id` is remembered, so a retried post is stored once |
| `GET` | `/api/stream` | server-sent events: `hello`, `worker` (online/offline), `message`, `changed`, `resync` |

The CLI (`python -m relay worker|channel|join|leave`) writes to the database
directly and works with the server stopped. Through the API, a connected worker
hears about changes immediately, and a removed worker is disconnected.

## Publishing over HTTP

Not every publisher can hold a websocket. A Google Apps Script, a cron job or
someone else's webhook gets one HTTP request and no more. `POST /publish` gives
them the worker protocol's authority without the admin token.

```bash
curl -X POST https://relay.example.com/publish \
  -H "Authorization: Bearer <worker token>" -H "Content-Type: application/json" \
  -d '{"channel": "jobs", "id": "gmail-18f2a1b:0", "body": {"url": "https://example.com/job/1"}}'
```

The message arrives from that worker, not from `@server`, and the worker must
be a member of the channel. A channel it is not in and a channel that does not
exist both answer `403`, so a token cannot be used to find out what exists.
`id` is optional and works like the websocket's: send the same one again and
the resend is dropped, with the original `seq` returned and `duplicate` true.

Give a publisher like this its own worker (`worker add`) and put it only in the
channels it should reach. Losing that token costs you one worker, replaceable
with `worker token <id>`; losing the admin token costs you the server.

## Wire protocol

Workers written in other languages only need a websocket and JSON. Connect to
`/ws` with `Authorization: Bearer <token>` (or `?token=` where headers are not
possible).

| direction | frame |
|---|---|
| server → worker | `{"type":"welcome","worker_id","channels":[...]}` |
| worker → server | `{"type":"publish","id":"<unique>","channel","body":<json>}` |
| server → worker | `{"type":"published","id","channel","seq","duplicate"}` |
| server → worker | `{"type":"message","channel","seq","sender","body","ts"}` |
| worker → server | `{"type":"ack","channel","seq"}` (everything up to `seq` is done) |
| server → worker | `{"type":"joined"\|"left","channel"}` |
| server → worker | `{"type":"error","id","code","message"}` (`not_member`, `too_large`, `bad_frame`, ...) |
| worker → server | `{"type":"ping","id"}` → `{"type":"pong","id"}` |

Close code `4401` means the token is invalid or was revoked, so stop
reconnecting. `4000` means the same worker connected again from somewhere else.

## Web console

`ui/` is the admin console: workspaces, workers with their waiting list,
channels, members, a live message feed per channel, and posting as `@server`.
Plain HTML, CSS and JS with no build step.

The server serves it, from the same address as the API. That is what makes
`/acme` both a page and a relay: the console knows where its relay is because
it is the same place it came from, and no CORS is involved. Opening the
deployment's root lists the workspaces it holds.

To run the whole thing locally:

```bash
pip install -e ".[server,client,dev]"
DATABASE_URL=postgresql://localhost/relay uvicorn app:app --port 8000
```

Without `DATABASE_URL` it still starts, and the first page says what is
missing rather than failing obscurely.

## Deploying the relay

The relay needs somewhere to run and a Postgres to talk to, reached through
`DATABASE_URL`. It keeps nothing of its own, so the container can be restarted
or replaced freely, and `Dockerfile` runs anywhere, taking its port from
`$PORT` where the host sets one.

**Railway.** `railway.json` selects the Dockerfile. Put a Postgres in the
**same project** and give the relay service one variable:

```
DATABASE_URL = ${{Postgres.DATABASE_URL}}
```

That is a reference to the service called `Postgres`, and it only resolves
within one project: a database in another project leaves it empty, which the
deployment then reports as having no database at all. Keeping them together
also keeps the database off the internet, and its traffic off the egress bill.

Pushing to `main` rebuilds and redeploys, the console included, since the
console is part of the image.

**Render.** `render.yaml` describes the same service; import it as a Blueprint
and set `DATABASE_URL` in the dashboard. A free instance sleeps when idle, so
a worker's poll waits for it to wake.

**Your own server.** Put it behind a TLS reverse proxy (Caddy, nginx) so workers
use `https://` and `wss://`. Tokens travel in a header, so plain HTTP exposes
them.

Wherever it runs:

- **Back up the Postgres.** It is the whole state: workspaces, workers, their
  token hashes, and every message.
- **Passwords and tokens are stored as hashes.** A workspace password that is
  lost cannot be read back, and a lost worker token can only be replaced.
- **Point a load balancer at `/readyz`, not `/healthz`.** The first is 503
  until the database answers; the second is 200 whenever the process is
  serving, so that a deployment missing a setting can still say which one.
- **`RELAY_POOL_MAX`** caps connections per instance. A managed pooler absorbs
  many instances; a plain Postgres has about a hundred connections to give out.

## Tests

```bash
python -m unittest discover tests -v
```

The end-to-end tests start a real server on a free local port and drive it
with the real client.
