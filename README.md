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
| `POST` | `/api/channels/{name}/messages` `{"body"}` | post as `@server` |
| `GET` | `/api/stream` | server-sent events: `hello`, `worker` (online/offline), `message`, `changed`, `resync` |

The CLI (`python -m relay worker|channel|join|leave`) writes to the database
directly and works with the server stopped. Through the API, a connected worker
hears about changes immediately, and a removed worker is disconnected.

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

## Web console on Vercel

`ui/` is a static admin console: workers with live online status, channels,
members, a live message feed per channel, and posting as `@server`. It is
plain HTML, CSS and JS with no build step.

**Vercel hosts the console only, not the relay.** Vercel runs short-lived
serverless functions. The relay needs long-lived websockets and a database file
on disk, so it runs on an ordinary server (any VPS) behind HTTPS. The console
in the browser talks to that server directly.

```
browser ── https://your-console.vercel.app   (static files from ui/)
   │
   └── https://relay.example.com/api ...     (your relay server, CORS-allowed)
```

1. **Allow the console's origin on the relay**, then restart it:

   ```bash
   export RELAY_CORS_ORIGINS="https://your-console.vercel.app,https://your-console-*.vercel.app"
   python -m relay serve --port 8700
   ```

   The `*` entry covers Vercel preview deployments. The console calls the API
   with the admin token in a header, so no cookies are involved.

2. **Deploy `ui/`.** Either with the CLI:

   ```bash
   cd ui
   npx vercel deploy --prod
   ```

   or by importing the repository in the Vercel dashboard with **Root Directory**
   set to `ui`, **Framework Preset** set to *Other*, and no build command.

3. **Open the console**, enter the relay's `https://` URL and `RELAY_ADMIN_TOKEN`.
   To pre-fill the URL, set it in `ui/config.js` or open the console with
   `?server=https://relay.example.com`.

The relay must be served over HTTPS. Vercel serves the console over HTTPS, and
browsers block calls from an HTTPS page to a plain `http://` server
(`localhost` excepted).

`ui/vercel.json` sets a Content-Security-Policy that only runs the console's
own scripts. That matters because the console displays message bodies written
by workers, and it inserts them as text, never as HTML.

To try the console locally:

```bash
RELAY_CORS_ORIGINS=http://127.0.0.1:5500 python -m relay serve --port 8700
python3 -m http.server 5500 --directory ui     # open http://127.0.0.1:5500
```

## Deploying

- Put it behind a TLS reverse proxy (Caddy, nginx) so workers use `https://` and
  `wss://`. Tokens travel in a header, so plain HTTP exposes them.
- Hold on to `RELAY_ADMIN_TOKEN`. Worker tokens are only stored as hashes, so a
  lost worker token can only be replaced (`worker token <id>`), not recovered.
- Back up `relay.db` (`RELAY_DB` sets its path). The database is the whole
  state of the server.
- One process only. Live routing is held in memory, so do not run several
  workers of the server behind a load balancer.

## Tests

```bash
python -m unittest discover tests -v
```

The end-to-end tests start a real server on a free local port and drive it
with the real client.
