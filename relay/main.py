"""Command line: run the server, and manage workers, channels and memberships.

The management commands write straight to the database file, so they work
whether or not the server is running. Changes made this way reach a connected
worker at its next delivery; use the HTTP API when a connected worker should be
told at once, or when a removed worker must be disconnected immediately.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time

from relay.store import Store


def _ago(ts: float | None) -> str:
    if not ts:
        return "never"
    secs = int(time.time() - ts)
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if secs >= size:
            return f"{secs // size}{unit} ago"
    return f"{secs}s ago"


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="relay", description="Channel relay between workers.")
    p.add_argument("--db", default=os.environ.get("RELAY_DB", "relay.db"), help="database file (env RELAY_DB)")
    sub = p.add_subparsers(dest="cmd", required=True)

    serve = sub.add_parser("serve", help="run the server (needs RELAY_ADMIN_TOKEN)")
    serve.add_argument("--host", default="0.0.0.0")
    serve.add_argument("--port", type=int, default=8700)
    serve.add_argument("--retention-days", type=float, default=float(os.environ.get("RELAY_RETENTION_DAYS", "7")),
                       help="delete messages older than this; 0 keeps them for ever")
    serve.add_argument("--max-body-bytes", type=int, default=256_000)
    serve.add_argument("--cors-origin", action="append",
                       default=[o for o in os.environ.get("RELAY_CORS_ORIGINS", "").split(",") if o.strip()],
                       help="origin allowed to call the admin API from a browser, e.g. the Vercel console; "
                            "repeatable, '*' wildcards allowed (env RELAY_CORS_ORIGINS, comma-separated)")

    worker = sub.add_parser("worker", help="manage workers").add_subparsers(dest="action", required=True)
    add = worker.add_parser("add", help="register a worker and print its token")
    add.add_argument("worker_id")
    add.add_argument("--label", default="")
    worker.add_parser("rm", help="remove a worker").add_argument("worker_id")
    worker.add_parser("token", help="issue a new token; the old one stops working").add_argument("worker_id")
    worker.add_parser("ls", help="list workers")

    channel = sub.add_parser("channel", help="manage channels").add_subparsers(dest="action", required=True)
    cadd = channel.add_parser("add", help="create a channel")
    cadd.add_argument("name")
    cadd.add_argument("--description", default="")
    channel.add_parser("rm", help="delete a channel and its messages").add_argument("name")
    channel.add_parser("ls", help="list channels")

    join = sub.add_parser("join", help="add workers to a channel")
    join.add_argument("channel")
    join.add_argument("worker_ids", nargs="+")
    join.add_argument("--from-start", action="store_true", help="also deliver the messages the channel still holds")
    leave = sub.add_parser("leave", help="remove workers from a channel")
    leave.add_argument("channel")
    leave.add_argument("worker_ids", nargs="+")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.cmd == "serve":
        token = os.environ.get("RELAY_ADMIN_TOKEN", "")
        if not token:
            print("set RELAY_ADMIN_TOKEN first, e.g.\n"
                  '  export RELAY_ADMIN_TOKEN=$(python3 -c "import secrets;print(secrets.token_urlsafe(24))")',
                  file=sys.stderr)
            return 2
        import uvicorn

        from relay.app import create_app
        app = create_app(Store(args.db), token, retention_days=args.retention_days,
                         max_body_bytes=args.max_body_bytes, cors_origins=args.cors_origin)
        if args.cors_origin:
            logging.getLogger("relay").info("browser consoles allowed from: %s", ", ".join(args.cors_origin))
        # Open console streams never end on their own; do not let them hold up a shutdown.
        uvicorn.run(app, host=args.host, port=args.port, timeout_graceful_shutdown=5)
        return 0

    store = Store(args.db)
    try:
        if args.cmd == "worker":
            if args.action == "add":
                token = store.add_worker(args.worker_id, args.label)
                print(f"worker {args.worker_id} registered. Its token (shown once):\n{token}")
            elif args.action == "rm":
                if not store.remove_worker(args.worker_id):
                    raise LookupError(f"no worker {args.worker_id!r}")
                print(f"removed worker {args.worker_id}")
            elif args.action == "token":
                token = store.rotate_token(args.worker_id)
                if token is None:
                    raise LookupError(f"no worker {args.worker_id!r}")
                print(f"new token for {args.worker_id} (shown once):\n{token}")
            else:
                for w in store.list_workers():
                    print(f"{w['worker_id']:<24} seen {_ago(w['last_seen']):<10} "
                          f"channels: {', '.join(w['channels']) or '-'}  {w['label']}")
        elif args.cmd == "channel":
            if args.action == "add":
                store.add_channel(args.name, args.description)
                print(f"created channel {args.name}")
            elif args.action == "rm":
                if not store.remove_channel(args.name):
                    raise LookupError(f"no channel {args.name!r}")
                print(f"deleted channel {args.name}")
            else:
                for c in store.list_channels():
                    print(f"{c['name']:<24} {c['messages']:>6} msgs  "
                          f"members: {', '.join(c['members']) or '-'}  {c['description']}")
        elif args.cmd == "join":
            for worker_id in args.worker_ids:
                added = store.join(args.channel, worker_id, from_start=args.from_start)
                print(f"{worker_id} {'joined' if added else 'is already in'} {args.channel}")
        elif args.cmd == "leave":
            for worker_id in args.worker_ids:
                left = store.leave(args.channel, worker_id)
                print(f"{worker_id} {'left' if left else 'was not in'} {args.channel}")
    except (ValueError, LookupError) as exc:
        print(f"error: {exc.args[0]}", file=sys.stderr)
        return 1
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
