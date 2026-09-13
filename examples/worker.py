"""A minimal worker: prints what arrives on its channels and posts what you type.

    python examples/worker.py --url http://127.0.0.1:8700 --token <token> --channel jobs

Each line you type is published to --channel. A line that parses as JSON is
sent as that JSON; anything else is sent as {"text": line}.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from relay import Message, PublishError, RelayClient  # noqa: E402


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=os.environ.get("RELAY_URL", "http://127.0.0.1:8700"))
    p.add_argument("--token", default=os.environ.get("RELAY_TOKEN", ""))
    p.add_argument("--channel", required=True, help="channel that typed lines are posted to")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    client = RelayClient(args.url, args.token)

    @client.on()
    async def show(msg: Message) -> None:
        # flush: when output goes to a file or another process, Python buffers it in blocks
        print(f"[{msg.channel} #{msg.seq}] {msg.sender}: {json.dumps(msg.body, ensure_ascii=False)}", flush=True)

    async def read_stdin() -> None:
        loop = asyncio.get_running_loop()
        while line := await loop.run_in_executor(None, sys.stdin.readline):
            line = line.strip()
            if not line:
                continue
            try:
                body = json.loads(line)
            except ValueError:
                body = {"text": line}
            try:
                seq = await client.publish(args.channel, body)
                print(f"  sent as #{seq}", flush=True)
            except PublishError as exc:
                print(f"  not sent: {exc}", flush=True)
        await client.close()

    stdin_task = asyncio.create_task(read_stdin())
    try:
        await client.run()
    finally:
        stdin_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
