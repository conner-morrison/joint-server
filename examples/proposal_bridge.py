"""Feed jobs from a relay channel into a local proposal-writer.

proposal-writer listens on localhost, so nothing outside the machine it runs
on can reach it. This runs beside it and dials out to the relay instead:
neither end needs an address anyone can find, and no firewall has to be opened.

    python3 proposal_bridge.py --relay https://relay-xxx.up.railway.app/upwork \
        --channel jobs --person zachary --guide general

Stdlib only, so it runs wherever python3 does, with nothing to install.

On first run it invents a token, asks the relay to enrol, and waits: a person
approves it once in the console and it carries on by itself from then on.
"""
from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

FACTS = [("Budget", "budget"), ("Published", "published"), ("Posted", "posted")]
CLIENT_FIELDS = [("Rank", "rank"), ("Rating", "rating"), ("Payment verified", "paymentVerified"),
                 ("Location", "location"), ("Reviews", "reviews"), ("Jobs posted", "jobsPosted"),
                 ("Hire rate", "hireRate"), ("Spent", "spent"), ("Registered", "registered")]
JD_KEYS = ("description", "jobDescription", "jd", "snippet", "summary", "details", "text")


def http(method: str, url: str, body: Any = None, token: str = "", timeout: float = 60.0
         ) -> tuple[int, Any]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as res:
            return res.status, json.loads(res.read() or b"null")
    except urllib.error.HTTPError as exc:
        with exc:
            raw = exc.read()
        try:
            return exc.code, json.loads(raw or b"null")
        except ValueError:
            return exc.code, {"detail": raw.decode("utf-8", "replace")[:300]}


def as_jd(body: Any) -> str:
    """A job description proposal-writer can work from.

    Its first line becomes the job's title, so the title goes first. The rest
    is what a person would want to read before writing a proposal, in the order
    they would want it: the posting itself, then the terms, then who is
    offering them.
    """
    if not isinstance(body, dict):
        return json.dumps(body, indent=2, ensure_ascii=False)

    lines: list[str] = [str(body.get("title") or "Untitled job").strip()]
    for key in JD_KEYS:
        if isinstance(body.get(key), str) and body[key].strip():
            lines += ["", body[key].strip()]
            break

    terms = [f"{label}: {body[key]}" for label, key in FACTS if body.get(key)]
    link = body.get("upworkUrl") or body.get("url") or body.get("link")
    if link:
        terms.append(f"Link: {link}")
    if terms:
        lines += ["", *terms]

    client = body.get("client") if isinstance(body.get("client"), dict) else {}
    facts = []
    for label, key in CLIENT_FIELDS:
        flat = body.get("client" + key[0].upper() + key[1:])
        value = client.get(key, flat)
        if value not in (None, ""):
            facts.append(f"{label}: {value}")
    if facts:
        lines += ["", "Client:", *[f"- {f}" for f in facts]]
    return "\n".join(lines)


class Bridge:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.relay = args.relay.rstrip("/")
        self.state_path = Path(args.state)
        self.state = self._load_state()
        self.token = args.token or self.state.get("token") or secrets.token_urlsafe(32)
        self.state["token"] = self.token
        self._save_state()

    def _load_state(self) -> dict[str, Any]:
        try:
            return json.loads(self.state_path.read_text())
        except (OSError, ValueError):
            return {}

    def _save_state(self) -> None:
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, indent=2))
        tmp.replace(self.state_path)              # atomic: never a half-written file

    def enrol(self) -> None:
        status, body = http("POST", f"{self.relay}/enrol", {
            "worker_id": self.args.worker_id,
            "token": self.token,
            "label": self.args.label,
        })
        if status in (200, 202):
            say(f"{body.get('status', 'pending')}: {body.get('message', '')}".strip(": "))
            return
        if status == 409:
            sys.exit(f"the relay already has a worker called {self.args.worker_id!r}. "
                     "Choose another --worker-id, or remove it in the console.")
        sys.exit(f"could not enrol: {status} {body}")

    def deliver(self, msg: dict[str, Any]) -> bool:
        """Hand one message to proposal-writer. False means try again later,
        so a local server that is down or restarting loses nothing."""
        jd = as_jd(msg.get("body"))
        payload = {"guide": self.args.guide, "person": self.args.person, "jd": jd}
        try:
            status, body = http("POST", f"{self.args.local.rstrip('/')}/api/generate", payload,
                                timeout=self.args.local_timeout)
        except OSError as exc:
            say(f"proposal-writer is not answering ({exc}); will try #{msg['seq']} again")
            return False
        if status != 200:
            # A rejection is about this message, not the connection: retrying
            # forever would stop everything behind it.
            say(f"proposal-writer refused #{msg['seq']}: {status} {body}")
            return True
        made = body.get("job") or body.get("mode") or "written"
        say(f"#{msg['seq']} {as_title(jd)} -> {made}")
        return True

    def run(self) -> None:
        say(f"watching {self.relay} as {self.args.worker_id}, feeding {self.args.local}")
        waiting_since = 0.0
        while True:
            try:
                status, body = http(
                    "GET", f"{self.relay}/messages?wait={self.args.wait}",
                    token=self.token, timeout=self.args.wait + 20)
            except OSError as exc:
                say(f"relay unreachable ({exc}); retrying in 10s")
                time.sleep(10)
                continue

            if status == 401:
                # Unknown here. Ask, then wait for a person to say yes.
                self.enrol()
                time.sleep(self.args.retry)
                continue
            if status == 403:
                if not waiting_since:
                    waiting_since = time.time()
                    say("waiting to be approved in the console")
                time.sleep(self.args.retry)
                continue
            if status != 200:
                say(f"relay said {status}: {body}; retrying in {self.args.retry}s")
                time.sleep(self.args.retry)
                continue

            if waiting_since:
                say(f"approved after {time.time() - waiting_since:.0f}s")
                waiting_since = 0.0

            for msg in body.get("messages", []):
                if self.args.channel and msg.get("channel") != self.args.channel:
                    continue
                if not self.deliver(msg):
                    time.sleep(self.args.retry)
                    break
                # Acknowledged only once it is somewhere else, so a crash in
                # between means the job is seen again rather than lost.
                http("POST", f"{self.relay}/ack",
                     {"channel": msg["channel"], "seq": msg["seq"]}, token=self.token)


def as_title(jd: str) -> str:
    first = jd.splitlines()[0] if jd else ""
    return first[:60] + ("..." if len(first) > 60 else "")


def say(text: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {text}", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--relay", default=os.environ.get("RELAY_URL", ""),
                   help="the workspace address, e.g. https://relay.example.com/upwork")
    p.add_argument("--local", default=os.environ.get("PROPOSAL_UI", "http://127.0.0.1:8765"),
                   help="where proposal-writer is listening")
    p.add_argument("--channel", default="jobs", help="only forward this channel; '' for all")
    p.add_argument("--worker-id", default=os.environ.get("RELAY_WORKER", "proposal-writer"))
    p.add_argument("--label", default="proposal-writer bridge")
    p.add_argument("--guide", default=os.environ.get("PROPOSAL_GUIDE", "general"))
    p.add_argument("--person", default=os.environ.get("PROPOSAL_PERSON", ""),
                   help="which profile writes the proposal")
    p.add_argument("--token", default=os.environ.get("RELAY_TOKEN", ""),
                   help="this worker's token; one is made and kept if not given")
    p.add_argument("--state", default=os.environ.get("BRIDGE_STATE", "proposal-bridge.json"))
    p.add_argument("--wait", type=float, default=25, help="seconds to hold each poll open")
    p.add_argument("--retry", type=float, default=10)
    p.add_argument("--local-timeout", type=float, default=120,
                   help="proposal-writer may be calling a model, which is not quick")
    args = p.parse_args()
    if not args.relay:
        sys.exit("give --relay, the workspace address the jobs are in")
    if not args.person:
        sys.exit("give --person: whose profile the proposals are written from")

    bridge = Bridge(args)
    try:
        bridge.run()
    except KeyboardInterrupt:
        say("stopped")


if __name__ == "__main__":
    main()
