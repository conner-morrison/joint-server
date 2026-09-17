"""Send what arrives on a relay channel to a Telegram chat.

    python3 telegram_alerts.py --relay https://relay.example.com/upwork \
        --channel upwork_alerts --bot-token 123456:ABC... --chat-id 987654321

Stdlib only. It dials out to both the relay and Telegram, so it runs anywhere
with internet access and needs no address of its own.

To find your chat id: message the bot once, then run with --whoami.

On first run it invents a token, asks the relay to enrol, and waits for someone
to approve it in the console.
"""
from __future__ import annotations

import argparse
import html
import json
import os
import secrets
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

TELEGRAM_LIMIT = 4096                     # characters in one message
FACTS = [("Budget", "budget"), ("Published", "published"), ("Posted", "posted")]
CLIENT_FIELDS = [("Rank", "rank"), ("Rating", "rating"), ("Payment", "paymentVerified"),
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
            return exc.code, {"description": raw.decode("utf-8", "replace")[:300]}


def esc(value: Any) -> str:
    """Telegram's HTML mode is real markup, so an ampersand or an angle bracket
    in a job title would break the message, or worse, change it."""
    return html.escape(str(value), quote=False)


def http_url(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value if value.startswith(("http://", "https://")) else None


def source_tag(body: dict[str, Any]) -> str:
    """A line saying where this came from, because an invitation is worth
    reading now and a search result is worth reading later."""
    source = str(body.get("source") or "").lower()
    kind = str(body.get("type") or "").lower()
    if kind == "invitation" or "invit" in source:
        return "\U0001f4e9 <b>INVITATION</b>"
    if source.startswith("vollna"):
        return "\U0001f50e Vollna"
    if source.startswith("upwork"):
        return "\U0001f4bc Upwork"
    return esc(source.replace("-", " ").replace("_", " ")) if source else ""


def as_telegram(body: Any) -> str:
    """One alert, formatted for a phone: what it is, what it pays, who is
    asking, and a way in. Long descriptions are cut, because a notification
    that fills the screen is not a notification."""
    if not isinstance(body, dict):
        return f"<pre>{esc(json.dumps(body, indent=2, ensure_ascii=False)[:1000])}</pre>"

    title = esc(body.get("title") or "New alert")
    link = http_url(body.get("upworkUrl") or body.get("url") or body.get("link"))
    lines = []
    tag = source_tag(body)
    if tag:
        lines.append(tag)
    lines.append(f'<b><a href="{esc(link)}">{title}</a></b>' if link else f"<b>{title}</b>")

    facts = [f"{label}: <b>{esc(body[key])}</b>" for label, key in FACTS if body.get(key)]
    if facts:
        lines.append(" · ".join(facts))

    client = body.get("client") if isinstance(body.get("client"), dict) else {}
    found = []
    for label, key in CLIENT_FIELDS:
        value = client.get(key, body.get("client" + key[0].upper() + key[1:]))
        if value in (None, ""):
            continue
        if key == "paymentVerified":
            verified = value is True or str(value).lower() in ("true", "yes", "verified")
            value = "verified" if verified else "not verified"
        found.append(f"{label}: {esc(value)}")
    if found:
        lines += ["", "<i>Client</i>", " · ".join(found)]

    for key in JD_KEYS:
        if isinstance(body.get(key), str) and body[key].strip():
            text = body[key].strip()
            room = TELEGRAM_LIMIT - sum(len(line) + 1 for line in lines) - 40
            if room > 200:
                lines += ["", esc(text[:room] + ("…" if len(text) > room else ""))]
            break
    return "\n".join(lines)[:TELEGRAM_LIMIT]


class Alerts:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.relay = args.relay.rstrip("/")
        self.bot = f"{args.api_base.rstrip('/')}/bot{args.bot_token}"
        self.state_path = Path(args.state)
        self.state = self._load()
        self.token = args.token or self.state.get("token") or secrets.token_urlsafe(32)
        self.state["token"] = self.token
        self._save()

    def _load(self) -> dict[str, Any]:
        try:
            return json.loads(self.state_path.read_text())
        except (OSError, ValueError):
            return {}

    def _save(self) -> None:
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, indent=2))
        tmp.replace(self.state_path)

    def enrol(self) -> None:
        status, body = http("POST", f"{self.relay}/enrol", {
            "worker_id": self.args.worker_id, "token": self.token, "label": self.args.label})
        if status in (200, 202):
            say(body.get("message") or body.get("status") or "asked to join")
            return
        if status == 409:
            sys.exit(f"a worker called {self.args.worker_id!r} is already registered; "
                     "choose another --worker-id")
        sys.exit(f"could not enrol: {status} {body}")

    def send(self, text: str) -> bool:
        """False means try again later. Telegram asks callers to back off when
        it is busy, and says how long to wait for; obeying that is the
        difference between a delayed alert and a blocked bot."""
        for attempt in range(1, 6):
            status, body = http("POST", f"{self.bot}/sendMessage", {
                "chat_id": self.args.chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }, timeout=30)
            if status == 200 and body.get("ok"):
                return True
            if status == 429:
                pause = float(body.get("parameters", {}).get("retry_after", 5))
                say(f"telegram is rate limiting; waiting {pause:g}s")
                time.sleep(pause)
                continue
            if status in (400, 403):
                # About this message or this chat, not the connection. Retrying
                # would stop every later alert behind a single bad one.
                say(f"telegram refused the message: {body.get('description', body)}")
                return True
            say(f"telegram said {status} ({body.get('description', '')}); attempt {attempt}")
            time.sleep(min(2 ** attempt, 30))
        return False

    def run(self) -> None:
        say(f"watching {self.relay} as {self.args.worker_id}, sending to chat {self.args.chat_id}")
        announced = False
        while True:
            try:
                status, body = http("GET", f"{self.relay}/messages?wait={self.args.wait}",
                                    token=self.token, timeout=self.args.wait + 20)
            except OSError as exc:
                say(f"relay unreachable ({exc}); retrying in 10s")
                time.sleep(10)
                continue

            if status == 401:
                self.enrol()
                time.sleep(self.args.retry)
                continue
            if status == 403:
                if not announced:
                    say("waiting to be approved in the console")
                    announced = True
                time.sleep(self.args.retry)
                continue
            if status != 200:
                say(f"relay said {status}: {body}; retrying")
                time.sleep(self.args.retry)
                continue
            announced = False

            for msg in body.get("messages", []):
                if self.args.channel and msg.get("channel") != self.args.channel:
                    continue
                if not self.send(as_telegram(msg.get("body"))):
                    time.sleep(self.args.retry)
                    break
                # Acknowledged only once it has been sent, so an alert is never
                # lost by being read.
                http("POST", f"{self.relay}/ack",
                     {"channel": msg["channel"], "seq": msg["seq"]}, token=self.token)
                say(f"sent #{msg['seq']} from {msg['channel']}")


def whoami(args: argparse.Namespace) -> None:
    """Chat ids are not shown anywhere in Telegram, so this reads one from the
    messages people have already sent the bot."""
    status, body = http("GET", f"{args.api_base.rstrip('/')}/bot{args.bot_token}/getUpdates")
    if status != 200 or not body.get("ok"):
        sys.exit(f"telegram said {status}: {body}")
    seen = {}
    for update in body.get("result", []):
        chat = (update.get("message") or update.get("channel_post") or {}).get("chat") or {}
        if chat.get("id"):
            seen[chat["id"]] = chat.get("title") or chat.get("username") or chat.get("first_name", "")
    if not seen:
        sys.exit("no chats yet: send your bot a message, then run this again")
    for chat_id, name in seen.items():
        print(f"--chat-id {chat_id}    {name}")


def say(text: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {text}", flush=True)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--relay", default=os.environ.get("RELAY_URL", ""))
    p.add_argument("--channel", default="upwork_alerts", help="'' for every channel it is in")
    p.add_argument("--bot-token", default=os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    p.add_argument("--chat-id", default=os.environ.get("TELEGRAM_CHAT_ID", ""))
    p.add_argument("--whoami", action="store_true", help="list chat ids the bot can see, and exit")
    p.add_argument("--worker-id", default=os.environ.get("RELAY_WORKER", "telegram"))
    p.add_argument("--label", default="telegram alerts")
    p.add_argument("--token", default=os.environ.get("RELAY_TOKEN", ""))
    p.add_argument("--state", default=os.environ.get("TELEGRAM_STATE", "telegram-alerts.json"))
    p.add_argument("--api-base", default=os.environ.get("TELEGRAM_API", "https://api.telegram.org"))
    p.add_argument("--wait", type=float, default=25)
    p.add_argument("--retry", type=float, default=10)
    args = p.parse_args()

    if not args.bot_token:
        sys.exit("give --bot-token, from @BotFather in Telegram")
    if args.whoami:
        return whoami(args)
    if not args.relay:
        sys.exit("give --relay, the workspace address the alerts are in")
    if not args.chat_id:
        sys.exit("give --chat-id, or run with --whoami to find it")

    try:
        Alerts(args).run()
    except KeyboardInterrupt:
        say("stopped")


if __name__ == "__main__":
    main()
