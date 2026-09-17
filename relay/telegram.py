"""Delivering a channel's messages to a Telegram chat, from the server itself.

A worker connects, holds a poll open and is approved by a person. A bot does
none of that: it is a place the server sends to, registered against a channel
the way a member is added, and removed the same way. There is nothing to
enrol, because nothing ever arrives from it.

Delivery keeps the same promise as everywhere else. Each registration has a
cursor, a message is sent before the cursor moves past it, and a send that
fails leaves the cursor alone, so an alert is never lost by being read.
"""
from __future__ import annotations

import asyncio
import html
import json
import logging
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any

log = logging.getLogger("relay.telegram")

API = "https://api.telegram.org"
LIMIT = 4096                              # characters Telegram accepts in one message
FACTS = [("Budget", "budget"), ("Terms", "terms"), ("Published", "published"),
         ("Posted", "posted")]
CLIENT_FIELDS = [("Rank", "rank"), ("Rating", "rating"), ("Payment", "paymentVerified"),
                 ("Location", "location"), ("Reviews", "reviews"), ("Jobs posted", "jobsPosted"),
                 ("Hire rate", "hireRate"), ("Spent", "spent"), ("Registered", "registered")]
LINK_KEYS = ("upworkUrl", "inviteUrl", "url", "link")
JD_KEYS = ("description", "jobDescription", "jd", "snippet", "summary", "details", "text")
INVITATION_WORDS = ("invit", "interview", "asked you to apply", "wants to interview")


def esc(value: Any) -> str:
    """Telegram's HTML mode is real markup: an ampersand in a job title would
    break the message, and angle brackets would change it."""
    return html.escape(str(value), quote=False)


def http_url(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value if value.startswith(("http://", "https://")) else None


def when(value: Any) -> str:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).strftime("%d %b %H:%M")
    except (ValueError, TypeError):
        return str(value)


def source_tag(body: dict[str, Any]) -> str:
    """Where it came from decides how much attention it deserves: a client
    asking for you is worth reading now, a search that matched is not."""
    source = str(body.get("source") or "").lower()
    kind = str(body.get("type") or "").lower()
    said = f"{body.get('title') or ''} {body.get('emailSubject') or ''}".lower()
    # Guessed from the subject only when nothing better is known: a parsed job
    # is a job however it is worded.
    guessable = kind != "job" and not source.startswith("vollna")
    if kind == "invitation" or "invit" in source or (
            guessable and any(word in said for word in INVITATION_WORDS)):
        return "\U0001f4e9 <b>INVITATION</b>"
    if source.startswith("vollna"):
        return "\U0001f50e Vollna"
    if source.startswith("upwork"):
        return "\U0001f4bc Upwork"
    return esc(source.replace("-", " ").replace("_", " ")) if source else ""


def render(body: Any, channel: str = "") -> str:
    """One message, formatted for a phone: what it is, what it pays, who is
    asking, and a way in."""
    if not isinstance(body, dict):
        return f"<pre>{esc(json.dumps(body, indent=2, ensure_ascii=False)[:1000])}</pre>"

    lines: list[str] = []
    tag = source_tag(body)
    if tag:
        lines.append(f"{tag}{f'  ·  #{esc(channel)}' if channel else ''}")

    title = esc(body.get("title") or body.get("emailSubject") or "New message")
    link = http_url(next((body[k] for k in LINK_KEYS if body.get(k)), None))
    lines.append(f'<b><a href="{esc(link)}">{title}</a></b>' if link else f"<b>{title}</b>")

    facts = [f"{label}: <b>{esc(body[key])}</b>" for label, key in FACTS if body.get(key)]
    if body.get("receivedAt"):
        facts.append(esc(when(body["receivedAt"])))
    if facts:
        lines.append(" · ".join(facts))

    client = body.get("client") if isinstance(body.get("client"), dict) else {}
    about = []
    for label, key in CLIENT_FIELDS:
        value = client.get(key, body.get("client" + key[0].upper() + key[1:]))
        if value in (None, ""):
            continue
        if key == "paymentVerified":
            value = "verified" if value is True or str(value).lower() in (
                "true", "yes", "verified") else "not verified"
        about.append(f"{label}: {esc(value)}")
    if about:
        lines += ["", "<i>Client</i>", " · ".join(about)]

    for key in JD_KEYS:
        if isinstance(body.get(key), str) and body[key].strip():
            text = body[key].strip()
            room = LIMIT - sum(len(line) + 1 for line in lines) - 40
            if room > 200:
                lines += ["", esc(text[:room] + ("…" if len(text) > room else ""))]
            break
    return "\n".join(lines)[:LIMIT]


class TelegramError(Exception):
    """A send that failed. `permanent` means about this message or this chat
    rather than about the connection, so retrying it would only hold up
    everything behind it."""

    def __init__(self, message: str, *, permanent: bool = False, retry_after: float = 0.0):
        super().__init__(message)
        self.permanent = permanent
        self.retry_after = retry_after


def _post(url: str, payload: dict[str, Any], timeout: float) -> tuple[int, Any]:
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
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


async def send(bot_token: str, chat_id: str, text: str, *, api: str | None = None,
               timeout: float = 20.0) -> None:
    """Send one message, or raise. Blocking work runs on a thread so a slow
    Telegram does not stop the server answering anyone else.

    `api` is read when called rather than bound as a default, so a test can
    point the whole module somewhere else and have it mean something."""
    status, body = await asyncio.to_thread(
        _post, f"{(api or API).rstrip('/')}/bot{bot_token}/sendMessage",
        {"chat_id": chat_id, "text": text, "parse_mode": "HTML",
         "disable_web_page_preview": True}, timeout)
    if status == 200 and isinstance(body, dict) and body.get("ok"):
        return
    said = body.get("description", body) if isinstance(body, dict) else body
    if status == 429:
        after = float((body or {}).get("parameters", {}).get("retry_after", 5))
        raise TelegramError(f"rate limited: {said}", retry_after=after)
    # 400 is about the message, 403 about the chat: neither improves by waiting.
    raise TelegramError(f"telegram said {status}: {said}", permanent=status in (400, 403))


async def check(bot_token: str, *, api: str | None = None,
                timeout: float = 15.0) -> dict[str, Any]:
    """Whether a token is a bot at all, so a typo is caught while someone is
    still looking at the form rather than in a log nobody reads."""
    status, body = await asyncio.to_thread(
        _post, f"{(api or API).rstrip('/')}/bot{bot_token}/getMe", {}, timeout)
    if status == 200 and isinstance(body, dict) and body.get("ok"):
        return body.get("result") or {}
    said = (body or {}).get("description") if isinstance(body, dict) else None
    raise TelegramError(said or f"telegram said {status}", permanent=True)
