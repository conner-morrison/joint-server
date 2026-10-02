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
MIN_JD = 320                              # too little room left to begin a description in
# Decision first: where a worker has judged a job, that judgement is what the
# notification is for.
FACTS = [("Decision", "decision"), ("Verdict", "verdict"), ("Reason", "reason"),
         ("Budget", "budget"), ("Terms", "terms"), ("Published", "published"),
         ("Posted", "posted"), ("Project", "projectId")]
CLIENT_FIELDS = [("Name", "name"), ("Rank", "rank"), ("Rating", "rating"),
                 ("Payment", "paymentVerified"),
                 ("Location", "location"), ("Reviews", "reviews"), ("Jobs posted", "jobsPosted"),
                 ("Hire rate", "hireRate"), ("Spent", "spent"), ("Registered", "registered")]
NAMED_LINKS = (("published", "Published"), ("deployed", "Published"), ("live", "Published"),
               ("demo", "Demo"), ("source", "Source"), ("repo", "Source"),
               ("repository", "Source"), ("homepage", "Homepage"),
               ("upworkUrl", "Upwork"), ("job_url", "Upwork"), ("jobUrl", "Upwork"),
               ("jobLink", "Upwork"), ("inviteUrl", "Invitation"),
               ("html_url", "Link"), ("htmlUrl", "Link"), ("url", "Link"), ("link", "Link"))
LINK_KEYS = tuple(key for key, _ in NAMED_LINKS)
JD_KEYS = ("description", "jobDescription", "job_description", "jd", "snippet", "summary",
           "details", "text")
# What a worker called the job. Each writes what is natural to it, and one that
# spells it job_title is not sending something less worth reading.
TITLE_KEYS = ("title", "job_title", "jobTitle", "jobName", "job_name", "heading", "name",
              "subject", "emailSubject")
CLIENT_NAME_KEYS = ("client_name", "clientName", "company", "buyer")

# A worker may send a job as a block of labelled text rather than as fields:
# "Job title: …" on one line, "Job description:" and then the posting. It is
# the same job, said differently, and reading the labels is all it takes to
# show it the way any other job is shown.
#
# Only these labels start a field. A job description is full of lines like
# "Community Engagement: identify niche communities", and treating every
# colon as a label would chop the description into nonsense.
LABELS = (
    (("job title", "title"), "title"),
    (("job link", "job url", "link", "url"), "url"),
    (("job description", "description", "jd"), "description"),
    (("client name", "client"), "clientName"),
    (("decision",), "decision"),
    (("reason", "why"), "reason"),
    (("project id", "projectid"), "projectId"),
    (("job id", "jobid"), "jobId"),
    (("budget", "rate"), "budget"),
    (("published",), "published"),
    (("posted",), "posted"),
)
LABEL_OF = {said: key for names, key in LABELS for said in names}
LABEL_MAX = 24                            # characters before the colon


def labelled(text: Any) -> dict[str, Any] | None:
    """A job written as labelled lines, read back as fields.

    A label only counts the first time it appears: a description that happens
    to say "Reason:" partway through is still the description.
    """
    if not isinstance(text, str) or ":" not in text:
        return None
    found: dict[str, Any] = {}
    lead: list[str] = []
    current: str | None = None
    for line in text.splitlines():
        at = line.find(":")
        key = None
        if 0 < at <= LABEL_MAX:
            key = LABEL_OF.get(line[:at].strip().lower())
            if key in found:
                key = None                # said once; after that it is prose
        if key:
            current = key
            found[key] = line[at + 1:].strip()
        elif current:
            found[current] = f"{found[current]}\n{line}"
        else:
            lead.append(line)
    if len(found) < 2:
        return None
    for key in found:
        found[key] = found[key].strip()
    # Anything said before the first label is the posting speaking for itself.
    said = "\n".join(lead).strip()
    if said and not found.get("description"):
        found["description"] = said
    return found
INVITATION_WORDS = ("invit", "interview", "asked you to apply", "wants to interview")

# Sent when a bot is registered. It is the test as well as the greeting: if
# this arrives, the token and the chat id are both right, which is more than
# asking Telegram whether the token exists would have proved.
WELCOME = ("\u2705 <b>Connected</b>\n"
           "This chat is now registered with your relay. "
           "Add this bot to a channel and what arrives there will be sent here.")


def esc(value: Any) -> str:
    """Telegram's HTML mode is real markup: an ampersand in a job title would
    break the message, and angle brackets would change it."""
    return html.escape(str(value), quote=False)


def http_url(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value if value.startswith(("http://", "https://")) else None


def label_for(url: str) -> str:
    """What to call a link when the publisher did not say."""
    rest = url.split("://", 1)[-1]
    host, _, path = rest.partition("/")
    tail = "/".join([p for p in path.split("/") if p][-2:])
    host = host.removeprefix("www.")
    return f"{host}/{tail}" if tail else host


def links_of(body: dict[str, Any]) -> list[tuple[str, str]]:
    """An item may carry no link, one, or several. Collected from wherever they
    were put, http and https only, in order and without repeats."""
    found: list[tuple[str, str]] = []
    seen: set[str] = set()

    def take(value: Any, label: str = "") -> None:
        if isinstance(value, list):
            for item in value:
                take(item)
            return
        if isinstance(value, dict):
            return take(value.get("url") or value.get("href") or value.get("link"),
                        value.get("label") or value.get("title") or value.get("name") or "")
        url = http_url(value)
        if not url or url in seen:
            return
        seen.add(url)
        found.append((url, label or label_for(url)))

    for key, label in NAMED_LINKS:
        take(body.get(key), "" if label == "Link" else label)
    take(body.get("links"))
    take(body.get("urls"))
    return found


def when(value: Any) -> str:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).strftime("%d %b %H:%M")
    except (ValueError, TypeError):
        return str(value)


def source_tag(body: dict[str, Any]) -> str:
    """Where it came from decides how much attention it deserves: a client
    asking for you is worth reading now, a search that matched is not."""
    source = "" if http_url(body.get("source")) else str(body.get("source") or "").lower()
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


def fit(text: str, budget: int) -> tuple[str, str]:
    """As much of `text` as fits in `budget` characters once escaped, and what
    is left over. Broken at the end of a line where there is one, otherwise
    between words, so a description continues mid-sentence only when a single
    word is longer than a whole message.

    Escaping is what decides the length: an ampersand becomes five characters,
    so measuring the text as typed would overrun and Telegram would refuse the
    message rather than shorten it.
    """
    if len(esc(text)) <= budget:
        return text, ""
    low, high = 0, len(text)
    while low < high:
        mid = (low + high + 1) // 2
        if len(esc(text[:mid])) <= budget:
            low = mid
        else:
            high = mid - 1
    window = text[:low]
    at = window.rfind("\n")
    if at < budget // 4:
        at = window.rfind(" ")
    if at < budget // 4:
        at = low                        # one unbroken run of characters
    return text[:at].rstrip(), text[at:].lstrip()


def title_of(body: dict[str, Any]) -> str:
    for key in TITLE_KEYS:
        said = body.get(key)
        if isinstance(said, str) and said.strip():
            return said.strip()
    return ""


def client_of(body: dict[str, Any]) -> dict[str, Any]:
    """The client, whether it arrived nested or as flat clientRank-style keys.
    A plain string under `client` is a name, which is the one field a worker
    that knows little else about a client still tends to have."""
    nested = body.get("client") if isinstance(body.get("client"), dict) else {}
    found: dict[str, Any] = {}
    for _, key in CLIENT_FIELDS:
        value = nested.get(key, body.get("client" + key[0].upper() + key[1:]))
        if value not in (None, ""):
            found[key] = value
    if isinstance(body.get("client"), str) and body["client"].strip():
        found["name"] = body["client"].strip()
    if not found.get("name"):
        for key in CLIENT_NAME_KEYS:
            said = body.get(key)
            if isinstance(said, str) and said.strip():
                found["name"] = said.strip()
                break
    return found


def render(body: Any, channel: str = "", source: str = "") -> list[str]:
    """One job as one message: what it is, what it pays, who is asking, and a
    way in, with the description under it.

    A list, because Telegram will not carry more than LIMIT characters and a
    job description is sometimes longer than that. Cutting it was the wrong
    answer - the part that got cut is as much the job as the part that fit - so
    a long description continues into the message after it. Everything a job
    says arrives; only rarely in one piece.
    """
    # A job sent as labelled text is a job. Read it into fields and it is shown
    # the way every other job is, rather than as a wall of escaped newlines.
    if isinstance(body, str):
        body = labelled(body) or body
    elif isinstance(body, dict) and not title_of(body):
        for key in ("text", "message", "raw"):
            read = labelled(body.get(key))
            if read:
                body = {**body, **read}
                break
    if not isinstance(body, dict):
        return [f"<pre>{esc(json.dumps(body, indent=2, ensure_ascii=False)[:1000])}</pre>"]

    lines: list[str] = []
    # The channel is named even when nothing else about the source is known.
    # Two notifications for one job are two channels carrying it, or two bots
    # sending from one - and a line that says which turns that from a mystery
    # into something a person can see and fix.
    # Who published it is part of what it is: two workers watch the same job
    # boards with different rules, and which one found this decides how much
    # the decision on it is worth.
    where = "  ·  ".join(part for part in (
        source_tag(body), esc(source) if source else "",
        f"#{esc(channel)}" if channel else "") if part)
    if where:
        lines.append(where)

    title = esc(title_of(body) or "New message")
    links = links_of(body)
    link = links[0][0] if links else None
    lines.append(f'<b><a href="{esc(link)}">{title}</a></b>' if link else f"<b>{title}</b>")
    # The first link is the title. The rest are worth their own line, because
    # an item with several is an item where the extra ones matter.
    if len(links) > 1:
        lines.append(" · ".join(f'<a href="{esc(url)}">{esc(label)}</a>'
                                for url, label in links[1:]))

    # `published` is a time on a job and a URL on a reply. A URL is a link,
    # already shown as one, and not a fact to repeat.
    facts = [f"{label}: <b>{esc(body[key])}</b>" for label, key in FACTS
             if body.get(key) and not http_url(body.get(key))]
    if body.get("receivedAt"):
        facts.append(esc(when(body["receivedAt"])))
    if facts:
        lines.append(" · ".join(facts))

    client = client_of(body)
    about = []
    for label, key in CLIENT_FIELDS:
        value = client.get(key)
        if value in (None, ""):
            continue
        if key == "paymentVerified":
            value = "verified" if value is True or str(value).lower() in (
                "true", "yes", "verified") else "not verified"
        about.append(f"{label}: {esc(value)}")
    if about:
        lines += ["", "<i>Client</i>", " · ".join(about)]

    head = "\n".join(lines)
    text = next((body[key].strip() for key in JD_KEYS
                 if isinstance(body.get(key), str) and body[key].strip()), "")
    if not text:
        return [head[:LIMIT]]

    # What is left of the message after the job's own facts, less the blank
    # line that separates them from the description.
    room = LIMIT - len(head) - 2
    if room < MIN_JD:
        # The facts filled the message. The description starts in the next one
        # rather than being squeezed into a few words here.
        messages, rest = [head], text
    else:
        piece, rest = fit(text, room)
        messages = [f"{head}\n\n{esc(piece)}"]
    while rest:
        piece, rest = fit(rest, LIMIT)
        messages.append(esc(piece))
    return messages


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
