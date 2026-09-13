"""A worker that emails what arrives on a channel.

    python examples/mailer.py --url https://relay.example.com --token <token> \
        --channel vollna-jobs --to you@gmail.com

The SMTP password comes from SMTP_PASSWORD, never the command line, because
arguments are visible to every process on the machine. For Gmail that is an
app password: a Google account with 2-step verification will not accept the
account password over SMTP.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import smtplib
import sys
from email.message import EmailMessage
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from relay import Message, RelayClient  # noqa: E402

log = logging.getLogger("mailer")


def render(body: object) -> tuple[str, str]:
    """Subject and text for one message. Bodies are whatever the publisher
    sent, so anything unrecognised is passed through as JSON rather than
    dropped: an alert that arrives in an odd shape is still worth seeing."""
    if not isinstance(body, dict):
        return "Relay message", json.dumps(body, indent=2, ensure_ascii=False)

    if body.get("type") == "job":
        title = str(body.get("title") or "Untitled job")
        lines = [title, ""]
        for label, key in (("Budget", "budget"), ("Published", "published"),
                           ("Job", "upworkUrl"), ("From", "emailSubject")):
            if body.get(key):
                lines.append(f"{label}: {body[key]}")
        return title, "\n".join(lines)

    if body.get("type") == "email":
        return str(body.get("emailSubject") or "Vollna email"), str(body.get("text") or "")

    return str(body.get("type") or "Relay message"), json.dumps(body, indent=2, ensure_ascii=False)


def send(args: argparse.Namespace, password: str, subject: str, text: str) -> None:
    """Blocking: the caller runs this off the event loop, or the websocket
    would go unanswered for as long as the SMTP conversation takes."""
    msg = EmailMessage()
    msg["From"] = args.sender or args.smtp_user
    msg["To"] = args.to
    msg["Subject"] = subject
    msg.set_content(text)
    with smtplib.SMTP(args.smtp_host, args.smtp_port, timeout=30) as smtp:
        smtp.starttls()
        smtp.login(args.smtp_user, password)
        smtp.send_message(msg)


class HighWater:
    """The last sequence number emailed on each channel, kept on disk.

    Delivery is at least once, so a worker that restarts mid-message sees that
    message again. Messages arrive in order per channel and handlers run one at
    a time, so one number per channel is enough to know what is already sent."""

    def __init__(self, path: Path | None):
        self.path = path
        self.seen: dict[str, int] = {}
        if path and path.exists():
            try:
                self.seen = {k: int(v) for k, v in json.loads(path.read_text()).items()}
            except (ValueError, OSError):
                log.warning("could not read %s; starting from empty", path)

    def already_sent(self, channel: str, seq: int) -> bool:
        return seq <= self.seen.get(channel, 0)

    def record(self, channel: str, seq: int) -> None:
        self.seen[channel] = max(seq, self.seen.get(channel, 0))
        if self.path:
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.seen))
            tmp.replace(self.path)                 # atomic: never a half-written file


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=os.environ.get("RELAY_URL", "http://127.0.0.1:8700"))
    p.add_argument("--token", default=os.environ.get("RELAY_TOKEN", ""), help="this worker's token")
    p.add_argument("--channel", action="append", default=[],
                   help="channel to email; repeatable, defaults to every channel the worker is in")
    p.add_argument("--to", required=True, help="where the mail goes")
    p.add_argument("--sender", default=os.environ.get("SMTP_SENDER", ""), help="From address (default: --smtp-user)")
    p.add_argument("--smtp-host", default=os.environ.get("SMTP_HOST", "smtp.gmail.com"))
    p.add_argument("--smtp-port", type=int, default=int(os.environ.get("SMTP_PORT", "587")))
    p.add_argument("--smtp-user", default=os.environ.get("SMTP_USER", ""))
    p.add_argument("--state", default=os.environ.get("MAILER_STATE", "mailer-state.json"),
                   help="file remembering what was already emailed; '' to keep nothing")
    p.add_argument("--retries", type=int, default=5, help="attempts per message before giving up")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    password = os.environ.get("SMTP_PASSWORD", "")
    if not password or not args.smtp_user:
        sys.exit("set SMTP_USER and SMTP_PASSWORD (an app password for Gmail)")

    client = RelayClient(args.url, args.token)
    sent = HighWater(Path(args.state) if args.state else None)

    async def deliver(msg: Message) -> None:
        if sent.already_sent(msg.channel, msg.seq):
            log.info("#%d on %s was already emailed", msg.seq, msg.channel)
            return
        subject, text = render(msg.body)

        # The relay acknowledges a message once the handler returns, including
        # when it raised. Retrying here is what keeps a temporary SMTP failure
        # from silently dropping an alert. Handlers run one at a time, so this
        # holds up the channel while it retries, which is the right trade for
        # mail: later alerts are worth less than a lost one.
        for attempt in range(1, args.retries + 1):
            try:
                await asyncio.to_thread(send, args, password, subject, text)
                sent.record(msg.channel, msg.seq)
                log.info("emailed #%d from %s: %s", msg.seq, msg.channel, subject)
                return
            except (smtplib.SMTPException, OSError) as exc:
                if attempt == args.retries:
                    log.error("giving up on #%d after %d attempts: %s", msg.seq, attempt, exc)
                    raise
                delay = min(2 ** attempt, 30)
                log.warning("attempt %d for #%d failed (%s); retrying in %gs", attempt, msg.seq, exc, delay)
                await asyncio.sleep(delay)

    for channel in args.channel:
        client.on(channel)(deliver)
    if not args.channel:
        client.on()(deliver)

    log.info("mailing %s to %s", ", ".join(args.channel) or "every channel", args.to)
    await client.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
