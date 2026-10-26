"""What a worker is, and what follows from it.

Every rule in this server is a switch somewhere: whether a worker may post a
job the channel already has, whether a chat is told about what it publishes,
who hears what it says. Set one at a time they are easy to get wrong and
impossible to remember - and the wrong setting is quiet, because a worker goes
on working while what it publishes stops arriving.

A kind is the bundle. Saying what a worker *is* sets every switch that follows
from it, and saying it again later sets them again, so a channel can be put
back in order by naming its workers rather than by remembering six decisions.

Nothing here is magic: each field below maps to a switch that can still be
thrown by hand. A worker with no kind is left exactly as it is.
"""
from __future__ import annotations

from typing import Any

# `repeats`     may post a job the channel already has. True only where the
#               second posting is a judgement about the job rather than the
#               job again.
# `chats_hear`  whether a Telegram chat in the channel is told what it
#               publishes. A search that has already decided is worth a phone
#               buzzing; one that sifts everything is not.
# `sends_to`    the kinds it hands work to, where they are in the same
#               channel. Empty means it reaches every member, which is what a
#               channel does when nothing is said.
KINDS: dict[str, dict[str, Any]] = {
    "gmail-alert": {
        "label": "Gmail alert",
        "about": "Watches a mailbox and posts the jobs it finds.",
        "repeats": False, "chats_hear": True, "sends_to": [],
    },
    "vollna-scraper": {
        "label": "Vollna scraper",
        "about": "Posts a job with its own judgement on it - a decision and a reason.",
        # Its second posting of a job is the judgement, not the job again.
        "repeats": True,
        # It sifts everything; a phone should hear the one that already decided.
        "chats_hear": False,
        "sends_to": [],
    },
    "proposal-writer": {
        "label": "Proposal writer",
        "about": "Takes a job and writes a proposal for it; asks for repositories.",
        "repeats": False, "chats_hear": True, "sends_to": ["repo-finder"],
    },
    "repo-finder": {
        "label": "Repo finder",
        "about": "Finds repositories for a job and says why each one matters.",
        # Its answer goes straight back to whoever asked. Nothing is published
        # in between any more, so there is no one else for it to hand to.
        "repeats": False, "chats_hear": False, "sends_to": ["proposal-writer"],
    },
    "github-editor": {
        "label": "GitHub editor",
        "about": "Publishes chosen repositories and reports where they went.",
        "repeats": False, "chats_hear": False, "sends_to": ["proposal-writer"],
    },
    "linkedin-harvester": {
        "label": "LinkedIn harvester",
        "about": "Posts leads it has found.",
        "repeats": False, "chats_hear": True, "sends_to": [],
    },
}


def listed() -> list[dict[str, Any]]:
    """The kinds, for a console that should not have to know them."""
    return [{"kind": kind, **{k: v for k, v in rules.items()}} for kind, rules in KINDS.items()]


def check_kind(kind: str | None) -> str | None:
    """A kind, or None for a worker that has none. Anything else is a typo,
    and a typo that silently did nothing would be worse than a refusal."""
    if kind in (None, ""):
        return None
    if kind not in KINDS:
        raise ValueError(f"no such kind: {kind!r}. One of: {', '.join(sorted(KINDS))}")
    return kind
