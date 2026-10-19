"""The answer to a job, written as one message.

A job is worked on by more than one worker: one finds repositories and says
why each matters, another publishes the ones that were chosen and reports
where they went. Neither of them is the answer. The answer is both, and
whoever asked for the job should be handed it once, whole, rather than two
messages and the job of joining them.

So the one that finishes is the trigger, not the author. When its message is
handed over, this is what goes with it: the job it is about, what became of it,
and for each published thing everything the search said about where it came
from.

Composed rather than stored. Nothing here is written back into a channel: the
messages stay exactly as their workers wrote them, and this is a reading of
them offered to the worker that needs it.
"""
from __future__ import annotations

import re
from typing import Any

from relay.reading import thread_of

# What a worker says it did, under whichever name it used.
SAID_KEYS = ("status", "state", "result", "event", "outcome", "message", "text")
# A worker that did several things reports them as a list.
RESULT_KEYS = ("repos", "results", "items", "outputs", "created")
# What a search says about a job as a whole. In order of preference, not by
# length: a report is often the whole analysis as prose, and taking it would
# print everything twice - once as the report, once as the fields below.
OVERALL_KEYS = ("summary", "overview", "report")
# Bookkeeping. A similarity or a date tells a proposal nothing, and where a
# published copy came from is the publisher's business, not the proposal's.
NOT_ABOUT = frozenset({"url", "published", "source", "similarity", "created", "last_push",
                       "name", "status", "error", "id", "job", "job_id", "jobId",
                       # The search's prose, which is the fields here said
                       # again at length. A repository's own description is
                       # not that and is kept: it is the description.
                       "report"})


def said_in(body: Any) -> str:
    if not isinstance(body, dict):
        return ""
    for key in SAID_KEYS:
        value = body.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def results_in(body: Any) -> list[dict[str, Any]]:
    if not isinstance(body, dict):
        return []
    for key in RESULT_KEYS:
        items = body.get(key)
        if isinstance(items, list) and any(isinstance(i, dict) for i in items):
            return [i for i in items if isinstance(i, dict)]
    return []


def humanise(key: str) -> str:
    words = re.sub(r"([a-z\d])([A-Z])", r"\1 \2", str(key).replace("_", " ")).split()
    return " ".join(words).capitalize() if words else str(key)


def unfinished(body: dict[str, Any]) -> str:
    """What the answer says it could not do.

    An answer carrying fewer results than were asked for reads as a complete
    one unless it says otherwise, and a proposal written from half an answer
    looks just as finished as one written from all of it.
    """
    failed = body.get("failed")
    said = []
    if isinstance(failed, int) and failed > 0:
        said.append(f"{failed} failed")
    said += [f"{item.get('name') or 'one item'}: {item['error']}"
             for item in results_in(body) if item.get("error")]
    return "; ".join(said)


def about(item: dict[str, Any]) -> list[str]:
    """One repository as the search described it, in full. A proposal is
    written from this, so none of it is summarised away."""
    said: list[str] = []
    for key, value in item.items():
        if key in NOT_ABOUT or value in (None, "", [], {}):
            continue
        if isinstance(value, (list, tuple)):
            said.append(f"{humanise(key)}:")
            said += [f"  - {line}" for line in value if line not in (None, "")]
        elif isinstance(value, dict):
            said.append(f"{humanise(key)}:")
            said += [f"  - {k}: {v}" for k, v in value.items() if v not in (None, "")]
        else:
            said.append(f"{humanise(key)}: {value}")
    return said


def searched(thread: list[dict[str, Any]]) -> tuple[str, dict[str, dict[str, Any]]]:
    """What the search said: about the job, and about each repository by url."""
    overall = ""
    by_url: dict[str, dict[str, Any]] = {}
    for other in thread:
        body = other.get("body")
        if not isinstance(body, dict):
            continue
        for key in OVERALL_KEYS:
            said = body.get(key)
            if isinstance(said, str) and said.strip():
                overall = overall or said.strip()
                break
        for item in results_in(body):
            url = item.get("url") or item.get("published") or item.get("source")
            if isinstance(url, str):
                by_url[url] = item
    return overall, by_url


def compose(body: Any, thread: list[dict[str, Any]]) -> str | None:
    """The answer to one job, as one message, or None when this is not one.

    None matters: a message that is not an answer - a job being posted, a note,
    anything without a result in it - must not arrive wearing the shape of one.
    """
    if not isinstance(body, dict):
        return None
    results = results_in(body)
    if not results:
        return None
    job = thread_of(body)
    if not job:
        return None

    overall, by_url = searched(thread or [])
    lines = [f"Job {job}: {said_in(body) or 'done'}"]
    short = unfinished(body)
    if short:
        # At the top, where it cannot be read as a complete answer.
        lines.append(f"Incomplete: {short}")
    if overall:
        lines += ["", overall]

    for item in results:
        url = item.get("published") or item.get("url")
        if not (isinstance(url, str) and url.startswith(("http://", "https://"))):
            if item.get("error"):
                lines += ["", f"Not published: {item.get('name') or 'one item'} - {item['error']}"]
            continue
        name = item.get("name") or ""
        lines += ["", f"Published: {url}" + (f"  ({name})" if name else "")]
        # Matched on where it came from: the search knows the original, and
        # what was published from it is a copy under another name. So the
        # answer must say `source` for its description to be found - it is
        # read here and never passed on.
        found = by_url.get(item.get("source") or "") or by_url.get(url)
        if found:
            lines += about(found)
    return "\n".join(lines)
