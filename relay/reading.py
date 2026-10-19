"""Reading a job out of whatever shape it arrived in.

Every worker that wants the parts of a posting - the description, the client's
details, the screening questions - would otherwise write this itself, and each
would get a slightly different answer. The server reads it once instead, and
hands the reading to every worker along with the message.

The body is never changed. What a publisher sent is what is stored and what is
delivered; this is an extra way of looking at it, offered beside it.
"""
from __future__ import annotations

import json
import re
from typing import Any

TITLE_KEYS = ("title", "job_title", "jobTitle", "jobName", "job_name", "heading", "name",
              "subject", "emailSubject")
CLIENT_NAME_KEYS = ("client_name", "clientName")
# What a client asks before reading a proposal. Optional, and the part of a
# posting a proposal is judged on, so it is never one of the fields quietly
# dropped for being too long to fit on a pill.
QUESTION_KEYS = ("screeningQuestions", "screening_questions", "screening", "questions")
# Sections a richer scraper sends alongside the posting. Each is shown under
# its own heading rather than being left to the leftovers, which drop anything
# longer than a pill - and these are all longer than a pill.
SECTIONS = (("projectInfo", "Project"), ("project_info", "Project"),
            ("clientInfo", "Client"), ("client_info", "Client"),
            ("workHistory", "Work history"), ("work_history", "Work history"))


def questions_in(body: dict[str, Any]) -> list[str]:
    """The screening questions, however they were sent: a block of lines, or a
    list. Numbering the publisher added is dropped, because it is numbered
    again when it is shown and nobody wants "1. 1. How many years"."""
    for key in QUESTION_KEYS:
        said = body.get(key)
        if isinstance(said, (list, tuple)):
            lines = [str(item).strip() for item in said]
        elif isinstance(said, str):
            lines = said.splitlines()
        else:
            continue
        found = [re.sub(r"^\s*(?:\d+[.)]|[-*\u2022])\s*", "", line).strip() for line in lines]
        found = [line for line in found if line]
        if found:
            return found
    return []

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
    (("screening questions", "screening question", "screening", "questions"),
     "screeningQuestions"),
    (("project info", "project details", "project"), "projectInfo"),
    (("client info", "client details", "about the client"), "clientInfo"),
    (("work history", "client history", "history", "hire history"), "workHistory"),
    (("client name", "client"), "clientName"),
    (("decision",), "decision"),
    (("reason", "why"), "reason"),
    (("project id", "projectid"), "projectId"),
    (("job id", "jobid"), "jobId"),
    (("budget", "rate"), "budget"),
    (("published",), "published"),
    (("posted",), "posted"),
    (("name", "full name", "contact", "lead"), "name"),
    (("headline",), "headline"),
    (("role", "position"), "role"),
    (("company", "organisation", "organization", "employer"), "company"),
    (("location", "based in", "country"), "location"),
    (("profile", "linkedin", "linkedin url", "profile url"), "profile"),
    (("email", "e-mail"), "email"),
    (("phone", "mobile"), "phone"),
    (("industry",), "industry"),
    (("connections",), "connections"),
    (("notes", "note", "about"), "notes"),
    (("status",), "status"),
    (("score", "fit"), "score"),
)
LABEL_OF = {said: key for names, key in LABELS for said in names}
# A section runs until the next section begins. Its own lines are often
# "Key: value" too - "Location: Italy" inside a client's details - and those
# are part of the section, not the start of a new field. Only a section-level
# label may interrupt one.
SECTION_KEYS = frozenset({"description", "projectInfo", "clientInfo", "workHistory",
                          "screeningQuestions"})
WEAK_LABELS = frozenset({"name", "headline", "role", "company", "location", "profile",
                         "email", "phone", "industry", "connections", "notes",
                         "status", "score"})
# A heading often counts what it is about - "Work history (50 past contracts):"
# - so what is in brackets is set aside before the name is looked up, and the
# limit is generous enough to let a counted heading through. It is still only a
# label if the name that remains is one.
LABEL_MAX = 60                            # characters before the colon
COUNTED = re.compile(r"\s*\([^)]*\)\s*$")


def label_of(head: str) -> str | None:
    return LABEL_OF.get(COUNTED.sub("", head.strip()).strip().lower())


def labelled(text: Any) -> dict[str, Any] | None:
    """A job written as labelled lines, read back as fields.

    A label only counts the first time it appears: a description that happens
    to say "Reason:" partway through is still the description.
    """
    if not isinstance(text, str) or ":" not in text:
        return None
    # A worker that encoded its text once too often sends the newlines as two
    # characters, so the whole job is one line and none of the labels after the
    # first can be seen. Reading it as it was meant costs one replacement.
    if "\n" not in text and "\\n" in text:
        text = text.replace("\\r\\n", "\n").replace("\\n", "\n")
    found: dict[str, Any] = {}
    lead: list[str] = []
    current: str | None = None
    for line in text.splitlines():
        at = line.find(":")
        key = None
        if 0 < at <= LABEL_MAX:
            key = label_of(line[:at])
            if key in found or (current in SECTION_KEYS and key in WEAK_LABELS):
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
def title_of(body: dict[str, Any]) -> str:
    for key in TITLE_KEYS:
        said = body.get(key)
        if isinstance(said, str) and said.strip():
            return said.strip()
    return ""


# What each part is called in the reading, whatever the publisher called it.
# One shape for every worker, so a consumer written against one publisher goes
# on working when the next one spells things differently.
ALIASES = (
    ("title", TITLE_KEYS),
    ("url", ("upworkUrl", "upwork_url", "job_url", "jobUrl", "jobLink", "url", "link")),
    ("description", ("description", "jobDescription", "job_description", "jd", "details")),
    ("projectInfo", ("projectInfo", "project_info", "project")),
    ("clientInfo", ("clientInfo", "client_info")),
    ("workHistory", ("workHistory", "work_history", "clientHistory", "client_history")),
    ("clientName", ("clientName", "client_name")),
    ("decision", ("decision", "verdict")),
    ("reason", ("reason", "why")),
    ("budget", ("budget", "rate")),
    ("published", ("published",)),
    ("posted", ("posted",)),
    ("projectId", ("projectId", "project_id", "vollnaProjectId")),
    ("jobId", ("jobId", "job_id")),
)
UPWORK_JOB = re.compile(r"upwork\.com/jobs/(~[0-9a-z]+)", re.I)


# What ties a job item to the answers about it. A worker answering names the
# job it is answering, which is the only thing the messages have in common:
# they are written by different workers, in different shapes, minutes apart.
THREAD_KEYS = ("jobId", "job_id", "job", "proposalId", "proposal_id")


def thread_of(body: Any) -> str | None:
    """The job a message belongs to, for tying a thread together.

    Taken from what the publisher called it, and failing that from an Upwork
    link, so a job item that names no id still gathers the answers that do.
    """
    found = body if isinstance(body, dict) else labelled(body) if isinstance(body, str) else None
    if isinstance(found, dict):
        for key in THREAD_KEYS:
            said = found.get(key)
            if isinstance(said, str) and said.strip():
                return said.strip()
            if isinstance(said, (int, float)) and not isinstance(said, bool):
                return str(said)
    seen = UPWORK_JOB.search(body if isinstance(body, str) else json.dumps(body, default=str))
    return seen.group(1).lower() if seen else None


def reading(body: Any) -> dict[str, Any]:
    """The parts of a posting, under one set of names.

    Empty when there is nothing to say - a reply, a bare string, a shape this
    knows nothing about - because half a reading is worse than none: it invites
    a worker to believe a field was absent when it was only unread.
    """
    said = body
    if isinstance(body, str):
        found = labelled(body)
        if not found:
            return {}
        body = found
    elif isinstance(body, dict):
        if not title_of(body):
            for key in ("text", "message", "raw"):
                found = labelled(body.get(key))
                if found:
                    body = {**body, **found}
                    break
    else:
        return {}

    read: dict[str, Any] = {}
    for name, keys in ALIASES:
        for key in keys:
            value = body.get(key)
            if isinstance(value, str) and value.strip():
                read[name] = value.strip()
                break
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                read[name] = str(value)
                break

    asked = questions_in(body)
    if asked:
        read["screeningQuestions"] = asked

    client = body.get("client")
    if isinstance(client, dict) and client:
        read["client"] = client
    elif isinstance(client, str) and client.strip() and "clientName" not in read:
        read["clientName"] = client.strip()

    # The job itself, from its own link, so a worker can match a posting to a
    # reply about it without having to take a URL apart.
    seen = UPWORK_JOB.search(read.get("url", "")) or UPWORK_JOB.search(
        said if isinstance(said, str) else "")
    if seen:
        read.setdefault("jobId", seen.group(1))
    return read
