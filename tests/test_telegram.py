"""How a job reads on a phone.

The renderer had no tests, which is how it came to quietly drop the end of
every long job description: the code that cut it looked deliberate, and
nothing said what the rule was meant to be.
"""
from __future__ import annotations

import unittest

from relay.telegram import LIMIT, fit, labelled, render


def body(**extra: object) -> dict[str, object]:
    return {"source": "vollna", "type": "job", "title": "Adaptive recommendation engine",
            "budget": "Hourly Rate: 100 - 300 USD",
            "upworkUrl": "https://www.upwork.com/jobs/~022102893892674124528", **extra}


class RenderTest(unittest.TestCase):
    def test_a_job_is_one_message(self) -> None:
        sent = render(body(description="Build a recommender."), channel="jobs")
        self.assertEqual(len(sent), 1)
        self.assertTrue(sent[0].startswith("\U0001f50e Vollna  ·  #jobs"))
        self.assertIn("Build a recommender.", sent[0])

    def test_the_channel_is_named_even_with_nothing_else_known(self) -> None:
        """Two notifications for one job is two senders, and the line saying
        which channel a message came from is how that gets found."""
        sent = render({"description": "No source, no title."}, channel="github")
        self.assertTrue(sent[0].startswith("#github"))

    def test_a_long_description_arrives_whole(self) -> None:
        """The failure this file exists for. A description longer than Telegram
        will carry used to lose its end; now it continues into the next
        message, because the part that was cut is as much the job as the part
        that fit."""
        paragraphs = [f"Paragraph {n}: " + "what the client wants. " * 14 for n in range(1, 30)]
        jd = "\n".join(paragraphs)
        self.assertGreater(len(jd), 2 * LIMIT, "the description under test must not fit")

        sent = render(body(description=jd), channel="jobs")
        self.assertGreater(len(sent), 1)
        for part in sent:
            self.assertLessEqual(len(part), LIMIT)
        # Every paragraph, in order, and none of them cut in half. Compared
        # without the whitespace a break lands on, which is dropped so that no
        # message starts with a blank line.
        for n, paragraph in enumerate(paragraphs, 1):
            self.assertIn(paragraph.strip(), "\n".join(sent), f"paragraph {n} went missing")
        arrived = " ".join(sent).split()
        self.assertEqual(arrived[-len(paragraphs[-1].split()):], paragraphs[-1].split(),
                         "the description does not end where it should")

    def test_a_description_is_never_dropped_for_want_of_room(self) -> None:
        """A job with so much said about it that the facts alone nearly fill a
        message used to arrive with no description at all."""
        crowded = body(description="The description itself.",
                       client={f"field{n}": "x" * 60 for n in range(1, 40)},
                       rank="Excellent", location="United States")
        crowded["client"] = {"rank": "Excellent " + "x" * 3900, "paymentVerified": True}
        sent = render(crowded, channel="jobs")
        self.assertIn("The description itself.", "\n".join(sent))

    def test_the_limit_counts_escaped_characters(self) -> None:
        """An ampersand becomes five characters. Measured as typed, a
        description full of them overruns and Telegram refuses the whole
        message rather than shortening it."""
        sent = render(body(description="&" * (LIMIT - 100)), channel="jobs")
        for part in sent:
            self.assertLessEqual(len(part), LIMIT)
        self.assertNotIn("&&", "".join(sent))       # every one of them escaped

    def test_markup_in_a_description_is_shown_not_obeyed(self) -> None:
        sent = render(body(description="Use <b>React</b> & Redux"), channel="jobs")
        self.assertIn("Use &lt;b&gt;React&lt;/b&gt; &amp; Redux", sent[0])


class RoughBodyTest(unittest.TestCase):
    """A worker writes what is natural to it.

    One spells it job_title and client_name and puts its judgement in decision;
    another sends title, upworkUrl and a nested client. Neither is sending
    something less worth reading, so both read the same way on a phone.
    """

    ROUGH = {"job_title": "Looker Studio dashboard", "job_url": "https://www.upwork.com/jobs/~0221",
             "job_description": "Connect and blend the sources.", "decision": "apply",
             "client_name": "Northwind Analytics"}

    def test_every_field_arrives(self) -> None:
        sent = render(self.ROUGH, channel="jobs", source="general-vollna-search")
        said = "\n".join(sent)
        self.assertIn("Looker Studio dashboard", said)
        self.assertIn("https://www.upwork.com/jobs/~0221", said)
        self.assertIn("Decision: <b>apply</b>", said)
        self.assertIn("Name: Northwind Analytics", said)
        self.assertIn("Connect and blend the sources.", said)

    def test_the_publisher_is_named(self) -> None:
        """Two workers watch the same boards under different rules, so which one
        found a job decides what its decision is worth."""
        sent = render(self.ROUGH, channel="jobs", source="general-vollna-search")
        self.assertIn("general-vollna-search", sent[0].splitlines()[0])
        self.assertIn("#jobs", sent[0].splitlines()[0])

    def test_it_reads_the_same_as_the_tidier_shape(self) -> None:
        tidy = {"source": "vollna", "type": "job", "title": "Looker Studio dashboard",
                "upworkUrl": "https://www.upwork.com/jobs/~0221", "decision": "apply",
                "client": {"name": "Northwind Analytics"},
                "description": "Connect and blend the sources."}
        rough = render(self.ROUGH, channel="jobs", source="a")[0].splitlines()
        same = render(tidy, channel="jobs", source="a")[0].splitlines()
        # Line for line the same message, bar the source tag the tidy one earns
        # by saying where it came from in its body.
        self.assertEqual([x for x in rough if not x.startswith("a  ·")][1:],
                         [x for x in same if not x.startswith("\U0001f50e")][1:])

    def test_a_title_is_found_wherever_it_was_put(self) -> None:
        for key in ("title", "job_title", "jobTitle", "job_name", "heading", "name"):
            sent = render({key: "Build a dashboard"}, channel="jobs")
            self.assertIn("Build a dashboard", sent[0], f"{key} was not read as the title")

    def test_a_client_name_is_found_wherever_it_was_put(self) -> None:
        for body in ({"client_name": "Acme"}, {"clientName": "Acme"}, {"client": "Acme"},
                     {"client": {"name": "Acme"}}, {"company": "Acme"}):
            sent = render({**body, "title": "A job"}, channel="jobs")
            self.assertIn("Name: Acme", "\n".join(sent), f"{body} lost the client's name")


class LabelledTextTest(unittest.TestCase):
    """A job sent as a block of labelled text rather than as fields.

    It is the same job, said differently. Read it and it is shown the way any
    other job is; leave it and it arrives as a wall of escaped newlines, which
    is how this one turned up.
    """

    RAW = ("Decision: manual check\n"
           "Reason: spent $3198 (< $100K) and pays $17.03/hr (> $10)\n"
           "Project id: 75319257\n\n"
           "Job title: Community Manager & Content Marketer for Indie Android App\n"
           "Job link: https://www.upwork.com/jobs/~02210583609411881381\n"
           "Client name: Comunicazione (named in 1 feedback)\n\n"
           "Job description:\n"
           "PopsPhone is a newly launched Android launcher.\n"
           "Key Responsibilities (Phase 1)\n"
           "Community Engagement: Identify and engage with niche communities.\n"
           "PR & Blog Outreach: Research relevant tech blogs.\n"
           "Reason: this line is prose, not a second label.")

    def test_every_label_is_read(self) -> None:
        read = labelled(self.RAW)
        self.assertEqual(read["decision"], "manual check")
        self.assertEqual(read["reason"], "spent $3198 (< $100K) and pays $17.03/hr (> $10)")
        self.assertEqual(read["projectId"], "75319257")
        self.assertEqual(read["title"], "Community Manager & Content Marketer for Indie Android App")
        self.assertEqual(read["url"], "https://www.upwork.com/jobs/~02210583609411881381")
        self.assertEqual(read["clientName"], "Comunicazione (named in 1 feedback)")

    def test_the_description_keeps_its_own_colons(self) -> None:
        """The reason only known labels start a field. A posting is full of
        lines like "Community Engagement: …", and treating every colon as a
        label would chop the description into nonsense."""
        read = labelled(self.RAW)
        self.assertIn("Community Engagement: Identify and engage with niche communities.",
                      read["description"])
        self.assertIn("PR & Blog Outreach: Research relevant tech blogs.", read["description"])
        self.assertIn("Key Responsibilities (Phase 1)", read["description"])

    def test_a_label_counts_once(self) -> None:
        """A description that says "Reason:" partway through is still the
        description, not a correction to the reason."""
        read = labelled(self.RAW)
        self.assertEqual(read["reason"], "spent $3198 (< $100K) and pays $17.03/hr (> $10)")
        self.assertIn("this line is prose, not a second label", read["description"])

    def test_it_renders_as_any_other_job_does(self) -> None:
        sent = render(self.RAW, channel="jobs", source="general-vollna-search")
        said = "\n".join(sent)
        self.assertIn("general-vollna-search", sent[0].splitlines()[0])
        self.assertIn('<a href="https://www.upwork.com/jobs/~02210583609411881381">', said)
        self.assertIn("Decision: <b>manual check</b>", said)
        self.assertIn("Name: Comunicazione (named in 1 feedback)", said)
        self.assertIn("PopsPhone is a newly launched Android launcher.", said)

    def test_text_that_is_not_labelled_is_left_alone(self) -> None:
        """Only a job written this way is read this way. Ordinary prose, and a
        single stray colon, must not be mistaken for fields."""
        self.assertIsNone(labelled("just some words"))
        self.assertIsNone(labelled("Note: nothing else here"))
        self.assertIsNone(labelled(""))
        self.assertIsNone(labelled(None))

    def test_a_posting_with_no_leading_label_keeps_its_words(self) -> None:
        read = labelled("A job nobody labelled.\nDecision: skip\nReason: too cheap")
        self.assertEqual(read["description"], "A job nobody labelled.")


class FitTest(unittest.TestCase):
    def test_text_that_fits_is_left_alone(self) -> None:
        self.assertEqual(fit("short", 100), ("short", ""))

    def test_a_break_prefers_the_end_of_a_line(self) -> None:
        first, rest = fit("line one\nline two\nline three", 20)
        self.assertEqual(first, "line one\nline two")
        self.assertEqual(rest, "line three")

    def test_a_break_falls_back_to_a_space(self) -> None:
        first, rest = fit("one two three four five six", 12)
        self.assertNotIn(" ", first[-1])
        self.assertTrue("one two three".startswith(first))
        self.assertTrue(rest.endswith("six"))

    def test_one_unbroken_run_is_split_where_it_must_be(self) -> None:
        first, rest = fit("x" * 50, 20)
        self.assertEqual((len(first), len(rest)), (20, 30))

    def test_nothing_is_lost_at_a_break(self) -> None:
        """The whitespace a break lands on is dropped, because each piece is
        its own message and a message does not begin with a blank line. Every
        word survives, which is what matters."""
        text = "alpha beta\ngamma delta epsilon zeta"
        pieces = []
        rest = text
        while rest:
            piece, rest = fit(rest, 15)
            pieces.append(piece)
        self.assertEqual(" ".join(pieces).split(), text.split())


if __name__ == "__main__":
    unittest.main()
