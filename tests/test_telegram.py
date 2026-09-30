"""How a job reads on a phone.

The renderer had no tests, which is how it came to quietly drop the end of
every long job description: the code that cut it looked deliberate, and
nothing said what the rule was meant to be.
"""
from __future__ import annotations

import unittest

from relay.telegram import LIMIT, fit, render


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
