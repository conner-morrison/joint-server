"""Calling somewhere when a channel gets something.

For a worker that cannot hold a request open - a scheduled task, a function,
anything woken by being called rather than by asking. The point of the design
is in the second test: it is a trigger, not a delivery.
"""
from __future__ import annotations

import json
import socket
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from relay.hooks import check_url


class Listener(threading.Thread):
    """Somewhere for a trigger to call, that records what arrived."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.calls: list[dict[str, Any]] = []
        self.answer = 200
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}/fire"
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:                       # noqa: N802
                size = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(size) if size else b""
                outer.calls.append({
                    "path": self.path,
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                    "body": json.loads(raw) if raw else None,
                })
                self.send_response(outer.answer)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args: Any) -> None:
                pass

        self.server = HTTPServer(("127.0.0.1", self.port), Handler)

    def run(self) -> None:
        self.server.serve_forever()

    def stop(self) -> None:
        self.server.shutdown()


class AddressTest(unittest.TestCase):
    """Anyone may create a workspace here, so a trigger is an address someone
    else chose. Unchecked, this server becomes a way to reach whatever it can
    reach and nobody else can."""

    def test_only_https_is_called(self) -> None:
        with self.assertRaises(ValueError):
            check_url("http://example.com/fire")
        with self.assertRaises(ValueError):
            check_url("file:///etc/passwd")
        with self.assertRaises(ValueError):
            check_url("")

    def test_nothing_inside_this_network(self) -> None:
        for inside in ("https://127.0.0.1/fire", "https://localhost/fire",
                       "https://10.0.0.5/fire", "https://192.168.1.1/fire",
                       "https://169.254.169.254/latest/meta-data/"):
            with self.assertRaises(ValueError, msg=inside):
                check_url(inside)

    def test_an_ordinary_address_is_allowed(self) -> None:
        self.assertEqual(check_url("https://api.anthropic.com/v1/fire"),
                         "https://api.anthropic.com/v1/fire")


if __name__ == "__main__":
    unittest.main()
