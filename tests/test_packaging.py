"""What a host installs when it is handed this repository.

A deployment platform reads pyproject.toml and installs exactly what
[project] dependencies lists. Anything the server imports that is missing from
that list is a crash at import, which reaches the browser as "this function
crashed" and says nothing about the cause. These tests are cheap insurance
against that, because the failure is invisible until something is deployed.
"""
from __future__ import annotations

import ast
import pathlib
import sys
import tomllib
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent


def top_level_imports(path: pathlib.Path) -> set[str]:
    """Modules imported when the file is imported, ignoring those inside
    functions: those are paid for only by whoever calls them."""
    tree = ast.parse(path.read_text())
    found: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            found.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            found.add(node.module.split(".")[0])
    return found


class PackagingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
        self.declared = {
            req.split("[")[0].split(">")[0].split("=")[0].split("<")[0].strip().lower()
            for req in self.pyproject["project"]["dependencies"]
        }

    # Modules that arrive with an extra of a declared package rather than under
    # a name of their own.
    FROM_EXTRAS = {"psycopg_pool": "psycopg"}     # psycopg[pool]

    def test_the_entrypoint_only_imports_what_is_declared(self) -> None:
        # Python's own list, rather than one kept by hand here: a missing name
        # in a hand-written set fails a correct import and teaches people to
        # distrust this test.
        stdlib = set(sys.stdlib_module_names) | {"__future__"}
        for module in ("app.py", "relay/serverless.py", "relay/pgstore.py"):
            for name in top_level_imports(ROOT / module):
                if name in stdlib or name == "relay":
                    continue
                wanted = self.FROM_EXTRAS.get(name, name).lower()
                self.assertIn(wanted, self.declared,
                              f"{module} imports {name!r} at module level, "
                              "but it is not in [project] dependencies")

    def test_the_vercel_entrypoint_is_named(self) -> None:
        self.assertEqual(self.pyproject["tool"]["vercel"]["entrypoint"], "app:app")

    def test_app_py_exposes_app(self) -> None:
        tree = ast.parse((ROOT / "app.py").read_text())
        assigned = {t.id for n in tree.body if isinstance(n, ast.Assign)
                    for t in n.targets if isinstance(t, ast.Name)}
        self.assertIn("app", assigned, "the host loads `app` from app.py")

    def test_requirements_and_pyproject_agree(self) -> None:
        """Both are read by different tools; disagreeing means one deployment
        path works and another does not."""
        req = (ROOT / "requirements.txt").read_text().splitlines()
        names = {r.split("[")[0].split(">")[0].split("=")[0].strip().lower()
                 for r in req if r.strip() and not r.startswith("#")}
        self.assertEqual(names, self.declared)
