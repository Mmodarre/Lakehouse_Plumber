"""Regenerate-and-compare drift check for tracked docs fixture projects.

Docs pages ``literalinclude`` generated code from
``docs/_fixtures/<name>/generated/``. Nothing else ties those committed files to
the generator, so this test regenerates each listed fixture with
``lhp generate --env dev`` and fails when the committed output no longer matches.
"""

import difflib
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_FIXTURES_DIR = REPO_ROOT / "docs" / "_fixtures"

# Fixture projects whose committed ``generated/`` tree is checked against fresh
# generator output. Add a fixture directory name here to extend coverage.
DRIFT_CHECKED_FIXTURES = ["guide_qo_quarantine"]

_MAX_DIFF_LINES = 40


def _relative_files(root: Path) -> set[str]:
    if not root.is_dir():
        return set()
    return {
        path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()
    }


def _short_unified_diff(committed: bytes, regenerated: bytes, rel_path: str) -> str:
    diff_lines = list(
        difflib.unified_diff(
            committed.decode("utf-8", errors="replace").splitlines(keepends=True),
            regenerated.decode("utf-8", errors="replace").splitlines(keepends=True),
            fromfile=f"committed/{rel_path}",
            tofile=f"regenerated/{rel_path}",
        )
    )
    if len(diff_lines) > _MAX_DIFF_LINES:
        omitted = len(diff_lines) - _MAX_DIFF_LINES
        diff_lines = [
            *diff_lines[:_MAX_DIFF_LINES],
            f"... ({omitted} more diff lines)\n",
        ]
    return "".join(diff_lines)


@pytest.mark.integration
@pytest.mark.parametrize("fixture_name", DRIFT_CHECKED_FIXTURES)
def test_docs_fixture_matches_generator_output(fixture_name, tmp_path, monkeypatch):
    fixture_dir = DOCS_FIXTURES_DIR / fixture_name
    committed_dir = fixture_dir / "generated"
    assert committed_dir.is_dir(), f"{committed_dir} is missing"

    project_dir = tmp_path / fixture_name
    shutil.copytree(
        fixture_dir, project_dir, ignore=shutil.ignore_patterns("generated", ".lhp")
    )
    monkeypatch.chdir(project_dir)

    result = CliRunner().invoke(cli, ["generate", "--env", "dev"])
    assert result.exit_code == 0, (
        f"lhp generate failed for docs fixture {fixture_name!r} "
        f"(exit {result.exit_code}, exception {result.exception!r}):\n{result.output}"
    )

    regenerated_dir = project_dir / "generated"
    committed_files = _relative_files(committed_dir)
    regenerated_files = _relative_files(regenerated_dir)

    only_committed = sorted(committed_files - regenerated_files)
    only_regenerated = sorted(regenerated_files - committed_files)
    changed = sorted(
        rel
        for rel in committed_files & regenerated_files
        if (committed_dir / rel).read_bytes() != (regenerated_dir / rel).read_bytes()
    )
    if not (only_committed or only_regenerated or changed):
        return

    report = [
        f"docs fixture {fixture_name!r} drifted from `lhp generate --env dev` output.",
        f"Regenerate {committed_dir.relative_to(REPO_ROOT).as_posix()}/ and commit it.",
    ]
    if only_committed:
        report.append(f"Committed but no longer generated: {only_committed}")
    if only_regenerated:
        report.append(f"Generated but not committed: {only_regenerated}")
    if changed:
        report.append(f"Content differs: {changed}")
        first = changed[0]
        report.append(
            _short_unified_diff(
                (committed_dir / first).read_bytes(),
                (regenerated_dir / first).read_bytes(),
                first,
            )
        )
    pytest.fail("\n".join(report), pytrace=False)
