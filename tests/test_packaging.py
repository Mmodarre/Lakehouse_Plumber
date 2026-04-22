"""Wheel-content regression test.

Guards against ``package-data`` drift in ``pyproject.toml``: every
version-controlled file under ``src/lhp/templates/`` must be present in the
built wheel. The v0.8.2 incident (TemplateNotFound for
``monitoring/union_event_logs.py.j2`` on Windows) was caused by such drift.

The expected-file set is derived from ``git ls-files``, not the working tree,
so developer-local noise (e.g. editor ``.vscode/`` dirs, ``.DS_Store``) does
not generate false positives.
"""

import pathlib
import subprocess
import sys
import zipfile

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent


def _tracked_template_files() -> set[str]:
    """Return the set of ``lhp/templates/...`` wheel paths that git tracks."""
    result = subprocess.run(
        ["git", "ls-files", "src/lhp/templates"],
        cwd=REPO,
        check=True,
        capture_output=True,
        text=True,
    )
    tracked = set()
    for line in result.stdout.splitlines():
        # Rewrite the repo-relative path (src/lhp/templates/...) to the
        # wheel-relative path (lhp/templates/...).
        wheel_path = line.removeprefix("src/")
        tracked.add(wheel_path)
    return tracked


@pytest.mark.slow
def test_wheel_contains_every_template_file(tmp_path: pathlib.Path) -> None:
    """Every git-tracked file under src/lhp/templates/ must appear in the built wheel."""
    subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", str(tmp_path)],
        cwd=REPO,
        check=True,
    )
    wheels = list(tmp_path.glob("lakehouse_plumber-*.whl"))
    assert len(wheels) == 1, f"Expected exactly one wheel, got {wheels}"

    with zipfile.ZipFile(wheels[0]) as zf:
        names = set(zf.namelist())

    expected = _tracked_template_files()
    assert expected, "Git found no tracked files under src/lhp/templates/ — refusing to trust an empty expectation set."

    missing = expected - names
    assert not missing, (
        f"{len(missing)} template file(s) missing from wheel distribution:\n"
        + "\n".join(f"  - {m}" for m in sorted(missing))
    )
