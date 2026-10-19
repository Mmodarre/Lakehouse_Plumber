"""The release staging step must tag the tested commit, and only that commit."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/publish.yml"
STEP = "Stage all assets in a draft release"

pytestmark = pytest.mark.skipif(
    sys.platform == "win32" or shutil.which("bash") is None,
    reason="runs the workflow step's bash script",
)

# Stands in for the gh calls the step makes. Like the real CLI, a `gh api` call
# for a missing ref writes the 404 response body to stdout, not stderr.
FAKE_GH = r"""#!/usr/bin/env bash
echo "gh $*" >> "$CALLS"
case "$1 $2" in
  "api repos/owner/repo/git/ref/tags/v1.2.3")
    if [ -n "$TAG_SHA" ]; then echo "$TAG_SHA"; exit 0; fi
    echo '{"message":"Not Found","status":"404"}'
    exit 1 ;;
  "api repos/owner/repo/git/matching-refs/tags/v1.2.3")
    if [ -n "$LOOKUP_ERROR" ]; then echo "HTTP 502: Bad Gateway" >&2; exit 1; fi
    if [ -n "$TAG_SHA" ]; then echo "$TAG_SHA"; fi ;;
  "release view")
    [ -n "$RELEASE_EXISTS" ] ;;
esac
"""

FAKE_PYTHON = """#!/usr/bin/env bash
echo "python $*" >> "$CALLS"
"""


def run_step(tmp_path, tag_sha="", lookup_error=False, release_exists=False):
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    step = next(s for s in jobs["stage-release"]["steps"] if s.get("name") == STEP)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in {"gh": FAKE_GH, "python": FAKE_PYTHON}.items():
        tool = bin_dir / name
        tool.write_text(body)
        tool.chmod(0o755)
    script = tmp_path / "step.sh"
    script.write_text(step["run"])
    calls = tmp_path / "calls"
    calls.touch()
    env = {
        **os.environ,
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "CALLS": str(calls),
        "VERSION": "1.2.3",
        "SOURCE_SHA": "tested-sha",
        "GITHUB_REPOSITORY": "owner/repo",
        "TAG_SHA": tag_sha,
        "LOOKUP_ERROR": "1" if lookup_error else "",
        "RELEASE_EXISTS": "1" if release_exists else "",
    }
    # GitHub runs a `run:` block with `bash -e {0}`.
    result = subprocess.run(
        ["bash", "-e", str(script)], env=env, capture_output=True, text=True
    )
    return result, calls.read_text().splitlines()


def test_missing_tag_is_created_at_the_tested_commit(tmp_path):
    result, calls = run_step(tmp_path)
    assert result.returncode == 0, result.stderr
    assert (
        "gh api repos/owner/repo/git/refs --method POST"
        " -f ref=refs/tags/v1.2.3 -f sha=tested-sha"
    ) in calls
    assert calls[-1] == "python scripts/publish_release_assets.py release-assets"


def test_tag_on_the_tested_commit_is_reused(tmp_path):
    result, calls = run_step(tmp_path, tag_sha="tested-sha", release_exists=True)
    assert result.returncode == 0, result.stderr
    assert not any("--method POST" in call for call in calls)
    assert not any(call.startswith("gh release create") for call in calls)


def test_tag_on_another_commit_stops_the_release(tmp_path):
    result, calls = run_step(tmp_path, tag_sha="previous-sha")
    assert result.returncode != 0
    assert not any(call.startswith(("gh release", "python")) for call in calls)


def test_failed_tag_lookup_stops_the_release(tmp_path):
    result, calls = run_step(tmp_path, lookup_error=True)
    assert result.returncode != 0
    assert not any("--method POST" in call for call in calls)
