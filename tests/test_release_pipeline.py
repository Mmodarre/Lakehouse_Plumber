"""Release decisions must bind version, source commit and package-index state."""

import importlib.util
import io
import json
import urllib.error
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "release_metadata", ROOT / "scripts/release_metadata.py"
)
assert spec and spec.loader
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


@pytest.fixture
def environment(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname="lakehouse-plumber"\nversion="1.2.3"\n'
    )
    (tmp_path / "uv.lock").write_text(
        '[[package]]\nname="lakehouse-plumber"\nversion="1.2.3"\n'
    )
    output = tmp_path / "output"
    monkeypatch.setenv("SOURCE_SHA", "tested-sha")
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setattr(
        release.subprocess, "check_output", lambda *args, **kwargs: "tested-sha\n"
    )
    tag = SimpleNamespace(returncode=1, stdout="")
    monkeypatch.setattr(release.subprocess, "run", lambda *args, **kwargs: tag)
    return output, tag


def missing(url, **kwargs):
    raise urllib.error.HTTPError(url, 404, "missing", {}, None)


def test_new_version_can_release(environment, monkeypatch):
    output, _ = environment
    monkeypatch.setattr(release.urllib.request, "urlopen", missing)
    release.main()
    assert "publish=true" in output.read_text()
    assert "release=true" in output.read_text()


def test_index_outage_cannot_trigger_publish(environment, monkeypatch):
    output, _ = environment

    def outage(url, **kwargs):
        raise urllib.error.HTTPError(url, 503, "unavailable", {}, None)

    monkeypatch.setattr(release.urllib.request, "urlopen", outage)
    with pytest.raises(urllib.error.HTTPError):
        release.main()
    assert not output.exists()


def test_ordinary_commit_does_not_republish_old_version(environment, monkeypatch):
    output, tag = environment
    tag.returncode, tag.stdout = 0, "previous-sha\n"
    monkeypatch.setattr(
        release.urllib.request,
        "urlopen",
        lambda *args, **kwargs: io.StringIO(json.dumps({"info": {"version": "1.2.3"}})),
    )
    release.main()
    assert "release=false" in output.read_text()


def test_same_commit_can_resume_after_pypi_publication(environment, monkeypatch):
    output, tag = environment
    tag.returncode, tag.stdout = 0, "tested-sha\n"
    monkeypatch.setattr(
        release.urllib.request,
        "urlopen",
        lambda *args, **kwargs: io.StringIO(json.dumps({"info": {"version": "1.2.3"}})),
    )
    release.main()
    assert "release=true" in output.read_text()
    assert "publish=false" in output.read_text()


def test_conflicting_tag_stops_new_publication(environment, monkeypatch):
    output, tag = environment
    tag.returncode, tag.stdout = 0, "previous-sha\n"
    monkeypatch.setattr(release.urllib.request, "urlopen", missing)
    with pytest.raises(ValueError, match="another commit"):
        release.main()
    assert not output.exists()


def test_stale_lock_cannot_release(environment):
    output, _ = environment
    Path("uv.lock").write_text(
        '[[package]]\nname="lakehouse-plumber"\nversion="1.2.2"\n'
    )
    with pytest.raises(ValueError, match="versions differ"):
        release.main()
    assert not output.exists()
