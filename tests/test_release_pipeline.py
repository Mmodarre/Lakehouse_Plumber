"""Release decisions must bind version, source commit and package-index state."""

import importlib.util
import io
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "release_metadata", ROOT / "scripts/release_metadata.py"
)
assert spec and spec.loader
release = importlib.util.module_from_spec(spec)
with pytest.MonkeyPatch.context() as patch:
    patch.syspath_prepend(str(ROOT / "scripts"))
    spec.loader.exec_module(release)


@pytest.fixture
def pypi(monkeypatch):
    response = io.BytesIO(json.dumps({"info": {"version": "1.2.3"}}).encode())
    response.status = 404
    connection = Mock()
    connection.getresponse.return_value = response
    monkeypatch.setattr("http.client.HTTPSConnection", Mock(return_value=connection))
    return response


@pytest.fixture
def environment(tmp_path, monkeypatch, pypi):
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


def test_new_version_can_release(environment):
    output, _ = environment
    release.main()
    assert "publish=true" in output.read_text()
    assert "release=true" in output.read_text()


@pytest.mark.parametrize("status", [301, 302, 307, 308, 401, 429, 503])
def test_index_failure_cannot_trigger_publish(environment, pypi, status):
    output, _ = environment
    pypi.status = status
    with pytest.raises(RuntimeError, match=f"HTTP {status}"):
        release.main()
    assert not output.exists()


def test_ordinary_commit_does_not_republish_old_version(environment, pypi):
    output, tag = environment
    tag.returncode, tag.stdout = 0, "previous-sha\n"
    pypi.status = 200
    release.main()
    assert "release=false" in output.read_text()


def test_same_commit_can_resume_after_pypi_publication(environment, pypi):
    output, tag = environment
    tag.returncode, tag.stdout = 0, "tested-sha\n"
    pypi.status = 200
    release.main()
    assert "release=true" in output.read_text()
    assert "publish=false" in output.read_text()


def test_conflicting_tag_stops_new_publication(environment):
    output, tag = environment
    tag.returncode, tag.stdout = 0, "previous-sha\n"
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
