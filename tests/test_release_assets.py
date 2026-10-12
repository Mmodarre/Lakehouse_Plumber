"""Release retries must preserve existing GitHub and PyPI distributions."""

import hashlib
import importlib.util
import io
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/publish_release_assets.py"
spec = importlib.util.spec_from_file_location("release_assets", SCRIPT)
assert spec and spec.loader
assets = importlib.util.module_from_spec(spec)
with pytest.MonkeyPatch.context() as patch:
    patch.syspath_prepend(str(SCRIPT.parent))
    spec.loader.exec_module(assets)


@pytest.fixture
def release(tmp_path, monkeypatch):
    wheel = tmp_path / "example.whl"
    wheel.write_bytes(b"canonical wheel")
    monkeypatch.setenv("VERSION", "1.2.3")
    metadata = {
        "urls": [
            {
                "filename": wheel.name,
                "digests": {"sha256": hashlib.sha256(wheel.read_bytes()).hexdigest()},
            }
        ]
    }
    response = io.BytesIO(json.dumps(metadata).encode())
    response.status = 200
    connection = Mock()
    connection.getresponse.return_value = response
    monkeypatch.setattr("http.client.HTTPSConnection", Mock(return_value=connection))
    state = {"isDraft": True, "assets": []}
    monkeypatch.setattr(
        assets.subprocess,
        "check_output",
        lambda *args, **kwargs: json.dumps(state),
    )
    calls = []

    def execute(command, **kwargs):
        calls.append(command)
        if command[2] == "download":
            destination = Path(command[command.index("--dir") + 1])
            (destination / wheel.name).write_bytes(b"different published wheel")

    monkeypatch.setattr(assets.subprocess, "run", execute)
    return wheel, state, calls, response


def test_missing_draft_asset_is_uploaded(release):
    wheel, _, calls, _ = release
    assets.main(wheel.parent)
    assert calls == [["gh", "release", "upload", "v1.2.3", str(wheel)]]


def test_missing_published_asset_is_not_added(release):
    wheel, state, calls, _ = release
    state["isDraft"] = False
    with pytest.raises(ValueError, match="Published release"):
        assets.main(wheel.parent)
    assert not calls


def test_differing_existing_asset_is_never_overwritten(release):
    wheel, state, calls, _ = release
    state["assets"] = [{"name": wheel.name}]
    with pytest.raises(ValueError, match="Existing release asset differs"):
        assets.main(wheel.parent)
    assert all(command[2] != "upload" for command in calls)


def test_pypi_mismatch_stops_github_upload(release):
    wheel, _, calls, _ = release
    wheel.write_bytes(b"rebuilt with different content")
    with pytest.raises(ValueError, match="differs from PyPI"):
        assets.main(wheel.parent)
    assert not calls


def test_direct_pypi_404_allows_new_draft_asset(release):
    wheel, _, calls, response = release
    response.status = 404
    assets.main(wheel.parent)
    assert calls == [["gh", "release", "upload", "v1.2.3", str(wheel)]]


@pytest.mark.parametrize("status", [301, 302, 307, 308, 401, 429, 503])
def test_index_failure_stops_github_upload(release, status):
    wheel, _, calls, response = release
    response.status = status
    with pytest.raises(RuntimeError, match=f"HTTP {status}"):
        assets.main(wheel.parent)
    assert not calls
