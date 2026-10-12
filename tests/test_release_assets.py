"""Release retries must preserve existing GitHub and PyPI distributions."""

import hashlib
import importlib.util
import io
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/publish_release_assets.py"
spec = importlib.util.spec_from_file_location("release_assets", SCRIPT)
assert spec and spec.loader
assets = importlib.util.module_from_spec(spec)
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
    monkeypatch.setattr(
        assets.urllib.request,
        "urlopen",
        lambda *args, **kwargs: io.StringIO(json.dumps(metadata)),
    )
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
    return wheel, state, calls


def test_missing_draft_asset_is_uploaded(release):
    wheel, _, calls = release
    assets.main(wheel.parent)
    assert calls == [["gh", "release", "upload", "v1.2.3", str(wheel)]]


def test_missing_published_asset_is_not_added(release):
    wheel, state, calls = release
    state["isDraft"] = False
    with pytest.raises(ValueError, match="Published release"):
        assets.main(wheel.parent)
    assert not calls


def test_differing_existing_asset_is_never_overwritten(release):
    wheel, state, calls = release
    state["assets"] = [{"name": wheel.name}]
    with pytest.raises(ValueError, match="Existing release asset differs"):
        assets.main(wheel.parent)
    assert all(command[2] != "upload" for command in calls)


def test_pypi_mismatch_stops_github_upload(release):
    wheel, _, calls = release
    wheel.write_bytes(b"rebuilt with different content")
    with pytest.raises(ValueError, match="differs from PyPI"):
        assets.main(wheel.parent)
    assert not calls
