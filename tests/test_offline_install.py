"""Offline installer must fail before pip on corruption or target mismatch."""

import hashlib
import importlib.util
import json
import platform
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "install_offline.py"
spec = importlib.util.spec_from_file_location("offline_install", SCRIPT)
assert spec and spec.loader
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


@pytest.fixture
def bundle(tmp_path: Path) -> Path:
    (tmp_path / "requirements.lock").write_bytes(b"example==1.0\n")
    machine = {"amd64": "x86_64", "aarch64": "arm64"}.get(
        platform.machine().lower(), platform.machine().lower()
    )
    manifest = {
        "system": platform.system().lower(),
        "machine": machine,
        "python": f"{sys.version_info.major}.{sys.version_info.minor}",
        "files": {"requirements.lock": hashlib.sha256(b"example==1.0\n").hexdigest()},
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    return tmp_path


def test_valid_bundle_passes_verification(bundle: Path) -> None:
    installer.verify(bundle)


def test_notebook_entrypoint_keeps_network_hook_in_child(
    bundle: Path, monkeypatch
) -> None:
    calls = []
    monkeypatch.setattr(
        installer.subprocess, "run", lambda args, **kwargs: calls.append(args)
    )
    monkeypatch.setattr(
        installer.sys,
        "addaudithook",
        lambda hook: pytest.fail("Audit hook leaked into notebook"),
    )
    installer.install(bundle)
    assert calls == [[sys.executable, str(bundle / "install.py"), "--worker"]]


def test_corrupt_bundle_fails_before_install(bundle: Path) -> None:
    (bundle / "requirements.lock").write_text("changed")
    with pytest.raises(ValueError, match="checksum"):
        installer.verify(bundle)


def test_wrong_interpreter_is_rejected(bundle: Path) -> None:
    manifest = json.loads((bundle / "manifest.json").read_text())
    manifest["python"] = "0.0"
    (bundle / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="interpreter"):
        installer.verify(bundle)


def test_manifest_cannot_escape_bundle(bundle: Path) -> None:
    manifest = json.loads((bundle / "manifest.json").read_text())
    manifest["files"] = {"../outside": "ignored"}
    (bundle / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="manifest path"):
        installer.verify(bundle)


@pytest.mark.parametrize("event", ["socket.connect", "socket.getaddrinfo"])
def test_installer_rejects_network_events(event: str) -> None:
    with pytest.raises(RuntimeError, match="network"):
        installer.deny_network(event, ())
