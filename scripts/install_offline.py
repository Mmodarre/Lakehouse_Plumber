"""Install this extracted bundle using only its verified local wheels.

Run: python install.py --python /path/to/environment/python
In Databricks use the current notebook's Python interpreter. Python itself is
not included. This script does not create or overwrite a virtual environment.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import runpy
import subprocess
import sys
from pathlib import Path


def verify(root: Path) -> dict[str, object]:
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    machine = {"amd64": "x86_64", "aarch64": "arm64"}.get(
        platform.machine().lower(), platform.machine().lower()
    )
    actual = (
        platform.system().lower(),
        machine,
        f"{sys.version_info.major}.{sys.version_info.minor}",
    )
    expected = (manifest["system"], manifest["machine"], manifest["python"])
    if actual != expected:
        raise ValueError(f"Bundle requires {expected}; this interpreter is {actual}")
    for name, digest in manifest["files"].items():
        path = (root / name).resolve()
        if not path.is_relative_to(root.resolve()):
            raise ValueError("Invalid bundle manifest path")
        if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise ValueError(f"Bundle checksum failed: {name}")
    return manifest


def deny_network(event: str, args: tuple[object, ...]) -> None:
    """Fail instead of silently retrieving a missing dependency."""
    if event in {"socket.connect", "socket.getaddrinfo"}:
        raise RuntimeError("Offline installation attempted network access")


def _install_in_process(root: Path) -> None:
    verify(root)
    os.environ["PIP_CONFIG_FILE"] = os.devnull
    os.environ["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
    sys.addaudithook(deny_network)
    sys.argv = [
        "pip",
        "install",
        "--isolated",
        "--disable-pip-version-check",
        "--no-index",
        "--no-cache-dir",
        "--find-links",
        str(root / "wheels"),
        "--only-binary=:all:",
        "--require-hashes",
        "-r",
        str(root / "requirements.lock"),
    ]
    runpy.run_module("pip", run_name="__main__")


def install(root: Path, python: Path | None = None) -> None:
    """Keep the network-denial hook out of the caller's notebook interpreter."""
    subprocess.run(
        [
            str(python or sys.executable),
            str(root / "install.py"),
            "--worker",
        ],
        check=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, help="Interpreter to install into")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    options = parser.parse_args()
    root = Path(__file__).resolve().parent
    if options.worker:
        _install_in_process(root)
    else:
        install(root, options.python)
