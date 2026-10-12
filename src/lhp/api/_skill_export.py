"""Portable, non-destructive export of the packaged skill."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from lhp.api import _skill_assets as assets
from lhp.errors import ErrorFactory, codes

MANIFEST = ".lhp_skill_manifest.json"


def skill_payload(skill_name: str = "lhp") -> dict[str, bytes]:
    """Include release identity and content hashes alongside all references.

    :stability: provisional
    """
    payload = assets.skill_files(skill_name)
    version = assets.current_version()
    payload[assets.MARKER_FILE] = f"{version}\n".encode()
    manifest = {
        "skill": skill_name,
        "version": version,
        "files": {
            name: hashlib.sha256(data).hexdigest() for name, data in payload.items()
        },
    }
    payload[MANIFEST] = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    return payload


def check_existing(existing: dict[str, bytes], payload: dict[str, bytes]) -> None:
    """Allow identical files and resumable partial installs, never replace edits.

    :stability: provisional
    """
    conflicts = sorted(
        name for name, data in existing.items() if payload.get(name) != data
    )
    if conflicts:
        raise ErrorFactory.io_error(
            codes.IO_020,
            title="Existing skill differs from the packaged release",
            details="Conflicting files: " + ", ".join(conflicts),
            suggestions=[
                "Preserve the existing skill before moving it aside or choosing a new destination.",
                "Read the installed package's skill directly to continue this session.",
            ],
        )


def export_files(destination: Path, payload: dict[str, bytes]) -> None:
    """Preflight the entire destination before writing; verify every output.

    :stability: provisional
    """
    if destination.is_symlink() or (destination.exists() and not destination.is_dir()):
        raise OSError(f"Skill destination must be a real directory: {destination}")
    existing = {}
    for path in destination.rglob("*"):
        if path.is_symlink():
            raise OSError(f"Skill destination contains a symbolic link: {path}")
        if path.is_file():
            existing[path.relative_to(destination).as_posix()] = path.read_bytes()
    check_existing(existing, payload)
    for name, data in payload.items():
        path = destination / name
        if name not in existing:
            path.parent.mkdir(parents=True, exist_ok=True)
            # Exclusive creation also protects against changes after preflight.
            with path.open("xb") as output:
                output.write(data)
        if path.read_bytes() != data:
            raise OSError(f"Skill verification failed: {path}")
