"""Build a native, hash-pinned release wheelhouse from uv.lock."""

from __future__ import annotations

import argparse
import email
import hashlib
import json
import platform
import shutil
import subprocess
import sys
import tempfile
import tomllib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def wheel_identity(wheel: Path) -> tuple[str, str]:
    with zipfile.ZipFile(wheel) as archive:
        names = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(names) != 1:
            raise ValueError(f"Expected one distribution in {wheel.name}")
        metadata = email.message_from_bytes(archive.read(names[0]))
    return str(metadata["Name"]), str(metadata["Version"])


def build(wheel: Path, output: Path) -> Path:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    if wheel_identity(wheel) != ("lakehouse-plumber", project["version"]):
        raise ValueError("Release wheel does not match pyproject.toml")
    system = platform.system().lower()
    machine = {"amd64": "x86_64", "aarch64": "arm64"}.get(
        platform.machine().lower(), platform.machine().lower()
    )
    python = f"{sys.version_info.major}.{sys.version_info.minor}"
    name = f"lhp-offline-{project['version']}-{system}-{machine}-py{python.replace('.', '')}"
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temporary:
        stage = Path(temporary) / name
        wheels = stage / "wheels"
        wheels.mkdir(parents=True)
        requirements = stage / "requirements.lock"
        # Locked checks freshness; frozen alone would silently accept stale metadata.
        subprocess.run(
            [
                "uv",
                "export",
                "--locked",
                "--no-dev",
                "--extra",
                "genie",
                "--no-emit-project",
                "--output-file",
                str(requirements),
            ],
            cwd=ROOT,
            check=True,
            stdout=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "download",
                "--only-binary=:all:",
                "--require-hashes",
                "--dest",
                str(wheels),
                "-r",
                str(requirements),
            ],
            check=True,
        )
        shutil.copy2(wheel, wheels / wheel.name)
        pins = []
        for path in sorted(wheels.glob("*.whl")):
            distribution, version = wheel_identity(path)
            extra = "[genie]" if distribution == "lakehouse-plumber" else ""
            pins.append(
                f"{distribution}{extra}=={version} --hash=sha256:{sha256(path)}"
            )
        requirements.write_text("\n".join(pins) + "\n", encoding="utf-8")
        shutil.copy2(ROOT / "scripts" / "install_offline.py", stage / "install.py")
        shutil.copy2(ROOT / "LICENSE", stage / "LICENSE")
        for guide in ("agent.md", "learn.md"):
            shutil.copy2(ROOT / "docs" / "_agent_guides" / guide, stage / guide)
        manifest = {
            "version": project["version"],
            "source_sha": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
            "python": python,
            "system": system,
            "machine": machine,
            "scope": "CLI, agent skill, lessons and Genie installer; no webapp extra",
            "lock_sha256": sha256(ROOT / "uv.lock"),
            "files": {
                p.relative_to(stage).as_posix(): sha256(p)
                for p in sorted(stage.rglob("*"))
                if p.is_file()
            },
        }
        (stage / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        archive = output / f"{name}.zip"
        # Stable archive bytes make interrupted release uploads safely retryable.
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zipped:
            for path in sorted(stage.rglob("*")):
                if path.is_file():
                    info = zipfile.ZipInfo(
                        f"{name}/{path.relative_to(stage).as_posix()}"
                    )
                    info.compress_type = zipfile.ZIP_DEFLATED
                    info.external_attr = 0o100644 << 16
                    zipped.writestr(info, path.read_bytes())
        archive.with_suffix(".zip.sha256").write_text(
            f"{sha256(archive)}  {archive.name}\n", encoding="utf-8"
        )
        return archive


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(build(args.wheel.resolve(), args.output.resolve()))
