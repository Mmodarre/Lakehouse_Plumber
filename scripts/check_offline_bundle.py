"""Exercise an extracted release archive in a fresh native environment."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import tempfile
import venv
import zipfile
from pathlib import Path


def check(archive: Path) -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        with zipfile.ZipFile(archive) as zipped:
            zipped.extractall(root)
        bundle = root / archive.stem
        environment = root / "env"
        venv.EnvBuilder(with_pip=True).create(environment)
        bindir = environment / ("Scripts" if os.name == "nt" else "bin")
        python = bindir / ("python.exe" if os.name == "nt" else "python")
        executable = bindir / ("lhp.exe" if os.name == "nt" else "lhp")
        env = {**os.environ, "LHP_TELEMETRY": "off", "PYTHONPATH": ""}
        subprocess.run([str(python), str(bundle / "install.py")], env=env, check=True)
        subprocess.run([str(python), "-m", "pip", "check"], env=env, check=True)
        project = root / "lesson"
        project.mkdir()
        commands = [
            ["--version"],
            ["skill", "export", "--output", str(root / "skill")],
            [
                "skill",
                "export",
                "--name",
                "lhp-learn",
                "--output",
                str(root / "teacher"),
            ],
            ["init", "offline_lesson", "--sample", "--no-git"],
            ["validate", "--env", "dev", "-pc", "config/pipeline_config.yaml"],
            ["generate", "--env", "dev", "-pc", "config/pipeline_config.yaml"],
        ]
        for command in commands:
            subprocess.run(
                [str(executable), *command], cwd=project, env=env, check=True
            )
        if (root / "skill" / "lessons").exists():
            raise AssertionError("Development skill contains teaching lessons")
        for name in (
            "lessons/00-understand-lhp.md",
            "lessons/05-make-a-change.md",
            "topics/quality-and-quarantine.md",
            "assets/progress.md",
            "references/actions-load-delta.md",
        ):
            if not (root / "teacher" / name).is_file():
                raise AssertionError(
                    f"Missing self-contained teaching material: {name}"
                )
        for directory in (root / "skill", root / "teacher"):
            manifest = json.loads((directory / ".lhp_skill_manifest.json").read_text())
            for name, digest in manifest["files"].items():
                if (
                    hashlib.sha256((directory / name).read_bytes()).hexdigest()
                    != digest
                ):
                    raise AssertionError(f"Skill manifest mismatch: {name}")
            for document in directory.rglob("*.md"):
                for link in re.findall(
                    r"\]\(([^)]+\.md)(?:#[^)]*)?\)",
                    document.read_text(encoding="utf-8"),
                ):
                    if "://" not in link and not (document.parent / link).is_file():
                        raise AssertionError(
                            f"Missing offline resource: {document}: {link}"
                        )
        if not list((project / "generated").rglob("*.py")):
            raise AssertionError("Sample generated no Python")
        gold = project / "pipelines" / "03_gold" / "sales_by_nation.yaml"
        description = "Revenue by nation, explained in the LHP first-project lesson"
        gold.write_text(
            gold.read_text() + f'\n      comment: "{description}"\n', encoding="utf-8"
        )
        for command in commands[-2:]:
            subprocess.run(
                [str(executable), *command], cwd=project, env=env, check=True
            )
        if not any(
            description in path.read_text()
            for path in (project / "generated").rglob("*.py")
        ):
            raise AssertionError("Lesson change did not reach generated Python")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    check(parser.parse_args().archive.resolve())
