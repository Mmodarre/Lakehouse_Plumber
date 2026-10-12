"""Upload missing draft assets; never replace differing published artifacts."""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(directory: Path) -> None:
    tag = f"v{os.environ['VERSION']}"
    try:
        with urllib.request.urlopen(
            f"https://pypi.org/pypi/lakehouse-plumber/{os.environ['VERSION']}/json",
            timeout=30,
        ) as response:
            packages = json.load(response)["urls"]
        hashes = {item["filename"]: item["digests"]["sha256"] for item in packages}
        for path in directory.iterdir():
            if path.name.endswith((".whl", ".tar.gz")) and hashes.get(
                path.name
            ) != digest(path):
                raise ValueError(
                    f"Canonical distribution differs from PyPI: {path.name}"
                )
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise
    release = json.loads(
        subprocess.check_output(
            ["gh", "release", "view", tag, "--json", "assets,isDraft"], text=True
        )
    )
    existing = {asset["name"] for asset in release["assets"]}
    for path in sorted(directory.iterdir()):
        if path.name in existing:
            with tempfile.TemporaryDirectory() as temporary:
                subprocess.run(
                    [
                        "gh",
                        "release",
                        "download",
                        tag,
                        "--pattern",
                        path.name,
                        "--dir",
                        temporary,
                    ],
                    check=True,
                )
                if digest(Path(temporary) / path.name) != digest(path):
                    raise ValueError(
                        f"Existing release asset differs: {path.name}; do not overwrite a release"
                    )
        elif not release["isDraft"]:
            raise ValueError(
                "Published release is missing an expected asset; investigate rather than modifying it"
            )
        else:
            subprocess.run(["gh", "release", "upload", tag, str(path)], check=True)


if __name__ == "__main__":
    main(Path(sys.argv[1]))
