"""Upload missing draft assets; never replace differing published artifacts."""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from _release_pypi import fetch_metadata


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(directory: Path) -> None:
    tag = f"v{os.environ['VERSION']}"
    metadata = fetch_metadata(os.environ["VERSION"])
    if metadata is not None:
        packages = metadata["urls"]
        hashes = {item["filename"]: item["digests"]["sha256"] for item in packages}
        for path in directory.iterdir():
            if path.name.endswith((".whl", ".tar.gz")) and hashes.get(
                path.name
            ) != digest(path):
                raise ValueError(
                    f"Canonical distribution differs from PyPI: {path.name}"
                )
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
