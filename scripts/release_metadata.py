"""Fail closed on inconsistent tags, lock metadata or package-index errors."""

import json
import os
import re
import subprocess
import tomllib
import urllib.error
import urllib.request
from pathlib import Path


def main() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text())["project"]
    version = project["version"]
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise ValueError("Release version must be a stable three-part version")
    lock = tomllib.loads(Path("uv.lock").read_text())
    locked = next(p for p in lock["package"] if p["name"] == project["name"])
    if locked["version"] != version:
        raise ValueError("uv.lock and pyproject.toml versions differ")
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if sha != os.environ["SOURCE_SHA"]:
        raise ValueError("Checkout differs from tested commit")
    tag = subprocess.run(
        ["git", "rev-parse", "--verify", f"refs/tags/v{version}^{{commit}}"],
        capture_output=True,
        text=True,
    )
    try:
        with urllib.request.urlopen(
            f"https://pypi.org/pypi/lakehouse-plumber/{version}/json", timeout=30
        ) as response:
            metadata = json.load(response)
        if metadata["info"]["version"] != version:
            raise ValueError("Unexpected package-index response")
        publish = "false"
    except urllib.error.HTTPError as exc:
        if exc.code != 404:
            raise
        publish = "true"
    same_tag = tag.returncode == 0 and tag.stdout.strip() == sha
    if publish == "true" and tag.returncode == 0 and not same_tag:
        raise ValueError(
            "Release tag already belongs to another commit; bump the version"
        )
    # Ordinary commits without a version bump do not rebuild an existing release.
    release = "true" if publish == "true" or same_tag else "false"
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        output.write(
            f"version={version}\nsha={sha}\npublish={publish}\nrelease={release}\n"
        )


if __name__ == "__main__":
    main()
