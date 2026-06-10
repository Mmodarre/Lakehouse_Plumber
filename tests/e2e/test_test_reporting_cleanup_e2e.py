"""End-to-end tests for test-reporting artifact cleanup on ``--include-tests`` transition.

Verifies that when a user generates with ``--include-tests`` and later
regenerates without the flag, the previously-created ``_test_reporting_hook.py``,
``test_reporting_providers/<stem>.py`` and ``test_reporting_providers/__init__.py``
are removed from disk AND untracked in ``.lhp_state.json``.
"""

import hashlib
import json
import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestTestReportingCleanupE2E:
    """E2E regression tests for test-reporting artifact cleanup on flag transition."""

    __test__ = True

    PIPELINE = "acmi_edw_bronze"

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"

        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)

        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

        yield
        os.chdir(self.original_cwd)

    def _run(self, *args: str) -> tuple:
        """Run ``lhp`` with the given args. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, list(args))
        return result.exit_code, result.output

    def _test_reporting_paths(self) -> dict:
        pipeline_dir = self.generated_dir / self.PIPELINE
        return {
            "hook": pipeline_dir / "_test_reporting_hook.py",
            "provider": pipeline_dir
            / "test_reporting_providers"
            / "test_reporting_publisher.py",
            "provider_init": pipeline_dir / "test_reporting_providers" / "__init__.py",
            "providers_dir": pipeline_dir / "test_reporting_providers",
        }

    def _load_state_file(self) -> dict:
        """Rebuild the legacy-shape state dict from the new sharded format.

        Merges ``.lhp_state/_global.json`` + per-pipeline shards into the
        old monolithic dict shape so the artifact_type filtering below
        keeps working unchanged.
        """
        state_dir = self.project_root / ".lhp_state"
        if not state_dir.exists():
            return {}

        result: dict = {"environments": {}}

        global_path = state_dir / "_global.json"
        if global_path.exists():
            with open(global_path, "r") as f:
                global_data = json.load(f)
            result["version"] = global_data.get("version", "1.0")
            result["last_updated"] = global_data.get("last_updated", "")
            result["global_dependencies"] = global_data.get("global_dependencies", {})
            result["last_generation_context"] = global_data.get(
                "last_generation_context", {}
            )

        for shard_path in sorted(state_dir.glob("*.json")):
            if shard_path.stem.startswith("_"):
                continue
            with open(shard_path, "r") as f:
                shard_data = json.load(f)
            for env_name, env_files in shard_data.get("environments", {}).items():
                result["environments"].setdefault(env_name, {}).update(env_files)

        return result

    def _test_reporting_entries(self, state: dict) -> list:
        env_files = state.get("environments", {}).get("dev", {})
        return [
            (path, entry)
            for path, entry in env_files.items()
            if (entry.get("artifact_type") or "").startswith("test_reporting")
        ]

    def _compare_file_hashes(self, file1: Path, file2: Path) -> str:
        """Compare two files by SHA-256. Returns '' if identical, else a diff msg."""

        def _hash(p: Path) -> str:
            return hashlib.sha256(p.read_bytes()).hexdigest()

        h1, h2 = _hash(file1), _hash(file2)
        if h1 != h2:
            return (
                f"Hash mismatch: {file1.name} ({h1[:12]}) != {file2.name} ({h2[:12]})"
            )
        return ""
