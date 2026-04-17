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
        """Create isolated copy of fixture project for each test."""
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

    # ========================================================================
    # HELPER METHODS
    # ========================================================================

    def _run(self, *args: str) -> tuple:
        """Run ``lhp`` with the given args. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, list(args))
        return result.exit_code, result.output

    def _test_reporting_paths(self) -> dict:
        """Return the three expected test-reporting artifact paths for the pipeline."""
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
        """Load and parse .lhp_state.json (empty dict if missing)."""
        state_file = self.project_root / ".lhp_state.json"
        if not state_file.exists():
            return {}
        with open(state_file, "r") as f:
            return json.load(f)

    def _test_reporting_entries(self, state: dict) -> list:
        """Return all state entries whose artifact_type starts with 'test_reporting'."""
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

    # ========================================================================
    # TEST CASES
    # ========================================================================

    def test_include_tests_transition_cleans_artifacts(self):
        """Regression: regen w/o --include-tests reaps stale test-reporting artifacts.

        Covers the full lifecycle:
          Phase 1: generate --include-tests → artifacts present, state tracks them.
          Phase 2: generate (non-force) → artifacts deleted, state untracks them,
                   flowgroup .py files still match the no-tests baseline, and the
                   now-empty ``test_reporting_providers/`` directory is pruned.
          Phase 3: regen again → idempotent (no changes, exit 0).
        """
        paths = self._test_reporting_paths()
        pipeline_dir = self.generated_dir / self.PIPELINE

        # ---------- Phase 1: generate WITH --include-tests ----------
        exit_code, output = self._run(
            "generate", "--env", "dev", "--force", "--include-tests"
        )
        assert exit_code == 0, f"Phase 1 generation failed: {output}"

        assert paths["hook"].exists(), "Phase 1: hook file should exist"
        assert paths["provider"].exists(), "Phase 1: provider module should be copied"
        assert paths[
            "provider_init"
        ].exists(), "Phase 1: provider __init__ should exist"

        state = self._load_state_file()
        tr_entries = self._test_reporting_entries(state)
        assert len(tr_entries) == 3, (
            f"Phase 1: expected 3 test_reporting_* state entries, "
            f"got {len(tr_entries)}: {[e[0] for e in tr_entries]}"
        )

        # ---------- Phase 2: regenerate WITHOUT --include-tests (non-force) ----------
        exit_code, output = self._run("generate", "--env", "dev")
        assert exit_code == 0, f"Phase 2 generation failed: {output}"

        assert not paths["hook"].exists(), "Phase 2: hook file should be deleted"
        assert not paths[
            "provider"
        ].exists(), "Phase 2: provider module should be deleted"
        assert not paths[
            "provider_init"
        ].exists(), "Phase 2: provider __init__ should be deleted"
        assert not paths["providers_dir"].exists(), (
            "Phase 2: empty test_reporting_providers/ directory should be pruned "
            "by cleanup_empty_directories"
        )

        state = self._load_state_file()
        tr_entries = self._test_reporting_entries(state)
        assert tr_entries == [], (
            f"Phase 2: all test_reporting_* state entries should be gone, "
            f"got {[e[0] for e in tr_entries]}"
        )

        # Flowgroup .py files still exist and match the no-tests baseline
        baseline_dir = self.project_root / "generated_baseline" / "dev" / self.PIPELINE
        assert baseline_dir.exists(), "no-tests baseline must exist for comparison"
        for baseline_file in baseline_dir.rglob("*.py"):
            rel = baseline_file.relative_to(baseline_dir)
            generated_file = pipeline_dir / rel
            assert (
                generated_file.exists()
            ), f"Phase 2: flowgroup file {rel} missing from generated output"
            diff = self._compare_file_hashes(generated_file, baseline_file)
            assert (
                diff == ""
            ), f"Phase 2: flowgroup diverged from no-tests baseline: {diff}"

        # Generated output must not contain any stray test-reporting file
        stragglers = [p for p in pipeline_dir.rglob("*test_reporting*") if p.exists()]
        assert (
            stragglers == []
        ), f"Phase 2: test_reporting artifacts still present on disk: {stragglers}"

        # ---------- Phase 3: idempotence — regenerate once more, no changes ----------
        state_before = self._load_state_file()
        exit_code, output = self._run("generate", "--env", "dev")
        assert exit_code == 0, f"Phase 3 generation failed: {output}"

        state_after = self._load_state_file()
        assert (
            self._test_reporting_entries(state_after) == []
        ), "Phase 3: no test_reporting_* entries should reappear"
        # Environment file set should be stable across phase 2 → phase 3
        before_keys = set(state_before.get("environments", {}).get("dev", {}).keys())
        after_keys = set(state_after.get("environments", {}).get("dev", {}).keys())
        assert before_keys == after_keys, (
            f"Phase 3: state key set changed unexpectedly. "
            f"added={after_keys - before_keys} removed={before_keys - after_keys}"
        )
