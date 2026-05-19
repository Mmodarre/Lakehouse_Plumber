"""End-to-end tests for ``lhp state`` CLI subcommand (B4).

Verifies the four state-management workflows that the upcoming
incremental-regen refactor needs to preserve:

- ``--orphaned``: list files whose source YAML was deleted.
- ``--stale``: list files whose referenced source content changed.
- ``--orphaned --cleanup``: physically delete the orphaned files.
- ``--stale --regen``: rerun generation for stale files; new
  checksum in ``.lhp_state.json`` matches the modified source.

Phase 2 invariant: all mutations target the deep-copied tmp project
(``self.project_root``); no permanent fixtures are added under
``tests/e2e/fixtures/testing_project/``. Destructive operations
(``--cleanup``, ``--regen``) only ever touch the deep copy.
"""

import json
import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestStateCliE2E:
    """E2E coverage for ``lhp state`` subcommand."""

    __test__ = True

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
        # Sharded state lives under .lhp_state/ — _global.json plus one
        # <pipeline>.json shard per pipeline. ``state_file`` is the legacy
        # monolith path kept here only as the "presence" proxy callers
        # still expect; assertions go through ``self.state_dir``.
        self.state_dir = self.project_root / ".lhp_state"
        self.state_file = self.state_dir / "_global.json"

        self._init_bundle_project()

        yield
        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        """Wipe and recreate working directories."""
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)

        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def run_generate(self, *args) -> tuple:
        """Run 'lhp generate' with the given args. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["generate", *args])
        return result.exit_code, result.output

    def run_state(self, *args) -> tuple:
        """Run 'lhp state' with the given args. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["state", *args])
        return result.exit_code, result.output

    def _load_state_file(self) -> dict:
        """Rebuild the legacy-shape state dict from the new sharded format.

        Reads ``.lhp_state/_global.json`` for project-wide fields and
        merges every per-pipeline shard's ``environments`` slice into a
        single dict so assertions written against the old monolithic
        ``.lhp_state.json`` shape keep working unchanged.

        Returns an empty dict when ``.lhp_state/`` does not exist.
        """
        if not self.state_dir.exists():
            return {}

        result: dict = {"environments": {}}

        global_path = self.state_dir / "_global.json"
        if global_path.exists():
            with open(global_path, "r") as f:
                global_data = json.load(f)
            result["version"] = global_data.get("version", "1.0")
            result["last_updated"] = global_data.get("last_updated", "")
            result["global_dependencies"] = global_data.get(
                "global_dependencies", {}
            )
            result["last_generation_context"] = global_data.get(
                "last_generation_context", {}
            )

        for shard_path in sorted(self.state_dir.glob("*.json")):
            if shard_path.stem.startswith("_"):
                continue
            with open(shard_path, "r") as f:
                shard_data = json.load(f)
            for env_name, env_files in shard_data.get("environments", {}).items():
                result["environments"].setdefault(env_name, {}).update(env_files)

        return result

    def _extract_py_function_checksum(self, state: dict, py_function_path: str) -> str:
        """Extract the recorded checksum for a Python function dependency.

        Mirrors the pattern from ``test_bundle_manager_e2e.py``: walks the
        environments → generated-file → file_dependencies map and returns
        the checksum recorded for ``py_function_path``.
        """
        environments = state.get("environments", {}).get("dev", {})
        for _, file_info in environments.items():
            file_deps = file_info.get("file_dependencies", {})
            if py_function_path in file_deps:
                return file_deps[py_function_path]["checksum"]
        raise AssertionError(
            f"Python function not found in state dependencies: {py_function_path}"
        )

    def _seed_generate(self):
        """Run an initial 'lhp generate --env dev --force' to populate
        generated/ and .lhp_state.json. Asserts success."""
        exit_code, output = self.run_generate("--env", "dev", "--force")
        assert exit_code == 0, f"Initial generate must succeed:\n{output[-2000:]}"
        assert self.state_file.exists(), "_global.json must be created under .lhp_state/"

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_state_orphaned_lists_orphaned_files(self):
        """Deleting a flowgroup YAML and running ``lhp state --orphaned``
        must list the orphan .py file (no cleanup yet)."""
        self._seed_generate()

        orphan_yaml = (
            self.project_root
            / "pipelines"
            / "02_bronze"
            / "orders"
            / "orders_bronze.yaml"
        )
        orphan_py = self.generated_dir / "acmi_edw_bronze" / "orders_bronze.py"
        assert orphan_py.exists(), "Setup precondition: orphan .py must exist"

        orphan_yaml.unlink()

        exit_code, output = self.run_state("--env", "dev", "--orphaned")
        assert exit_code == 0, f"state --orphaned should succeed, got:\n{output[-2000:]}"
        assert "orders_bronze.py" in output, (
            f"Orphan .py path must be listed. Got:\n{output[-2000:]}"
        )
        # Sanity: file should still exist (no --cleanup)
        assert orphan_py.exists(), "Without --cleanup, the .py must remain on disk"

    def test_state_stale_lists_stale_files(self):
        """Modifying a referenced Python function and running
        ``lhp state --stale`` must list files dependent on it."""
        self._seed_generate()

        py_function = self.project_root / "py_functions" / "sample_func.py"
        original = py_function.read_text()
        # Append a comment to change the source checksum without breaking syntax.
        py_function.write_text(original + "\n# B4-stale: trigger checksum diff\n")

        exit_code, output = self.run_state("--env", "dev", "--stale")
        assert exit_code == 0, f"state --stale should succeed, got:\n{output[-2000:]}"
        assert "sample_func.py" in output or "python_func_flowgroup.py" in output, (
            f"Stale output must reference dependent file(s). Got:\n{output[-2000:]}"
        )
        assert (
            "stale" in output.lower()
            or "changed" in output.lower()
            or "dependency" in output.lower()
        ), f"Expected stale/changed indicator. Got:\n{output[-2000:]}"

    def test_state_cleanup_removes_orphans(self):
        """``lhp state --orphaned --cleanup`` must physically delete the
        orphan .py file and update ``.lhp_state.json`` accordingly."""
        self._seed_generate()

        orphan_yaml = (
            self.project_root
            / "pipelines"
            / "02_bronze"
            / "orders"
            / "orders_bronze.yaml"
        )
        orphan_py = self.generated_dir / "acmi_edw_bronze" / "orders_bronze.py"
        orphan_rel = "generated/dev/acmi_edw_bronze/orders_bronze.py"

        orphan_yaml.unlink()
        assert orphan_py.exists(), "Setup precondition: orphan .py present before cleanup"

        # Confirm the orphan is tracked in state before cleanup
        pre_state = self._load_state_file()
        pre_env = pre_state.get("environments", {}).get("dev", {})
        assert any(orphan_rel in key for key in pre_env), (
            "Orphan .py must be present in .lhp_state.json before cleanup"
        )

        exit_code, output = self.run_state(
            "--env", "dev", "--orphaned", "--cleanup"
        )
        assert exit_code == 0, f"--cleanup should succeed, got:\n{output[-2000:]}"

        assert not orphan_py.exists(), (
            "--cleanup must physically delete the orphan .py file"
        )

        # State file must drop the orphan key after cleanup
        post_state = self._load_state_file()
        post_env = post_state.get("environments", {}).get("dev", {})
        assert not any(orphan_rel in key for key in post_env), (
            "Orphan entry must be removed from .lhp_state.json after cleanup"
        )

    def test_state_regen_regenerates_stale(self):
        """Modifying a referenced Python function and running
        ``lhp state --stale --regen`` must regenerate the dependent .py
        files; the dependency checksum in ``.lhp_state.json`` updates to
        match the new source."""
        self._seed_generate()

        py_function_path = "py_functions/sample_func.py"
        py_function = self.project_root / py_function_path

        baseline_state = self._load_state_file()
        baseline_checksum = self._extract_py_function_checksum(
            baseline_state, py_function_path
        )

        original = py_function.read_text()
        py_function.write_text(original + "\n# B4-regen: trigger checksum diff\n")

        exit_code, output = self.run_state(
            "--env", "dev", "--stale", "--regen"
        )
        assert exit_code == 0, f"--regen should succeed, got:\n{output[-2000:]}"
        assert (
            "Regenerated" in output or "regenerated" in output.lower()
        ), f"Expected regeneration confirmation. Got:\n{output[-2000:]}"

        updated_state = self._load_state_file()
        updated_checksum = self._extract_py_function_checksum(
            updated_state, py_function_path
        )

        assert updated_checksum != baseline_checksum, (
            "Checksum for the modified Python function must change after --regen "
            f"(baseline={baseline_checksum!r}, updated={updated_checksum!r})"
        )
