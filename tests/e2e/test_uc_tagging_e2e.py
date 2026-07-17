"""E2E tests for Unity Catalog tagging hook generation.

Covers the real CLI generate path (YAML -> generated/dev/<pipeline>/_uc_tagging_hook.py)
against byte-exact baselines, mirroring test_test_reporting_e2e.py. The dedicated
uc_tagging_* fixture pipelines exercise table-only / column-only / combined tags,
value coercions, materialized_view parity, and ${token} substitution in tag
keys/values. Column tags are sourced from a ``tags_file`` sidecar (not the schema
file); one flowgroup declares column tags with no ``table_schema`` at all. The two
negative emission cases (no tags, disabled) are also covered.
"""

import hashlib
import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestUCTaggingE2E:
    __test__ = True

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"

        self._init_bundle_project()

        yield
        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)

        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    def run_generate(self) -> tuple:
        """Run 'lhp generate --env dev'. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--pipeline-config",
                "config/pipeline_config.yaml",
            ],
        )
        return result.exit_code, result.output

    def _compare_file_hashes(self, file1: Path, file2: Path) -> str:
        """Compare two files by SHA-256. Returns '' if identical."""

        def get_hash(f):
            return hashlib.sha256(f.read_bytes()).hexdigest()

        h1, h2 = get_hash(file1), get_hash(file2)
        if h1 != h2:
            return (
                f"Hash mismatch: {file1.name} ({h1[:12]}) != {file2.name} ({h2[:12]})"
            )
        return ""

    def test_core_hook_matches_baseline(self):
        """uc_tagging_core hook matches baseline: table tags, column tags sourced
        from tags_file sidecars (including a no-schema flowgroup), value coercions,
        multi-flowgroup aggregation, and the combined database: form."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        generated_file = self.generated_dir / "uc_tagging_core" / "_uc_tagging_hook.py"
        assert generated_file.exists(), "_uc_tagging_hook.py should be generated"

        baseline_file = (
            self.project_root
            / "generated_baseline"
            / "dev"
            / "uc_tagging_core"
            / "_uc_tagging_hook.py"
        )
        assert baseline_file.exists(), "Baseline file should exist"

        hash_diff = self._compare_file_hashes(generated_file, baseline_file)
        assert hash_diff == "", f"Baseline mismatch: {hash_diff}"

    def test_mv_hook_matches_baseline(self):
        """uc_tagging_mv hook matches baseline: materialized_view parity, table +
        column tags on one FQN via a single tags_file, and ${token} substitution in
        a tag key and value."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        generated_file = self.generated_dir / "uc_tagging_mv" / "_uc_tagging_hook.py"
        assert generated_file.exists(), "_uc_tagging_hook.py should be generated"

        baseline_file = (
            self.project_root
            / "generated_baseline"
            / "dev"
            / "uc_tagging_mv"
            / "_uc_tagging_hook.py"
        )
        assert baseline_file.exists(), "Baseline file should exist"

        hash_diff = self._compare_file_hashes(generated_file, baseline_file)
        assert hash_diff == "", f"Baseline mismatch: {hash_diff}"

    def test_noschema_column_tags_via_tags_file(self):
        """A write target with NO table_schema still gets column tags from its
        tags_file (uc_region.r_name), keyed by the resolved FQN, while its
        column-only sidecar adds no table-level tag."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        hook_file = self.generated_dir / "uc_tagging_core" / "_uc_tagging_hook.py"
        content = hook_file.read_text()

        # Column tags resolve for a schema-less table, keyed by the resolved FQN.
        assert (
            '"acme_edw_dev.edw_bronze.uc_region": '
            '{"r_name": {"classification": "internal"}}'
        ) in content, (
            "no-schema column tags must appear in _COLUMN_TAGS keyed by the resolved FQN"
        )

        # The column-only sidecar (no `tags:` block) must NOT add a _TABLE_TAGS entry.
        table_tags_block = content.split("_COLUMN_TAGS")[0]
        assert "uc_region" not in table_tags_block, (
            "a column-only tags_file must not emit a _TABLE_TAGS entry"
        )

    def test_no_hook_when_no_tags(self):
        """A taggable table with no tags anywhere produces no hook (emission gated off)."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        hook_file = self.generated_dir / "uc_tagging_none" / "_uc_tagging_hook.py"
        assert not hook_file.exists(), (
            "Hook file should NOT exist when no tags are declared"
        )

    def test_no_hook_when_disabled(self):
        """With uc_tagging.enabled=false, no hook is emitted even for tagged tables."""
        lhp_yaml = self.project_root / "lhp.yaml"
        lhp_yaml.write_text(lhp_yaml.read_text() + "\nuc_tagging:\n  enabled: false\n")

        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        hook_file = self.generated_dir / "uc_tagging_core" / "_uc_tagging_hook.py"
        assert not hook_file.exists(), (
            "Hook file should NOT exist when uc_tagging is disabled"
        )


# Sandbox profile scoping the run to just the tags_file flowgroup's pipeline.
SANDBOX_TAGS_FILE_PROFILE = """sandbox:
  namespace: sbx
  pipelines:
    - uc_tagging_core
"""

# Team policy appended to the copy's lhp.yaml: the DEFAULT pattern stated
# explicitly, plus allowed_envs so dev is positively sandbox-enabled.
SANDBOX_TAGS_FILE_POLICY = """
sandbox:
  strategy: table
  table_pattern: "{namespace}_{table}"
  allowed_envs:
    - dev
"""


@pytest.mark.e2e
class TestUCTaggingTagsFileSandboxE2E:
    """Sandbox smoke for a write target that sources its tags from a ``tags_file``.

    ``uc_tag_orders.write_orders`` references ``tags_file: tags/uc_orders.yaml``,
    whose sidecar declares ``table: uc_orders``. Under ``--sandbox`` the write
    target is renamed (``uc_orders`` -> ``sbx_uc_orders``) before the tagging hook
    runs, so the sidecar's literal ``table:`` no longer equals the write target's
    (renamed) table name. The commit-time table cross-check is skipped under
    sandbox (Task 3), so generation must still succeed and the hook must carry the
    resolved tags keyed by the RENAMED FQN — proving the cross-check does not
    spuriously fail when the table has been namespaced.
    """

    __test__ = True

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

        lhp_dir = self.project_root / ".lhp"
        lhp_dir.mkdir()
        (lhp_dir / "profile.yaml").write_text(SANDBOX_TAGS_FILE_PROFILE)
        lhp_yaml = self.project_root / "lhp.yaml"
        lhp_yaml.write_text(lhp_yaml.read_text() + SANDBOX_TAGS_FILE_POLICY)

        yield
        os.chdir(self.original_cwd)

    def run_sandbox_generate(self) -> tuple:
        """Run `lhp generate --env dev --sandbox` with the project pipeline config."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--pipeline-config",
                "config/pipeline_config.yaml",
                "--sandbox",
            ],
        )
        return result.exit_code, result.output

    def test_tags_file_sandbox_skips_table_cross_check(self):
        """tags_file resolves under sandbox against the renamed table (no CFG-067)."""
        exit_code, output = self.run_sandbox_generate()
        assert exit_code == 0, f"Sandbox generation should succeed: {output}"

        hook_file = self.generated_dir / "uc_tagging_core" / "_uc_tagging_hook.py"
        assert hook_file.exists(), (
            "_uc_tagging_hook.py should be generated under sandbox"
        )
        content = hook_file.read_text()

        # Tags resolve, keyed by the sandbox-renamed FQN (uc_orders -> sbx_uc_orders).
        assert "acme_edw_dev.edw_bronze.sbx_uc_orders" in content, (
            "Tags must be keyed by the renamed FQN under sandbox"
        )
        assert '"team": "data-eng"' in content
        assert '"pii": ""' in content
        assert '"owner": ""' in content

        # The un-renamed table name must be gone from the tag keys — proof the
        # rename applied and the cross-check (which would have compared the
        # sidecar's 'uc_orders' against 'sbx_uc_orders') was skipped.
        assert "edw_bronze.uc_orders" not in content, (
            "The unrenamed uc_orders FQN must not appear once the table is namespaced"
        )
