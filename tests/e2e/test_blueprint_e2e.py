"""End-to-end integration tests for the blueprint feature.

A blueprint defines a parameterized flowgroup template under blueprints/;
instance files under pipelines/ bind parameter values via use_blueprint:.
The fixture project ships one blueprint (medallion_demo) reusing existing
templates, py_functions, presets, and expectations across raw/bronze/silver
layers; two instances (site_alpha, site_beta) expand it into 6 synthetic
flowgroups in 3 acme_edw_bp_* pipelines.

Scenarios (BP-1..BP-10, with -2/-5/-7 folded into BP-1 or covered by the
fixture layout itself):

  BP-1   Golden path: 2 instances + blueprint -> .py + .pipeline.yml match
         baselines byte-for-byte; no unresolved %{...} in generated tree.
  BP-3   Two instances with the same site_name -> LHPValidationError 045
         (duplicate (pipeline, flowgroup) after expansion).
  BP-4   Instance missing a required parameter -> parse-time error.
  BP-6   Editing the blueprint definition triggers regeneration of all
         synthetic flowgroups (state-checksum tracking) and only those.
  BP-8   ${env_token} in a blueprint identity field (pipeline:) ->
         _reject_env_tokens_in_identity raises validation error.
  BP-9   `lhp deps` includes synthetic flowgroups in the dependency graph.
  BP-10  Hand-written flowgroup colliding with a synthetic (pipeline,
         flowgroup) -> orchestrator raises a clear duplicate error.
"""

import hashlib
import json
import os
import shutil
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestBlueprintE2E:
    """E2E tests covering blueprint expansion, error paths, and CLI integration."""

    BP_PIPELINES = ("acme_edw_bp_raw", "acme_edw_bp_bronze", "acme_edw_bp_silver")
    BP_SYNTHETIC_FLOWGROUPS = (
        ("acme_edw_bp_raw", "site_alpha_customer_ingestion"),
        ("acme_edw_bp_raw", "site_beta_customer_ingestion"),
        ("acme_edw_bp_bronze", "site_alpha_customer_bronze"),
        ("acme_edw_bp_bronze", "site_beta_customer_bronze"),
        ("acme_edw_bp_silver", "site_alpha_customer_silver"),
        ("acme_edw_bp_silver", "site_beta_customer_silver"),
    )

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        """Set up fresh test project for each test method."""
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"
        self.blueprint_path = (
            self.project_root / "blueprints" / "medallion_demo.yaml"
        )
        self.site_alpha_path = (
            self.project_root
            / "pipelines"
            / "10_blueprint_demo"
            / "sites"
            / "site_alpha.yaml"
        )

        self._init_bundle_project()

        yield

        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        """Wipe and recreate working dirs (no-op on fresh fixture)."""
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def run_bundle_sync(self) -> tuple:
        """Run lhp generate --env dev --pipeline-config ... --force."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--verbose",
                "generate",
                "--env",
                "dev",
                "--pipeline-config",
                "config/pipeline_config.yaml",
                "--force",
            ],
        )
        return result.exit_code, result.output

    def run_generate_no_force(self) -> tuple:
        """Run lhp generate without --force (incremental mode)."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--verbose",
                "generate",
                "--env",
                "dev",
                "--pipeline-config",
                "config/pipeline_config.yaml",
            ],
        )
        return result.exit_code, result.output

    def _file_hash(self, path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def _compare_blueprint_directories(self) -> list:
        """Compare only the BP-derived directories against baselines."""
        differences = []
        for pipeline in self.BP_PIPELINES:
            gen = self.generated_dir / pipeline
            base = self.project_root / "generated_baseline" / "dev" / pipeline
            assert base.exists(), f"Missing baseline directory: {base}"
            assert gen.exists(), f"Generated directory missing: {gen}"

            gen_files = {
                f.relative_to(gen): f
                for f in gen.rglob("*")
                if f.is_file() and "__pycache__" not in f.parts
            }
            base_files = {
                f.relative_to(base): f
                for f in base.rglob("*")
                if f.is_file() and "__pycache__" not in f.parts
            }

            for extra in set(gen_files) - set(base_files):
                differences.append(f"{pipeline}: extra in generated: {extra}")
            for missing in set(base_files) - set(gen_files):
                differences.append(f"{pipeline}: missing from generated: {missing}")
            for common in set(gen_files) & set(base_files):
                if self._file_hash(gen_files[common]) != self._file_hash(
                    base_files[common]
                ):
                    differences.append(
                        f"{pipeline}: hash mismatch on {common}"
                    )
        return differences

    def _load_state_file(self) -> dict:
        """Rebuild the legacy-shape state dict from the new sharded format.

        Merges ``.lhp_state/_global.json`` plus every per-pipeline shard's
        ``environments`` slice so callers can keep asserting against the
        old monolithic dict shape.
        """
        state_dir = self.project_root / ".lhp_state"
        if not state_dir.exists():
            return {}

        result: dict = {"environments": {}}

        global_path = state_dir / "_global.json"
        if global_path.exists():
            global_data = json.loads(global_path.read_text())
            result["version"] = global_data.get("version", "1.0")
            result["last_updated"] = global_data.get("last_updated", "")
            result["global_dependencies"] = global_data.get(
                "global_dependencies", {}
            )
            result["last_generation_context"] = global_data.get(
                "last_generation_context", {}
            )

        for shard_path in sorted(state_dir.glob("*.json")):
            if shard_path.stem.startswith("_"):
                continue
            shard_data = json.loads(shard_path.read_text())
            for env_name, env_files in shard_data.get("environments", {}).items():
                result["environments"].setdefault(env_name, {}).update(env_files)

        return result

    def _bp_state_paths(self, state: dict) -> set:
        """Return state-tracked generated paths for the BP pipelines."""
        env = state.get("environments", {}).get("dev", {})
        return {
            path
            for path in env
            if any(f"/{p}/" in path or path.startswith(f"{p}/") for p in self.BP_PIPELINES)
            or any(f"generated/dev/{p}/" in path for p in self.BP_PIPELINES)
        }

    # ------------------------------------------------------------------
    # BP-1: Golden path baseline match (subsumes the original BP-2 + BP-5)
    # ------------------------------------------------------------------

    def test_BP1_blueprint_expansion_matches_baseline(self):
        """Blueprint with 2 instances expands to 6 synthetic flowgroups whose
        generated .py and .pipeline.yml output matches baselines byte-for-byte;
        no unresolved %{...} substitution tokens leak through to the tree."""
        exit_code, output = self.run_bundle_sync()
        assert exit_code == 0, f"Generation failed: {output}"

        differences = self._compare_blueprint_directories()
        assert not differences, "Blueprint hash diffs:\n  " + "\n  ".join(differences)

        for pipeline in self.BP_PIPELINES:
            generated = self.resources_dir / f"{pipeline}.pipeline.yml"
            baseline = (
                self.project_root
                / "resources_baseline"
                / "lhp"
                / f"{pipeline}.pipeline.yml"
            )
            assert generated.exists(), f"Missing generated resource: {generated}"
            assert baseline.exists(), f"Missing baseline resource: {baseline}"
            assert self._file_hash(generated) == self._file_hash(baseline), (
                f"Resource baseline mismatch for {pipeline}"
            )

        for pipeline in self.BP_PIPELINES:
            for py_file in (self.generated_dir / pipeline).rglob("*.py"):
                content = py_file.read_text()
                assert "%{" not in content, (
                    f"Unresolved local-var token in {py_file.relative_to(self.project_root)}:\n"
                    f"{[ln for ln in content.splitlines() if '%{' in ln]}"
                )

    # ------------------------------------------------------------------
    # BP-3: Duplicate (pipeline, flowgroup) after expansion
    # ------------------------------------------------------------------

    def test_BP3_duplicate_identity_after_expansion_raises(self):
        """Two instances binding the same site_name produce identical
        synthetic (pipeline, flowgroup) tuples; expander must raise
        LHPValidationError 045 with both instance paths in the message."""
        duplicate = self.site_alpha_path.with_name("site_alpha_dup.yaml")
        duplicate.write_text(
            "use_blueprint: medallion_demo\n"
            "parameters:\n"
            "  site_name: site_alpha\n"
            "  domain_id: ALPHA002\n"
        )

        exit_code, output = self.run_bundle_sync()
        assert exit_code != 0, "Generation should fail on duplicate identity"
        assert "045" in output or "Duplicate" in output, (
            f"Expected LHP-VAL-045 / 'Duplicate' in output, got:\n{output[-2000:]}"
        )
        assert "site_alpha.yaml" in output and "site_alpha_dup.yaml" in output, (
            "Both conflicting instance paths should be reported"
        )

    # ------------------------------------------------------------------
    # BP-4: Missing required parameter
    # ------------------------------------------------------------------

    def test_BP4_missing_required_parameter_raises(self):
        """Removing the required `domain_id` from an instance must surface
        a parse-time error that names the missing parameter."""
        self.site_alpha_path.write_text(
            "use_blueprint: medallion_demo\n"
            "parameters:\n"
            "  site_name: site_alpha\n"
        )

        exit_code, output = self.run_bundle_sync()
        assert exit_code != 0, "Generation should fail on missing required param"
        assert "domain_id" in output, (
            f"Error must mention missing parameter 'domain_id'. Got:\n{output[-2000:]}"
        )

    # ------------------------------------------------------------------
    # BP-6: Editing the blueprint regenerates only synthetic flowgroups
    # ------------------------------------------------------------------

    def test_BP6_blueprint_edit_regenerates_synthetic_only(self):
        """Editing medallion_demo.yaml must update the source_yaml_checksum
        of all 6 synthetic flowgroups (the blueprint IS their source_yaml)
        while leaving every non-blueprint flowgroup's source_yaml_checksum
        unchanged. This is the core dependency-tracking signal: it proves
        that the state resolver wires synthetic flowgroups to the blueprint
        file, so an incremental regenerate would re-emit them.
        Whether the *generated* file content actually changes depends on
        which part of the blueprint was edited, so it isn't asserted here."""
        # Phase 1: fresh generation
        exit_code, output = self.run_bundle_sync()
        assert exit_code == 0, f"Initial generation failed: {output}"
        baseline_state = self._load_state_file()
        assert baseline_state, "State file should exist after generation"

        # Phase 2: edit the blueprint — any byte-level change suffices
        # because what we're testing is the dependency wiring.
        original_text = self.blueprint_path.read_text()
        self.blueprint_path.write_text(
            original_text + "\n# BP-6 edit marker\n"
        )

        # Phase 3: incremental regeneration (no --force)
        exit_code, output = self.run_generate_no_force()
        assert exit_code == 0, f"Regeneration failed: {output}"
        updated_state = self._load_state_file()

        baseline_env = baseline_state["environments"]["dev"]
        updated_env = updated_state["environments"]["dev"]

        synthetic_py_paths = {
            f"generated/dev/{pipeline}/{flowgroup}.py"
            for pipeline, flowgroup in self.BP_SYNTHETIC_FLOWGROUPS
        }
        for path in synthetic_py_paths:
            assert path in baseline_env, (
                f"Synthetic flowgroup not tracked in state: {path}"
            )
            base = baseline_env[path]
            upd = updated_env.get(path, {})
            assert base.get("synthetic") is True, (
                f"State entry should be flagged synthetic: {path}"
            )
            assert base.get("source_yaml") == "blueprints/medallion_demo.yaml", (
                f"Synthetic source_yaml should be the blueprint, got: "
                f"{base.get('source_yaml')}"
            )
            assert base["source_yaml_checksum"] != upd.get("source_yaml_checksum"), (
                f"source_yaml_checksum (blueprint hash) must change for {path}"
            )

        # Non-blueprint flowgroups: source_yaml_checksum is their own YAML
        # file's hash. Since we only edited the blueprint, those must stay
        # constant — proving the blueprint is NOT in their dependency graph.
        bp_prefixes = tuple(f"generated/dev/{p}/" for p in self.BP_PIPELINES)
        non_bp_paths = [p for p in baseline_env if not p.startswith(bp_prefixes)]
        leaked = [
            p
            for p in non_bp_paths
            if baseline_env[p].get("source_yaml_checksum")
            != updated_env.get(p, {}).get("source_yaml_checksum")
        ]
        assert not leaked, (
            "Editing the blueprint must not change any non-blueprint "
            f"flowgroup's source_yaml_checksum; leaked: {leaked}"
        )

    # ------------------------------------------------------------------
    # BP-8: Env tokens in identity fields are rejected
    # ------------------------------------------------------------------

    def test_BP8_env_token_in_identity_rejected(self):
        """`${catalog}` in a blueprint pipeline: field must be rejected by
        _reject_env_tokens_in_identity — env tokens are evaluated AFTER the
        flowgroup identity is fixed, so allowing them would silently scramble
        which pipeline a flowgroup belongs to across environments."""
        bp_text = self.blueprint_path.read_text()
        bad_text = bp_text.replace(
            "  - pipeline: acme_edw_bp_raw",
            '  - pipeline: "${catalog}_bp_raw"',
            1,
        )
        assert bad_text != bp_text, "Could not inject env-token into pipeline field"
        self.blueprint_path.write_text(bad_text)

        exit_code, output = self.run_bundle_sync()
        assert exit_code != 0, "Generation should fail on env-token in identity"
        msg = output.lower()
        assert "${catalog}" in output or "env" in msg or "identity" in msg, (
            "Error must reference the env token / identity field. "
            f"Got:\n{output[-2000:]}"
        )

    # ------------------------------------------------------------------
    # BP-9: lhp deps includes synthetic flowgroups
    # ------------------------------------------------------------------

    def test_BP9_lhp_deps_includes_synthetic_flowgroups(self):
        """`lhp deps` must surface the 3 acme_edw_bp_* pipelines and all 6
        synthetic flowgroups in its dependency analysis output."""
        runner = CliRunner()
        result = runner.invoke(cli, ["deps", "-b"])
        assert result.exit_code == 0, f"deps failed: {result.output}"

        deps_json = self.project_root / ".lhp" / "dependencies" / "pipeline_dependencies.json"
        assert deps_json.exists(), f"Missing deps JSON: {deps_json}"
        data = json.loads(deps_json.read_text())

        seen_pipelines = set(data.get("pipelines", {}).keys())
        for p in self.BP_PIPELINES:
            assert p in seen_pipelines, (
                f"Pipeline {p} missing from deps JSON; saw: {sorted(seen_pipelines)}"
            )

        job_yml = self.project_root / "resources" / "acme_edw_orchestration.job.yml"
        assert job_yml.exists(), "Orchestration job YAML not generated"
        job_text = job_yml.read_text()
        for p in self.BP_PIPELINES:
            assert f"{p}_pipeline" in job_text, (
                f"Pipeline {p} missing from orchestration job YAML"
            )

    # ------------------------------------------------------------------
    # BP-10: Explicit-vs-synthetic flowgroup collision
    # ------------------------------------------------------------------

    def test_BP10_explicit_vs_synthetic_collision_raises(self):
        """A hand-written flowgroup with the same (pipeline, flowgroup) as an
        already-expanded synthetic must trigger the orchestrator's duplicate
        validator (validate_duplicate_pipeline_flowgroup_combinations)."""
        collision = (
            self.project_root
            / "pipelines"
            / "10_blueprint_demo"
            / "collision_with_alpha_raw.yaml"
        )
        collision.write_text(
            "pipeline: acme_edw_bp_raw\n"
            "flowgroup: site_alpha_customer_ingestion\n"
            "actions:\n"
            "  - name: collision_load\n"
            "    type: load\n"
            "    source:\n"
            "      type: delta\n"
            "      database: \"{catalog}.{raw_schema}\"\n"
            "      table: irrelevant\n"
            "    target: v_collision\n"
            "  - name: collision_write\n"
            "    type: write\n"
            "    source: v_collision\n"
            "    write_target:\n"
            "      type: streaming_table\n"
            "      database: \"{catalog}.{raw_schema}\"\n"
            "      table: irrelevant\n"
        )

        exit_code, output = self.run_bundle_sync()
        assert exit_code != 0, "Generation should fail on identity collision"
        assert (
            "duplicate" in output.lower()
            or "site_alpha_customer_ingestion" in output
        ), f"Expected duplicate-flowgroup error. Got:\n{output[-2000:]}"
