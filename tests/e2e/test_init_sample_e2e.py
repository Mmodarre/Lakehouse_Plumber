"""E2E tests for ``lhp init <name> --sample`` (the TPC-H sample quickstart).

Covers the full sample-project workflow from the build spec
(``LOCAL/SAMPLE_PROJECT_SPEC.md``):

  1. ``lhp init sample_demo --sample`` scaffolds the §4 project tree into an
     empty directory (``.j2`` rendered + stripped, ``bundle/`` prefix
     stripped, everything else copied verbatim — no ``.j2`` / ``.tmpl``
     artifacts may leak into the scaffold).
  2. ``lhp validate --env dev`` passes on the freshly scaffolded project
     (spec §1 — the 3-step quickstart promises a runnable project with no
     extra setup).
  3. ``lhp generate --env dev`` produces one ``.py`` per flowgroup under
     ``generated/dev/{sample_ingest,sample_silver,sample_gold}/`` and one
     ``resources/lhp/<pipeline>.pipeline.yml`` per pipeline (§7), leaving
     the hand-authored ``resources/sample_job.yml`` untouched.

Assertions are STRUCTURAL and content-spot checks derived from the spec's
verified codegen facts (§11) — no hash baselines:

  - ``dp.create_auto_cdc_flow`` + ``apply_as_deletes`` in dim_customer
    (§4.4; ``templates/write/streaming_table.py.j2:45-72``).
  - ``dp.create_auto_cdc_from_snapshot_flow`` in dim_supplier
    (§4.5; ``templates/write/streaming_table.py.j2:113-126``).
  - ``@dp.materialized_view`` + ``def sales_by_nation()`` for the gold MV
    (§4.8; ``templates/write/materialized_view.py.j2``).
  - ``.option("csv.header", "true")`` in lineitem (§4.3;
    ``generators/load/cloudfiles.py:123-127`` prefixes ``format_options``
    keys with ``<format>.``).
"""

import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli

#: Spec §4 — every file the sample scaffold must contain, after the loader's
#: rewrite rules (``.j2`` suffix stripped on render, ``bundle/`` prefix
#: stripped on write). ``config/`` contents are "as in plain init" (§4) and
#: deliberately not pinned here.
EXPECTED_SCAFFOLD_FILES = [
    "lhp.yaml",
    "README.md",
    "databricks.yml",
    "substitutions/dev.yaml",
    "substitutions/prod.yaml",
    "presets/bronze_layer.yaml",
    "presets/silver_quality.yaml",
    "templates/cloudfiles_ingest.yaml",
    "pipelines/01_ingest/nation_region.yaml",
    "pipelines/01_ingest/orders_ingest.yaml",
    "pipelines/01_ingest/lineitem_ingest.yaml",
    "pipelines/02_silver/dim_customer.yaml",
    "pipelines/02_silver/dim_supplier.yaml",
    "pipelines/02_silver/orders_clean.yaml",
    "pipelines/03_gold/sales_by_nation.yaml",
    "sql/sales_by_nation.sql",
    "schemas/orders_hints.yaml",
    "schema_transforms/orders_typed.yaml",
    "expectations/orders.yaml",
    "transforms/normalize_tz.py",
    "functions/supplier_snapshot.py",
    "notebooks/data_prep.py",
    "resources/sample_job.yml",
    "examples_optional/jdbc_with_secret.yaml.txt",
]

#: Spec §3/§4/§7 — pipeline name → flowgroup .py files generated per pipeline.
EXPECTED_GENERATED_LAYOUT = {
    "sample_ingest": ["nation_region.py", "orders_ingest.py", "lineitem_ingest.py"],
    "sample_silver": ["dim_customer.py", "dim_supplier.py", "orders_clean.py"],
    "sample_gold": ["sales_by_nation.py"],
}


@pytest.mark.e2e
class TestInitSampleE2E:
    """E2E coverage for the ``--sample`` quickstart scaffold."""

    @pytest.fixture(autouse=True)
    def setup_empty_project_dir(self, isolated_project):
        """Create an empty target dir for the scaffold and chdir into it.

        ``lhp init`` scaffolds into the *current working directory* (the NAME
        argument is only baked into template substitutions), so each test gets
        a fresh empty ``sample_demo/`` under the isolated temp dir.
        """
        self.project_root = isolated_project / "sample_demo"
        self.project_root.mkdir()

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"

        yield
        os.chdir(self.original_cwd)

    def run_init_sample(self) -> tuple:
        """Run ``lhp init sample_demo --sample``. Returns (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["init", "sample_demo", "--sample"])
        return result.exit_code, result.output

    def _run_with_optional_pc(self, command: str) -> tuple:
        """Run ``lhp <command> --env dev`` the way the spec quickstart does.

        House-style mirror of ``run_bundle_sync`` / ``run_validate``: when the
        project carries a default ``config/pipeline_config.yaml``, inject
        ``--pipeline-config`` so bundle preflight does not shadow the
        assertion under test. The spec's 3-step quickstart (§1) is the bare
        ``lhp generate --env dev`` — so with no default config present the
        command runs bare, exactly as a quickstart user would type it.
        """
        runner = CliRunner()
        argv = [command, "--env", "dev"]
        default_pc = self.project_root / "config" / "pipeline_config.yaml"
        if default_pc.exists():
            argv.extend(["--pipeline-config", "config/pipeline_config.yaml"])
        result = runner.invoke(cli, argv)
        return result.exit_code, result.output

    def run_validate(self) -> tuple:
        return self._run_with_optional_pc("validate")

    def run_generate(self) -> tuple:
        return self._run_with_optional_pc("generate")

    def _init_or_fail(self):
        """Init precondition shared by the validate/generate tests."""
        exit_code, output = self.run_init_sample()
        assert exit_code == 0, f"lhp init --sample failed:\n{output[-2000:]}"

    def test_init_sample_scaffolds_spec_tree(self):
        """``lhp init sample_demo --sample`` lays down the §4 project tree.

        Every file in the spec inventory must exist; templates must be
        rendered (no ``.j2`` left behind) and nothing may be scaffolded with
        a ``.tmpl`` suffix; ``lhp.yaml`` / ``databricks.yml`` must carry the
        rendered project name.
        """
        exit_code, output = self.run_init_sample()
        assert exit_code == 0, f"lhp init --sample failed:\n{output[-2000:]}"

        missing = [
            rel
            for rel in EXPECTED_SCAFFOLD_FILES
            if not (self.project_root / rel).is_file()
        ]
        assert missing == [], f"Sample scaffold is missing spec §4 files: {missing}"

        # Loader contract: .j2 rendered+stripped; nothing ships as .tmpl.
        leaked_j2 = sorted(self.project_root.rglob("*.j2"))
        assert leaked_j2 == [], f"Unrendered .j2 files leaked: {leaked_j2}"
        leaked_tmpl = sorted(self.project_root.rglob("*.tmpl"))
        assert leaked_tmpl == [], f".tmpl files leaked into scaffold: {leaked_tmpl}"

        # lhp.yaml rendered with the project name + §4.1 op-metadata block.
        lhp_yaml = (self.project_root / "lhp.yaml").read_text()
        assert "name: sample_demo" in lhp_yaml, (
            f"lhp.yaml must carry the rendered project name. Got:\n{lhp_yaml[:400]}"
        )
        assert "operational_metadata:" in lhp_yaml, (
            "lhp.yaml must define the §4.1 operational_metadata block. "
            f"Got:\n{lhp_yaml[:400]}"
        )

        # databricks.yml: bundle name rendered + the §4.10 catalog variable.
        databricks_yml = (self.project_root / "databricks.yml").read_text()
        assert "sample_demo" in databricks_yml, (
            "databricks.yml must carry the rendered bundle name. "
            f"Got:\n{databricks_yml[:400]}"
        )
        assert "catalog" in databricks_yml, (
            "databricks.yml must define the §4.10 `catalog` bundle variable. "
            f"Got:\n{databricks_yml[:400]}"
        )

    def test_init_sample_project_validates(self):
        """Spec §1 step 3: the scaffolded project passes ``lhp validate --env dev``
        with zero extra setup."""
        self._init_or_fail()

        exit_code, output = self.run_validate()
        assert exit_code == 0, (
            f"lhp validate --env dev must pass on the fresh sample project, "
            f"got exit {exit_code}:\n{output[-3000:]}"
        )

    def test_init_sample_generates_pipelines(self):
        """``lhp generate --env dev`` emits the §3/§7 output structure.

        One ``.py`` per flowgroup under ``generated/dev/<pipeline>/``,
        exactly the three spec pipelines, one bundle resource YAML per
        pipeline under ``resources/lhp/``, and the hand-authored
        ``resources/sample_job.yml`` untouched (§7 — LHP wipes only
        ``resources/lhp/``).
        """
        self._init_or_fail()

        exit_code, output = self.run_generate()
        assert exit_code == 0, (
            f"lhp generate --env dev must succeed on the fresh sample project, "
            f"got exit {exit_code}:\n{output[-3000:]}"
        )

        # generated/dev/: exactly the three spec pipelines.
        assert self.generated_dir.is_dir(), "generated/dev/ must exist"
        pipeline_dirs = sorted(
            p.name for p in self.generated_dir.iterdir() if p.is_dir()
        )
        assert pipeline_dirs == sorted(EXPECTED_GENERATED_LAYOUT), (
            f"generated/dev/ must contain exactly the three sample pipelines, "
            f"got: {pipeline_dirs}"
        )

        # One .py per flowgroup.
        missing_py = [
            f"{pipeline}/{py_file}"
            for pipeline, py_files in EXPECTED_GENERATED_LAYOUT.items()
            for py_file in py_files
            if not (self.generated_dir / pipeline / py_file).is_file()
        ]
        assert missing_py == [], f"Missing generated flowgroup files: {missing_py}"

        # Bundle resources: one pipeline.yml per pipeline (§7).
        missing_resources = [
            f"{pipeline}.pipeline.yml"
            for pipeline in EXPECTED_GENERATED_LAYOUT
            if not (self.resources_dir / f"{pipeline}.pipeline.yml").is_file()
        ]
        assert missing_resources == [], (
            f"Missing bundle resource files under resources/lhp/: {missing_resources}"
        )

        # Hand-authored job survives the resources/lhp/ wipe (§7).
        assert (self.project_root / "resources" / "sample_job.yml").is_file(), (
            "resources/sample_job.yml must survive generate (LHP only wipes "
            "resources/lhp/)"
        )

        # Spot content: §11-verified codegen facts.
        dim_customer = (
            self.generated_dir / "sample_silver" / "dim_customer.py"
        ).read_text()
        assert "dp.create_auto_cdc_flow(" in dim_customer, (
            "dim_customer must use AUTO CDC "
            "(§4.4; templates/write/streaming_table.py.j2:45)"
        )
        assert "apply_as_deletes=\"_change_type = 'delete'\"" in dim_customer, (
            "dim_customer AUTO CDC must route CDF deletes via apply_as_deletes "
            "(§4.4; streaming_table.py.j2:68)"
        )
        assert "stored_as_scd_type=2" in dim_customer, (
            "dim_customer must be SCD type 2 (§4.4; streaming_table.py.j2:57)"
        )

        dim_supplier = (
            self.generated_dir / "sample_silver" / "dim_supplier.py"
        ).read_text()
        assert "dp.create_auto_cdc_from_snapshot_flow(" in dim_supplier, (
            "dim_supplier must use snapshot AUTO CDC "
            "(§4.5; templates/write/streaming_table.py.j2:114)"
        )

        sales_by_nation = (
            self.generated_dir / "sample_gold" / "sales_by_nation.py"
        ).read_text()
        assert "@dp.materialized_view(" in sales_by_nation, (
            "sales_by_nation must be a materialized view "
            "(§4.8; templates/write/materialized_view.py.j2:1)"
        )
        assert "def sales_by_nation():" in sales_by_nation, (
            "the MV function must be named after the target table "
            "(§4.8; materialized_view.py.j2:45)"
        )

        lineitem = (
            self.generated_dir / "sample_ingest" / "lineitem_ingest.py"
        ).read_text()
        assert '.option("csv.header", "true")' in lineitem, (
            "lineitem CSV ingest must set the header reader option — "
            "format_options keys are prefixed with '<format>.' "
            "(§4.3; generators/load/cloudfiles.py:123-127)"
        )
