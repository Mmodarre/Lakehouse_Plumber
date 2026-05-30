"""Test cases for pipeline field determining output directory structure.

These tests verify that:
1. Output directories are named after the pipeline field (not directory name)
2. Multiple flowgroups with same pipeline field generate to the same output directory
3. CLI --pipeline flag finds flowgroups by pipeline field across directories
4. State manager tracks files by pipeline field
5. Orchestrator discovers flowgroups by pipeline field
"""

import tempfile
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from lhp.api import collect_response
from lhp.api.facade import LakehousePlumberApplicationFacade
from lhp.cli.main import cli
from lhp.core.coordination.layers import build_facade_orchestrator


class TestPipelineFieldOutputStructure:
    """Test pipeline field determines output directory structure."""

    @pytest.fixture
    def runner(self):
        """Create CLI runner."""
        return CliRunner()

    @pytest.fixture
    def project_with_pipeline_field_structure(self):
        """Create a project with flowgroups that have same pipeline field in different directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)

            # Create project structure
            directories = [
                "presets",
                "templates",
                "pipelines",
                "substitutions",
                "schemas",
                "expectations",
                "generated",
            ]

            for dir_name in directories:
                (project_root / dir_name).mkdir(parents=True)

            # Create project config
            (project_root / "lhp.yaml").write_text("""
name: test_pipeline_field_project
version: "1.0"
description: "Test project for pipeline field output structure"
""")

            # Create substitution files
            (project_root / "substitutions" / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  bronze_schema: bronze
  silver_schema: silver
""")

            # Create pipeline directories with different names
            (project_root / "pipelines" / "01_raw_ingestion" / "csv_files").mkdir(
                parents=True
            )
            (project_root / "pipelines" / "01_raw_ingestion" / "json_files").mkdir(
                parents=True
            )
            (project_root / "pipelines" / "different_folder_name").mkdir(parents=True)
            (project_root / "pipelines" / "02_silver_layer").mkdir(parents=True)

            # Create flowgroups with same pipeline field in different directories
            # Both of these should generate to generated/raw_ingestions/ (not 01_raw_ingestion/)
            customer_ingestion = {
                "pipeline": "raw_ingestions",  # This should determine output directory
                "flowgroup": "customer_ingestion",
                "actions": [
                    {
                        "name": "load_customers",
                        "type": "load",
                        "target": "v_customers",
                        "source": {
                            "type": "cloudfiles",
                            "path": "/mnt/data/customers",
                            "format": "csv",
                        },
                    },
                    {
                        "name": "write_customers",
                        "type": "write",
                        "source": "v_customers",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "${catalog}",
                            "schema": "${bronze_schema}",
                            "table": "customers",
                            "create_table": True,
                        },
                    },
                ],
            }

            orders_ingestion = {
                "pipeline": "raw_ingestions",  # Same pipeline field
                "flowgroup": "orders_ingestion",
                "actions": [
                    {
                        "name": "load_orders",
                        "type": "load",
                        "target": "v_orders",
                        "source": {
                            "type": "cloudfiles",
                            "path": "/mnt/data/orders",
                            "format": "json",
                        },
                    },
                    {
                        "name": "write_orders",
                        "type": "write",
                        "source": "v_orders",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "${catalog}",
                            "schema": "${bronze_schema}",
                            "table": "orders",
                            "create_table": True,
                        },
                    },
                ],
            }

            # This should also go to generated/raw_ingestions/ despite being in different_folder_name
            lineitem_ingestion = {
                "pipeline": "raw_ingestions",  # Same pipeline field again
                "flowgroup": "lineitem_ingestion",
                "actions": [
                    {
                        "name": "load_lineitem",
                        "type": "load",
                        "target": "v_lineitem",
                        "source": {
                            "type": "cloudfiles",
                            "path": "/mnt/data/lineitem",
                            "format": "parquet",
                        },
                    },
                    {
                        "name": "write_lineitem",
                        "type": "write",
                        "source": "v_lineitem",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "${catalog}",
                            "schema": "${bronze_schema}",
                            "table": "lineitem",
                            "create_table": True,
                        },
                    },
                ],
            }

            # This should generate to generated/silver_transforms/
            customer_transforms = {
                "pipeline": "silver_transforms",  # Different pipeline field
                "flowgroup": "customer_transforms",
                "actions": [
                    {
                        "name": "load_customers_bronze",
                        "type": "load",
                        "target": "v_customers_bronze",
                        "source": {
                            "type": "delta",
                            "catalog": "${catalog}",
                            "schema": "${bronze_schema}",
                            "table": "customers",
                        },
                    },
                    {
                        "name": "transform_customers",
                        "type": "transform",
                        "transform_type": "sql",
                        "source": "v_customers_bronze",
                        "target": "v_customers_silver",
                        "sql": "SELECT * FROM v_customers_bronze WHERE active = true",
                    },
                    {
                        "name": "write_customers_silver",
                        "type": "write",
                        "source": "v_customers_silver",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "${catalog}",
                            "schema": "${silver_schema}",
                            "table": "customers",
                            "create_table": True,
                        },
                    },
                ],
            }

            # Save flowgroups to different directories
            customer_file = (
                project_root
                / "pipelines"
                / "01_raw_ingestion"
                / "csv_files"
                / "customer_ingestion.yaml"
            )
            orders_file = (
                project_root
                / "pipelines"
                / "01_raw_ingestion"
                / "json_files"
                / "orders_ingestion.yaml"
            )
            lineitem_file = (
                project_root
                / "pipelines"
                / "different_folder_name"
                / "lineitem_ingestion.yaml"
            )
            transforms_file = (
                project_root
                / "pipelines"
                / "02_silver_layer"
                / "customer_transforms.yaml"
            )

            with open(customer_file, "w") as f:
                yaml.dump(customer_ingestion, f)

            with open(orders_file, "w") as f:
                yaml.dump(orders_ingestion, f)

            with open(lineitem_file, "w") as f:
                yaml.dump(lineitem_ingestion, f)

            with open(transforms_file, "w") as f:
                yaml.dump(customer_transforms, f)

            yield project_root

    def test_output_directory_uses_pipeline_field_not_directory_name(
        self, project_with_pipeline_field_structure
    ):
        """Test that output directories are named after pipeline field, not directory name."""
        project_root = project_with_pipeline_field_structure

        # Generate all pipelines
        facade = LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )

        # Discover all flowgroups and organize by pipeline field. This is
        # driven through a SINGLE ``generate_pipelines`` call that passes BOTH
        # pipeline fields via ``pipeline_fields`` — mirroring how the CLI
        # invokes generate once per run with all discovered fields. Each
        # ``generate_pipelines`` call now does a full-env wipe of
        # ``generated/dev`` up front, so generating both pipeline fields in one
        # call is required — two separate calls sharing one ``output_dir`` would
        # self-clobber (the second wipe would delete the first call's output).
        output_dir = project_root / "generated" / "dev"
        batch_response = collect_response(
            facade.generation.generate_pipelines(
                pipeline_fields=["raw_ingestions", "silver_transforms"],
                env="dev",
                output_dir=output_dir,
            )
        )

        pipeline_responses = batch_response.pipeline_responses
        generated_files_raw = pipeline_responses["raw_ingestions"].generated_filenames
        generated_files_silver = pipeline_responses[
            "silver_transforms"
        ].generated_filenames

        # Should have 3 files for raw_ingestions pipeline (customer, orders, lineitem)
        assert len(generated_files_raw) == 3
        assert "customer_ingestion.py" in generated_files_raw
        assert "orders_ingestion.py" in generated_files_raw
        assert "lineitem_ingestion.py" in generated_files_raw

        # Should have 1 file for silver_transforms pipeline
        assert len(generated_files_silver) == 1
        assert "customer_transforms.py" in generated_files_silver

        # Verify files are in directories named after pipeline field, not directory name
        raw_ingestions_dir = project_root / "generated" / "dev" / "raw_ingestions"
        silver_transforms_dir = project_root / "generated" / "dev" / "silver_transforms"

        assert (raw_ingestions_dir / "customer_ingestion.py").exists()
        assert (raw_ingestions_dir / "orders_ingestion.py").exists()
        assert (raw_ingestions_dir / "lineitem_ingestion.py").exists()
        assert (silver_transforms_dir / "customer_transforms.py").exists()

        # Should NOT create directories named after folder names
        assert not (project_root / "generated" / "dev" / "01_raw_ingestion").exists()
        assert not (
            project_root / "generated" / "dev" / "different_folder_name"
        ).exists()
        assert not (project_root / "generated" / "dev" / "02_silver_layer").exists()

    def test_cli_generate_all_finds_by_pipeline_field(
        self, runner, project_with_pipeline_field_structure
    ):
        """Test that CLI generate (without --pipeline) finds all flowgroups and organizes by pipeline field."""
        with runner.isolated_filesystem():
            import os

            os.chdir(str(project_with_pipeline_field_structure))

            # Generate all pipelines
            result = runner.invoke(cli, ["generate", "--env", "dev"])

            assert result.exit_code == 0

            # Should organize by pipeline field, not directory name
            generated_dir = project_with_pipeline_field_structure / "generated" / "dev"

            # Should have directories named after pipeline fields
            raw_ingestions_dir = generated_dir / "raw_ingestions"
            silver_transforms_dir = generated_dir / "silver_transforms"

            assert raw_ingestions_dir.exists()
            assert silver_transforms_dir.exists()

            # raw_ingestions should have 3 files from different source directories
            assert (raw_ingestions_dir / "customer_ingestion.py").exists()
            assert (raw_ingestions_dir / "orders_ingestion.py").exists()
            assert (raw_ingestions_dir / "lineitem_ingestion.py").exists()

            # silver_transforms should have 1 file
            assert (silver_transforms_dir / "customer_transforms.py").exists()

            # Should NOT have directories named after source folder names
            assert not (generated_dir / "01_raw_ingestion").exists()
            assert not (generated_dir / "different_folder_name").exists()
            assert not (generated_dir / "02_silver_layer").exists()

    def test_cli_pipeline_flag_uses_pipeline_field(
        self, runner, project_with_pipeline_field_structure
    ):
        """Test that CLI --pipeline flag finds flowgroups by pipeline field, not directory name."""
        import os

        old_cwd = os.getcwd()
        try:
            os.chdir(str(project_with_pipeline_field_structure))

            # Generate using pipeline field. ``--show-all`` opts into the
            # full per-pipeline summary table; the failures-only default
            # would suppress the table on a clean run, but this test
            # asserts that the pipeline name appears in stdout.
            result = runner.invoke(
                cli,
                [
                    "generate",
                    "--env",
                    "dev",
                    "--pipeline",
                    "raw_ingestions",
                    "--show-all",
                ],
            )

            assert result.exit_code == 0
            assert "raw_ingestions" in result.output

            # Should find all 3 flowgroups with pipeline: raw_ingestions
            # even though they're in different directories
            generated_dir = (
                project_with_pipeline_field_structure
                / "generated"
                / "dev"
                / "raw_ingestions"
            )

            assert (generated_dir / "customer_ingestion.py").exists()
            assert (generated_dir / "orders_ingestion.py").exists()
            assert (generated_dir / "lineitem_ingestion.py").exists()

            # Should NOT generate silver_transforms
            silver_dir = (
                project_with_pipeline_field_structure
                / "generated"
                / "dev"
                / "silver_transforms"
            )
            assert not silver_dir.exists()
        finally:
            os.chdir(old_cwd)

    def test_cli_pipeline_flag_different_pipeline_field(
        self, runner, project_with_pipeline_field_structure
    ):
        """Test CLI --pipeline flag with different pipeline field."""
        with runner.isolated_filesystem():
            import os

            os.chdir(str(project_with_pipeline_field_structure))

            # Generate using different pipeline field. ``--show-all``
            # opts into the full per-pipeline summary table; the
            # failures-only default would suppress the table on a clean
            # run, but this test asserts that the pipeline name appears
            # in stdout.
            result = runner.invoke(
                cli,
                [
                    "generate",
                    "--env",
                    "dev",
                    "--pipeline",
                    "silver_transforms",
                    "--show-all",
                ],
            )

            assert result.exit_code == 0
            assert "silver_transforms" in result.output

            # Should only find the flowgroup with pipeline: silver_transforms
            generated_dir = (
                project_with_pipeline_field_structure
                / "generated"
                / "dev"
                / "silver_transforms"
            )

            assert (generated_dir / "customer_transforms.py").exists()

            # Should NOT generate raw_ingestions files
            raw_dir = (
                project_with_pipeline_field_structure
                / "generated"
                / "dev"
                / "raw_ingestions"
            )
            assert not raw_dir.exists()

    def test_cli_validate_pipeline_flag_uses_pipeline_field(
        self, runner, project_with_pipeline_field_structure
    ):
        """Test that CLI validate --pipeline flag finds flowgroups by pipeline field."""
        with runner.isolated_filesystem():
            import os

            os.chdir(str(project_with_pipeline_field_structure))

            # Validate using pipeline field. ``--show-all`` opts into
            # the full per-pipeline summary table; the failures-only
            # default would suppress the table on a clean run, but this
            # test asserts that the pipeline name appears in stdout.
            result = runner.invoke(
                cli,
                [
                    "validate",
                    "--env",
                    "dev",
                    "--pipeline",
                    "raw_ingestions",
                    "--show-all",
                ],
            )

            assert result.exit_code == 0
            assert "raw_ingestions" in result.output

            # Rendering contract is asserted by test_validate_rendering;
            # here we only check that the pipeline name appears in the
            # per-pipeline display section (exit_code 0 covers validity).

    def test_orchestrator_discover_flowgroups_by_pipeline_field(
        self, project_with_pipeline_field_structure
    ):
        """Test that orchestrator can discover flowgroups by pipeline field across directories."""
        project_root = project_with_pipeline_field_structure

        orchestrator = build_facade_orchestrator(
            project_root, enforce_version=False
        )

        # This method should discover all flowgroups with the given pipeline field
        raw_flowgroups = orchestrator.discover_flowgroups_by_pipeline_field(
            "raw_ingestions"
        )
        silver_flowgroups = orchestrator.discover_flowgroups_by_pipeline_field(
            "silver_transforms"
        )

        # Should find 3 flowgroups for raw_ingestions
        assert len(raw_flowgroups) == 3
        flowgroup_names = {fg.flowgroup for fg in raw_flowgroups}
        assert "customer_ingestion" in flowgroup_names
        assert "orders_ingestion" in flowgroup_names
        assert "lineitem_ingestion" in flowgroup_names

        # All should have the same pipeline field
        for fg in raw_flowgroups:
            assert fg.pipeline == "raw_ingestions"

        # Should find 1 flowgroup for silver_transforms
        assert len(silver_flowgroups) == 1
        assert silver_flowgroups[0].flowgroup == "customer_transforms"
        assert silver_flowgroups[0].pipeline == "silver_transforms"

    def test_pipeline_field_validation_across_directories(
        self, project_with_pipeline_field_structure
    ):
        """Test that pipeline+flowgroup validation works across directories."""
        project_root = project_with_pipeline_field_structure

        # Create a duplicate pipeline+flowgroup combination in a different directory
        duplicate_dir = project_root / "pipelines" / "another_folder"
        duplicate_dir.mkdir(parents=True)

        # This should conflict with existing customer_ingestion in raw_ingestions pipeline
        duplicate_flowgroup = {
            "pipeline": "raw_ingestions",
            "flowgroup": "customer_ingestion",  # Duplicate combination
            "actions": [
                {
                    "name": "load_duplicate",
                    "type": "load",
                    "target": "v_duplicate",
                    "source": {"type": "sql", "sql": "SELECT * FROM duplicate"},
                },
                {
                    "name": "write_duplicate",
                    "type": "write",
                    "source": "v_duplicate",
                    "write_target": {
                        "type": "streaming_table",
                        "catalog": "test_cat",
                        "schema": "bronze",
                        "table": "duplicate",
                        "create_table": True,
                    },
                },
            ],
        }

        duplicate_file = duplicate_dir / "duplicate_customer.yaml"
        with open(duplicate_file, "w") as f:
            yaml.dump(duplicate_flowgroup, f)

        # This should fail validation due to duplicate pipeline+flowgroup
        orchestrator = build_facade_orchestrator(
            project_root, enforce_version=False
        )

        with pytest.raises(
            ValueError, match="(?s)Duplicate.*raw_ingestions.customer_ingestion"
        ):
            all_flowgroups = orchestrator.discover_all_flowgroups()
            orchestrator.validate_duplicate_pipeline_flowgroup_combinations(
                all_flowgroups
            )
