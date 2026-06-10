import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli
from lhp.core.coordination.layers import build_facade_orchestrator
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.models import FlowGroupContext
from lhp.parsers.yaml_parser import YAMLParser


@pytest.mark.e2e
class TestLocalVariablesE2E:
    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        yield

        os.chdir(self.original_cwd)

    def test_local_variables_resolve_correctly(self):
        """Test that local variables are resolved correctly during processing."""
        flowgroup_file = (
            self.project_root
            / "pipelines"
            / "02_bronze"
            / "local_variables_example.yaml"
        )
        parser = YAMLParser()
        flowgroups = parser.parse_flowgroups_from_file(flowgroup_file)

        assert len(flowgroups) == 1
        flowgroup = flowgroups[0]

        assert flowgroup.variables is not None
        assert flowgroup.variables["entity"] == "test_customer"
        assert flowgroup.variables["source_table"] == "customer_raw"
        assert flowgroup.variables["target_table"] == "test_customer"

        # Actions should still have unresolved patterns before processing
        assert "%{entity}" in flowgroup.actions[0].name

        orchestrator = build_facade_orchestrator(
            self.project_root, enforce_version=False
        )
        substitution_file = self.project_root / "substitutions" / "dev.yaml"
        substitution_mgr = EnhancedSubstitutionManager(substitution_file, env="dev")

        ctx_in = FlowGroupContext(flowgroup=flowgroup, source_yaml=None)
        processed_fg = orchestrator.processor.process_flowgroup(
            ctx_in, substitution_mgr
        ).flowgroup

        assert processed_fg.actions[0].name == "test_customer_raw_load"
        assert processed_fg.actions[0].target == "v_test_customer_raw"
        assert processed_fg.actions[1].name == "test_customer_cleanse"
        assert processed_fg.actions[1].source == "v_test_customer_raw"
        assert processed_fg.actions[1].target == "v_test_customer_cleaned"
        assert processed_fg.actions[2].name == "write_test_customer_bronze"

        # namespace normalized to catalog/schema
        assert processed_fg.actions[0].source["catalog"] == "acme_edw_dev"
        assert processed_fg.actions[0].source["schema"] == "edw_raw"
        assert processed_fg.actions[0].source["table"] == "customer_raw"
        assert processed_fg.actions[2].write_target["catalog"] == "acme_edw_dev"
        assert processed_fg.actions[2].write_target["schema"] == "edw_bronze"
        assert processed_fg.actions[2].write_target["table"] == "test_customer"

        assert (
            processed_fg.actions[0].description
            == "Load test_customer table from raw schema"
        )

    def test_undefined_local_variable_raises_error(self):
        """Test that undefined local variables cause proper error."""
        bad_flowgroup_content = """
pipeline: acmi_edw_bronze
flowgroup: bad_variables_example
#job_name: j_two

variables:
  entity: customer

actions:
  - name: "%{undefined_var}_action"
    type: load
    source:
      type: delta
      database: "{catalog}.{raw_schema}"
      table: "test"
    target: "v_test"
"""
        bad_file = (
            self.project_root / "pipelines" / "02_bronze" / "bad_variables_example.yaml"
        )
        bad_file.write_text(bad_flowgroup_content)

        parser = YAMLParser()
        flowgroups = parser.parse_flowgroups_from_file(bad_file)
        flowgroup = flowgroups[0]

        orchestrator = build_facade_orchestrator(
            self.project_root, enforce_version=False
        )
        substitution_file = self.project_root / "substitutions" / "dev.yaml"
        substitution_mgr = EnhancedSubstitutionManager(substitution_file, env="dev")

        from lhp.errors import LHPError

        ctx_in = FlowGroupContext(flowgroup=flowgroup, source_yaml=None)
        with pytest.raises(LHPError) as exc_info:
            orchestrator.processor.process_flowgroup(ctx_in, substitution_mgr)

        error = exc_info.value
        assert "Undefined local variable" in error.title
        assert "undefined_var" in error.details

    def test_generate_command_with_local_variables(self):
        """Test that lhp generate command works with local variables."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "generate",
                "-e",
                "dev",
                "--pipeline-config",
                "config/pipeline_config.yaml",
            ],
        )

        assert result.exit_code == 0, f"Generate command failed: {result.output}"

        generated_dir = self.project_root / "generated" / "dev"
        assert generated_dir.exists(), "Generated directory should exist"

        generated_file = (
            generated_dir / "acmi_edw_bronze" / "local_variables_example.py"
        )
        assert generated_file.exists(), f"Generated file should exist: {generated_file}"

        generated_content = generated_file.read_text()

        assert "v_test_customer_raw" in generated_content, (
            "Local variable should be resolved in function name"
        )
        assert "v_test_customer_cleaned" in generated_content, (
            "Local variable should be resolved in transformation view"
        )
        assert "test_customer" in generated_content, (
            "Local variable should be resolved in table name"
        )

        assert "%{entity}" not in generated_content, (
            "No unresolved %{entity} should remain"
        )
        assert "%{source_table}" not in generated_content, (
            "No unresolved %{source_table} should remain"
        )
        assert "%{target_table}" not in generated_content, (
            "No unresolved %{target_table} should remain"
        )

        assert "acme_edw_dev" in generated_content, (
            "Environment variable {catalog} should be resolved"
        )
        assert "edw_raw" in generated_content, (
            "Environment variable {raw_schema} should be resolved"
        )
        assert "edw_bronze" in generated_content, (
            "Environment variable {bronze_schema} should be resolved"
        )

        assert "Load test_customer table from raw schema" in generated_content, (
            "Description should have %{entity} resolved to 'test_customer'"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
