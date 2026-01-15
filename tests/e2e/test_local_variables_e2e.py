"""E2E tests for local variables feature."""

import pytest
import shutil
import os
from pathlib import Path
from click.testing import CliRunner

from lhp.cli.main import cli
from lhp.core.orchestrator import ActionOrchestrator
from lhp.parsers.yaml_parser import YAMLParser
from lhp.utils.substitution import EnhancedSubstitutionManager


@pytest.mark.e2e
class TestLocalVariablesE2E:
    """E2E tests for local variables feature with full generation pipeline."""
    
    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        """Set up fresh test project for each test method."""
        # Copy fixture to isolated temp directory
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)
        
        # Change to project directory
        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)
        
        yield
        
        # Cleanup
        os.chdir(self.original_cwd)
    
    def test_local_variables_resolve_correctly(self):
        """Test that local variables are resolved correctly during processing."""
        # Parse the flowgroup
        flowgroup_file = self.project_root / "pipelines" / "02_bronze" / "local_variables_example.yaml"
        parser = YAMLParser()
        flowgroups = parser.parse_flowgroups_from_file(flowgroup_file)
        
        assert len(flowgroups) == 1
        flowgroup = flowgroups[0]
        
        # Verify variables were parsed
        assert flowgroup.variables is not None
        assert flowgroup.variables["entity"] == "test_customer"
        assert flowgroup.variables["source_table"] == "customer_raw"
        assert flowgroup.variables["target_table"] == "test_customer"
        
        # Verify actions still have unresolved patterns (before processing)
        assert "%{entity}" in flowgroup.actions[0].name
        
        # Process the flowgroup with orchestrator
        orchestrator = ActionOrchestrator(self.project_root, enforce_version=False)
        substitution_file = self.project_root / "substitutions" / "dev.yaml"
        substitution_mgr = EnhancedSubstitutionManager(substitution_file, env="dev")
        
        processed_fg = orchestrator.processor.process_flowgroup(flowgroup, substitution_mgr)
        
        # Verify local variables were resolved (entity = test_customer)
        assert processed_fg.actions[0].name == "test_customer_raw_load"
        assert processed_fg.actions[0].target == "v_test_customer_raw"
        assert processed_fg.actions[1].name == "test_customer_cleanse"
        assert processed_fg.actions[1].source == "v_test_customer_raw"
        assert processed_fg.actions[1].target == "v_test_customer_cleaned"
        assert processed_fg.actions[2].name == "write_test_customer_bronze"
        
        # Verify environment tokens were also resolved
        assert processed_fg.actions[0].source["database"] == "acme_edw_dev.edw_raw"
        assert processed_fg.actions[0].source["table"] == "customer_raw"
        assert processed_fg.actions[2].write_target["database"] == "acme_edw_dev.edw_bronze"
        assert processed_fg.actions[2].write_target["table"] == "test_customer"
        
        # Verify descriptions resolved
        assert processed_fg.actions[0].description == "Load test_customer table from raw schema"
    
    def test_undefined_local_variable_raises_error(self):
        """Test that undefined local variables cause proper error."""
        # Create a flowgroup with undefined variable
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
        bad_file = self.project_root / "pipelines" / "02_bronze" / "bad_variables_example.yaml"
        bad_file.write_text(bad_flowgroup_content)
        
        # Parse and process
        parser = YAMLParser()
        flowgroups = parser.parse_flowgroups_from_file(bad_file)
        flowgroup = flowgroups[0]
        
        orchestrator = ActionOrchestrator(self.project_root, enforce_version=False)
        substitution_file = self.project_root / "substitutions" / "dev.yaml"
        substitution_mgr = EnhancedSubstitutionManager(substitution_file, env="dev")
        
        # Should raise error about undefined variable
        from lhp.utils.error_formatter import LHPError
        with pytest.raises(LHPError) as exc_info:
            orchestrator.processor.process_flowgroup(flowgroup, substitution_mgr)
        
        error = exc_info.value
        assert "Undefined local variable" in error.title
        assert "undefined_var" in error.details
    
    def test_generate_command_with_local_variables(self):
        """Test that lhp generate command works with local variables."""
        runner = CliRunner()
        result = runner.invoke(cli, ['generate', '-e', 'dev'])
        
        # Should succeed without errors
        assert result.exit_code == 0, f"Generate command failed: {result.output}"
        
        # Check that generated files directory was created
        generated_dir = self.project_root / "generated" / "dev"
        assert generated_dir.exists(), "Generated directory should exist"
        
        # Check that pipeline flowgroup file was generated
        generated_file = generated_dir / "acmi_edw_bronze" / "local_variables_example.py"
        assert generated_file.exists(), f"Generated file should exist: {generated_file}"
        
        # Verify the generated content has resolved local variables
        generated_content = generated_file.read_text()
        
        # Local variables should be resolved (entity = test_customer)
        assert "v_test_customer_raw" in generated_content, \
            "Local variable should be resolved in function name"
        assert "v_test_customer_cleaned" in generated_content, \
            "Local variable should be resolved in transformation view"
        assert "test_customer" in generated_content, \
            "Local variable should be resolved in table name"
        
        # No unresolved local variables should remain
        assert "%{entity}" not in generated_content, \
            "No unresolved %{entity} should remain"
        assert "%{source_table}" not in generated_content, \
            "No unresolved %{source_table} should remain"
        assert "%{target_table}" not in generated_content, \
            "No unresolved %{target_table} should remain"
        
        # Environment variables should be resolved
        assert "acme_edw_dev" in generated_content, \
            "Environment variable {catalog} should be resolved"
        assert "edw_raw" in generated_content, \
            "Environment variable {raw_schema} should be resolved"
        assert "edw_bronze" in generated_content, \
            "Environment variable {bronze_schema} should be resolved"
        
        # Description should have local variable resolved
        assert "Load test_customer table from raw schema" in generated_content, \
            "Description should have %{entity} resolved to 'test_customer'"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
