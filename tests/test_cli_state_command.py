"""Comprehensive tests for the CLI state command.

This test suite covers the 372-line state method with all its functionality:
- Basic state display (overall stats, environment-specific)
- File filtering (orphaned, stale, new files)
- Actions (cleanup, regeneration, dry-run)
- Edge cases and error scenarios
- Output formatting validation
"""

import pytest
import tempfile
import json
import yaml
from pathlib import Path
from click.testing import CliRunner
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from lhp.cli.main import cli
from lhp.core.state_manager import StateManager, FileState


# Test fixtures available to all test classes
@pytest.fixture
def runner():
    """Create CLI runner."""
    return CliRunner()

@pytest.fixture
def sample_project():
    """Create a sample project with full structure for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)
        
        # Create project structure
        directories = [
            'pipelines/bronze_layer',
            'pipelines/silver_layer', 
            'pipelines/gold_layer',
            'presets',
            'templates',
            'substitutions',
            'generated/bronze_layer',
            'generated/silver_layer',
            'generated/gold_layer'
        ]
        
        for dir_path in directories:
            (project_root / dir_path).mkdir(parents=True)
        
        # Create project config
        (project_root / "lhp.yaml").write_text("""
name: test_state_project
version: "1.0"
description: "Test project for state command"
""")
        
        # Create substitution files
        (project_root / "substitutions" / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  bronze_schema: bronze
  silver_schema: silver
""")
        
        (project_root / "substitutions" / "prod.yaml").write_text("""
prod:
  catalog: prod_catalog
  bronze_schema: bronze
  silver_schema: silver
""")
        
        # Create flowgroup files
        flowgroups = [
            ("pipelines/bronze_layer/customers.yaml", "bronze_layer", "customers"),
            ("pipelines/bronze_layer/orders.yaml", "bronze_layer", "orders"),
            ("pipelines/silver_layer/customer_dim.yaml", "silver_layer", "customer_dim"),
            ("pipelines/silver_layer/order_facts.yaml", "silver_layer", "order_facts"),
            ("pipelines/gold_layer/analytics.yaml", "gold_layer", "analytics")
        ]
        
        for file_path, pipeline, flowgroup in flowgroups:
            content = f"""
pipeline: {pipeline}
flowgroup: {flowgroup}
actions:
  - name: load_{flowgroup}
    type: load
    source:
      type: sql
      sql: "SELECT * FROM source_{flowgroup}"
    target: v_{flowgroup}
    
  - name: save_{flowgroup}
    type: write
    source: v_{flowgroup}
    write_target:
      type: streaming_table
      database: "{{bronze_schema}}"
      table: "{flowgroup}"
      create_table: true
"""
            (project_root / file_path).write_text(content)
        
        yield project_root

@pytest.fixture
def project_with_state(sample_project):
    """Create a project with existing state file and generated files."""
    project_root = sample_project
    
    # Create some generated files
    generated_files = [
        ("generated/bronze_layer/customers.py", "pipelines/bronze_layer/customers.yaml", "bronze_layer", "customers"),
        ("generated/bronze_layer/orders.py", "pipelines/bronze_layer/orders.yaml", "bronze_layer", "orders"),
        ("generated/silver_layer/customer_dim.py", "pipelines/silver_layer/customer_dim.yaml", "silver_layer", "customer_dim"),
        ("generated/silver_layer/order_facts.py", "pipelines/silver_layer/order_facts.yaml", "silver_layer", "order_facts"),
        # Orphaned file (source YAML doesn't exist)
        ("generated/bronze_layer/old_table.py", "pipelines/bronze_layer/old_table.yaml", "bronze_layer", "old_table")
    ]
    
    for gen_path, source_path, pipeline, flowgroup in generated_files:
        file_path = project_root / gen_path
        file_path.write_text(f"# Generated code for {flowgroup}\nimport dlt\n@dlt.view()\ndef {flowgroup}():\n    pass")
    
    # Create state file
    now = datetime.now().isoformat()
    old_time = (datetime.now() - timedelta(days=1)).isoformat()
    
    state_data = {
        "version": "1.0",
        "last_updated": now,
        "environments": {
            "dev": {},
            "prod": {}
        }
    }
    
    # Add file states for dev environment
    for gen_path, source_path, pipeline, flowgroup in generated_files:
        # Create checksum for existing files
        if (project_root / source_path).exists():
            checksum = "current_checksum"
            source_checksum = "current_source_checksum"
        else:
            # Orphaned file
            checksum = "old_checksum"  
            source_checksum = "old_source_checksum"
        
        state_data["environments"]["dev"][gen_path] = {
            "source_yaml": source_path,
            "generated_path": gen_path,
            "checksum": checksum,
            "source_yaml_checksum": source_checksum,
            "timestamp": old_time if flowgroup == "orders" else now,  # Make orders stale
            "environment": "dev",
            "pipeline": pipeline,
            "flowgroup": flowgroup
        }
    
    # Save state file
    (project_root / ".lhp_state.json").write_text(json.dumps(state_data, indent=2))
    
    return project_root

@pytest.fixture  
def project_with_new_files(project_with_state):
    """Create a project with new YAML files that haven't been generated yet."""
    project_root = project_with_state
    
    # Add new YAML files not in state
    new_files = [
        ("pipelines/bronze_layer/suppliers.yaml", "bronze_layer", "suppliers"),
        ("pipelines/gold_layer/dashboard.yaml", "gold_layer", "dashboard")
    ]
    
    for file_path, pipeline, flowgroup in new_files:
        content = f"""
pipeline: {pipeline}
flowgroup: {flowgroup}
actions:
  - name: load_{flowgroup}
    type: load
    source:
      type: sql
      sql: "SELECT * FROM source_{flowgroup}"
    target: v_{flowgroup}
"""
        (project_root / file_path).write_text(content)
    
    return project_root


class TestStateCommandInfrastructure:
    """Test infrastructure validation for state command testing."""
    
    def test_sample_project_structure(self, sample_project):
        """Verify sample project structure is created correctly."""
        assert (sample_project / "lhp.yaml").exists()
        assert (sample_project / "pipelines/bronze_layer").exists()
        assert (sample_project / "pipelines/bronze_layer/customers.yaml").exists()
        assert (sample_project / "substitutions/dev.yaml").exists()
    
    def test_project_with_state_structure(self, project_with_state):
        """Verify project with state has correct structure."""
        assert (project_with_state / ".lhp_state.json").exists()
        assert (project_with_state / "generated/bronze_layer/customers.py").exists()
        assert (project_with_state / "generated/bronze_layer/old_table.py").exists()  # orphaned
    
    def test_project_with_new_files_structure(self, project_with_new_files):
        """Verify project with new files has correct structure."""
        assert (project_with_new_files / "pipelines/bronze_layer/suppliers.yaml").exists()
        assert (project_with_new_files / "pipelines/gold_layer/dashboard.yaml").exists()


class TestStateCommandBasicFunctionality:
    """Test basic state command functionality without flags."""
    
    def test_state_command_no_args_no_files(self, runner, sample_project):
        """Test state command with no arguments when no files are tracked."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(sample_project))
            
            result = runner.invoke(cli, ['state'])
            
            assert result.exit_code == 0
            assert "📭 No tracked files found" in result.output
            assert "💡 Generate code to start tracking files" in result.output
    
    def test_state_command_no_args_with_files(self, runner, project_with_state):
        """Test state command with no arguments when files are tracked."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            result = runner.invoke(cli, ['state'])
            
            assert result.exit_code == 0
            assert "📊 LakehousePlumber State Information" in result.output
            assert "Total environments:" in result.output
            assert "🌍 Environment: dev" in result.output
            assert "💡 Use --env <environment> to see detailed file information" in result.output
    
    def test_state_command_specific_env(self, runner, project_with_state):
        """Test state command with specific environment."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            result = runner.invoke(cli, ['state', '--env', 'dev'])
            
            assert result.exit_code == 0
            assert "📊 State for Environment: dev" in result.output
            assert "📁 Tracked Files" in result.output
            assert "🔧 Pipeline: bronze_layer" in result.output
            assert "🔧 Pipeline: silver_layer" in result.output
    
    def test_state_command_specific_env_no_files(self, runner, project_with_state):
        """Test state command with environment that has no files."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            result = runner.invoke(cli, ['state', '--env', 'staging'])
            
            assert result.exit_code == 0
            assert "📊 State for Environment: staging" in result.output
            assert "📭 No tracked files found for this environment" in result.output
    
    def test_state_command_specific_pipeline(self, runner, project_with_state):
        """Test state command with specific pipeline filter."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            result = runner.invoke(cli, ['state', '--env', 'dev', '--pipeline', 'bronze_layer'])
            
            assert result.exit_code == 0
            assert "📊 State for Environment: dev" in result.output
            assert "🔧 Pipeline: bronze_layer" in result.output
            assert "silver_layer" not in result.output
    
    def test_state_command_nonexistent_pipeline(self, runner, project_with_state):
        """Test state command with nonexistent pipeline."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            result = runner.invoke(cli, ['state', '--env', 'dev', '--pipeline', 'nonexistent'])
            
            assert result.exit_code == 0
            assert "📭 No tracked files found for pipeline 'nonexistent' in environment 'dev'" in result.output


class TestStateCommandFileOperations:
    """Test state command with file operation flags."""
    
    def test_orphaned_files_flag(self, runner, project_with_state):
        """Test --orphaned flag to show orphaned files."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            result = runner.invoke(cli, ['state', '--env', 'dev', '--orphaned'])
            
            assert result.exit_code == 0
            assert "🗑️  Orphaned Files" in result.output
            assert "old_table.py" in result.output
            assert "pipelines/bronze_layer/old_table.yaml (missing)" in result.output
            assert "💡 Use --cleanup flag to remove these orphaned files" in result.output
    
    def test_orphaned_files_none_found(self, runner, sample_project):
        """Test --orphaned flag when no orphaned files exist."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(sample_project))
            
            # Create state manager and add a valid file with the actual generated file
            state_manager = StateManager(sample_project)
            
            # Create the generated file first
            gen_file = sample_project / "generated/bronze_layer/customers.py"
            gen_file.parent.mkdir(parents=True, exist_ok=True)
            gen_file.write_text("# Generated code")
            
            state_manager.track_generated_file(
                source_yaml=Path("pipelines/bronze_layer/customers.yaml"),
                generated_path=Path("generated/bronze_layer/customers.py"),
                environment="dev",
                pipeline="bronze_layer",
                flowgroup="customers"
            )
            
            # Save the state so the CLI command can see it
            state_manager._save_state()
            
            result = runner.invoke(cli, ['state', '--env', 'dev', '--orphaned'])
            
            assert result.exit_code == 0
            assert "✅ No orphaned files found" in result.output
    
    def test_stale_files_flag(self, runner, project_with_state):
        """Test --stale flag to show stale files."""
        # First, let's modify a YAML file to make it stale
        orders_yaml = project_with_state / "pipelines/bronze_layer/orders.yaml"
        content = orders_yaml.read_text()
        orders_yaml.write_text(content + "\n# Modified comment")
        
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            result = runner.invoke(cli, ['state', '--env', 'dev', '--stale'])
            
            assert result.exit_code == 0
            assert "📝 Stale Files" in result.output
            assert "💡 Use --regen flag to regenerate these stale files" in result.output
    
    def test_stale_files_none_found(self, runner, project_with_state):
        """Test --stale flag when no stale files exist."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            # Mock the state manager to return no stale files
            with patch('lhp.core.state_manager.StateManager.find_stale_files') as mock_stale:
                mock_stale.return_value = []
                
                result = runner.invoke(cli, ['state', '--env', 'dev', '--stale'])
                
                assert result.exit_code == 0
                assert "✅ No stale files found" in result.output
    
    def test_new_files_flag(self, runner, project_with_new_files):
        """Test --new flag to show new YAML files."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_new_files))
            
            result = runner.invoke(cli, ['state', '--env', 'dev', '--new'])
            
            assert result.exit_code == 0
            assert "🆕 New YAML Files" in result.output
            assert "suppliers.yaml" in result.output
            assert "dashboard.yaml" in result.output
            assert "lhp generate --env dev" in result.output
    
    def test_new_files_none_found(self, runner, project_with_state):
        """Test --new flag when no new files exist."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            # Remove the gold_layer analytics file that makes it show as "new"
            analytics_file = project_with_state / "pipelines/gold_layer/analytics.yaml"
            if analytics_file.exists():
                analytics_file.unlink()
            
            result = runner.invoke(cli, ['state', '--env', 'dev', '--new'])
            
            assert result.exit_code == 0
            assert "✅ No new YAML files found" in result.output


class TestStateCommandActions:
    """Test state command with action flags."""
    
    def test_cleanup_orphaned_dry_run(self, runner, project_with_state):
        """Test --cleanup with --dry-run for orphaned files."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            result = runner.invoke(cli, ['state', '--env', 'dev', '--orphaned', '--cleanup', '--dry-run'])
            
            assert result.exit_code == 0
            assert "📋 Would delete these orphaned files" in result.output
            assert "old_table.py" in result.output
            # File should still exist
            assert (project_with_state / "generated/bronze_layer/old_table.py").exists()
    
    def test_cleanup_orphaned_actual(self, runner, project_with_state):
        """Test --cleanup without --dry-run for orphaned files."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            # Verify file exists before cleanup
            orphaned_file = project_with_state / "generated/bronze_layer/old_table.py"
            assert orphaned_file.exists()
            
            result = runner.invoke(cli, ['state', '--env', 'dev', '--orphaned', '--cleanup'])
            
            assert result.exit_code == 0
            assert "🗑️  Cleaning up orphaned files..." in result.output
            assert "✅ Deleted" in result.output
    
    def test_regen_stale_dry_run(self, runner, project_with_state):
        """Test --regen with --dry-run for stale files."""
        # Make a file stale by modifying its source
        orders_yaml = project_with_state / "pipelines/bronze_layer/orders.yaml"
        content = orders_yaml.read_text()
        orders_yaml.write_text(content + "\n# Modified to make stale")
        
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            result = runner.invoke(cli, ['state', '--env', 'dev', '--stale', '--regen', '--dry-run'])
            
            assert result.exit_code == 0
            assert "📋 Would regenerate these stale files" in result.output
    
    def test_regen_stale_actual(self, runner, project_with_state):
        """Test --regen without --dry-run for stale files."""
        # Make a file stale by modifying its source
        orders_yaml = project_with_state / "pipelines/bronze_layer/orders.yaml"
        content = orders_yaml.read_text()
        orders_yaml.write_text(content + "\n# Modified to make stale")
        
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            # Mock the orchestrator to avoid actual generation
            with patch('lhp.core.orchestrator.ActionOrchestrator.generate_pipeline') as mock_gen:
                mock_gen.return_value = ["generated/bronze_layer/orders.py"]
                
                result = runner.invoke(cli, ['state', '--env', 'dev', '--stale', '--regen'])
                
                assert result.exit_code == 0
                assert "🔄 Regenerating stale files..." in result.output
                assert "✅ Regenerated" in result.output


class TestStateCommandEdgeCases:
    """Test edge cases and error scenarios."""
    
    def test_not_in_project_directory(self, runner):
        """Test state command when not in a project directory."""
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ['state'])
            
            assert result.exit_code == 1
            assert "Not in a LakehousePlumber project directory" in result.output
    
    def test_corrupted_state_file(self, runner, sample_project):
        """Test handling of corrupted state file."""
        # Create corrupted state file
        (sample_project / ".lhp_state.json").write_text("invalid json {")
        
        with runner.isolated_filesystem():
            import os
            os.chdir(str(sample_project))
            
            # Should handle gracefully and show no files
            result = runner.invoke(cli, ['state'])
            
            assert result.exit_code == 0
            assert "📭 No tracked files found" in result.output
    
    def test_permission_error_cleanup(self, runner, project_with_state):
        """Test cleanup when file deletion fails due to permissions."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            # Mock file deletion to raise PermissionError
            with patch('pathlib.Path.unlink', side_effect=PermissionError("Permission denied")):
                result = runner.invoke(cli, ['state', '--env', 'dev', '--orphaned', '--cleanup'])
                
                # Should handle error gracefully
                assert result.exit_code == 0
                # Error should be logged but command shouldn't crash
    
    def test_verbose_logging(self, runner, project_with_state):
        """Test state command with verbose logging."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            result = runner.invoke(cli, ['--verbose', 'state', '--env', 'dev'])
            
            assert result.exit_code == 0
            # Should include detailed logs
    
    def test_regen_pipeline_error(self, runner, project_with_state):
        """Test regeneration when pipeline generation fails."""
        # Make a file stale
        orders_yaml = project_with_state / "pipelines/bronze_layer/orders.yaml"
        content = orders_yaml.read_text()
        orders_yaml.write_text(content + "\n# Modified")
        
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            # Mock orchestrator to raise an error
            with patch('lhp.core.orchestrator.ActionOrchestrator.generate_pipeline') as mock_gen:
                mock_gen.side_effect = Exception("Generation failed")
                
                result = runner.invoke(cli, ['state', '--env', 'dev', '--stale', '--regen'])
                
                assert result.exit_code == 0  # Should handle error gracefully
                assert "Regeneration for pipeline 'bronze_layer' failed" in result.output


class TestStateCommandOutputFormatting:
    """Test output formatting and user experience."""
    
    def test_comprehensive_summary_output(self, runner, project_with_new_files):
        """Test comprehensive summary shows all file types correctly."""
        # Make one file stale
        orders_yaml = project_with_new_files / "pipelines/bronze_layer/orders.yaml"
        content = orders_yaml.read_text()
        orders_yaml.write_text(content + "\n# Modified")
        
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_new_files))
            
            result = runner.invoke(cli, ['state', '--env', 'dev'])
            
            assert result.exit_code == 0
            assert "📊 Summary:" in result.output
            assert "🟢" in result.output  # up-to-date files
            assert "🟡" in result.output  # stale files  
            assert "🔴" in result.output  # orphaned files
            assert "🆕" in result.output  # new files
            assert "💡 Smart generation tips:" in result.output
    
    def test_perfect_sync_message(self, runner, sample_project):
        """Test message when everything is in perfect sync."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(sample_project))
            
            # Remove all YAML files except the one we want to track (but keep lhp.yaml)
            for yaml_file in sample_project.rglob("*.yaml"):
                if "customers.yaml" not in str(yaml_file) and "lhp.yaml" not in str(yaml_file):
                    yaml_file.unlink()
            
            # Create the generated file first
            gen_file = sample_project / "generated/bronze_layer/customers.py"
            gen_file.parent.mkdir(parents=True, exist_ok=True)
            gen_file.write_text("# Generated code")
            
            # Create state with no issues
            state_manager = StateManager(sample_project)
            state_manager.track_generated_file(
                source_yaml=Path("pipelines/bronze_layer/customers.yaml"),
                generated_path=Path("generated/bronze_layer/customers.py"),
                environment="dev",
                pipeline="bronze_layer", 
                flowgroup="customers"
            )
            
            # Save the state so the CLI command can see it
            state_manager._save_state()
            
            result = runner.invoke(cli, ['state', '--env', 'dev'])
            
            assert result.exit_code == 0
            assert "✨ Everything is in perfect sync!" in result.output
    
    def test_file_status_indicators(self, runner, project_with_state):
        """Test file status indicators (✅, ❌, 🟢, 🟡)."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            result = runner.invoke(cli, ['state', '--env', 'dev'])
            
            assert result.exit_code == 0
            # Should show file existence indicators
            assert "✅" in result.output or "❌" in result.output
            # Should show change status indicators  
            assert "🟢" in result.output or "🟡" in result.output
    
    def test_pipeline_grouping(self, runner, project_with_state):
        """Test that files are properly grouped by pipeline."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(project_with_state))
            
            result = runner.invoke(cli, ['state', '--env', 'dev'])
            
            assert result.exit_code == 0
            # Should group by pipeline
            assert "🔧 Pipeline: bronze_layer" in result.output
            assert "🔧 Pipeline: silver_layer" in result.output
            # Files should be listed under their respective pipelines 