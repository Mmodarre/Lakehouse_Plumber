"""Tests for CLI commands with include functionality."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from click.testing import CliRunner

# Note: The actual implementation will be done later - these tests define the expected behavior


class TestCLIIncludeFunctionality:
    """Test cases for CLI commands with include patterns."""

    def test_validate_command_with_include_patterns(self, tmp_path):
        """Test validate command with include patterns."""
        # Given: A project with include patterns and various files
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()

        # Create project configuration with include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "bronze_*.yaml"
  - "silver_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)

        # Create test files
        (pipelines_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "bronze_orders.yaml").write_text("""
pipeline: bronze
flowgroup: orders
actions:
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /data/orders
    target: v_orders
""")
        (pipelines_dir / "silver_customers.yaml").write_text("""
pipeline: silver
flowgroup: customers
actions:
  - name: transform_customers
    type: transform
    source: v_customers
    target: v_customers_silver
""")
        (pipelines_dir / "gold_analytics.yaml").write_text("""
pipeline: gold
flowgroup: analytics
actions:
  - name: aggregate_data
    type: transform
    source: v_data
    target: v_analytics
""")

        # Create substitution file
        substitutions_dir = project_root / "substitutions"
        substitutions_dir.mkdir()
        (substitutions_dir / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  bronze_schema: bronze
  silver_schema: silver
""")

        # When: Running validate command with include patterns
        # Expected: Only files matching include patterns should be validated

        # This will be implemented later
        # from lhp.cli.main import cli
        #
        # runner = CliRunner()
        # with runner.isolated_filesystem():
        #     # Copy project to isolated filesystem
        #     # ... copy logic ...
        #
        #     result = runner.invoke(cli, ['validate', '--env', 'dev'])
        #
        #     # Should validate only bronze and silver files (gold excluded)
        #     assert result.exit_code == 0
        #     assert "bronze" in result.output
        #     assert "silver" in result.output
        #     assert "gold" not in result.output

    def test_validate_command_without_include_patterns(self, tmp_path):
        """Test validate command without include patterns (backwards compatibility)."""
        # Given: A project without include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()

        # Create project configuration without include patterns
        config_content = """
name: test_project
version: "1.0"
"""
        (project_root / "lhp.yaml").write_text(config_content)

        # Create test files
        (pipelines_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "silver_orders.yaml").write_text("""
pipeline: silver
flowgroup: orders
actions:
  - name: transform_orders
    type: transform
    source: v_orders
    target: v_orders_silver
""")

        # Create substitution file
        substitutions_dir = project_root / "substitutions"
        substitutions_dir.mkdir()
        (substitutions_dir / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  bronze_schema: bronze
  silver_schema: silver
""")

        # When: Running validate command without include patterns
        # Expected: All files should be validated (backwards compatibility)

        # This will be implemented later
        # from lhp.cli.main import cli
        #
        # runner = CliRunner()
        # with runner.isolated_filesystem():
        #     # Copy project to isolated filesystem
        #     # ... copy logic ...
        #
        #     result = runner.invoke(cli, ['validate', '--env', 'dev'])
        #
        #     # Should validate all files
        #     assert result.exit_code == 0
        #     assert "bronze" in result.output
        #     assert "silver" in result.output

    def test_generate_command_with_include_patterns(self, tmp_path):
        """Test generate command with include patterns."""
        # Given: A project with include patterns and various files
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()

        # Create project configuration with include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "bronze_*.yaml"
  - "silver_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)

        # Create test files
        (pipelines_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze_layer
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "bronze_orders.yaml").write_text("""
pipeline: bronze_layer
flowgroup: orders
actions:
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /data/orders
    target: v_orders
""")
        (pipelines_dir / "silver_customers.yaml").write_text("""
pipeline: silver_layer
flowgroup: customers
actions:
  - name: transform_customers
    type: transform
    source: v_customers
    target: v_customers_silver
""")
        (pipelines_dir / "gold_analytics.yaml").write_text("""
pipeline: gold_layer
flowgroup: analytics
actions:
  - name: aggregate_data
    type: transform
    source: v_data
    target: v_analytics
""")

        # Create substitution file
        substitutions_dir = project_root / "substitutions"
        substitutions_dir.mkdir()
        (substitutions_dir / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  bronze_schema: bronze
  silver_schema: silver
""")

        # When: Running generate command with include patterns
        # Expected: Only files matching include patterns should be generated

        # This will be implemented later
        # from lhp.cli.main import cli
        #
        # runner = CliRunner()
        # with runner.isolated_filesystem():
        #     # Copy project to isolated filesystem
        #     # ... copy logic ...
        #
        #     result = runner.invoke(cli, ['generate', '--env', 'dev'])
        #
        #     # Should generate only bronze and silver files (gold excluded)
        #     assert result.exit_code == 0
        #     assert "bronze_layer" in result.output
        #     assert "silver_layer" in result.output
        #     assert "gold_layer" not in result.output
        #
        #     # Check generated files
        #     generated_dir = Path("generated")
        #     assert (generated_dir / "bronze_layer" / "customers.py").exists()
        #     assert (generated_dir / "bronze_layer" / "orders.py").exists()
        #     assert (generated_dir / "silver_layer" / "customers.py").exists()
        #     assert not (generated_dir / "gold_layer").exists()

    def test_generate_command_without_include_patterns(self, tmp_path):
        """Test generate command without include patterns (backwards compatibility)."""
        # Given: A project without include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()

        # Create project configuration without include patterns
        config_content = """
name: test_project
version: "1.0"
"""
        (project_root / "lhp.yaml").write_text(config_content)

        # Create test files
        (pipelines_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze_layer
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "silver_orders.yaml").write_text("""
pipeline: silver_layer
flowgroup: orders
actions:
  - name: transform_orders
    type: transform
    source: v_orders
    target: v_orders_silver
""")

        # Create substitution file
        substitutions_dir = project_root / "substitutions"
        substitutions_dir.mkdir()
        (substitutions_dir / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  bronze_schema: bronze
  silver_schema: silver
""")

        # When: Running generate command without include patterns
        # Expected: All files should be generated (backwards compatibility)

        # This will be implemented later
        # from lhp.cli.main import cli
        #
        # runner = CliRunner()
        # with runner.isolated_filesystem():
        #     # Copy project to isolated filesystem
        #     # ... copy logic ...
        #
        #     result = runner.invoke(cli, ['generate', '--env', 'dev'])
        #
        #     # Should generate all files
        #     assert result.exit_code == 0
        #     assert "bronze_layer" in result.output
        #     assert "silver_layer" in result.output
        #
        #     # Check generated files
        #     generated_dir = Path("generated")
        #     assert (generated_dir / "bronze_layer" / "customers.py").exists()
        #     assert (generated_dir / "silver_layer" / "orders.py").exists()

    def test_generate_command_dry_run_with_include_patterns(self, tmp_path):
        """Test generate command with dry-run and include patterns."""
        # Given: A project with include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()

        # Create project configuration with include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "bronze_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)

        # Create test files
        (pipelines_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze_layer
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "silver_orders.yaml").write_text("""
pipeline: silver_layer
flowgroup: orders
actions:
  - name: transform_orders
    type: transform
    source: v_orders
    target: v_orders_silver
""")

        # Create substitution file
        substitutions_dir = project_root / "substitutions"
        substitutions_dir.mkdir()
        (substitutions_dir / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  bronze_schema: bronze
""")

        # When: Running generate command with dry-run and include patterns
        # Expected: Only files matching include patterns should be shown in preview

        # This will be implemented later
        # from lhp.cli.main import cli
        #
        # runner = CliRunner()
        # with runner.isolated_filesystem():
        #     # Copy project to isolated filesystem
        #     # ... copy logic ...
        #
        #     result = runner.invoke(cli, ['generate', '--env', 'dev', '--dry-run'])
        #
        #     # Should show only bronze files in preview (silver excluded)
        #     assert result.exit_code == 0
        #     assert "bronze_layer" in result.output
        #     assert "silver_layer" not in result.output
        #     assert "Would generate" in result.output
        #
        #     # No files should be created in dry-run
        #     assert not Path("generated").exists()

    def test_validate_command_specific_pipeline_with_include_patterns(self, tmp_path):
        """Test validate command for specific pipeline with include patterns."""
        # Given: A project with include patterns and specific pipeline
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()

        # Create project configuration with include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "bronze_*.yaml"
  - "silver_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)

        # Create test files for different pipelines
        (pipelines_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze_layer
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "silver_orders.yaml").write_text("""
pipeline: silver_layer
flowgroup: orders
actions:
  - name: transform_orders
    type: transform
    source: v_orders
    target: v_orders_silver
""")
        (pipelines_dir / "gold_analytics.yaml").write_text("""
pipeline: bronze_layer
flowgroup: analytics
actions:
  - name: aggregate_data
    type: transform
    source: v_data
    target: v_analytics
""")

        # Create substitution file
        substitutions_dir = project_root / "substitutions"
        substitutions_dir.mkdir()
        (substitutions_dir / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  bronze_schema: bronze
  silver_schema: silver
""")

        # When: Running validate command for specific pipeline with include patterns
        # Expected: Only files matching both pipeline and include patterns should be validated

        # This will be implemented later
        # from lhp.cli.main import cli
        #
        # runner = CliRunner()
        # with runner.isolated_filesystem():
        #     # Copy project to isolated filesystem
        #     # ... copy logic ...
        #
        #     result = runner.invoke(cli, ['validate', '--env', 'dev', '--pipeline', 'bronze_layer'])
        #
        #     # Should validate only bronze_layer files matching include patterns
        #     assert result.exit_code == 0
        #     assert "customers" in result.output  # bronze_customers.yaml matches
        #     assert "orders" not in result.output  # silver_orders.yaml doesn't match pipeline
        #     assert "analytics" not in result.output  # gold_analytics.yaml doesn't match include pattern

    def test_generate_command_specific_pipeline_with_include_patterns(self, tmp_path):
        """Test generate command for specific pipeline with include patterns."""
        # Given: A project with include patterns and specific pipeline
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()

        # Create project configuration with include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "bronze_*.yaml"
  - "silver_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)

        # Create test files for different pipelines
        (pipelines_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze_layer
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "silver_orders.yaml").write_text("""
pipeline: silver_layer
flowgroup: orders
actions:
  - name: transform_orders
    type: transform
    source: v_orders
    target: v_orders_silver
""")
        (pipelines_dir / "gold_analytics.yaml").write_text("""
pipeline: bronze_layer
flowgroup: analytics
actions:
  - name: aggregate_data
    type: transform
    source: v_data
    target: v_analytics
""")

        # Create substitution file
        substitutions_dir = project_root / "substitutions"
        substitutions_dir.mkdir()
        (substitutions_dir / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  bronze_schema: bronze
  silver_schema: silver
""")

        # When: Running generate command for specific pipeline with include patterns
        # Expected: Only files matching both pipeline and include patterns should be generated

        # This will be implemented later
        # from lhp.cli.main import cli
        #
        # runner = CliRunner()
        # with runner.isolated_filesystem():
        #     # Copy project to isolated filesystem
        #     # ... copy logic ...
        #
        #     result = runner.invoke(cli, ['generate', '--env', 'dev', '--pipeline', 'bronze_layer'])
        #
        #     # Should generate only bronze_layer files matching include patterns
        #     assert result.exit_code == 0
        #     assert "customers" in result.output  # bronze_customers.yaml matches
        #     assert "orders" not in result.output  # silver_orders.yaml doesn't match pipeline
        #     assert "analytics" not in result.output  # gold_analytics.yaml doesn't match include pattern
        #
        #     # Check generated files
        #     generated_dir = Path("generated")
        #     assert (generated_dir / "bronze_layer" / "customers.py").exists()
        #     assert not (generated_dir / "bronze_layer" / "analytics.py").exists()

    def test_validate_command_no_files_match_include_patterns(self, tmp_path):
        """Test validate command when no files match include patterns."""
        # Given: A project with include patterns that don't match any files
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()

        # Create project configuration with non-matching include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "gold_*.yaml"
  - "platinum_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)

        # Create test files that don't match patterns
        (pipelines_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "silver_orders.yaml").write_text("""
pipeline: silver
flowgroup: orders
actions:
  - name: transform_orders
    type: transform
    source: v_orders
    target: v_orders_silver
""")

        # Create substitution file
        substitutions_dir = project_root / "substitutions"
        substitutions_dir.mkdir()
        (substitutions_dir / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  bronze_schema: bronze
""")

        # When: Running validate command with non-matching include patterns
        # Expected: Should report no flowgroups found

        # This will be implemented later
        # from lhp.cli.main import cli
        #
        # runner = CliRunner()
        # with runner.isolated_filesystem():
        #     # Copy project to isolated filesystem
        #     # ... copy logic ...
        #
        #     result = runner.invoke(cli, ['validate', '--env', 'dev'])
        #
        #     # Should report no flowgroups found
        #     assert result.exit_code == 1
        #     assert "No flowgroups found" in result.output

    def test_generate_command_no_files_match_include_patterns(self, tmp_path):
        """Test generate command when no files match include patterns."""
        # Given: A project with include patterns that don't match any files
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()

        # Create project configuration with non-matching include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "gold_*.yaml"
  - "platinum_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)

        # Create test files that don't match patterns
        (pipelines_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze_layer
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "silver_orders.yaml").write_text("""
pipeline: silver_layer
flowgroup: orders
actions:
  - name: transform_orders
    type: transform
    source: v_orders
    target: v_orders_silver
""")

        # Create substitution file
        substitutions_dir = project_root / "substitutions"
        substitutions_dir.mkdir()
        (substitutions_dir / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  bronze_schema: bronze
""")

        # When: Running generate command with non-matching include patterns
        # Expected: Should report no flowgroups found

        # This will be implemented later
        # from lhp.cli.main import cli
        #
        # runner = CliRunner()
        # with runner.isolated_filesystem():
        #     # Copy project to isolated filesystem
        #     # ... copy logic ...
        #
        #     result = runner.invoke(cli, ['generate', '--env', 'dev'])
        #
        #     # Should report no flowgroups found
        #     assert result.exit_code == 1
        #     assert "No flowgroups found" in result.output

    def test_validate_command_with_invalid_include_patterns(self, tmp_path):
        """Test validate command with invalid include patterns."""
        # Given: A project with invalid include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()

        # Create project configuration with invalid include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "valid_*.yaml"
  - ""  # Invalid empty pattern
  - "invalid[pattern"  # Invalid regex
"""
        (project_root / "lhp.yaml").write_text(config_content)

        # Create test files
        (pipelines_dir / "valid_customers.yaml").write_text("""
pipeline: bronze
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")

        # Create substitution file
        substitutions_dir = project_root / "substitutions"
        substitutions_dir.mkdir()
        (substitutions_dir / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  bronze_schema: bronze
""")

        # When: Running validate command with invalid include patterns
        # Expected: Should report error about invalid patterns

        # This will be implemented later
        # from lhp.cli.main import cli
        #
        # runner = CliRunner()
        # with runner.isolated_filesystem():
        #     # Copy project to isolated filesystem
        #     # ... copy logic ...
        #
        #     result = runner.invoke(cli, ['validate', '--env', 'dev'])
        #
        #     # Should report error about invalid patterns
        #     assert result.exit_code == 1
        #     assert "Invalid include pattern" in result.output

        # When: Running generate command with cleanup and include patterns
        # Expected: Should clean up orphaned files and generate only included files

        # This will be implemented later
        # from lhp.cli.main import cli
        #
        # runner = CliRunner()
        # with runner.isolated_filesystem():
        #     # Copy project to isolated filesystem
        #     # ... copy logic ...
        #
        #     result = runner.invoke(cli, ['generate', '--env', 'dev', '--cleanup'])
        #
        #     # Should clean up orphaned files and generate included files
        #     assert result.exit_code == 0
        #     assert "bronze_layer" in result.output
        #     assert "Cleaning up" in result.output
        #
        #     # Check that orphaned files are cleaned up
        #     assert (Path("generated") / "bronze_layer" / "customers.py").exists()
        #     assert not (Path("generated") / "bronze_layer" / "old_file.py").exists()

    def test_stats_command_with_include_patterns(self, tmp_path):
        """Test stats command with include patterns."""
        # Given: A project with include patterns and various files
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()

        # Create project configuration with include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "bronze_*.yaml"
  - "silver_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)

        # Create test files
        (pipelines_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "bronze_orders.yaml").write_text("""
pipeline: bronze
flowgroup: orders
actions:
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /data/orders
    target: v_orders
""")
        (pipelines_dir / "silver_customers.yaml").write_text("""
pipeline: silver
flowgroup: customers
actions:
  - name: transform_customers
    type: transform
    source: v_customers
    target: v_customers_silver
""")
        (pipelines_dir / "gold_analytics.yaml").write_text("""
pipeline: gold
flowgroup: analytics
actions:
  - name: aggregate_data
    type: transform
    source: v_data
    target: v_analytics
""")

        # When: Running stats command with include patterns
        # Expected: Should show statistics for only included files

        # This will be implemented later
        # from lhp.cli.main import cli
        #
        # runner = CliRunner()
        # with runner.isolated_filesystem():
        #     # Copy project to isolated filesystem
        #     # ... copy logic ...
        #
        #     result = runner.invoke(cli, ['stats'])
        #
        #     # Should show statistics for only included files
        #     assert result.exit_code == 0
        #     assert "FlowGroups: 3" in result.output  # bronze: 2, silver: 1
        #     assert "Actions: 3" in result.output  # 3 actions total from included files


class TestCLIIncludeIntegration:
    """Integration tests for CLI commands with include functionality."""

    def test_full_workflow_with_include_patterns(self, tmp_path):
        """Test full workflow (validate -> generate) with include patterns."""
        # Given: A complete project with include patterns
        project_root = tmp_path
        self._create_complete_project(project_root)

        # When: Running full workflow with include patterns
        # Expected: Should work end-to-end with include filtering

        # This will be implemented later
        # from lhp.cli.main import cli
        #
        # runner = CliRunner()
        # with runner.isolated_filesystem():
        #     # Copy project to isolated filesystem
        #     # ... copy logic ...
        #
        #     # First validate
        #     validate_result = runner.invoke(cli, ['validate', '--env', 'dev'])
        #     assert validate_result.exit_code == 0
        #
        #     # Then generate
        #     generate_result = runner.invoke(cli, ['generate', '--env', 'dev'])
        #     assert generate_result.exit_code == 0
        #
        #     # Check that only included files were processed
        #     assert "bronze_layer" in generate_result.output
        #     assert "silver_layer" in generate_result.output
        #     assert "gold_layer" not in generate_result.output

    def test_init_command_with_include_patterns(self, tmp_path):
        """Test init command creates example include patterns."""
        # Given: A directory for new project

        # When: Running init command
        # Expected: Should create lhp.yaml with example include patterns

        # This will be implemented later
        # from lhp.cli.main import cli
        #
        # runner = CliRunner()
        # with runner.isolated_filesystem():
        #     result = runner.invoke(cli, ['init', 'test_project'])
        #
        #     # Should create project with example include patterns
        #     assert result.exit_code == 0
        #     assert Path("test_project").exists()
        #     assert (Path("test_project") / "lhp.yaml").exists()
        #
        #     # Check that lhp.yaml contains include examples
        #     with open(Path("test_project") / "lhp.yaml", "r") as f:
        #         content = f.read()
        #         assert "include:" in content
        #         assert "Examples:" in content or "bronze_*.yaml" in content

    def _create_complete_project(self, project_root):
        """Helper method to create a complete project structure."""
        # Create directories
        pipelines_dir = project_root / "pipelines"
        substitutions_dir = project_root / "substitutions"
        pipelines_dir.mkdir()
        substitutions_dir.mkdir()

        # Create project configuration with include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "bronze_*.yaml"
  - "silver_*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)

        # Create pipeline files
        (pipelines_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze_layer
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        (pipelines_dir / "silver_orders.yaml").write_text("""
pipeline: silver_layer
flowgroup: orders
actions:
  - name: transform_orders
    type: transform
    source: v_orders
    target: v_orders_silver
""")
        (pipelines_dir / "gold_analytics.yaml").write_text("""
pipeline: gold_layer
flowgroup: analytics
actions:
  - name: aggregate_data
    type: transform
    source: v_data
    target: v_analytics
""")

        # Create substitution file
        (substitutions_dir / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  bronze_schema: bronze
  silver_schema: silver
""")
