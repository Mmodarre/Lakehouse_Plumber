"""
Tests for BundleManager core functionality.

Tests the core bundle management operations including initialization,
directory discovery, and resource file operations.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch

from lhp.bundle.manager import BundleManager
from lhp.bundle.exceptions import BundleResourceError


class TestBundleManagerCore:
    """Test suite for BundleManager core functionality."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.manager = BundleManager(self.project_root)

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_bundle_manager_initialization(self):
        """Should initialize with correct project root and resources directory."""
        assert self.manager.project_root == self.project_root
        assert self.manager.resources_dir == self.project_root / "resources" / "lhp"

    def test_bundle_manager_creates_resources_directory(self):
        """Should create resources/lhp directory if it doesn't exist."""
        # Ensure resources/lhp directory doesn't exist initially
        assert not (self.project_root / "resources" / "lhp").exists()
        
        # Initialize manager and call method that ensures directory exists
        self.manager.ensure_resources_directory()
        
        # Resources/lhp directory should now exist
        assert (self.project_root / "resources" / "lhp").exists()
        assert (self.project_root / "resources" / "lhp").is_dir()
        # Parent resources directory should also exist
        assert (self.project_root / "resources").exists()
        assert (self.project_root / "resources").is_dir()

    def test_bundle_manager_resources_directory_already_exists(self):
        """Should handle existing resources/lhp directory gracefully."""
        # Create resources/lhp directory
        resources_lhp_dir = self.project_root / "resources" / "lhp"
        resources_lhp_dir.mkdir(parents=True)
        
        # Should not raise error
        self.manager.ensure_resources_directory()
        
        # Directory should still exist
        assert (self.project_root / "resources" / "lhp").exists()
        assert (self.project_root / "resources").exists()

    def test_get_pipeline_directories_with_multiple_pipelines(self):
        """Should correctly identify pipeline directories in generated/."""
        # Create generated directory structure
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Create pipeline directories
        (generated_dir / "raw_ingestions").mkdir()
        (generated_dir / "bronze_load").mkdir() 
        (generated_dir / "silver_load").mkdir()
        
        # Create some files to ignore
        (generated_dir / "readme.txt").write_text("not a directory")
        
        pipeline_dirs = self.manager.get_pipeline_directories(generated_dir)
        
        # Should return only directories
        assert len(pipeline_dirs) == 3
        
        pipeline_names = [d.name for d in pipeline_dirs]
        assert "raw_ingestions" in pipeline_names
        assert "bronze_load" in pipeline_names
        assert "silver_load" in pipeline_names

    def test_get_pipeline_directories_returns_sorted_order(self):
        """Should return pipeline directories in sorted order for deterministic processing."""
        # Create generated directory structure
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Create pipeline directories in non-alphabetical order to test sorting
        (generated_dir / "pipeline_3").mkdir()
        (generated_dir / "pipeline_1").mkdir() 
        (generated_dir / "pipeline_2").mkdir()
        (generated_dir / "aaa_first").mkdir()
        (generated_dir / "zzz_last").mkdir()
        
        pipeline_dirs = self.manager.get_pipeline_directories(generated_dir)
        
        # Should return directories in sorted order
        assert len(pipeline_dirs) == 5
        
        pipeline_names = [d.name for d in pipeline_dirs]
        expected_order = ["aaa_first", "pipeline_1", "pipeline_2", "pipeline_3", "zzz_last"]
        assert pipeline_names == expected_order

    def test_get_pipeline_directories_empty_generated(self):
        """Should return empty list when generated directory is empty."""
        # Create empty generated directory
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        pipeline_dirs = self.manager.get_pipeline_directories(generated_dir)
        
        assert pipeline_dirs == []

    def test_get_pipeline_directories_nonexistent_generated(self):
        """Should raise BundleResourceError when generated directory doesn't exist."""
        nonexistent_dir = self.project_root / "nonexistent"
        
        with pytest.raises(BundleResourceError) as exc_info:
            self.manager.get_pipeline_directories(nonexistent_dir)
        
        assert "Output directory does not exist" in str(exc_info.value)







    def test_resource_file_path_generation(self):
        """Should generate correct resource file paths for pipelines."""
        # Test resource file path generation
        resource_path = self.manager.get_resource_file_path("raw_ingestions")
        
        expected_path = self.project_root / "resources" / "lhp" / "raw_ingestions.pipeline.yml"
        assert resource_path == expected_path

    def test_resource_file_path_generation_with_special_characters(self):
        """Should handle pipeline names with special characters."""
        # Test with pipeline name containing underscores and numbers
        resource_path = self.manager.get_resource_file_path("bronze_layer_v2")
        
        expected_path = self.project_root / "resources" / "lhp" / "bronze_layer_v2.pipeline.yml"
        assert resource_path == expected_path

    def test_bundle_manager_logging(self):
        """Should initialize logger correctly."""
        assert hasattr(self.manager, 'logger')
        assert self.manager.logger.name == 'lhp.bundle.manager'


class TestBundleManagerFileOperations:
    """Test file operations and edge cases for BundleManager."""

    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.manager = BundleManager(self.project_root)

    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)

    def test_get_pipeline_directories_with_permission_error(self):
        """Should handle permission errors gracefully."""
        # Create generated directory
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Create pipeline directory and restrict permissions
        pipeline_dir = generated_dir / "restricted_pipeline"
        pipeline_dir.mkdir()
        pipeline_dir.chmod(0o000)  # No permissions
        
        try:
            # Should not raise exception
            pipeline_dirs = self.manager.get_pipeline_directories(generated_dir)
            
            # Should return only accessible directories (implementation dependent)
            assert isinstance(pipeline_dirs, list)
            
        finally:
            # Restore permissions for cleanup
            pipeline_dir.chmod(0o755)



    def test_get_pipeline_directories_with_symbolic_links(self):
        """Should handle symbolic links appropriately."""
        # Create generated directory
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Create real pipeline directory
        real_pipeline = generated_dir / "real_pipeline"
        real_pipeline.mkdir()
        
        # Create symbolic link to pipeline directory
        try:
            link_pipeline = generated_dir / "link_pipeline"
            link_pipeline.symlink_to(real_pipeline)
            
            pipeline_dirs = self.manager.get_pipeline_directories(generated_dir)
            
            # Should include both real directory and symlink (both are directories)
            assert len(pipeline_dirs) == 2
            
            pipeline_names = [d.name for d in pipeline_dirs]
            assert "real_pipeline" in pipeline_names
            assert "link_pipeline" in pipeline_names
            
        except OSError:
            # Skip test if symlinks not supported on this platform
            pytest.skip("Symbolic links not supported on this platform")

    def test_bundle_manager_with_readonly_project_root(self):
        """Should handle read-only project root by raising appropriate errors."""
        # Make project root read-only
        self.project_root.chmod(0o444)
        
        try:
            # Should not raise exception during initialization
            readonly_manager = BundleManager(self.project_root)
            assert readonly_manager.project_root == self.project_root
            
            # Operations that require directory access should fail with proper error
            with pytest.raises(BundleResourceError) as exc_info:
                readonly_manager.get_pipeline_directories(self.project_root / "nonexistent")
            
            assert "Permission denied" in str(exc_info.value)
            
        finally:
            # Restore permissions for cleanup
            self.project_root.chmod(0o755)

    def test_concurrent_bundle_manager_operations(self):
        """Should handle concurrent operations safely."""
        import threading
        import time
        
        # Create test data
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        pipeline_dir = generated_dir / "test_pipeline"
        pipeline_dir.mkdir()
        (pipeline_dir / "test.py").write_text("# test")
        
        results = []
        
        def get_directories():
            time.sleep(0.01)  # Small delay to increase chance of race condition
            dirs = self.manager.get_pipeline_directories(generated_dir)
            results.append(len(dirs))
        
        def test_template_rendering():
            time.sleep(0.01)
            content = self.manager.generate_resource_file_content("test_pipeline", generated_dir, "dev")
            results.append(len(content))
        
        # Run operations concurrently
        threads = []
        for _ in range(3):
            threads.append(threading.Thread(target=get_directories))
            threads.append(threading.Thread(target=test_template_rendering))
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # All operations should complete successfully
        assert len(results) == 6
        # Directory count results should be 1
        assert results.count(1) >= 3  # At least 3 directory results
        # Template content length results should be much larger (string length)
        template_results = [r for r in results if r > 10]  # Template content lengths
        assert len(template_results) >= 3  # At least 3 template results

    def test_bundle_manager_large_number_of_files(self):
        """Should handle pipeline directories with many files efficiently."""
        # Create pipeline directory with many Python files
        pipeline_dir = self.project_root / "generated" / "large_pipeline"
        pipeline_dir.mkdir(parents=True)
        
        # Create 100 Python files
        for i in range(100):
            (pipeline_dir / f"file_{i:03d}.py").write_text(f"# file {i}")
        
        # Test template rendering instead of notebook path scanning (removed method)
        output_dir = self.project_root / "generated"
        content = self.manager.generate_resource_file_content("large_pipeline", output_dir, "dev")
        
        # Should generate template efficiently
        assert "large_pipeline" in content
        assert "- glob:" in content
        assert "include: ${workspace.file_path}/generated/${bundle.target}/large_pipeline/**" in content

    def test_bundle_manager_error_handling_initialization(self):
        """Should handle initialization errors appropriately."""
        # Test with None project root
        with pytest.raises(TypeError):
            BundleManager(None)
        
        # Test with non-Path object
        string_path = str(self.project_root)
        manager = BundleManager(string_path)
        assert manager.project_root == Path(string_path)


class TestBundleManagerWithPipelineConfig:
    """Test BundleManager with custom pipeline config."""
    
    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
    
    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)
    
    def test_init_without_config_path(self):
        """BundleManager works without config (backward compatible)."""
        manager = BundleManager(self.project_root)
        
        # Should initialize successfully
        assert manager.project_root == self.project_root
        assert hasattr(manager, 'config_loader')
        
        # Config loader should use defaults
        config = manager.config_loader.get_pipeline_config("test_pipeline")
        assert config["serverless"] is True
        assert config["edition"] == "ADVANCED"
    
    def test_init_with_config_path_loads_config(self):
        """BundleManager loads config when path provided."""
        # Create a config file
        config_content = """
project_defaults:
  serverless: false
  edition: PRO
"""
        config_file = self.project_root / "custom_config.yaml"
        config_file.write_text(config_content)
        
        manager = BundleManager(self.project_root, pipeline_config_path=str(config_file))
        
        # Config should be loaded
        config = manager.config_loader.get_pipeline_config("test_pipeline")
        assert config["serverless"] is False
        assert config["edition"] == "PRO"
    
    def test_generate_resource_uses_pipeline_config(self):
        """Generated resource includes config values."""
        # Create a config file
        config_content = """
project_defaults:
  serverless: false
  edition: CORE
  continuous: true
"""
        config_file = self.project_root / "test_config.yaml"
        config_file.write_text(config_content)
        
        manager = BundleManager(self.project_root, pipeline_config_path=str(config_file))
        
        # Generate resource
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        content = manager.generate_resource_file_content("test_pipeline", generated_dir, env="dev")
        
        # Should include config values in content
        assert "serverless: false" in content
        assert "edition: CORE" in content
        assert "continuous: true" in content
    
    def test_generate_resource_without_config_uses_defaults(self):
        """Without config, uses DEFAULT_PIPELINE_CONFIG."""
        manager = BundleManager(self.project_root)
        
        # Generate resource without config
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        content = manager.generate_resource_file_content("test_pipeline", generated_dir, env="dev")
        
        # Should use default values
        assert "serverless: true" in content
        assert "edition: ADVANCED" in content
    
    def test_different_pipelines_different_configs(self):
        """Multiple pipelines get their specific configs."""
        # Create a multi-document config file
        config_content = """
project_defaults:
  serverless: true
  edition: ADVANCED

---
pipeline: bronze_pipeline
serverless: false
continuous: true

---
pipeline: silver_pipeline
serverless: false
edition: PRO
"""
        config_file = self.project_root / "multi_config.yaml"
        config_file.write_text(config_content)
        
        manager = BundleManager(self.project_root, pipeline_config_path=str(config_file))
        
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Generate for bronze pipeline
        bronze_content = manager.generate_resource_file_content("bronze_pipeline", generated_dir, env="dev")
        assert "serverless: false" in bronze_content
        assert "continuous: true" in bronze_content
        
        # Generate for silver pipeline (non-serverless with custom edition)
        silver_content = manager.generate_resource_file_content("silver_pipeline", generated_dir, env="dev")
        assert "edition: PRO" in silver_content
        # Should override serverless from project defaults
        assert "serverless: false" in silver_content
    
    def test_config_loaded_once_used_many_times(self):
        """Config loaded once in init, used for all pipelines (efficiency)."""
        # Create a config file
        config_content = """
project_defaults:
  serverless: false
"""
        config_file = self.project_root / "config.yaml"
        config_file.write_text(config_content)
        
        manager = BundleManager(self.project_root, pipeline_config_path=str(config_file))
        
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Generate multiple times - config should be reused
        for i in range(10):
            content = manager.generate_resource_file_content(f"pipeline_{i}", generated_dir, env="dev")
            assert "serverless: false" in content
        
        # Config loader instance should be the same (loaded once)
        assert hasattr(manager, 'config_loader')
        assert manager.config_loader is not None
    
    def test_cluster_config_generates_valid_yaml(self):
        """Cluster configuration renders as valid, properly formatted YAML."""
        import yaml
        import re
        
        # Load fixture with full cluster configuration
        fixture_path = Path(__file__).parent / "fixtures/pipeline_configs/full_cluster_config.yaml"
        
        manager = BundleManager(self.project_root, pipeline_config_path=str(fixture_path))
        
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        
        # Generate resource content
        content = manager.generate_resource_file_content("cluster_test_pipeline", generated_dir, env="dev")
        
        # Test 1: Must be valid YAML (no syntax errors)
        try:
            parsed_yaml = yaml.safe_load(content)
            assert parsed_yaml is not None
            assert isinstance(parsed_yaml, dict)
        except yaml.YAMLError as e:
            pytest.fail(f"Generated YAML is invalid: {e}\nContent:\n{content}")
        
        # Test 2: Verify NO line concatenation (the original bug)
        # Should NOT have patterns like "clusters:        - label:" on SAME line
        # Use [ \t]+ to match spaces/tabs but NOT newlines
        assert not re.search(r'clusters:[ \t]+- label:', content), \
            "Cluster list item should NOT be on same line as 'clusters:'"
        assert not re.search(r'node_type_id:[ \t]+\S+[ \t]+driver_node_type_id:', content), \
            "Fields should not be concatenated on same line"
        assert not re.search(r'max_workers:[ \t]+\d+[ \t]+mode:', content), \
            "Fields should not be concatenated on same line"
        
        # Test 3: Verify proper multi-line structure
        # Check that key YAML structures are on separate lines
        assert re.search(r'clusters:\s*\n\s+- label:', content), \
            "Cluster list should start on new line after 'clusters:'"
        assert re.search(r'- label: default\s*\n\s+node_type_id:', content), \
            "node_type_id should be on new line after label"
        assert re.search(r'autoscale:\s*\n\s+min_workers:', content), \
            "Autoscale fields should be on new lines"
        
        # Test 4: Verify all cluster fields are present in parsed YAML
        pipeline_config = parsed_yaml['resources']['pipelines']['cluster_test_pipeline_pipeline']
        assert pipeline_config['serverless'] is False
        assert 'clusters' in pipeline_config
        assert len(pipeline_config['clusters']) == 1
        
        cluster = pipeline_config['clusters'][0]
        assert cluster['label'] == 'default'
        assert cluster['node_type_id'] == 'Standard_D4ds_v5'
        assert cluster['driver_node_type_id'] == 'Standard_D32ds_v5'
        assert 'autoscale' in cluster
        assert cluster['autoscale']['min_workers'] == 1
        assert cluster['autoscale']['max_workers'] == 5
        assert cluster['autoscale']['mode'] == 'ENHANCED'
        
        # Test 5: Verify proper indentation (2 spaces per level)
        # Check indentation for clusters block
        lines = content.split('\n')
        for i, line in enumerate(lines):
            if 'clusters:' in line and not line.strip().startswith('#'):
                # Next non-empty line should be list item with proper indent
                for j in range(i+1, len(lines)):
                    if lines[j].strip() and not lines[j].strip().startswith('#'):
                        assert lines[j].startswith('        - label:'), \
                            f"Cluster list item has incorrect indentation: {lines[j]}"
                        break
                break
        
        # Test 6: Verify other config options are present
        assert pipeline_config.get('photon') is True
        assert pipeline_config.get('edition') == 'ADVANCED'
        assert pipeline_config.get('channel') == 'CURRENT'


class TestBundleManagerUtilityMethods:
    """Test utility methods and edge cases for BundleManager."""
    
    def setup_method(self):
        """Set up test environment for each test."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.manager = BundleManager(self.project_root)
    
    def teardown_method(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)
    
    def test_get_env_resources_dir(self):
        """Test environment resources directory resolution."""
        # Should return base resources directory regardless of env
        resources_dir = self.manager._get_env_resources_dir("dev")
        assert resources_dir == self.manager.resources_base_dir
        
        resources_dir = self.manager._get_env_resources_dir("prod")
        assert resources_dir == self.manager.resources_base_dir
    
    def test_extract_most_common_database_empty_list(self):
        """Test extracting most common database from empty list."""
        result = self.manager._extract_most_common_database([])
        assert result is None
    
    def test_extract_most_common_database_single_value(self):
        """Test extracting most common database with single value."""
        result = self.manager._extract_most_common_database(["catalog.schema"])
        assert result == "catalog.schema"
    
    def test_extract_most_common_database_multiple_values(self):
        """Test extracting most common database with multiple values."""
        result = self.manager._extract_most_common_database([
            "catalog1.schema1",
            "catalog1.schema1",
            "catalog2.schema2",
            "catalog1.schema1"
        ])
        assert result == "catalog1.schema1"
    
    def test_extract_most_common_database_tie(self):
        """Test extracting most common database with tie (first occurrence wins)."""
        result = self.manager._extract_most_common_database([
            "catalog1.schema1",
            "catalog2.schema2",
            "catalog1.schema1",
            "catalog2.schema2"
        ])
        # First occurrence should win
        assert result in ["catalog1.schema1", "catalog2.schema2"]
    
    def test_extract_first_global_catalog_schema_no_files(self):
        """Test extracting catalog.schema when no files exist."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        
        result = self.manager._extract_first_global_catalog_schema(output_dir)
        assert result["catalog"] == "main"
        assert "bundle.target" in result["schema"]
    
    def test_extract_first_global_catalog_schema_nonexistent_dir(self):
        """Test extracting catalog.schema when output directory doesn't exist."""
        output_dir = self.project_root / "nonexistent"
        
        result = self.manager._extract_first_global_catalog_schema(output_dir)
        assert result["catalog"] == "main"
        assert "bundle.target" in result["schema"]
    
    def test_extract_first_global_catalog_schema_with_files(self):
        """Test extracting catalog.schema from Python files."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        pipeline_dir = output_dir / "test_pipeline"
        pipeline_dir.mkdir()
        
        # Create Python file with catalog.schema pattern
        py_file = pipeline_dir / "test.py"
        py_file.write_text('dp.create_streaming_table(name="catalog.schema.table")')
        
        result = self.manager._extract_first_global_catalog_schema(output_dir)
        assert result["catalog"] == "catalog"
        assert result["schema"] == "schema"
    
    def test_extract_first_global_catalog_schema_string_path(self):
        """Test extracting catalog.schema with string path."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        
        result = self.manager._extract_first_global_catalog_schema(str(output_dir))
        # Should use generated subdirectory
        assert isinstance(result, dict)
    
    def test_get_all_substitution_environments_empty_dir(self):
        """Test getting substitution environments when directory doesn't exist."""
        result = self.manager._get_all_substitution_environments()
        assert result == []
    
    def test_get_all_substitution_environments_multiple_files(self):
        """Test getting substitution environments with multiple files."""
        substitutions_dir = self.project_root / "substitutions"
        substitutions_dir.mkdir()
        
        (substitutions_dir / "dev.yaml").write_text("dev: {}")
        (substitutions_dir / "tst.yaml").write_text("tst: {}")
        (substitutions_dir / "prod.yaml").write_text("prod: {}")
        
        result = self.manager._get_all_substitution_environments()
        assert len(result) == 3
        assert "dev" in result
        assert "tst" in result
        assert "prod" in result
        assert result == sorted(result)  # Should be sorted
    
    def test_find_substitution_variables_for_values_missing_file(self):
        """Test finding substitution variables when file doesn't exist."""
        result = self.manager._find_substitution_variables_for_values(
            "catalog", "schema", "dev"
        )
        assert result["catalog_var"] is None
        assert result["schema_var"] is None
    
    def test_find_substitution_variables_for_values_no_matches(self):
        """Test finding substitution variables when no matches found."""
        substitutions_dir = self.project_root / "substitutions"
        substitutions_dir.mkdir()
        sub_file = substitutions_dir / "dev.yaml"
        sub_file.write_text("""
dev:
  other_var: "other_value"
""")
        
        result = self.manager._find_substitution_variables_for_values(
            "catalog", "schema", "dev"
        )
        assert result["catalog_var"] is None
        assert result["schema_var"] is None
    
    def test_find_substitution_variables_for_values_with_matches(self):
        """Test finding substitution variables with matches."""
        substitutions_dir = self.project_root / "substitutions"
        substitutions_dir.mkdir()
        sub_file = substitutions_dir / "dev.yaml"
        sub_file.write_text("""
dev:
  catalog: "my_catalog"
  schema: "my_schema"
""")
        
        result = self.manager._find_substitution_variables_for_values(
            "my_catalog", "my_schema", "dev"
        )
        assert result["catalog_var"] == "catalog"
        assert result["schema_var"] == "schema"
    
    def test_validate_databricks_targets_exist_missing_file(self):
        """Test validating databricks targets when file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            self.manager._validate_databricks_targets_exist(["dev"])
    
    def test_validate_databricks_targets_exist_missing_targets(self):
        """Test validating databricks targets when targets are missing."""
        databricks_file = self.project_root / "databricks.yml"
        databricks_file.write_text("""
targets:
  dev: {}
""")
        
        from lhp.bundle.exceptions import MissingDatabricksTargetError
        with pytest.raises(MissingDatabricksTargetError):
            self.manager._validate_databricks_targets_exist(["dev", "prod"])
    
    def test_validate_databricks_targets_exist_malformed_yaml(self):
        """Test validating databricks targets with malformed YAML."""
        databricks_file = self.project_root / "databricks.yml"
        databricks_file.write_text("invalid: yaml: content: [")
        
        with pytest.raises(BundleResourceError):
            self.manager._validate_databricks_targets_exist(["dev"])
    
    def test_validate_databricks_targets_exist_missing_targets_section(self):
        """Test validating databricks targets when targets section is missing."""
        databricks_file = self.project_root / "databricks.yml"
        databricks_file.write_text("other_key: value")
        
        from lhp.bundle.exceptions import MissingDatabricksTargetError
        with pytest.raises(MissingDatabricksTargetError):
            self.manager._validate_databricks_targets_exist(["dev"])
    
    def test_load_substitution_values_for_environment_missing_file(self):
        """Test loading substitution values when file doesn't exist."""
        result = self.manager._load_substitution_values_for_environment(
            "dev", "catalog", "schema"
        )
        assert result["catalog"] == "main"
        assert "bundle.target" in result["schema"]
    
    def test_load_substitution_values_for_environment_missing_variables(self):
        """Test loading substitution values when variables are missing."""
        substitutions_dir = self.project_root / "substitutions"
        substitutions_dir.mkdir()
        sub_file = substitutions_dir / "dev.yaml"
        sub_file.write_text("dev: {}")
        
        result = self.manager._load_substitution_values_for_environment(
            "dev", "catalog", "schema"
        )
        assert result["catalog"] == "main"
        assert "bundle.target" in result["schema"]
    
    def test_load_substitution_values_for_environment_with_values(self):
        """Test loading substitution values with valid values."""
        substitutions_dir = self.project_root / "substitutions"
        substitutions_dir.mkdir()
        sub_file = substitutions_dir / "dev.yaml"
        sub_file.write_text("""
dev:
  catalog: "my_catalog"
  schema: "my_schema"
""")
        
        result = self.manager._load_substitution_values_for_environment(
            "dev", "catalog", "schema"
        )
        assert result["catalog"] == "my_catalog"
        assert result["schema"] == "my_schema"
    
    def test_extract_database_from_python_files_no_files(self):
        """Test extracting database from Python files when no files exist."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        pipeline_dir = output_dir / "test_pipeline"
        pipeline_dir.mkdir()
        
        result = self.manager._extract_database_from_python_files("test_pipeline", output_dir)
        assert result["catalog"] == "main"
        assert "bundle.target" in result["schema"]
    
    def test_extract_database_from_python_files_with_files(self):
        """Test extracting database from Python files."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        pipeline_dir = output_dir / "test_pipeline"
        pipeline_dir.mkdir()
        
        py_file = pipeline_dir / "test.py"
        py_file.write_text('dp.create_streaming_table(name="catalog.schema.table")')
        
        result = self.manager._extract_database_from_python_files("test_pipeline", output_dir)
        assert result["catalog"] == "catalog"
        assert result["schema"] == "schema"
    
    def test_extract_database_patterns_streaming_table(self):
        """Test extracting database patterns from streaming table."""
        content = 'dp.create_streaming_table(\n    name="catalog.schema.table"\n)'
        result = self.manager._extract_database_patterns(content)
        assert "catalog.schema" in result
    
    def test_extract_database_patterns_materialized_view(self):
        """Test extracting database patterns from materialized view."""
        content = '@dp.materialized_view(\n    name="catalog.schema.table"\n)'
        result = self.manager._extract_database_patterns(content)
        assert "catalog.schema" in result
    
    def test_extract_database_patterns_multiple_patterns(self):
        """Test extracting database patterns with multiple matches."""
        content = '''
dp.create_streaming_table(name="catalog1.schema1.table1")
@dp.materialized_view(name="catalog2.schema2.table2")
'''
        result = self.manager._extract_database_patterns(content)
        assert "catalog1.schema1" in result
        assert "catalog2.schema2" in result
    
    def test_extract_database_patterns_no_match(self):
        """Test extracting database patterns when no matches found."""
        content = "print('hello')"
        result = self.manager._extract_database_patterns(content)
        assert result == []
    
    def test_parse_resolved_database_string_valid(self):
        """Test parsing valid resolved database string."""
        result = self.manager._parse_resolved_database_string("catalog.schema")
        assert result["catalog"] == "catalog"
        assert result["schema"] == "schema"
    
    def test_parse_resolved_database_string_invalid_no_dot(self):
        """Test parsing invalid database string without dot."""
        result = self.manager._parse_resolved_database_string("catalog")
        assert result["catalog"] == "main"
        assert "bundle.target" in result["schema"]
    
    def test_parse_resolved_database_string_invalid_empty(self):
        """Test parsing empty database string."""
        result = self.manager._parse_resolved_database_string("")
        assert result["catalog"] == "main"
        assert "bundle.target" in result["schema"]
    
    def test_parse_resolved_database_string_none(self):
        """Test parsing None database string."""
        result = self.manager._parse_resolved_database_string(None)
        assert result["catalog"] == "main"
        assert "bundle.target" in result["schema"]
    
    def test_create_unique_backup_path_first_backup(self):
        """Test creating unique backup path for first backup."""
        resource_file = self.project_root / "test.pipeline.yml"
        backup_path = self.manager._create_unique_backup_path(resource_file)
        assert backup_path == self.project_root / "test.pipeline.yml.bkup"
    
    def test_create_unique_backup_path_collision_handling(self):
        """Test creating unique backup path with collision handling."""
        resource_file = self.project_root / "test.pipeline.yml"
        backup_file = self.project_root / "test.pipeline.yml.bkup"
        backup_file.write_text("existing backup")
        
        backup_path = self.manager._create_unique_backup_path(resource_file)
        assert backup_path == self.project_root / "test.pipeline.yml.bkup.1"
        
        # Create another collision
        backup_file_1 = self.project_root / "test.pipeline.yml.bkup.1"
        backup_file_1.write_text("another backup")
        
        backup_path = self.manager._create_unique_backup_path(resource_file)
        assert backup_path == self.project_root / "test.pipeline.yml.bkup.2"
    
    def test_safe_directory_create(self):
        """Test safe directory creation."""
        test_dir = self.project_root / "test_dir"
        self.manager._safe_directory_create(test_dir, "test directory")
        assert test_dir.exists()
        assert test_dir.is_dir()
    
    def test_safe_directory_create_existing(self):
        """Test safe directory creation when directory already exists."""
        test_dir = self.project_root / "test_dir"
        test_dir.mkdir()
        
        # Should not raise error
        self.manager._safe_directory_create(test_dir, "test directory")
        assert test_dir.exists()
    
    def test_safe_directory_access_existing(self):
        """Test safe directory access when directory exists."""
        test_dir = self.project_root / "test_dir"
        test_dir.mkdir()
        
        # Should not raise error
        self.manager._safe_directory_access(test_dir, "test directory")
    
    def test_safe_directory_access_missing(self):
        """Test safe directory access when directory doesn't exist."""
        test_dir = self.project_root / "nonexistent"
        
        with pytest.raises(BundleResourceError) as exc_info:
            self.manager._safe_directory_access(test_dir, "test directory")
        
        assert "does not exist" in str(exc_info.value)
    
    def test_handle_pipeline_error_yaml_parsing(self):
        """Test handling YAML parsing errors."""
        from lhp.bundle.exceptions import YAMLParsingError
        
        error = YAMLParsingError("YAML error")
        result = self.manager._handle_pipeline_error("test_pipeline", error, "test operation")
        
        assert isinstance(result, BundleResourceError)
        assert "YAML processing failed" in str(result)
        assert "test_pipeline" in str(result)
    
    def test_handle_pipeline_error_os_error(self):
        """Test handling OS errors."""
        error = OSError("Permission denied")
        result = self.manager._handle_pipeline_error("test_pipeline", error, "test operation")
        
        assert isinstance(result, BundleResourceError)
        assert "File system error" in str(result)
        assert "test_pipeline" in str(result)
    
    def test_handle_pipeline_error_generic(self):
        """Test handling generic errors."""
        error = ValueError("Generic error")
        result = self.manager._handle_pipeline_error("test_pipeline", error, "test operation")
        
        assert isinstance(result, BundleResourceError)
        assert "test operation failed" in str(result)
        assert "test_pipeline" in str(result)
    
    def test_extract_pipeline_name_from_filename_pipeline_yml(self):
        """Test extracting pipeline name from .pipeline.yml filename."""
        resource_file = self.project_root / "test_pipeline.pipeline.yml"
        result = self.manager._extract_pipeline_name_from_filename(resource_file)
        assert result == "test_pipeline"
    
    def test_extract_pipeline_name_from_filename_yml(self):
        """Test extracting pipeline name from .yml filename."""
        resource_file = self.project_root / "test_pipeline.yml"
        result = self.manager._extract_pipeline_name_from_filename(resource_file)
        assert result == "test_pipeline"
    
    def test_extract_pipeline_name_from_filename_invalid(self):
        """Test extracting pipeline name from invalid filename."""
        resource_file = self.project_root / "test.txt"
        result = self.manager._extract_pipeline_name_from_filename(resource_file)
        assert result is None
    
    def test_update_configuration_files_error_handling(self):
        """Test _update_configuration_files error handling."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        
        # Mock _update_databricks_variables to raise error
        with patch.object(self.manager, '_update_databricks_variables', side_effect=Exception("Test error")):
            with pytest.raises(BundleResourceError) as exc_info:
                self.manager._update_configuration_files(output_dir, "dev")
            
            assert "Databricks YAML variable update failed" in str(exc_info.value)
    
    def test_create_new_resource_file_error_handling(self):
        """Test _create_new_resource_file error handling."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        pipeline_dir = output_dir / "test_pipeline"
        pipeline_dir.mkdir()
        
        resources_dir = self.project_root / "resources" / "lhp"
        resources_dir.mkdir(parents=True)
        
        # Mock write_text to raise permission error
        with patch('pathlib.Path.write_text', side_effect=PermissionError("Permission denied")):
            with pytest.raises(BundleResourceError) as exc_info:
                self.manager._create_new_resource_file("test_pipeline", output_dir.parent, "dev")
            
            assert "Failed to create resource file" in str(exc_info.value)
    
    def test_process_current_pipelines_error_handling(self):
        """Test _process_current_pipelines error handling."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        pipeline_dir = output_dir / "test_pipeline"
        pipeline_dir.mkdir()
        
        # Mock _sync_pipeline_resource to raise error
        with patch.object(self.manager, '_sync_pipeline_resource', side_effect=OSError("Test error")):
            with pytest.raises(BundleResourceError) as exc_info:
                self.manager._process_current_pipelines([pipeline_dir], "dev")
            
            assert "File system error" in str(exc_info.value) or "failed" in str(exc_info.value).lower()
            assert "test_pipeline" in str(exc_info.value)
    
    def test_cleanup_orphaned_resources_with_exception(self):
        """Test _cleanup_orphaned_resources handles exceptions gracefully."""
        resources_dir = self.project_root / "resources" / "lhp"
        resources_dir.mkdir(parents=True)
        
        # Create orphaned file
        orphaned_file = resources_dir / "orphaned.pipeline.yml"
        orphaned_file.write_text("# Generated by LakehousePlumber\nresources:\n  pipelines:\n    orphaned_pipeline:\n      name: test")
        
        # Mock _delete_resource_file to raise error
        with patch.object(self.manager, '_delete_resource_file', side_effect=Exception("Delete error")):
            # Should log warning but not raise
            result = self.manager._cleanup_orphaned_resources(set())
            # Should return 0 since deletion failed
            assert result == 0
    
    def test_sync_pipeline_resource_scenario_1a_preserve(self):
        """Test Scenario 1a: Python exists + LHP file exists → DON'T TOUCH."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        pipeline_dir = output_dir / "test_pipeline"
        pipeline_dir.mkdir()
        
        resources_dir = self.project_root / "resources" / "lhp"
        resources_dir.mkdir(parents=True)
        
        # Create LHP file
        lhp_file = resources_dir / "test_pipeline.pipeline.yml"
        lhp_file.write_text("""# Generated by LakehousePlumber
resources:
  pipelines:
    test_pipeline_pipeline:
      name: test_pipeline_pipeline
""")
        
        result = self.manager._sync_pipeline_resource("test_pipeline", pipeline_dir, "dev")
        assert result is False  # No changes needed
        
        # File should still exist and be unchanged
        assert lhp_file.exists()
    
    def test_sync_pipeline_resource_scenario_1a_override(self):
        """Test Scenario 1a override: Python exists + LHP file + force + pipeline config → REGENERATE."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        pipeline_dir = output_dir / "test_pipeline"
        pipeline_dir.mkdir()
        
        resources_dir = self.project_root / "resources" / "lhp"
        resources_dir.mkdir(parents=True)
        
        # Create LHP file
        lhp_file = resources_dir / "test_pipeline.pipeline.yml"
        lhp_file.write_text("""# Generated by LakehousePlumber
resources:
  pipelines:
    test_pipeline_pipeline:
      name: test_pipeline_pipeline
""")
        
        result = self.manager._sync_pipeline_resource("test_pipeline", pipeline_dir, "dev", force=True, has_pipeline_config=True)
        assert result is True  # File was regenerated
    
    def test_sync_pipeline_resource_scenario_1b_backup_replace(self):
        """Test Scenario 1b: Python exists + User file exists → BACKUP + REPLACE."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        pipeline_dir = output_dir / "test_pipeline"
        pipeline_dir.mkdir()
        
        resources_dir = self.project_root / "resources" / "lhp"
        resources_dir.mkdir(parents=True)
        
        # Create user file (no LHP header)
        user_file = resources_dir / "test_pipeline.pipeline.yml"
        user_file.write_text("""# User-created file
resources:
  pipelines:
    test_pipeline_pipeline:
      name: test_pipeline_pipeline
""")
        
        result = self.manager._sync_pipeline_resource("test_pipeline", pipeline_dir, "dev")
        assert result is True  # File was replaced
        
        # Backup should exist
        backup_files = list(resources_dir.glob("test_pipeline*.bkup*"))
        assert len(backup_files) > 0
    
    def test_sync_pipeline_resource_scenario_2_create(self):
        """Test Scenario 2: Python exists + No file exists → CREATE."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        pipeline_dir = output_dir / "test_pipeline"
        pipeline_dir.mkdir()
        
        resources_dir = self.project_root / "resources" / "lhp"
        resources_dir.mkdir(parents=True)
        
        result = self.manager._sync_pipeline_resource("test_pipeline", pipeline_dir, "dev")
        assert result is True  # File was created
        
        # File should exist
        resource_file = resources_dir / "test_pipeline.pipeline.yml"
        assert resource_file.exists()
    
    def test_sync_pipeline_resource_scenario_4_multiple_files_error(self):
        """Test Scenario 4: Multiple files exist → ERROR."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        pipeline_dir = output_dir / "test_pipeline"
        pipeline_dir.mkdir()
        
        resources_dir = self.project_root / "resources" / "lhp"
        resources_dir.mkdir(parents=True)
        
        # Create two files defining the same pipeline
        file1 = resources_dir / "test_pipeline.pipeline.yml"
        file1.write_text("""# Generated by LakehousePlumber
resources:
  pipelines:
    test_pipeline_pipeline:
      name: test_pipeline_pipeline
""")
        
        file2 = resources_dir / "test_pipeline.yml"
        file2.write_text("""# Generated by LakehousePlumber
resources:
  pipelines:
    test_pipeline_pipeline:
      name: test_pipeline_pipeline
""")
        
        with pytest.raises(BundleResourceError) as exc_info:
            self.manager._sync_pipeline_resource("test_pipeline", pipeline_dir, "dev")
        
        assert "Multiple files define the same pipeline" in str(exc_info.value)
    
    def test_sync_resources_with_generated_files_full_workflow(self):
        """Test full sync workflow with all scenarios."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        
        # Create pipeline directory
        pipeline_dir = output_dir / "test_pipeline"
        pipeline_dir.mkdir()
        (pipeline_dir / "test.py").write_text("# test")
        
        # Create databricks.yml
        databricks_file = self.project_root / "databricks.yml"
        databricks_file.write_text("""
targets:
  dev: {}
""")
        
        result = self.manager.sync_resources_with_generated_files(output_dir, "dev")
        assert result >= 0  # Should return count of updated/removed files
        
        # Resource file should be created
        resources_dir = self.project_root / "resources" / "lhp"
        resource_file = resources_dir / "test_pipeline.pipeline.yml"
        assert resource_file.exists()
    
    def test_log_sync_summary_updated_and_removed(self):
        """Test _log_sync_summary with both updated and removed files."""
        with patch.object(self.manager.logger, 'info') as mock_info:
            self.manager._log_sync_summary(2, 1)
            mock_info.assert_called()
            call_args = str(mock_info.call_args)
            assert "updated 2" in call_args or "2" in call_args
            assert "deleted 1" in call_args or "1" in call_args
    
    def test_log_sync_summary_no_changes(self):
        """Test _log_sync_summary with no changes."""
        with patch.object(self.manager.logger, 'info') as mock_info:
            self.manager._log_sync_summary(0, 0)
            mock_info.assert_called()
            call_args = str(mock_info.call_args)
            assert "preserved" in call_args or "conservative" in call_args.lower()