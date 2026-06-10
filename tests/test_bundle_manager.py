"""Tests for BundleManager core functionality."""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from lhp.bundle.exceptions import BundleResourceError
from lhp.bundle.manager import BundleManager


class TestBundleManagerCore:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.manager = BundleManager(self.project_root)

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def test_bundle_manager_initialization(self):
        """Should initialize with correct project root and resources directory."""
        assert self.manager.project_root == self.project_root
        assert self.manager.resources_dir == self.project_root / "resources" / "lhp"

    def test_bundle_manager_creates_resources_directory(self):
        """Should create resources/lhp directory if it doesn't exist."""
        assert not (self.project_root / "resources" / "lhp").exists()

        self.manager.ensure_resources_directory()

        assert (self.project_root / "resources" / "lhp").exists()
        assert (self.project_root / "resources" / "lhp").is_dir()
        assert (self.project_root / "resources").exists()
        assert (self.project_root / "resources").is_dir()

    def test_bundle_manager_resources_directory_already_exists(self):
        """Should handle existing resources/lhp directory gracefully."""
        resources_lhp_dir = self.project_root / "resources" / "lhp"
        resources_lhp_dir.mkdir(parents=True)

        self.manager.ensure_resources_directory()

        assert (self.project_root / "resources" / "lhp").exists()
        assert (self.project_root / "resources").exists()

    def test_get_pipeline_directories_with_multiple_pipelines(self):
        """Should correctly identify pipeline directories in generated/."""
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()

        (generated_dir / "raw_ingestions").mkdir()
        (generated_dir / "bronze_load").mkdir()
        (generated_dir / "silver_load").mkdir()

        (generated_dir / "readme.txt").write_text("not a directory")

        pipeline_dirs = self.manager.get_pipeline_directories(generated_dir)

        assert len(pipeline_dirs) == 3

        pipeline_names = [d.name for d in pipeline_dirs]
        assert "raw_ingestions" in pipeline_names
        assert "bronze_load" in pipeline_names
        assert "silver_load" in pipeline_names

    def test_get_pipeline_directories_returns_sorted_order(self):
        """Should return pipeline directories in sorted order for deterministic processing."""
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()

        (generated_dir / "pipeline_3").mkdir()
        (generated_dir / "pipeline_1").mkdir()
        (generated_dir / "pipeline_2").mkdir()
        (generated_dir / "aaa_first").mkdir()
        (generated_dir / "zzz_last").mkdir()

        pipeline_dirs = self.manager.get_pipeline_directories(generated_dir)

        assert len(pipeline_dirs) == 5

        pipeline_names = [d.name for d in pipeline_dirs]
        expected_order = [
            "aaa_first",
            "pipeline_1",
            "pipeline_2",
            "pipeline_3",
            "zzz_last",
        ]
        assert pipeline_names == expected_order

    def test_get_pipeline_directories_empty_generated(self):
        """Should return empty list when generated directory is empty."""
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
        resource_path = self.manager.get_resource_file_path("raw_ingestions")

        expected_path = (
            self.project_root / "resources" / "lhp" / "raw_ingestions.pipeline.yml"
        )
        assert resource_path == expected_path

    def test_resource_file_path_generation_with_special_characters(self):
        """Should handle pipeline names with special characters."""
        resource_path = self.manager.get_resource_file_path("bronze_layer_v2")

        expected_path = (
            self.project_root / "resources" / "lhp" / "bronze_layer_v2.pipeline.yml"
        )
        assert resource_path == expected_path

    def test_bundle_manager_logging(self):
        """Should initialize logger correctly."""
        assert hasattr(self.manager, "logger")
        assert self.manager.logger.name == "lhp.bundle.manager"


class TestBundleManagerFileOperations:
    """Test file operations and edge cases for BundleManager."""

    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.manager = BundleManager(self.project_root)

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def test_get_pipeline_directories_with_permission_error(self):
        """Should handle permission errors gracefully."""
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()

        pipeline_dir = generated_dir / "restricted_pipeline"
        pipeline_dir.mkdir()
        pipeline_dir.chmod(0o000)

        try:
            pipeline_dirs = self.manager.get_pipeline_directories(generated_dir)
            assert isinstance(pipeline_dirs, list)

        finally:
            pipeline_dir.chmod(0o755)

    def test_get_pipeline_directories_with_symbolic_links(self):
        """Should handle symbolic links appropriately."""
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()

        real_pipeline = generated_dir / "real_pipeline"
        real_pipeline.mkdir()

        try:
            link_pipeline = generated_dir / "link_pipeline"
            link_pipeline.symlink_to(real_pipeline)

            pipeline_dirs = self.manager.get_pipeline_directories(generated_dir)

            assert len(pipeline_dirs) == 2

            pipeline_names = [d.name for d in pipeline_dirs]
            assert "real_pipeline" in pipeline_names
            assert "link_pipeline" in pipeline_names

        except OSError:
            pytest.skip("Symbolic links not supported on this platform")

    def test_bundle_manager_with_readonly_project_root(self):
        """Should handle read-only project root by raising appropriate errors."""
        self.project_root.chmod(0o444)

        try:
            readonly_manager = BundleManager(self.project_root)
            assert readonly_manager.project_root == self.project_root

            with pytest.raises(BundleResourceError) as exc_info:
                readonly_manager.get_pipeline_directories(
                    self.project_root / "nonexistent"
                )

            assert exc_info.value.code == "LHP-CFG-020"
            assert isinstance(exc_info.value.original_error, PermissionError)
            assert "Permission denied" in exc_info.value.details

        finally:
            self.project_root.chmod(0o755)

    def test_concurrent_bundle_manager_operations(self):
        """Should handle concurrent operations safely."""
        import threading
        import time

        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        pipeline_dir = generated_dir / "test_pipeline"
        pipeline_dir.mkdir()
        (pipeline_dir / "test.py").write_text("# test")

        config_file = self.project_root / "pipeline_config.yaml"
        config_file.write_text(
            "project_defaults:\n"
            "  catalog: test_catalog\n"
            "  schema: test_schema\n"
            "  serverless: true\n"
        )
        self.manager = BundleManager(
            self.project_root, pipeline_config_path=str(config_file)
        )

        results = []

        def get_directories():
            time.sleep(0.01)  # Small delay to increase chance of race condition
            dirs = self.manager.get_pipeline_directories(generated_dir)
            results.append(len(dirs))

        def test_template_rendering():
            time.sleep(0.01)
            content = self.manager.generate_resource_file_content(
                "test_pipeline", generated_dir, "dev"
            )
            results.append(len(content))

        threads = []
        for _ in range(3):
            threads.append(threading.Thread(target=get_directories))
            threads.append(threading.Thread(target=test_template_rendering))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        assert len(results) == 6
        assert results.count(1) >= 3
        template_results = [r for r in results if r > 10]
        assert len(template_results) >= 3

    def test_bundle_manager_large_number_of_files(self):
        """Should handle pipeline directories with many files efficiently."""
        pipeline_dir = self.project_root / "generated" / "large_pipeline"
        pipeline_dir.mkdir(parents=True)

        for i in range(100):
            (pipeline_dir / f"file_{i:03d}.py").write_text(f"# file {i}")

        config_file = self.project_root / "pipeline_config.yaml"
        config_file.write_text(
            "project_defaults:\n"
            "  catalog: test_catalog\n"
            "  schema: test_schema\n"
            "  serverless: true\n"
        )
        manager = BundleManager(
            self.project_root, pipeline_config_path=str(config_file)
        )

        output_dir = self.project_root / "generated"
        content = manager.generate_resource_file_content(
            "large_pipeline", output_dir, "dev"
        )

        assert "large_pipeline" in content
        assert "- glob:" in content
        assert (
            "include: ${workspace.file_path}/generated/${bundle.target}/large_pipeline/**"
            in content
        )

    def test_bundle_manager_error_handling_initialization(self):
        """Should handle initialization errors appropriately."""
        from lhp.errors import LHPConfigError

        with pytest.raises(LHPConfigError, match="project_root cannot be None"):
            BundleManager(None)

        string_path = str(self.project_root)
        manager = BundleManager(string_path)
        assert manager.project_root == Path(string_path)


class TestBundleManagerWithPipelineConfig:
    """Test BundleManager with custom pipeline config."""

    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def test_init_without_config_path(self):
        """BundleManager works without config (backward compatible)."""
        manager = BundleManager(self.project_root)

        assert manager.project_root == self.project_root
        assert hasattr(manager, "config_loader")

        config = manager.config_loader.get_pipeline_config("test_pipeline")
        assert config["serverless"] is True
        assert config["edition"] == "ADVANCED"

    def test_init_with_config_path_loads_config(self):
        """BundleManager loads config when path provided."""
        config_content = """
project_defaults:
  serverless: false
  edition: PRO
"""
        config_file = self.project_root / "custom_config.yaml"
        config_file.write_text(config_content)

        manager = BundleManager(
            self.project_root, pipeline_config_path=str(config_file)
        )

        config = manager.config_loader.get_pipeline_config("test_pipeline")
        assert config["serverless"] is False
        assert config["edition"] == "PRO"

    def test_generate_resource_uses_pipeline_config(self):
        """Generated resource includes config values."""
        config_content = """
project_defaults:
  catalog: test_catalog
  schema: test_schema
  serverless: false
  edition: CORE
  continuous: true
"""
        config_file = self.project_root / "test_config.yaml"
        config_file.write_text(config_content)

        manager = BundleManager(
            self.project_root, pipeline_config_path=str(config_file)
        )

        # Generate resource
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        content = manager.generate_resource_file_content(
            "test_pipeline", generated_dir, env="dev"
        )

        assert "serverless: false" in content
        assert "edition: CORE" in content
        assert "continuous: true" in content

    def test_generate_resource_without_config_uses_defaults(self):
        """Without config, uses DEFAULT_PIPELINE_CONFIG (catalog/schema still required)."""
        # Even when "without config" semantics apply, catalog/schema must be
        # defined for fail-fast contract. Use a minimal config that supplies only
        # the required keys so we still observe DEFAULT_PIPELINE_CONFIG values.
        config_content = """
project_defaults:
  catalog: test_catalog
  schema: test_schema
"""
        config_file = self.project_root / "test_config.yaml"
        config_file.write_text(config_content)
        manager = BundleManager(
            self.project_root, pipeline_config_path=str(config_file)
        )

        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        content = manager.generate_resource_file_content(
            "test_pipeline", generated_dir, env="dev"
        )

        assert "serverless: true" in content
        assert "edition: ADVANCED" in content

    def test_different_pipelines_different_configs(self):
        """Multiple pipelines get their specific configs."""
        config_content = """
project_defaults:
  catalog: test_catalog
  schema: test_schema
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

        manager = BundleManager(
            self.project_root, pipeline_config_path=str(config_file)
        )

        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()

        bronze_content = manager.generate_resource_file_content(
            "bronze_pipeline", generated_dir, env="dev"
        )
        assert "serverless: false" in bronze_content
        assert "continuous: true" in bronze_content

        silver_content = manager.generate_resource_file_content(
            "silver_pipeline", generated_dir, env="dev"
        )
        assert "edition: PRO" in silver_content
        # Should override serverless from project defaults
        assert "serverless: false" in silver_content

    def test_config_loaded_once_used_many_times(self):
        """Config loaded once in init, used for all pipelines (efficiency)."""
        config_content = """
project_defaults:
  catalog: test_catalog
  schema: test_schema
  serverless: false
"""
        config_file = self.project_root / "config.yaml"
        config_file.write_text(config_content)

        manager = BundleManager(
            self.project_root, pipeline_config_path=str(config_file)
        )

        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()

        for i in range(10):
            content = manager.generate_resource_file_content(
                f"pipeline_{i}", generated_dir, env="dev"
            )
            assert "serverless: false" in content

        assert hasattr(manager, "config_loader")
        assert manager.config_loader is not None

    def test_cluster_config_generates_valid_yaml(self):
        """Cluster configuration renders as valid, properly formatted YAML."""
        import re

        import yaml

        # Load fixture with full cluster configuration
        fixture_path = (
            Path(__file__).parent / "fixtures/pipeline_configs/full_cluster_config.yaml"
        )

        manager = BundleManager(
            self.project_root, pipeline_config_path=str(fixture_path)
        )

        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()

        # Generate resource content
        content = manager.generate_resource_file_content(
            "cluster_test_pipeline", generated_dir, env="dev"
        )

        try:
            parsed_yaml = yaml.safe_load(content)
            assert parsed_yaml is not None
            assert isinstance(parsed_yaml, dict)
        except yaml.YAMLError as e:
            pytest.fail(f"Generated YAML is invalid: {e}\nContent:\n{content}")

        # Use [ \t]+ to match spaces/tabs but NOT newlines — guards against line concatenation bug
        assert not re.search(r"clusters:[ \t]+- label:", content), (
            "Cluster list item should NOT be on same line as 'clusters:'"
        )
        assert not re.search(
            r"node_type_id:[ \t]+\S+[ \t]+driver_node_type_id:", content
        ), "Fields should not be concatenated on same line"
        assert not re.search(r"max_workers:[ \t]+\d+[ \t]+mode:", content), (
            "Fields should not be concatenated on same line"
        )

        assert re.search(r"clusters:\s*\n\s+- label:", content), (
            "Cluster list should start on new line after 'clusters:'"
        )
        assert re.search(r"- label: default\s*\n\s+node_type_id:", content), (
            "node_type_id should be on new line after label"
        )
        assert re.search(r"autoscale:\s*\n\s+min_workers:", content), (
            "Autoscale fields should be on new lines"
        )

        pipeline_config = parsed_yaml["resources"]["pipelines"][
            "cluster_test_pipeline_pipeline"
        ]
        assert pipeline_config["serverless"] is False
        assert "clusters" in pipeline_config
        assert len(pipeline_config["clusters"]) == 1

        cluster = pipeline_config["clusters"][0]
        assert cluster["label"] == "default"
        assert cluster["node_type_id"] == "Standard_D4ds_v5"
        assert cluster["driver_node_type_id"] == "Standard_D32ds_v5"
        assert "autoscale" in cluster
        assert cluster["autoscale"]["min_workers"] == 1
        assert cluster["autoscale"]["max_workers"] == 5
        assert cluster["autoscale"]["mode"] == "ENHANCED"

        lines = content.split("\n")
        for i, line in enumerate(lines):
            if "clusters:" in line and not line.strip().startswith("#"):
                for j in range(i + 1, len(lines)):
                    if lines[j].strip() and not lines[j].strip().startswith("#"):
                        assert lines[j].startswith("        - label:"), (
                            f"Cluster list item has incorrect indentation: {lines[j]}"
                        )
                        break
                break

        assert pipeline_config.get("photon") is True
        assert pipeline_config.get("edition") == "ADVANCED"
        assert pipeline_config.get("channel") == "CURRENT"


class TestBundleManagerUtilityMethods:
    """Test utility methods and edge cases for BundleManager."""

    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
        self.manager = BundleManager(self.project_root)

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def test_safe_directory_create(self):
        test_dir = self.project_root / "test_dir"
        self.manager._safe_directory_create(test_dir, "test directory")
        assert test_dir.exists()
        assert test_dir.is_dir()

    def test_safe_directory_create_existing(self):
        test_dir = self.project_root / "test_dir"
        test_dir.mkdir()

        self.manager._safe_directory_create(test_dir, "test directory")
        assert test_dir.exists()

    def test_safe_directory_access_existing(self):
        test_dir = self.project_root / "test_dir"
        test_dir.mkdir()

        self.manager._safe_directory_access(test_dir, "test directory")

    def test_safe_directory_access_missing(self):
        test_dir = self.project_root / "nonexistent"

        with pytest.raises(BundleResourceError) as exc_info:
            self.manager._safe_directory_access(test_dir, "test directory")

        assert "does not exist" in str(exc_info.value)

    def test_handle_pipeline_error_os_error(self):
        error = OSError("Permission denied")
        result = self.manager._handle_pipeline_error(
            "test_pipeline", error, "test operation"
        )

        assert isinstance(result, BundleResourceError)
        assert "File system error" in str(result)
        assert "test_pipeline" in str(result)

    def test_handle_pipeline_error_generic(self):
        error = ValueError("Generic error")
        result = self.manager._handle_pipeline_error(
            "test_pipeline", error, "test operation"
        )

        assert isinstance(result, BundleResourceError)
        assert "test operation failed" in str(result)
        assert "test_pipeline" in str(result)

    def test_create_new_resource_file_error_handling(self):
        """Test _create_new_resource_file error handling."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        pipeline_dir = output_dir / "test_pipeline"
        pipeline_dir.mkdir()

        resources_dir = self.project_root / "resources" / "lhp"
        resources_dir.mkdir(parents=True)

        config_file = self.project_root / "pipeline_config.yaml"
        config_file.write_text(
            "project_defaults:\n  catalog: test_catalog\n  schema: test_schema\n"
        )
        manager = BundleManager(
            self.project_root, pipeline_config_path=str(config_file)
        )

        with patch(
            "pathlib.Path.write_text", side_effect=PermissionError("Permission denied")
        ):
            with pytest.raises(BundleResourceError) as exc_info:
                manager._create_new_resource_file(
                    "test_pipeline", output_dir.parent, "dev"
                )

            assert "Failed to create resource file" in str(exc_info.value)

    def test_process_current_pipelines_error_handling(self):
        """Test _process_current_pipelines error handling."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()
        pipeline_dir = output_dir / "test_pipeline"
        pipeline_dir.mkdir()

        with patch.object(
            self.manager, "_sync_pipeline_resource", side_effect=OSError("Test error")
        ):
            with pytest.raises(BundleResourceError) as exc_info:
                self.manager._process_current_pipelines([pipeline_dir], "dev")

            assert (
                "File system error" in str(exc_info.value)
                or "failed" in str(exc_info.value).lower()
            )
            assert "test_pipeline" in str(exc_info.value)

    def test_sync_resources_with_generated_files_full_workflow(self):
        """Test full sync workflow."""
        output_dir = self.project_root / "generated"
        output_dir.mkdir()

        pipeline_dir = output_dir / "test_pipeline"
        pipeline_dir.mkdir()
        (pipeline_dir / "test.py").write_text("# test")

        databricks_file = self.project_root / "databricks.yml"
        databricks_file.write_text("""
targets:
  dev: {}
""")

        config_file = self.project_root / "pipeline_config.yaml"
        config_file.write_text(
            "project_defaults:\n  catalog: test_catalog\n  schema: test_schema\n"
        )
        manager = BundleManager(
            self.project_root, pipeline_config_path=str(config_file)
        )

        result = manager.sync_resources_with_generated_files(output_dir, "dev")
        assert result >= 0

        resources_dir = self.project_root / "resources" / "lhp"
        resource_file = resources_dir / "test_pipeline.pipeline.yml"
        assert resource_file.exists()


class TestBundleManagerPermissionsAndPassthrough:
    """Template rendering for permissions + unknown-key pass-through.

    Mirrors the job_config pass-through pattern (see tests/test_job_generator.py).
    """

    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def _render(self, config_body: str) -> str:
        config_file = self.project_root / "pipeline_config.yaml"
        config_file.write_text(config_body)
        manager = BundleManager(
            self.project_root, pipeline_config_path=str(config_file)
        )
        generated_dir = self.project_root / "generated"
        generated_dir.mkdir()
        return manager.generate_resource_file_content(
            "test_pipeline", generated_dir, env="dev"
        )

    def _pipeline_block(self, content: str) -> dict:
        import yaml

        parsed = yaml.safe_load(content)
        return parsed["resources"]["pipelines"]["test_pipeline_pipeline"]

    def test_permissions_with_user_name_renders(self):
        content = self._render("""
project_defaults:
  catalog: test_catalog
  schema: test_schema
  serverless: true
  permissions:
    - level: CAN_MANAGE
      user_name: admin@example.com
""")
        permissions = self._pipeline_block(content)["permissions"]
        assert permissions == [
            {"level": "CAN_MANAGE", "user_name": "admin@example.com"}
        ]

    def test_permissions_with_group_name_renders(self):
        content = self._render("""
project_defaults:
  catalog: test_catalog
  schema: test_schema
  serverless: true
  permissions:
    - level: CAN_VIEW
      group_name: data-engineers
""")
        permissions = self._pipeline_block(content)["permissions"]
        assert permissions == [{"level": "CAN_VIEW", "group_name": "data-engineers"}]

    def test_permissions_with_service_principal_name_renders(self):
        content = self._render("""
project_defaults:
  catalog: test_catalog
  schema: test_schema
  serverless: true
  permissions:
    - level: CAN_MANAGE_RUN
      service_principal_name: 2aa4ed8e-0a18-4072-97c6-9c074c8be40d
""")
        permissions = self._pipeline_block(content)["permissions"]
        assert permissions == [
            {
                "level": "CAN_MANAGE_RUN",
                "service_principal_name": "2aa4ed8e-0a18-4072-97c6-9c074c8be40d",
            }
        ]

    def test_permissions_mixed_identity_types_renders_all(self):
        content = self._render("""
project_defaults:
  catalog: test_catalog
  schema: test_schema
  serverless: true
  permissions:
    - level: CAN_MANAGE
      user_name: admin@example.com
    - level: CAN_VIEW
      group_name: data-engineers
    - level: CAN_MANAGE_RUN
      service_principal_name: 2aa4ed8e-0a18-4072-97c6-9c074c8be40d
""")
        permissions = self._pipeline_block(content)["permissions"]
        assert len(permissions) == 3
        assert permissions[0] == {
            "level": "CAN_MANAGE",
            "user_name": "admin@example.com",
        }
        assert permissions[1] == {"level": "CAN_VIEW", "group_name": "data-engineers"}
        assert permissions[2] == {
            "level": "CAN_MANAGE_RUN",
            "service_principal_name": "2aa4ed8e-0a18-4072-97c6-9c074c8be40d",
        }

    def test_no_permissions_block_when_absent(self):
        content = self._render("""
project_defaults:
  catalog: test_catalog
  schema: test_schema
  serverless: true
  edition: ADVANCED
""")
        block = self._pipeline_block(content)
        assert "permissions" not in block
        # Template may include commented-out examples — only those are allowed.
        for line in content.splitlines():
            if "permissions:" in line:
                assert line.lstrip().startswith("#"), (
                    f"Unexpected active 'permissions:' line: {line!r}"
                )

    def test_run_as_renders_via_passthrough(self):
        content = self._render("""
project_defaults:
  catalog: test_catalog
  schema: test_schema
  serverless: true
  run_as:
    service_principal_name: 2aa4ed8e-0a18-4072-97c6-9c074c8be40d
""")
        block = self._pipeline_block(content)
        assert block["run_as"] == {
            "service_principal_name": "2aa4ed8e-0a18-4072-97c6-9c074c8be40d"
        }

    def test_unknown_scalar_key_renders_via_passthrough(self):
        """An arbitrary new Databricks API field flows through as-is."""
        content = self._render("""
project_defaults:
  catalog: test_catalog
  schema: test_schema
  serverless: true
  some_future_api_field: hello-world
""")
        block = self._pipeline_block(content)
        assert block["some_future_api_field"] == "hello-world"

    def test_passthrough_does_not_duplicate_known_keys(self):
        """Explicit blocks and pass-through must not both emit the same key."""
        import yaml

        # serverless: false so the edition explicit block renders (it's gated
        # on `not serverless` — edition is ignored for serverless pipelines).
        content = self._render("""
project_defaults:
  catalog: test_catalog
  schema: test_schema
  serverless: false
  edition: ADVANCED
  tags:
    team: platform
  permissions:
    - level: CAN_MANAGE
      user_name: admin@example.com
""")
        parsed = yaml.safe_load(content)
        pipeline_block = parsed["resources"]["pipelines"]["test_pipeline_pipeline"]
        assert pipeline_block["edition"] == "ADVANCED"
        assert pipeline_block["tags"] == {"team": "platform"}
        assert pipeline_block["permissions"] == [
            {"level": "CAN_MANAGE", "user_name": "admin@example.com"}
        ]
        for key in ("edition:", "tags:", "permissions:"):
            active_hits = [
                line
                for line in content.splitlines()
                if key in line and not line.lstrip().startswith("#")
            ]
            assert len(active_hits) == 1, (
                f"Key {key!r} rendered {len(active_hits)} times (expected 1): "
                f"{active_hits}"
            )
