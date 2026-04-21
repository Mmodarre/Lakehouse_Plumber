"""End-to-end tests for pipeline configuration feature."""

import hashlib
import os
import re
import shutil
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestPipelineConfigE2E:
    """E2E tests for pipeline config with cluster configuration."""

    @pytest.fixture
    def testing_project(self, tmp_path):
        """Copy testing project fixture to temp directory."""
        fixture_dir = Path(__file__).parent / "fixtures" / "testing_project"
        project_dir = tmp_path / "test_project"
        shutil.copytree(fixture_dir, project_dir)
        return project_dir

    def test_generate_with_pipeline_config_non_serverless(self, testing_project):
        """Test lhp generate with non-serverless cluster configuration."""
        from lhp.bundle.manager import BundleManager

        # Create pipeline_config.yaml in config directory
        config_dir = testing_project / "config"
        config_dir.mkdir(exist_ok=True)

        pipeline_config_content = """
project_defaults:
  serverless: false
  edition: ADVANCED
  channel: CURRENT
  photon: true

---
pipeline: acmi_edw_raw
clusters:
  - label: default
    node_type_id: Standard_D16ds_v5
    driver_node_type_id: Standard_D32ds_v5
    autoscale:
      min_workers: 2
      max_workers: 10
      mode: ENHANCED
continuous: true

---
pipeline: acmi_edw_bronze
clusters:
  - label: default
    node_type_id: Standard_D8ds_v5
    autoscale:
      min_workers: 1
      max_workers: 5
      mode: ENHANCED
"""
        config_file = config_dir / "pipeline_config.yaml"
        config_file.write_text(pipeline_config_content)

        # Create bundle manager with pipeline config
        bundle_manager = BundleManager(
            testing_project, pipeline_config_path=str(config_file)
        )

        # Generate resource file content
        output_dir = testing_project / "generated" / "dev"
        output_dir.mkdir(parents=True, exist_ok=True)

        resource_content = bundle_manager.generate_resource_file_content(
            "acmi_edw_raw", output_dir, env="dev"
        )

        # Test 1: Generated YAML must be valid
        try:
            parsed_yaml = yaml.safe_load(resource_content)
            assert parsed_yaml is not None
        except yaml.YAMLError as e:
            pytest.fail(
                f"Generated resource YAML is invalid: {e}\n\nContent:\n{resource_content}"
            )

        # Test 2: Verify NO line concatenation (the bug this PR fixes)
        # Use [ \t]+ to match spaces/tabs but NOT newlines
        assert not re.search(
            r"clusters:[ \t]+- label:", resource_content
        ), "BUG: Cluster list item concatenated on same line as 'clusters:'"
        assert not re.search(
            r"node_type_id:[ \t]+\w+[ \t]+driver_node_type_id:", resource_content
        ), "BUG: Fields concatenated on same line"

        # Test 3: Verify proper YAML structure
        assert re.search(
            r"clusters:\s*\n\s+- label:", resource_content
        ), "Cluster list should start on new line"

        # Test 4: Verify cluster configuration is present
        pipeline_config = parsed_yaml["resources"]["pipelines"]["acmi_edw_raw_pipeline"]
        assert pipeline_config["serverless"] is False
        assert "clusters" in pipeline_config
        assert len(pipeline_config["clusters"]) == 1

        cluster = pipeline_config["clusters"][0]
        assert cluster["label"] == "default"
        assert cluster["node_type_id"] == "Standard_D16ds_v5"
        assert cluster["driver_node_type_id"] == "Standard_D32ds_v5"
        assert cluster["autoscale"]["min_workers"] == 2
        assert cluster["autoscale"]["max_workers"] == 10
        assert cluster["autoscale"]["mode"] == "ENHANCED"

        # Test 5: Verify other config options
        assert pipeline_config.get("continuous") is True
        assert pipeline_config.get("photon") is True
        assert pipeline_config.get("edition") == "ADVANCED"
        assert pipeline_config.get("channel") == "CURRENT"

    def test_generate_different_pipelines_different_clusters(self, testing_project):
        """Test multiple pipelines with different cluster configurations."""
        from lhp.bundle.manager import BundleManager

        # Create pipeline_config.yaml with different configs per pipeline
        config_dir = testing_project / "config"
        config_dir.mkdir(exist_ok=True)

        pipeline_config_content = """
project_defaults:
  serverless: false
  edition: PRO

---
pipeline: acmi_edw_raw
clusters:
  - label: default
    node_type_id: Standard_D16ds_v5
    autoscale:
      min_workers: 2
      max_workers: 10

---
pipeline: acmi_edw_bronze
clusters:
  - label: default
    node_type_id: Standard_D8ds_v5
    autoscale:
      min_workers: 1
      max_workers: 5
"""
        config_file = config_dir / "pipeline_config.yaml"
        config_file.write_text(pipeline_config_content)

        bundle_manager = BundleManager(
            testing_project, pipeline_config_path=str(config_file)
        )

        output_dir = testing_project / "generated" / "dev"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate for raw pipeline
        raw_content = bundle_manager.generate_resource_file_content(
            "acmi_edw_raw", output_dir, env="dev"
        )
        raw_yaml = yaml.safe_load(raw_content)
        raw_pipeline = raw_yaml["resources"]["pipelines"]["acmi_edw_raw_pipeline"]

        # Generate for bronze pipeline
        bronze_content = bundle_manager.generate_resource_file_content(
            "acmi_edw_bronze", output_dir, env="dev"
        )
        bronze_yaml = yaml.safe_load(bronze_content)
        bronze_pipeline = bronze_yaml["resources"]["pipelines"][
            "acmi_edw_bronze_pipeline"
        ]

        # Verify each has correct cluster config
        assert raw_pipeline["clusters"][0]["node_type_id"] == "Standard_D16ds_v5"
        assert raw_pipeline["clusters"][0]["autoscale"]["max_workers"] == 10

        assert bronze_pipeline["clusters"][0]["node_type_id"] == "Standard_D8ds_v5"
        assert bronze_pipeline["clusters"][0]["autoscale"]["max_workers"] == 5

        # Both should have project defaults applied
        assert raw_pipeline["edition"] == "PRO"
        assert bronze_pipeline["edition"] == "PRO"

    def test_comprehensive_cluster_config_matches_baseline(self, testing_project):
        """Test comprehensive cluster config with ALL options matches baseline."""
        from lhp.bundle.manager import BundleManager

        # Load comprehensive config fixture
        fixture_path = (
            Path(__file__).parent.parent
            / "fixtures"
            / "pipeline_configs"
            / "comprehensive_cluster_config.yaml"
        )
        assert (
            fixture_path.exists()
        ), f"Comprehensive config fixture should exist: {fixture_path}"

        bundle_manager = BundleManager(
            testing_project, pipeline_config_path=str(fixture_path)
        )

        output_dir = testing_project / "generated" / "dev"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate resource content
        generated_content = bundle_manager.generate_resource_file_content(
            "comprehensive_cluster_config", output_dir, env="dev"
        )

        # Load baseline
        baseline_path = (
            Path(__file__).parent
            / "fixtures"
            / "testing_project"
            / "resources_baseline"
            / "lhp"
            / "comprehensive_cluster_config.pipeline.yml"
        )
        assert baseline_path.exists(), f"Baseline should exist: {baseline_path}"
        baseline_content = baseline_path.read_text()

        # Parse both as YAML to verify validity
        generated_yaml = yaml.safe_load(generated_content)
        baseline_yaml = yaml.safe_load(baseline_content)

        # Verify both are valid
        assert generated_yaml is not None, "Generated YAML should parse successfully"
        assert baseline_yaml is not None, "Baseline YAML should parse successfully"

        # Compare structure (ignore comments)
        pipeline_config = generated_yaml["resources"]["pipelines"][
            "comprehensive_cluster_config_pipeline"
        ]
        baseline_config = baseline_yaml["resources"]["pipelines"][
            "comprehensive_cluster_config_pipeline"
        ]

        # Verify ALL configuration options are present and correct

        # 1. Non-serverless with clusters
        assert pipeline_config["serverless"] is False
        assert "clusters" in pipeline_config
        assert len(pipeline_config["clusters"]) == 1

        cluster = pipeline_config["clusters"][0]
        assert cluster["label"] == "default"
        assert cluster["node_type_id"] == "Standard_D16ds_v5"
        assert cluster["driver_node_type_id"] == "Standard_D32ds_v5"
        assert cluster["policy_id"] == "1234ABCD1234ABCD"
        assert cluster["autoscale"]["min_workers"] == 2
        assert cluster["autoscale"]["max_workers"] == 10
        assert cluster["autoscale"]["mode"] == "ENHANCED"

        # 2. Processing mode
        assert pipeline_config["continuous"] is True

        # 3. Compute settings
        assert pipeline_config["photon"] is True
        assert pipeline_config["edition"] == "ADVANCED"
        assert pipeline_config["channel"] == "PREVIEW"

        # 4. Notifications
        assert "notifications" in pipeline_config
        assert len(pipeline_config["notifications"]) == 1
        notification = pipeline_config["notifications"][0]
        assert "data-engineering@company.com" in notification["email_recipients"]
        assert "ops-team@company.com" in notification["email_recipients"]
        assert "on-update-failure" in notification["alerts"]
        assert "on-update-fatal-failure" in notification["alerts"]
        assert "on-update-success" in notification["alerts"]
        assert "on-flow-failure" in notification["alerts"]

        # 5. Tags
        assert "tags" in pipeline_config
        assert pipeline_config["tags"]["environment"] == "production"
        assert pipeline_config["tags"]["cost_center"] == "data_platform"
        assert pipeline_config["tags"]["criticality"] == "high"
        assert pipeline_config["tags"]["team"] == "data_engineering"

        # 6. Event log
        assert "event_log" in pipeline_config
        assert pipeline_config["event_log"]["name"] == "pipeline_event_log"
        assert pipeline_config["event_log"]["schema"] == "_meta"
        assert pipeline_config["event_log"]["catalog"] == "main"

        # Verify NO line concatenation (the bug we fixed)
        assert not re.search(r"clusters:[ \t]+- label:", generated_content)
        assert not re.search(
            r"notifications:[ \t]+- email_recipients:", generated_content
        )
        assert not re.search(r"tags:[ \t]+\w+:", generated_content)

        # Verify proper multi-line structure
        assert re.search(r"clusters:\s*\n\s+- label:", generated_content)
        assert re.search(
            r"notifications:\s*\n\s+- email_recipients:", generated_content
        )
        assert re.search(r"tags:\s*\n\s+\w+:", generated_content)

        # Deep comparison: generated should match baseline structure
        assert (
            pipeline_config == baseline_config
        ), "Generated config should match baseline structure exactly"

    def test_generate_with_instance_pool_cluster(self, testing_project):
        """Test full generation with instance pool config (no node_type_id)."""
        from lhp.bundle.manager import BundleManager

        config_dir = testing_project / "config"
        config_dir.mkdir(exist_ok=True)

        pipeline_config_content = """
project_defaults:
  serverless: false
  edition: ADVANCED

---
pipeline: acmi_edw_raw
clusters:
  - label: default
    instance_pool_id: 1010-095823-mud11-pool-i204xeov
    driver_instance_pool_id: driver-pool-456
    autoscale:
      min_workers: 1
      max_workers: 5
      mode: ENHANCED
"""
        config_file = config_dir / "pipeline_config.yaml"
        config_file.write_text(pipeline_config_content)

        bundle_manager = BundleManager(
            testing_project, pipeline_config_path=str(config_file)
        )

        output_dir = testing_project / "generated" / "dev"
        output_dir.mkdir(parents=True, exist_ok=True)

        resource_content = bundle_manager.generate_resource_file_content(
            "acmi_edw_raw", output_dir, env="dev"
        )

        # Must produce valid YAML
        try:
            parsed_yaml = yaml.safe_load(resource_content)
            assert parsed_yaml is not None
        except yaml.YAMLError as e:
            pytest.fail(
                f"Generated resource YAML is invalid: {e}\n\nContent:\n{resource_content}"
            )

        pipeline = parsed_yaml["resources"]["pipelines"]["acmi_edw_raw_pipeline"]

        # Verify cluster has instance_pool_id, not node_type_id
        assert "clusters" in pipeline
        cluster = pipeline["clusters"][0]
        assert cluster["instance_pool_id"] == "1010-095823-mud11-pool-i204xeov"
        assert cluster["driver_instance_pool_id"] == "driver-pool-456"
        assert "node_type_id" not in cluster

        # Verify no line concatenation
        assert not re.search(
            r"clusters:[ \t]+- label:", resource_content
        ), "BUG: Cluster list item concatenated on same line as 'clusters:'"

        # Verify proper multi-line structure
        assert re.search(r"clusters:\s*\n\s+- label:", resource_content)

    def test_generate_with_mixed_pool_and_node_type(self, testing_project):
        """Test one pipeline with instance pool, another with node_type_id."""
        from lhp.bundle.manager import BundleManager

        config_dir = testing_project / "config"
        config_dir.mkdir(exist_ok=True)

        pipeline_config_content = """
project_defaults:
  serverless: false
  edition: ADVANCED

---
pipeline: acmi_edw_raw
clusters:
  - label: default
    instance_pool_id: pool-abc-123
    autoscale:
      min_workers: 1
      max_workers: 5

---
pipeline: acmi_edw_bronze
clusters:
  - label: default
    node_type_id: Standard_D8ds_v5
    autoscale:
      min_workers: 1
      max_workers: 5
"""
        config_file = config_dir / "pipeline_config.yaml"
        config_file.write_text(pipeline_config_content)

        bundle_manager = BundleManager(
            testing_project, pipeline_config_path=str(config_file)
        )

        output_dir = testing_project / "generated" / "dev"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate for pool-based pipeline
        raw_content = bundle_manager.generate_resource_file_content(
            "acmi_edw_raw", output_dir, env="dev"
        )
        raw_yaml = yaml.safe_load(raw_content)
        raw_cluster = raw_yaml["resources"]["pipelines"]["acmi_edw_raw_pipeline"][
            "clusters"
        ][0]

        # Generate for node_type-based pipeline
        bronze_content = bundle_manager.generate_resource_file_content(
            "acmi_edw_bronze", output_dir, env="dev"
        )
        bronze_yaml = yaml.safe_load(bronze_content)
        bronze_cluster = bronze_yaml["resources"]["pipelines"][
            "acmi_edw_bronze_pipeline"
        ]["clusters"][0]

        # Raw pipeline: instance_pool_id, no node_type_id
        assert raw_cluster["instance_pool_id"] == "pool-abc-123"
        assert "node_type_id" not in raw_cluster

        # Bronze pipeline: node_type_id, no instance_pool_id
        assert bronze_cluster["node_type_id"] == "Standard_D8ds_v5"
        assert "instance_pool_id" not in bronze_cluster


@pytest.mark.e2e
class TestEnvironmentDependenciesE2E:
    """E2E tests for environment dependencies propagation in bundle resources."""

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        """Create isolated copy of fixture project for each test."""
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"
        self.baseline_dir = self.project_root / "resources_baseline" / "lhp"
        self.config_file = self.project_root / "config" / "pipeline_config.yaml"

        self._init_bundle_project()

        yield
        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        """Wipe and recreate working directories."""
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)

        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _uncomment_environment(self, pipeline_name):
        """Uncomment the #environment: block for a specific pipeline section."""
        content = self.config_file.read_text()
        lines = content.splitlines(keepends=True)
        result = []
        in_target_pipeline = False

        for line in lines:
            stripped = line.lstrip()
            # Detect pipeline section boundaries
            if stripped.startswith("pipeline:"):
                name = stripped.split(":", 1)[1].strip()
                in_target_pipeline = name == pipeline_name
            elif stripped.startswith("---"):
                in_target_pipeline = False

            # Uncomment #environment and its children in the target section
            if in_target_pipeline and stripped.startswith("#"):
                bare = stripped[1:]  # Remove leading #
                indent = len(line) - len(line.lstrip())
                result.append(" " * indent + bare)
            else:
                result.append(line)

        self.config_file.write_text("".join(result))

    def _uncomment_project_defaults_environment(self):
        """Uncomment the #environment: block inside project_defaults."""
        content = self.config_file.read_text()
        # The commented block uses 2-space indent under project_defaults
        content = content.replace(
            '  #environment:\n  #  dependencies:\n  #    - "msal=={msal_version}"',
            '  environment:\n    dependencies:\n      - "msal=={msal_version}"',
        )
        self.config_file.write_text(content)

    def _generate_resource(self, pipeline_name):
        """Generate resource file content via BundleManager."""
        from lhp.bundle.manager import BundleManager

        bm = BundleManager(
            self.project_root,
            pipeline_config_path=str(self.config_file),
        )
        return bm.generate_resource_file_content(
            pipeline_name, self.generated_dir, env="dev"
        )

    @staticmethod
    def _compare_file_hashes(file1, file2):
        """Compare two files by SHA-256. Returns '' if identical, error string if different."""

        def get_hash(f):
            return hashlib.sha256(f.read_bytes()).hexdigest()

        h1, h2 = get_hash(file1), get_hash(file2)
        if h1 != h2:
            return (
                f"Hash mismatch: {file1.name} "
                f"(generated={h1[:12]}) != (baseline={h2[:12]})"
            )
        return ""

    def _assert_matches_baseline(self, generated_content, baseline_name):
        """Write generated content to a temp file and compare against baseline."""
        gen_file = self.resources_dir / f"{baseline_name}.pipeline.yml"
        gen_file.write_text(generated_content)

        baseline_file = self.baseline_dir / f"{baseline_name}.pipeline.yml"
        assert baseline_file.exists(), f"Missing baseline: {baseline_file.name}"

        diff = self._compare_file_hashes(gen_file, baseline_file)
        assert diff == "", diff

    # ------------------------------------------------------------------
    # Test 1: Basic environment dependencies rendering
    # ------------------------------------------------------------------

    def test_environment_dependencies_rendered_in_resource(self):
        """Uncomment environment on env_deps_basic — output matches baseline."""
        self._uncomment_environment("env_deps_basic")
        content = self._generate_resource("env_deps_basic")

        # Structural verification
        parsed = yaml.safe_load(content)
        pipeline = parsed["resources"]["pipelines"]["env_deps_basic_pipeline"]
        assert pipeline["environment"]["dependencies"] == [
            "msal==1.31.0",
            "requests>=2.28.0",
        ]
        assert "libraries" in pipeline

        # Baseline hash comparison
        self._assert_matches_baseline(content, "env_deps_basic_with_env")

    # ------------------------------------------------------------------
    # Test 2: Substitution tokens resolved in environment dependencies
    # ------------------------------------------------------------------

    def test_environment_with_substitution_tokens(self):
        """Uncomment environment on env_deps_substitution — tokens resolved, matches baseline."""
        self._uncomment_environment("env_deps_substitution")
        content = self._generate_resource("env_deps_substitution")

        # Structural verification
        parsed = yaml.safe_load(content)
        pipeline = parsed["resources"]["pipelines"]["env_deps_substitution_pipeline"]
        assert pipeline["environment"]["dependencies"] == ["msal==1.31.0"]
        assert pipeline["catalog"] == "acme_edw_dev"

        # Baseline hash comparison
        self._assert_matches_baseline(content, "env_deps_substitution_with_env")

    # ------------------------------------------------------------------
    # Test 3: environment absent when not configured
    # ------------------------------------------------------------------

    def test_environment_absent_when_not_configured(self):
        """env_deps_basic with environment commented out — no environment in output, matches baseline."""
        # Do NOT uncomment — environment stays commented
        content = self._generate_resource("env_deps_basic")

        # Structural verification
        parsed = yaml.safe_load(content)
        pipeline = parsed["resources"]["pipelines"]["env_deps_basic_pipeline"]
        assert "environment" not in pipeline

        # Baseline hash comparison (default baseline without environment)
        self._assert_matches_baseline(content, "env_deps_basic")

    # ------------------------------------------------------------------
    # Test 4: project_defaults environment inheritance
    # ------------------------------------------------------------------

    def test_environment_with_project_defaults_inheritance(self):
        """Uncomment environment in project_defaults — env_deps_inherited gets it, matches baseline."""
        self._uncomment_project_defaults_environment()
        content = self._generate_resource("env_deps_inherited")

        # Structural verification
        parsed = yaml.safe_load(content)
        pipeline = parsed["resources"]["pipelines"]["env_deps_inherited_pipeline"]
        assert pipeline["environment"]["dependencies"] == ["msal==1.31.0"]

        # Baseline hash comparison
        self._assert_matches_baseline(content, "env_deps_inherited_with_env")

    # ------------------------------------------------------------------
    # Test 5: Existing baselines still match after template change
    # ------------------------------------------------------------------

    def test_existing_baselines_match_after_template_change(self):
        """Full CLI generation produces resource files matching updated baselines."""
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
        assert result.exit_code == 0, f"Generation should succeed:\n{result.output}"

        generated_files = sorted(self.resources_dir.glob("*.pipeline.yml"))
        assert len(generated_files) > 0, "Should have generated resource files"

        mismatches = []
        for gen_file in generated_files:
            baseline_file = self.baseline_dir / gen_file.name
            assert baseline_file.exists(), f"Missing baseline for {gen_file.name}"
            diff = self._compare_file_hashes(gen_file, baseline_file)
            if diff:
                mismatches.append(diff)

        if mismatches:
            detail = "\n".join(f"  {i}. {m}" for i, m in enumerate(mismatches, 1))
            assert False, (
                f"Resource files differ from baselines "
                f"({len(mismatches)} mismatches):\n{detail}"
            )


@pytest.mark.e2e
class TestConfigurationBlockE2E:
    """E2E tests for configuration block propagation in bundle resources."""

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        """Create isolated copy of fixture project for each test."""
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"
        self.baseline_dir = self.project_root / "resources_baseline" / "lhp"
        self.config_file = self.project_root / "config" / "pipeline_config.yaml"

        self._init_bundle_project()

        yield
        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        """Wipe and recreate working directories."""
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)

        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _uncomment_configuration(self, pipeline_name):
        """Uncomment the #configuration: block for a specific pipeline section."""
        content = self.config_file.read_text()
        lines = content.splitlines(keepends=True)
        result = []
        in_target_pipeline = False

        for line in lines:
            stripped = line.lstrip()
            if stripped.startswith("pipeline:"):
                name = stripped.split(":", 1)[1].strip()
                in_target_pipeline = name == pipeline_name
            elif stripped.startswith("---"):
                in_target_pipeline = False

            if in_target_pipeline and stripped.startswith("#"):
                bare = stripped[1:]  # Remove leading #
                indent = len(line) - len(line.lstrip())
                result.append(" " * indent + bare)
            else:
                result.append(line)

        self.config_file.write_text("".join(result))

    def _uncomment_project_defaults_configuration(self):
        """Uncomment the #configuration: block inside project_defaults."""
        content = self.config_file.read_text()
        content = content.replace(
            '  #configuration:\n  #  "pipelines.incompatibleViewCheck.enabled": "false"',
            '  configuration:\n    "pipelines.incompatibleViewCheck.enabled": "false"',
        )
        self.config_file.write_text(content)

    def _generate_resource(self, pipeline_name):
        """Generate resource file content via BundleManager."""
        from lhp.bundle.manager import BundleManager

        bm = BundleManager(
            self.project_root,
            pipeline_config_path=str(self.config_file),
        )
        return bm.generate_resource_file_content(
            pipeline_name, self.generated_dir, env="dev"
        )

    @staticmethod
    def _compare_file_hashes(file1, file2):
        """Compare two files by SHA-256."""

        def get_hash(f):
            return hashlib.sha256(f.read_bytes()).hexdigest()

        h1, h2 = get_hash(file1), get_hash(file2)
        if h1 != h2:
            return (
                f"Hash mismatch: {file1.name} "
                f"(generated={h1[:12]}) != (baseline={h2[:12]})"
            )
        return ""

    def _assert_matches_baseline(self, generated_content, baseline_name):
        """Write generated content to a temp file and compare against baseline."""
        gen_file = self.resources_dir / f"{baseline_name}.pipeline.yml"
        gen_file.write_text(generated_content)

        baseline_file = self.baseline_dir / f"{baseline_name}.pipeline.yml"
        assert baseline_file.exists(), f"Missing baseline: {baseline_file.name}"

        diff = self._compare_file_hashes(gen_file, baseline_file)
        assert diff == "", diff

    # ------------------------------------------------------------------
    # Test 1: Basic configuration entries rendering
    # ------------------------------------------------------------------

    def test_configuration_entries_rendered_in_resource(self):
        """Uncomment configuration on config_basic — output matches baseline."""
        self._uncomment_configuration("config_basic")
        content = self._generate_resource("config_basic")

        # Structural verification
        parsed = yaml.safe_load(content)
        pipeline = parsed["resources"]["pipelines"]["config_basic_pipeline"]
        config = pipeline["configuration"]
        assert config["bundle.sourcePath"] == (
            "${workspace.file_path}/generated/${bundle.target}"
        )
        assert config["pipelines.incompatibleViewCheck.enabled"] == "false"
        assert config["spark.databricks.delta.minFileSize"] == "134217728"

        # Baseline hash comparison
        self._assert_matches_baseline(content, "config_basic_with_config")

    # ------------------------------------------------------------------
    # Test 2: Substitution tokens resolved in configuration values
    # ------------------------------------------------------------------

    def test_configuration_with_substitution_tokens(self):
        """Uncomment configuration on config_substitution — tokens resolved."""
        self._uncomment_configuration("config_substitution")
        content = self._generate_resource("config_substitution")

        # Structural verification
        parsed = yaml.safe_load(content)
        pipeline = parsed["resources"]["pipelines"]["config_substitution_pipeline"]
        config = pipeline["configuration"]
        assert config["spark.databricks.delta.minFileSize"] == "134217728"
        assert pipeline["catalog"] == "acme_edw_dev"

        # Baseline hash comparison
        self._assert_matches_baseline(content, "config_substitution_with_config")

    # ------------------------------------------------------------------
    # Test 3: configuration absent when not configured
    # ------------------------------------------------------------------

    def test_configuration_absent_preserves_default(self):
        """config_basic with configuration commented out — only bundle.sourcePath."""
        content = self._generate_resource("config_basic")

        # Structural verification
        parsed = yaml.safe_load(content)
        pipeline = parsed["resources"]["pipelines"]["config_basic_pipeline"]
        config = pipeline["configuration"]
        assert config == {
            "bundle.sourcePath": ("${workspace.file_path}/generated/${bundle.target}")
        }

        # Baseline hash comparison (default baseline without configuration)
        self._assert_matches_baseline(content, "config_basic")

    # ------------------------------------------------------------------
    # Test 4: project_defaults configuration inheritance
    # ------------------------------------------------------------------

    def test_configuration_with_project_defaults_inheritance(self):
        """Uncomment configuration in project_defaults — config_inherited gets it."""
        self._uncomment_project_defaults_configuration()
        content = self._generate_resource("config_inherited")

        # Structural verification
        parsed = yaml.safe_load(content)
        pipeline = parsed["resources"]["pipelines"]["config_inherited_pipeline"]
        config = pipeline["configuration"]
        assert config["pipelines.incompatibleViewCheck.enabled"] == "false"

        # Baseline hash comparison
        self._assert_matches_baseline(content, "config_inherited_with_config")

    # ------------------------------------------------------------------
    # Test 5: Existing baselines still match after configuration change
    # ------------------------------------------------------------------

    def test_existing_baselines_match_after_configuration_change(self):
        """Full CLI generation produces resource files matching baselines."""
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
        assert result.exit_code == 0, f"Generation should succeed:\n{result.output}"

        generated_files = sorted(self.resources_dir.glob("*.pipeline.yml"))
        assert len(generated_files) > 0, "Should have generated resource files"

        mismatches = []
        for gen_file in generated_files:
            baseline_file = self.baseline_dir / gen_file.name
            assert baseline_file.exists(), f"Missing baseline for {gen_file.name}"
            diff = self._compare_file_hashes(gen_file, baseline_file)
            if diff:
                mismatches.append(diff)

        if mismatches:
            detail = "\n".join(f"  {i}. {m}" for i, m in enumerate(mismatches, 1))
            assert False, (
                f"Resource files differ from baselines "
                f"({len(mismatches)} mismatches):\n{detail}"
            )


@pytest.mark.e2e
class TestEventLogE2E:
    """E2E tests for declarative event_log configuration in lhp.yaml.

    Tests that event_log defined in lhp.yaml is automatically injected into
    pipeline resource files, with support for pipeline-level overrides and opt-out.
    """

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        """Create isolated copy of fixture project for each test."""
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"
        self.baseline_dir = self.project_root / "resources_baseline" / "lhp"
        self.config_file = self.project_root / "config" / "pipeline_config.yaml"
        self.lhp_yaml = self.project_root / "lhp.yaml"

        self._init_bundle_project()

        yield
        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        """Wipe and recreate working directories."""
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)

        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _uncomment_lhp_event_log(self):
        """Uncomment the #event_log: block in lhp.yaml."""
        content = self.lhp_yaml.read_text()
        content = content.replace(
            '#event_log:\n#  catalog: "{catalog}"\n#  schema: _meta\n'
            '#  name_suffix: "_event_log"',
            'event_log:\n  catalog: "{catalog}"\n  schema: _meta\n'
            '  name_suffix: "_event_log"',
        )
        self.lhp_yaml.write_text(content)

    def _uncomment_pipeline_event_log(self, pipeline_name):
        """Uncomment the #event_log: block for a specific pipeline section."""
        content = self.config_file.read_text()
        lines = content.splitlines(keepends=True)
        result = []
        in_target_pipeline = False

        for line in lines:
            stripped = line.lstrip()
            if stripped.startswith("pipeline:"):
                name = stripped.split(":", 1)[1].strip()
                in_target_pipeline = name == pipeline_name
            elif stripped.startswith("---"):
                in_target_pipeline = False

            if in_target_pipeline and stripped.startswith("#"):
                bare = stripped[1:]  # Remove leading #
                indent = len(line) - len(line.lstrip())
                result.append(" " * indent + bare)
            else:
                result.append(line)

        self.config_file.write_text("".join(result))

    def _generate_resource(self, pipeline_name):
        """Generate resource file content via BundleManager with project config."""
        from lhp.bundle.manager import BundleManager
        from lhp.core.project_config_loader import ProjectConfigLoader

        loader = ProjectConfigLoader(self.project_root)
        project_config = loader.load_project_config()

        bm = BundleManager(
            self.project_root,
            pipeline_config_path=str(self.config_file),
            project_config=project_config,
        )
        return bm.generate_resource_file_content(
            pipeline_name, self.generated_dir, env="dev"
        )

    @staticmethod
    def _compare_file_hashes(file1, file2):
        """Compare two files by SHA-256."""

        def get_hash(f):
            return hashlib.sha256(f.read_bytes()).hexdigest()

        h1, h2 = get_hash(file1), get_hash(file2)
        if h1 != h2:
            return (
                f"Hash mismatch: {file1.name} "
                f"(generated={h1[:12]}) != (baseline={h2[:12]})"
            )
        return ""

    def _assert_matches_baseline(self, generated_content, baseline_name):
        """Write generated content to a temp file and compare against baseline."""
        gen_file = self.resources_dir / f"{baseline_name}.pipeline.yml"
        gen_file.write_text(generated_content)

        baseline_file = self.baseline_dir / f"{baseline_name}.pipeline.yml"
        assert baseline_file.exists(), f"Missing baseline: {baseline_file.name}"

        diff = self._compare_file_hashes(gen_file, baseline_file)
        assert diff == "", diff

    # ------------------------------------------------------------------
    # Test 1: event_log injected from lhp.yaml project config
    # ------------------------------------------------------------------

    def test_event_log_injected_from_project_config(self):
        """Uncomment event_log in lhp.yaml — output contains event_log with resolved tokens."""
        self._uncomment_lhp_event_log()
        content = self._generate_resource("event_log_basic")

        # Structural verification
        parsed = yaml.safe_load(content)
        pipeline = parsed["resources"]["pipelines"]["event_log_basic_pipeline"]
        assert "event_log" in pipeline
        assert pipeline["event_log"]["name"] == "event_log_basic_event_log"
        assert pipeline["event_log"]["catalog"] == "acme_edw_dev"
        assert pipeline["event_log"]["schema"] == "_meta"

        # Baseline hash comparison
        self._assert_matches_baseline(content, "event_log_basic_with_event_log")

    # ------------------------------------------------------------------
    # Test 2: event_log absent when not configured in lhp.yaml
    # ------------------------------------------------------------------

    def test_event_log_absent_when_not_configured(self):
        """Leave event_log commented in lhp.yaml — no event_log in output."""
        # Do NOT uncomment — event_log stays commented in lhp.yaml
        content = self._generate_resource("event_log_basic")

        # Structural verification
        parsed = yaml.safe_load(content)
        pipeline = parsed["resources"]["pipelines"]["event_log_basic_pipeline"]
        assert "event_log" not in pipeline

        # Baseline hash comparison (default baseline without event_log)
        self._assert_matches_baseline(content, "event_log_basic")

    # ------------------------------------------------------------------
    # Test 3: pipeline_config event_log fully replaces project-level
    # ------------------------------------------------------------------

    def test_pipeline_config_overrides_project_event_log(self):
        """Uncomment both lhp.yaml and pipeline_config event_log — pipeline's own wins."""
        self._uncomment_lhp_event_log()
        self._uncomment_pipeline_event_log("event_log_override")
        content = self._generate_resource("event_log_override")

        # Structural verification — pipeline_config values should win
        parsed = yaml.safe_load(content)
        pipeline = parsed["resources"]["pipelines"]["event_log_override_pipeline"]
        assert "event_log" in pipeline
        assert pipeline["event_log"]["name"] == "custom_event_log"
        assert pipeline["event_log"]["catalog"] == "override_catalog"
        assert pipeline["event_log"]["schema"] == "override_schema"

        # Baseline hash comparison
        self._assert_matches_baseline(content, "event_log_override_with_event_log")

    # ------------------------------------------------------------------
    # Test 4: pipeline_config disables event_log with false
    # ------------------------------------------------------------------

    def test_pipeline_config_disables_event_log(self):
        """Uncomment lhp.yaml event_log + event_log: false in pipeline_config — no event_log."""
        self._uncomment_lhp_event_log()
        self._uncomment_pipeline_event_log("event_log_disabled")
        content = self._generate_resource("event_log_disabled")

        # Structural verification — event_log should be absent
        parsed = yaml.safe_load(content)
        pipeline = parsed["resources"]["pipelines"]["event_log_disabled_pipeline"]
        assert "event_log" not in pipeline

        # Baseline hash comparison (same as default — no event_log)
        self._assert_matches_baseline(content, "event_log_disabled")

    # ------------------------------------------------------------------
    # Test 5: backward compatibility — pipeline_config-only event_log
    # ------------------------------------------------------------------

    def test_backward_compat_pipeline_config_only_event_log(self):
        """Leave lhp.yaml commented — existing pipeline_config event_log still works."""
        # Use the comprehensive_cluster_config pipeline which already has
        # event_log defined in the fixtures pipeline_config
        from lhp.bundle.manager import BundleManager
        from lhp.core.project_config_loader import ProjectConfigLoader

        fixture_path = (
            Path(__file__).parent.parent
            / "fixtures"
            / "pipeline_configs"
            / "comprehensive_cluster_config.yaml"
        )
        assert fixture_path.exists(), f"Fixture should exist: {fixture_path}"

        loader = ProjectConfigLoader(self.project_root)
        project_config = loader.load_project_config()

        bm = BundleManager(
            self.project_root,
            pipeline_config_path=str(fixture_path),
            project_config=project_config,
        )
        content = bm.generate_resource_file_content(
            "comprehensive_cluster_config", self.generated_dir, env="dev"
        )

        # Structural verification — event_log from pipeline_config should be present
        parsed = yaml.safe_load(content)
        pipeline = parsed["resources"]["pipelines"][
            "comprehensive_cluster_config_pipeline"
        ]
        assert "event_log" in pipeline
        assert pipeline["event_log"]["name"] == "pipeline_event_log"
        assert pipeline["event_log"]["schema"] == "_meta"
        assert pipeline["event_log"]["catalog"] == "main"


@pytest.mark.e2e
class TestMonitoringPipelineE2E:
    """E2E tests for the event log monitoring pipeline.

    Validates that when monitoring: is configured in lhp.yaml alongside event_log:,
    lhp generate creates a synthetic monitoring pipeline that UNIONs all pipeline
    event log tables into a streaming table with materialized views.
    """

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        """Create isolated copy of fixture project for each test."""
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"
        self.resources_root = self.project_root / "resources"
        self.monitoring_dir = self.project_root / "monitoring" / "dev"
        self.generated_baseline_dir = self.project_root / "generated_baseline" / "dev"
        self.resources_baseline_dir = self.project_root / "resources_baseline" / "lhp"
        self.monitoring_baseline_dir = self.project_root / "monitoring_baseline"
        self.lhp_yaml = self.project_root / "lhp.yaml"

        self._init_bundle_project()

        yield
        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        """Wipe and recreate working directories."""
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)

        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Variant definitions: (commented_text, uncommented_text)
    # ------------------------------------------------------------------

    _CP = "/Volumes/${catalog}/_meta/checkpoints/event_logs"

    # job_config_path is required when monitoring is enabled — every variant
    # points at ``config/monitoring_job_config.yaml`` in the fixture project.
    # The ``monitoring_custom_job_config`` variant uses a richer file to
    # exercise deep-merge, schedule, notifications, and tags.
    _JC = "config/monitoring_job_config.yaml"
    _JC_CUSTOM = "config/monitoring_job_config_custom.yaml"

    MONITORING_VARIANTS = {
        "monitoring_default": (
            "#monitoring:\n"
            '#  checkpoint_path: "' + _CP + '"\n'
            '#  job_config_path: "' + _JC + '"',
            "monitoring:\n"
            '  checkpoint_path: "' + _CP + '"\n'
            '  job_config_path: "' + _JC + '"',
        ),
        "monitoring_custom_pipeline_name": (
            "#monitoring:\n"
            '#  pipeline_name: "my_custom_monitor"\n'
            '#  checkpoint_path: "' + _CP + '"\n'
            '#  job_config_path: "' + _JC + '"',
            "monitoring:\n"
            '  pipeline_name: "my_custom_monitor"\n'
            '  checkpoint_path: "' + _CP + '"\n'
            '  job_config_path: "' + _JC + '"',
        ),
        "monitoring_catalog_schema_override": (
            "#monitoring:\n"
            '#  catalog: "analytics_cat"\n'
            '#  schema: "_analytics"\n'
            '#  checkpoint_path: "' + _CP + '"\n'
            '#  job_config_path: "' + _JC + '"',
            "monitoring:\n"
            '  catalog: "analytics_cat"\n'
            '  schema: "_analytics"\n'
            '  checkpoint_path: "' + _CP + '"\n'
            '  job_config_path: "' + _JC + '"',
        ),
        "monitoring_custom_streaming_table": (
            "#monitoring:\n"
            '#  streaming_table: "unified_event_log"\n'
            '#  checkpoint_path: "' + _CP + '"\n'
            '#  job_config_path: "' + _JC + '"',
            "monitoring:\n"
            '  streaming_table: "unified_event_log"\n'
            '  checkpoint_path: "' + _CP + '"\n'
            '  job_config_path: "' + _JC + '"',
        ),
        "monitoring_custom_mvs": (
            "#monitoring:\n"
            '#  checkpoint_path: "' + _CP + '"\n'
            '#  job_config_path: "' + _JC + '"\n'
            "#  materialized_views:\n"
            '#    - name: "error_events"\n'
            '#      sql: "SELECT * FROM all_pipelines_event_log'
            " WHERE event_type = 'error'\"\n"
            '#    - name: "pipeline_latency"\n'
            '#      sql: "SELECT _source_pipeline, avg(duration_ms)'
            " as avg_duration FROM all_pipelines_event_log"
            ' GROUP BY _source_pipeline"',
            "monitoring:\n"
            '  checkpoint_path: "' + _CP + '"\n'
            '  job_config_path: "' + _JC + '"\n'
            "  materialized_views:\n"
            '    - name: "error_events"\n'
            '      sql: "SELECT * FROM all_pipelines_event_log'
            " WHERE event_type = 'error'\"\n"
            '    - name: "pipeline_latency"\n'
            '      sql: "SELECT _source_pipeline, avg(duration_ms)'
            " as avg_duration FROM all_pipelines_event_log"
            ' GROUP BY _source_pipeline"',
        ),
        "monitoring_no_mvs": (
            "#monitoring:\n"
            '#  checkpoint_path: "' + _CP + '"\n'
            '#  job_config_path: "' + _JC + '"\n'
            "#  materialized_views: []",
            "monitoring:\n"
            '  checkpoint_path: "' + _CP + '"\n'
            '  job_config_path: "' + _JC + '"\n'
            "  materialized_views: []",
        ),
        "monitoring_mv_sql_path": (
            "#monitoring:\n"
            '#  checkpoint_path: "' + _CP + '"\n'
            '#  job_config_path: "' + _JC + '"\n'
            "#  materialized_views:\n"
            '#    - name: "custom_analysis"\n'
            '#      sql_path: "sql/monitoring_custom_analysis.sql"',
            "monitoring:\n"
            '  checkpoint_path: "' + _CP + '"\n'
            '  job_config_path: "' + _JC + '"\n'
            "  materialized_views:\n"
            '    - name: "custom_analysis"\n'
            '      sql_path: "sql/monitoring_custom_analysis.sql"',
        ),
        "monitoring_enable_job_monitoring": (
            "#monitoring:\n"
            '#  checkpoint_path: "' + _CP + '"\n'
            '#  job_config_path: "' + _JC + '"\n'
            "#  enable_job_monitoring: true",
            "monitoring:\n"
            '  checkpoint_path: "' + _CP + '"\n'
            '  job_config_path: "' + _JC + '"\n'
            "  enable_job_monitoring: true",
        ),
        "monitoring_custom_job_config": (
            "#monitoring:\n"
            '#  checkpoint_path: "' + _CP + '"\n'
            '#  job_config_path: "' + _JC_CUSTOM + '"',
            "monitoring:\n"
            '  checkpoint_path: "' + _CP + '"\n'
            '  job_config_path: "' + _JC_CUSTOM + '"',
        ),
    }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _enable_event_log(self):
        """Uncomment the event_log block only."""
        content = self.lhp_yaml.read_text()
        content = content.replace(
            '#event_log:\n#  catalog: "{catalog}"\n#  schema: _meta\n'
            '#  name_suffix: "_event_log"',
            'event_log:\n  catalog: "{catalog}"\n  schema: _meta\n'
            '  name_suffix: "_event_log"',
        )
        self.lhp_yaml.write_text(content)

    def _enable_monitoring_variant(self, variant: str):
        """Uncomment a specific monitoring variant block."""
        commented, uncommented = self.MONITORING_VARIANTS[variant]
        content = self.lhp_yaml.read_text()
        content = content.replace(commented, uncommented)
        self.lhp_yaml.write_text(content)

    def _enable_event_log_and_monitoring(self):
        """Backward compat: enable event_log + default monitoring."""
        self._enable_event_log()
        self._enable_monitoring_variant("monitoring_default")

    def _run_generate(self):
        """Run lhp generate --env dev --force via CLI."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["--verbose", "generate", "--env", "dev", "--force"]
        )
        return result.exit_code, result.output

    @staticmethod
    def _compare_file_hashes(file1, file2):
        """Compare two files by SHA-256."""

        def get_hash(f):
            return hashlib.sha256(f.read_bytes()).hexdigest()

        h1, h2 = get_hash(file1), get_hash(file2)
        if h1 != h2:
            # Include first 50 lines of unified diff for debugging
            import difflib

            lines1 = file1.read_text().splitlines(keepends=True)
            lines2 = file2.read_text().splitlines(keepends=True)
            diff = list(
                difflib.unified_diff(
                    lines2, lines1, fromfile="baseline", tofile="generated", n=3
                )
            )
            diff_preview = "".join(diff[:50])
            return (
                f"Hash mismatch: {file1.name} "
                f"(generated={h1[:12]}) != (baseline={h2[:12]})\n"
                f"Diff preview:\n{diff_preview}"
            )
        return ""

    def _assert_monitoring_py_baseline(
        self, variant, pipeline_name="acme_edw_event_log_monitoring"
    ):
        """Hash-compare generated monitoring.py (DLT code) against variant baseline."""
        monitoring_py = self.generated_dir / pipeline_name / "monitoring.py"
        assert (
            monitoring_py.exists()
        ), f"Monitoring pipeline file not generated: {monitoring_py}"

        baseline_py = (
            self.monitoring_baseline_dir / variant / pipeline_name / "monitoring.py"
        )
        assert baseline_py.exists(), f"Missing baseline: {baseline_py}"

        diff = self._compare_file_hashes(monitoring_py, baseline_py)
        assert diff == "", f"Generated monitoring.py mismatch:\n{diff}"

    def _assert_monitoring_notebook_baseline(self, variant):
        """Hash-compare generated union_event_logs.py notebook against variant baseline."""
        notebook = self.monitoring_dir / "union_event_logs.py"
        assert notebook.exists(), f"Monitoring notebook not generated: {notebook}"

        baseline_nb = self.monitoring_baseline_dir / variant / "union_event_logs.py"
        assert baseline_nb.exists(), f"Missing notebook baseline: {baseline_nb}"

        diff = self._compare_file_hashes(notebook, baseline_nb)
        assert diff == "", f"Generated union_event_logs.py mismatch:\n{diff}"

    def _assert_monitoring_job_baseline(
        self, variant, pipeline_name="acme_edw_event_log_monitoring"
    ):
        """Hash-compare generated <pipeline>.job.yml against variant baseline."""
        job_yml = self.resources_root / f"{pipeline_name}.job.yml"
        assert job_yml.exists(), f"Monitoring job YAML not generated: {job_yml}"

        baseline_job = (
            self.monitoring_baseline_dir / variant / f"{pipeline_name}.job.yml"
        )
        assert baseline_job.exists(), f"Missing job baseline: {baseline_job}"

        diff = self._compare_file_hashes(job_yml, baseline_job)
        assert diff == "", f"Generated job YAML mismatch:\n{diff}"

    # ------------------------------------------------------------------
    # Test 1: monitoring absent — backward compatibility
    # ------------------------------------------------------------------

    def test_monitoring_absent_no_monitoring_pipeline(self):
        """Without monitoring: in lhp.yaml, no monitoring pipeline is generated."""
        # Do NOT enable monitoring — leave lhp.yaml as-is
        exit_code, output = self._run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        # Monitoring pipeline directory should NOT exist
        monitoring_dir = self.generated_dir / "acme_edw_event_log_monitoring"
        assert (
            not monitoring_dir.exists()
        ), f"Monitoring pipeline directory should not exist: {monitoring_dir}"

        # Monitoring resource file should NOT exist
        monitoring_resource = (
            self.resources_dir / "acme_edw_event_log_monitoring.pipeline.yml"
        )
        assert (
            not monitoring_resource.exists()
        ), f"Monitoring resource file should not exist: {monitoring_resource}"

    # ------------------------------------------------------------------
    # Test 2: monitoring enabled — full generation with hash baseline
    # ------------------------------------------------------------------

    def test_monitoring_enabled_generates_pipeline(self):
        """monitoring: {} with event_log generates monitoring pipeline matching baseline."""
        self._enable_event_log_and_monitoring()
        exit_code, output = self._run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        # Hash comparison for all three monitoring artifacts
        self._assert_monitoring_py_baseline("default")
        self._assert_monitoring_notebook_baseline("default")
        self._assert_monitoring_job_baseline("default")

    # ------------------------------------------------------------------
    # Test 3: monitoring resource has no event_log
    # ------------------------------------------------------------------

    def test_monitoring_resource_no_event_log(self):
        """Monitoring pipeline resource YAML has no event_log block."""
        self._enable_event_log_and_monitoring()
        exit_code, output = self._run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        # Verify monitoring resource file was generated
        monitoring_resource = (
            self.resources_dir / "acme_edw_event_log_monitoring.pipeline.yml"
        )
        assert (
            monitoring_resource.exists()
        ), f"Monitoring resource not generated: {monitoring_resource}"

        # Structural verification: no event_log in the resource
        parsed = yaml.safe_load(monitoring_resource.read_text())
        pipeline = parsed["resources"]["pipelines"][
            "acme_edw_event_log_monitoring_pipeline"
        ]
        assert (
            "event_log" not in pipeline
        ), "Monitoring pipeline resource should NOT have event_log"

        # Hash comparison with resource baseline
        baseline_resource = (
            self.resources_baseline_dir / "acme_edw_event_log_monitoring.pipeline.yml"
        )
        assert (
            baseline_resource.exists()
        ), f"Missing resource baseline: {baseline_resource}"

        diff = self._compare_file_hashes(monitoring_resource, baseline_resource)
        assert diff == "", f"Resource YAML mismatch:\n{diff}"

    # ------------------------------------------------------------------
    # Test 4: custom pipeline name
    # ------------------------------------------------------------------

    def test_monitoring_custom_pipeline_name(self):
        """Custom pipeline_name changes directory, PIPELINE_ID, and resource filename."""
        self._enable_event_log()
        self._enable_monitoring_variant("monitoring_custom_pipeline_name")
        exit_code, output = self._run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        # Structural: directory and PIPELINE_ID
        monitoring_py = self.generated_dir / "my_custom_monitor" / "monitoring.py"
        assert (
            monitoring_py.exists()
        ), f"Custom pipeline directory not created: {monitoring_py}"
        content = monitoring_py.read_text()
        assert 'PIPELINE_ID = "my_custom_monitor"' in content

        # Default pipeline directory should NOT exist
        default_dir = self.generated_dir / "acme_edw_event_log_monitoring"
        assert (
            not default_dir.exists()
        ), "Default monitoring dir should not exist with custom pipeline_name"

        # Resource YAML uses custom pipeline name
        resource_yml = self.resources_dir / "my_custom_monitor.pipeline.yml"
        assert (
            resource_yml.exists()
        ), f"Custom resource YAML not generated: {resource_yml}"
        parsed = yaml.safe_load(resource_yml.read_text())
        assert "my_custom_monitor_pipeline" in parsed["resources"]["pipelines"]

        # Hash comparison: Python baseline
        self._assert_monitoring_py_baseline(
            "custom_pipeline_name", pipeline_name="my_custom_monitor"
        )
        self._assert_monitoring_notebook_baseline("custom_pipeline_name")
        self._assert_monitoring_job_baseline(
            "custom_pipeline_name", pipeline_name="my_custom_monitor"
        )

        # Hash comparison: resource YAML baseline
        baseline_resource = (
            self.resources_baseline_dir / "my_custom_monitor.pipeline.yml"
        )
        assert (
            baseline_resource.exists()
        ), f"Missing resource baseline: {baseline_resource}"
        diff = self._compare_file_hashes(resource_yml, baseline_resource)
        assert diff == "", f"Resource YAML mismatch:\n{diff}"

    # ------------------------------------------------------------------
    # Test 5: catalog and schema override
    # ------------------------------------------------------------------

    def test_monitoring_catalog_schema_override(self):
        """Custom catalog/schema changes ST + MV FQNs, source refs unchanged."""
        self._enable_event_log()
        self._enable_monitoring_variant("monitoring_catalog_schema_override")
        exit_code, output = self._run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        # Structural: overridden catalog/schema in MV target references
        monitoring_py = (
            self.generated_dir / "acme_edw_event_log_monitoring" / "monitoring.py"
        )
        content = monitoring_py.read_text()
        assert "analytics_cat._analytics.all_pipelines_event_log" in content
        assert "analytics_cat._analytics.events_summary" in content
        # Event-log catalog/schema no longer appears in monitoring.py — the
        # UNION moved to the notebook; the monitoring override only retargets
        # the ST/MV FQNs.
        assert "acme_edw_dev._meta" not in content

        # Notebook SOURCES still point at the event_log catalog/schema
        notebook = self.monitoring_dir / "union_event_logs.py"
        nb_content = notebook.read_text()
        assert (
            "acme_edw_dev._meta.acmi_edw_bronze_event_log" in nb_content
        ), "Notebook sources should reference the event_log catalog/schema"

        # Hash comparison for all three monitoring artifacts
        self._assert_monitoring_py_baseline("catalog_schema_override")
        self._assert_monitoring_notebook_baseline("catalog_schema_override")
        self._assert_monitoring_job_baseline("catalog_schema_override")

    # ------------------------------------------------------------------
    # Test 6: custom streaming table name
    # ------------------------------------------------------------------

    def test_monitoring_custom_streaming_table(self):
        """Custom streaming_table changes ST name, append_flow target, and MV FROM."""
        self._enable_event_log()
        self._enable_monitoring_variant("monitoring_custom_streaming_table")
        exit_code, output = self._run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        # Structural: unified_event_log used everywhere instead of
        # all_pipelines_event_log in monitoring.py MV targets
        monitoring_py = (
            self.generated_dir / "acme_edw_event_log_monitoring" / "monitoring.py"
        )
        content = monitoring_py.read_text()
        assert "unified_event_log" in content
        assert "acme_edw_dev._meta.unified_event_log" in content
        # Default ST name must not appear in monitoring.py — UNION is no
        # longer here and MV target was retargeted.
        assert "all_pipelines_event_log" not in content

        # Notebook TARGET_TABLE retargets to unified_event_log
        notebook = self.monitoring_dir / "union_event_logs.py"
        nb_content = notebook.read_text()
        assert 'TARGET_TABLE = "acme_edw_dev._meta.unified_event_log"' in nb_content

        # Hash comparison for all three monitoring artifacts
        self._assert_monitoring_py_baseline("custom_streaming_table")
        self._assert_monitoring_notebook_baseline("custom_streaming_table")
        self._assert_monitoring_job_baseline("custom_streaming_table")

    # ------------------------------------------------------------------
    # Test 7: custom materialized views
    # ------------------------------------------------------------------

    def test_monitoring_custom_mvs(self):
        """Custom MVs replace the default events_summary with user-defined MVs."""
        self._enable_event_log()
        self._enable_monitoring_variant("monitoring_custom_mvs")
        exit_code, output = self._run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        # Structural: custom MVs present, default absent
        monitoring_py = (
            self.generated_dir / "acme_edw_event_log_monitoring" / "monitoring.py"
        )
        content = monitoring_py.read_text()
        assert "def error_events():" in content
        assert "def pipeline_latency():" in content
        assert "events_summary" not in content

        # Hash comparison for all three monitoring artifacts
        self._assert_monitoring_py_baseline("custom_mvs")
        self._assert_monitoring_notebook_baseline("custom_mvs")
        self._assert_monitoring_job_baseline("custom_mvs")

    # ------------------------------------------------------------------
    # Test 8: no materialized views
    # ------------------------------------------------------------------

    def test_monitoring_no_mvs(self):
        """Empty materialized_views list: notebook-only — no DLT artifact."""
        self._enable_event_log()
        self._enable_monitoring_variant("monitoring_no_mvs")
        exit_code, output = self._run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        # DLT file should NOT exist — the builder returns a FlowGroup with no
        # actions, which skips DLT code generation entirely. Ingestion lives
        # in the union notebook.
        monitoring_py = (
            self.generated_dir / "acme_edw_event_log_monitoring" / "monitoring.py"
        )
        assert not monitoring_py.exists(), (
            f"monitoring.py should not be generated with materialized_views: [], "
            f"got {monitoring_py}"
        )

        # Notebook is always written when monitoring is enabled
        notebook = self.monitoring_dir / "union_event_logs.py"
        assert notebook.exists(), f"Notebook should exist: {notebook}"

        # Job YAML is always written when monitoring is enabled
        job_yml = self.resources_root / "acme_edw_event_log_monitoring.job.yml"
        assert job_yml.exists(), f"Job YAML should exist: {job_yml}"

        # Job should contain only the union_event_logs task (no pipeline_task)
        parsed = yaml.safe_load(job_yml.read_text())
        jobs = parsed["resources"]["jobs"]
        job_config = next(iter(jobs.values()))
        tasks = job_config["tasks"]
        assert len(tasks) == 1, (
            f"no_mvs job should have exactly 1 task, got {len(tasks)}: "
            f"{[t.get('task_key') for t in tasks]}"
        )
        assert tasks[0]["task_key"] == "union_event_logs"
        assert "pipeline_task" not in tasks[0]

        # Hash comparison — no monitoring.py baseline for this variant
        self._assert_monitoring_notebook_baseline("no_mvs")
        self._assert_monitoring_job_baseline("no_mvs")

    # ------------------------------------------------------------------
    # Test 9: MV with external sql_path
    # ------------------------------------------------------------------

    def test_monitoring_mv_sql_path(self):
        """MV with sql_path loads SQL from external file at generation time."""
        self._enable_event_log()
        self._enable_monitoring_variant("monitoring_mv_sql_path")
        exit_code, output = self._run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        # Structural: custom_analysis MV with SQL from external file
        monitoring_py = (
            self.generated_dir / "acme_edw_event_log_monitoring" / "monitoring.py"
        )
        content = monitoring_py.read_text()
        assert "def custom_analysis():" in content
        assert "daily_event_count" in content
        assert "FLOW_PROGRESS" in content

        # Default MV should not be present
        assert "events_summary" not in content

        # Hash comparison for all three monitoring artifacts
        self._assert_monitoring_py_baseline("mv_sql_path")
        self._assert_monitoring_notebook_baseline("mv_sql_path")
        self._assert_monitoring_job_baseline("mv_sql_path")

    # ------------------------------------------------------------------
    # Test 10: enable_job_monitoring adds jobs stats loader
    # ------------------------------------------------------------------

    def test_monitoring_enable_job_monitoring(self):
        """enable_job_monitoring: true generates Python load action + auxiliary loader file."""
        self._enable_event_log()
        self._enable_monitoring_variant("monitoring_enable_job_monitoring")
        exit_code, output = self._run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        pipeline_dir = self.generated_dir / "acme_edw_event_log_monitoring"

        # monitoring.py should contain python load + jobs stats write
        monitoring_py = pipeline_dir / "monitoring.py"
        assert (
            monitoring_py.exists()
        ), f"Monitoring pipeline file not generated: {monitoring_py}"
        content = monitoring_py.read_text()
        assert "v_jobs_stats" in content
        assert "jobs_stats" in content

        # Auxiliary file should be generated
        loader_py = pipeline_dir / "jobs_stats_loader.py"
        assert loader_py.exists(), f"Jobs stats loader not generated: {loader_py}"
        loader_content = loader_py.read_text()
        assert "def get_jobs_stats" in loader_content
        assert "WorkspaceClient" in loader_content

        # Hash comparison for all three monitoring artifacts
        self._assert_monitoring_py_baseline("enable_job_monitoring")
        self._assert_monitoring_notebook_baseline("enable_job_monitoring")
        self._assert_monitoring_job_baseline("enable_job_monitoring")

        # Verify auxiliary file matches the package resource (no baseline needed)
        from importlib.resources import files

        expected = (
            files("lhp.templates.monitoring") / "jobs_stats_loader.py"
        ).read_text(encoding="utf-8")
        assert (
            loader_content == expected
        ), "Generated jobs_stats_loader.py does not match package resource"

    # ------------------------------------------------------------------
    # Test 10b: dedicated monitoring job_config_path with rich overrides
    # ------------------------------------------------------------------

    def test_monitoring_custom_job_config(self):
        """A rich monitoring_job_config.yaml is loaded, token-substituted, and
        deep-merged over defaults; all overrides flow into the generated job
        YAML. Regression guard for the dedicated-file contract that replaced
        the legacy ``__eventlog_monitoring`` alias path."""
        self._enable_event_log()
        self._enable_monitoring_variant("monitoring_custom_job_config")

        exit_code, output = self._run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        # Hash-check monitoring notebook + pipeline code match the default
        # variant (this variant only changes the *job* config, not the
        # notebook or MVs).
        self._assert_monitoring_py_baseline("default")
        self._assert_monitoring_notebook_baseline("default")

        # Job YAML must reflect the rich overrides + substituted tokens.
        job_yml = (
            self.resources_root / "acme_edw_event_log_monitoring.job.yml"
        )
        assert job_yml.exists(), f"Monitoring job YAML not generated: {job_yml}"
        parsed = yaml.safe_load(job_yml.read_text())
        job = parsed["resources"]["jobs"]["acme_edw_event_log_monitoring_job"]

        # Scalar overrides
        assert job["max_concurrent_runs"] == 2
        assert job["performance_target"] == "PERFORMANCE_OPTIMIZED"
        assert job["timeout_seconds"] == 3600
        assert job["queue"]["enabled"] is True

        # Tags deep-merged + token substituted. ${bundle_target} is not
        # defined in substitutions/dev.yaml so it is preserved verbatim for
        # Databricks Asset Bundles to resolve at deploy time.
        assert job["tags"]["managed_by"] == "lakehouse_plumber"
        assert job["tags"]["environment"] == "${bundle_target}"
        assert job["tags"]["purpose"] == "event_log_monitoring"
        assert job["tags"]["team"] == "data-platform"

        # Email notifications flow through
        assert job["email_notifications"]["on_failure"] == [
            "monitoring-alerts@example.com"
        ]

        # Schedule flows through
        assert job["schedule"]["quartz_cron_expression"] == "0 0 * * * ?"
        assert job["schedule"]["timezone_id"] == "UTC"
        assert job["schedule"]["pause_status"] == "UNPAUSED"

    # ------------------------------------------------------------------
    # Test 10c: missing job_config_path file surfaces a clear error
    # ------------------------------------------------------------------

    def test_monitoring_job_config_file_missing_fails(self):
        """If monitoring.job_config_path points to a non-existent file, the
        CLI must fail with a clear LHP-CFG-008 error at validation time —
        before any generation work starts."""
        self._enable_event_log()
        # Hand-roll a monitoring block that references a file we do NOT create
        self.lhp_yaml.write_text(
            self.lhp_yaml.read_text()
            + "\nmonitoring:\n"
            '  checkpoint_path: "/Volumes/cat/_meta/checkpoints/event_logs"\n'
            '  job_config_path: "config/does_not_exist.yaml"\n'
        )

        exit_code, output = self._run_generate()
        assert exit_code != 0, "Expected generation to fail for missing job config"
        assert (
            "job_config" in output or "job_config_path" in output
        ), f"Error message should mention job_config: {output}"

    # ------------------------------------------------------------------
    # Test 11: monitoring pipeline picks up pipeline_config.yaml settings
    # ------------------------------------------------------------------

    def test_monitoring_pipeline_config_settings(self):
        """Pipeline config document for monitoring pipeline flows into resource YAML.

        Validates that when pipeline_config.yaml contains a document with
        pipeline: acme_edw_event_log_monitoring, the settings (serverless,
        clusters, notifications, tags, edition, photon) appear in the
        generated bundle resource YAML for the monitoring pipeline.
        """
        self._enable_event_log_and_monitoring()

        # Add a pipeline_config document targeting the monitoring pipeline
        config_file = self.project_root / "config" / "pipeline_config.yaml"
        existing_content = config_file.read_text()

        monitoring_config_doc = """
---
pipeline: acme_edw_event_log_monitoring
serverless: false
edition: ADVANCED
photon: true
clusters:
  - label: default
    node_type_id: Standard_D4ds_v5
    autoscale:
      min_workers: 1
      max_workers: 4
      mode: ENHANCED
notifications:
  - email_recipients:
      - monitoring-team@example.com
    alerts:
      - on-update-failure
      - on-update-fatal-failure
tags:
  team: data-platform
  purpose: monitoring
"""
        config_file.write_text(existing_content + monitoring_config_doc)

        # Run generation WITH --pipeline-config flag
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--verbose",
                "generate",
                "--env",
                "dev",
                "--force",
                "--pipeline-config",
                "config/pipeline_config.yaml",
            ],
        )
        exit_code, output = result.exit_code, result.output
        assert exit_code == 0, f"Generation failed: {output}"

        # Verify monitoring resource file was generated
        monitoring_resource = (
            self.resources_dir / "acme_edw_event_log_monitoring.pipeline.yml"
        )
        assert (
            monitoring_resource.exists()
        ), f"Monitoring resource not generated: {monitoring_resource}"

        # Parse the generated resource YAML
        parsed = yaml.safe_load(monitoring_resource.read_text())
        pipeline = parsed["resources"]["pipelines"][
            "acme_edw_event_log_monitoring_pipeline"
        ]

        # Verify pipeline_config settings flowed into the resource
        assert (
            pipeline["serverless"] is False
        ), "serverless should be False from pipeline_config"
        assert (
            pipeline["edition"] == "ADVANCED"
        ), "edition should be ADVANCED from pipeline_config"
        assert pipeline["photon"] is True, "photon should be True from pipeline_config"

        # Verify cluster configuration
        assert "clusters" in pipeline, "clusters should be present from pipeline_config"
        assert len(pipeline["clusters"]) == 1
        cluster = pipeline["clusters"][0]
        assert cluster["label"] == "default"
        assert cluster["node_type_id"] == "Standard_D4ds_v5"
        assert cluster["autoscale"]["min_workers"] == 1
        assert cluster["autoscale"]["max_workers"] == 4
        assert cluster["autoscale"]["mode"] == "ENHANCED"

        # Verify notifications
        assert (
            "notifications" in pipeline
        ), "notifications should be present from pipeline_config"
        assert len(pipeline["notifications"]) == 1
        notification = pipeline["notifications"][0]
        assert "monitoring-team@example.com" in notification["email_recipients"]
        assert "on-update-failure" in notification["alerts"]
        assert "on-update-fatal-failure" in notification["alerts"]

        # Verify tags
        assert "tags" in pipeline, "tags should be present from pipeline_config"
        assert pipeline["tags"]["team"] == "data-platform"
        assert pipeline["tags"]["purpose"] == "monitoring"

        # Verify monitoring pipeline still does NOT have event_log
        # (monitoring pipeline should never self-reference its own event_log)
        assert (
            "event_log" not in pipeline
        ), "Monitoring pipeline resource should NOT have event_log"

        # Verify proper YAML structure (no line concatenation)
        resource_content = monitoring_resource.read_text()
        assert not re.search(
            r"clusters:[ \t]+- label:", resource_content
        ), "BUG: Cluster list item concatenated on same line as 'clusters:'"
        assert not re.search(
            r"notifications:[ \t]+- email_recipients:", resource_content
        ), "BUG: Notifications concatenated on same line"
        assert not re.search(
            r"tags:[ \t]+\w+:", resource_content
        ), "BUG: Tags concatenated on same line"

    # ------------------------------------------------------------------
    # Test 11: monitoring pipeline inherits project_defaults from pipeline_config
    # ------------------------------------------------------------------

    def test_monitoring_pipeline_inherits_project_defaults(self):
        """Monitoring pipeline inherits project_defaults when no pipeline-specific doc exists.

        Validates that project_defaults (e.g. serverless: false, edition: PRO)
        apply to the monitoring pipeline resource even without a
        pipeline: acme_edw_event_log_monitoring document.
        """
        self._enable_event_log_and_monitoring()

        # Overwrite pipeline_config with only project_defaults — no monitoring doc
        config_file = self.project_root / "config" / "pipeline_config.yaml"
        config_file.write_text(
            "project_defaults:\n"
            "  serverless: false\n"
            "  edition: PRO\n"
            "  photon: true\n"
        )

        # Run generation WITH --pipeline-config flag
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--verbose",
                "generate",
                "--env",
                "dev",
                "--force",
                "--pipeline-config",
                "config/pipeline_config.yaml",
            ],
        )
        exit_code, output = result.exit_code, result.output
        assert exit_code == 0, f"Generation failed: {output}"

        # Verify monitoring resource file was generated
        monitoring_resource = (
            self.resources_dir / "acme_edw_event_log_monitoring.pipeline.yml"
        )
        assert (
            monitoring_resource.exists()
        ), f"Monitoring resource not generated: {monitoring_resource}"

        # Parse and verify project_defaults were inherited
        parsed = yaml.safe_load(monitoring_resource.read_text())
        pipeline = parsed["resources"]["pipelines"][
            "acme_edw_event_log_monitoring_pipeline"
        ]

        assert (
            pipeline["serverless"] is False
        ), "serverless should be False from project_defaults"
        assert (
            pipeline["edition"] == "PRO"
        ), "edition should be PRO from project_defaults"
        assert pipeline["photon"] is True, "photon should be True from project_defaults"

    # ------------------------------------------------------------------
    # Test 12: __eventlog_monitoring alias resolves in generated bundle
    # ------------------------------------------------------------------

    def test_monitoring_alias_resolves_in_generated_bundle(self):
        """pipeline: __eventlog_monitoring alias resolves to actual monitoring pipeline name.

        Validates that users can write 'pipeline: __eventlog_monitoring' in
        pipeline_config.yaml without knowing the runtime monitoring pipeline name.
        The alias should resolve to the actual name and settings should appear
        in the generated bundle resource YAML.
        """
        self._enable_event_log_and_monitoring()

        # Write pipeline_config.yaml using the alias
        config_file = self.project_root / "config" / "pipeline_config.yaml"
        config_file.write_text(
            "project_defaults:\n"
            "  serverless: true\n"
            "\n"
            "---\n"
            "pipeline: __eventlog_monitoring\n"
            "serverless: false\n"
            "edition: ADVANCED\n"
            "photon: true\n"
            "clusters:\n"
            "  - label: default\n"
            "    node_type_id: Standard_D4ds_v5\n"
            "    autoscale:\n"
            "      min_workers: 1\n"
            "      max_workers: 3\n"
            "tags:\n"
            "  purpose: event_log_monitoring\n"
            "  managed_by: lhp\n"
        )

        # Run generation WITH --pipeline-config flag
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--verbose",
                "generate",
                "--env",
                "dev",
                "--force",
                "--pipeline-config",
                "config/pipeline_config.yaml",
            ],
        )
        exit_code, output = result.exit_code, result.output
        assert exit_code == 0, f"Generation failed: {output}"

        # Verify monitoring resource file was generated with the REAL name
        monitoring_resource = (
            self.resources_dir / "acme_edw_event_log_monitoring.pipeline.yml"
        )
        assert (
            monitoring_resource.exists()
        ), f"Monitoring resource not generated: {monitoring_resource}"

        # Parse the generated resource YAML
        parsed = yaml.safe_load(monitoring_resource.read_text())
        pipeline = parsed["resources"]["pipelines"][
            "acme_edw_event_log_monitoring_pipeline"
        ]

        # Verify alias settings flowed through to the resource
        assert (
            pipeline["serverless"] is False
        ), "serverless should be False from __eventlog_monitoring alias config"
        assert (
            pipeline["edition"] == "ADVANCED"
        ), "edition should be ADVANCED from alias config"
        assert pipeline["photon"] is True, "photon should be True from alias config"

        # Verify cluster configuration from alias
        assert "clusters" in pipeline, "clusters should be present from alias config"
        assert len(pipeline["clusters"]) == 1
        cluster = pipeline["clusters"][0]
        assert cluster["node_type_id"] == "Standard_D4ds_v5"
        assert cluster["autoscale"]["min_workers"] == 1
        assert cluster["autoscale"]["max_workers"] == 3

        # Verify tags from alias
        assert "tags" in pipeline, "tags should be present from alias config"
        assert pipeline["tags"]["purpose"] == "event_log_monitoring"
        assert pipeline["tags"]["managed_by"] == "lhp"

        # Verify monitoring pipeline still does NOT have event_log
        assert (
            "event_log" not in pipeline
        ), "Monitoring pipeline resource should NOT have event_log"
