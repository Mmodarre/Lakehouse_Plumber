"""End-to-end tests for pipeline configuration feature."""

import pytest
import yaml
import re
import os
import hashlib
from pathlib import Path
import shutil
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
            testing_project,
            pipeline_config_path=str(config_file)
        )
        
        # Generate resource file content
        output_dir = testing_project / "generated" / "dev"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        resource_content = bundle_manager.generate_resource_file_content(
            "acmi_edw_raw",
            output_dir,
            env="dev"
        )
        
        # Test 1: Generated YAML must be valid
        try:
            parsed_yaml = yaml.safe_load(resource_content)
            assert parsed_yaml is not None
        except yaml.YAMLError as e:
            pytest.fail(f"Generated resource YAML is invalid: {e}\n\nContent:\n{resource_content}")
        
        # Test 2: Verify NO line concatenation (the bug this PR fixes)
        # Use [ \t]+ to match spaces/tabs but NOT newlines
        assert not re.search(r'clusters:[ \t]+- label:', resource_content), \
            "BUG: Cluster list item concatenated on same line as 'clusters:'"
        assert not re.search(r'node_type_id:[ \t]+\w+[ \t]+driver_node_type_id:', resource_content), \
            "BUG: Fields concatenated on same line"
        
        # Test 3: Verify proper YAML structure
        assert re.search(r'clusters:\s*\n\s+- label:', resource_content), \
            "Cluster list should start on new line"
        
        # Test 4: Verify cluster configuration is present
        pipeline_config = parsed_yaml['resources']['pipelines']['acmi_edw_raw_pipeline']
        assert pipeline_config['serverless'] is False
        assert 'clusters' in pipeline_config
        assert len(pipeline_config['clusters']) == 1
        
        cluster = pipeline_config['clusters'][0]
        assert cluster['label'] == 'default'
        assert cluster['node_type_id'] == 'Standard_D16ds_v5'
        assert cluster['driver_node_type_id'] == 'Standard_D32ds_v5'
        assert cluster['autoscale']['min_workers'] == 2
        assert cluster['autoscale']['max_workers'] == 10
        assert cluster['autoscale']['mode'] == 'ENHANCED'
        
        # Test 5: Verify other config options
        assert pipeline_config.get('continuous') is True
        assert pipeline_config.get('photon') is True
        assert pipeline_config.get('edition') == 'ADVANCED'
        assert pipeline_config.get('channel') == 'CURRENT'
    
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
            testing_project,
            pipeline_config_path=str(config_file)
        )
        
        output_dir = testing_project / "generated" / "dev"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate for raw pipeline
        raw_content = bundle_manager.generate_resource_file_content(
            "acmi_edw_raw",
            output_dir,
            env="dev"
        )
        raw_yaml = yaml.safe_load(raw_content)
        raw_pipeline = raw_yaml['resources']['pipelines']['acmi_edw_raw_pipeline']
        
        # Generate for bronze pipeline
        bronze_content = bundle_manager.generate_resource_file_content(
            "acmi_edw_bronze",
            output_dir,
            env="dev"
        )
        bronze_yaml = yaml.safe_load(bronze_content)
        bronze_pipeline = bronze_yaml['resources']['pipelines']['acmi_edw_bronze_pipeline']
        
        # Verify each has correct cluster config
        assert raw_pipeline['clusters'][0]['node_type_id'] == 'Standard_D16ds_v5'
        assert raw_pipeline['clusters'][0]['autoscale']['max_workers'] == 10
        
        assert bronze_pipeline['clusters'][0]['node_type_id'] == 'Standard_D8ds_v5'
        assert bronze_pipeline['clusters'][0]['autoscale']['max_workers'] == 5
        
        # Both should have project defaults applied
        assert raw_pipeline['edition'] == 'PRO'
        assert bronze_pipeline['edition'] == 'PRO'
    
    def test_comprehensive_cluster_config_matches_baseline(self, testing_project):
        """Test comprehensive cluster config with ALL options matches baseline."""
        from lhp.bundle.manager import BundleManager
        
        # Load comprehensive config fixture
        fixture_path = Path(__file__).parent.parent / "fixtures" / "pipeline_configs" / "comprehensive_cluster_config.yaml"
        assert fixture_path.exists(), f"Comprehensive config fixture should exist: {fixture_path}"
        
        bundle_manager = BundleManager(
            testing_project,
            pipeline_config_path=str(fixture_path)
        )
        
        output_dir = testing_project / "generated" / "dev"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate resource content
        generated_content = bundle_manager.generate_resource_file_content(
            "comprehensive_cluster_config",
            output_dir,
            env="dev"
        )
        
        # Load baseline
        baseline_path = Path(__file__).parent / "fixtures" / "testing_project" / "resources_baseline" / "lhp" / "comprehensive_cluster_config.pipeline.yml"
        assert baseline_path.exists(), f"Baseline should exist: {baseline_path}"
        baseline_content = baseline_path.read_text()
        
        # Parse both as YAML to verify validity
        generated_yaml = yaml.safe_load(generated_content)
        baseline_yaml = yaml.safe_load(baseline_content)
        
        # Verify both are valid
        assert generated_yaml is not None, "Generated YAML should parse successfully"
        assert baseline_yaml is not None, "Baseline YAML should parse successfully"
        
        # Compare structure (ignore comments)
        pipeline_config = generated_yaml['resources']['pipelines']['comprehensive_cluster_config_pipeline']
        baseline_config = baseline_yaml['resources']['pipelines']['comprehensive_cluster_config_pipeline']
        
        # Verify ALL configuration options are present and correct
        
        # 1. Non-serverless with clusters
        assert pipeline_config['serverless'] is False
        assert 'clusters' in pipeline_config
        assert len(pipeline_config['clusters']) == 1
        
        cluster = pipeline_config['clusters'][0]
        assert cluster['label'] == 'default'
        assert cluster['node_type_id'] == 'Standard_D16ds_v5'
        assert cluster['driver_node_type_id'] == 'Standard_D32ds_v5'
        assert cluster['policy_id'] == '1234ABCD1234ABCD'
        assert cluster['autoscale']['min_workers'] == 2
        assert cluster['autoscale']['max_workers'] == 10
        assert cluster['autoscale']['mode'] == 'ENHANCED'
        
        # 2. Processing mode
        assert pipeline_config['continuous'] is True
        
        # 3. Compute settings
        assert pipeline_config['photon'] is True
        assert pipeline_config['edition'] == 'ADVANCED'
        assert pipeline_config['channel'] == 'PREVIEW'
        
        # 4. Notifications
        assert 'notifications' in pipeline_config
        assert len(pipeline_config['notifications']) == 1
        notification = pipeline_config['notifications'][0]
        assert 'data-engineering@company.com' in notification['email_recipients']
        assert 'ops-team@company.com' in notification['email_recipients']
        assert 'on-update-failure' in notification['alerts']
        assert 'on-update-fatal-failure' in notification['alerts']
        assert 'on-update-success' in notification['alerts']
        assert 'on-flow-failure' in notification['alerts']
        
        # 5. Tags
        assert 'tags' in pipeline_config
        assert pipeline_config['tags']['environment'] == 'production'
        assert pipeline_config['tags']['cost_center'] == 'data_platform'
        assert pipeline_config['tags']['criticality'] == 'high'
        assert pipeline_config['tags']['team'] == 'data_engineering'
        
        # 6. Event log
        assert 'event_log' in pipeline_config
        assert pipeline_config['event_log']['name'] == 'pipeline_event_log'
        assert pipeline_config['event_log']['schema'] == '_meta'
        assert pipeline_config['event_log']['catalog'] == 'main'
        
        # Verify NO line concatenation (the bug we fixed)
        assert not re.search(r'clusters:[ \t]+- label:', generated_content)
        assert not re.search(r'notifications:[ \t]+- email_recipients:', generated_content)
        assert not re.search(r'tags:[ \t]+\w+:', generated_content)
        
        # Verify proper multi-line structure
        assert re.search(r'clusters:\s*\n\s+- label:', generated_content)
        assert re.search(r'notifications:\s*\n\s+- email_recipients:', generated_content)
        assert re.search(r'tags:\s*\n\s+\w+:', generated_content)
        
        # Deep comparison: generated should match baseline structure
        assert pipeline_config == baseline_config, "Generated config should match baseline structure exactly"


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
                in_target_pipeline = (name == pipeline_name)
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
            "  #environment:\n  #  dependencies:\n  #    - \"msal=={msal_version}\"",
            "  environment:\n    dependencies:\n      - \"msal=={msal_version}\"",
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
        assert result.exit_code == 0, (
            f"Generation should succeed:\n{result.output}"
        )

        generated_files = sorted(self.resources_dir.glob("*.pipeline.yml"))
        assert len(generated_files) > 0, "Should have generated resource files"

        mismatches = []
        for gen_file in generated_files:
            baseline_file = self.baseline_dir / gen_file.name
            assert baseline_file.exists(), (
                f"Missing baseline for {gen_file.name}"
            )
            diff = self._compare_file_hashes(gen_file, baseline_file)
            if diff:
                mismatches.append(diff)

        if mismatches:
            detail = "\n".join(
                f"  {i}. {m}" for i, m in enumerate(mismatches, 1)
            )
            assert False, (
                f"Resource files differ from baselines "
                f"({len(mismatches)} mismatches):\n{detail}"
            )

