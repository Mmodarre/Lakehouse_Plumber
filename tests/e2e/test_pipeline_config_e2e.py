"""End-to-end tests for pipeline configuration feature."""

import pytest
import yaml
import re
from pathlib import Path
import shutil


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

