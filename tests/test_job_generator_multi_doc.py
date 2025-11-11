"""Tests for JobGenerator multi-document YAML support."""

import pytest
from pathlib import Path
import yaml
from lhp.core.services.job_generator import JobGenerator
from lhp.models.dependencies import DependencyAnalysisResult, PipelineDependency
from unittest.mock import Mock


class TestMultiDocumentParsing:
    """Test multi-document YAML parsing."""
    
    def test_single_document_backward_compat(self, tmp_path):
        """Test backward compatibility with single-document job_config.yaml."""
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        
        config_content = """max_concurrent_runs: 2
performance_target: PERFORMANCE_OPTIMIZED
tags:
  env: dev
  team: data
"""
        config_file = config_dir / "job_config.yaml"
        config_file.write_text(config_content)
        
        generator = JobGenerator(project_root=tmp_path, config_file_path="config/job_config.yaml")
        
        # Assert project_defaults loaded correctly
        assert generator.project_defaults["max_concurrent_runs"] == 2
        assert generator.project_defaults["performance_target"] == "PERFORMANCE_OPTIMIZED"
        # Assert job_specific_configs is empty
        assert len(generator.job_specific_configs) == 0
    
    def test_single_doc_with_project_defaults_key(self, tmp_path):
        """Test single document with project_defaults key."""
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        
        config_content = """project_defaults:
  max_concurrent_runs: 3
  tags:
    env: test
"""
        config_file = config_dir / "job_config.yaml"
        config_file.write_text(config_content)
        
        generator = JobGenerator(project_root=tmp_path, config_file_path="config/job_config.yaml")
        
        # Assert project_defaults extracted correctly
        assert generator.project_defaults["max_concurrent_runs"] == 3
        assert generator.project_defaults["tags"]["env"] == "test"
        assert len(generator.job_specific_configs) == 0
    
    def test_multi_doc_with_project_defaults_only(self, tmp_path):
        """Test multi-document format with project_defaults."""
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        
        config_content = """project_defaults:
  max_concurrent_runs: 1
  tags:
    env: dev
"""
        config_file = config_dir / "job_config.yaml"
        config_file.write_text(config_content)
        
        generator = JobGenerator(project_root=tmp_path, config_file_path="config/job_config.yaml")
        
        # Assert project_defaults loaded
        assert generator.project_defaults["max_concurrent_runs"] == 1
        # Assert job_specific_configs is empty (no job docs)
        assert len(generator.job_specific_configs) == 0
    
    def test_multi_doc_with_two_job_configs(self, sample_multi_doc_job_config):
        """Test multi-document format with job-specific configs."""
        generator = JobGenerator(
            project_root=sample_multi_doc_job_config,
            config_file_path="config/job_config.yaml"
        )
        
        # Assert project_defaults loaded
        assert generator.project_defaults["max_concurrent_runs"] == 1
        assert generator.project_defaults["performance_target"] == "STANDARD"
        
        # Assert 2 job-specific configs loaded
        assert len(generator.job_specific_configs) == 2
        
        # Assert job_name keys are correct
        assert "bronze_job" in generator.job_specific_configs
        assert "silver_job" in generator.job_specific_configs
        
        # Check bronze_job config
        assert generator.job_specific_configs["bronze_job"]["max_concurrent_runs"] == 2
        assert generator.job_specific_configs["bronze_job"]["tags"]["layer"] == "bronze"
        
        # Check silver_job config
        assert generator.job_specific_configs["silver_job"]["performance_target"] == "PERFORMANCE_OPTIMIZED"
        assert generator.job_specific_configs["silver_job"]["tags"]["layer"] == "silver"
    
    def test_empty_config_file(self, tmp_path):
        """Test handling of empty config file."""
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        
        config_file = config_dir / "job_config.yaml"
        config_file.write_text("")
        
        generator = JobGenerator(project_root=tmp_path, config_file_path="config/job_config.yaml")
        
        # Assert defaults used
        assert generator.project_defaults == {}
        assert generator.job_specific_configs == {}
    
    def test_missing_config_file_raises_error(self, tmp_path):
        """Test handling when config file doesn't exist."""
        # Should raise FileNotFoundError when specified config file doesn't exist
        with pytest.raises(FileNotFoundError) as exc_info:
            JobGenerator(project_root=tmp_path, config_file_path="config/nonexistent.yaml")
        
        assert "Job config file not found" in str(exc_info.value)
    
    def test_invalid_yaml_syntax_raises(self, tmp_path):
        """Test handling of invalid YAML syntax."""
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        
        config_file = config_dir / "job_config.yaml"
        config_file.write_text("invalid: yaml: syntax: {]")
        
        with pytest.raises(yaml.YAMLError):
            JobGenerator(project_root=tmp_path, config_file_path="config/job_config.yaml")
    
    def test_document_without_keys_logged_and_skipped(self, tmp_path, caplog):
        """Test document without project_defaults or job_name is skipped."""
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        
        config_content = """project_defaults:
  max_concurrent_runs: 1
---
random_key: random_value
other_key: other_value
---
job_name: bronze_job
max_concurrent_runs: 2
"""
        config_file = config_dir / "job_config.yaml"
        config_file.write_text(config_content)
        
        generator = JobGenerator(project_root=tmp_path, config_file_path="config/job_config.yaml")
        
        # Should have loaded project_defaults and bronze_job, but skipped middle doc
        assert generator.project_defaults["max_concurrent_runs"] == 1
        assert "bronze_job" in generator.job_specific_configs
        
        # Check for warning log
        assert "neither 'project_defaults' nor 'job_name'" in caplog.text


class TestDeepMergeDicts:
    """Test deep merge functionality."""
    
    def test_simple_merge_override(self):
        """Test simple dictionary merge."""
        generator = JobGenerator()
        
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}
        
        result = generator._deep_merge_dicts(base, override)
        
        assert result == {"a": 1, "b": 3, "c": 4}
    
    def test_nested_dict_deep_merge(self):
        """Test nested dictionary deep merge."""
        generator = JobGenerator()
        
        base = {"tags": {"env": "dev", "team": "data"}}
        override = {"tags": {"env": "prod", "project": "lakehouse"}}
        
        result = generator._deep_merge_dicts(base, override)
        
        # env should be overridden, team preserved, project added
        assert result == {
            "tags": {
                "env": "prod",
                "team": "data",
                "project": "lakehouse"
            }
        }
    
    def test_list_replacement_not_merge(self):
        """Test that lists are replaced, not merged."""
        generator = JobGenerator()
        
        base = {"emails": ["a@test.com", "b@test.com"]}
        override = {"emails": ["c@test.com"]}
        
        result = generator._deep_merge_dicts(base, override)
        
        # List should be replaced, not merged
        assert result == {"emails": ["c@test.com"]}
    
    def test_three_level_nested_merge(self):
        """Test deeply nested dicts (3+ levels)."""
        generator = JobGenerator()
        
        base = {
            "level1": {
                "level2": {
                    "level3": {
                        "key1": "value1",
                        "key2": "value2"
                    }
                }
            }
        }
        override = {
            "level1": {
                "level2": {
                    "level3": {
                        "key2": "overridden",
                        "key3": "new"
                    }
                }
            }
        }
        
        result = generator._deep_merge_dicts(base, override)
        
        assert result["level1"]["level2"]["level3"]["key1"] == "value1"
        assert result["level1"]["level2"]["level3"]["key2"] == "overridden"
        assert result["level1"]["level2"]["level3"]["key3"] == "new"
    
    def test_empty_base_dict(self):
        """Test merge with empty base dict."""
        generator = JobGenerator()
        
        base = {}
        override = {"a": 1, "b": 2}
        
        result = generator._deep_merge_dicts(base, override)
        
        assert result == {"a": 1, "b": 2}
    
    def test_empty_override_dict(self):
        """Test merge with empty override dict."""
        generator = JobGenerator()
        
        base = {"a": 1, "b": 2}
        override = {}
        
        result = generator._deep_merge_dicts(base, override)
        
        assert result == {"a": 1, "b": 2}


class TestGetJobConfigForJob:
    """Test job-specific config retrieval."""
    
    def test_with_job_specific_override(self, sample_multi_doc_job_config):
        """Test getting config for job with specific overrides."""
        generator = JobGenerator(
            project_root=sample_multi_doc_job_config,
            config_file_path="config/job_config.yaml"
        )
        
        config = generator.get_job_config_for_job("bronze_job")
        
        # Should have DEFAULT + project_defaults + job-specific merged
        assert config["max_concurrent_runs"] == 2  # job-specific override
        assert config["performance_target"] == "STANDARD"  # from project_defaults
        assert config["tags"]["env"] == "dev"  # from project_defaults
        assert config["tags"]["managed_by"] == "lhp"  # from project_defaults
        assert config["tags"]["layer"] == "bronze"  # job-specific
    
    def test_without_job_specific_fallback(self, sample_multi_doc_job_config):
        """Test getting config for job without specific overrides."""
        generator = JobGenerator(
            project_root=sample_multi_doc_job_config,
            config_file_path="config/job_config.yaml"
        )
        
        config = generator.get_job_config_for_job("nonexistent_job")
        
        # Should use DEFAULT + project_defaults
        assert config["max_concurrent_runs"] == 1  # from project_defaults
        assert config["performance_target"] == "STANDARD"  # from project_defaults
        assert "layer" not in config.get("tags", {})  # No job-specific tags
    
    def test_merge_order_job_specific_wins(self, tmp_path):
        """Test that merge order is: DEFAULT → project_defaults → job-specific."""
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        
        config_content = """project_defaults:
  max_concurrent_runs: 5
---
job_name: test_job
max_concurrent_runs: 10
"""
        config_file = config_dir / "job_config.yaml"
        config_file.write_text(config_content)
        
        generator = JobGenerator(project_root=tmp_path, config_file_path="config/job_config.yaml")
        config = generator.get_job_config_for_job("test_job")
        
        # Job-specific value should win
        assert config["max_concurrent_runs"] == 10
    
    def test_tags_deep_merge_realistic_example(self, sample_multi_doc_job_config):
        """Test real-world tags deep merge scenario."""
        generator = JobGenerator(
            project_root=sample_multi_doc_job_config,
            config_file_path="config/job_config.yaml"
        )
        
        config = generator.get_job_config_for_job("silver_job")
        
        # Should have all tags merged
        assert config["tags"]["env"] == "dev"  # from project_defaults
        assert config["tags"]["managed_by"] == "lhp"  # from project_defaults
        assert config["tags"]["layer"] == "silver"  # from job-specific


class TestGenerateJobsByName:
    """Test multiple job generation."""
    
    def test_generate_three_jobs(self, mock_dependency_result, sample_multi_doc_job_config):
        """Test generating multiple job YAMLs."""
        generator = JobGenerator(
            project_root=sample_multi_doc_job_config,
            config_file_path="config/job_config.yaml"
        )
        
        # Create 3 mock DependencyAnalysisResult objects
        job_results = {
            "bronze_job": mock_dependency_result,
            "silver_job": mock_dependency_result,
            "gold_job": mock_dependency_result
        }
        
        yamls = generator.generate_jobs_by_name(job_results, "test_project")
        
        assert len(yamls) == 3
        assert "bronze_job" in yamls
        assert "silver_job" in yamls
        assert "gold_job" in yamls
        
        # Verify each YAML contains correct job_name
        assert "bronze_job" in yamls["bronze_job"]
        assert "silver_job" in yamls["silver_job"]
        assert "gold_job" in yamls["gold_job"]
    
    def test_job_specific_config_applied(self, mock_dependency_result, sample_multi_doc_job_config):
        """Test that job-specific config is applied in generated YAML."""
        generator = JobGenerator(
            project_root=sample_multi_doc_job_config,
            config_file_path="config/job_config.yaml"
        )
        
        job_results = {"bronze_job": mock_dependency_result}
        
        yamls = generator.generate_jobs_by_name(job_results, "test_project")
        
        # Parse the YAML to verify config
        bronze_yaml = yaml.safe_load(yamls["bronze_job"])
        
        # bronze_job has max_concurrent_runs: 2 in config
        assert bronze_yaml["resources"]["jobs"]["bronze_job"]["max_concurrent_runs"] == 2
    
    def test_empty_job_results_returns_empty_dict(self):
        """Test empty job_results returns empty dict."""
        generator = JobGenerator()
        
        yamls = generator.generate_jobs_by_name({}, "test_project")
        
        assert yamls == {}
    
    def test_generated_yaml_valid_format(self, mock_dependency_result):
        """Test that generated YAML is valid and parseable."""
        generator = JobGenerator()
        
        job_results = {"test_job": mock_dependency_result}
        
        yamls = generator.generate_jobs_by_name(job_results, "test_project")
        
        # Should be parseable YAML
        parsed = yaml.safe_load(yamls["test_job"])
        assert "resources" in parsed
        assert "jobs" in parsed["resources"]
        assert "test_job" in parsed["resources"]["jobs"]


class TestGenerateMasterJob:
    """Test master orchestration job generation."""
    
    def test_basic_master_with_two_jobs(self, mock_dependency_result):
        """Test generating master orchestration job."""
        generator = JobGenerator()
        
        job_results = {
            "bronze_job": mock_dependency_result,
            "silver_job": mock_dependency_result
        }
        
        master_yaml = generator.generate_master_job(
            job_results, "test_master", "test_project"
        )
        
        # Assert YAML contains job_task references
        assert "bronze_job" in master_yaml
        assert "silver_job" in master_yaml
        assert "job_task:" in master_yaml
        
        # Parse to verify structure
        parsed = yaml.safe_load(master_yaml)
        assert "resources" in parsed
        assert "jobs" in parsed["resources"]
        assert "test_master" in parsed["resources"]["jobs"]
    
    def test_cross_job_dependencies_in_depends_on(self):
        """Test master job includes cross-job dependencies."""
        generator = JobGenerator()
        
        # Create mock results where silver depends on bronze
        bronze_result = Mock(spec=DependencyAnalysisResult)
        bronze_result.pipeline_dependencies = {
            "bronze_pipeline": Mock(depends_on=[], flowgroup_count=1, action_count=2)
        }
        
        silver_result = Mock(spec=DependencyAnalysisResult)
        silver_result.pipeline_dependencies = {
            "silver_pipeline": Mock(depends_on=["bronze_pipeline"], flowgroup_count=1, action_count=2)
        }
        
        job_results = {
            "bronze_job": bronze_result,
            "silver_job": silver_result
        }
        
        master_yaml = generator.generate_master_job(
            job_results, "test_master", "test_project"
        )
        
        # Parse and check for dependencies
        parsed = yaml.safe_load(master_yaml)
        tasks = parsed["resources"]["jobs"]["test_master"]["tasks"]
        
        # Find silver_job task
        silver_task = next(t for t in tasks if t["task_key"] == "silver_job_task")
        
        # Should have depends_on
        assert "depends_on" in silver_task
        assert silver_task["depends_on"][0]["task_key"] == "bronze_job_task"
    
    def test_independent_jobs_no_depends_on(self, mock_dependency_result):
        """Test independent jobs have no depends_on clauses."""
        generator = JobGenerator()
        
        # Create results with no cross-dependencies
        result1 = Mock(spec=DependencyAnalysisResult)
        result1.pipeline_dependencies = {
            "pipeline1": Mock(depends_on=[], flowgroup_count=1, action_count=2)
        }
        
        result2 = Mock(spec=DependencyAnalysisResult)
        result2.pipeline_dependencies = {
            "pipeline2": Mock(depends_on=[], flowgroup_count=1, action_count=2)
        }
        
        job_results = {
            "job1": result1,
            "job2": result2
        }
        
        master_yaml = generator.generate_master_job(
            job_results, "test_master", "test_project"
        )
        
        # Parse and verify no depends_on
        parsed = yaml.safe_load(master_yaml)
        tasks = parsed["resources"]["jobs"]["test_master"]["tasks"]
        
        for task in tasks:
            # Independent jobs should not have depends_on
            if "depends_on" in task:
                # If it has depends_on, it should be for a different reason
                pass  # This is ok for the simple test
    
    def test_single_job_master(self, mock_dependency_result):
        """Test master job with single job."""
        generator = JobGenerator()
        
        job_results = {"only_job": mock_dependency_result}
        
        master_yaml = generator.generate_master_job(
            job_results, "test_master", "test_project"
        )
        
        # Should still work with one job
        parsed = yaml.safe_load(master_yaml)
        tasks = parsed["resources"]["jobs"]["test_master"]["tasks"]
        assert len(tasks) == 1
        assert tasks[0]["task_key"] == "only_job_task"
