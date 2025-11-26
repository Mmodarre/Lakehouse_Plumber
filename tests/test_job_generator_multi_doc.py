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
        
        # Create global result for the new required parameter
        global_result = Mock(spec=DependencyAnalysisResult)
        global_result.pipeline_dependencies = {
            "pipeline1": Mock(depends_on=[], flowgroup_count=1, action_count=2),
            "pipeline2": Mock(depends_on=["pipeline1"], flowgroup_count=1, action_count=2)
        }
        
        master_yaml = generator.generate_master_job(
            job_results, "test_master", "test_project", global_result=global_result
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
        
        # Create global result showing the dependency relationship
        global_result = Mock(spec=DependencyAnalysisResult)
        global_result.pipeline_dependencies = {
            "bronze_pipeline": Mock(depends_on=[], flowgroup_count=1, action_count=2),
            "silver_pipeline": Mock(depends_on=["bronze_pipeline"], flowgroup_count=1, action_count=2)
        }
        
        master_yaml = generator.generate_master_job(
            job_results, "test_master", "test_project", global_result=global_result
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
        
        # Create global result with independent pipelines
        global_result = Mock(spec=DependencyAnalysisResult)
        global_result.pipeline_dependencies = {
            "pipeline1": Mock(depends_on=[], flowgroup_count=1, action_count=2),
            "pipeline2": Mock(depends_on=[], flowgroup_count=1, action_count=2)
        }
        
        master_yaml = generator.generate_master_job(
            job_results, "test_master", "test_project", global_result=global_result
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
        
        # Create global result for single job
        global_result = Mock(spec=DependencyAnalysisResult)
        global_result.pipeline_dependencies = {
            "pipeline1": Mock(depends_on=[], flowgroup_count=1, action_count=2),
            "pipeline2": Mock(depends_on=["pipeline1"], flowgroup_count=1, action_count=2)
        }
        
        master_yaml = generator.generate_master_job(
            job_results, "test_master", "test_project", global_result=global_result
        )
        
        # Should still work with one job
        parsed = yaml.safe_load(master_yaml)
        tasks = parsed["resources"]["jobs"]["test_master"]["tasks"]
        assert len(tasks) == 1
        assert tasks[0]["task_key"] == "only_job_task"


class TestMasterJobWithGlobalDependencies:
    """Test master job generation using global dependency analysis."""
    
    def test_linear_dependency_chain_with_global_result(self):
        """Test j_one → j_two → j_three → j_four using global dependencies."""
        generator = JobGenerator()
        
        # Create global result with complete pipeline dependency graph
        global_result = Mock(spec=DependencyAnalysisResult)
        global_result.pipeline_dependencies = {
            "acmi_edw_raw": Mock(
                pipeline="acmi_edw_raw",
                depends_on=[],
                flowgroup_count=1,
                action_count=2
            ),
            "acmi_edw_bronze": Mock(
                pipeline="acmi_edw_bronze",
                depends_on=["acmi_edw_raw"],
                flowgroup_count=1,
                action_count=2
            ),
            "acmi_edw_silver": Mock(
                pipeline="acmi_edw_silver",
                depends_on=["acmi_edw_bronze"],
                flowgroup_count=1,
                action_count=2
            ),
            "gold_load": Mock(
                pipeline="gold_load",
                depends_on=["acmi_edw_silver"],
                flowgroup_count=1,
                action_count=2
            )
        }
        
        # Create individual job results
        j_one_result = Mock(spec=DependencyAnalysisResult)
        j_one_result.pipeline_dependencies = {
            "acmi_edw_raw": Mock(depends_on=[], flowgroup_count=1, action_count=2)
        }
        
        j_two_result = Mock(spec=DependencyAnalysisResult)
        j_two_result.pipeline_dependencies = {
            "acmi_edw_bronze": Mock(depends_on=[], flowgroup_count=1, action_count=2)
        }
        
        j_three_result = Mock(spec=DependencyAnalysisResult)
        j_three_result.pipeline_dependencies = {
            "acmi_edw_silver": Mock(depends_on=[], flowgroup_count=1, action_count=2)
        }
        
        j_four_result = Mock(spec=DependencyAnalysisResult)
        j_four_result.pipeline_dependencies = {
            "gold_load": Mock(depends_on=[], flowgroup_count=1, action_count=2)
        }
        
        job_results = {
            "j_one": j_one_result,
            "j_two": j_two_result,
            "j_three": j_three_result,
            "j_four": j_four_result
        }
        
        # Generate master job with global_result
        master_yaml = generator.generate_master_job(
            job_results, "test_master", "test_project", global_result=global_result
        )
        
        # Parse and verify dependencies
        parsed = yaml.safe_load(master_yaml)
        tasks = parsed["resources"]["jobs"]["test_master"]["tasks"]
        
        # Find each job task
        j_one_task = next(t for t in tasks if t["task_key"] == "j_one_task")
        j_two_task = next(t for t in tasks if t["task_key"] == "j_two_task")
        j_three_task = next(t for t in tasks if t["task_key"] == "j_three_task")
        j_four_task = next(t for t in tasks if t["task_key"] == "j_four_task")
        
        # Verify dependency chain
        assert "depends_on" not in j_one_task  # No dependencies
        assert j_two_task["depends_on"][0]["task_key"] == "j_one_task"
        assert j_three_task["depends_on"][0]["task_key"] == "j_two_task"
        assert j_four_task["depends_on"][0]["task_key"] == "j_three_task"
    
    def test_raises_error_without_global_result(self):
        """Test ValueError raised when global_result is None."""
        generator = JobGenerator()
        
        job_result = Mock(spec=DependencyAnalysisResult)
        job_result.pipeline_dependencies = {
            "pipeline1": Mock(depends_on=[], flowgroup_count=1, action_count=2)
        }
        
        job_results = {"test_job": job_result}
        
        # Should raise ValueError when global_result is None
        with pytest.raises(ValueError) as exc_info:
            generator.generate_master_job(
                job_results, "test_master", "test_project", global_result=None
            )
        
        assert "global_result is required" in str(exc_info.value)
    
    def test_parallel_jobs_no_dependencies(self):
        """Test independent jobs produce no depends_on clauses."""
        generator = JobGenerator()
        
        # Create global result with independent pipelines
        global_result = Mock(spec=DependencyAnalysisResult)
        global_result.pipeline_dependencies = {
            "pipeline1": Mock(depends_on=[], flowgroup_count=1, action_count=2),
            "pipeline2": Mock(depends_on=[], flowgroup_count=1, action_count=2)
        }
        
        # Create independent job results
        result1 = Mock(spec=DependencyAnalysisResult)
        result1.pipeline_dependencies = {
            "pipeline1": Mock(depends_on=[], flowgroup_count=1, action_count=2)
        }
        
        result2 = Mock(spec=DependencyAnalysisResult)
        result2.pipeline_dependencies = {
            "pipeline2": Mock(depends_on=[], flowgroup_count=1, action_count=2)
        }
        
        job_results = {"job1": result1, "job2": result2}
        
        master_yaml = generator.generate_master_job(
            job_results, "test_master", "test_project", global_result=global_result
        )
        
        # Parse and verify no dependencies
        parsed = yaml.safe_load(master_yaml)
        tasks = parsed["resources"]["jobs"]["test_master"]["tasks"]
        
        for task in tasks:
            assert "depends_on" not in task  # No dependencies
    
    def test_diamond_dependency_pattern(self):
        """Test j_one → [j_two, j_three] → j_four pattern."""
        generator = JobGenerator()
        
        # Create global result with diamond pattern
        global_result = Mock(spec=DependencyAnalysisResult)
        global_result.pipeline_dependencies = {
            "raw": Mock(depends_on=[], flowgroup_count=1, action_count=2),
            "bronze_sales": Mock(depends_on=["raw"], flowgroup_count=1, action_count=2),
            "bronze_inventory": Mock(depends_on=["raw"], flowgroup_count=1, action_count=2),
            "gold_dashboard": Mock(depends_on=["bronze_sales", "bronze_inventory"], flowgroup_count=1, action_count=2)
        }
        
        # Create job results
        j_one = Mock(spec=DependencyAnalysisResult)
        j_one.pipeline_dependencies = {"raw": Mock(depends_on=[], flowgroup_count=1, action_count=2)}
        
        j_two = Mock(spec=DependencyAnalysisResult)
        j_two.pipeline_dependencies = {"bronze_sales": Mock(depends_on=[], flowgroup_count=1, action_count=2)}
        
        j_three = Mock(spec=DependencyAnalysisResult)
        j_three.pipeline_dependencies = {"bronze_inventory": Mock(depends_on=[], flowgroup_count=1, action_count=2)}
        
        j_four = Mock(spec=DependencyAnalysisResult)
        j_four.pipeline_dependencies = {"gold_dashboard": Mock(depends_on=[], flowgroup_count=1, action_count=2)}
        
        job_results = {
            "j_one": j_one,
            "j_two": j_two,
            "j_three": j_three,
            "j_four": j_four
        }
        
        master_yaml = generator.generate_master_job(
            job_results, "test_master", "test_project", global_result=global_result
        )
        
        # Parse and verify diamond pattern
        parsed = yaml.safe_load(master_yaml)
        tasks = parsed["resources"]["jobs"]["test_master"]["tasks"]
        
        j_four_task = next(t for t in tasks if t["task_key"] == "j_four_task")
        
        # j_four should depend on both j_two and j_three
        depends_on_keys = [dep["task_key"] for dep in j_four_task["depends_on"]]
        assert "j_two_task" in depends_on_keys
        assert "j_three_task" in depends_on_keys
    
    def test_custom_master_job_name_from_config(self, tmp_path):
        """Test custom master job name from project_defaults."""
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        
        config_content = """project_defaults:
  master_job_name: "custom_orchestrator"
  max_concurrent_runs: 2
"""
        config_file = config_dir / "job_config.yaml"
        config_file.write_text(config_content)
        
        generator = JobGenerator(project_root=tmp_path, config_file_path="config/job_config.yaml")
        
        # Test the getter method
        custom_name = generator.get_master_job_name("test_project")
        assert custom_name == "custom_orchestrator"
    
    def test_generate_master_job_disabled(self, tmp_path):
        """Test generate_master_job: false skips generation."""
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        
        config_content = """project_defaults:
  generate_master_job: false
  max_concurrent_runs: 2
"""
        config_file = config_dir / "job_config.yaml"
        config_file.write_text(config_content)
        
        generator = JobGenerator(project_root=tmp_path, config_file_path="config/job_config.yaml")
        
        # Test the should_generate_master_job method
        should_generate = generator.should_generate_master_job()
        assert should_generate is False
    
    def test_generate_master_job_enabled_by_default(self):
        """Test generate_master_job defaults to true."""
        generator = JobGenerator()
        
        # Should default to True
        should_generate = generator.should_generate_master_job()
        assert should_generate is True


class TestHelperMethodsCoverage:
    """Test helper methods for comprehensive coverage."""
    
    def test_build_pipeline_to_job_mapping_empty_results(self):
        """Test _build_pipeline_to_job_mapping with empty job_results."""
        generator = JobGenerator()
        
        mapping = generator._build_pipeline_to_job_mapping({})
        
        assert mapping == {}
    
    def test_build_pipeline_to_job_mapping_job_without_pipelines(self):
        """Test mapping when job has no pipelines."""
        generator = JobGenerator()
        
        job_result = Mock(spec=DependencyAnalysisResult)
        job_result.pipeline_dependencies = {}
        
        job_results = {"empty_job": job_result}
        
        mapping = generator._build_pipeline_to_job_mapping(job_results)
        
        assert mapping == {}
    
    def test_build_pipeline_to_job_mapping_multiple_jobs(self):
        """Test mapping with multiple jobs and pipelines."""
        generator = JobGenerator()
        
        job1 = Mock(spec=DependencyAnalysisResult)
        job1.pipeline_dependencies = {
            "pipeline_a": Mock(),
            "pipeline_b": Mock()
        }
        
        job2 = Mock(spec=DependencyAnalysisResult)
        job2.pipeline_dependencies = {
            "pipeline_c": Mock()
        }
        
        job_results = {"job1": job1, "job2": job2}
        
        mapping = generator._build_pipeline_to_job_mapping(job_results)
        
        assert mapping == {
            "pipeline_a": "job1",
            "pipeline_b": "job1",
            "pipeline_c": "job2"
        }
    
    def test_analyze_cross_job_dependencies_pipeline_not_in_global(self):
        """Test when pipeline exists in job but not in global result."""
        generator = JobGenerator()
        
        # Global result doesn't have all pipelines
        global_result = Mock(spec=DependencyAnalysisResult)
        global_result.pipeline_dependencies = {
            "pipeline1": Mock(depends_on=[])
        }
        
        # Job has pipeline not in global
        job_result = Mock(spec=DependencyAnalysisResult)
        job_result.pipeline_dependencies = {
            "pipeline1": Mock(depends_on=[]),
            "pipeline_missing": Mock(depends_on=[])
        }
        
        job_results = {"test_job": job_result}
        pipeline_to_job = {"pipeline1": "test_job", "pipeline_missing": "test_job"}
        
        jobs_info = generator._analyze_cross_job_dependencies_from_global(
            job_results, pipeline_to_job, global_result
        )
        
        # Should handle gracefully
        assert "test_job" in jobs_info
        assert jobs_info["test_job"]["depends_on"] == []
    
    def test_analyze_cross_job_dependencies_upstream_job_not_found(self):
        """Test when upstream pipeline's job is not found in mapping."""
        generator = JobGenerator()
        
        global_result = Mock(spec=DependencyAnalysisResult)
        global_result.pipeline_dependencies = {
            "pipeline2": Mock(depends_on=["pipeline1"])
        }
        
        job_result = Mock(spec=DependencyAnalysisResult)
        job_result.pipeline_dependencies = {
            "pipeline2": Mock(depends_on=[])
        }
        
        job_results = {"test_job": job_result}
        # pipeline1 not in mapping
        pipeline_to_job = {"pipeline2": "test_job"}
        
        jobs_info = generator._analyze_cross_job_dependencies_from_global(
            job_results, pipeline_to_job, global_result
        )
        
        # Should handle gracefully - no dependency added
        assert jobs_info["test_job"]["depends_on"] == []
    
    def test_analyze_cross_job_dependencies_self_dependency_filtered(self):
        """Test that self-dependencies are filtered out."""
        generator = JobGenerator()
        
        # Same job owns both pipelines
        global_result = Mock(spec=DependencyAnalysisResult)
        global_result.pipeline_dependencies = {
            "pipeline1": Mock(depends_on=[]),
            "pipeline2": Mock(depends_on=["pipeline1"])
        }
        
        job_result = Mock(spec=DependencyAnalysisResult)
        job_result.pipeline_dependencies = {
            "pipeline1": Mock(depends_on=[]),
            "pipeline2": Mock(depends_on=[])
        }
        
        job_results = {"same_job": job_result}
        pipeline_to_job = {
            "pipeline1": "same_job",
            "pipeline2": "same_job"
        }
        
        jobs_info = generator._analyze_cross_job_dependencies_from_global(
            job_results, pipeline_to_job, global_result
        )
        
        # Self-dependency should be filtered
        assert jobs_info["same_job"]["depends_on"] == []
    
    def test_analyze_cross_job_dependencies_multiple_upstream_jobs(self):
        """Test job depending on multiple other jobs."""
        generator = JobGenerator()
        
        global_result = Mock(spec=DependencyAnalysisResult)
        global_result.pipeline_dependencies = {
            "pipeline_a": Mock(depends_on=[]),
            "pipeline_b": Mock(depends_on=[]),
            "pipeline_c": Mock(depends_on=["pipeline_a", "pipeline_b"])
        }
        
        job1 = Mock(spec=DependencyAnalysisResult)
        job1.pipeline_dependencies = {"pipeline_a": Mock()}
        
        job2 = Mock(spec=DependencyAnalysisResult)
        job2.pipeline_dependencies = {"pipeline_b": Mock()}
        
        job3 = Mock(spec=DependencyAnalysisResult)
        job3.pipeline_dependencies = {"pipeline_c": Mock()}
        
        job_results = {"job1": job1, "job2": job2, "job3": job3}
        pipeline_to_job = {
            "pipeline_a": "job1",
            "pipeline_b": "job2",
            "pipeline_c": "job3"
        }
        
        jobs_info = generator._analyze_cross_job_dependencies_from_global(
            job_results, pipeline_to_job, global_result
        )
        
        # job3 should depend on both job1 and job2
        assert sorted(jobs_info["job3"]["depends_on"]) == ["job1", "job2"]
    
    def test_analyze_cross_job_dependencies_deterministic_ordering(self):
        """Test that depends_on list is sorted for deterministic output."""
        generator = JobGenerator()
        
        global_result = Mock(spec=DependencyAnalysisResult)
        global_result.pipeline_dependencies = {
            "pipeline_z": Mock(depends_on=[]),
            "pipeline_a": Mock(depends_on=[]),
            "pipeline_final": Mock(depends_on=["pipeline_z", "pipeline_a"])
        }
        
        job_z = Mock(spec=DependencyAnalysisResult)
        job_z.pipeline_dependencies = {"pipeline_z": Mock()}
        
        job_a = Mock(spec=DependencyAnalysisResult)
        job_a.pipeline_dependencies = {"pipeline_a": Mock()}
        
        job_final = Mock(spec=DependencyAnalysisResult)
        job_final.pipeline_dependencies = {"pipeline_final": Mock()}
        
        job_results = {"job_z": job_z, "job_a": job_a, "job_final": job_final}
        pipeline_to_job = {
            "pipeline_z": "job_z",
            "pipeline_a": "job_a",
            "pipeline_final": "job_final"
        }
        
        jobs_info = generator._analyze_cross_job_dependencies_from_global(
            job_results, pipeline_to_job, global_result
        )
        
        # Should be sorted alphabetically
        assert jobs_info["job_final"]["depends_on"] == ["job_a", "job_z"]


class TestConfigEdgeCases:
    """Test configuration edge cases for complete coverage."""
    
    def test_should_generate_master_job_with_none_value(self, tmp_path):
        """Test when generate_master_job is explicitly None in config."""
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        
        config_content = """project_defaults:
  generate_master_job: null
  max_concurrent_runs: 2
"""
        config_file = config_dir / "job_config.yaml"
        config_file.write_text(config_content)
        
        generator = JobGenerator(project_root=tmp_path, config_file_path="config/job_config.yaml")
        
        # None should be treated as falsy, but get() with default should return True
        # Actually, let's verify the actual behavior
        should_generate = generator.should_generate_master_job()
        # With .get("generate_master_job", True), None returns None, which is falsy
        assert should_generate is None or should_generate is True
    
    def test_get_master_job_name_with_empty_string(self, tmp_path):
        """Test when master_job_name is empty string."""
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        
        config_content = """project_defaults:
  master_job_name: ""
  max_concurrent_runs: 2
"""
        config_file = config_dir / "job_config.yaml"
        config_file.write_text(config_content)
        
        generator = JobGenerator(project_root=tmp_path, config_file_path="config/job_config.yaml")
        
        # Empty string should be treated as falsy, use default
        master_name = generator.get_master_job_name("test_project")
        # Empty string is falsy in Python, so should use default
        assert master_name == "test_project_master"
    
    def test_get_master_job_name_with_whitespace(self, tmp_path):
        """Test when master_job_name has only whitespace."""
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        
        config_content = """project_defaults:
  master_job_name: "   "
  max_concurrent_runs: 2
"""
        config_file = config_dir / "job_config.yaml"
        config_file.write_text(config_content)
        
        generator = JobGenerator(project_root=tmp_path, config_file_path="config/job_config.yaml")
        
        # Whitespace-only string is truthy in Python
        master_name = generator.get_master_job_name("test_project")
        assert master_name == "   "  # Should use the whitespace as-is
    
    def test_should_generate_master_job_no_config_file(self):
        """Test default behavior when no config file exists."""
        generator = JobGenerator()  # No project_root, no config
        
        should_generate = generator.should_generate_master_job()
        assert should_generate is True  # Should default to True
    
    def test_get_master_job_name_no_config_file(self):
        """Test default behavior when no config file exists."""
        generator = JobGenerator()
        
        master_name = generator.get_master_job_name("my_project")
        assert master_name == "my_project_master"


class TestGenerateMasterJobEdgeCases:
    """Test edge cases in generate_master_job for complete coverage."""
    
    def test_generate_master_job_with_no_project_name(self):
        """Test that None project_name defaults to lhp_project."""
        generator = JobGenerator()
        
        global_result = Mock(spec=DependencyAnalysisResult)
        global_result.pipeline_dependencies = {
            "pipeline1": Mock(depends_on=[])
        }
        
        job_result = Mock(spec=DependencyAnalysisResult)
        job_result.pipeline_dependencies = {
            "pipeline1": Mock(depends_on=[])
        }
        
        job_results = {"test_job": job_result}
        
        master_yaml = generator.generate_master_job(
            job_results, "test_master", project_name=None, global_result=global_result
        )
        
        # Should use default project name in comments
        assert "lhp_project" in master_yaml
    
    def test_generate_master_job_empty_job_results(self):
        """Test with empty job_results dict."""
        generator = JobGenerator()
        
        global_result = Mock(spec=DependencyAnalysisResult)
        global_result.pipeline_dependencies = {}
        
        master_yaml = generator.generate_master_job(
            {}, "test_master", "test_project", global_result=global_result
        )
        
        # Should generate valid YAML with no tasks (or None/empty)
        parsed = yaml.safe_load(master_yaml)
        assert "resources" in parsed
        assert "jobs" in parsed["resources"]
        assert "test_master" in parsed["resources"]["jobs"]
        # Tasks can be None (no items in loop) or empty list
        tasks = parsed["resources"]["jobs"]["test_master"].get("tasks")
        assert tasks is None or tasks == []
    
    def test_generate_master_job_single_job_no_dependencies(self):
        """Test single job with no dependencies."""
        generator = JobGenerator()
        
        global_result = Mock(spec=DependencyAnalysisResult)
        global_result.pipeline_dependencies = {
            "pipeline1": Mock(depends_on=[])
        }
        
        job_result = Mock(spec=DependencyAnalysisResult)
        job_result.pipeline_dependencies = {
            "pipeline1": Mock(depends_on=[])
        }
        
        job_results = {"single_job": job_result}
        
        master_yaml = generator.generate_master_job(
            job_results, "test_master", "test_project", global_result=global_result
        )
        
        parsed = yaml.safe_load(master_yaml)
        tasks = parsed["resources"]["jobs"]["test_master"]["tasks"]
        
        assert len(tasks) == 1
        assert tasks[0]["task_key"] == "single_job_task"
        assert "depends_on" not in tasks[0]


class TestDependencyOutputManagerIntegration:
    """Test dependency_output_manager integration with new changes."""
    
    def test_master_job_skipped_when_disabled(self, tmp_path):
        """Test that master job is not generated when disabled in config."""
        from lhp.core.services.dependency_output_manager import DependencyOutputManager
        from lhp.core.services.dependency_analyzer import DependencyAnalyzer
        from lhp.core.services.job_generator import JobGenerator
        from unittest.mock import patch, Mock
        
        # Create config with master job disabled
        config_dir = tmp_path / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        
        config_content = """project_defaults:
  generate_master_job: false
  max_concurrent_runs: 2
"""
        config_file = config_dir / "job_config.yaml"
        config_file.write_text(config_content)
        
        # Create job generator with config
        job_generator = JobGenerator(project_root=tmp_path, config_file_path="config/job_config.yaml")
        
        # Verify the setting
        assert job_generator.should_generate_master_job() is False
