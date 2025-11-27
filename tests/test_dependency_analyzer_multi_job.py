"""Tests for DependencyAnalyzer multi-job analysis."""

import pytest
from pathlib import Path
from lhp.core.services.dependency_analyzer import DependencyAnalyzer
from lhp.models.config import FlowGroup, Action, ActionType
from lhp.utils.error_formatter import LHPError, ErrorCategory
from unittest.mock import Mock, patch
import tempfile


class TestAnalyzeDependenciesByJob:
    """Test analyze_dependencies_by_job method."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.mock_config_loader = Mock()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer._get_flowgroups')
    def test_single_job_mode_returns_one_result(self, mock_get_flowgroups, create_flowgroup):
        """Test that single-job mode returns dict with one entry."""
        # Create flowgroups WITHOUT job_name
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", None),
            create_flowgroup("bronze_pipeline", "fg2", None),
            create_flowgroup("silver_pipeline", "fg3", None),
        ]
        mock_get_flowgroups.return_value = flowgroups
        
        analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)
        
        results, global_result = analyzer.analyze_dependencies_by_job()
        
        # Should return single result with project_orchestration name
        assert len(results) == 1
        assert global_result is not None  # Should have global result too
        assert any("orchestration" in key for key in results.keys())
    
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer._get_flowgroups')
    def test_multi_job_mode_returns_multiple(self, mock_get_flowgroups, create_flowgroup):
        """Test that multi-job mode returns dict with multiple entries."""
        # Create flowgroups WITH different job_names
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", "bronze_job"),
            create_flowgroup("bronze_pipeline", "fg2", "bronze_job"),
            create_flowgroup("silver_pipeline", "fg3", "silver_job"),
        ]
        mock_get_flowgroups.return_value = flowgroups
        
        analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)
        
        results, global_result = analyzer.analyze_dependencies_by_job()
        
        # Should return 2 results
        assert len(results) == 2
        assert global_result is not None  # Should have global result
        assert "bronze_job" in results
        assert "silver_job" in results
    
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer._get_flowgroups')
    def test_validation_raises_LHPError_on_mixed_usage(self, mock_get_flowgroups, 
                                                       sample_flowgroups_mixed_job_name):
        """Test that job_name validation runs before analysis."""
        mock_get_flowgroups.return_value = sample_flowgroups_mixed_job_name
        
        analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)
        
        # Should raise LHPError from validator
        with pytest.raises(LHPError) as exc_info:
            results, global_result = analyzer.analyze_dependencies_by_job()
        
        assert exc_info.value.code == "LHP-VAL-002"
    
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer._get_flowgroups')
    def test_grouping_by_job_name_correct(self, mock_get_flowgroups, create_flowgroup):
        """Test that flowgroups are correctly grouped by job_name."""
        # Create 6 flowgroups: 3 with job_name=A, 3 with job_name=B
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", "job_a"),
            create_flowgroup("bronze_pipeline", "fg2", "job_a"),
            create_flowgroup("bronze_pipeline", "fg3", "job_a"),
            create_flowgroup("silver_pipeline", "fg4", "job_b"),
            create_flowgroup("silver_pipeline", "fg5", "job_b"),
            create_flowgroup("silver_pipeline", "fg6", "job_b"),
        ]
        mock_get_flowgroups.return_value = flowgroups
        
        analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)
        
        results, global_result = analyzer.analyze_dependencies_by_job()
        
        # Should have 2 results
        assert len(results) == 2
        assert "job_a" in results
        assert "job_b" in results
        
        # Each should have analyzed its pipelines
        assert results["job_a"].total_pipelines >= 1
        assert results["job_b"].total_pipelines >= 1
    
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer._get_flowgroups')
    def test_backward_compatibility_preserves_behavior(self, mock_get_flowgroups, create_flowgroup):
        """Test that backward compatibility is maintained."""
        # Create flowgroups without job_name (old behavior)
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", None),
            create_flowgroup("silver_pipeline", "fg2", None),
        ]
        mock_get_flowgroups.return_value = flowgroups
        
        analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)
        
        # Both methods should work
        single_result = analyzer.analyze_dependencies()
        multi_result, multi_global_result = analyzer.analyze_dependencies_by_job()
        
        # Multi-job should return single result in backward compat mode
        assert len(multi_result) == 1
        
        # The analysis should be similar
        assert single_result.total_pipelines == list(multi_result.values())[0].total_pipelines


class TestGlobalAndPerJobAnalysis:
    """Test global + per-job analysis flow."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.mock_config_loader = Mock()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer._get_flowgroups')
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer.analyze_dependencies')
    def test_global_analysis_runs_first(self, mock_analyze, mock_get_flowgroups, create_flowgroup):
        """Test that global analysis runs before per-job analysis."""
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", "bronze_job"),
            create_flowgroup("silver_pipeline", "fg2", "silver_job"),
        ]
        mock_get_flowgroups.return_value = flowgroups
        
        # Setup mock to return a result
        from lhp.models.dependencies import DependencyAnalysisResult
        mock_result = Mock(spec=DependencyAnalysisResult)
        mock_result.external_sources = []
        mock_result.pipeline_dependencies = {}
        mock_analyze.return_value = mock_result
        
        analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)
        
        results, global_result = analyzer.analyze_dependencies_by_job()
        
        # analyze_dependencies should be called: 1 for global + 2 for each job = 3 times
        assert mock_analyze.call_count == 3
    
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer._get_flowgroups')
    def test_external_sources_tracked_correctly(self, mock_get_flowgroups, create_flowgroup):
        """Test that external sources tracked at global and job levels."""
        # Create flowgroups where bronze reads external, silver reads bronze
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", "bronze_job", 
                           "external.source1", "bronze.table1"),
            create_flowgroup("silver_pipeline", "fg2", "silver_job",
                           "bronze.table1", "silver.table1"),
        ]
        mock_get_flowgroups.return_value = flowgroups
        
        analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)
        
        results, global_result = analyzer.analyze_dependencies_by_job()
        
        # bronze_job should have external.source1 as external
        assert "external.source1" in results["bronze_job"].external_sources
        
        # silver_job reads bronze.table1 which is produced by bronze_job
        # So bronze.table1 is external to silver_job (cross-job dependency)
        assert "bronze.table1" in results["silver_job"].external_sources
    
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer._get_flowgroups')
    def test_cross_job_sources_logged(self, mock_get_flowgroups, create_flowgroup, caplog):
        """Test that cross-job dependencies are logged."""
        import logging
        caplog.set_level(logging.INFO, logger="lhp.core.services.dependency_analyzer")
        
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", "bronze_job",
                           "raw.source", "bronze.table1"),
            create_flowgroup("silver_pipeline", "fg2", "silver_job",
                           "bronze.table1", "silver.table1"),
        ]
        mock_get_flowgroups.return_value = flowgroups
        
        analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)
        
        results, global_result = analyzer.analyze_dependencies_by_job()
        
        # Check for log messages about multi-job analysis
        assert "multi-job" in caplog.text.lower() or "job(s)" in caplog.text.lower()
    
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer._get_flowgroups')
    def test_job_external_vs_global_external(self, mock_get_flowgroups, create_flowgroup):
        """Test distinction between job-external and global-external sources."""
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", "bronze_job",
                           "external.global_source", "bronze.table1"),
            create_flowgroup("silver_pipeline", "fg2", "silver_job",
                           "bronze.table1", "silver.table1"),
        ]
        mock_get_flowgroups.return_value = flowgroups
        
        analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)
        
        results, global_result = analyzer.analyze_dependencies_by_job()
        
        # bronze_job: external.global_source is globally external
        assert "external.global_source" in results["bronze_job"].external_sources
        
        # silver_job: bronze.table1 is external to silver_job but internal to project
        assert "bronze.table1" in results["silver_job"].external_sources


class TestMultiJobEdgeCases:
    """Test edge cases for multi-job analysis."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.mock_config_loader = Mock()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer._get_flowgroups')
    def test_empty_flowgroups_returns_empty_dict(self, mock_get_flowgroups):
        """Test behavior with no flowgroups."""
        mock_get_flowgroups.return_value = []
        
        analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)
        
        results, global_result = analyzer.analyze_dependencies_by_job()
        
        # Should return empty dict
        assert results == {}
    
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer._get_flowgroups')
    def test_single_flowgroup_with_job_name(self, mock_get_flowgroups, create_flowgroup):
        """Test single flowgroup with job_name."""
        flowgroups = [create_flowgroup("bronze_pipeline", "fg1", "bronze_job")]
        mock_get_flowgroups.return_value = flowgroups
        
        analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)
        
        results, global_result = analyzer.analyze_dependencies_by_job()
        
        # Should return single job result
        assert len(results) == 1
        assert "bronze_job" in results
    
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer._get_flowgroups')
    def test_all_same_job_name_single_result(self, mock_get_flowgroups, create_flowgroup):
        """Test all flowgroups sharing same job_name."""
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", "bronze_job"),
            create_flowgroup("bronze_pipeline", "fg2", "bronze_job"),
            create_flowgroup("silver_pipeline", "fg3", "bronze_job"),
        ]
        mock_get_flowgroups.return_value = flowgroups
        
        analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)
        
        results, global_result = analyzer.analyze_dependencies_by_job()
        
        # Should return single result with all flowgroups
        assert len(results) == 1
        assert "bronze_job" in results
    
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer._get_flowgroups')
    def test_three_jobs_correct_grouping(self, mock_get_flowgroups, create_flowgroup):
        """Test correct grouping with three different jobs."""
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", "bronze_job"),
            create_flowgroup("bronze_pipeline", "fg2", "bronze_job"),
            create_flowgroup("silver_pipeline", "fg3", "silver_job"),
            create_flowgroup("silver_pipeline", "fg4", "silver_job"),
            create_flowgroup("gold_pipeline", "fg5", "gold_job"),
        ]
        mock_get_flowgroups.return_value = flowgroups
        
        analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)
        
        results, global_result = analyzer.analyze_dependencies_by_job()
        
        # Should have 3 results
        assert len(results) == 3
        assert "bronze_job" in results
        assert "silver_job" in results
        assert "gold_job" in results
    
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer._get_flowgroups')
    def test_none_vs_empty_string_job_name(self, mock_get_flowgroups, create_flowgroup):
        """Test that None and empty string are handled correctly."""
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", None),
            create_flowgroup("bronze_pipeline", "fg2", None),
        ]
        mock_get_flowgroups.return_value = flowgroups
        
        analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)
        
        # Should not raise, should use single-job mode
        results, global_result = analyzer.analyze_dependencies_by_job()
        
        assert len(results) == 1
