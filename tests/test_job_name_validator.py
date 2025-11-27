"""Tests for job_name validator."""

import pytest
from lhp.core.services.job_name_validator import validate_job_names, validate_job_name_format
from lhp.models.config import FlowGroup, Action, ActionType
from lhp.utils.error_formatter import LHPError, ErrorCategory


class TestJobNameFormatValidation:
    """Test job_name format validation."""
    
    @pytest.mark.parametrize("valid_name", [
        "bronze_job",
        "silver-transform",
        "GOLD_LAYER",
        "job123",
        "my_job-2",
        "a",
        "A1",
        "job_name_2024"
    ])
    def test_valid_job_name_formats(self, valid_name):
        """Test that valid job_name formats pass validation."""
        assert validate_job_name_format(valid_name) is True
    
    @pytest.mark.parametrize("invalid_name", [
        "job name",      # space
        "job@name",      # special char
        "job.name",      # dot
        "job$name",      # dollar
        "job#123",       # hash
        "job/path",      # slash
    ])
    def test_invalid_job_name_formats(self, invalid_name):
        """Test that invalid job_name formats fail validation."""
        assert validate_job_name_format(invalid_name) is False
    
    def test_empty_string_returns_false(self):
        """Test empty string explicitly returns False."""
        assert validate_job_name_format("") is False
    
    def test_none_value_returns_false(self):
        """Test None returns False."""
        assert validate_job_name_format(None) is False
    
    def test_boundary_cases(self):
        """Test boundary cases like very long names and single char."""
        # Very long name (should be valid)
        long_name = "a" * 200
        assert validate_job_name_format(long_name) is True
        
        # Single character (should be valid)
        assert validate_job_name_format("a") is True
        assert validate_job_name_format("1") is True


class TestJobNameAllOrNothingValidation:
    """Test all-or-nothing validation rule for job_name."""
    
    def test_all_flowgroups_have_job_name(self, sample_flowgroups_with_job_name):
        """Test valid case: all flowgroups have job_name."""
        # Should not raise
        validate_job_names(sample_flowgroups_with_job_name)
    
    def test_no_flowgroups_have_job_name(self, create_flowgroup):
        """Test valid case: no flowgroups have job_name."""
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", None),
            create_flowgroup("bronze_pipeline", "fg2", None),
            create_flowgroup("silver_pipeline", "fg3", None),
        ]
        # Should not raise
        validate_job_names(flowgroups)
    
    def test_mixed_usage_raises_LHPError_002(self, sample_flowgroups_mixed_job_name):
        """Test error case: some flowgroups have job_name, some don't."""
        with pytest.raises(LHPError) as exc_info:
            validate_job_names(sample_flowgroups_mixed_job_name)
        
        error = exc_info.value
        assert error.code == "LHP-VAL-002"
        assert "Inconsistent job_name usage" in error.title
    
    def test_error_message_shows_both_lists(self, sample_flowgroups_mixed_job_name):
        """Test that error message contains helpful information."""
        with pytest.raises(LHPError) as exc_info:
            validate_job_names(sample_flowgroups_mixed_job_name)
        
        error = exc_info.value
        # Should show flowgroups WITH job_name
        assert "fg1" in error.details
        assert "fg2" in error.details
        # Should show flowgroups WITHOUT job_name
        assert "fg3" in error.details
        assert "fg4" in error.details
        # Should have suggestions
        assert len(error.suggestions) > 0
    
    def test_invalid_format_raises_LHPError_001_first(self, create_flowgroup):
        """Test that format validation happens before all-or-nothing check."""
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", "bronze job"),  # Invalid: space
            create_flowgroup("bronze_pipeline", "fg2", "silver_job"),
        ]
        
        with pytest.raises(LHPError) as exc_info:
            validate_job_names(flowgroups)
        
        error = exc_info.value
        assert error.code == "LHP-VAL-001"  # Format error comes first
        assert "Invalid job_name format" in error.title
        assert "bronze job" in error.details


class TestJobNameValidationEdgeCases:
    """Test edge cases for job_name validation."""
    
    def test_empty_list_passes(self):
        """Test validation with empty flowgroups list."""
        # Should not raise
        validate_job_names([])
    
    def test_single_flowgroup_with_job_name(self, create_flowgroup):
        """Test single flowgroup with job_name (should pass)."""
        flowgroups = [create_flowgroup("bronze_pipeline", "fg1", "bronze_job")]
        # Should not raise
        validate_job_names(flowgroups)
    
    def test_duplicate_job_names_allowed(self, create_flowgroup):
        """Test multiple flowgroups with same job_name (should be allowed)."""
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", "bronze_job"),
            create_flowgroup("bronze_pipeline", "fg2", "bronze_job"),
            create_flowgroup("bronze_pipeline", "fg3", "bronze_job"),
        ]
        # Should not raise - grouping is allowed
        validate_job_names(flowgroups)
    
    def test_multiple_invalid_formats_all_listed(self, create_flowgroup):
        """Test that multiple invalid formats are all listed in error."""
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", "job name"),    # space
            create_flowgroup("bronze_pipeline", "fg2", "job@name"),    # special char
            create_flowgroup("bronze_pipeline", "fg3", "job.name"),    # dot
        ]
        
        with pytest.raises(LHPError) as exc_info:
            validate_job_names(flowgroups)
        
        error = exc_info.value
        assert error.code == "LHP-VAL-001"
        # All three should be listed
        assert "fg1" in error.details
        assert "fg2" in error.details
        assert "fg3" in error.details


class TestJobNameValidationLogging:
    """Test logging for job_name validation."""
    
    def test_logs_multi_job_success(self, caplog, create_flowgroup):
        """Test that successful validation logs appropriately."""
        import logging
        caplog.set_level(logging.INFO, logger="lhp.core.services.job_name_validator")
        
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", "bronze_job"),
            create_flowgroup("bronze_pipeline", "fg2", "bronze_job"),
            create_flowgroup("silver_pipeline", "fg3", "silver_job"),
            create_flowgroup("silver_pipeline", "fg4", "silver_job"),
            create_flowgroup("gold_pipeline", "fg5", "silver_job"),
        ]
        
        validate_job_names(flowgroups)
        
        # Check for info log about job count
        assert "job_name validation passed" in caplog.text
        assert "5 flowgroups" in caplog.text
        assert "2 job(s)" in caplog.text
        assert "bronze_job" in caplog.text
        assert "silver_job" in caplog.text
    
    def test_logs_single_job_mode(self, caplog, create_flowgroup):
        """Test that single-job mode is logged."""
        import logging
        caplog.set_level(logging.DEBUG, logger="lhp.core.services.job_name_validator")
        
        flowgroups = [
            create_flowgroup("bronze_pipeline", "fg1", None),
            create_flowgroup("bronze_pipeline", "fg2", None),
        ]
        
        validate_job_names(flowgroups)
        
        # Check for debug log about single-job mode
        assert "single-job mode" in caplog.text.lower()
        assert "2 flowgroups" in caplog.text
