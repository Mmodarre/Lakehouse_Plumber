"""Tests for file pattern matching utility used in include functionality."""

import pytest
from pathlib import Path
from unittest.mock import Mock

# Note: The actual utility will be implemented later - these tests define the expected behavior


class TestFilePatternMatching:
    """Test cases for file pattern matching utility."""

    def test_exact_file_match(self):
        """Test exact file matching."""
        # Given: A specific file pattern
        pattern = "customers.yaml"
        files = [
            Path("pipelines/bronze/customers.yaml"),
            Path("pipelines/bronze/orders.yaml"),
            Path("pipelines/silver/customers.yaml"),
        ]
        
        # When: Matching against the pattern
        # Expected: Only files with exact name match
        expected = [
            Path("pipelines/bronze/customers.yaml"),
            Path("pipelines/silver/customers.yaml"),
        ]
        
        # This will be implemented later
        # assert match_patterns([pattern], files) == expected

    def test_wildcard_matching(self):
        """Test wildcard pattern matching."""
        # Given: A wildcard pattern
        pattern = "bronze_*.yaml"
        files = [
            Path("pipelines/ingestion/bronze_customers.yaml"),
            Path("pipelines/ingestion/bronze_orders.yaml"),
            Path("pipelines/ingestion/silver_customers.yaml"),
            Path("pipelines/ingestion/bronze_products.yml"),  # Different extension
        ]
        
        # When: Matching against the pattern
        # Expected: Only files matching the wildcard pattern
        expected = [
            Path("pipelines/ingestion/bronze_customers.yaml"),
            Path("pipelines/ingestion/bronze_orders.yaml"),
        ]
        
        # This will be implemented later
        # assert match_patterns([pattern], files) == expected

    def test_recursive_glob_matching(self):
        """Test recursive glob pattern matching."""
        # Given: A recursive glob pattern
        pattern = "bronze/**/*.yaml"
        files = [
            Path("pipelines/bronze/customers.yaml"),
            Path("pipelines/bronze/orders.yaml"),
            Path("pipelines/bronze/raw/events.yaml"),
            Path("pipelines/bronze/raw/deep/nested.yaml"),
            Path("pipelines/silver/customers.yaml"),
            Path("pipelines/bronze/customers.yml"),  # Different extension
        ]
        
        # When: Matching against the pattern
        # Expected: Only files in bronze directory tree with .yaml extension
        expected = [
            Path("pipelines/bronze/customers.yaml"),
            Path("pipelines/bronze/orders.yaml"),
            Path("pipelines/bronze/raw/events.yaml"),
            Path("pipelines/bronze/raw/deep/nested.yaml"),
        ]
        
        # This will be implemented later
        # assert match_patterns([pattern], files) == expected

    def test_directory_specific_matching(self):
        """Test directory-specific pattern matching."""
        # Given: A directory-specific pattern
        pattern = "pipelines/bronze/*.yaml"
        files = [
            Path("pipelines/bronze/customers.yaml"),
            Path("pipelines/bronze/orders.yaml"),
            Path("pipelines/bronze/raw/events.yaml"),  # Nested, shouldn't match
            Path("pipelines/silver/customers.yaml"),
        ]
        
        # When: Matching against the pattern
        # Expected: Only direct files in bronze directory
        expected = [
            Path("pipelines/bronze/customers.yaml"),
            Path("pipelines/bronze/orders.yaml"),
        ]
        
        # This will be implemented later
        # assert match_patterns([pattern], files) == expected

    def test_multiple_patterns(self):
        """Test matching against multiple patterns."""
        # Given: Multiple patterns
        patterns = ["bronze_*.yaml", "silver_*.yaml"]
        files = [
            Path("pipelines/bronze_customers.yaml"),
            Path("pipelines/bronze_orders.yaml"),
            Path("pipelines/silver_customers.yaml"),
            Path("pipelines/gold_customers.yaml"),
        ]
        
        # When: Matching against multiple patterns
        # Expected: Files matching any of the patterns
        expected = [
            Path("pipelines/bronze_customers.yaml"),
            Path("pipelines/bronze_orders.yaml"),
            Path("pipelines/silver_customers.yaml"),
        ]
        
        # This will be implemented later
        # assert match_patterns(patterns, files) == expected

    def test_empty_patterns_list(self):
        """Test behavior with empty patterns list."""
        # Given: Empty patterns list
        patterns = []
        files = [
            Path("pipelines/bronze/customers.yaml"),
            Path("pipelines/silver/orders.yaml"),
        ]
        
        # When: Matching against empty patterns
        # Expected: All files should be included (no filtering)
        expected = files
        
        # This will be implemented later
        # assert match_patterns(patterns, files) == expected

    def test_no_matches(self):
        """Test behavior when no files match patterns."""
        # Given: Pattern that doesn't match any files
        pattern = "nonexistent_*.yaml"
        files = [
            Path("pipelines/bronze/customers.yaml"),
            Path("pipelines/silver/orders.yaml"),
        ]
        
        # When: Matching against non-matching pattern
        # Expected: Empty list
        expected = []
        
        # This will be implemented later
        # assert match_patterns([pattern], files) == expected

    def test_case_sensitivity(self):
        """Test case sensitivity in pattern matching."""
        # Given: A pattern with specific case
        pattern = "Bronze_*.yaml"
        files = [
            Path("pipelines/Bronze_customers.yaml"),
            Path("pipelines/bronze_customers.yaml"),
            Path("pipelines/BRONZE_customers.yaml"),
        ]
        
        # When: Matching against the pattern
        # Expected: Only exact case matches (Unix filesystem behavior)
        expected = [
            Path("pipelines/Bronze_customers.yaml"),
        ]
        
        # This will be implemented later
        # assert match_patterns([pattern], files) == expected

    def test_both_yaml_and_yml_extensions(self):
        """Test matching both .yaml and .yml extensions."""
        # Given: Patterns for both extensions
        patterns = ["*.yaml", "*.yml"]
        files = [
            Path("pipelines/customers.yaml"),
            Path("pipelines/orders.yml"),
            Path("pipelines/products.json"),
        ]
        
        # When: Matching against both extension patterns
        # Expected: Files with both extensions
        expected = [
            Path("pipelines/customers.yaml"),
            Path("pipelines/orders.yml"),
        ]
        
        # This will be implemented later
        # assert match_patterns(patterns, files) == expected

    def test_complex_nested_patterns(self):
        """Test complex nested directory patterns."""
        # Given: Complex nested patterns
        patterns = [
            "pipelines/*/bronze/*.yaml",
            "pipelines/ingestion/**/raw_*.yaml"
        ]
        files = [
            Path("pipelines/data/bronze/customers.yaml"),
            Path("pipelines/events/bronze/orders.yaml"),
            Path("pipelines/data/silver/customers.yaml"),
            Path("pipelines/ingestion/raw_events.yaml"),
            Path("pipelines/ingestion/subfolder/raw_logs.yaml"),
            Path("pipelines/ingestion/deep/nested/raw_metrics.yaml"),
        ]
        
        # When: Matching against complex patterns
        # Expected: Files matching either pattern
        expected = [
            Path("pipelines/data/bronze/customers.yaml"),
            Path("pipelines/events/bronze/orders.yaml"),
            Path("pipelines/ingestion/raw_events.yaml"),
            Path("pipelines/ingestion/subfolder/raw_logs.yaml"),
            Path("pipelines/ingestion/deep/nested/raw_metrics.yaml"),
        ]
        
        # This will be implemented later
        # assert match_patterns(patterns, files) == expected

    def test_edge_case_dot_patterns(self):
        """Test edge cases with dot patterns."""
        # Given: Patterns with dots and special characters
        patterns = [
            "*.yaml",
            "test.*.yaml",
            "**/*.yaml"
        ]
        files = [
            Path("test.yaml"),
            Path("test.config.yaml"),
            Path("test.backup.yaml"),
            Path("subfolder/test.yaml"),
            Path("deep/nested/test.yaml"),
        ]
        
        # When: Matching against dot patterns
        # Expected: All files should match at least one pattern
        expected = files
        
        # This will be implemented later
        # assert match_patterns(patterns, files) == expected

    def test_absolute_vs_relative_paths(self):
        """Test pattern matching with absolute vs relative paths."""
        # Given: Patterns and files with different path types
        pattern = "pipelines/**/*.yaml"
        files = [
            Path("pipelines/bronze/customers.yaml"),
            Path("/absolute/pipelines/bronze/customers.yaml"),
            Path("./pipelines/bronze/customers.yaml"),
        ]
        
        # When: Matching against the pattern
        # Expected: Only relative paths matching the pattern
        expected = [
            Path("pipelines/bronze/customers.yaml"),
            Path("./pipelines/bronze/customers.yaml"),
        ]
        
        # This will be implemented later
        # assert match_patterns([pattern], files) == expected

    def test_pattern_validation(self):
        """Test validation of pattern strings."""
        # Given: Various pattern strings
        valid_patterns = [
            "*.yaml",
            "pipelines/**/*.yaml",
            "bronze_*.yaml",
            "data/*/bronze/*.yaml"
        ]
        
        invalid_patterns = [
            "",  # Empty pattern
            None,  # None pattern
            "invalid[pattern",  # Invalid regex
        ]
        
        # When: Validating patterns
        # Expected: Valid patterns pass, invalid ones raise errors
        
        # This will be implemented later
        # for pattern in valid_patterns:
        #     assert validate_pattern(pattern) is True
        # 
        # for pattern in invalid_patterns:
        #     with pytest.raises(ValueError):
        #         validate_pattern(pattern)

    def test_performance_with_large_file_lists(self):
        """Test performance with large numbers of files."""
        # Given: Large number of files
        patterns = ["bronze_*.yaml", "silver_*.yaml"]
        files = []
        
        # Create 1000 test files
        for i in range(1000):
            if i % 3 == 0:
                files.append(Path(f"pipelines/bronze_file_{i}.yaml"))
            elif i % 3 == 1:
                files.append(Path(f"pipelines/silver_file_{i}.yaml"))
            else:
                files.append(Path(f"pipelines/gold_file_{i}.yaml"))
        
        # When: Matching against patterns
        # Expected: Should complete in reasonable time and return correct results
        
        # This will be implemented later
        # import time
        # start_time = time.time()
        # result = match_patterns(patterns, files)
        # end_time = time.time()
        # 
        # assert end_time - start_time < 1.0  # Should complete in under 1 second
        # assert len(result) == 667  # 2/3 of the files should match


class TestFilePatternMatchingIntegration:
    """Integration tests for file pattern matching with actual file system."""

    def test_real_file_system_matching(self, tmp_path):
        """Test pattern matching against real file system."""
        # Given: Real files in temporary directory
        pipelines_dir = tmp_path / "pipelines"
        bronze_dir = pipelines_dir / "bronze"
        silver_dir = pipelines_dir / "silver"
        
        bronze_dir.mkdir(parents=True)
        silver_dir.mkdir(parents=True)
        
        # Create test files
        (bronze_dir / "customers.yaml").write_text("pipeline: bronze\nflowgroup: customers")
        (bronze_dir / "orders.yaml").write_text("pipeline: bronze\nflowgroup: orders")
        (silver_dir / "customers.yaml").write_text("pipeline: silver\nflowgroup: customers")
        (pipelines_dir / "config.json").write_text("{}")
        
        # When: Discovering files with patterns
        patterns = ["bronze/*.yaml"]
        
        # Expected: Should find only bronze YAML files
        # This will be implemented later
        # discovered_files = discover_files_with_patterns(pipelines_dir, patterns)
        # expected = [bronze_dir / "customers.yaml", bronze_dir / "orders.yaml"]
        # assert set(discovered_files) == set(expected)

    def test_backwards_compatibility_no_patterns(self, tmp_path):
        """Test backwards compatibility when no patterns are specified."""
        # Given: Real files and no patterns
        pipelines_dir = tmp_path / "pipelines"
        bronze_dir = pipelines_dir / "bronze"
        bronze_dir.mkdir(parents=True)
        
        (bronze_dir / "customers.yaml").write_text("pipeline: bronze\nflowgroup: customers")
        (bronze_dir / "orders.yaml").write_text("pipeline: bronze\nflowgroup: orders")
        
        # When: Discovering files without patterns (backwards compatibility)
        patterns = []
        
        # Expected: Should find all YAML files (current behavior)
        # This will be implemented later
        # discovered_files = discover_files_with_patterns(pipelines_dir, patterns)
        # expected = [bronze_dir / "customers.yaml", bronze_dir / "orders.yaml"]
        # assert set(discovered_files) == set(expected) 