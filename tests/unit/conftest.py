"""Test configuration and shared fixtures."""

import pytest
import logging
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch
import sys


def force_close_all_log_handlers():
    """Force close all logging handlers to release file locks on Windows."""
    root_logger = logging.getLogger()
    
    # Close and remove all handlers
    for handler in root_logger.handlers[:]:
        try:
            handler.close()
        except Exception:
            pass  # Ignore errors during cleanup
        root_logger.removeHandler(handler)
    
    # Reset logging configuration
    logging.basicConfig(force=True)


@pytest.fixture(autouse=True)
def clean_logging():
    """Automatically clean up logging for all tests to prevent Windows file locking."""
    # Setup: Clean logging state before test
    force_close_all_log_handlers()
    
    yield
    
    # Teardown: Clean logging state after test
    force_close_all_log_handlers()


@pytest.fixture
def isolated_project():
    """Create a completely isolated temporary project directory with proper cleanup."""
    temp_dir = None
    try:
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
    finally:
        if temp_dir and temp_dir.exists():
            # Force close any log handlers before cleanup
            force_close_all_log_handlers()
            
            # Try to remove directory, with special handling for Windows
            try:
                shutil.rmtree(temp_dir)
            except PermissionError as e:
                if sys.platform == "win32":
                    # On Windows, try to handle file locking issues
                    import time
                    import gc
                    
                    # Force garbage collection to close any remaining file handles
                    gc.collect()
                    time.sleep(0.1)  # Brief pause to let Windows release locks
                    
                    try:
                        shutil.rmtree(temp_dir)
                    except PermissionError:
                        # Last resort: rename the directory and let system cleanup later
                        try:
                            temp_dir.rename(temp_dir.with_suffix('.cleanup'))
                        except Exception:
                            pass  # Give up gracefully
                else:
                    raise


@pytest.fixture
def mock_logging_config():
    """Mock the configure_logging function to prevent file creation during tests."""
    with patch('lhp.cli.main.configure_logging') as mock_config:
        # Return a mock log file path that doesn't actually create files
        mock_config.return_value = Path(tempfile.gettempdir()) / "mock_lhp.log"
        yield mock_config


@pytest.fixture
def temp_project_with_logging_cleanup():
    """Create a temporary project with proper logging cleanup."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)
        
        # Ensure logging is configured to not interfere
        force_close_all_log_handlers()
        
        # Configure minimal logging for tests
        logging.basicConfig(
            level=logging.WARNING,  # Reduce log noise in tests
            format='%(levelname)s: %(message)s',
            force=True
        )
        
        yield project_root
        
        # Clean up logging before directory deletion
        force_close_all_log_handlers()


# Platform-specific fixtures
@pytest.fixture
def windows_safe_tempdir():
    """Create a temporary directory with Windows-safe cleanup."""
    if sys.platform != "win32":
        # On non-Windows platforms, use standard tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    else:
        # On Windows, use custom cleanup logic
        temp_dir = Path(tempfile.mkdtemp())
        try:
            yield temp_dir
        finally:
            # Windows-specific cleanup
            force_close_all_log_handlers()
            
            # Try multiple cleanup strategies
            for attempt in range(3):
                try:
                    shutil.rmtree(temp_dir)
                    break
                except PermissionError:
                    if attempt < 2:
                        import time
                        import gc
                        gc.collect()
                        time.sleep(0.1 * (attempt + 1))
                    else:
                        # Final attempt: rename and let OS clean up later
                        try:
                            temp_dir.rename(temp_dir.with_suffix(f'.cleanup.{attempt}'))
                        except Exception:
                            pass


@pytest.fixture(autouse=True, scope="session")
def configure_test_logging():
    """Configure logging for the entire test session."""
    # Set up minimal logging configuration for tests
    logging.getLogger('lhp').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.ERROR)
    logging.getLogger('requests').setLevel(logging.ERROR)
    
    yield
    
    # Final cleanup at end of test session
    force_close_all_log_handlers()


# ============================================================================
# Multi-Job Orchestration Test Fixtures
# ============================================================================

@pytest.fixture
def create_flowgroup():
    """Factory fixture to create FlowGroup with minimal valid structure."""
    from lhp.models.config import FlowGroup, Action, ActionType
    
    def _create(pipeline: str, flowgroup: str, job_name: str = None, 
                source_table: str = "raw.source", target_table: str = "bronze.target"):
        return FlowGroup(
            pipeline=pipeline,
            flowgroup=flowgroup,
            job_name=job_name,
            actions=[
                Action(
                    name=f"load_{flowgroup}",
                    type=ActionType.LOAD,
                    source=source_table,
                    target=f"v_{flowgroup}_raw"
                ),
                Action(
                    name=f"write_{flowgroup}",
                    type=ActionType.WRITE,
                    source=f"v_{flowgroup}_raw",
                    write_target={
                        "type": "streaming_table",
                        "database": "bronze",
                        "table": target_table
                    }
                )
            ]
        )
    return _create


@pytest.fixture
def sample_flowgroups_with_job_name(create_flowgroup):
    """3 flowgroups: 2 bronze, 1 silver."""
    return [
        create_flowgroup("bronze_pipeline", "bronze_fg1", "bronze_job", "raw.table1", "bronze.table1"),
        create_flowgroup("bronze_pipeline", "bronze_fg2", "bronze_job", "raw.table2", "bronze.table2"),
        create_flowgroup("silver_pipeline", "silver_fg1", "silver_job", "bronze.table1", "silver.table1"),
    ]


@pytest.fixture
def sample_flowgroups_mixed_job_name(create_flowgroup):
    """4 flowgroups: 2 with job_name, 2 without."""
    return [
        create_flowgroup("bronze_pipeline", "fg1", "bronze_job"),
        create_flowgroup("bronze_pipeline", "fg2", "bronze_job"),
        create_flowgroup("silver_pipeline", "fg3", None),
        create_flowgroup("silver_pipeline", "fg4", None),
    ]


@pytest.fixture
def sample_multi_doc_job_config(tmp_path):
    """Creates temp multi-doc job_config.yaml at tmp_path/config/job_config.yaml."""
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    
    config_content = """project_defaults:
  max_concurrent_runs: 1
  performance_target: STANDARD
  tags:
    env: dev
    managed_by: lhp
---
job_name: bronze_job
max_concurrent_runs: 2
tags:
  layer: bronze
---
job_name: silver_job
performance_target: PERFORMANCE_OPTIMIZED
tags:
  layer: silver
"""
    config_file = config_dir / "job_config.yaml"
    config_file.write_text(config_content)
    return tmp_path  # Return tmp_path (project root)


@pytest.fixture
def mock_dependency_result():
    """Creates mock DependencyAnalysisResult with required attributes."""
    from lhp.models.dependencies import DependencyAnalysisResult
    from unittest.mock import Mock
    
    result = Mock(spec=DependencyAnalysisResult)
    result.total_pipelines = 2
    result.execution_stages = [["pipeline1"], ["pipeline2"]]
    result.pipeline_dependencies = {
        "pipeline1": Mock(depends_on=[], flowgroup_count=1, action_count=2, external_sources=[], stage=1),
        "pipeline2": Mock(depends_on=["pipeline1"], flowgroup_count=1, action_count=2, external_sources=[], stage=2)
    }
    result.external_sources = []
    result.circular_dependencies = []
    return result 