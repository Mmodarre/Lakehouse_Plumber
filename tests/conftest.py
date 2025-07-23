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