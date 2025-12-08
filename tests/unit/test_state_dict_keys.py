"""Unit tests for dictionary key normalization in state management."""

import tempfile
import json
from pathlib import Path
import pytest
from lhp.core.state.state_persistence import StatePersistence
from lhp.core.state.dependency_tracker import DependencyTracker
from lhp.core.state_models import ProjectState, FileState, DependencyInfo


def test_file_state_dictionary_key_normalization():
    """Test that FileState is stored with forward-slash keys regardless of OS."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)
        
        # Create project structure
        (project_root / "pipelines").mkdir()
        (project_root / "generated" / "dev").mkdir(parents=True)
        yaml_file = project_root / "pipelines" / "test.yaml"
        yaml_file.write_text("test: data")
        generated_file = project_root / "generated" / "dev" / "test.py"
        generated_file.write_text("# test")
        
        # Initialize tracker
        tracker = DependencyTracker(project_root)
        state = ProjectState()
        
        # Track with forward slashes (Mac/Linux style)
        tracker.track_generated_file(
            state=state,
            generated_path=Path("generated/dev/test.py"),
            source_yaml=Path("pipelines/test.yaml"),
            environment="dev",
            pipeline="test_pipeline",
            flowgroup="test_fg"
        )
        
        # Verify key uses forward slashes
        assert "generated/dev/test.py" in state.environments["dev"]
        assert "generated\\dev\\test.py" not in state.environments["dev"]


def test_state_file_load_normalizes_windows_keys():
    """Test that loading a Windows-generated state file normalizes keys on Mac."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)
        state_file = project_root / ".lhp_state.json"
        
        # Create a mock Windows-style state file
        windows_state = {
            "version": "1.0",
            "last_updated": "2025-01-01T00:00:00",
            "environments": {
                "dev": {
                    "generated\\dev\\pipeline\\file.py": {
                        "source_yaml": "pipelines\\test.yaml",
                        "generated_path": "generated/dev/pipeline/file.py",
                        "checksum": "abc123",
                        "source_yaml_checksum": "def456",
                        "timestamp": "2025-01-01T00:00:00",
                        "environment": "dev",
                        "pipeline": "pipeline",
                        "flowgroup": "flowgroup",
                        "file_dependencies": {
                            "py_functions\\module.py": {
                                "path": "py_functions/module.py",
                                "checksum": "xyz789",
                                "type": "external_file",
                                "last_modified": "2025-01-01T00:00:00"
                            }
                        },
                        "file_composite_checksum": "composite123"
                    }
                }
            }
        }
        
        # Write Windows-style state file
        with open(state_file, "w") as f:
            json.dump(windows_state, f)
        
        # Load state
        persistence = StatePersistence(project_root)
        state = persistence.load_state()
        
        # Verify keys are normalized to forward slashes
        assert "generated/dev/pipeline/file.py" in state.environments["dev"]
        assert "generated\\dev\\pipeline\\file.py" not in state.environments["dev"]
        
        # Verify file dependency keys are normalized
        file_state = state.environments["dev"]["generated/dev/pipeline/file.py"]
        assert "py_functions/module.py" in file_state.file_dependencies
        assert "py_functions\\module.py" not in file_state.file_dependencies


def test_global_dependency_keys_normalized():
    """Test that global dependencies use forward-slash keys."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)
        
        # Create substitution file
        (project_root / "substitutions").mkdir()
        sub_file = project_root / "substitutions" / "dev.yaml"
        sub_file.write_text("test: value")
        
        # Create project config
        config_file = project_root / "lhp.yaml"
        config_file.write_text("config: data")
        
        # Resolve global dependencies
        from lhp.core.state_dependency_resolver import StateDependencyResolver
        resolver = StateDependencyResolver(project_root)
        global_deps = resolver.resolve_global_dependencies("dev")
        
        # Verify keys use forward slashes
        assert "substitutions/dev.yaml" in global_deps
        assert "lhp.yaml" in global_deps
        assert "substitutions\\dev.yaml" not in global_deps


def test_cross_platform_state_lookup():
    """Test that lookups work after storing and loading across platforms."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)
        
        # Create project structure
        (project_root / "pipelines").mkdir()
        (project_root / "generated" / "dev" / "test_pipeline").mkdir(parents=True)
        yaml_file = project_root / "pipelines" / "test.yaml"
        yaml_file.write_text("test: data")
        generated_file = project_root / "generated" / "dev" / "test_pipeline" / "output.py"
        generated_file.write_text("# test")
        
        # Initialize tracker and persistence
        tracker = DependencyTracker(project_root)
        persistence = StatePersistence(project_root)
        state = ProjectState()
        
        # Track a file
        tracker.track_generated_file(
            state=state,
            generated_path=Path("generated/dev/test_pipeline/output.py"),
            source_yaml=Path("pipelines/test.yaml"),
            environment="dev",
            pipeline="test_pipeline",
            flowgroup="test_fg"
        )
        
        # Save state
        persistence.save_state(state)
        
        # Load state (simulating loading on different OS)
        loaded_state = persistence.load_state()
        
        # Verify lookup works with forward slashes
        assert "generated/dev/test_pipeline/output.py" in loaded_state.environments["dev"]
        
        # Verify we can retrieve the FileState
        file_state = loaded_state.environments["dev"]["generated/dev/test_pipeline/output.py"]
        assert file_state.source_yaml == "pipelines/test.yaml"
        assert file_state.generated_path == "generated/dev/test_pipeline/output.py"


def test_mixed_separator_state_file_normalization():
    """Test that a state file with mixed separators is normalized on load."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)
        state_file = project_root / ".lhp_state.json"
        
        # Create a state file with mixed separators (could happen from manual edits)
        mixed_state = {
            "version": "1.0",
            "last_updated": "2025-01-01T00:00:00",
            "environments": {
                "dev": {
                    "generated/dev/file1.py": {  # Forward slashes
                        "source_yaml": "pipelines/test1.yaml",
                        "generated_path": "generated/dev/file1.py",
                        "checksum": "abc123",
                        "source_yaml_checksum": "def456",
                        "timestamp": "2025-01-01T00:00:00",
                        "environment": "dev",
                        "pipeline": "pipeline",
                        "flowgroup": "flowgroup1",
                        "file_dependencies": {},
                        "file_composite_checksum": "composite123"
                    },
                    "generated\\dev\\file2.py": {  # Backslashes
                        "source_yaml": "pipelines\\test2.yaml",
                        "generated_path": "generated\\dev\\file2.py",
                        "checksum": "ghi789",
                        "source_yaml_checksum": "jkl012",
                        "timestamp": "2025-01-01T00:00:00",
                        "environment": "dev",
                        "pipeline": "pipeline",
                        "flowgroup": "flowgroup2",
                        "file_dependencies": {},
                        "file_composite_checksum": "composite456"
                    }
                }
            }
        }
        
        # Write mixed state file
        with open(state_file, "w") as f:
            json.dump(mixed_state, f)
        
        # Load state
        persistence = StatePersistence(project_root)
        state = persistence.load_state()
        
        # Verify all keys are normalized to forward slashes
        env_files = state.environments["dev"]
        assert len(env_files) == 2
        assert "generated/dev/file1.py" in env_files
        assert "generated/dev/file2.py" in env_files  # Should be normalized
        assert "generated\\dev\\file2.py" not in env_files
        
        # Verify both files are accessible
        file1 = env_files["generated/dev/file1.py"]
        file2 = env_files["generated/dev/file2.py"]
        assert file1.flowgroup == "flowgroup1"
        assert file2.flowgroup == "flowgroup2"

