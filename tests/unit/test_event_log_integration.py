"""
Integration tests for declarative event_log configuration.

Tests the full round-trip: lhp.yaml event_log → BundleManager → rendered YAML,
including substitution token resolution, pipeline_config overrides, and backward compatibility.
"""

import pytest
import tempfile
import shutil
from pathlib import Path

import yaml

from lhp.bundle.manager import BundleManager
from lhp.models.config import EventLogConfig, ProjectConfig


class TestEventLogIntegration:
    """Full round-trip integration tests for event_log injection."""

    @pytest.fixture
    def temp_project(self):
        """Create temporary project structure with substitution files."""
        temp_dir = tempfile.mkdtemp()
        project_root = Path(temp_dir)

        # Create directories
        (project_root / "substitutions").mkdir(parents=True)
        (project_root / "config").mkdir(parents=True)
        (project_root / "pipelines").mkdir(parents=True)
        (project_root / "generated").mkdir(parents=True)

        # Create dev substitution file
        sub_content = {
            "dev": {
                "catalog": "dev_catalog",
                "schema": "dev_schema",
                "event_log_catalog": "dev_meta",
                "event_log_schema": "meta_dev",
            }
        }
        with open(project_root / "substitutions" / "dev.yaml", "w") as f:
            yaml.dump(sub_content, f)

        yield project_root

        shutil.rmtree(temp_dir)

    def _create_pipeline_config(self, project_root, content):
        """Helper to write a pipeline_config.yaml file."""
        config_path = project_root / "config" / "pipeline_config.yaml"
        with open(config_path, "w") as f:
            f.write(content)
        return str(config_path)

    def test_full_roundtrip_event_log_injected(self, temp_project):
        """Test event_log from lhp.yaml appears in rendered resource YAML."""
        event_log = EventLogConfig(
            catalog="my_meta_catalog",
            schema="_meta",
            name_suffix="_event_log",
        )
        project_config = ProjectConfig(name="test", event_log=event_log)

        manager = BundleManager(
            project_root=temp_project,
            project_config=project_config,
        )

        # Capture the template context
        captured_context = None

        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"

        manager.template_renderer.render_template = mock_render

        output_dir = temp_project / "generated"
        manager.generate_resource_file_content("bronze_load", output_dir, "dev")

        assert captured_context is not None
        config = captured_context["pipeline_config"]
        assert "event_log" in config
        assert config["event_log"]["name"] == "bronze_load_event_log"
        assert config["event_log"]["catalog"] == "my_meta_catalog"
        assert config["event_log"]["schema"] == "_meta"

    def test_substitution_tokens_resolved_in_event_log(self, temp_project):
        """Test that LHP tokens in event_log catalog/schema are resolved by substitution."""
        event_log = EventLogConfig(
            catalog="${event_log_catalog}",
            schema="${event_log_schema}",
        )
        project_config = ProjectConfig(name="test", event_log=event_log)

        manager = BundleManager(
            project_root=temp_project,
            project_config=project_config,
        )

        captured_context = None

        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"

        manager.template_renderer.render_template = mock_render

        output_dir = temp_project / "generated"
        manager.generate_resource_file_content("my_pipeline", output_dir, "dev")

        assert captured_context is not None
        config = captured_context["pipeline_config"]
        assert config["event_log"]["catalog"] == "dev_meta"
        assert config["event_log"]["schema"] == "meta_dev"

    def test_pipeline_config_full_replace(self, temp_project):
        """Test pipeline_config event_log fully replaces project-level event_log."""
        # Project-level event_log
        event_log = EventLogConfig(
            catalog="project_catalog",
            schema="project_schema",
            name_suffix="_project_log",
        )
        project_config = ProjectConfig(name="test", event_log=event_log)

        # Pipeline-specific event_log in pipeline_config.yaml
        config_path = self._create_pipeline_config(
            temp_project,
            """
---
project_defaults:
  serverless: true

---
pipeline: test_pipeline
event_log:
  name: custom_event_log
  catalog: "${event_log_catalog}"
  schema: custom_schema
""",
        )

        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=config_path,
            project_config=project_config,
        )

        captured_context = None

        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"

        manager.template_renderer.render_template = mock_render

        output_dir = temp_project / "generated"
        manager.generate_resource_file_content("test_pipeline", output_dir, "dev")

        assert captured_context is not None
        config = captured_context["pipeline_config"]
        # Pipeline config's event_log should be used, not project-level
        assert config["event_log"]["name"] == "custom_event_log"
        # Substitution tokens in pipeline config's event_log should also resolve
        assert config["event_log"]["catalog"] == "dev_meta"
        assert config["event_log"]["schema"] == "custom_schema"

    def test_pipeline_config_disable_event_log(self, temp_project):
        """Test event_log: false in pipeline_config disables project-level event_log."""
        event_log = EventLogConfig(
            catalog="project_catalog",
            schema="project_schema",
        )
        project_config = ProjectConfig(name="test", event_log=event_log)

        config_path = self._create_pipeline_config(
            temp_project,
            """
---
project_defaults:
  serverless: true

---
pipeline: test_pipeline
event_log: false
""",
        )

        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=config_path,
            project_config=project_config,
        )

        captured_context = None

        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"

        manager.template_renderer.render_template = mock_render

        output_dir = temp_project / "generated"
        manager.generate_resource_file_content("test_pipeline", output_dir, "dev")

        assert captured_context is not None
        config = captured_context["pipeline_config"]
        assert "event_log" not in config

    def test_backward_compat_pipeline_config_only_event_log(self, temp_project):
        """Test that existing pipeline_config-only event_log still works without lhp.yaml event_log."""
        # No project-level event_log
        project_config = ProjectConfig(name="test")

        config_path = self._create_pipeline_config(
            temp_project,
            """
---
project_defaults:
  serverless: true

---
pipeline: test_pipeline
event_log:
  name: legacy_event_log
  catalog: "${event_log_catalog}"
  schema: _meta
""",
        )

        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=config_path,
            project_config=project_config,
        )

        captured_context = None

        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"

        manager.template_renderer.render_template = mock_render

        output_dir = temp_project / "generated"
        manager.generate_resource_file_content("test_pipeline", output_dir, "dev")

        assert captured_context is not None
        config = captured_context["pipeline_config"]
        assert config["event_log"]["name"] == "legacy_event_log"
        assert config["event_log"]["catalog"] == "dev_meta"
        assert config["event_log"]["schema"] == "_meta"

    def test_no_project_config_no_pipeline_config_event_log(self, temp_project):
        """Test no event_log when neither project nor pipeline config defines it."""
        manager = BundleManager(
            project_root=temp_project,
            project_config=None,
        )

        captured_context = None

        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"

        manager.template_renderer.render_template = mock_render

        output_dir = temp_project / "generated"
        manager.generate_resource_file_content("test_pipeline", output_dir, "dev")

        assert captured_context is not None
        config = captured_context["pipeline_config"]
        assert "event_log" not in config

    def test_injection_for_pipeline_without_specific_config(self, temp_project):
        """Test project event_log is injected for pipelines not mentioned in pipeline_config."""
        event_log = EventLogConfig(
            catalog="global_cat",
            schema="global_sch",
            name_suffix="_log",
        )
        project_config = ProjectConfig(name="test", event_log=event_log)

        # Pipeline config only has defaults, no pipeline-specific section
        config_path = self._create_pipeline_config(
            temp_project,
            """
---
project_defaults:
  serverless: true
""",
        )

        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=config_path,
            project_config=project_config,
        )

        captured_context = None

        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"

        manager.template_renderer.render_template = mock_render

        output_dir = temp_project / "generated"
        # This pipeline is not in pipeline_config, so project-level should apply
        manager.generate_resource_file_content("unlisted_pipeline", output_dir, "dev")

        assert captured_context is not None
        config = captured_context["pipeline_config"]
        assert "event_log" in config
        assert config["event_log"]["name"] == "unlisted_pipeline_log"
        assert config["event_log"]["catalog"] == "global_cat"
