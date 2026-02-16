"""
Test suite for environment dependencies propagation in bundle resources.

Tests that pipeline_config.yaml 'environment' section is correctly:
- Rendered in the generated bundle resource YAML
- Passed through substitution
- Validated (must be a dict if present)
"""

import pytest
from pathlib import Path
from copy import deepcopy
import tempfile
import shutil
import yaml

from lhp.bundle.manager import BundleManager
from lhp.core.services.pipeline_config_loader import PipelineConfigLoader


class TestEnvironmentTemplateRendering:
    """Test that environment is correctly rendered in bundle resource output."""

    @pytest.fixture
    def temp_project(self):
        """Create temporary project structure."""
        temp_dir = tempfile.mkdtemp()
        project_root = Path(temp_dir)

        (project_root / "substitutions").mkdir(parents=True)
        (project_root / "config").mkdir(parents=True)
        (project_root / "pipelines").mkdir(parents=True)
        (project_root / "generated").mkdir(parents=True)

        # Create minimal substitution file
        sub_content = {"dev": {"catalog": "dev_catalog", "schema": "dev_schema"}}
        with open(project_root / "substitutions" / "dev.yaml", "w") as f:
            yaml.dump(sub_content, f)

        yield project_root
        shutil.rmtree(temp_dir)

    def _make_config(self, temp_project, config_yaml: str) -> Path:
        config_path = temp_project / "config" / "pipeline_config.yaml"
        with open(config_path, "w") as f:
            f.write(config_yaml)
        return config_path

    def _render_resource(self, temp_project, config_path, pipeline_name="test_pipeline"):
        """Render the actual template and return the output YAML string."""
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(config_path),
        )
        output_dir = temp_project / "generated"
        return manager.generate_resource_file_content(pipeline_name, output_dir, "dev")

    def test_environment_dependencies_rendered_in_output(self, temp_project):
        """Environment with dependencies list renders correctly in generated resource."""
        config_path = self._make_config(temp_project, """
---
pipeline: test_pipeline
catalog: dev_catalog
schema: dev_schema
serverless: true
environment:
  dependencies:
    - "msal==1.31.0"
    - "requests>=2.28.0"
""")
        content = self._render_resource(temp_project, config_path)
        parsed = yaml.safe_load(content)

        pipeline = parsed["resources"]["pipelines"]["test_pipeline_pipeline"]
        assert "environment" in pipeline
        assert pipeline["environment"]["dependencies"] == [
            "msal==1.31.0",
            "requests>=2.28.0",
        ]

    def test_no_environment_section_omitted(self, temp_project):
        """No environment key means no environment in parsed output."""
        config_path = self._make_config(temp_project, """
---
pipeline: test_pipeline
catalog: dev_catalog
schema: dev_schema
serverless: true
""")
        content = self._render_resource(temp_project, config_path)
        parsed = yaml.safe_load(content)
        pipeline = parsed["resources"]["pipelines"]["test_pipeline_pipeline"]
        assert "environment" not in pipeline

    def test_environment_with_complex_structure(self, temp_project):
        """Arbitrary nested dict/list structure passes through correctly."""
        config_path = self._make_config(temp_project, """
---
pipeline: test_pipeline
catalog: dev_catalog
schema: dev_schema
serverless: true
environment:
  dependencies:
    - "msal==1.31.0"
  custom_key:
    nested: value
    items:
      - a
      - b
""")
        content = self._render_resource(temp_project, config_path)
        parsed = yaml.safe_load(content)

        env = parsed["resources"]["pipelines"]["test_pipeline_pipeline"]["environment"]
        assert env["dependencies"] == ["msal==1.31.0"]
        assert env["custom_key"]["nested"] == "value"
        assert env["custom_key"]["items"] == ["a", "b"]

    def test_environment_is_peer_to_libraries(self, temp_project):
        """Environment is at the same YAML level as libraries, tags, etc."""
        config_path = self._make_config(temp_project, """
---
pipeline: test_pipeline
catalog: dev_catalog
schema: dev_schema
serverless: true
tags:
  team: data-eng
environment:
  dependencies:
    - "msal==1.31.0"
""")
        content = self._render_resource(temp_project, config_path)
        parsed = yaml.safe_load(content)

        pipeline = parsed["resources"]["pipelines"]["test_pipeline_pipeline"]
        # Both should be direct children of the pipeline definition
        assert "tags" in pipeline
        assert "environment" in pipeline
        assert "libraries" in pipeline


class TestEnvironmentSubstitution:
    """Test that substitution tokens in environment values get resolved."""

    @pytest.fixture
    def temp_project(self):
        temp_dir = tempfile.mkdtemp()
        project_root = Path(temp_dir)

        (project_root / "substitutions").mkdir(parents=True)
        (project_root / "config").mkdir(parents=True)
        (project_root / "pipelines").mkdir(parents=True)
        (project_root / "generated").mkdir(parents=True)

        sub_content = {
            "dev": {
                "catalog": "dev_catalog",
                "schema": "dev_schema",
                "msal_version": "1.31.0",
            }
        }
        with open(project_root / "substitutions" / "dev.yaml", "w") as f:
            yaml.dump(sub_content, f)

        yield project_root
        shutil.rmtree(temp_dir)

    def test_substitution_in_environment_dependencies(self, temp_project):
        """Tokens like {msal_version} in dependency values get substituted."""
        config_path = temp_project / "config" / "pipeline_config.yaml"
        with open(config_path, "w") as f:
            f.write("""
---
pipeline: test_pipeline
catalog: "{catalog}"
schema: "{schema}"
serverless: true
environment:
  dependencies:
    - "msal=={msal_version}"
""")
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(config_path),
        )

        captured_context = None

        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"

        manager.template_renderer.render_template = mock_render

        output_dir = temp_project / "generated"
        manager.generate_resource_file_content("test_pipeline", output_dir, "dev")

        config = captured_context["pipeline_config"]
        assert config["environment"]["dependencies"] == ["msal==1.31.0"]

    def test_environment_inherited_from_project_defaults(self, temp_project):
        """project_defaults environment flows to pipeline config."""
        config_path = temp_project / "config" / "pipeline_config.yaml"
        with open(config_path, "w") as f:
            f.write("""
project_defaults:
  serverless: true
  environment:
    dependencies:
      - "msal==1.31.0"

---
pipeline: test_pipeline
catalog: "{catalog}"
schema: "{schema}"
""")
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(config_path),
        )

        captured_context = None

        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"

        manager.template_renderer.render_template = mock_render

        output_dir = temp_project / "generated"
        manager.generate_resource_file_content("test_pipeline", output_dir, "dev")

        config = captured_context["pipeline_config"]
        assert config["environment"]["dependencies"] == ["msal==1.31.0"]

    def test_environment_pipeline_overrides_project_defaults(self, temp_project):
        """Pipeline-specific environment replaces project defaults (lists replace, not append)."""
        config_path = temp_project / "config" / "pipeline_config.yaml"
        with open(config_path, "w") as f:
            f.write("""
project_defaults:
  serverless: true
  environment:
    dependencies:
      - "msal==1.31.0"

---
pipeline: test_pipeline
catalog: "{catalog}"
schema: "{schema}"
environment:
  dependencies:
    - "requests>=2.28.0"
""")
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(config_path),
        )

        captured_context = None

        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"

        manager.template_renderer.render_template = mock_render

        output_dir = temp_project / "generated"
        manager.generate_resource_file_content("test_pipeline", output_dir, "dev")

        config = captured_context["pipeline_config"]
        # Lists replace, not append — only the pipeline-specific value should remain
        assert config["environment"]["dependencies"] == ["requests>=2.28.0"]


class TestEnvironmentValidation:
    """Test validation of the environment key in PipelineConfigLoader."""

    def test_valid_environment_dict_passes(self):
        """Dict value for environment is accepted."""
        temp_dir = tempfile.mkdtemp()
        project_root = Path(temp_dir)

        try:
            (project_root / "config").mkdir(parents=True)
            config_path = project_root / "config" / "pipeline_config.yaml"
            with open(config_path, "w") as f:
                f.write("""
---
pipeline: test_pipeline
serverless: true
environment:
  dependencies:
    - "msal==1.31.0"
""")
            # Should not raise
            loader = PipelineConfigLoader(project_root, str(config_path))
            config = loader.get_pipeline_config("test_pipeline")
            assert config["environment"]["dependencies"] == ["msal==1.31.0"]
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.parametrize(
        "bad_value,type_name",
        [
            ('"just_a_string"', "str"),
            ("42", "int"),
            ("true", "bool"),
        ],
    )
    def test_environment_non_dict_raises_error(self, bad_value, type_name):
        """Non-dict values for environment raise ValueError."""
        temp_dir = tempfile.mkdtemp()
        project_root = Path(temp_dir)

        try:
            (project_root / "config").mkdir(parents=True)
            config_path = project_root / "config" / "pipeline_config.yaml"
            with open(config_path, "w") as f:
                f.write(f"""
---
pipeline: test_pipeline
serverless: true
environment: {bad_value}
""")
            with pytest.raises(ValueError, match="Invalid 'environment' value"):
                PipelineConfigLoader(project_root, str(config_path))
        finally:
            shutil.rmtree(temp_dir)
