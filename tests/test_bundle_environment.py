"""
Test suite for environment dependencies and configuration propagation in bundle resources.

Tests that pipeline_config.yaml 'environment' and 'configuration' sections are correctly:
- Rendered in the generated bundle resource YAML
- Passed through substitution
- Validated (must be a dict if present; configuration values must be strings)
"""

import shutil
import tempfile
from copy import deepcopy
from pathlib import Path

import pytest
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

    def _render_resource(
        self, temp_project, config_path, pipeline_name="test_pipeline"
    ):
        """Render the actual template and return the output YAML string."""
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(config_path),
        )
        output_dir = temp_project / "generated"
        return manager.generate_resource_file_content(pipeline_name, output_dir, "dev")

    def test_environment_dependencies_rendered_in_output(self, temp_project):
        """Environment with dependencies list renders correctly in generated resource."""
        config_path = self._make_config(
            temp_project,
            """
---
pipeline: test_pipeline
catalog: dev_catalog
schema: dev_schema
serverless: true
environment:
  dependencies:
    - "msal==1.31.0"
    - "requests>=2.28.0"
""",
        )
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
        config_path = self._make_config(
            temp_project,
            """
---
pipeline: test_pipeline
catalog: dev_catalog
schema: dev_schema
serverless: true
""",
        )
        content = self._render_resource(temp_project, config_path)
        parsed = yaml.safe_load(content)
        pipeline = parsed["resources"]["pipelines"]["test_pipeline_pipeline"]
        assert "environment" not in pipeline

    def test_environment_with_complex_structure(self, temp_project):
        """Arbitrary nested dict/list structure passes through correctly."""
        config_path = self._make_config(
            temp_project,
            """
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
""",
        )
        content = self._render_resource(temp_project, config_path)
        parsed = yaml.safe_load(content)

        env = parsed["resources"]["pipelines"]["test_pipeline_pipeline"]["environment"]
        assert env["dependencies"] == ["msal==1.31.0"]
        assert env["custom_key"]["nested"] == "value"
        assert env["custom_key"]["items"] == ["a", "b"]

    def test_environment_is_peer_to_libraries(self, temp_project):
        """Environment is at the same YAML level as libraries, tags, etc."""
        config_path = self._make_config(
            temp_project,
            """
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
""",
        )
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


class TestConfigurationTemplateRendering:
    """Test that configuration entries are correctly rendered in bundle resource output."""

    @pytest.fixture
    def temp_project(self):
        """Create temporary project structure."""
        temp_dir = tempfile.mkdtemp()
        project_root = Path(temp_dir)

        (project_root / "substitutions").mkdir(parents=True)
        (project_root / "config").mkdir(parents=True)
        (project_root / "pipelines").mkdir(parents=True)
        (project_root / "generated").mkdir(parents=True)

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

    def _render_resource(
        self, temp_project, config_path, pipeline_name="test_pipeline"
    ):
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(config_path),
        )
        output_dir = temp_project / "generated"
        return manager.generate_resource_file_content(pipeline_name, output_dir, "dev")

    def test_configuration_entries_rendered_in_output(self, temp_project):
        """User configuration entries appear alongside bundle.sourcePath."""
        config_path = self._make_config(
            temp_project,
            """
---
pipeline: test_pipeline
catalog: dev_catalog
schema: dev_schema
serverless: true
configuration:
  "pipelines.incompatibleViewCheck.enabled": "false"
""",
        )
        content = self._render_resource(temp_project, config_path)
        parsed = yaml.safe_load(content)

        pipeline = parsed["resources"]["pipelines"]["test_pipeline_pipeline"]
        config = pipeline["configuration"]
        assert (
            config["bundle.sourcePath"]
            == "${workspace.file_path}/generated/${bundle.target}"
        )
        assert config["pipelines.incompatibleViewCheck.enabled"] == "false"

    def test_no_configuration_preserves_default(self, temp_project):
        """Only bundle.sourcePath when no user configuration provided."""
        config_path = self._make_config(
            temp_project,
            """
---
pipeline: test_pipeline
catalog: dev_catalog
schema: dev_schema
serverless: true
""",
        )
        content = self._render_resource(temp_project, config_path)
        parsed = yaml.safe_load(content)

        pipeline = parsed["resources"]["pipelines"]["test_pipeline_pipeline"]
        config = pipeline["configuration"]
        assert config == {
            "bundle.sourcePath": "${workspace.file_path}/generated/${bundle.target}"
        }

    def test_configuration_with_multiple_entries(self, temp_project):
        """Multiple key-value pairs render correctly."""
        config_path = self._make_config(
            temp_project,
            """
---
pipeline: test_pipeline
catalog: dev_catalog
schema: dev_schema
serverless: true
configuration:
  "pipelines.incompatibleViewCheck.enabled": "false"
  "spark.databricks.delta.minFileSize": "134217728"
""",
        )
        content = self._render_resource(temp_project, config_path)
        parsed = yaml.safe_load(content)

        pipeline = parsed["resources"]["pipelines"]["test_pipeline_pipeline"]
        config = pipeline["configuration"]
        assert (
            config["bundle.sourcePath"]
            == "${workspace.file_path}/generated/${bundle.target}"
        )
        assert config["pipelines.incompatibleViewCheck.enabled"] == "false"
        assert config["spark.databricks.delta.minFileSize"] == "134217728"

    def test_bundle_source_path_not_duplicated(self, temp_project):
        """User including bundle.sourcePath doesn't cause duplication."""
        config_path = self._make_config(
            temp_project,
            """
---
pipeline: test_pipeline
catalog: dev_catalog
schema: dev_schema
serverless: true
configuration:
  "bundle.sourcePath": "user_should_not_override"
  "pipelines.incompatibleViewCheck.enabled": "false"
""",
        )
        content = self._render_resource(temp_project, config_path)
        parsed = yaml.safe_load(content)

        pipeline = parsed["resources"]["pipelines"]["test_pipeline_pipeline"]
        config = pipeline["configuration"]
        # The LHP-managed bundle.sourcePath always wins
        assert (
            config["bundle.sourcePath"]
            == "${workspace.file_path}/generated/${bundle.target}"
        )
        assert config["pipelines.incompatibleViewCheck.enabled"] == "false"

    def test_configuration_is_peer_to_libraries(self, temp_project):
        """Configuration is at the same YAML level as libraries, tags, etc."""
        config_path = self._make_config(
            temp_project,
            """
---
pipeline: test_pipeline
catalog: dev_catalog
schema: dev_schema
serverless: true
configuration:
  "pipelines.incompatibleViewCheck.enabled": "false"
""",
        )
        content = self._render_resource(temp_project, config_path)
        parsed = yaml.safe_load(content)

        pipeline = parsed["resources"]["pipelines"]["test_pipeline_pipeline"]
        assert "configuration" in pipeline
        assert "libraries" in pipeline


class TestConfigurationSubstitution:
    """Test that substitution tokens in configuration values get resolved."""

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
                "min_file_size": "134217728",
            }
        }
        with open(project_root / "substitutions" / "dev.yaml", "w") as f:
            yaml.dump(sub_content, f)

        yield project_root
        shutil.rmtree(temp_dir)

    def test_substitution_in_configuration_values(self, temp_project):
        """Tokens like {min_file_size} in configuration values get substituted."""
        config_path = temp_project / "config" / "pipeline_config.yaml"
        with open(config_path, "w") as f:
            f.write("""
---
pipeline: test_pipeline
catalog: "{catalog}"
schema: "{schema}"
serverless: true
configuration:
  "spark.databricks.delta.minFileSize": "{min_file_size}"
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
        assert (
            config["configuration"]["spark.databricks.delta.minFileSize"] == "134217728"
        )

    def test_configuration_inherited_from_project_defaults(self, temp_project):
        """project_defaults configuration flows to pipeline config."""
        config_path = temp_project / "config" / "pipeline_config.yaml"
        with open(config_path, "w") as f:
            f.write("""
project_defaults:
  serverless: true
  configuration:
    "pipelines.incompatibleViewCheck.enabled": "false"

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
        assert (
            config["configuration"]["pipelines.incompatibleViewCheck.enabled"]
            == "false"
        )

    def test_configuration_pipeline_overrides_project_defaults(self, temp_project):
        """Pipeline-specific configuration deep-merges with project defaults."""
        config_path = temp_project / "config" / "pipeline_config.yaml"
        with open(config_path, "w") as f:
            f.write("""
project_defaults:
  serverless: true
  configuration:
    "pipelines.incompatibleViewCheck.enabled": "false"
    "spark.databricks.delta.minFileSize": "134217728"

---
pipeline: test_pipeline
catalog: "{catalog}"
schema: "{schema}"
configuration:
  "pipelines.incompatibleViewCheck.enabled": "true"
  "spark.sql.shuffle.partitions": "200"
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
        # Pipeline-specific overrides the default
        assert (
            config["configuration"]["pipelines.incompatibleViewCheck.enabled"] == "true"
        )
        # Project default is preserved via deep merge
        assert (
            config["configuration"]["spark.databricks.delta.minFileSize"] == "134217728"
        )
        # Pipeline-specific addition
        assert config["configuration"]["spark.sql.shuffle.partitions"] == "200"


class TestConfigurationValidation:
    """Test validation of the configuration key in PipelineConfigLoader."""

    def test_valid_configuration_dict_passes(self):
        """Dict value with string values for configuration is accepted."""
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
configuration:
  "pipelines.incompatibleViewCheck.enabled": "false"
""")
            loader = PipelineConfigLoader(project_root, str(config_path))
            config = loader.get_pipeline_config("test_pipeline")
            assert (
                config["configuration"]["pipelines.incompatibleViewCheck.enabled"]
                == "false"
            )
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.parametrize(
        "bad_value,type_name",
        [
            ('"just_a_string"', "str"),
            ("42", "int"),
            ("true", "bool"),
            ("[a, b]", "list"),
        ],
    )
    def test_configuration_non_dict_raises_error(self, bad_value, type_name):
        """Non-dict values for configuration raise ValueError."""
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
configuration: {bad_value}
""")
            with pytest.raises(ValueError, match="Invalid 'configuration' value"):
                PipelineConfigLoader(project_root, str(config_path))
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.parametrize(
        "bad_val_yaml,type_name",
        [
            ("false", "bool"),
            ("42", "int"),
        ],
    )
    def test_configuration_non_string_value_raises_error(self, bad_val_yaml, type_name):
        """Boolean/int values in configuration dict are rejected with helpful message."""
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
configuration:
  "some.key": {bad_val_yaml}
""")
            with pytest.raises(ValueError, match="Invalid configuration value for key"):
                PipelineConfigLoader(project_root, str(config_path))
        finally:
            shutil.rmtree(temp_dir)

    def test_configuration_empty_dict_passes(self):
        """Empty dict for configuration is accepted."""
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
configuration: {}
""")
            loader = PipelineConfigLoader(project_root, str(config_path))
            config = loader.get_pipeline_config("test_pipeline")
            assert config["configuration"] == {}
        finally:
            shutil.rmtree(temp_dir)
