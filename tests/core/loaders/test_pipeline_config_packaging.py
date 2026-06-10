"""Tests for PipelineConfigLoader.resolve_packaging_modes and the
``packaging`` enum validation (LHP-VAL-062).

Precedence under test (lowest to highest):
hard default ``"source"`` -> ``project_defaults.packaging`` -> per-pipeline
``packaging``. The enum check in ``_validate_config`` rejects any ``packaging``
value outside ``{"wheel", "source"}`` with code ``LHP-VAL-062``.
"""

import pytest

from lhp.core.loaders.pipeline_config_loader import PipelineConfigLoader
from lhp.errors import LHPError, LHPValidationError


@pytest.mark.unit
class TestResolvePackagingModes:
    """Precedence resolution for per-pipeline packaging modes."""

    def test_default_resolves_to_source(self, tmp_path):
        """No packaging anywhere -> hard default 'source'."""
        config_content = """project_defaults:
  serverless: true
---
pipeline: bronze
serverless: false
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)

        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))

        modes = loader.resolve_packaging_modes(["bronze"])
        assert modes == {"bronze": "source"}

    def test_no_config_file_resolves_to_source(self, tmp_path):
        """No config file at all -> hard default 'source'."""
        loader = PipelineConfigLoader(tmp_path, config_file_path=None)

        modes = loader.resolve_packaging_modes(["any_pipeline"])
        assert modes == {"any_pipeline": "source"}

    def test_project_default_wheel_applies_without_override(self, tmp_path):
        """project_defaults.packaging: wheel + no per-pipeline override -> 'wheel'."""
        config_content = """project_defaults:
  packaging: wheel
---
pipeline: bronze
serverless: false
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)

        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))

        modes = loader.resolve_packaging_modes(["bronze"])
        assert modes == {"bronze": "wheel"}

    def test_project_default_wheel_unlisted_pipeline_resolves_wheel(self, tmp_path):
        """A pipeline absent from the config still inherits project_defaults.packaging."""
        config_content = """project_defaults:
  packaging: wheel
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)

        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))

        modes = loader.resolve_packaging_modes(["never_declared"])
        assert modes == {"never_declared": "wheel"}

    def test_pipeline_override_source_beats_project_default_wheel(self, tmp_path):
        """project-default wheel but pipeline sets packaging: source -> 'source'."""
        config_content = """project_defaults:
  packaging: wheel
---
pipeline: bronze
packaging: source
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)

        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))

        modes = loader.resolve_packaging_modes(["bronze"])
        assert modes == {"bronze": "source"}

    def test_pipeline_override_wheel_beats_project_default_source(self, tmp_path):
        """project-default source but pipeline sets packaging: wheel -> 'wheel'."""
        config_content = """project_defaults:
  packaging: source
---
pipeline: bronze
packaging: wheel
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)

        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))

        modes = loader.resolve_packaging_modes(["bronze"])
        assert modes == {"bronze": "wheel"}

    def test_pipeline_override_wheel_with_no_project_default(self, tmp_path):
        """project-default absent but pipeline sets packaging: wheel -> 'wheel'."""
        config_content = """project_defaults:
  serverless: true
---
pipeline: bronze
packaging: wheel
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)

        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))

        modes = loader.resolve_packaging_modes(["bronze"])
        assert modes == {"bronze": "wheel"}

    def test_mixed_pipelines_resolve_independently(self, tmp_path):
        """Per-pipeline overrides resolve independently over a wheel project default."""
        config_content = """project_defaults:
  packaging: wheel
---
pipeline: bronze
packaging: source
---
pipeline: silver
serverless: false
---
pipeline: gold
packaging: wheel
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)

        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))

        modes = loader.resolve_packaging_modes(["bronze", "silver", "gold"])
        assert modes == {
            "bronze": "source",  # explicit override
            "silver": "wheel",  # inherits project default
            "gold": "wheel",  # explicit, matches default
        }


@pytest.mark.unit
class TestPackagingEnumValidation:
    """LHP-VAL-062 enum check on the ``packaging`` value."""

    def test_invalid_packaging_value_raises_val_062(self, tmp_path):
        """A bad packaging value (zip) raises LHPError with code LHP-VAL-062."""
        config_content = """project_defaults:
  serverless: true
---
pipeline: bronze
packaging: zip
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)

        with pytest.raises(LHPError) as exc_info:
            PipelineConfigLoader(tmp_path, config_file_path=str(config_file))

        assert exc_info.value.code == "LHP-VAL-062"

    def test_invalid_packaging_value_is_validation_error(self, tmp_path):
        """The packaging enum failure is an LHPValidationError (ValueError bridge)."""
        config_content = """project_defaults:
  serverless: true
---
pipeline: bronze
packaging: zip
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)

        with pytest.raises(LHPValidationError) as exc_info:
            PipelineConfigLoader(tmp_path, config_file_path=str(config_file))

        error_msg = str(exc_info.value)
        assert "Invalid packaging 'zip'" in error_msg
        assert "wheel" in error_msg
        assert "source" in error_msg

    def test_invalid_packaging_in_project_defaults_raises_val_062(self, tmp_path):
        """A bad packaging value in project_defaults also raises LHP-VAL-062."""
        config_content = """project_defaults:
  packaging: tarball
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)

        with pytest.raises(LHPError) as exc_info:
            PipelineConfigLoader(tmp_path, config_file_path=str(config_file))

        assert exc_info.value.code == "LHP-VAL-062"
