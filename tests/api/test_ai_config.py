"""Tests for AIConfig — YAML loading, env overrides, validation."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from lhp.api.services.ai_config import AIConfig


class TestAIConfigDefaults:
    """Verify defaults when no YAML file or env vars exist."""

    def test_load_without_project_root(self):
        config = AIConfig.load()
        assert config.provider == "anthropic"
        assert config.model == "anthropic/databricks-claude-sonnet-4-6"
        assert config.max_processes == 10
        assert config.idle_timeout_minutes == 30

    def test_session_max_age_hours_default(self):
        config = AIConfig.load()
        assert config.session_max_age_hours == 72.0

    def test_load_with_nonexistent_yaml(self, tmp_path):
        config = AIConfig.load(tmp_path)
        assert config.provider == "anthropic"
        assert config.model == "anthropic/databricks-claude-sonnet-4-6"

    def test_default_allowed_providers(self):
        config = AIConfig.load()
        assert "anthropic" in config.allowed_providers
        assert "openai" not in config.allowed_providers

    def test_default_allowed_models(self):
        config = AIConfig.load()
        assert "anthropic/databricks-claude-sonnet-4-6" in config.allowed_models["anthropic"]
        assert "openai" not in config.allowed_models

    def test_default_provider_config(self):
        config = AIConfig.load()
        assert "anthropic" in config.provider_config
        models = config.provider_config["anthropic"]["models"]
        assert "databricks-claude-sonnet-4-6" in models
        assert models["databricks-claude-sonnet-4-6"]["name"] == "Claude Sonnet 4.6 (Databricks)"


class TestAIConfigYAML:
    """Verify YAML loading and merging."""

    def test_load_from_yaml(self, tmp_path):
        yaml_content = """\
provider: anthropic
model: anthropic/databricks-claude-sonnet-4-6
max_processes: 5
"""
        (tmp_path / "ai_config.yaml").write_text(yaml_content)
        config = AIConfig.load(tmp_path)
        assert config.provider == "anthropic"
        assert config.model == "anthropic/databricks-claude-sonnet-4-6"
        assert config.max_processes == 5
        # Unset fields keep defaults
        assert config.idle_timeout_minutes == 30

    def test_yaml_partial_override(self, tmp_path):
        yaml_content = "model: anthropic/databricks-claude-sonnet-4-6\n"
        (tmp_path / "ai_config.yaml").write_text(yaml_content)
        config = AIConfig.load(tmp_path)
        assert config.provider == "anthropic"  # default
        assert config.model == "anthropic/databricks-claude-sonnet-4-6"

    def test_yaml_custom_allowed_models(self, tmp_path):
        yaml_content = """\
allowed_providers:
  - anthropic
allowed_models:
  anthropic:
    - anthropic/databricks-claude-sonnet-4-6
"""
        (tmp_path / "ai_config.yaml").write_text(yaml_content)
        config = AIConfig.load(tmp_path)
        assert config.allowed_providers == ["anthropic"]
        assert len(config.allowed_models["anthropic"]) == 1

    def test_invalid_yaml_falls_back_to_defaults(self, tmp_path):
        (tmp_path / "ai_config.yaml").write_text(": : invalid yaml [")
        config = AIConfig.load(tmp_path)
        assert config.provider == "anthropic"

    def test_provider_config_from_yaml(self, tmp_path):
        """Verify provider_config is loaded from YAML."""
        yaml_content = """\
provider_config:
  anthropic:
    models:
      custom-model:
        name: Custom Model
"""
        (tmp_path / "ai_config.yaml").write_text(yaml_content)
        config = AIConfig.load(tmp_path)
        assert "custom-model" in config.provider_config["anthropic"]["models"]
        assert config.provider_config["anthropic"]["models"]["custom-model"]["name"] == "Custom Model"


class TestAIConfigEnvOverrides:
    """Verify environment variable overrides."""

    def test_env_overrides_provider(self, tmp_path):
        with patch.dict(os.environ, {"LHP_AI_PROVIDER": "openai"}):
            config = AIConfig.load(tmp_path)
        assert config.provider == "openai"

    def test_env_overrides_model(self, tmp_path):
        with patch.dict(os.environ, {"LHP_AI_MODEL": "openai/gpt-4o-mini"}):
            config = AIConfig.load(tmp_path)
        assert config.model == "openai/gpt-4o-mini"

    def test_env_overrides_max_processes(self, tmp_path):
        with patch.dict(os.environ, {"LHP_AI_MAX_PROCESSES": "20"}):
            config = AIConfig.load(tmp_path)
        assert config.max_processes == 20

    def test_env_overrides_idle_timeout(self, tmp_path):
        with patch.dict(os.environ, {"LHP_AI_IDLE_TIMEOUT_MINUTES": "60.5"}):
            config = AIConfig.load(tmp_path)
        assert config.idle_timeout_minutes == 60.5

    def test_env_overrides_yaml(self, tmp_path):
        """Env vars should win over YAML values."""
        (tmp_path / "ai_config.yaml").write_text("provider: openai\n")
        with patch.dict(os.environ, {"LHP_AI_PROVIDER": "anthropic"}):
            config = AIConfig.load(tmp_path)
        assert config.provider == "anthropic"

    def test_env_overrides_session_max_age_hours(self, tmp_path):
        with patch.dict(os.environ, {"LHP_AI_SESSION_MAX_AGE_HOURS": "24.5"}):
            config = AIConfig.load(tmp_path)
        assert config.session_max_age_hours == 24.5

    def test_invalid_env_value_ignored(self, tmp_path):
        with patch.dict(os.environ, {"LHP_AI_MAX_PROCESSES": "not-a-number"}):
            config = AIConfig.load(tmp_path)
        assert config.max_processes == 10  # default


class TestAIConfigHelpers:
    """Verify to_opencode_json, get_allowed_models, validate_selection."""

    def test_to_opencode_json_defaults(self):
        config = AIConfig.load()
        result = config.to_opencode_json()
        assert result["$schema"] == "https://opencode.ai/config.json"
        assert "anthropic" in result["provider"]
        assert result["model"] == "anthropic/databricks-claude-sonnet-4-6"

    def test_to_opencode_json_includes_provider_config(self):
        """Verify models map is included in opencode.json output."""
        config = AIConfig.load()
        result = config.to_opencode_json()
        provider_block = result["provider"]["anthropic"]
        assert "models" in provider_block
        assert "databricks-claude-sonnet-4-6" in provider_block["models"]
        assert provider_block["models"]["databricks-claude-sonnet-4-6"]["name"] == "Claude Sonnet 4.6 (Databricks)"

    def test_to_opencode_json_has_no_env_block(self):
        """Verify no credentials/env block in opencode.json output."""
        config = AIConfig.load()
        result = config.to_opencode_json()
        assert "env" not in result

    def test_to_opencode_json_custom(self):
        config = AIConfig.load()
        result = config.to_opencode_json(provider="openai", model="openai/gpt-4o")
        assert "openai" in result["provider"]
        assert result["model"] == "openai/gpt-4o"

    def test_get_allowed_models(self):
        config = AIConfig.load()
        models = config.get_allowed_models()
        assert "anthropic" in models
        assert "anthropic/databricks-claude-sonnet-4-6" in models["anthropic"]

    def test_validate_selection_valid(self):
        config = AIConfig.load()
        assert config.validate_selection("anthropic", "anthropic/databricks-claude-sonnet-4-6")

    def test_validate_selection_invalid_provider(self):
        config = AIConfig.load()
        assert not config.validate_selection("invalid", "some-model")

    def test_validate_selection_invalid_model(self):
        config = AIConfig.load()
        assert not config.validate_selection("anthropic", "invalid-model")
