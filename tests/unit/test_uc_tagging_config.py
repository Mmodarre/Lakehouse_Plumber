"""Unit tests for UC tagging configuration model and config loader."""

import pytest

from lhp.models import ProjectConfig, UCTaggingConfig


@pytest.mark.unit
class TestUCTaggingConfig:
    def test_defaults(self):
        config = UCTaggingConfig()
        assert config.enabled is True
        assert config.remove_undeclared_tags is False
        assert config.tag_update_concurrency == 16

    def test_overrides(self):
        config = UCTaggingConfig(
            enabled=False, remove_undeclared_tags=True, tag_update_concurrency=16
        )
        assert config.enabled is False
        assert config.remove_undeclared_tags is True
        assert config.tag_update_concurrency == 16


@pytest.mark.unit
class TestProjectConfigUCTagging:
    def test_defaults_to_none(self):
        config = ProjectConfig(name="test_project")
        assert config.uc_tagging is None

    def test_with_uc_tagging(self):
        config = ProjectConfig(
            name="test_project",
            uc_tagging=UCTaggingConfig(remove_undeclared_tags=True),
        )
        assert config.uc_tagging is not None
        assert config.uc_tagging.remove_undeclared_tags is True


@pytest.mark.unit
class TestProjectConfigLoaderUCTagging:
    def test_parses_uc_tagging_section(self, tmp_path):
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text(
            "name: test_project\n"
            "uc_tagging:\n"
            "  enabled: true\n"
            "  remove_undeclared_tags: true\n"
        )
        from lhp.core.loaders import ProjectConfigLoader

        config = ProjectConfigLoader(tmp_path).load_project_config()
        assert config is not None
        assert config.uc_tagging is not None
        assert config.uc_tagging.enabled is True
        assert config.uc_tagging.remove_undeclared_tags is True

    def test_partial_section_uses_defaults(self, tmp_path):
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text(
            "name: test_project\nuc_tagging:\n  remove_undeclared_tags: true\n"
        )
        from lhp.core.loaders import ProjectConfigLoader

        config = ProjectConfigLoader(tmp_path).load_project_config()
        assert config.uc_tagging.enabled is True
        assert config.uc_tagging.remove_undeclared_tags is True

    def test_bare_block_enables_with_defaults(self, tmp_path):
        # `uc_tagging:` with no body opts in with defaults.
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("name: test_project\nuc_tagging:\n")
        from lhp.core.loaders import ProjectConfigLoader

        config = ProjectConfigLoader(tmp_path).load_project_config()
        assert config.uc_tagging is not None
        assert config.uc_tagging.enabled is True
        assert config.uc_tagging.tag_update_concurrency == 16

    def test_parses_tag_update_concurrency(self, tmp_path):
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text(
            "name: test_project\nuc_tagging:\n  tag_update_concurrency: 32\n"
        )
        from lhp.core.loaders import ProjectConfigLoader

        config = ProjectConfigLoader(tmp_path).load_project_config()
        assert config.uc_tagging.tag_update_concurrency == 32

    def test_rejects_invalid_concurrency(self, tmp_path):
        from lhp.core.loaders import ProjectConfigLoader
        from lhp.errors import LHPError

        for bad in ("0", "-1", "true", "eight"):
            lhp_yaml = tmp_path / "lhp.yaml"
            lhp_yaml.write_text(
                f"name: test_project\nuc_tagging:\n  tag_update_concurrency: {bad}\n"
            )
            with pytest.raises(LHPError, match="positive integer"):
                ProjectConfigLoader(tmp_path).load_project_config()

    def test_parses_without_uc_tagging(self, tmp_path):
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("name: test_project\n")
        from lhp.core.loaders import ProjectConfigLoader

        config = ProjectConfigLoader(tmp_path).load_project_config()
        assert config is not None
        assert config.uc_tagging is None

    def test_rejects_non_dict_uc_tagging(self, tmp_path):
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("name: test_project\nuc_tagging: just_a_string\n")
        from lhp.core.loaders import ProjectConfigLoader
        from lhp.errors import LHPError

        loader = ProjectConfigLoader(tmp_path)
        with pytest.raises(LHPError, match="must be a mapping"):
            loader.load_project_config()

    def test_rejects_non_bool_field(self, tmp_path):
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text(
            "name: test_project\nuc_tagging:\n  enabled: maybe\n"
        )
        from lhp.core.loaders import ProjectConfigLoader
        from lhp.errors import LHPError

        loader = ProjectConfigLoader(tmp_path)
        with pytest.raises(LHPError, match="must be a boolean"):
            loader.load_project_config()
