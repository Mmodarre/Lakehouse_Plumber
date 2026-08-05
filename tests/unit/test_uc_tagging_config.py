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
        assert config.max_allowable_consecutive_failures is None

    def test_overrides(self):
        config = UCTaggingConfig(
            enabled=False,
            remove_undeclared_tags=True,
            tag_update_concurrency=16,
            # 0 is falsy but legal — it must survive as 0, not normalize to None.
            max_allowable_consecutive_failures=0,
        )
        assert config.enabled is False
        assert config.remove_undeclared_tags is True
        assert config.tag_update_concurrency == 16
        assert config.max_allowable_consecutive_failures == 0


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
        assert config.uc_tagging.max_allowable_consecutive_failures is None

    def test_parses_tag_update_concurrency(self, tmp_path):
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text(
            "name: test_project\nuc_tagging:\n  tag_update_concurrency: 8\n"
        )
        from lhp.core.loaders import ProjectConfigLoader

        config = ProjectConfigLoader(tmp_path).load_project_config()
        assert config.uc_tagging.tag_update_concurrency == 8

    def test_rejects_invalid_concurrency(self, tmp_path):
        from lhp.core.loaders import ProjectConfigLoader
        from lhp.errors import LHPError

        # 21 is over the [1, 20] cap; the rest are floor/type rejections.
        for bad in ("0", "-1", "21", "true", "eight"):
            lhp_yaml = tmp_path / "lhp.yaml"
            lhp_yaml.write_text(
                f"name: test_project\nuc_tagging:\n  tag_update_concurrency: {bad}\n"
            )
            with pytest.raises(LHPError, match="LHP-CFG-009") as exc_info:
                ProjectConfigLoader(tmp_path).load_project_config()
            assert exc_info.value.code == "LHP-CFG-009"

    def test_accepts_max_concurrency(self, tmp_path):
        # 20 is the upper bound and must be accepted.
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text(
            "name: test_project\nuc_tagging:\n  tag_update_concurrency: 20\n"
        )
        from lhp.core.loaders import ProjectConfigLoader

        config = ProjectConfigLoader(tmp_path).load_project_config()
        assert config.uc_tagging.tag_update_concurrency == 20

    def test_model_rejects_out_of_range_concurrency(self):
        # Direct construction must also enforce the [1, 20] bounds.
        import pydantic

        with pytest.raises(pydantic.ValidationError):
            UCTaggingConfig(tag_update_concurrency=0)
        with pytest.raises(pydantic.ValidationError):
            UCTaggingConfig(tag_update_concurrency=21)

    def test_parses_max_allowable_consecutive_failures(self, tmp_path):
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text(
            "name: test_project\nuc_tagging:\n  max_allowable_consecutive_failures: 5\n"
        )
        from lhp.core.loaders import ProjectConfigLoader

        config = ProjectConfigLoader(tmp_path).load_project_config()
        assert config.uc_tagging.max_allowable_consecutive_failures == 5

    def test_max_failures_defaults_to_none_when_absent(self, tmp_path):
        # Another key present, this one absent → no limit (the SDP default).
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text(
            "name: test_project\nuc_tagging:\n  tag_update_concurrency: 8\n"
        )
        from lhp.core.loaders import ProjectConfigLoader

        config = ProjectConfigLoader(tmp_path).load_project_config()
        assert config.uc_tagging.max_allowable_consecutive_failures is None

    def test_explicit_null_max_failures_is_none(self, tmp_path):
        # An explicit `null` is indistinguishable from absent — both mean no limit.
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text(
            "name: test_project\n"
            "uc_tagging:\n"
            "  max_allowable_consecutive_failures: null\n"
        )
        from lhp.core.loaders import ProjectConfigLoader

        config = ProjectConfigLoader(tmp_path).load_project_config()
        assert config.uc_tagging.max_allowable_consecutive_failures is None

    def test_accepts_zero_max_failures(self, tmp_path):
        # 0 is falsy but legal, and means something very different from null:
        # disable the hook on the FIRST failure. A naive truthiness check in the
        # parser would silently turn it into "no limit".
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text(
            "name: test_project\nuc_tagging:\n  max_allowable_consecutive_failures: 0\n"
        )
        from lhp.core.loaders import ProjectConfigLoader

        config = ProjectConfigLoader(tmp_path).load_project_config()
        assert config.uc_tagging.max_allowable_consecutive_failures == 0

    def test_accepts_large_max_failures(self, tmp_path):
        # There is deliberately NO upper bound — the SDP contract is "integer >= 0
        # or None". This pins that decision against a future `le=` being added.
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text(
            "name: test_project\n"
            "uc_tagging:\n"
            "  max_allowable_consecutive_failures: 1000\n"
        )
        from lhp.core.loaders import ProjectConfigLoader

        config = ProjectConfigLoader(tmp_path).load_project_config()
        assert config.uc_tagging.max_allowable_consecutive_failures == 1000

    def test_rejects_invalid_max_failures(self, tmp_path):
        from lhp.core.loaders import ProjectConfigLoader
        from lhp.errors import LHPError

        # `true`/`false` are load-bearing: bool is an int subclass, so without an
        # explicit guard Pydantic would coerce `true` to a budget of 1. `1.5` pins
        # that YAML floats are rejected here, not coerced by Pydantic downstream.
        for bad in ("-1", "true", "false", "three", "1.5"):
            lhp_yaml = tmp_path / "lhp.yaml"
            lhp_yaml.write_text(
                f"name: test_project\n"
                f"uc_tagging:\n"
                f"  max_allowable_consecutive_failures: {bad}\n"
            )
            with pytest.raises(LHPError, match="LHP-CFG-009") as exc_info:
                ProjectConfigLoader(tmp_path).load_project_config()
            assert exc_info.value.code == "LHP-CFG-009"

    def test_model_rejects_negative_max_failures(self):
        # Direct construction must also enforce ge=0 on the Optional[int].
        import pydantic

        with pytest.raises(pydantic.ValidationError):
            UCTaggingConfig(max_allowable_consecutive_failures=-1)

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
        lhp_yaml.write_text("name: test_project\nuc_tagging:\n  enabled: maybe\n")
        from lhp.core.loaders import ProjectConfigLoader
        from lhp.errors import LHPError

        loader = ProjectConfigLoader(tmp_path)
        with pytest.raises(LHPError, match="must be a boolean"):
            loader.load_project_config()
