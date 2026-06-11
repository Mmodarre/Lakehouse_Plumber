"""Tests for ``parse_sandbox_config`` (the ``sandbox`` section of lhp.yaml).

All fields are optional with defaults (strategy ``table``, table_pattern
``{namespace}_{table}``, allowed_envs ``None`` = unrestricted). A non-mapping
``sandbox`` value, a strategy other than ``table``, or an empty
``allowed_envs`` list raises ``LHPError`` with code ``LHP-CFG-062``; a bad
``table_pattern`` raises ``LHP-CFG-063``.
"""

import pytest

from lhp.core.loaders import ProjectConfigLoader
from lhp.core.loaders._sandbox_config_parser import parse_sandbox_config
from lhp.errors import LHPError
from lhp.models import SandboxConfig


@pytest.mark.unit
class TestParseSandboxConfigValid:
    """Well-formed ``sandbox`` mappings parse into a ``SandboxConfig``."""

    def test_empty_mapping_applies_defaults(self):
        """An empty mapping is tolerated; all defaults apply."""
        config = parse_sandbox_config({})

        assert isinstance(config, SandboxConfig)
        assert config.strategy == "table"
        assert config.table_pattern == "{namespace}_{table}"
        assert config.allowed_envs is None

    def test_full_block_round_trips(self):
        """A fully-specified block round-trips onto the model."""
        config = parse_sandbox_config(
            {
                "strategy": "table",
                "table_pattern": "{namespace}__{table}",
                "allowed_envs": ["dev", "tst"],
            }
        )

        assert config.strategy == "table"
        assert config.table_pattern == "{namespace}__{table}"
        assert config.allowed_envs == ["dev", "tst"]

    def test_allowed_envs_explicit_none_means_unrestricted(self):
        """allowed_envs: null is accepted and means unrestricted."""
        config = parse_sandbox_config({"allowed_envs": None})

        assert config.allowed_envs is None

    def test_allowed_envs_non_empty_list_accepted(self):
        """A non-empty allowed_envs list is accepted as-is."""
        config = parse_sandbox_config({"allowed_envs": ["dev", "tst"]})

        assert config.allowed_envs == ["dev", "tst"]


@pytest.mark.unit
class TestParseSandboxConfigInvalid:
    """Block-level violations raise ``LHPError`` with code ``LHP-CFG-062``."""

    @pytest.mark.parametrize(
        "bad_value",
        [
            "table",  # string, not a mapping
            ["strategy"],  # list, not a mapping
            123,  # int, not a mapping
            None,  # bare `sandbox:` key in YAML
        ],
        ids=["string", "list", "int", "none"],
    )
    def test_non_mapping_raises_cfg_062(self, bad_value):
        """A non-mapping sandbox value raises LHPError with code LHP-CFG-062."""
        with pytest.raises(LHPError) as exc_info:
            parse_sandbox_config(bad_value)

        assert exc_info.value.code == "LHP-CFG-062"

    def test_bad_strategy_raises_cfg_062(self):
        """A strategy other than 'table' raises LHP-CFG-062 naming the value."""
        with pytest.raises(LHPError) as exc_info:
            parse_sandbox_config({"strategy": "schema"})

        assert exc_info.value.code == "LHP-CFG-062"
        assert "schema" in str(exc_info.value)

    def test_empty_allowed_envs_raises_cfg_062(self):
        """allowed_envs: [] (no env allowed) is a misconfiguration -> LHP-CFG-062."""
        with pytest.raises(LHPError) as exc_info:
            parse_sandbox_config({"allowed_envs": []})

        assert exc_info.value.code == "LHP-CFG-062"
        assert "allowed_envs" in str(exc_info.value)

    def test_non_list_allowed_envs_raises_cfg_062(self):
        """A non-list allowed_envs (e.g. a bare string) raises LHP-CFG-062."""
        with pytest.raises(LHPError) as exc_info:
            parse_sandbox_config({"allowed_envs": "dev"})

        assert exc_info.value.code == "LHP-CFG-062"

    def test_bad_pattern_plus_other_bad_field_raises_cfg_062(self):
        """When table_pattern AND another field fail, the block-level 062 wins."""
        with pytest.raises(LHPError) as exc_info:
            parse_sandbox_config({"strategy": "schema", "table_pattern": "{foo}"})

        assert exc_info.value.code == "LHP-CFG-062"


@pytest.mark.unit
class TestParseSandboxConfigBadTablePattern:
    """table_pattern violations raise ``LHPError`` with code ``LHP-CFG-063``."""

    @pytest.mark.parametrize(
        "bad_pattern",
        [
            "{foo}_{table}",  # unknown placeholder
            "{namespace}",  # missing {table}
            "{table}",  # missing {namespace}
            "{namespace}_{table:>10}",  # format spec not allowed
            "{namespace}_{table!r}",  # conversion not allowed
            "{namespace}-{table}",  # literal text limited to [A-Za-z0-9_]
            "{namespace_{table}",  # not a valid format string
        ],
        ids=[
            "unknown-placeholder",
            "missing-table",
            "missing-namespace",
            "format-spec",
            "conversion",
            "bad-literal-char",
            "unbalanced-brace",
        ],
    )
    def test_bad_table_pattern_raises_cfg_063(self, bad_pattern):
        """An invalid table_pattern raises LHPError with code LHP-CFG-063."""
        with pytest.raises(LHPError) as exc_info:
            parse_sandbox_config({"table_pattern": bad_pattern})

        assert exc_info.value.code == "LHP-CFG-063"

    def test_error_names_the_offending_pattern(self):
        """The 063 error details quote the offending pattern value."""
        with pytest.raises(LHPError) as exc_info:
            parse_sandbox_config({"table_pattern": "{foo}_{table}"})

        assert "{foo}_{table}" in str(exc_info.value)


@pytest.mark.unit
class TestProjectConfigLoaderSandboxWiring:
    """A ``sandbox`` block in lhp.yaml lands on ``ProjectConfig.sandbox``."""

    def test_sandbox_block_present_populates_project_config(self, tmp_path):
        """sandbox block in lhp.yaml -> ProjectConfig.sandbox is a SandboxConfig."""
        (tmp_path / "lhp.yaml").write_text(
            "name: sandbox_wiring_project\n"
            'version: "1.0"\n'
            "sandbox:\n"
            "  strategy: table\n"
            '  table_pattern: "{namespace}__{table}"\n'
            "  allowed_envs:\n"
            "    - dev\n"
            "    - tst\n"
        )

        project_config = ProjectConfigLoader(tmp_path).load_project_config()

        assert project_config is not None
        assert isinstance(project_config.sandbox, SandboxConfig)
        assert project_config.sandbox.strategy == "table"
        assert project_config.sandbox.table_pattern == "{namespace}__{table}"
        assert project_config.sandbox.allowed_envs == ["dev", "tst"]

    def test_sandbox_block_absent_leaves_none(self, tmp_path):
        """No sandbox block in lhp.yaml -> ProjectConfig.sandbox is None."""
        (tmp_path / "lhp.yaml").write_text(
            'name: sandbox_wiring_project\nversion: "1.0"\n'
        )

        project_config = ProjectConfigLoader(tmp_path).load_project_config()

        assert project_config is not None
        assert project_config.sandbox is None

    def test_invalid_sandbox_block_propagates_cfg_062(self, tmp_path):
        """A sub-parser LHPError propagates unchanged through load_project_config."""
        (tmp_path / "lhp.yaml").write_text(
            "name: sandbox_wiring_project\n"
            'version: "1.0"\n'
            "sandbox:\n"
            "  allowed_envs: []\n"
        )

        with pytest.raises(LHPError) as exc_info:
            ProjectConfigLoader(tmp_path).load_project_config()

        assert exc_info.value.code == "LHP-CFG-062"
