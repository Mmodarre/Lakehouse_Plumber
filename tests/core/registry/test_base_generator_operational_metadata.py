"""Render path of #289: ``BaseActionGenerator._get_operational_metadata``.

Generators receive the run's ``substitution_manager`` in their context; the
render path must hand it to the shared expression resolver so environment
``${tokens}`` resolve into generated code, and an unresolved token or a secret
fails loudly instead of being emitted literally.
"""

from pathlib import Path

import pytest

from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.errors import LHPError
from lhp.generators.load import DeltaLoadGenerator, JDBCLoadGenerator, SQLLoadGenerator
from lhp.models import (
    Action,
    ActionType,
    FlowGroup,
    MetadataColumnConfig,
    ProjectConfig,
    ProjectOperationalMetadataConfig,
)


def _context(tmp_path: Path, env: str, body: str, columns: dict) -> dict:
    sub_file = tmp_path / f"{env}.yaml"
    sub_file.write_text(body)
    project_config = ProjectConfig(
        name="p",
        version="1.0",
        operational_metadata=ProjectOperationalMetadataConfig(
            columns={
                name: MetadataColumnConfig(expression=expr, applies_to=["view"])
                for name, expr in columns.items()
            }
        ),
    )
    return {
        "flowgroup": FlowGroup(pipeline="repro", flowgroup="repro_fg", actions=[]),
        "project_config": project_config,
        "preset_config": {},
        "substitution_manager": EnhancedSubstitutionManager(
            substitution_file=sub_file, env=env
        ),
    }


def _sql_load(*columns: str) -> Action:
    return Action(
        name="load_raw",
        type=ActionType.LOAD,
        source={"type": "sql", "sql": "SELECT 1 AS id"},
        target="v_raw",
        operational_metadata=list(columns),
    )


@pytest.mark.unit
class TestRenderPathResolution:
    @pytest.mark.parametrize(("env", "value"), [("dev", "STXX"), ("prod", "STPROD")])
    def test_env_token_resolves_into_generated_code(self, tmp_path, env, value):
        context = _context(
            tmp_path,
            env,
            f"{env}:\n  fd_source: {value}\n",
            {"_source": "F.lit('${fd_source}')"},
        )
        code = SQLLoadGenerator().generate(_sql_load("_source"), context)
        assert f"F.lit('{value}')" in code
        assert "${fd_source}" not in code

    def test_unresolved_token_raises_cfg_010(self, tmp_path):
        context = _context(
            tmp_path,
            "dev",
            "dev:\n  fd_source: STXX\n",
            {"_source": "F.lit('${fd_missing}')"},
        )
        with pytest.raises(LHPError) as excinfo:
            SQLLoadGenerator().generate(_sql_load("_source"), context)
        assert excinfo.value.code == "LHP-CFG-010"

    def test_secret_raises_cfg_070(self, tmp_path):
        context = _context(
            tmp_path,
            "dev",
            "dev:\n  fd_source: STXX\n",
            {"_key": "F.lit('${secret:scope/key}')"},
        )
        with pytest.raises(LHPError) as excinfo:
            SQLLoadGenerator().generate(_sql_load("_key"), context)
        assert excinfo.value.code == "LHP-CFG-070"

    def test_regex_expression_is_emitted_unchanged(self, tmp_path):
        expression = 'F.col("a").rlike("\\\\d{8}")'
        context = _context(
            tmp_path, "dev", "dev:\n  d: DEE\n", {"_is_date": expression}
        )
        code = SQLLoadGenerator().generate(_sql_load("_is_date"), context)
        assert expression in code

    def test_source_table_context_token_wins_for_delta(self, tmp_path):
        context = _context(
            tmp_path,
            "dev",
            "dev:\n  source_table: FROM_SUBS\n",
            {"_source_table": "F.lit('${source_table}')"},
        )
        action = Action(
            name="load_delta",
            type=ActionType.LOAD,
            source={"type": "delta", "table": "src.customers"},
            target="v_customers",
            operational_metadata=["_source_table"],
        )
        code = DeltaLoadGenerator().generate(action, context)
        assert "F.lit('src.customers')" in code

    def test_source_table_context_token_for_jdbc(self, tmp_path):
        context = _context(
            tmp_path,
            "dev",
            "dev:\n  x: y\n",
            {"_source_table": "F.lit('${source_table}')"},
        )
        action = Action(
            name="load_jdbc",
            type=ActionType.LOAD,
            source={
                "type": "jdbc",
                "url": "jdbc:postgresql://h/db",
                "user": "u",
                "password": "p",
                "driver": "org.postgresql.Driver",
                "table": "customers",
            },
            target="v_customers",
            operational_metadata=["_source_table"],
        )
        code = JDBCLoadGenerator().generate(action, context)
        assert "F.lit('customers')" in code
