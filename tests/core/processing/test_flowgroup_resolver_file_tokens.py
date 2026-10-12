"""External-file token check wired into flowgroup resolution (#298).

``FlowgroupResolutionService.process_flowgroup`` is the step BOTH ``lhp
validate`` and ``lhp generate`` run per flowgroup, so wiring the
``ExternalFileTokenValidator`` there makes both commands reject an unresolved
``${name}`` in a ``sql_path`` file. ``validate_config=False`` (``lhp dag``) and
a ``skip_validation`` manager keep resolving without reading the file.
Real validators — no mocks.
"""

import functools
from pathlib import Path

import pytest

from lhp.core.codegen import check_tags_file_tokens
from lhp.core.loaders import load_sql_file
from lhp.core.processing import EnhancedSubstitutionManager
from lhp.core.processing.flowgroup_resolver import FlowgroupResolutionService
from lhp.core.validators import (
    ConfigValidator,
    ExternalFileTokenValidator,
    SecretValidator,
)
from lhp.errors import LHPError
from lhp.models import Action, ActionType, FlowGroup, FlowGroupContext, ProjectConfig

pytestmark = pytest.mark.unit


def _resolver(root: Path) -> FlowgroupResolutionService:
    project_config = ProjectConfig(name="p", version="1.0")
    return FlowgroupResolutionService(
        config_validator=ConfigValidator(root, project_config),
        secret_validator=SecretValidator(),
        file_token_validator=ExternalFileTokenValidator(
            root,
            load_sql_file,
            functools.partial(
                check_tags_file_tokens,
                project_config=project_config,
                project_root=root,
            ),
        ),
    )


def _mgr(root: Path, *, skip_validation: bool = False):
    sub_file = root / "dev.yaml"
    sub_file.write_text("dev:\n  catalog: main\n")
    return EnhancedSubstitutionManager(
        substitution_file=sub_file, env="dev", skip_validation=skip_validation
    )


def _ctx(root: Path, sql: str) -> FlowGroupContext:
    (root / "sql").mkdir(exist_ok=True)
    (root / "sql" / "q.sql").write_text(sql)
    flowgroup = FlowGroup(
        pipeline="gold",
        flowgroup="orders_fg",
        actions=[
            Action(
                name="write_orders",
                type=ActionType.WRITE,
                write_target={
                    "type": "materialized_view",
                    "catalog": "c",
                    "schema": "s",
                    "table": "orders",
                    "sql_path": "sql/q.sql",
                },
            )
        ],
    )
    return FlowGroupContext(flowgroup=flowgroup, source_yaml=Path("pipelines/p.yaml"))


@pytest.mark.unit
class TestResolverFileTokenCheck:
    def test_undefined_token_in_sql_path_raises_cfg_010(self, tmp_path):
        with pytest.raises(LHPError) as excinfo:
            _resolver(tmp_path).process_flowgroup(
                _ctx(tmp_path, "SELECT * FROM t${order_suffix}"), _mgr(tmp_path)
            )

        assert excinfo.value.code == "LHP-CFG-010"
        assert excinfo.value.context["File"] == "sql/q.sql"
        assert excinfo.value.context["Unresolved"] == "${order_suffix}"

    def test_defined_token_resolves(self, tmp_path):
        ctx = _resolver(tmp_path).process_flowgroup(
            _ctx(tmp_path, "SELECT * FROM ${catalog}.s.t"), _mgr(tmp_path)
        )

        assert ctx.flowgroup.flowgroup == "orders_fg"

    def test_validate_config_false_skips_the_check(self, tmp_path):
        """``lhp dag`` resolves with ``validate_config=False``."""
        ctx = _resolver(tmp_path).process_flowgroup(
            _ctx(tmp_path, "SELECT * FROM t${order_suffix}"),
            _mgr(tmp_path),
            validate_config=False,
        )

        assert ctx.flowgroup.flowgroup == "orders_fg"

    def test_skip_validation_manager_skips_the_check(self, tmp_path):
        ctx = _resolver(tmp_path).process_flowgroup(
            _ctx(tmp_path, "SELECT * FROM t${order_suffix}"),
            _mgr(tmp_path, skip_validation=True),
        )

        assert ctx.flowgroup.flowgroup == "orders_fg"

    def test_resolver_without_the_validator_does_not_read_files(self, tmp_path):
        """Resolvers whose output never reaches generation (template preview,
        dependency analysis) are built without it."""
        resolver = FlowgroupResolutionService(
            config_validator=ConfigValidator(tmp_path, ProjectConfig(name="p")),
            secret_validator=SecretValidator(),
        )

        ctx = resolver.process_flowgroup(
            _ctx(tmp_path, "SELECT * FROM t${order_suffix}"), _mgr(tmp_path)
        )

        assert ctx.flowgroup.flowgroup == "orders_fg"
