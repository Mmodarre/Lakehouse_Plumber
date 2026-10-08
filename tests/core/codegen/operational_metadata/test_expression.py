"""Unit tests for the shared operational-metadata expression resolver (#289).

``resolve_metadata_expression`` is the ONE function both the render path
(``OperationalMetadataCatalog.get_selected_columns``) and the validate path
(``OperationalMetadataExpressionValidator``) call, so ``lhp validate`` and
``lhp generate`` cannot drift. Order, per the approved behaviour:

1. context tokens (``${pipeline_name}`` / ``${flowgroup_name}`` / per-action
   ``${source_table}``), winning over a substitutions key of the same name;
2. ``${secret:...}`` rejected (LHP-CFG-070);
3. environment ``${token}`` values from ``substitutions/<env>.yaml``;
4. any leftover ``${...}`` / ``%{...}`` / ``{{ ... }}`` -> LHP-CFG-010.
"""

from pathlib import Path

import pytest

from lhp.core.codegen.operational_metadata.expression import (
    action_context_tokens,
    resolve_metadata_expression,
)
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.errors import LHPError
from lhp.models import Action, ActionType

CONTEXT = {"pipeline_name": "repro", "flowgroup_name": "repro_fg"}


def _flat(error: LHPError) -> str:
    return " ".join(str(error).split())


def _mgr(
    tmp_path: Path, body: str = "", env: str = "dev"
) -> EnhancedSubstitutionManager:
    sub_file = tmp_path / f"{env}.yaml"
    sub_file.write_text(body or f"{env}:\n  placeholder: x\n")
    return EnhancedSubstitutionManager(substitution_file=sub_file, env=env)


@pytest.mark.unit
class TestEnvironmentTokens:
    @pytest.mark.parametrize(("env", "value"), [("dev", "STXX"), ("prod", "STPROD")])
    def test_env_token_resolves_per_environment(self, tmp_path, env, value):
        mgr = _mgr(tmp_path, f"{env}:\n  fd_source: {value}\n", env=env)
        result = resolve_metadata_expression(
            "_source",
            "F.lit('${fd_source}')",
            context_tokens=CONTEXT,
            substitution_mgr=mgr,
        )
        assert result == f"F.lit('{value}')"

    def test_context_tokens_resolve(self, tmp_path):
        result = resolve_metadata_expression(
            "_lineage",
            "F.lit('${pipeline_name}/${flowgroup_name}')",
            context_tokens=CONTEXT,
            substitution_mgr=_mgr(tmp_path),
        )
        assert result == "F.lit('repro/repro_fg')"

    def test_context_tokens_win_over_substitution_keys(self, tmp_path):
        mgr = _mgr(
            tmp_path,
            "dev:\n  pipeline_name: FROM_SUBS\n  flowgroup_name: FROM_SUBS_FG\n",
        )
        result = resolve_metadata_expression(
            "_lineage",
            "F.lit('${pipeline_name}:${flowgroup_name}')",
            context_tokens=CONTEXT,
            substitution_mgr=mgr,
        )
        assert result == "F.lit('repro:repro_fg')"

    def test_expression_without_tokens_is_unchanged(self, tmp_path):
        expression = "F.current_timestamp()"
        result = resolve_metadata_expression(
            "_ts",
            expression,
            context_tokens=CONTEXT,
            substitution_mgr=_mgr(tmp_path),
        )
        assert result == expression

    def test_regex_quantifier_is_not_flagged_or_rewritten(self, tmp_path):
        expression = 'F.col("a").rlike("\\\\d{8}")'
        mgr = _mgr(tmp_path, "dev:\n  '8': EIGHT\n  d: DEE\n")
        result = resolve_metadata_expression(
            "_is_date", expression, context_tokens=CONTEXT, substitution_mgr=mgr
        )
        assert result == expression

    def test_bare_brace_word_is_not_substituted(self, tmp_path):
        """Bare ``{token}`` is deprecated and never applied to expressions."""
        expression = "F.regexp_extract(F.col('f'), '{word}', 0)"
        mgr = _mgr(tmp_path, "dev:\n  word: REPLACED\n")
        result = resolve_metadata_expression(
            "_x", expression, context_tokens=CONTEXT, substitution_mgr=mgr
        )
        assert result == expression


@pytest.mark.unit
class TestSecretsRejected:
    def test_secret_reference_is_rejected(self, tmp_path):
        with pytest.raises(LHPError) as excinfo:
            resolve_metadata_expression(
                "_api_key",
                "F.lit('${secret:scope/key}')",
                context_tokens=CONTEXT,
                substitution_mgr=_mgr(tmp_path),
            )
        error = excinfo.value
        assert error.code == "LHP-CFG-070"
        text = _flat(error)
        assert "_api_key" in text
        assert "table data" in text
        assert "substitutions/dev.yaml" in text
        assert "transform" in text

    def test_secret_reached_through_env_token_is_rejected(self, tmp_path):
        mgr = _mgr(tmp_path, "dev:\n  creds: ${secret:scope/key}\n")
        with pytest.raises(LHPError) as excinfo:
            resolve_metadata_expression(
                "_creds",
                "F.lit('${creds}')",
                context_tokens=CONTEXT,
                substitution_mgr=mgr,
            )
        assert excinfo.value.code == "LHP-CFG-070"
        assert "_creds" in _flat(excinfo.value)

    def test_secret_is_rejected_even_without_substitution_manager(self):
        with pytest.raises(LHPError) as excinfo:
            resolve_metadata_expression(
                "_api_key",
                "F.lit('${secret:key}')",
                context_tokens=CONTEXT,
                substitution_mgr=None,
            )
        assert excinfo.value.code == "LHP-CFG-070"


@pytest.mark.unit
class TestUnresolvedTokens:
    @pytest.mark.parametrize(
        ("expression", "token"),
        [
            ("F.lit('${fd_missing}')", "${fd_missing}"),
            ("F.lit('%{local_var}')", "%{local_var}"),
            ("F.lit('{{ table_name }}')", "{{ table_name }}"),
        ],
    )
    def test_leftover_token_is_cfg_010(self, tmp_path, expression, token):
        with pytest.raises(LHPError) as excinfo:
            resolve_metadata_expression(
                "_source",
                expression,
                context_tokens=CONTEXT,
                substitution_mgr=_mgr(tmp_path),
            )
        error = excinfo.value
        assert error.code == "LHP-CFG-010"
        text = _flat(error)
        assert "_source" in text
        assert token in text
        assert "'dev'" in text
        assert "substitutions/dev.yaml" in text

    @pytest.mark.parametrize(
        "expression",
        ["F.lit('${fd_missing}')", "F.lit('${secret:scope/key}')"],
        ids=["cfg_010", "cfg_070"],
    )
    def test_error_locates_the_pipeline_and_flowgroup(self, tmp_path, expression):
        with pytest.raises(LHPError) as excinfo:
            resolve_metadata_expression(
                "_source",
                expression,
                context_tokens=CONTEXT,
                substitution_mgr=_mgr(tmp_path),
            )
        context = excinfo.value.context
        assert context["Pipeline"] == "repro"
        assert context["FlowGroup"] == "repro_fg"
        assert context["Column"] == "_source"

    def test_error_omits_location_without_flowgroup_context(self, tmp_path):
        with pytest.raises(LHPError) as excinfo:
            resolve_metadata_expression(
                "_source",
                "F.lit('${fd_missing}')",
                context_tokens={},
                substitution_mgr=_mgr(tmp_path),
            )
        assert "Pipeline" not in excinfo.value.context
        assert "FlowGroup" not in excinfo.value.context

    def test_without_substitution_manager_only_context_tokens_apply(self):
        """Library callers rendering outside an environment keep the
        pre-#289 behaviour: context tokens only, no unresolved-token check."""
        result = resolve_metadata_expression(
            "_x",
            "F.lit('${pipeline_name}-${source}')",
            context_tokens=CONTEXT,
            substitution_mgr=None,
        )
        assert result == "F.lit('repro-${source}')"

    def test_skip_validation_manager_does_not_raise(self, tmp_path):
        sub_file = tmp_path / "dev.yaml"
        sub_file.write_text("dev:\n  known: k\n")
        mgr = EnhancedSubstitutionManager(
            substitution_file=sub_file, env="dev", skip_validation=True
        )
        result = resolve_metadata_expression(
            "_x",
            "F.lit('${known}-${missing}')",
            context_tokens=CONTEXT,
            substitution_mgr=mgr,
        )
        assert result == "F.lit('k-${missing}')"


@pytest.mark.unit
class TestActionContextTokens:
    def test_delta_load_supplies_qualified_source_table(self):
        action = Action(
            name="l",
            type=ActionType.LOAD,
            source={"type": "delta", "catalog": "c", "schema": "s", "table": "t"},
            target="v",
        )
        assert action_context_tokens(action) == {"source_table": "c.s.t"}

    def test_delta_load_without_namespace_uses_table(self):
        action = Action(
            name="l",
            type=ActionType.LOAD,
            source={"type": "delta", "table": "src.customers"},
            target="v",
        )
        assert action_context_tokens(action) == {"source_table": "src.customers"}

    def test_jdbc_load_supplies_table(self):
        action = Action(
            name="l",
            type=ActionType.LOAD,
            source={"type": "jdbc", "table": "customers"},
            target="v",
        )
        assert action_context_tokens(action) == {"source_table": "customers"}

    def test_other_actions_supply_nothing(self):
        cloudfiles = Action(
            name="l",
            type=ActionType.LOAD,
            source={"type": "cloudfiles", "path": "/p", "format": "csv"},
            target="v",
        )
        assert action_context_tokens(cloudfiles) == {}
        assert action_context_tokens(None) == {}
