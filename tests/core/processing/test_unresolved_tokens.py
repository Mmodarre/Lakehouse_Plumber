"""Leftover ``${name}`` scan and its LHP-CFG-010 error (#298).

``find_unresolved_tokens`` reports only plain ``${name}`` references; dotted or
prefixed references (Spark / pipeline variables) and the deprecated bare
``{name}`` form are not reported. ``unresolved_tokens_error`` builds the
LHP-CFG-010 error raised for a file that still holds such a token.
"""

from pathlib import Path

import pytest

from lhp.core.processing.unresolved_tokens import (
    find_unresolved_tokens,
    unresolved_tokens_error,
)
from lhp.errors import ErrorCategory, LHPConfigError

pytestmark = pytest.mark.unit


class TestFindUnresolvedTokens:
    def test_reports_plain_tokens_once_in_first_appearance_order(self):
        text = "SELECT ${b} FROM ${a}.t WHERE x = '${b}'"

        assert find_unresolved_tokens(text) == ["b", "a"]

    @pytest.mark.parametrize(
        "reference",
        [
            "${mypipeline.start}",
            "${hiveconf:x}",
            "${env:X}",
            "${var.x}",
            "${secret:scope/key}",
            "{legacy_brace}",
            "%{local_var}",
            "{{ template_param }}",
            "${ spaced }",
        ],
    )
    def test_ignores_references_that_are_not_plain_tokens(self, reference):
        assert find_unresolved_tokens(f"SELECT * FROM t WHERE c = '{reference}'") == []

    def test_no_tokens_returns_empty_list(self):
        assert find_unresolved_tokens("SELECT 1") == []


class TestUnresolvedTokensError:
    def test_builds_cfg_010_naming_file_and_tokens(self):
        error = unresolved_tokens_error(
            ["order_suffix", "region"],
            file="sql/gold/orders.sql",
            env="dev",
            pipeline="gold",
            flowgroup="orders_fg",
            action="write_orders",
        )

        assert isinstance(error, LHPConfigError)
        assert error.code == "LHP-CFG-010"
        assert error.category is ErrorCategory.CONFIG
        assert error.title == "Unresolved substitution tokens detected"
        assert "sql/gold/orders.sql" in error.details
        assert "${order_suffix}, ${region}" in error.details
        assert error.context == {
            "File": "sql/gold/orders.sql",
            "Unresolved": "${order_suffix}, ${region}",
            "Environment": "dev",
            "Pipeline": "gold",
            "FlowGroup": "orders_fg",
            "Action": "write_orders",
        }
        assert any("substitutions/dev.yaml" in s for s in error.suggestions)
        assert any("${mypipeline.start_date}" in s for s in error.suggestions)

    def test_optional_context_is_omitted_when_unknown(self):
        error = unresolved_tokens_error(["x"], file="sql/a.sql")

        assert error.context == {"File": "sql/a.sql", "Unresolved": "${x}"}
        assert any("substitutions/<env>.yaml" in s for s in error.suggestions)

    def test_field_names_the_location_inside_the_file(self):
        error = unresolved_tokens_error(
            ["x"], file="lhp.yaml", field="monitoring.checkpoint_path", env="dev"
        )

        assert error.context["Field"] == "monitoring.checkpoint_path"
        assert "monitoring.checkpoint_path" in error.details

    def test_absolute_path_inside_project_is_shown_project_relative(self, tmp_path):
        error = unresolved_tokens_error(
            ["x"], file=tmp_path / "sql" / "a.sql", project_root=tmp_path
        )

        assert error.context["File"] == "sql/a.sql"

    def test_path_outside_project_is_shown_as_given(self, tmp_path):
        outside = Path("/elsewhere/a.sql")

        error = unresolved_tokens_error(["x"], file=outside, project_root=tmp_path)

        assert error.context["File"] == outside.as_posix()

    def test_context_never_carries_the_operational_metadata_keys(self):
        """The editor attributes CFG-010 carrying both ``Column`` and
        ``Expression`` to ``lhp.yaml``; file errors must not match that."""
        error = unresolved_tokens_error(["x"], file="sql/a.sql", env="dev")

        assert not {"Column", "Expression"} <= error.context.keys()
