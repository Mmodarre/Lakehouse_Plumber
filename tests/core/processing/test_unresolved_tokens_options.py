"""The title and SQL-note options of :func:`unresolved_tokens_error` (#298).

The builder's defaults are the shared title and the SQL note. The ``sql_path``
loader turns on the located title, so ``lhp validate``'s one-line summary and
the editor name the file and the token; the tags-file and unresolved-setting
checks also turn the SQL note off, since their values are not SQL.
"""

from __future__ import annotations

import pytest

from lhp.core.processing import unresolved_tokens_error

pytestmark = pytest.mark.unit

_SQL_NOTE = (
    "in SQL, if nothing defines it at runtime (a pipeline 'configuration:' value "
    "or a Spark conf), Spark substitutes an empty string."
)


def test_defaults_keep_the_shared_title_and_the_sql_note():
    err = unresolved_tokens_error(["x"], file="sql/q.sql", env="dev")

    assert err.title == "Unresolved substitution tokens detected"
    assert err.details.endswith(
        "A token LHP has no value for is left in the generated output as "
        f"written; {_SQL_NOTE}"
    )


def test_located_title_names_tokens_file_and_field():
    err = unresolved_tokens_error(
        ["a", "b"], file="lhp.yaml", field="event_log", located_title=True
    )

    assert (
        err.title
        == "Unresolved substitution token(s) ${a}, ${b} in lhp.yaml (event_log)"
    )


def test_located_title_without_field_names_the_file(tmp_path):
    err = unresolved_tokens_error(
        ["a"], file=tmp_path / "x.yaml", project_root=tmp_path, located_title=True
    )

    assert err.title == "Unresolved substitution token(s) ${a} in x.yaml"


def test_sql_note_can_be_left_off():
    err = unresolved_tokens_error(["x"], file="lhp.yaml", sql_note=False)

    assert err.details.endswith(
        "A token LHP has no value for is left in the generated output as written."
    )
    assert "Spark" not in err.details


def test_sql_suggestions_point_a_pipeline_parameter_at_a_dotted_key():
    """A pipeline ``configuration:`` key read by Spark SQL at runtime must not be
    frozen into ``substitutions/<env>.yaml`` as a constant: the advice keeps the
    substitutions fix and adds the dotted-key form LHP leaves untouched."""
    err = unresolved_tokens_error(["start_date"], file="sql/q.sql", env="dev")

    assert err.suggestions[0].startswith("Add the missing key(s) to substitutions/")
    assert any(
        "pipeline 'configuration:' parameter" in s
        and "${mypipeline.start_date}" in s
        and "LHP leaves untouched" in s
        for s in err.suggestions
    )


def test_non_sql_suggestions_do_not_mention_pipeline_parameters():
    err = unresolved_tokens_error(["x"], file="lhp.yaml", sql_note=False)

    assert not any("configuration:" in s for s in err.suggestions)
    assert any("${mypipeline.start}" in s for s in err.suggestions)
