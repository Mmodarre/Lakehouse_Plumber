"""Tests for ReplaceWhereConfigValidator (``mode: replace_where``).

Every rule yields exactly one error string that starts with the action prefix,
so each test asserts the error count and the key phrase of the message.
"""

from typing import Any, Dict, List, Optional

import pytest

from lhp.core.validators.compatibility import ReplaceWhereConfigValidator
from lhp.models import Action, ActionType

_PREFIX = "Action[0] 'write_orders_recent'"
_PREDICATE = "order_date >= date_add(current_date(), -7)"
_ABSENT = object()


def _replace_where_action(
    replace_where_config: Any = None,
    *,
    read_mode: Optional[str] = None,
    expectations: Optional[List[Dict[str, Any]]] = None,
    **write_target_overrides: Any,
) -> Action:
    write_target: Dict[str, Any] = {
        "type": "streaming_table",
        "mode": "replace_where",
        "catalog": "cat",
        "schema": "silver",
        "table": "orders_recent",
    }
    if replace_where_config is None:
        replace_where_config = {"replace_where": _PREDICATE}
    if replace_where_config is not _ABSENT:
        write_target["replace_where_config"] = replace_where_config
    write_target.update(write_target_overrides)

    return Action(
        name="write_orders_recent",
        type=ActionType.WRITE,
        source="v_orders_source",
        readMode=read_mode,
        expectations=expectations,
        write_target=write_target,
    )


def _validate(action: Action) -> List[str]:
    return ReplaceWhereConfigValidator().validate(action, _PREFIX)


def _assert_single_error(errors: List[str], phrase: str) -> None:
    assert len(errors) == 1, errors
    assert errors[0].startswith(f"{_PREFIX}: "), errors[0]
    assert phrase in errors[0], errors[0]


@pytest.mark.parametrize(
    ("config", "phrase"),
    [
        pytest.param(_ABSENT, "requires 'replace_where_config'", id="absent"),
        pytest.param(
            "order_date >= '2026-01-01'", "must be a dictionary", id="not_a_dict"
        ),
    ],
)
def test_missing_config_block(config: Any, phrase: str) -> None:
    errors = _validate(_replace_where_action(config))

    _assert_single_error(errors, phrase)


@pytest.mark.parametrize(
    ("config", "phrase"),
    [
        pytest.param({}, "must have 'replace_where'", id="missing_key"),
        pytest.param({"replace_where": None}, "must have 'replace_where'", id="null"),
        pytest.param({"replace_where": 7}, "must be a SQL predicate string", id="int"),
        pytest.param({"replace_where": ""}, "cannot be blank", id="empty"),
        pytest.param({"replace_where": "  \n\t "}, "cannot be blank", id="whitespace"),
    ],
)
def test_blank_predicate(config: Dict[str, Any], phrase: str) -> None:
    errors = _validate(_replace_where_action(config))

    _assert_single_error(errors, phrase)


def test_unknown_nested_key() -> None:
    config = {"replace_where": _PREDICATE, "replace_using": ["order_id"]}

    errors = _validate(_replace_where_action(config))

    _assert_single_error(errors, "unknown key(s) 'replace_using'")
    assert "only allowed key is 'replace_where'" in errors[0]


def test_secret_placeholder_rejected() -> None:
    # The substitution pass has already turned ${secret:etl/cutoff} into this
    # placeholder by the time validation sees the resolved flowgroup.
    config = {"replace_where": "order_date >= '__SECRET_etl_cutoff__'"}

    errors = _validate(_replace_where_action(config))

    _assert_single_error(errors, "cannot reference a secret")


def test_create_table_false_rejected() -> None:
    errors = _validate(_replace_where_action(create_table=False))

    _assert_single_error(errors, "does not support 'create_table: false'")


def test_temporary_rejected() -> None:
    errors = _validate(_replace_where_action(temporary=True))

    _assert_single_error(errors, "does not support 'temporary'")
    assert "private: true" in errors[0]


def test_stream_read_mode_rejected() -> None:
    errors = _validate(_replace_where_action(read_mode="stream"))

    _assert_single_error(errors, "requires a batch source")
    assert "REPLACE_WHERE_WITH_STREAMING_SOURCE" in errors[0]


def test_inline_expectations_rejected() -> None:
    expectations = [{"name": "valid_id", "expression": "id IS NOT NULL"}]

    errors = _validate(_replace_where_action(expectations=expectations))

    _assert_single_error(errors, "does not support expectations")
    assert (
        "Expectations are not supported on tables targeted by REPLACE WHERE flows."
        in errors[0]
    )


@pytest.mark.parametrize(
    ("read_mode", "expectations", "overrides"),
    [
        pytest.param(None, None, {}, id="minimal"),
        pytest.param(
            "batch",
            [],
            {"create_table": True, "private": True, "temporary": False},
            id="explicit_compatible_settings",
        ),
    ],
)
def test_valid_config_returns_no_errors(
    read_mode: Optional[str],
    expectations: Optional[List[Dict[str, Any]]],
    overrides: Dict[str, Any],
) -> None:
    action = _replace_where_action(
        read_mode=read_mode, expectations=expectations, **overrides
    )

    assert _validate(action) == []
