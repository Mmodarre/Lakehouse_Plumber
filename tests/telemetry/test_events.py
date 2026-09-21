"""Tests for :mod:`lhp.telemetry._events` and :mod:`lhp.telemetry._project_shape`.

Three contracts are pinned here because the Worker's allowlist, the Delta
table and the dashboards are all written against them: the envelope's key
ORDER, the exact prop key sets, and the fact that an unrecognised
``action_counts_by_type`` key can never reach the wire as a new column.
"""

import copy
import json
import pickle
from dataclasses import FrozenInstanceError, asdict, fields
from typing import Any, Dict

import pytest

from lhp.telemetry._events import (
    EVENT_NAMES,
    LHP_CODE_PATTERN,
    PROJECT_SHAPE_KEYS,
    SCHEMA_VERSION,
    CliCommandProps,
    InstallProps,
    ProjectShape,
    TelemetryEnvelope,
    WebRunProps,
    WebSessionProps,
    fold_project_shape,
    is_lhp_code,
    to_json_dict,
)

ENVELOPE_KEY_ORDER = [
    "schema_version",
    "event_id",
    "event",
    "ts",
    "install_id",
    "project_id",
    "project_id_source",
    "lhp_version",
    "python",
    "os",
    "arch",
    "install_kind",
    "ci_vendor",
    "agent",
    "databricks_runtime",
    "interactive",
    "props",
]


def _envelope(**overrides: Any) -> TelemetryEnvelope:
    defaults: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "event_id": "5a1d2b6e-3f0c-4c7e-9b1a-0d8e2f4a6c11",
        "event": "cli.command",
        "ts": "2026-09-21T10:15:30.123Z",
        "install_id": "c0ffee11-2222-4333-8444-555566667777",
        "project_id": "9f2c1e0a7b3d4e5f6a7b8c9d0e1f2a3b",
        "project_id_source": "lhp_yaml",
        "lhp_version": "0.9.2",
        "python": "3.12",
        "os": "macos",
        "arch": "arm64",
        "install_kind": "wheel",
        "ci_vendor": "none",
        "agent": "claude_code",
        "databricks_runtime": False,
        "interactive": True,
        "props": {"command": "generate"},
    }
    defaults.update(overrides)
    return TelemetryEnvelope(**defaults)


# schema constants


@pytest.mark.unit
def test_schema_version_is_one() -> None:
    assert SCHEMA_VERSION == 1


@pytest.mark.unit
def test_event_names_are_the_five_emitted_names() -> None:
    assert EVENT_NAMES == (
        "cli.command",
        "web.session",
        "web.run",
        "install.first_seen",
        "install.upgraded",
    )


@pytest.mark.unit
def test_web_ui_is_reserved_but_not_emitted() -> None:
    assert "web.ui" not in EVENT_NAMES


# envelope


@pytest.mark.unit
def test_to_json_dict_emits_the_envelope_keys_in_order() -> None:
    assert list(to_json_dict(_envelope())) == ENVELOPE_KEY_ORDER


@pytest.mark.unit
def test_to_json_dict_props_is_a_plain_dict() -> None:
    payload = to_json_dict(_envelope())
    assert type(payload["props"]) is dict
    assert payload["props"] == {"command": "generate"}


@pytest.mark.unit
def test_to_json_dict_survives_a_json_round_trip() -> None:
    payload = to_json_dict(_envelope())
    assert json.loads(json.dumps(payload)) == payload


@pytest.mark.unit
def test_envelope_accepts_null_identifiers() -> None:
    payload = to_json_dict(_envelope(install_id=None, project_id=None))
    assert payload["install_id"] is None
    assert payload["project_id"] is None
    assert json.loads(json.dumps(payload))["project_id_source"] == "lhp_yaml"


@pytest.mark.unit
def test_envelope_is_frozen() -> None:
    envelope = _envelope()
    with pytest.raises(FrozenInstanceError):
        envelope.event = "web.run"  # type: ignore[misc]


@pytest.mark.unit
def test_envelope_props_do_not_alias_the_caller_mapping() -> None:
    source = {"command": "generate"}
    envelope = _envelope(props=source)
    source["command"] = "validate"
    assert envelope.props["command"] == "generate"


@pytest.mark.unit
def test_equal_envelopes_compare_equal() -> None:
    assert _envelope() == _envelope()


@pytest.mark.unit
def test_envelope_survives_the_copy_protocols_the_client_uses() -> None:
    # The spool writer and the sender thread hand envelopes around, so the
    # props mapping must stay a type these protocols accept.
    envelope = _envelope()
    assert copy.deepcopy(envelope) == envelope
    assert pickle.loads(pickle.dumps(envelope)) == envelope
    assert asdict(envelope)["props"] == {"command": "generate"}


@pytest.mark.unit
def test_to_json_dict_result_does_not_alias_the_envelope() -> None:
    envelope = _envelope()
    payload = to_json_dict(envelope)
    payload["props"]["command"] = "validate"
    assert envelope.props["command"] == "generate"


@pytest.mark.unit
def test_cli_command_props_default_code_counters_are_independent() -> None:
    first = CliCommandProps(
        command="generate", flags=(), env_class=None, duration_ms=1, exit_code=0
    )
    second = CliCommandProps(
        command="validate", flags=(), env_class=None, duration_ms=1, exit_code=0
    )
    assert first.warning_codes == {} and second.warning_codes == {}
    assert first.warning_codes is not second.warning_codes


# props dataclasses


@pytest.mark.unit
def test_cli_command_props_keys_match_the_design() -> None:
    assert [f.name for f in fields(CliCommandProps)] == [
        "command",
        "flags",
        "env_class",
        "duration_ms",
        "exit_code",
        "error_code",
        "exception_class",
        "warning_codes",
        "failure_codes",
        "files_written",
        "bundle_enabled",
        "cache_used",
        "project",
    ]


@pytest.mark.unit
def test_web_session_props_keys_match_the_design() -> None:
    assert [f.name for f in fields(WebSessionProps)] == [
        "session_id",
        "duration_s",
        "end_reason",
        "sse_seen",
        "requests_by_family",
        "files_created",
        "files_updated",
        "files_deleted",
        "runs",
        "dag_views",
        "lineage_views",
        "sandbox_toggles",
        "assistant_used",
        "assistant_provider",
        "assistant_mode",
        "ui",
    ]


@pytest.mark.unit
def test_web_run_props_keys_match_the_design() -> None:
    assert [f.name for f in fields(WebRunProps)] == [
        "session_id",
        "kind",
        "trigger",
        "env_class",
        "sandbox",
        "pipeline_filter",
        "bundle_enabled",
        "duration_ms",
        "success",
        "aborted",
        "error_code",
        "error_count",
        "warning_count",
        "files_written",
    ]


@pytest.mark.unit
def test_install_props_carry_only_the_previous_version() -> None:
    assert asdict(InstallProps(previous_version="0.9.1")) == {
        "previous_version": "0.9.1"
    }


@pytest.mark.unit
@pytest.mark.parametrize(
    "props",
    [
        CliCommandProps(
            command="generate",
            flags=("env",),
            env_class="development",
            duration_ms=10,
            exit_code=0,
        ),
        WebSessionProps(
            session_id="s",
            duration_s=5,
            end_reason="idle",
            sse_seen=True,
            requests_by_family={},
            files_created={},
            files_updated={},
            files_deleted={},
            runs={},
            dag_views=0,
            lineage_views=0,
            sandbox_toggles=0,
            assistant_used=False,
            assistant_provider=None,
            assistant_mode=None,
            ui={},
        ),
        WebRunProps(
            session_id="s",
            kind="validate",
            trigger="auto",
            env_class="none",
            sandbox=False,
            pipeline_filter=False,
            bundle_enabled=None,
            duration_ms=1,
            success=True,
            aborted=False,
            error_code=None,
            error_count=0,
            warning_count=0,
            files_written=None,
        ),
        InstallProps(previous_version="0.9.1"),
    ],
)
def test_props_dataclasses_are_frozen_and_json_serialisable(props: Any) -> None:
    with pytest.raises(FrozenInstanceError):
        setattr(props, fields(props)[0].name, "mutated")
    # A tuple field serialises to a JSON array, so the round-trip is asserted
    # on the key order rather than on value identity.
    parsed = json.loads(json.dumps(asdict(props)))
    assert list(parsed) == [f.name for f in fields(props)]


@pytest.mark.unit
def test_cli_command_props_nest_the_project_shape_one_level_deep() -> None:
    props = CliCommandProps(
        command="generate",
        flags=("env",),
        env_class="development",
        duration_ms=10,
        exit_code=0,
        project=ProjectShape(pipelines=2, flowgroups=5),
    )
    payload = asdict(props)
    assert payload["project"]["pipelines"] == 2
    assert payload["project"]["load_other"] == 0
    assert json.dumps(payload)


# project shape allowlist


@pytest.mark.unit
def test_project_shape_keys_are_the_design_allowlist_in_order() -> None:
    assert PROJECT_SHAPE_KEYS == (
        "pipelines",
        "flowgroups",
        "actions",
        "tables",
        "load_cloudfiles",
        "load_delta",
        "load_sql",
        "load_python",
        "load_jdbc",
        "load_custom_datasource",
        "load_kafka",
        "load_other",
        "transform_sql",
        "transform_python",
        "transform_data_quality",
        "transform_temp_table",
        "transform_schema",
        "transform_other",
        "write_streaming_table",
        "write_materialized_view",
        "write_sink",
        "write_other",
        "write_mode_standard",
        "write_mode_cdc",
        "write_mode_snapshot_cdc",
        "write_mode_other",
        "test_row_count",
        "test_uniqueness",
        "test_referential_integrity",
        "test_completeness",
        "test_range",
        "test_schema_match",
        "test_all_lookups_found",
        "test_custom_sql",
        "test_custom_expectations",
        "test_other",
        "templates",
        "flowgroups_using_templates",
        "presets",
        "blueprints",
        "blueprint_instances",
        "environments",
        "has_operational_metadata",
        "has_event_log",
        "has_monitoring",
        "has_uc_tagging",
        "has_test_reporting",
        "has_wheel",
        "has_sandbox",
        "has_required_lhp_version",
        "apply_formatting",
    )


@pytest.mark.unit
def test_project_shape_fields_match_the_allowlist() -> None:
    assert tuple(f.name for f in fields(ProjectShape)) == PROJECT_SHAPE_KEYS


@pytest.mark.unit
def test_project_shape_defaults_to_zeros_and_false() -> None:
    shape = asdict(ProjectShape())
    assert shape["pipelines"] == 0
    assert shape["has_wheel"] is False
    assert shape["apply_formatting"] is False


@pytest.mark.unit
def test_project_shape_is_frozen() -> None:
    with pytest.raises(FrozenInstanceError):
        ProjectShape().pipelines = 1  # type: ignore[misc]


# fold_project_shape


@pytest.mark.unit
def test_fold_normalises_an_enum_rendered_suffix() -> None:
    shape = fold_project_shape({"transform_TransformType.SQL": 14})
    assert shape.transform_sql == 14
    assert shape.transform_other == 0


@pytest.mark.unit
def test_fold_normalises_every_action_family_suffix() -> None:
    raw = {
        "load_CloudFiles": 1,
        "transform_TransformType.DATA_QUALITY": 2,
        "write_WriteTargetType.SINK": 3,
        "write_mode_WriteMode.CDC": 4,
        "test_TestActionType.RANGE": 5,
    }
    shape = fold_project_shape(raw)
    assert (shape.load_cloudfiles, shape.transform_data_quality) == (1, 2)
    assert (shape.write_sink, shape.write_mode_cdc, shape.test_range) == (3, 4, 5)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("key", "attribute"),
    [
        ("load_snowflake", "load_other"),
        ("transform_wizardry", "transform_other"),
        ("write_iceberg", "write_other"),
        ("write_mode_upsert", "write_mode_other"),
        ("test_smoke", "test_other"),
    ],
)
def test_fold_sends_an_unknown_subtype_to_its_family_other(
    key: str, attribute: str
) -> None:
    shape = fold_project_shape({key: 7})
    assert getattr(shape, attribute) == 7


@pytest.mark.unit
def test_fold_accumulates_several_unknown_subtypes_into_one_other() -> None:
    shape = fold_project_shape(
        {"load_snowflake": 2, "load_bigquery": 3, "load_other": 1}
    )
    assert shape.load_other == 6


@pytest.mark.unit
def test_fold_drops_keys_outside_the_allowlist() -> None:
    shape = fold_project_shape(
        {"project_id": 1, "load": 9, "transform": 4, "catalog_name": 2, "pipelines": 3}
    )
    assert shape == ProjectShape(pipelines=3)


@pytest.mark.unit
def test_fold_keeps_flags_as_booleans() -> None:
    shape = fold_project_shape(
        {"has_wheel": True, "has_sandbox": False, "apply_formatting": True}
    )
    assert (shape.has_wheel, shape.has_sandbox, shape.apply_formatting) == (
        True,
        False,
        True,
    )


@pytest.mark.unit
def test_fold_of_an_empty_mapping_is_the_default_shape() -> None:
    assert fold_project_shape({}) == ProjectShape()


@pytest.mark.unit
def test_fold_never_produces_a_key_outside_the_allowlist() -> None:
    raw = {f"load_{index}": 1 for index in range(50)}
    raw["totally_unknown"] = 1
    payload = asdict(fold_project_shape(raw))
    assert set(payload) == set(PROJECT_SHAPE_KEYS)
    assert payload["load_other"] == 50


# LHP code filter: the only free-form strings that may become wire keys


@pytest.mark.unit
@pytest.mark.parametrize(
    "code",
    [
        "LHP-DEP-002",
        "LHP-IO-028",
        "LHP-DEPR-001",
        "LHP-VAL-DUPFG",
        "LHP-EVT-SOFT-CAP",
        "LHP-GEN-902",
    ],
)
def test_is_lhp_code_accepts_every_registered_code_shape(code: str) -> None:
    assert is_lhp_code(code) is True
    assert LHP_CODE_PATTERN.fullmatch(code)


@pytest.mark.unit
@pytest.mark.parametrize(
    "value",
    [
        "",
        None,
        42,
        ["LHP-DEP-002"],
        "lhp-dep-002",
        "LHP-DEP-",
        "LHP-D-002",
        "LHP-TOOLONG-002",
        "LHP-DEP-002 extra",
        "LHP-DEP-002\n",
        "Unknown action type 'nope'",
        "LHP-DEP-" + "0" * 15,
    ],
)
def test_is_lhp_code_rejects_anything_else(value: Any) -> None:
    assert is_lhp_code(value) is False


@pytest.mark.unit
def test_an_accepted_code_never_exceeds_the_error_code_cap() -> None:
    longest = "LHP-ABCDE-" + "A" * 14
    assert is_lhp_code(longest) is True
    assert len(longest) == 24
    assert is_lhp_code(longest + "A") is False
