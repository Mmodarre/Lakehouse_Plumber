"""Dual-syntax tests for `BlueprintInstance.model_validator`.

The validator is the single normalization point for the two accepted input
shapes: ``use_blueprint:`` + nested ``parameters:`` (preferred) and the
legacy ``blueprint:`` + flat parameters form (deprecated, removed in V0.9).

These tests verify both forms round-trip through ``parameter_values()`` and
``blueprint_name`` to identical outputs, and that mixing the forms raises
LHP-VAL-061 with a clear migration message.
"""

import logging

import pytest
from pydantic import ValidationError

from lhp.models.config import BlueprintInstance
from lhp.utils.error_formatter import LHPValidationError  # noqa: F401


def _new_form() -> dict:
    return {
        "use_blueprint": "halo_bronze",
        "parameters": {
            "variant": "acme",
            "domain_id": "ACME001",
            "source_path": "s3://bucket/acme/",
        },
    }


def _legacy_form() -> dict:
    return {
        "blueprint": "halo_bronze",
        "variant": "acme",
        "domain_id": "ACME001",
        "source_path": "s3://bucket/acme/",
    }


def test_new_form_normalizes_to_canonical():
    inst = BlueprintInstance(**_new_form())
    assert inst.blueprint_name == "halo_bronze"
    assert inst.parameter_values() == {
        "variant": "acme",
        "domain_id": "ACME001",
        "source_path": "s3://bucket/acme/",
    }


def test_legacy_form_normalizes_to_canonical():
    inst = BlueprintInstance(**_legacy_form())
    assert inst.blueprint_name == "halo_bronze"
    assert inst.parameter_values() == {
        "variant": "acme",
        "domain_id": "ACME001",
        "source_path": "s3://bucket/acme/",
    }


def test_new_and_legacy_produce_identical_normalized_state():
    new = BlueprintInstance(**_new_form())
    legacy = BlueprintInstance(**_legacy_form())
    assert new.blueprint_name == legacy.blueprint_name
    assert new.parameter_values() == legacy.parameter_values()


def test_legacy_blueprint_field_mirrored_for_back_compat():
    """The legacy `instance.blueprint` accessor must keep working with both
    input shapes — readers in CLI/expander still use it."""
    new = BlueprintInstance(**_new_form())
    legacy = BlueprintInstance(**_legacy_form())
    assert new.blueprint == "halo_bronze"
    assert legacy.blueprint == "halo_bronze"


def test_mutual_exclusion_raises_061():
    """Pydantic v2 wraps any ValueError raised in a model_validator inside
    its own ValidationError. The wrapped error preserves the LHP-VAL-061
    code/message for the user; the parser unwraps it for typed handling."""
    bad = {**_new_form(), "blueprint": "halo_bronze"}
    with pytest.raises(ValidationError) as exc:
        BlueprintInstance(**bad)
    msg = str(exc.value)
    assert "LHP-VAL-061" in msg
    assert "use_blueprint" in msg and "blueprint" in msg


def test_mixing_use_blueprint_with_flat_extras_raises_061():
    bad = {
        "use_blueprint": "halo_bronze",
        "parameters": {"variant": "acme"},
        "domain_id": "stray",  # a flat extra alongside the new form
    }
    with pytest.raises(ValidationError) as exc:
        BlueprintInstance(**bad)
    assert "LHP-VAL-061" in str(exc.value)


def test_legacy_form_emits_deprecation_warning_once_per_file(caplog):
    BlueprintInstance._legacy_warned_paths.clear()
    caplog.set_level(logging.WARNING, logger="lhp.models.config")

    BlueprintInstance.model_validate(
        _legacy_form(), context={"file_path": "/proj/instances/sg.yaml"}
    )
    BlueprintInstance.model_validate(
        _legacy_form(), context={"file_path": "/proj/instances/sg.yaml"}
    )
    BlueprintInstance.model_validate(
        _legacy_form(), context={"file_path": "/proj/instances/uk.yaml"}
    )

    deprecation_records = [
        r for r in caplog.records if "Deprecated blueprint instance syntax" in r.message
    ]
    assert len(deprecation_records) == 2
    files_warned = {r.args[0] for r in deprecation_records}
    assert files_warned == {"/proj/instances/sg.yaml", "/proj/instances/uk.yaml"}


def test_new_form_does_not_emit_deprecation_warning(caplog):
    BlueprintInstance._legacy_warned_paths.clear()
    caplog.set_level(logging.WARNING, logger="lhp.models.config")
    BlueprintInstance.model_validate(
        _new_form(), context={"file_path": "/proj/pipelines/halo/bronze/sg.yaml"}
    )
    assert not any(
        "Deprecated blueprint instance syntax" in r.message for r in caplog.records
    )


def test_overrides_field_accepted_in_new_form():
    payload = _new_form()
    payload["overrides"] = {"presets": ["bronze_alt"]}
    inst = BlueprintInstance(**payload)
    assert inst.overrides == {"presets": ["bronze_alt"]}


def test_unknown_top_level_keys_in_new_form_rejected():
    """`extra='forbid'` after normalization rejects truly unknown keys."""
    payload = _new_form()
    payload["something_random"] = "x"
    with pytest.raises(ValidationError) as exc:
        BlueprintInstance(**payload)
    # Mixing-forms detection fires for any non-allowed top-level key when
    # use_blueprint is present.
    assert "LHP-VAL-061" in str(exc.value)
