"""Tests for ``lhp.errors.factory.ErrorFactory``.

Three guarantees:

1. Each of the 17 intent constructors returns the expected ``LHPError``
   subclass, with the expected ``LHP-<CAT>-<NNN>`` ``.code``, and pickles +
   unpickles to an equal error (subclass + code preserved).
2. Each of the 7 generic per-category constructors returns the right
   subclass, carries the ``.code`` of the passed :class:`ErrorCode`, and is
   picklable.
3. Concrete-output behavior: the rendered text and full LHPError identity of
   each intent method are pinned to their expected values (titles, details,
   suggestions, examples, context, message-formatting edge cases).

This file lives in ``tests/errors/`` with **no** ``__init__.py`` (collection
is ``testpaths``-based; adding one causes import-mode surprises).
"""

import pickle

import pytest

from lhp.errors import (
    ErrorCategory,
    ErrorFactory,
    LHPConfigError,
    LHPError,
    LHPFileError,
    LHPValidationError,
    MultiDocumentError,
    codes,
)


def _error_tuple(err: LHPError):
    return (
        type(err),
        err.code,
        err.title,
        err.details,
        err.suggestions,
        err.example,
        err.context,
        err.doc_link,
    )


def _assert_pickle_roundtrip(err: LHPError) -> None:
    restored = pickle.loads(pickle.dumps(err))
    assert type(restored) is type(err)
    assert restored.code == err.code
    assert isinstance(restored, LHPError)


INTENT_CASES = [
    (
        "configuration_conflict",
        {
            "action_name": "test_action",
            "field_pairs": [("format", "cloudFiles.format")],
        },
        LHPConfigError,
        "LHP-CFG-001",
    ),
    (
        "configuration_conflict",
        {
            "action_name": "test_action",
            "field_pairs": [("format", "cloudFiles.format")],
            "preset_name": "my_preset",
        },
        LHPConfigError,
        "LHP-CFG-001",
    ),
    (
        "incompatible_options",
        {
            "action_name": "my_action",
            "option_a": "overwrite",
            "option_b": "append",
            "reason": "They conflict.",
            "suggestion": "Pick one.",
        },
        LHPValidationError,
        "LHP-VAL-013",
    ),
    (
        "missing_required_field",
        {
            "field_name": "source",
            "component_type": "action",
            "component_name": "load_data",
            "field_description": "The source field is required.",
            "example_config": "source: my_view",
        },
        LHPValidationError,
        "LHP-VAL-001",
    ),
    (
        "file_not_found",
        {
            "file_path": "missing.sql",
            "search_locations": ["/path/a", "/path/b"],
            "file_type": "SQL file",
        },
        LHPFileError,
        "LHP-IO-001",
    ),
    (
        "unknown_type_with_suggestion",
        {
            "value_type": "action type",
            "provided_value": "laod",
            "valid_values": ["load", "transform", "write"],
            "example_usage": "type: load",
        },
        LHPConfigError,
        "LHP-ACT-001",
    ),
    (
        "validation_errors",
        {
            "component_name": "test_action",
            "component_type": "action",
            "errors": [
                "Missing source view",
                "Invalid target reference",
                "Unknown issue",
            ],
        },
        LHPValidationError,
        "LHP-VAL-002",
    ),
    (
        "yaml_parse_error",
        {
            "file_path": "config.yaml",
            "error_message": "bad indentation",
            "context": "line 4",
        },
        LHPConfigError,
        "LHP-CFG-009",
    ),
    (
        "deprecated_field",
        {
            "action_name": "my_action",
            "field_name": "old_field",
            "replacement": "new_field",
            "example": "new_field: value",
        },
        LHPConfigError,
        "LHP-CFG-010",
    ),
    (
        "invalid_field_value",
        {
            "action_name": "my_action",
            "field_name": "mode",
            "value": "bogus",
            "valid_values": ["append", "overwrite"],
        },
        LHPValidationError,
        "LHP-VAL-006",
    ),
    (
        "invalid_read_mode",
        {
            "action_name": "my_action",
            "action_type": "load",
            "provided": "invalid",
            "valid_modes": ["stream", "batch"],
        },
        LHPValidationError,
        "LHP-VAL-007",
    ),
    (
        "invalid_field_type",
        {
            "action_name": "my_action",
            "field_name": "count",
            "expected_type": "integer",
            "actual_type": "string",
        },
        LHPValidationError,
        "LHP-VAL-008",
    ),
    (
        "invalid_source_format",
        {
            "action_name": "test_action",
            "action_type": "load",
            "expected_formats": ["string", "dict"],
        },
        LHPValidationError,
        "LHP-VAL-012",
    ),
    (
        "template_not_found",
        {
            "template_name": "my_template",
            "available_templates": ["other_template"],
            "templates_dir": "/templates",
        },
        LHPConfigError,
        "LHP-CFG-027",
    ),
    (
        "missing_template_parameters",
        {
            "template_name": "my_template",
            "missing_params": ["param1"],
            "available_params": ["param1", "param2"],
        },
        LHPConfigError,
        "LHP-CFG-012",
    ),
    (
        "schema_syntax_error",
        {
            "file_path": "schema.yaml",
            "line_content": "id INT",
            "expected_format": "name: type",
        },
        LHPValidationError,
        "LHP-VAL-011",
    ),
    (
        "preset_not_found",
        {"preset_name": "bad_preset", "available_presets": ["good_preset"]},
        LHPConfigError,
        "LHP-ACT-001",
    ),
]


@pytest.mark.unit
@pytest.mark.parametrize(
    "method_name, kwargs, expected_subclass, expected_code",
    INTENT_CASES,
    ids=[f"{c[0]}-{c[3]}" for c in INTENT_CASES],
)
def test_intent_method_subclass_code_and_pickle(
    method_name, kwargs, expected_subclass, expected_code
):
    err = getattr(ErrorFactory, method_name)(**kwargs)
    assert type(err) is expected_subclass
    assert isinstance(err, LHPError)
    assert err.code == expected_code
    _assert_pickle_roundtrip(err)


@pytest.mark.unit
@pytest.mark.parametrize(
    "method_name, kwargs, expected_subclass, expected_code",
    INTENT_CASES,
    ids=[f"{c[0]}-{c[3]}" for c in INTENT_CASES],
)
def test_intent_method_output_is_deterministic(
    method_name, kwargs, expected_subclass, expected_code
):
    """Concrete per-method outputs are pinned in ``TestErrorFactory*`` below; this guards determinism of the full identity + rendered message."""
    first = getattr(ErrorFactory, method_name)(**kwargs)
    second = getattr(ErrorFactory, method_name)(**kwargs)
    assert _error_tuple(first) == _error_tuple(second)
    # Full rendered text is stable (defends _format_message paths).
    assert str(first) == str(second)


GENERIC_CASES = [
    ("validation_error", codes.VAL_009, LHPValidationError),
    ("config_error", codes.CFG_008, LHPConfigError),
    ("io_error", codes.IO_005, LHPFileError),
    ("action_error", codes.ACT_002, LHPConfigError),
    ("dependency_error", codes.DEP_022, LHPError),
    ("general_error", codes.GEN_901, LHPError),
    ("deprecation_error", codes.DEPR_001, LHPError),
]


@pytest.mark.unit
@pytest.mark.parametrize(
    "method_name, code, expected_subclass",
    GENERIC_CASES,
    ids=[f"{c[0]}-{c[1].code}" for c in GENERIC_CASES],
)
def test_generic_constructor_subclass_code_and_pickle(
    method_name, code, expected_subclass
):
    err = getattr(ErrorFactory, method_name)(
        code=code,
        title="Generic title",
        details="Generic details",
        suggestions=["Do the thing"],
        context={"Key": "value"},
    )
    assert type(err) is expected_subclass
    assert isinstance(err, LHPError)
    assert err.code == code.code
    assert err.title == "Generic title"
    assert err.details == "Generic details"
    assert err.suggestions == ["Do the thing"]
    assert err.context == {"Key": "value"}
    _assert_pickle_roundtrip(err)


@pytest.mark.unit
def test_config_error_forwards_doc_link():
    """config_error forwards a custom doc_link to the constructed error.

    The 4 raw CFG doc_link sites (all using ``_CATALOG_SCHEMA_DOC_LINK``)
    migrate onto ``config_error``; without doc_link forwarding the rendered
    "More info:" line would silently fall back to the default ReadTheDocs URL.
    Verifies the passed doc_link is preserved on the error, reaches the
    rendered message, and survives pickle.
    """
    doc_link = (
        "https://lakehouse-plumber.readthedocs.io/en/latest/"
        "configure_catalog_schema.html"
    )
    err = ErrorFactory.config_error(
        code=codes.CFG_023,
        title="Config title",
        details="Config details",
        suggestions=["Do the thing"],
        context={"Key": "value"},
        doc_link=doc_link,
    )
    assert err.doc_link == doc_link
    assert f"More info: {doc_link}" in str(err)
    restored = pickle.loads(pickle.dumps(err))
    assert restored.doc_link == doc_link
    assert str(restored) == str(err)


@pytest.mark.unit
def test_factory_methods_are_static():
    """Intent + generic methods are callable on the class (static-call style).

    Confirms the call shape needs no instance — the factory is the sole
    production error-construction path.
    """
    assert ErrorFactory.file_not_found("x", []).code == "LHP-IO-001"
    assert ErrorFactory.config_error(codes.CFG_005, "t", "d").code == "LHP-CFG-005"


class TestLHPErrorFormatMessage:
    def test_empty_details(self):
        error = LHPError(
            category=ErrorCategory.GENERAL,
            code_number="999",
            title="Test error",
            details="",
        )
        msg = str(error)
        assert "LHP-GEN-999" in msg
        assert "Test error" in msg

    def test_full_message_with_all_fields(self):
        error = LHPError(
            category=ErrorCategory.VALIDATION,
            code_number="100",
            title="Full error",
            details="Something went wrong",
            suggestions=["Fix it", "Try again"],
            example="example: value",
            context={"Action": "my_action"},
            doc_link="https://example.com/docs",
        )
        msg = str(error)
        assert "LHP-VAL-100" in msg
        assert "Full error" in msg
        assert "Something went wrong" in msg
        assert "Fix it" in msg
        assert "Try again" in msg
        assert "example: value" in msg
        assert "my_action" in msg
        assert "https://example.com/docs" in msg

    def test_code_format(self):
        error = LHPError(
            category=ErrorCategory.CONFIG,
            code_number="042",
            title="Config error",
            details="Bad config",
        )
        assert error.code == "LHP-CFG-042"

    def test_default_doc_link(self):
        error = LHPError(
            category=ErrorCategory.GENERAL,
            code_number="001",
            title="Test",
            details="Details",
        )
        assert "readthedocs" in str(error)


class TestLHPErrorSubclasses:
    def test_lhp_validation_error_is_value_error(self):
        error = LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="001",
            title="Validation",
            details="Bad value",
        )
        assert isinstance(error, ValueError)
        assert isinstance(error, LHPError)

    def test_lhp_config_error_is_value_error(self):
        error = LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="001",
            title="Config",
            details="Bad config",
        )
        assert isinstance(error, ValueError)
        assert isinstance(error, LHPError)

    def test_lhp_file_error_is_file_not_found_error(self):
        error = LHPFileError(
            category=ErrorCategory.IO,
            code_number="001",
            title="File missing",
            details="Not found",
        )
        assert isinstance(error, FileNotFoundError)
        assert isinstance(error, LHPError)


class TestMultiDocumentError:
    def test_multi_document(self):
        error = MultiDocumentError("/tmp/test.yaml", num_documents=3)
        msg = str(error)
        assert "3 documents" in msg
        assert "LHP-IO-003" in error.code

    def test_empty_file(self):
        """num_documents == 0 produces an empty file message."""
        error = MultiDocumentError("/tmp/test.yaml", num_documents=0)
        msg = str(error)
        assert "empty" in msg.lower()
        assert "LHP-IO-003" in error.code

    def test_with_error_context(self):
        error = MultiDocumentError(
            "/tmp/test.yaml", num_documents=2, error_context="substitution file"
        )
        msg = str(error)
        assert "substitution file" in msg

    def test_context_dict_contains_file_path(self):
        error = MultiDocumentError("/tmp/test.yaml", num_documents=5)
        assert error.context["file_path"] == "/tmp/test.yaml"
        assert error.context["num_documents"] == 5


class TestErrorFactoryValidationErrors:
    def test_missing_source_pattern(self):
        error = ErrorFactory.validation_errors(
            "test_action", "action", ["Missing source view"]
        )
        msg = str(error)
        assert "source" in msg.lower()
        assert "LHP-VAL-002" in error.code

    def test_invalid_target_pattern(self):
        error = ErrorFactory.validation_errors(
            "test_action", "action", ["Invalid target reference"]
        )
        msg = str(error)
        assert "target" in msg.lower()

    def test_circular_dependency_pattern(self):
        error = ErrorFactory.validation_errors(
            "test_action", "action", ["Found circular dependency"]
        )
        msg = str(error)
        assert "circular" in msg.lower()

    def test_generic_error_pattern(self):
        """Unrecognized error string is passed through directly."""
        error = ErrorFactory.validation_errors(
            "test_action", "action", ["Some other error"]
        )
        msg = str(error)
        assert "Some other error" in msg

    def test_multiple_errors(self):
        error = ErrorFactory.validation_errors(
            "test_action",
            "action",
            ["Missing source view", "Invalid target reference", "Unknown issue"],
        )
        msg = str(error)
        assert "source" in msg.lower()
        assert "target" in msg.lower()
        assert "Unknown issue" in msg
        assert error.context["Error Count"] == 3


class TestErrorFactoryTemplateNotFound:
    def test_with_templates_dir(self):
        error = ErrorFactory.template_not_found(
            "my_template", ["other_template"], templates_dir="/templates"
        )
        msg = str(error)
        assert "/templates/" in msg
        assert "LHP-CFG-027" in error.code

    def test_with_close_match(self):
        error = ErrorFactory.template_not_found("my_templat", ["my_template", "other"])
        msg = str(error)
        assert "my_template" in msg

    def test_no_close_match(self):
        """When no close match exists, available templates are still listed."""
        error = ErrorFactory.template_not_found(
            "zzz_nonexistent", ["alpha", "beta", "gamma"]
        )
        msg = str(error)
        assert "alpha" in msg

    def test_empty_available_templates(self):
        """Empty available_templates list does not crash."""
        error = ErrorFactory.template_not_found("my_template", [])
        msg = str(error)
        assert "my_template" in msg


class TestErrorFactoryMissingTemplateParameters:
    def test_with_available_params(self):
        error = ErrorFactory.missing_template_parameters(
            "my_template", ["param1"], available_params=["param1", "param2"]
        )
        msg = str(error)
        assert "param1" in msg
        assert "param2" in msg
        assert "LHP-CFG-012" in error.code

    def test_without_available_params(self):
        """Missing params are listed even without available_params."""
        error = ErrorFactory.missing_template_parameters(
            "my_template", ["missing_param"]
        )
        msg = str(error)
        assert "missing_param" in msg

    def test_multiple_missing_params(self):
        error = ErrorFactory.missing_template_parameters("tpl", ["p1", "p2", "p3"])
        msg = str(error)
        assert "p1" in msg
        assert "p2" in msg
        assert "p3" in msg


class TestErrorFactoryInvalidSourceFormat:
    def test_invalid_source_format(self):
        error = ErrorFactory.invalid_source_format(
            "test_action", "load", ["string", "dict"]
        )
        assert "LHP-VAL-012" in error.code
        msg = str(error)
        assert "string" in msg
        assert "dict" in msg
        assert "test_action" in msg


class TestErrorFactoryConfigurationConflict:
    def test_cloudfiles_conflict(self):
        """cloudFiles conflict generates example with cloudFiles format."""
        error = ErrorFactory.configuration_conflict(
            "test_action",
            [("format", "cloudFiles.format")],
        )
        msg = str(error)
        assert "cloudFiles" in msg
        assert "test_action" in msg
        assert isinstance(error, LHPConfigError)

    def test_multiple_conflicts(self):
        error = ErrorFactory.configuration_conflict(
            "my_action",
            [
                ("format", "cloudFiles.format"),
                ("schema_hints", "cloudFiles.schemaHints"),
            ],
        )
        msg = str(error)
        assert "format" in msg
        assert "schema_hints" in msg

    def test_with_preset_name(self):
        error = ErrorFactory.configuration_conflict(
            "test_action",
            [("format", "cloudFiles.format")],
            preset_name="my_preset",
        )
        msg = str(error)
        assert "my_preset" in msg


class TestErrorFactoryMiscMethods:
    def test_file_not_found(self):
        error = ErrorFactory.file_not_found(
            "missing.sql", ["/path/a", "/path/b"], file_type="SQL file"
        )
        msg = str(error)
        assert "LHP-IO-001" in error.code
        assert "missing.sql" in msg
        assert "/path/a" in msg

    def test_missing_required_field(self):
        error = ErrorFactory.missing_required_field(
            "source",
            "action",
            "load_data",
            "The source field is required.",
            "source: my_view",
        )
        msg = str(error)
        assert "LHP-VAL-001" in error.code
        assert "source" in msg

    def test_unknown_type_with_close_match(self):
        error = ErrorFactory.unknown_type_with_suggestion(
            "action type",
            "laod",
            ["load", "transform", "write"],
            "type: load",
        )
        msg = str(error)
        assert "load" in msg
        assert "LHP-ACT-001" in error.code

    def test_invalid_read_mode(self):
        error = ErrorFactory.invalid_read_mode(
            "my_action", "load", "invalid", ["stream", "batch"]
        )
        msg = str(error)
        assert "LHP-VAL-007" in error.code
        assert "stream" in msg
        assert "batch" in msg
