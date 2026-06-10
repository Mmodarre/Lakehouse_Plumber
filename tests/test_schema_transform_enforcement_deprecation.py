"""The schema-transform ``enforcement`` key must emit a DEPR-003 warning.

# REMOVE_AT_V1.0.0: Delete with the enforcement-in-schema deprecation it guards.

``enforcement`` belongs at the action level. Specifying it inside an inline
``schema_inline`` (where it is ignored) or an external schema file (where it is
deprecated and ignored) previously emitted a swallowed ``logger.warning`` —
workers run under a NullHandler, so it never reached the user. It now records a
structured :class:`DeprecationWarningRecord` (``LHP-DEPR-003``) onto the active
worker scope. These tests open a ``collect_deprecations`` scope — as the worker
wrapper does around each flowgroup — and assert on the drained records.
"""

from pathlib import Path

import pytest

from lhp.errors import codes
from lhp.models.deprecations import collect_deprecations, drain_deprecations
from lhp.parsers.schema_transform_parser import SchemaTransformParser


@pytest.mark.unit
class TestSchemaTransformEnforcementDeprecation:
    """The ``enforcement`` key in inline / external schema → DEPR-003."""

    def test_inline_enforcement_emits_depr_003(self):
        """``enforcement`` in a full-YAML inline schema records DEPR-003."""
        parser = SchemaTransformParser()
        # Full YAML format with a columns key triggers the file-data path that
        # warns; enforcement here is ignored (action-level only). The arrow
        # column is quoted so YAML keeps it a string (the trailing ": TYPE"
        # would otherwise parse as a nested mapping).
        schema_inline = 'enforcement: strict\ncolumns:\n  - "old -> new: BIGINT"\n'
        with collect_deprecations(
            file=Path("flowgroups/fg.yaml"), flowgroup="fg_t"
        ) as collector:
            parser.parse_inline_schema(schema_inline)
        records = drain_deprecations(collector)
        assert len(records) == 1
        (record,) = records
        assert record.code == codes.DEPR_003.code
        assert record.file == Path("flowgroups/fg.yaml")
        assert record.flowgroup == "fg_t"
        assert "enforcement" in record.message

    def test_external_file_enforcement_emits_depr_003(self):
        """``enforcement`` in external schema file data records DEPR-003."""
        parser = SchemaTransformParser()
        data = {
            "enforcement": "permissive",
            "columns": ["old -> new: BIGINT"],
        }
        with collect_deprecations(
            file=Path("flowgroups/fg.yaml"), flowgroup="fg_t"
        ) as collector:
            parser.parse_file_data(data)
        records = drain_deprecations(collector)
        assert len(records) == 1
        assert records[0].code == codes.DEPR_003.code
        assert records[0].flowgroup == "fg_t"

    def test_no_enforcement_key_emits_no_warning(self):
        """A schema without ``enforcement`` emits nothing."""
        parser = SchemaTransformParser()
        with collect_deprecations(file=None, flowgroup="fg_t") as collector:
            parser.parse_file_data({"columns": ["old -> new: BIGINT"]})
        assert drain_deprecations(collector) == ()
