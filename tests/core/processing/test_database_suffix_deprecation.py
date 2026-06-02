"""The deprecated preset ``database_suffix`` key must emit a DEPR-004 warning.

# REMOVE_AT_V1.0.0: Delete with the database_suffix support it guards.

Previously the legacy ``database_suffix`` path in
:meth:`FlowgroupResolutionService._apply_suffix` was applied SILENTLY. It now
records a structured :class:`DeprecationWarningRecord` (``LHP-DEPR-004``) onto
the active worker scope (workers run under a NullHandler, so a log warning would
be lost). These tests open a ``collect_deprecations`` scope — as the worker
wrapper does around each flowgroup — and assert on the drained records.
"""

from pathlib import Path

import pytest

from lhp.core.processing.flowgroup_resolver import FlowgroupResolutionService
from lhp.errors import codes
from lhp.models.deprecations import collect_deprecations, drain_deprecations

_apply_suffix = FlowgroupResolutionService._apply_suffix


@pytest.mark.unit
class TestDatabaseSuffixDeprecation:
    """``database_suffix`` is deprecated → DEPR-004; ``schema_suffix`` is silent."""

    def test_database_suffix_legacy_target_emits_depr_004(self):
        """database_suffix applied to a legacy ``database`` target → DEPR-004."""
        target = {"database": "cat.sch"}
        with collect_deprecations(
            file=Path("flowgroups/fg.yaml"), flowgroup="fg_s"
        ) as collector:
            _apply_suffix(target, {"database_suffix": "_dev"})
        # The suffix is still applied (back-compat behavior preserved).
        assert target["database"] == "cat.sch_dev"
        records = drain_deprecations(collector)
        assert len(records) == 1
        (record,) = records
        assert record.code == codes.DEPR_004.code
        assert record.file == Path("flowgroups/fg.yaml")
        assert record.flowgroup == "fg_s"
        assert "database_suffix" in record.message

    def test_database_suffix_on_new_schema_target_still_warns(self):
        """database_suffix is deprecated regardless of the target field shape."""
        target = {"schema": "sch"}
        with collect_deprecations(file=None, flowgroup="fg_s") as collector:
            _apply_suffix(target, {"database_suffix": "_dev"})
        assert target["schema"] == "sch_dev"
        records = drain_deprecations(collector)
        assert len(records) == 1
        assert records[0].code == codes.DEPR_004.code

    def test_schema_suffix_emits_no_warning(self):
        """The supported ``schema_suffix`` key emits nothing."""
        target = {"schema": "sch"}
        with collect_deprecations(file=None, flowgroup="fg_s") as collector:
            _apply_suffix(target, {"schema_suffix": "_dev"})
        assert target["schema"] == "sch_dev"
        assert drain_deprecations(collector) == ()

    def test_schema_suffix_wins_over_database_suffix_no_warning(self):
        """When both keys are present, ``schema_suffix`` is used → no deprecation."""
        target = {"schema": "sch"}
        with collect_deprecations(file=None, flowgroup="fg_s") as collector:
            _apply_suffix(target, {"schema_suffix": "_prod", "database_suffix": "_dev"})
        # schema_suffix takes precedence, so the deprecated key is not the source.
        assert target["schema"] == "sch_prod"
        assert drain_deprecations(collector) == ()

    def test_no_suffix_emits_no_warning(self):
        """No suffix key at all → no deprecation, no mutation."""
        target = {"schema": "sch"}
        with collect_deprecations(file=None, flowgroup="fg_s") as collector:
            _apply_suffix(target, {})
        assert target == {"schema": "sch"}
        assert drain_deprecations(collector) == ()
