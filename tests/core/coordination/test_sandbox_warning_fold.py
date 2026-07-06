"""Tests for per-file folding of sandbox rewriter warnings.

:func:`fold_file_warnings` splits the rewriter's mixed per-site warning stream
for ONE file into transport records: :class:`UnrewritableTableRead` ->
``LHP-VAL-066``, :class:`UnverifiableSqlRead` -> ``LHP-VAL-067``, at most one of
each per file. The two codes are distinct ``(code, file)`` keys, so both survive
the downstream merge dedup.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lhp.core.coordination._sandbox_warning_fold import fold_file_warnings
from lhp.core.coordination._warning_merge import _dedup_by_code_file
from lhp.core.sandbox import UnrewritableTableRead, UnverifiableSqlRead

_FILE = Path("consumer_fg.py")


@pytest.mark.unit
class TestFoldFileWarnings:
    def test_empty_folds_nothing(self):
        assert fold_file_warnings((), _FILE, "fg") == []

    def test_unrewritable_only_folds_one_val066(self):
        reads = (
            UnrewritableTableRead(
                lineno=3, table="dev.silver.orders", kind="table_read"
            ),
            UnrewritableTableRead(lineno=7, table="stg.events", kind="spark_sql"),
        )

        records = fold_file_warnings(reads, _FILE, "fg")

        assert len(records) == 1
        record = records[0]
        assert record.code == "LHP-VAL-066"
        assert record.file == _FILE
        assert record.flowgroup == "fg"
        assert "dev.silver.orders" in record.message
        assert "stg.events" in record.message

    def test_unverifiable_only_folds_one_val067(self):
        reads = (UnverifiableSqlRead(lineno=2), UnverifiableSqlRead(lineno=9))

        records = fold_file_warnings(reads, _FILE, "fg")

        assert len(records) == 1
        record = records[0]
        assert record.code == "LHP-VAL-067"
        assert record.file == _FILE
        assert record.flowgroup == "fg"
        assert "2" in record.message and "9" in record.message
        assert "runtime" in record.message

    def test_val067_message_dedups_and_sorts_lines(self):
        reads = (
            UnverifiableSqlRead(lineno=9),
            UnverifiableSqlRead(lineno=2),
            UnverifiableSqlRead(lineno=9),
        )

        record = fold_file_warnings(reads, _FILE, "fg")[0]

        assert "(line(s) 2, 9)" in record.message
        assert "2 dynamic SQL statement(s)" in record.message

    def test_mixed_folds_both_codes(self):
        reads = (
            UnrewritableTableRead(
                lineno=3, table="dev.silver.orders", kind="table_read"
            ),
            UnverifiableSqlRead(lineno=5),
        )

        records = fold_file_warnings(reads, _FILE, "fg")

        assert {r.code for r in records} == {"LHP-VAL-066", "LHP-VAL-067"}
        assert all(r.file == _FILE for r in records)

    def test_both_codes_survive_merge_dedup(self):
        # VAL-066 and VAL-067 for the SAME file are distinct (code, file) keys,
        # so the merge chain keeps both (dedup is by code AND file).
        records = fold_file_warnings(
            (
                UnrewritableTableRead(
                    lineno=3, table="dev.silver.orders", kind="table_read"
                ),
                UnverifiableSqlRead(lineno=5),
            ),
            _FILE,
            "fg",
        )

        deduped = _dedup_by_code_file(records)

        assert {r.code for r in deduped} == {"LHP-VAL-066", "LHP-VAL-067"}
