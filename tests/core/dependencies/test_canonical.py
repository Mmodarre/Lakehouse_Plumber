"""Unit tests for the package-private canonicalization seam.

Covers :func:`canonicalize_table_ref` (case-insensitivity, backtick stripping,
whitespace stripping, idempotency) and :func:`split_canonical_parts` (2-part and
3-part splitting). This module is the pure string layer beneath the 2-part vs
3-part reconciliation rule, so the tests assert only canonicalization
behavior — not matching/lookup.
"""

from __future__ import annotations

import pytest

from lhp.core.dependencies._canonical import (
    canonicalize_table_ref,
    split_canonical_parts,
)


@pytest.mark.unit
class TestCanonicalizeTableRef:
    def test_lowercases(self):
        assert (
            canonicalize_table_ref("MyCat.MySchema.MyTable") == "mycat.myschema.mytable"
        )

    def test_strips_surrounding_and_embedded_backticks(self):
        assert canonicalize_table_ref("`cat`.`schema`.`table`") == "cat.schema.table"

    def test_strips_whitespace_around_each_part(self):
        assert canonicalize_table_ref("  cat . schema . table  ") == "cat.schema.table"

    def test_combined_messy_reference(self):
        assert (
            canonicalize_table_ref("  `MyCat`.`MySchema`.`MyTable` ")
            == "mycat.myschema.mytable"
        )

    def test_two_part_reference(self):
        assert canonicalize_table_ref("MySchema.MyTable") == "myschema.mytable"

    def test_single_part_reference(self):
        assert canonicalize_table_ref("`MyTable`") == "mytable"

    def test_idempotent(self):
        messy = "  `MyCat`.`MySchema`.`MyTable` "
        once = canonicalize_table_ref(messy)
        twice = canonicalize_table_ref(once)
        assert once == twice == "mycat.myschema.mytable"


@pytest.mark.unit
class TestSplitCanonicalParts:
    def test_three_part_reference(self):
        assert split_canonical_parts("`MyCat`.`MySchema`.`MyTable`") == [
            "mycat",
            "myschema",
            "mytable",
        ]

    def test_two_part_reference(self):
        assert split_canonical_parts("  MySchema . MyTable ") == ["myschema", "mytable"]

    def test_single_part_reference(self):
        assert split_canonical_parts("MyTable") == ["mytable"]

    def test_parts_rejoin_to_canonical_form(self):
        ref = "  `MyCat`.`MySchema`.`MyTable` "
        assert ".".join(split_canonical_parts(ref)) == canonicalize_table_ref(ref)
