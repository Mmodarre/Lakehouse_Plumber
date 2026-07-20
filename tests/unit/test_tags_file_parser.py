"""Unit tests for the strict-format UC tags-file parser (LHP-CFG-067).

Covers the strict-format contract of ``parse_tags_file``: accepted shapes
(YAML + JSON, optional ``1.0`` / ``1.0.0`` versions, the ``name`` identifier
alias, empty tag maps) and every format rejection (LHP-CFG-067), plus the
pass-through of loader-level errors (LHP-IO-001 missing, LHP-IO-003 zero/multi
document) and the LHP-CFG-068 ``table``/``name`` conflict warning.
"""

import json
import logging
from pathlib import Path

import pytest

from lhp.errors import LHPError
from lhp.parsers.tags_file_parser import ParsedTagsFile, parse_tags_file


def _write(tmp_path: Path, name: str, content: str) -> Path:
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    return path


VALID_YAML = """version: "1.0"
table: catalog.schema.orders
tags:
  team: platform
  cost_center: "1234"
"""


# --- accepted shapes ---------------------------------------------------------


@pytest.mark.unit
def test_valid_yaml_returns_table_and_tags(tmp_path):
    path = _write(tmp_path, "tags.yaml", VALID_YAML)
    parsed = parse_tags_file(path)
    assert isinstance(parsed, ParsedTagsFile)
    assert parsed.table == "catalog.schema.orders"
    assert parsed.tags == {"team": "platform", "cost_center": "1234"}
    assert parsed.columns == {}


@pytest.mark.unit
def test_valid_json_returns_table_and_tags(tmp_path):
    content = json.dumps(
        {
            "version": "1.0",
            "table": "catalog.schema.orders",
            "tags": {"team": "platform", "cost_center": "1234"},
        }
    )
    path = _write(tmp_path, "tags.json", content)
    parsed = parse_tags_file(path)
    assert parsed.table == "catalog.schema.orders"
    assert parsed.tags == {"team": "platform", "cost_center": "1234"}
    assert parsed.columns == {}


@pytest.mark.unit
def test_version_1_0_0_accepted(tmp_path):
    path = _write(tmp_path, "tags.yaml", 'version: "1.0.0"\ntable: c.s.t\ntags: {}\n')
    parsed = parse_tags_file(path)
    assert parsed.table == "c.s.t"
    assert parsed.tags == {}


@pytest.mark.unit
def test_version_absent_parses(tmp_path):
    # ``version`` is optional (absent ⇒ 1.0); a version-less file parses.
    path = _write(tmp_path, "tags.yaml", "table: c.s.t\ntags: {}\n")
    parsed = parse_tags_file(path)
    assert parsed.table == "c.s.t"
    assert parsed.tags == {}


@pytest.mark.unit
def test_name_alias_accepted(tmp_path):
    # ``name`` is accepted as an alias for ``table`` (schema-file / ODCS
    # cross-compatibility); the resolved identifier lands on ``.table``.
    path = _write(tmp_path, "tags.yaml", "name: orders\ntags: {}\n")
    parsed = parse_tags_file(path)
    assert parsed.table == "orders"
    assert parsed.tags == {}


@pytest.mark.unit
def test_table_and_name_equal_accepted(tmp_path, caplog):
    # Both keys present with equal values: no conflict, no warning, ``table``.
    path = _write(tmp_path, "tags.yaml", "table: orders\nname: orders\ntags: {}\n")
    with caplog.at_level(logging.WARNING):
        parsed = parse_tags_file(path)
    assert parsed.table == "orders"
    assert "LHP-CFG-068" not in caplog.text


@pytest.mark.unit
def test_table_and_name_differing_warns_and_table_wins(tmp_path, caplog):
    # Both keys present with differing values: ``table`` wins and an
    # LHP-CFG-068 warning is logged (not raised).
    path = _write(tmp_path, "tags.yaml", "table: orders\nname: sales\ntags: {}\n")
    with caplog.at_level(logging.WARNING):
        parsed = parse_tags_file(path)
    assert parsed.table == "orders"
    assert "LHP-CFG-068" in caplog.text


@pytest.mark.unit
def test_empty_tags_mapping_allowed(tmp_path):
    path = _write(tmp_path, "tags.yaml", 'version: "1.0"\ntable: c.s.t\ntags: {}\n')
    parsed = parse_tags_file(path)
    assert parsed.table == "c.s.t"
    assert parsed.tags == {}
    assert parsed.columns == {}


@pytest.mark.unit
def test_table_is_stripped(tmp_path):
    path = _write(
        tmp_path, "tags.yaml", 'version: "1.0"\ntable: "  c.s.t  "\ntags: {}\n'
    )
    parsed = parse_tags_file(path)
    assert parsed.table == "c.s.t"


@pytest.mark.unit
def test_unquoted_float_version_1_0_accepted(tmp_path):
    # ``version: 1.0`` (unquoted) parses as the float 1.0; ``str(1.0) == "1.0"``
    # so the mandated ``str(version).strip()`` normalization accepts it.
    path = _write(tmp_path, "tags.yaml", "version: 1.0\ntable: c.s.t\ntags: {}\n")
    parsed = parse_tags_file(path)
    assert parsed.table == "c.s.t"
    assert parsed.tags == {}


# --- column tags (accepted shapes) ------------------------------------------


@pytest.mark.unit
def test_columns_only_returns_columns_and_none_tags(tmp_path):
    # A columns-only file: the ``tags:`` key is absent, so ``.tags is None``
    # (absent-≠-empty invariant), and ``.columns`` carries the block, built from
    # the ``columns:`` list of ``{name, tags}`` entries in entry order.
    content = (
        'version: "1.0"\n'
        "table: c.s.t\n"
        "columns:\n"
        "  - name: email\n"
        "    tags:\n"
        "      pii: high\n"
        "  - name: region\n"
        "    tags:\n"
        "      classification: public\n"
    )
    path = _write(tmp_path, "tags.yaml", content)
    parsed = parse_tags_file(path)
    assert parsed.table == "c.s.t"
    assert parsed.tags is None
    assert parsed.columns == {
        "email": {"pii": "high"},
        "region": {"classification": "public"},
    }


@pytest.mark.unit
def test_tags_and_columns_both_present(tmp_path):
    content = (
        'version: "1.0"\n'
        "table: c.s.t\n"
        "tags:\n"
        "  team: platform\n"
        "columns:\n"
        "  - name: email\n"
        "    tags:\n"
        "      pii: high\n"
    )
    path = _write(tmp_path, "tags.yaml", content)
    parsed = parse_tags_file(path)
    assert parsed.tags == {"team": "platform"}
    assert parsed.columns == {"email": {"pii": "high"}}


@pytest.mark.unit
def test_columns_json_parity(tmp_path):
    # A ``columns:`` file round-trips identically whether authored as YAML or
    # JSON (JSON is a YAML subset via the shared loader); the list of
    # ``{name, tags}`` objects yields the same column mapping.
    content = json.dumps(
        {
            "version": "1.0",
            "table": "c.s.t",
            "columns": [{"name": "email", "tags": {"pii": "high"}}],
        }
    )
    path = _write(tmp_path, "tags.json", content)
    parsed = parse_tags_file(path)
    assert parsed.table == "c.s.t"
    assert parsed.tags is None
    assert parsed.columns == {"email": {"pii": "high"}}


@pytest.mark.unit
def test_empty_columns_list_allowed(tmp_path):
    # An explicit empty ``columns: []`` counts as the key being present, so the
    # tags/columns presence requirement is satisfied without a ``tags`` key, and
    # yields an empty ``.columns`` mapping.
    path = _write(tmp_path, "tags.yaml", 'version: "1.0"\ntable: c.s.t\ncolumns: []\n')
    parsed = parse_tags_file(path)
    assert parsed.tags is None
    assert parsed.columns == {}


@pytest.mark.unit
def test_columns_entry_order_preserved(tmp_path):
    # ``.columns`` is built in ``columns:`` entry order.
    content = (
        'version: "1.0"\n'
        "table: c.s.t\n"
        "columns:\n"
        "  - name: gamma\n"
        "    tags:\n"
        "      a: '1'\n"
        "  - name: alpha\n"
        "    tags:\n"
        "      b: '2'\n"
        "  - name: beta\n"
        "    tags:\n"
        "      c: '3'\n"
    )
    path = _write(tmp_path, "tags.yaml", content)
    parsed = parse_tags_file(path)
    assert list(parsed.columns) == ["gamma", "alpha", "beta"]


@pytest.mark.unit
def test_column_name_stored_stripped(tmp_path):
    # ``name`` is stored stripped (mirrors ``table.strip()``).
    content = (
        'version: "1.0"\n'
        "table: c.s.t\n"
        "columns:\n"
        '  - name: "  email  "\n'
        "    tags:\n"
        "      pii: high\n"
    )
    path = _write(tmp_path, "tags.yaml", content)
    parsed = parse_tags_file(path)
    assert parsed.columns == {"email": {"pii": "high"}}


@pytest.mark.unit
def test_empty_column_tags_mapping_allowed(tmp_path):
    # An explicit empty ``tags: {}`` on a column entry is allowed (meaningful
    # under remove_undeclared_tags).
    content = 'version: "1.0"\ntable: c.s.t\ncolumns:\n  - name: email\n    tags: {}\n'
    path = _write(tmp_path, "tags.yaml", content)
    parsed = parse_tags_file(path)
    assert parsed.columns == {"email": {}}


# --- format rejections (LHP-CFG-067) -----------------------------------------


@pytest.mark.unit
def test_missing_identifier_rejected(tmp_path):
    # Neither ``table`` nor its alias ``name`` present — an identifier is
    # required (``version`` alone is optional and no longer required).
    path = _write(tmp_path, "tags.yaml", 'version: "1.0"\ntags:\n  a: b\n')
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"
    assert "table" in exc.value.details


@pytest.mark.unit
def test_neither_tags_nor_columns_rejected(tmp_path):
    # A ``table`` alone is not enough — at least one of the ``tags``/``columns``
    # keys must be declared.
    path = _write(tmp_path, "tags.yaml", 'version: "1.0"\ntable: c.s.t\n')
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"
    assert "columns" in exc.value.details


@pytest.mark.unit
def test_non_mapping_entry_rejected(tmp_path):
    # Each ``columns`` entry must be a mapping; a bare scalar list item fails.
    path = _write(
        tmp_path,
        "tags.yaml",
        'version: "1.0"\ntable: c.s.t\ncolumns:\n  - a\n',
    )
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_columns_mapping_form_rejected(tmp_path):
    # A mapping shape under ``columns:`` is rejected — it must be a list of
    # ``{name, tags}`` entries.
    path = _write(
        tmp_path,
        "tags.yaml",
        'version: "1.0"\ntable: c.s.t\ncolumns:\n  email:\n    pii: high\n',
    )
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"
    assert "must be a list" in exc.value.details


@pytest.mark.unit
def test_old_column_tags_key_rejected(tmp_path):
    # The former ``column_tags:`` key is no longer allowed and fails the
    # unknown-key check (clean break, no dual support — ``columns:`` is canonical).
    path = _write(
        tmp_path,
        "tags.yaml",
        'version: "1.0"\ntable: c.s.t\ncolumn_tags:\n'
        "  - name: email\n    tags:\n      pii: high\n",
    )
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"
    assert "column_tags" in exc.value.details


@pytest.mark.unit
def test_column_value_not_a_mapping_rejected(tmp_path):
    # A column entry whose ``tags`` is not a mapping is rejected.
    path = _write(
        tmp_path,
        "tags.yaml",
        'version: "1.0"\ntable: c.s.t\ncolumns:\n'
        "  - name: email\n    tags: not_a_mapping\n",
    )
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_non_string_column_key_rejected(tmp_path):
    # A non-string ``name`` (YAML ``name: 123``) would break the hook's repr/name
    # lookup downstream, so it is rejected here.
    path = _write(
        tmp_path,
        "tags.yaml",
        'version: "1.0"\ntable: c.s.t\ncolumns:\n'
        "  - name: 123\n    tags:\n      pii: high\n",
    )
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_missing_column_name_rejected(tmp_path):
    path = _write(
        tmp_path,
        "tags.yaml",
        'version: "1.0"\ntable: c.s.t\ncolumns:\n  - tags:\n      pii: high\n',
    )
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_whitespace_only_column_name_rejected(tmp_path):
    path = _write(
        tmp_path,
        "tags.yaml",
        'version: "1.0"\ntable: c.s.t\ncolumns:\n'
        '  - name: "   "\n    tags:\n      pii: high\n',
    )
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_missing_column_tags_key_rejected(tmp_path):
    # The per-column ``tags`` key is REQUIRED (managed-empty must be explicit).
    path = _write(
        tmp_path,
        "tags.yaml",
        'version: "1.0"\ntable: c.s.t\ncolumns:\n  - name: email\n',
    )
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_null_column_tags_rejected(tmp_path):
    # ``tags: ~`` (null) is not a mapping and is rejected.
    path = _write(
        tmp_path,
        "tags.yaml",
        'version: "1.0"\ntable: c.s.t\ncolumns:\n  - name: email\n    tags: ~\n',
    )
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_unknown_entry_key_rejected(tmp_path):
    # A ``columns`` entry allows only ``name`` and ``tags``.
    path = _write(
        tmp_path,
        "tags.yaml",
        'version: "1.0"\ntable: c.s.t\ncolumns:\n'
        "  - name: email\n    tags:\n      pii: high\n    extra: nope\n",
    )
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_duplicate_column_name_rejected(tmp_path):
    # Duplicate ``name`` (post-strip) would let last-wins silently drop tags.
    path = _write(
        tmp_path,
        "tags.yaml",
        'version: "1.0"\ntable: c.s.t\ncolumns:\n'
        "  - name: email\n    tags:\n      pii: high\n"
        "  - name: email\n    tags:\n      classification: internal\n",
    )
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_unsupported_version_string(tmp_path):
    path = _write(tmp_path, "tags.yaml", 'version: "2.0"\ntable: c.s.t\ntags: {}\n')
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_bare_int_version_rejected(tmp_path):
    # ``version: 1`` parses as the int 1; ``str(1) == "1"`` is not in
    # {"1.0", "1.0.0"} so it is rejected as an unsupported version.
    path = _write(tmp_path, "tags.yaml", "version: 1\ntable: c.s.t\ntags: {}\n")
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_unknown_key_rejected(tmp_path):
    path = _write(
        tmp_path,
        "tags.yaml",
        'version: "1.0"\ntable: c.s.t\ntags: {}\nextra: nope\n',
    )
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_tags_not_a_mapping_rejected(tmp_path):
    path = _write(
        tmp_path,
        "tags.yaml",
        'version: "1.0"\ntable: c.s.t\ntags:\n  - a\n  - b\n',
    )
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_table_not_a_string_rejected(tmp_path):
    path = _write(tmp_path, "tags.yaml", 'version: "1.0"\ntable: 123\ntags: {}\n')
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_empty_string_table_rejected(tmp_path):
    path = _write(tmp_path, "tags.yaml", 'version: "1.0"\ntable: "   "\ntags: {}\n')
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_name_not_a_string_rejected(tmp_path):
    # The ``name`` alias gets the same non-empty-string type check as ``table``.
    path = _write(tmp_path, "tags.yaml", "name: 123\ntags: {}\n")
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_list_root_rejected(tmp_path):
    path = _write(tmp_path, "tags.yaml", "- a\n- b\n")
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_null_document_raises_missing_key(tmp_path):
    # A single ``null`` document loads to ``{}`` (allow_empty) -> missing identifier.
    path = _write(tmp_path, "tags.yaml", "null\n")
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_dashes_only_document_raises_missing_key(tmp_path):
    # A lone ``---`` is a single null document -> {} -> missing identifier.
    path = _write(tmp_path, "tags.yaml", "---\n")
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


# --- loader-level pass-through errors -----------------------------------------


@pytest.mark.unit
def test_empty_file_raises_io_003(tmp_path):
    path = _write(tmp_path, "tags.yaml", "")
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-IO-003"


@pytest.mark.unit
def test_comments_only_file_raises_io_003(tmp_path):
    path = _write(tmp_path, "tags.yaml", "# only a comment\n")
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-IO-003"


@pytest.mark.unit
def test_missing_file_raises_io_001(tmp_path):
    path = tmp_path / "does_not_exist.yaml"
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-IO-001"
