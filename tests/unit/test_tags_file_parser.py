"""Unit tests for the tags reader over the unified schema/tags format.

Covers the ``tags_file`` contract of ``parse_tags_file``: accepted shapes
(YAML + JSON, the optional ``table``/``name`` identifier, schema-style files
whose schema-only ``type``/``nullable``/``comment`` fields are ignored,
empty/absent tag maps), the no-op path (a file with no tags), the whitelist
rejections (LHP-CFG-067), the pass-through of loader-level errors (LHP-IO-001
missing, LHP-IO-003 zero/multi document), and the LHP-CFG-068 ``table``/``name``
conflict warning.
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


@pytest.mark.unit
def test_schema_style_file_parses_as_tags(tmp_path):
    # A unified schema/tags file whose columns carry schema-only fields
    # (type/nullable/comment) alongside tags parses as tags: the schema-only
    # fields are ignored, and only tagged columns land in ``.columns``.
    content = (
        "table: c.s.t\n"
        "description: a schema file\n"
        "columns:\n"
        "  - name: id\n"
        "    type: BIGINT\n"
        "    nullable: false\n"
        "  - name: email\n"
        "    type: STRING\n"
        "    comment: PII\n"
        "    tags:\n"
        "      pii: high\n"
    )
    path = _write(tmp_path, "orders.yaml", content)
    parsed = parse_tags_file(path)
    assert parsed.table == "c.s.t"
    assert parsed.tags is None
    # ``id`` has no tags -> contributes nothing; only ``email`` is tagged.
    assert parsed.columns == {"email": {"pii": "high"}}


@pytest.mark.unit
def test_legacy_keys_tolerated(tmp_path):
    # The legacy schema keys version/description/primary_key are tolerated and
    # ignored, so a real schema file works unchanged as a tags_file.
    content = (
        'version: "1.0"\n'
        "description: legacy\n"
        "primary_key: [id]\n"
        "table: c.s.t\n"
        "tags:\n"
        "  team: platform\n"
    )
    path = _write(tmp_path, "tags.yaml", content)
    parsed = parse_tags_file(path)
    assert parsed.table == "c.s.t"
    assert parsed.tags == {"team": "platform"}


# --- format rejections (LHP-CFG-067) -----------------------------------------


@pytest.mark.unit
def test_missing_identifier_is_optional(tmp_path):
    # The identifier is optional: a file with tags but no ``table``/``name``
    # parses, and ``.table`` is None (so the CFG-068 cross-check is skipped).
    path = _write(tmp_path, "tags.yaml", "tags:\n  a: b\n")
    parsed = parse_tags_file(path)
    assert parsed.table is None
    assert parsed.tags == {"a": "b"}


@pytest.mark.unit
def test_no_tags_and_no_columns_is_noop(tmp_path):
    # A file with an identifier but no tags at all is a no-op, not an error.
    path = _write(tmp_path, "tags.yaml", "table: c.s.t\n")
    parsed = parse_tags_file(path)
    assert parsed.table == "c.s.t"
    assert parsed.tags is None
    assert parsed.columns == {}


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
def test_column_without_tags_contributes_none(tmp_path):
    # A column with no ``tags`` key contributes no column tags (schema-only
    # column) — tolerated, not rejected, so one file can serve both readers.
    path = _write(
        tmp_path,
        "tags.yaml",
        "table: c.s.t\ncolumns:\n  - name: email\n",
    )
    parsed = parse_tags_file(path)
    assert parsed.columns == {}


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
def test_legacy_version_tolerated_and_ignored(tmp_path):
    # ``version`` is now a tolerated-and-ignored legacy key; any value parses.
    path = _write(tmp_path, "tags.yaml", 'version: "2.0"\ntable: c.s.t\ntags: {}\n')
    parsed = parse_tags_file(path)
    assert parsed.table == "c.s.t"
    assert parsed.tags == {}


@pytest.mark.unit
def test_bare_int_version_tolerated(tmp_path):
    # A bare-int ``version: 1`` is tolerated-and-ignored (legacy key).
    path = _write(tmp_path, "tags.yaml", "version: 1\ntable: c.s.t\ntags: {}\n")
    parsed = parse_tags_file(path)
    assert parsed.table == "c.s.t"


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
def test_null_document_is_noop(tmp_path):
    # A single ``null`` document loads to ``{}`` (allow_empty); with the
    # identifier optional and no-tags allowed, that is a no-op, not an error.
    path = _write(tmp_path, "tags.yaml", "null\n")
    parsed = parse_tags_file(path)
    assert parsed.table is None
    assert parsed.tags is None
    assert parsed.columns == {}


@pytest.mark.unit
def test_dashes_only_document_is_noop(tmp_path):
    # A lone ``---`` is a single null document -> {} -> a no-op.
    path = _write(tmp_path, "tags.yaml", "---\n")
    parsed = parse_tags_file(path)
    assert parsed.table is None
    assert parsed.columns == {}


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
