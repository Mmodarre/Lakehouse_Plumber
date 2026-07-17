"""Unit tests for the strict-format UC tags-file parser (LHP-CFG-067).

Covers the strict-format contract of ``parse_tags_file``: accepted shapes
(YAML + JSON, ``1.0`` / ``1.0.0`` versions, empty tag maps) and every format
rejection (LHP-CFG-067), plus the pass-through of loader-level errors
(LHP-IO-001 missing, LHP-IO-003 zero/multi document).
"""

import json
from pathlib import Path

import pytest

from lhp.errors import LHPError
from lhp.parsers.tags_file_parser import parse_tags_file


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
    table, tags = parse_tags_file(path)
    assert table == "catalog.schema.orders"
    assert tags == {"team": "platform", "cost_center": "1234"}


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
    table, tags = parse_tags_file(path)
    assert table == "catalog.schema.orders"
    assert tags == {"team": "platform", "cost_center": "1234"}


@pytest.mark.unit
def test_version_1_0_0_accepted(tmp_path):
    path = _write(tmp_path, "tags.yaml", 'version: "1.0.0"\ntable: c.s.t\ntags: {}\n')
    table, tags = parse_tags_file(path)
    assert table == "c.s.t"
    assert tags == {}


@pytest.mark.unit
def test_empty_tags_mapping_allowed(tmp_path):
    path = _write(tmp_path, "tags.yaml", 'version: "1.0"\ntable: c.s.t\ntags: {}\n')
    table, tags = parse_tags_file(path)
    assert table == "c.s.t"
    assert tags == {}


@pytest.mark.unit
def test_table_is_stripped(tmp_path):
    path = _write(
        tmp_path, "tags.yaml", 'version: "1.0"\ntable: "  c.s.t  "\ntags: {}\n'
    )
    table, _ = parse_tags_file(path)
    assert table == "c.s.t"


@pytest.mark.unit
def test_unquoted_float_version_1_0_accepted(tmp_path):
    # ``version: 1.0`` (unquoted) parses as the float 1.0; ``str(1.0) == "1.0"``
    # so the mandated ``str(version).strip()`` normalization accepts it.
    path = _write(tmp_path, "tags.yaml", "version: 1.0\ntable: c.s.t\ntags: {}\n")
    table, tags = parse_tags_file(path)
    assert table == "c.s.t"
    assert tags == {}


# --- format rejections (LHP-CFG-067) -----------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize("missing", ["version", "table", "tags"])
def test_missing_required_key(tmp_path, missing):
    data = {"version": "1.0", "table": "c.s.t", "tags": {"a": "b"}}
    del data[missing]
    path = _write(tmp_path, "tags.yaml", json.dumps(data))
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"
    assert missing in exc.value.details


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
def test_list_root_rejected(tmp_path):
    path = _write(tmp_path, "tags.yaml", "- a\n- b\n")
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_null_document_raises_missing_key(tmp_path):
    # A single ``null`` document loads to ``{}`` (allow_empty) -> missing key.
    path = _write(tmp_path, "tags.yaml", "null\n")
    with pytest.raises(LHPError) as exc:
        parse_tags_file(path)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_dashes_only_document_raises_missing_key(tmp_path):
    # A lone ``---`` is a single null document -> {} -> missing key.
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
