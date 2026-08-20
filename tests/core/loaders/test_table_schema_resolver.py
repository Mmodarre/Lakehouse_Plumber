"""Unit tests for the shared `table_schema` resolver.

`resolve_table_schema` turns a write target's ``table_schema`` value — inline DDL /
StructType, or a ``.yaml``/``.yml``/``.json``/``.ddl``/``.sql`` file path — into a
schema string (plus parsed data for YAML/JSON). It is shared by the streaming-table
and materialized-view generators and by ``CdcSchemaValidator``.
"""

import pytest

from lhp.core.loaders import ResolvedTableSchema, resolve_table_schema
from lhp.errors import LHPError

_SCHEMA_YAML = """\
name: customer_scd2_dim
columns:
  - name: customer_id
    type: BIGINT
  - name: __START_AT
    type: TIMESTAMP
  - name: __END_AT
    type: TIMESTAMP
"""


class TestResolveTableSchema:
    def test_inline_ddl_passthrough(self):
        """A non-file value is returned unchanged as text, with no schema_data."""
        resolved = resolve_table_schema(
            "id INT, __START_AT TIMESTAMP, __END_AT TIMESTAMP", project_root=None
        )
        assert isinstance(resolved, ResolvedTableSchema)
        assert resolved.text == "id INT, __START_AT TIMESTAMP, __END_AT TIMESTAMP"
        assert resolved.schema_data is None
        assert resolved.resolved_path is None

    def test_empty_value_returns_none_text(self):
        resolved = resolve_table_schema("", project_root=None)
        assert resolved.text is None

    def test_yaml_file_parsed_to_hints(self, tmp_path):
        """A YAML file is parsed to schema hints; schema_data + resolved_path are populated."""
        schemas = tmp_path / "schemas"
        schemas.mkdir()
        (schemas / "customer_scd2_dim.yaml").write_text(_SCHEMA_YAML)

        resolved = resolve_table_schema(
            "schemas/customer_scd2_dim.yaml", project_root=tmp_path
        )
        assert "__START_AT" in resolved.text
        assert "__END_AT" in resolved.text
        assert "customer_id" in resolved.text
        assert resolved.schema_data is not None
        assert {c["name"] for c in resolved.schema_data["columns"]} == {
            "customer_id",
            "__START_AT",
            "__END_AT",
        }
        assert resolved.resolved_path == (schemas / "customer_scd2_dim.yaml")

    def test_ddl_file_returns_stripped_text(self, tmp_path):
        """A .sql/.ddl file returns its text (stripped), with no schema_data."""
        (tmp_path / "customer.sql").write_text(
            "  id INT, __START_AT TIMESTAMP, __END_AT TIMESTAMP\n"
        )
        resolved = resolve_table_schema("customer.sql", project_root=tmp_path)
        assert resolved.text == "id INT, __START_AT TIMESTAMP, __END_AT TIMESTAMP"
        assert resolved.schema_data is None

    def test_file_path_without_project_root_is_unresolvable(self):
        """A file path but no project_root yields text=None (caller decides)."""
        resolved = resolve_table_schema(
            "schemas/customer_scd2_dim.yaml", project_root=None
        )
        assert resolved.text is None

    def test_missing_file_raises(self, tmp_path):
        """A referenced-but-missing file raises LHPError (generator surfaces it)."""
        with pytest.raises(LHPError):
            resolve_table_schema("schemas/nope.yaml", project_root=tmp_path)
