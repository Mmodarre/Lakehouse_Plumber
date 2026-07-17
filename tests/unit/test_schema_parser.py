"""Unit tests for SchemaParser schema-hints (DDL) generation and the
column-tags migration guard."""

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from lhp.errors import LHPError
from lhp.parsers.schema_parser import SchemaParser


def _assert_well_formed_databricks_ddl(ddl: str, expected_columns: list[str]) -> None:
    """Assert that a ``to_schema_hints`` fragment is well-formed Databricks SQL.

    The fragment is embedded in a ``CREATE TABLE`` statement and parsed with the
    ``databricks`` dialect. A parse failure — or column identifiers that do not
    round-trip to ``expected_columns`` — means the DDL is malformed (e.g. an
    unquoted identifier containing whitespace or ``$``).
    """
    statement = f"CREATE TABLE t ({ddl})"
    try:
        parsed = sqlglot.parse_one(statement, read="databricks")
    except SqlglotError as exc:
        raise AssertionError(
            f"schema hints are not well-formed Databricks SQL DDL: {ddl!r} ({exc})"
        ) from exc

    names = [col.find(exp.Identifier).name for col in parsed.find_all(exp.ColumnDef)]
    assert names == expected_columns, (
        f"parsed column identifiers {names} do not round-trip to "
        f"{expected_columns} for DDL {ddl!r}"
    )


@pytest.mark.unit
class TestToSchemaHints:
    def setup_method(self):
        self.parser = SchemaParser()

    def test_valid_column_produces_hint(self):
        schema = {
            "name": "s",
            "columns": [{"name": "id", "type": "BIGINT", "nullable": False}],
        }
        assert self.parser.to_schema_hints(schema) == "id BIGINT NOT NULL"

    def test_column_without_name_raises_clean_error(self):
        # E-4: a column missing `name` must raise a clean LHPError, not a bare
        # KeyError from `column["name"]`.
        schema = {
            "name": "s",
            "columns": [{"type": "STRING"}],
        }
        with pytest.raises(LHPError) as exc_info:
            self.parser.to_schema_hints(schema)
        assert "name" in str(exc_info.value)

    def test_column_without_type_raises_clean_error(self):
        # E-4: a column missing `type` must raise a clean LHPError, not a bare
        # KeyError from `column["type"]`.
        schema = {
            "name": "s",
            "columns": [{"name": "id"}],
        }
        with pytest.raises(LHPError) as exc_info:
            self.parser.to_schema_hints(schema)
        assert "type" in str(exc_info.value)

    def test_plain_columns_produce_well_formed_ddl(self):
        # Positive control: ordinary identifiers need no quoting and must parse
        # as well-formed Databricks SQL DDL. Proves the sqlglot check is sound.
        schema = {
            "name": "s",
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {"name": "name", "type": "STRING"},
            ],
        }
        ddl = self.parser.to_schema_hints(schema)
        _assert_well_formed_databricks_ddl(ddl, ["id", "name"])

    def test_whitespace_column_name_produces_well_formed_ddl(self):
        # A column name containing whitespace must be emitted as a quoted
        # identifier so the schema hints remain valid Databricks SQL DDL.
        schema = {
            "name": "s",
            "columns": [{"name": "my column", "type": "STRING"}],
        }
        ddl = self.parser.to_schema_hints(schema)
        _assert_well_formed_databricks_ddl(ddl, ["my column"])

    def test_dollar_column_name_produces_well_formed_ddl(self):
        # A column name containing `$` must be emitted as a quoted identifier so
        # the schema hints remain valid Databricks SQL DDL.
        schema = {
            "name": "s",
            "columns": [{"name": "amount$", "type": "STRING"}],
        }
        ddl = self.parser.to_schema_hints(schema)
        _assert_well_formed_databricks_ddl(ddl, ["amount$"])

    def test_whitespace_and_dollar_column_name_produces_well_formed_ddl(self):
        # Exercises whitespace and `$` in a single identifier.
        schema = {
            "name": "s",
            "columns": [{"name": "gross $ amount", "type": "DECIMAL(18,2)"}],
        }
        ddl = self.parser.to_schema_hints(schema)
        _assert_well_formed_databricks_ddl(ddl, ["gross $ amount"])


@pytest.mark.unit
class TestColumnTagsMigrationGuard:
    def setup_method(self):
        self.parser = SchemaParser()

    def test_column_tags_in_schema_file_raise_val_016(self, tmp_path):
        # Column tags are no longer a schema-file concern (they live in the
        # write target's tags_file). A stray column `tags:` key must be a clear
        # hard error at parse time, never a silent drop.
        schema_file = tmp_path / "orders.yaml"
        schema_file.write_text(
            "name: orders\n"
            "columns:\n"
            "  - name: email\n"
            "    type: STRING\n"
            "    tags:\n"
            "      classification: pii\n"
        )
        with pytest.raises(LHPError) as exc_info:
            self.parser.parse_schema_file(schema_file)
        assert exc_info.value.code == "LHP-VAL-016"

    def test_schema_file_without_column_tags_parses_cleanly(self, tmp_path):
        # A schema file with no column `tags:` key must parse without error.
        schema_file = tmp_path / "orders.yaml"
        schema_file.write_text(
            "name: orders\n"
            "columns:\n"
            "  - name: id\n"
            "    type: BIGINT\n"
            "  - name: email\n"
            "    type: STRING\n"
        )
        schema_data = self.parser.parse_schema_file(schema_file)
        assert schema_data["name"] == "orders"
