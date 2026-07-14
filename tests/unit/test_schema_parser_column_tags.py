"""Unit tests for SchemaParser column-tag extraction and validation."""

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
class TestToColumnTags:
    def setup_method(self):
        self.parser = SchemaParser()

    def test_extracts_tags_and_normalizes_values(self):
        schema = {
            "name": "s",
            "columns": [
                {"name": "id", "type": "BIGINT"},  # no tags key -> omitted
                {
                    "name": "email",
                    "type": "STRING",
                    "tags": {"classification": "pii", "masked": "", "n": 1, "x": None},
                },
            ],
        }
        result = self.parser.to_column_tags(schema)
        assert "id" not in result
        assert result["email"] == {
            "classification": "pii",
            "masked": "",
            "n": "1",
            "x": "",
        }

    def test_preserves_explicit_empty_tags(self):
        schema = {
            "name": "s",
            "columns": [{"name": "c", "type": "STRING", "tags": {}}],
        }
        assert self.parser.to_column_tags(schema) == {"c": {}}

    def test_no_columns_returns_empty(self):
        assert self.parser.to_column_tags({"name": "s"}) == {}

    def test_non_dict_tags_raise_clean_error(self):
        # A malformed `tags` (non-mapping) must raise a clean LHPError at
        # generation time, not an AttributeError from `.items()`.
        schema = {
            "name": "s",
            "columns": [{"name": "email", "type": "STRING", "tags": "pii"}],
        }
        with pytest.raises(LHPError) as exc_info:
            self.parser.to_column_tags(schema)
        message = str(exc_info.value)
        assert "email" in message
        assert "mapping" in message

    def test_tagged_column_without_name_raises_clean_error(self):
        # S-4: a column that carries `tags` but lacks `name` must raise a clean
        # LHPError, not a bare KeyError from `column["name"]`.
        schema = {
            "name": "s",
            "columns": [{"type": "STRING", "tags": {"classification": "pii"}}],
        }
        with pytest.raises(LHPError) as exc_info:
            self.parser.to_column_tags(schema)
        assert "name" in str(exc_info.value)


@pytest.mark.unit
class TestValidateSchemaColumnTags:
    def setup_method(self):
        self.parser = SchemaParser()

    def test_valid_tags_pass(self):
        schema = {
            "name": "s",
            "columns": [{"name": "c", "type": "STRING", "tags": {"a": "b"}}],
        }
        assert self.parser.validate_schema(schema) == []

    def test_non_dict_tags_rejected(self):
        schema = {
            "name": "s",
            "columns": [{"name": "c", "type": "STRING", "tags": "nope"}],
        }
        errors = self.parser.validate_schema(schema)
        assert any("tags" in e and "mapping" in e for e in errors)


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
