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
class TestUnifiedSchemaFormat:
    """The schema reader over the unified schema/tags format: table/column tags
    are tolerated and ignored by the schema view, the strict whitelist rejects
    typos, legacy keys are tolerated, and the top-level identifier is optional.
    """

    def setup_method(self):
        self.parser = SchemaParser()

    def test_column_and_table_tags_are_ignored_by_schema_view(self, tmp_path):
        # A unified file may carry table- and column-level tags; the schema
        # reader tolerates them and ignores them when building schema hints.
        schema_file = tmp_path / "orders.yaml"
        schema_file.write_text(
            "name: orders\n"
            "tags:\n"
            "  team: platform\n"
            "columns:\n"
            "  - name: id\n"
            "    type: BIGINT\n"
            "    nullable: false\n"
            "  - name: email\n"
            "    type: STRING\n"
            "    tags:\n"
            "      classification: pii\n"
        )
        schema_data = self.parser.parse_schema_file(schema_file)
        hints = self.parser.to_schema_hints(schema_data)
        assert hints == "id BIGINT NOT NULL, email STRING"

    def test_unknown_top_level_key_rejected(self, tmp_path):
        # A typo'd top-level key fails the unified whitelist (LHP-CFG-067).
        schema_file = tmp_path / "orders.yaml"
        schema_file.write_text("name: orders\ncolumsn:\n  - name: id\n")
        with pytest.raises(LHPError) as exc_info:
            self.parser.parse_schema_file(schema_file)
        assert exc_info.value.code == "LHP-CFG-067"

    def test_unknown_column_key_rejected(self, tmp_path):
        # A typo'd per-column key (``typ:`` for ``type:``) fails the whitelist.
        schema_file = tmp_path / "orders.yaml"
        schema_file.write_text(
            "name: orders\ncolumns:\n  - name: id\n    typ: BIGINT\n"
        )
        with pytest.raises(LHPError) as exc_info:
            self.parser.parse_schema_file(schema_file)
        assert exc_info.value.code == "LHP-CFG-067"

    def test_legacy_keys_tolerated(self, tmp_path):
        # version/description/primary_key are tolerated-and-ignored legacy keys.
        schema_file = tmp_path / "orders.yaml"
        schema_file.write_text(
            'version: "1.0"\n'
            "description: legacy\n"
            "primary_key: [id]\n"
            "name: orders\n"
            "columns:\n"
            "  - name: id\n"
            "    type: BIGINT\n"
        )
        schema_data = self.parser.parse_schema_file(schema_file)
        assert schema_data["name"] == "orders"

    def test_validate_schema_no_longer_requires_top_level_name(self):
        # The top-level identifier is optional in the unified format.
        errors = self.parser.validate_schema(
            {"columns": [{"name": "id", "type": "BIGINT"}]}
        )
        assert errors == []
