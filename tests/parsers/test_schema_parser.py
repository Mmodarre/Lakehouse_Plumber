"""Tests for SchemaParser inline-dict schema handling.

Covers the DDL-hint conversion (``to_schema_hints``) and structural validation
(``validate_schema``) that back the inline structured YAML schema feature for
``write_target.table_schema``. The inline dict has the same shape as the existing
separate YAML schema files, so it flows through the SAME conversion.

SchemaParser lives at ``src/lhp/parsers/schema_parser.py``:
- ``to_schema_hints(schema_data: dict) -> str`` joins ``"<name> <type>[ NOT NULL]"``
  entries with ", " and raises an LHPError (VAL_016) when ``columns`` is missing.
- ``validate_schema(schema_data: dict) -> list`` returns error strings; it requires
  ``name`` and ``columns``, each column needs ``name``/``type``, and ``nullable`` must be a bool.
"""

import pytest

from lhp.errors import LHPError
from lhp.parsers.schema_parser import SchemaParser


class TestToSchemaHints:
    """Conversion of inline dict schemas to a DDL hint string."""

    def setup_method(self):
        self.parser = SchemaParser()

    def test_default_nullable_has_no_suffix(self):
        """A column without a 'nullable' key defaults to nullable -> no NOT NULL suffix."""
        result = self.parser.to_schema_hints(
            {"columns": [{"name": "name", "type": "STRING"}]}
        )
        assert result == "name STRING"

    def test_nullable_true_has_no_suffix(self):
        """An explicit nullable: true adds no suffix."""
        result = self.parser.to_schema_hints(
            {"columns": [{"name": "name", "type": "STRING", "nullable": True}]}
        )
        assert result == "name STRING"

    def test_nullable_false_appends_not_null(self):
        """nullable: false appends ' NOT NULL'."""
        result = self.parser.to_schema_hints(
            {"columns": [{"name": "customer_id", "type": "BIGINT", "nullable": False}]}
        )
        assert result == "customer_id BIGINT NOT NULL"

    def test_decimal_type_preserved(self):
        """Parameterised types such as DECIMAL(p,s) are preserved verbatim."""
        result = self.parser.to_schema_hints(
            {"columns": [{"name": "amount", "type": "DECIMAL(18,2)", "nullable": False}]}
        )
        assert result == "amount DECIMAL(18,2) NOT NULL"

    def test_multiple_columns_joined_with_comma_space(self):
        """Multiple columns are joined with ', ', mixing nullable and non-nullable."""
        result = self.parser.to_schema_hints(
            {
                "columns": [
                    {"name": "customer_id", "type": "BIGINT", "nullable": False},
                    {"name": "customer_name", "type": "STRING", "nullable": True},
                    {"name": "email", "type": "STRING"},
                    {"name": "signup_date", "type": "DATE", "nullable": False},
                ]
            }
        )
        assert result == (
            "customer_id BIGINT NOT NULL, customer_name STRING, "
            "email STRING, signup_date DATE NOT NULL"
        )

    def test_missing_columns_raises_lhp_error(self):
        """A dict without a 'columns' key raises an LHPError (VAL_016)."""
        with pytest.raises(LHPError) as exc_info:
            self.parser.to_schema_hints({"name": "no_columns"})
        # The error should reference the missing 'columns' field (VAL_016).
        assert "columns" in str(exc_info.value).lower()


class TestValidateSchema:
    """Structural validation of inline dict schemas."""

    def setup_method(self):
        self.parser = SchemaParser()

    def test_valid_schema_returns_empty_list(self):
        """A well-formed schema with name and columns returns no errors."""
        errors = self.parser.validate_schema(
            {
                "name": "customer_table",
                "columns": [
                    {"name": "customer_id", "type": "BIGINT", "nullable": False},
                    {"name": "customer_name", "type": "STRING"},
                ],
            }
        )
        assert errors == []

    def test_missing_columns_returns_error(self):
        """A schema without 'columns' produces a columns error."""
        errors = self.parser.validate_schema({"name": "customer_table"})
        assert any("columns" in e for e in errors)

    def test_non_bool_nullable_returns_error(self):
        """A column whose 'nullable' is not a bool produces a nullable error."""
        errors = self.parser.validate_schema(
            {
                "name": "customer_table",
                "columns": [
                    {"name": "customer_id", "type": "BIGINT", "nullable": "false"},
                ],
            }
        )
        assert any("nullable" in e for e in errors)

    def test_column_missing_type_returns_error(self):
        """A column without a 'type' produces a type error."""
        errors = self.parser.validate_schema(
            {
                "name": "customer_table",
                "columns": [
                    {"name": "customer_id"},
                ],
            }
        )
        assert any("type" in e for e in errors)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
