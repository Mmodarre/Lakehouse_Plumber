"""Unit tests for SchemaParser column-tag extraction and validation."""

import pytest

from lhp.errors import LHPError
from lhp.parsers.schema_parser import SchemaParser


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
