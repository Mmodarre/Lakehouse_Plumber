"""Unit tests for SchemaParser column-tag extraction and validation."""

import pytest

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
