"""Tests for data quality expectations (DQE) parser functionality of LakehousePlumber."""

import pytest

from lhp.core.processing.dqe import DQEParser


class TestDQEParser:
    """Test data quality expectations parser."""

    def test_parse_expectations(self):
        """Test parsing expectations into categories."""
        parser = DQEParser()

        expectations = [
            {
                "constraint": "id IS NOT NULL",
                "type": "expect",
                "message": "ID required",
            },
            {
                "constraint": "age > 0",
                "type": "expect_or_drop",
                "message": "Invalid age",
            },
            {
                "constraint": "COUNT(*) > 0",
                "type": "expect_or_fail",
                "message": "No data",
            },
        ]

        expect_all, expect_drop, expect_fail = parser.parse_expectations(expectations)

        assert "ID required" in expect_all
        assert expect_all["ID required"] == "id IS NOT NULL"

        assert "Invalid age" in expect_drop
        assert "No data" in expect_fail


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
