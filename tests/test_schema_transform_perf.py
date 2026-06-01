"""Perf instrumentation test for SchemaTransformGenerator._load_schema_file.

Verifies that the ``schema_parse`` perf category fires exactly once per
schema-file load (instrumentation-only; does not assert on generated output).
"""

import pytest

from lhp.generators.transform.schema import SchemaTransformGenerator
from lhp.utils.performance_timer import (
    _perf_logger,
    enable_perf_timing,
    export_perf_for_merge,
)


@pytest.fixture(autouse=True)
def _reset_perf_state():
    """Reset module-level perf state before and after each test.

    Mirrors the fixture in tests/unit/test_performance_timer.py so the global
    summary does not leak across tests.
    """
    import lhp.utils.performance_timer as pt

    pt._enabled = False
    pt._start_wall_clock = None
    pt._summary.reset()
    for handler in _perf_logger.handlers[:]:
        handler.close()
        _perf_logger.removeHandler(handler)
    yield
    pt._enabled = False
    pt._start_wall_clock = None
    pt._summary.reset()
    for handler in _perf_logger.handlers[:]:
        handler.close()
        _perf_logger.removeHandler(handler)


def _write_schema_file(directory, name):
    """Create a minimal valid schema-transform file and return its name."""
    schema_file = directory / name
    schema_file.write_text("""
columns:
  - "c_custkey -> customer_id: BIGINT"
  - "c_name -> customer_name"
""")
    return name


class TestSchemaParsePerf:
    """schema_parse category fires once per _load_schema_file call."""

    def test_schema_parse_records_one_sample_per_load(self, tmp_path):
        """N schema loads => N samples in the schema_parse timing bucket."""
        generator = SchemaTransformGenerator()

        names = [_write_schema_file(tmp_path, f"transform_{i}.yaml") for i in range(3)]

        enable_perf_timing(tmp_path)

        for name in names:
            result = generator._load_schema_file(name, tmp_path)
            # Output neutrality sanity check: parsing still works as before.
            assert result["column_mapping"] == {
                "c_custkey": "customer_id",
                "c_name": "customer_name",
            }

        timings = export_perf_for_merge()["timings"]
        assert "schema_parse" in timings
        assert len(timings["schema_parse"]) == 3

    def test_schema_parse_not_recorded_when_perf_disabled(self, tmp_path):
        """No schema_parse bucket when perf timing is disabled (zero overhead)."""
        generator = SchemaTransformGenerator()
        name = _write_schema_file(tmp_path, "transform.yaml")

        # perf timing left disabled by the fixture
        result = generator._load_schema_file(name, tmp_path)
        assert result["column_mapping"] == {
            "c_custkey": "customer_id",
            "c_name": "customer_name",
        }

        timings = export_perf_for_merge()["timings"]
        assert "schema_parse" not in timings
