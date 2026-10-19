"""Unit tests for the data-quality transform generator's quarantine mode.

Covers ``DataQualityTransformGenerator`` output for ``mode: quarantine`` (DLQ
inbox/outbox sinks, source-scoped MERGEs, recycle flows, hash exclusions,
imports), plus the ``dqe`` mode used when ``mode`` is omitted or explicit.

Moved verbatim from ``tests/test_transform_generators.py`` so the tests mirror
``src/lhp/generators/transform/data_quality.py`` (constitution §8.2).
"""

import pytest
import yaml

from lhp.generators.transform import DataQualityTransformGenerator
from lhp.models import Action, ActionType, QuarantineConfig, TransformType

pytestmark = pytest.mark.unit


class TestDataQualityQuarantine:
    """Test quarantine mode for data quality transform generator."""

    def _create_expectations_file(self, tmpdir, expectations_data):
        expectations_file = tmpdir / "expectations.yaml"
        expectations_file.write_text(yaml.dump(expectations_data))
        return str(expectations_file)

    def test_data_quality_quarantine_mode_unified(self, tmp_path):
        """Test quarantine generates unified code with runtime _rescued_data detection."""
        generator = DataQualityTransformGenerator()
        expectations_data = {
            "email IS NOT NULL": {"action": "drop", "name": "email_not_null"},
            "age >= 18": {"action": "drop", "name": "age_check"},
        }
        exp_file = self._create_expectations_file(tmp_path, expectations_data)

        action = Action(
            name="validate_orders",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_raw_data",
            target="v_validated",
            readMode="stream",
            expectations_file=exp_file,
            mode="quarantine",
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.universal_dlq",
                source_table="cat.sch.bronze_orders",
            ),
        )

        code = generator.generate(action, {"spec_dir": tmp_path})

        assert "_EXPECTATIONS_v_raw_data = {" in code
        assert "_INVERSE_FILTER_v_raw_data" in code
        assert "_FAILED_RULE_EXPRS_v_raw_data" in code
        assert 'DLQ_TABLE_v_raw_data = "cat.sch.universal_dlq"' in code
        assert 'DLQ_OUTBOX_TABLE_v_raw_data = "cat.sch.universal_dlq_outbox"' in code
        assert 'SOURCE_TABLE_v_raw_data = "cat.sch.bronze_orders"' in code

        # Static dict, not comprehension
        assert "_EXPECTATIONS_RECYCLED_v_raw_data = {" in code

        # Built-in defaults, no project config
        assert "_HASH_EXCLUDE_COLS_v_raw_data" in code
        assert "hash_cols" in code

        # Native Spark rescued_data handling (no UDF)
        assert "map_zip_with" in code
        assert "map_filter" in code
        assert "from_json" in code

        # Runtime detection: both branches present
        assert 'if "_rescued_data" in batch_df.columns:' in code
        assert "variant_cols" in code
        assert "F.parse_json(F.to_json(F.struct(*data_cols)))" in code
        assert 'F.lit(None).cast("string")' in code

        assert "_clean_v_raw_data" in code
        assert "@dp.expect_all_or_drop(_EXPECTATIONS_v_raw_data)" in code

        assert "dlq_sink_v_raw_data" in code
        assert "@dp.foreach_batch_sink" in code
        assert "DeltaTable.forName" in code
        assert ".whenNotMatchedInsertAll()" in code

        assert "quarantine_flow_v_raw_data" in code
        assert "@dp.append_flow" in code

        # inbox → outbox dedup
        assert "recycle_sink_v_raw_data" in code
        assert "Window.partitionBy" in code
        assert "row_number" in code
        assert "whenNotMatchedInsertAll" in code

        # CDF from DLQ
        assert "recycle_flow_v_raw_data" in code
        assert "readChangeFeed" in code

        # outbox → validated
        assert "_recycled_v_raw_data" in code
        assert "@dp.expect_all_or_drop(_EXPECTATIONS_RECYCLED_v_raw_data" in code
        assert "skipChangeCommits" in code

        # output view reads from _recycled_ (no direct CDF read)
        assert "def v_validated():" in code
        assert "clean.union(recycled)" in code
        assert 'spark.readStream.table("_recycled_v_raw_data")' in code
        assert "try_variant_get" in code

    def test_data_quality_quarantine_coerces_all_expectations_to_drop(self, tmp_path):
        """All expectations should appear in @dp.expect_all_or_drop."""
        generator = DataQualityTransformGenerator()
        expectations_data = {
            "col1 IS NOT NULL": {"action": "fail", "name": "rule1"},
            "col2 > 0": {"action": "warn", "name": "rule2"},
            "col3 IS NOT NULL": {"action": "drop", "name": "rule3"},
        }
        exp_file = self._create_expectations_file(tmp_path, expectations_data)

        action = Action(
            name="validate_data",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_src",
            target="v_out",
            readMode="stream",
            expectations_file=exp_file,
            mode="quarantine",
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.dlq", source_table="cat.sch.src"
            ),
        )

        code = generator.generate(action, {"spec_dir": tmp_path})

        assert "@dp.expect_all_or_drop(_EXPECTATIONS_v_src)" in code
        assert '"rule1": "col1 IS NOT NULL"' in code
        assert '"rule2": "col2 > 0"' in code
        assert '"rule3": "col3 IS NOT NULL"' in code

        assert "@dp.expect_all_or_fail" not in code
        assert "@dp.expect_all({" not in code

    def test_data_quality_default_mode_is_dqe(self, tmp_path):
        """Backward compatibility: no mode field produces DQE output."""
        generator = DataQualityTransformGenerator()
        expectations_data = {
            "email IS NOT NULL": {"action": "warn", "name": "email_not_null"},
        }
        exp_file = self._create_expectations_file(tmp_path, expectations_data)

        action = Action(
            name="validate_data",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_src",
            target="v_out",
            readMode="stream",
            expectations_file=exp_file,
        )

        code = generator.generate(action, {"spec_dir": tmp_path})

        assert "@dp.temporary_view()" in code
        assert "def v_out():" in code
        assert "foreach_batch_sink" not in code
        assert "append_flow" not in code
        assert "DLQ_TABLE" not in code

    def test_data_quality_explicit_dqe_mode(self, tmp_path):
        """mode='dqe' produces same output as no mode."""
        generator = DataQualityTransformGenerator()
        expectations_data = {
            "email IS NOT NULL": {"action": "warn", "name": "email_not_null"},
        }
        exp_file = self._create_expectations_file(tmp_path, expectations_data)

        action = Action(
            name="validate_data",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_src",
            target="v_out",
            readMode="stream",
            expectations_file=exp_file,
            mode="dqe",
        )

        code = generator.generate(action, {"spec_dir": tmp_path})

        assert "@dp.temporary_view()" in code
        assert "foreach_batch_sink" not in code

    def test_data_quality_quarantine_imports(self, tmp_path):
        """Verify quarantine adds json, DeltaTable, F imports."""
        generator = DataQualityTransformGenerator()
        expectations_data = {
            "id IS NOT NULL": {"action": "drop", "name": "id_check"},
        }
        exp_file = self._create_expectations_file(tmp_path, expectations_data)

        action = Action(
            name="validate_data",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_src",
            target="v_out",
            readMode="stream",
            expectations_file=exp_file,
            mode="quarantine",
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.dlq", source_table="cat.sch.src"
            ),
        )

        generator.generate(action, {"spec_dir": tmp_path})

        imports = generator.imports
        assert "from delta.tables import DeltaTable" in imports
        assert "from pyspark.sql import functions as F" in imports
        assert "from pyspark.sql.types import MapType, StringType" in imports
        assert "from pyspark.sql.window import Window" in imports
        assert "from pyspark import pipelines as dp" in imports

    def test_data_quality_quarantine_operational_metadata(self, tmp_path):
        """Metadata columns should appear in final output view."""
        from lhp.models import (
            MetadataColumnConfig,
            ProjectConfig,
            ProjectOperationalMetadataConfig,
        )

        generator = DataQualityTransformGenerator()
        expectations_data = {
            "id IS NOT NULL": {"action": "drop", "name": "id_check"},
        }
        exp_file = self._create_expectations_file(tmp_path, expectations_data)

        action = Action(
            name="validate_data",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_src",
            target="v_out",
            readMode="stream",
            expectations_file=exp_file,
            mode="quarantine",
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.dlq", source_table="cat.sch.src"
            ),
            operational_metadata=["_load_timestamp", "_source_file"],
        )

        metadata_config = ProjectOperationalMetadataConfig(
            columns={
                "_load_timestamp": MetadataColumnConfig(
                    expression="current_timestamp()",
                    description="Load timestamp",
                    applies_to=["streaming_table", "materialized_view", "view"],
                ),
                "_source_file": MetadataColumnConfig(
                    expression="input_file_name()",
                    description="Source file path",
                    applies_to=["streaming_table", "materialized_view", "view"],
                ),
            }
        )
        project_config = ProjectConfig(
            name="test_project",
            version="1.0",
            operational_metadata=metadata_config,
        )

        flowgroup_config = {
            "spec_dir": tmp_path,
            "project_config": project_config,
        }

        code = generator.generate(action, flowgroup_config)

        assert "_recycled_v_src" in code
        assert "@dp.expect_all_or_drop(_EXPECTATIONS_RECYCLED_v_src" in code

        assert "_load_timestamp" in code
        assert "_source_file" in code
        assert "# Add operational metadata columns" in code

        # Hash exclusion includes both project-config AND built-in default columns
        assert "_HASH_EXCLUDE_COLS_v_src" in code
        assert "hash_cols" in code
        assert '"_load_timestamp"' in code
        assert '"_source_file"' in code
        assert '"_ingestion_timestamp"' in code
        assert '"_pipeline_run_id"' in code

    def test_data_quality_quarantine_hash_excludes_metadata_cols(self, tmp_path):
        """Verify the exclusion set content and hash_cols filtering is present."""
        generator = DataQualityTransformGenerator()
        expectations_data = {
            "id IS NOT NULL": {"action": "drop", "name": "id_check"},
        }
        exp_file = self._create_expectations_file(tmp_path, expectations_data)

        action = Action(
            name="validate_data",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_src",
            target="v_out",
            readMode="stream",
            expectations_file=exp_file,
            mode="quarantine",
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.dlq", source_table="cat.sch.src"
            ),
        )

        # No project config — only built-in defaults in exclusion set
        code = generator.generate(action, {"spec_dir": tmp_path})

        assert "_HASH_EXCLUDE_COLS_v_src" in code
        assert '"_ingestion_timestamp"' in code
        assert '"_source_file"' in code
        assert '"_pipeline_run_id"' in code
        assert '"_pipeline_name"' in code
        assert '"_flowgroup_name"' in code

        assert "hash_cols = [c for c in data_cols if c not in _exclude]" in code

        # _row_json uses hash_cols, not data_cols
        assert "F.to_json(F.struct(*hash_cols))" in code

        # _row_data uses data_cols (non-rescued branch)
        assert "F.parse_json(F.to_json(F.struct(*data_cols)))" in code

    def test_data_quality_quarantine_rules_with_double_quotes(self, tmp_path):
        """Rules containing double quotes must not cause a SyntaxError."""
        generator = DataQualityTransformGenerator()
        expectations_data = {
            'status = "active"': {"action": "drop", "name": "status_active"},
            "age >= 18": {"action": "drop", "name": "age_check"},
        }
        exp_file = self._create_expectations_file(tmp_path, expectations_data)

        action = Action(
            name="validate_quotes",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_src",
            target="v_out",
            readMode="stream",
            expectations_file=exp_file,
            mode="quarantine",
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.dlq", source_table="cat.sch.src"
            ),
        )

        code = generator.generate(action, {"spec_dir": tmp_path})

        try:
            compile(code, "<string>", "exec")
        except SyntaxError as e:
            pytest.fail(
                f"Generated code with double-quoted rule is not valid Python: {e}"
            )

        assert r"status = \"active\"" in code

    def test_data_quality_quarantine_merges_scoped_to_source_table(self, tmp_path):
        """Both DLQ MERGEs must scope the target side to this action's source table."""
        generator = DataQualityTransformGenerator()
        expectations_data = {
            "email IS NOT NULL": {"action": "drop", "name": "email_not_null"},
            "age >= 18": {"action": "drop", "name": "age_check"},
        }
        exp_file = self._create_expectations_file(tmp_path, expectations_data)

        action = Action(
            name="validate_orders",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_raw_data",
            target="v_validated",
            readMode="stream",
            expectations_file=exp_file,
            mode="quarantine",
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.universal_dlq",
                source_table="cat.sch.bronze_orders",
            ),
        )

        code = generator.generate(action, {"spec_dir": tmp_path})

        assert (
            '(F.col("dlq._dlq_source_table") == F.lit(SOURCE_TABLE_v_raw_data))' in code
        )
        assert '& (F.col("dlq._dlq_sk") == F.col("new._dlq_sk"))' in code
        assert (
            '(F.col("out._dlq_source_table") == F.lit(SOURCE_TABLE_v_raw_data))' in code
        )
        assert '& (F.col("out._dlq_sk") == F.col("new._dlq_sk"))' in code
        assert '"dlq._dlq_sk = new._dlq_sk"' not in code
        assert '"out._dlq_sk = new._dlq_sk"' not in code
        compile(code, "<string>", "exec")

    def test_data_quality_quarantine_merge_scoped_with_backtick_source_table(
        self, tmp_path
    ):
        """A backtick-quoted source_table is referenced via its constant only."""
        generator = DataQualityTransformGenerator()
        expectations_data = {
            "email IS NOT NULL": {"action": "drop", "name": "email_not_null"},
            "age >= 18": {"action": "drop", "name": "age_check"},
        }
        exp_file = self._create_expectations_file(tmp_path, expectations_data)

        action = Action(
            name="validate_orders",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_raw_data",
            target="v_validated",
            readMode="stream",
            expectations_file=exp_file,
            mode="quarantine",
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.universal_dlq",
                source_table="`my-cat`.silver.orders",
            ),
        )

        code = generator.generate(action, {"spec_dir": tmp_path})

        assert 'SOURCE_TABLE_v_raw_data = "`my-cat`.silver.orders"' in code
        assert "F.lit(SOURCE_TABLE_v_raw_data)" in code
        assert (
            '(F.col("dlq._dlq_source_table") == F.lit(SOURCE_TABLE_v_raw_data))' in code
        )
        assert (
            '(F.col("out._dlq_source_table") == F.lit(SOURCE_TABLE_v_raw_data))' in code
        )
        # only the constant; never inlined into a condition
        assert code.count("my-cat") == 1
        compile(code, "<string>", "exec")

    def test_data_quality_quarantine_filters_safe_for_quoted_source_table(
        self, tmp_path
    ):
        """A quote in source_table must not reach a SQL-string recycle/outbox filter."""
        generator = DataQualityTransformGenerator()
        expectations_data = {
            "email IS NOT NULL": {"action": "drop", "name": "email_not_null"},
            "age >= 18": {"action": "drop", "name": "age_check"},
        }
        exp_file = self._create_expectations_file(tmp_path, expectations_data)

        action = Action(
            name="validate_orders",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_raw_data",
            target="v_validated",
            readMode="stream",
            expectations_file=exp_file,
            mode="quarantine",
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.universal_dlq",
                source_table="cat.sch.o'brien",
            ),
        )

        code = generator.generate(action, {"spec_dir": tmp_path})

        compile(code, "<string>", "exec")
        assert "_dlq_source_table = '" not in code
        assert 'F.col("_dlq_source_table") == F.lit(SOURCE_TABLE_v_raw_data)' in code
        assert 'F.col("_change_type").isin("insert", "update_postimage")' in code
        # only the constant; never inlined into a filter
        assert code.count("o'brien") == 1
