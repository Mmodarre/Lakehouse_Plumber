"""
Delta Test Reporter — Publishes DQ test results to a Delta table.

Simplest provider: appends each result as a row to a pre-existing Delta table.
The table must be created before this provider is used (see DDL in README.md).

Usage in lhp.yaml:
    test_reporting:
      module_path: "providers/delta_test_reporter.py"
      function_name: "publish_results"
      config:
        result_table: "{catalog}.{audit_schema}.lhp_test_results"

Provider function contract:
    publish_results(results, config, context, spark) -> {"published": N, "failed": M}
"""

import logging
from datetime import datetime, timezone

RESULT_TABLE = "{catalog}.{audit_schema}.lhp_test_results"

logger = logging.getLogger("delta_test_reporter")


def _configure_logging(config):
    """Set log level from config and ensure at least one handler exists."""
    level = config.get("log_level", "INFO").upper()
    logger.setLevel(getattr(logging, level, logging.INFO))
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")
        )
        logger.addHandler(handler)


def publish_results(results, config, context, spark):
    """Publish DQ test results to a Delta table.

    Args:
        results: List of result dicts, each with keys:
            test_id, flow_name, expectation_name, passed_records,
            failed_records, status, collected_at
        config: Provider configuration dict. Supported keys:
            result_table (str): Fully qualified table name.
            dry_run (bool): If True, log and return without writing.
            log_level (str): Logging level (default: INFO).
        context: Pipeline context dict with keys:
            pipeline_name, pipeline_id, update_id, terminal_state
        spark: SparkSession instance.

    Returns:
        Dict with "published" and "failed" counts.
    """
    _configure_logging(config)

    table_name = config.get("result_table", RESULT_TABLE)
    logger.info(
        f"Delta reporter invoked — {len(results)} result(s), "
        f"table={table_name}, terminal_state={context.get('terminal_state')}"
    )

    # Verify the target table exists before attempting writes
    if not spark.catalog.tableExists(table_name):
        logger.error(
            f"Table '{table_name}' does not exist. "
            f"Create it first using the DDL in the providers README, then re-run."
        )
        return {"published": 0, "failed": len(results)}

    # Dry-run mode: log what would be written without touching the table
    if config.get("dry_run", False):
        logger.info(
            f"DRY RUN — would publish {len(results)} result(s) to '{table_name}'"
        )
        for r in results:
            logger.debug(
                f"  {r.get('flow_name')}/{r.get('expectation_name')}: {r.get('status')}"
            )
        return {"published": 0, "failed": 0}

    # Build rows by merging result data with pipeline context
    published_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for r in results:
        rows.append(
            {
                "pipeline_name": context.get("pipeline_name", ""),
                "pipeline_id": context.get("pipeline_id", ""),
                "update_id": context.get("update_id", ""),
                "test_id": r.get("test_id", ""),
                "flow_name": r.get("flow_name", ""),
                "expectation_name": r.get("expectation_name", ""),
                "passed_records": r.get("passed_records", 0),
                "failed_records": r.get("failed_records", 0),
                "status": r.get("status", ""),
                "terminal_state": context.get("terminal_state", ""),
                "collected_at": r.get("collected_at", published_at),
                "published_at": published_at,
            }
        )

    # Write with explicit schema to prevent type inference surprises
    try:
        from pyspark.sql.types import (
            LongType,
            StringType,
            StructField,
            StructType,
        )

        schema = StructType(
            [
                StructField("pipeline_name", StringType(), False),
                StructField("pipeline_id", StringType(), False),
                StructField("update_id", StringType(), False),
                StructField("test_id", StringType(), False),
                StructField("flow_name", StringType(), False),
                StructField("expectation_name", StringType(), False),
                StructField("passed_records", LongType(), False),
                StructField("failed_records", LongType(), False),
                StructField("status", StringType(), False),
                StructField("terminal_state", StringType(), False),
                StructField("collected_at", StringType(), False),
                StructField("published_at", StringType(), False),
            ]
        )

        df = spark.createDataFrame(rows, schema=schema)
        df.write.mode("append").option("mergeSchema", "true").saveAsTable(table_name)

        logger.info(f"Published {len(rows)} result(s) to '{table_name}'")
        return {"published": len(rows), "failed": 0}

    except Exception as e:
        logger.error(f"Delta write failed: {e}")
        return {"published": 0, "failed": len(results)}
