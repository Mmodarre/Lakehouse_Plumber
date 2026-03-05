"""Jobs stats loader for monitoring pipeline.

Uses the Databricks SDK to correlate pipeline runs with their triggering jobs
and enrich with pipeline and job tags.
"""

import json
from collections import defaultdict

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

JOBS_STATS_SCHEMA = StructType(
    [
        StructField("pipeline_id", StringType()),
        StructField("pipeline_name", StringType()),
        StructField("update_id", StringType()),
        StructField("job_id", StringType()),
        StructField("job_run_id", StringType()),
        StructField("job_name", StringType()),
        StructField("job_run_start_time", TimestampType()),
        StructField("job_run_end_time", TimestampType()),
        StructField("job_run_status", StringType()),
        StructField("pipeline_tags", StringType()),  # JSON map of pipeline spec.tags
        StructField("job_tags", StringType()),  # JSON map of job settings.tags
    ]
)


def get_jobs_stats(spark, parameters) -> DataFrame:
    """Correlate pipeline runs with triggering jobs via Databricks SDK.

    Scans recent job runs for pipeline_task entries, matches each
    (pipeline_id, update_id) to its triggering job, and enriches
    with pipeline tags (from spec.tags) and job tags (from settings.tags).

    Args:
        spark: SparkSession instance
        parameters: Pipeline parameters dict (supports lookback_hours)

    Returns:
        DataFrame with job-pipeline correlation and tags
    """
    from datetime import datetime, timedelta, timezone

    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()

    lookback_hours = int(parameters.get("lookback_hours", "168"))  # default 7 days
    cutoff_dt = datetime.now(tz=timezone.utc) - timedelta(hours=lookback_hours)
    cutoff_ms = int(cutoff_dt.timestamp() * 1000)

    # --- Phase 1: Scan job runs to find pipeline tasks ---
    pipeline_to_jobs = defaultdict(list)
    job_ids_with_pipelines = set()  # only jobs that have pipeline tasks
    job_run_count = 0

    try:
        for run in w.jobs.list_runs(
            expand_tasks=True,
            start_time_from=cutoff_ms,
        ):
            job_run_count += 1
            if not run.tasks:
                continue
            for task in run.tasks:
                if task.pipeline_task and task.pipeline_task.pipeline_id:
                    pid = task.pipeline_task.pipeline_id
                    job_ids_with_pipelines.add(run.job_id)
                    pipeline_to_jobs[pid].append(
                        {
                            "job_id": str(run.job_id),
                            "job_run_id": str(run.run_id),
                            "job_name": run.run_name or "",
                            "start_time_ms": run.start_time or 0,
                            "end_time_ms": run.end_time or 0,
                            "status": (
                                run.state.result_state.value
                                if run.state and run.state.result_state
                                else "UNKNOWN"
                            ),
                        }
                    )
    except Exception as e:
        print(f"[jobs_stats] Error scanning job runs: {e}")

    print(
        f"[jobs_stats] Scanned {job_run_count} job runs, "
        f"found {len(pipeline_to_jobs)} pipelines triggered by "
        f"{len(job_ids_with_pipelines)} jobs"
    )

    # --- Phase 2: Fetch job tags (only for jobs with pipeline tasks) ---
    job_tags_cache = {}
    for job_id in job_ids_with_pipelines:
        try:
            job_info = w.jobs.get(job_id)
            tags = {}
            if job_info.settings and job_info.settings.tags:
                tags = {k: v for k, v in job_info.settings.tags.items()}
            job_tags_cache[str(job_id)] = tags
        except Exception as e:
            print(f"[jobs_stats] Error fetching job {job_id} tags: {e}")
            job_tags_cache[str(job_id)] = {}

    print(f"[jobs_stats] Fetched tags for {len(job_tags_cache)} jobs")

    # --- Phase 3: Get pipeline info + tags, correlate updates to jobs ---
    correlation_rows = []

    for pid, jobs in pipeline_to_jobs.items():
        try:
            pipeline_info = w.pipelines.get(pipeline_id=pid)
            pname = pipeline_info.name or ""

            # Extract pipeline tags via raw API (SDK model may not expose tags)
            pipeline_tags = _get_pipeline_tags(w, pid)

            pipeline_tags_json = json.dumps(pipeline_tags) if pipeline_tags else "{}"

            # Get recent updates
            updates = []
            if pipeline_info.latest_updates:
                for u in pipeline_info.latest_updates:
                    updates.append(
                        {
                            "update_id": u.update_id,
                            "creation_time_ms": _parse_ts_ms(u.creation_time),
                        }
                    )

            # Match each update to closest job run by start_time
            for upd in updates:
                upd_ts = upd["creation_time_ms"]
                best_match = None
                best_diff = float("inf")
                for j in jobs:
                    diff = abs(j["start_time_ms"] - upd_ts)
                    if diff < best_diff:
                        best_diff = diff
                        best_match = j

                # Only correlate if within 5 minutes
                if best_match and best_diff < 300_000:
                    j_tags = job_tags_cache.get(best_match["job_id"], {})
                    job_tags_json = json.dumps(j_tags) if j_tags else "{}"

                    correlation_rows.append(
                        (
                            pid,
                            pname,
                            upd["update_id"],
                            best_match["job_id"],
                            best_match["job_run_id"],
                            best_match["job_name"],
                            _ms_to_timestamp(best_match["start_time_ms"]),
                            _ms_to_timestamp(best_match["end_time_ms"]),
                            best_match["status"],
                            pipeline_tags_json,
                            job_tags_json,
                        )
                    )
        except Exception as e:
            print(f"[jobs_stats] Error processing pipeline {pid}: {e}")

    print(f"[jobs_stats] Built {len(correlation_rows)} correlations")

    if not correlation_rows:
        return spark.createDataFrame([], JOBS_STATS_SCHEMA)

    return spark.createDataFrame(correlation_rows, JOBS_STATS_SCHEMA)


def _get_pipeline_tags(w, pipeline_id):
    """Fetch pipeline tags via raw REST API (SDK model may not expose spec.tags)."""
    try:
        resp = w.api_client.do(
            "GET", f"/api/2.0/pipelines/{pipeline_id}"
        )
        if resp and isinstance(resp, dict):
            return resp.get("spec", {}).get("tags", {})
    except Exception as e:
        print(f"[jobs_stats] Error fetching tags for pipeline {pipeline_id}: {e}")
    return {}


def _parse_ts_ms(val):
    """Parse creation_time (ISO string or epoch ms) to epoch ms."""
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val) if val > 1e12 else int(val * 1000)
    try:
        return int(str(val))
    except ValueError:
        from datetime import datetime

        dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)


def _ms_to_timestamp(ms):
    """Convert epoch milliseconds to datetime for Spark TimestampType."""
    if not ms:
        return None
    from datetime import datetime, timezone

    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
