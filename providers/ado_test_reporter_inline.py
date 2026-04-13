"""
ADO Test Reporter (Inline) — Publishes DQ test results to Azure DevOps Test Plans.

In this variant, test_id values in the pipeline YAML ARE the ADO Test Case IDs
directly. No test_case_mapping is needed in the config.

Example pipeline YAML:
    - name: tst_pk_unique
      type: test
      test_id: "2272983"   # This IS the ADO Test Case ID

Creates ONE Test Run per pipeline execution, batches all results, and completes
the run. Completing the run auto-updates test point outcomes — no separate PATCH
of test points is needed.

API flow (3-4 HTTP calls total):
    1. GET test points for the configured suite → build test_case_id → point_id map
    2. POST create Test Run linked to the plan, with resolved pointIds
    3. PATCH add results — batch array with outcome, comment, completedDate
    4. PATCH complete run — set state to Completed

Usage in lhp.yaml:
    test_reporting:
      module_path: "providers/ado_test_reporter_inline.py"
      function_name: "publish_results"
      config_file: "config/ado_config.yaml"

Provider function contract:
    publish_results(results, config, context, spark) -> {"published": N, "failed": M}
"""

import base64
import logging
from datetime import datetime, timezone
from urllib.parse import quote

import requests

logger = logging.getLogger("ado_test_reporter_inline")


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


def _get_pat(config, spark):
    """Retrieve ADO PAT from Databricks secrets.

    Config must include:
        ado.pat_secret_scope — Databricks secret scope name
        ado.pat_secret_key   — Key within that scope
    """
    scope = config["ado"]["pat_secret_scope"]
    key = config["ado"]["pat_secret_key"]
    logger.debug("Retrieving PAT from Databricks secrets")
    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
        pat = dbutils.secrets.get(scope=scope, key=key)
    except Exception as e:
        raise RuntimeError(
            f"Failed to retrieve ADO PAT from Databricks secrets: {e}"
        ) from e

    if not pat:
        raise RuntimeError(
            "ADO PAT is empty. Verify the secret scope and key in ado_config.yaml "
            "and ensure the secret is set in Databricks."
        )
    return pat


def _build_headers(pat):
    """Build HTTP headers with Basic auth for the ADO REST API."""
    encoded = base64.b64encode(f":{pat}".encode("utf-8")).decode("utf-8")
    return {
        "Content-Type": "application/json",
        "Authorization": f"Basic {encoded}",
    }


def _base_url(config):
    """Build the ADO REST API base URL from config."""
    org = config["ado"]["organization"]
    project = quote(config["ado"]["project"], safe="")
    return f"https://dev.azure.com/{org}/{project}/_apis"


def _get_test_points(base, headers, plan_id, suite_id, api_version):
    """GET test points for a suite → return {test_case_id: point_id} map."""
    url = (
        f"{base}/test/Plans/{plan_id}/Suites/{suite_id}"
        f"/points?api-version={api_version}"
    )
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    points = resp.json().get("value", [])
    tc_to_point = {}
    for point in points:
        tc_ref = point.get("testCaseReference", point.get("testCase", {}))
        tc_id = tc_ref.get("id")
        if tc_id is not None:
            tc_to_point[int(tc_id)] = point["id"]

    logger.debug(f"Retrieved {len(tc_to_point)} test points from suite {suite_id}")
    return tc_to_point


def _create_test_run(base, headers, api_version, plan_id, run_name, point_ids):
    """POST create a new Test Run linked to the plan with the given point IDs."""
    url = f"{base}/test/runs?api-version={api_version}"
    payload = {
        "name": run_name,
        "isAutomated": True,
        "plan": {"id": str(plan_id)},
        "pointIds": point_ids,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    run = resp.json()
    run_id = run["id"]
    logger.info(f"Created ADO Test Run {run_id}: '{run_name}'")
    return run_id


def _add_results(base, headers, api_version, run_id, result_entries):
    """PATCH result entries into an existing Test Run.

    Each entry: {id, outcome, state, testCase.id, testPoint.id, comment, completedDate}
    """
    url = f"{base}/test/runs/{run_id}/results?api-version={api_version}"

    # GET existing results to obtain their IDs (created when the run was created)
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    existing = resp.json().get("value", [])

    # Build a map from testCaseReference.id → existing result ID
    existing_map = {}
    for er in existing:
        tc_ref = er.get("testCaseReference", er.get("testCase", {}))
        tc_id = tc_ref.get("id")
        if tc_id is not None:
            existing_map[int(tc_id)] = er["id"]

    # Merge result entries with existing result IDs
    updates = []
    for entry in result_entries:
        tc_id = entry.get("testCase", {}).get("id")
        if tc_id and int(tc_id) in existing_map:
            entry["id"] = existing_map[int(tc_id)]
            updates.append(entry)
        else:
            logger.warning(
                f"No existing result slot for test case {tc_id} in run {run_id}"
            )

    if not updates:
        logger.warning(
            "No result entries matched existing run results — nothing to update"
        )
        return 0

    resp = requests.patch(url, headers=headers, json=updates, timeout=30)
    resp.raise_for_status()
    logger.debug(f"Updated {len(updates)} result(s) in run {run_id}")
    return len(updates)


def _complete_run(base, headers, api_version, run_id):
    """PATCH the Test Run to Completed state."""
    url = f"{base}/test/runs/{run_id}?api-version={api_version}"
    resp = requests.patch(url, headers=headers, json={"state": "Completed"}, timeout=30)
    resp.raise_for_status()
    logger.info(f"Completed ADO Test Run {run_id}")


def _validate_test_id(test_id):
    """Validate that a test_id is a valid integer string (ADO Test Case ID)."""
    try:
        int(test_id)
        return True
    except (ValueError, TypeError):
        return False


def _resolve_results_inline(results, tc_to_point, suite_id, context):
    """Resolve pipeline results to ADO result entries (inline mode).

    In inline mode, each result's test_id IS the ADO Test Case ID directly.

    Returns:
        Tuple of (matched_point_ids, result_entries, failed_count).
    """
    pipeline_name = context.get("pipeline_name", "unknown")
    terminal_state = context.get("terminal_state", "unknown")
    now_iso = datetime.now(timezone.utc).isoformat()

    matched_point_ids = []
    result_entries = []
    failed_count = 0

    for r in results:
        test_id = r.get("test_id", "")

        # Validate test_id is a valid integer (ADO Test Case ID)
        if not _validate_test_id(test_id):
            logger.warning(
                f"Invalid test_id='{test_id}' — must be a numeric ADO Test Case ID. "
                f"Skipping result for {r.get('flow_name')}/{r.get('expectation_name')}"
            )
            failed_count += 1
            continue

        ado_tc_id = int(test_id)
        point_id = tc_to_point.get(ado_tc_id)
        if point_id is None:
            logger.warning(
                f"Test case {ado_tc_id} has no test point in suite {suite_id} — "
                f"skipping result for {r.get('flow_name')}/{r.get('expectation_name')}"
            )
            failed_count += 1
            continue

        matched_point_ids.append(point_id)
        outcome = "Passed" if r.get("status") == "PASS" else "Failed"
        comment = (
            f"Test Case: {ado_tc_id}\n"
            f"Expectation: {r.get('expectation_name', '')}\n"
            f"Flow: {r.get('flow_name', '')}\n"
            f"Passed: {r.get('passed_records', 0)}, "
            f"Failed: {r.get('failed_records', 0)}\n"
            f"Pipeline: {pipeline_name}\n"
            f"Terminal state: {terminal_state}"
        )

        entry = {
            "outcome": outcome,
            "state": "Completed",
            "testCase": {"id": str(ado_tc_id)},
            "testPoint": {"id": point_id},
            "comment": comment,
            "completedDate": now_iso,
        }
        if outcome == "Failed":
            entry["errorMessage"] = comment

        result_entries.append(entry)

    return matched_point_ids, result_entries, failed_count


def publish_results(results, config, context, spark):
    """Publish DQ test results to Azure DevOps Test Plans (inline mode).

    In this mode, each result's test_id IS the ADO Test Case ID directly.
    No test_case_mapping is needed.

    Args:
        results: List of result dicts, each with keys:
            test_id (str — must be a valid ADO Test Case ID, e.g., "2272983"),
            flow_name, expectation_name, passed_records,
            failed_records, status, collected_at
        config: Provider configuration dict (loaded from ado_config.yaml).
            Required keys: ado.organization, ado.project, ado.pat_secret_scope,
            ado.pat_secret_key, test_plan.plan_id, test_plan.suite_id
            NOT required: test_case_mapping (ignored if present)
        context: Pipeline context dict with keys:
            pipeline_name, pipeline_id, update_id, terminal_state
        spark: SparkSession instance.

    Returns:
        Dict with "published" and "failed" counts.
    """
    _configure_logging(config)

    pipeline_name = context.get("pipeline_name", "unknown")
    terminal_state = context.get("terminal_state", "unknown")
    logger.info(
        f"ADO inline reporter invoked — {len(results)} result(s), "
        f"pipeline={pipeline_name}, terminal_state={terminal_state}"
    )

    # Dry-run mode
    if config.get("dry_run", False):
        logger.info(f"DRY RUN — would publish {len(results)} result(s) to ADO")
        for r in results:
            logger.debug(
                f"  test_id={r.get('test_id')}/{r.get('expectation_name')}: "
                f"{r.get('status')}"
            )
        return {"published": 0, "failed": 0}

    try:
        pat = _get_pat(config, spark)
        headers = _build_headers(pat)
        base = _base_url(config)

        plan_id = config["test_plan"]["plan_id"]
        suite_id = config["test_plan"]["suite_id"]
        api_version = config.get("ado", {}).get("api_version", "7.1")

        # Step 1: GET test points for the suite
        tc_to_point = _get_test_points(base, headers, plan_id, suite_id, api_version)

        # Step 1b: Resolve results (test_id IS the ADO test case ID)
        matched_point_ids, result_entries, failed_count = _resolve_results_inline(
            results, tc_to_point, suite_id, context
        )

        if not matched_point_ids:
            logger.warning("No results matched ADO test points — nothing to publish")
            return {"published": 0, "failed": failed_count + len(results)}

        # Step 2: Create Test Run
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        run_name = (
            f"LHP DQ Validation — {pipeline_name} — {terminal_state} — {timestamp}"
        )
        run_id = _create_test_run(
            base, headers, api_version, plan_id, run_name, matched_point_ids
        )

        # Step 3: Add results
        updated = _add_results(base, headers, api_version, run_id, result_entries)
        published_count = updated

        # Step 4: Complete the run
        _complete_run(base, headers, api_version, run_id)

        logger.info(
            f"Published {published_count} result(s), "
            f"{failed_count} skipped — ADO Test Run {run_id}"
        )
        return {"published": published_count, "failed": failed_count}

    except requests.HTTPError as e:
        body = ""
        if e.response is not None:
            try:
                body = e.response.text[:500]
            except Exception:
                pass
        logger.error(f"ADO API error: {e}\nResponse body: {body}")
        return {"published": 0, "failed": len(results)}

    except Exception as e:
        logger.error(f"ADO publish failed: {e}")
        return {"published": 0, "failed": len(results)}
