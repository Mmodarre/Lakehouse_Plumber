"""Test result publisher stub for E2E testing.

Implements the provider function contract for test reporting.
In production this would call an external API (e.g., Azure DevOps).
"""


def publish_results(results, config, context, spark):
    """Publish DQ test results to external system.

    Args:
        results: List of result dicts with test_id, flow_name, status, etc.
        config: Provider configuration dict (from config_file)
        context: Pipeline context (pipeline_id, update_id, terminal_state)
        spark: SparkSession instance

    Returns:
        Dict with published/failed counts
    """
    return {
        "published": len(results),
        "failed": 0,
        "errors": [],
    }
