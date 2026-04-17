"""Test reporting publisher for performance testing project.

This is a minimal provider that logs results without external integration.
Used to exercise the test_reporting hook code generation path at scale.
"""


def publish_results(results, config, context, spark):
    """Publish test results (no-op for perf testing).

    Args:
        results: List of DQ expectation results
        config: Provider configuration from config_file
        context: Pipeline context (pipeline_id, update_id, terminal_state)
        spark: SparkSession instance

    Returns:
        Summary dict with published/failed counts
    """
    return {"published": len(results), "failed": 0}
