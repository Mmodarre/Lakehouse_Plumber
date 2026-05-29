"""Lakehouse Plumber - YAML-driven framework for Databricks Lakeflow Spark Declarative Pipelines."""

# Composition root: wire the per-action generators into the core ActionRegistry.
# ``core`` must not import ``generators`` (layering), so registration is pushed
# from here, above both layers. Runs once on first ``import lhp``.
from lhp.generators import registration as _registration  # noqa: F401,E402
