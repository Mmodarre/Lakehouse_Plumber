"""Lakehouse Plumber - YAML-driven framework for Databricks Lakeflow Spark Declarative Pipelines."""

# Per-action generators are wired into the core ``ActionRegistry`` lazily at the
# single composition point — ``LakehousePlumberApplicationFacade.for_project``
# in ``lhp.api.facade`` (the legal ``api -> generators`` edge). Importing them
# here eagerly would pull the full generator/dependency stack (e.g. networkx)
# into ``sys.modules`` on a bare ``import lhp``, which we deliberately avoid.
