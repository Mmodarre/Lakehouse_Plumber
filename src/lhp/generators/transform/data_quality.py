"""Data quality transformation generator."""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from lhp.models import Action

from ...core.loaders import Expectations, load_action_expectations
from ...core.processing.dqe import DQEParser
from ...core.registry import BaseActionGenerator, ReadMode
from ...errors import ErrorFactory, codes
from ...utils.identifiers import replace_non_identifier_chars

logger = logging.getLogger(__name__)

# The reads the dqe template renders; validate_data_quality_transform rejects
# any other readMode (and any but stream in quarantine mode) with LHP-VAL-068.
_DQE_READ_MODES = ("stream", "batch")


class DataQualityTransformGenerator(BaseActionGenerator):
    renders_operational_metadata = True
    default_read_mode = "stream"

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")
        self.dqe_parser = DQEParser()

    @classmethod
    def consumer_read_mode(cls, action: Action) -> Optional[ReadMode]:
        """``stream`` in ``quarantine`` mode, else the action's ``readMode``.

        Quarantine always reads with ``spark.readStream``: its dead-letter
        recycling is a streaming flow. A ``dqe`` action with no ``readMode``
        reads its in-flowgroup producer's kind, which code generation passes
        in the context as ``effective_read_mode``.
        """
        if action.mode == "quarantine":
            return "stream"
        return super().consumer_read_mode(action)

    def generate(self, action: Action, flowgroup_config: Dict[str, Any]) -> str:
        logger.debug(
            f"Generating data quality transform for target '{action.target}', action '{action.name}'"
        )
        requested = self.read_mode(action)
        valid_modes = ("stream",) if action.mode == "quarantine" else _DQE_READ_MODES
        if requested not in valid_modes:
            raise AssertionError(
                f"Unvalidated data_quality readMode {requested!r} in action "
                f"'{action.name}'"
            )

        expectations_file = action.expectations_file
        if not expectations_file:
            raise ErrorFactory.missing_required_field(
                field_name="expectations_file",
                component_type="Data quality transform action",
                component_name=action.name,
                field_description="The expectations file containing data quality rules is required.",
                example_config="""actions:
  - name: check_quality
    type: transform
    sub_type: data_quality
    source: v_raw_data
    target: v_validated_data
    expectations_file: "expectations/quality_rules.yaml" """,
            )

        expectations = self._load_expectations(action, flowgroup_config.get("spec_dir"))

        total_rules = len(expectations) if isinstance(expectations, (list, dict)) else 0
        logger.debug(
            f"Data quality '{action.name}': {total_rules} expectation rules loaded, source='{self._extract_source_view(action.source)}'"
        )

        dq_mode = getattr(action, "mode", None) or "dqe"

        if dq_mode == "quarantine":
            return self._generate_quarantine_mode(
                action, expectations, flowgroup_config
            )
        return self._generate_dqe_mode(action, expectations, flowgroup_config)

    def _generate_dqe_mode(
        self, action: Action, expectations, flowgroup_config: Dict[str, Any]
    ) -> str:
        # Code generation passes the effective read, which follows the source
        # view's kind when the action sets no readMode; a context without it
        # (a generator called on its own) reads what the action asks for.
        readMode = flowgroup_config.get("effective_read_mode") or self.read_mode(action)

        warn_expectations, drop_expectations, fail_expectations = (
            self.dqe_parser.parse_expectations(expectations or {})
        )

        source_view = self._extract_source_view(action.source)

        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, flowgroup_config
        )

        template_context = {
            "target_view": action.target,
            "source_view": source_view,
            "readMode": readMode,
            "fail_expectations": fail_expectations,
            "drop_expectations": drop_expectations,
            "warn_expectations": warn_expectations,
            "description": action.description
            or f"Data quality checks for {action.source}",
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
        }

        return self.render_template("transform/data_quality.py.j2", template_context)

    def _generate_quarantine_mode(
        self,
        action,
        expectations: Expectations,
        flowgroup_config: Dict[str, Any],
    ) -> str:
        """Generate quarantine mode code with DLQ recycling."""
        logger.debug(f"Generating quarantine mode for action '{action.name}'")

        # SQL MERGE (not DeltaTable.forName) — session-safe on serverless (#355).
        self.add_import("from pyspark.sql import functions as F")
        self.add_import("from pyspark.sql.types import MapType, StringType")
        self.add_import("from pyspark.sql.window import Window")

        quarantine_config = action.quarantine
        if isinstance(quarantine_config, dict):
            dlq_table = quarantine_config.get("dlq_table", "")
            source_table = quarantine_config.get("source_table", "")
        else:
            dlq_table = quarantine_config.dlq_table
            source_table = quarantine_config.source_table

        dlq_outbox_table = dlq_table + "_outbox"

        # Parse ALL expectations as drop — DQEParser is the single owner
        all_expectations = self.dqe_parser.get_all_expectations_as_drop(expectations)

        # Filter out _rescued_data expectations for recycled path
        recycled_expectations = {
            k: v for k, v in all_expectations.items() if "_rescued_data" not in v
        }

        # Defensive check (validator should catch this first)
        if not all_expectations:
            raise ErrorFactory.validation_error(
                codes.VAL_014,
                title="Quarantine mode requires at least one expectation",
                details=(
                    f"Action '{action.name}' has mode='quarantine' but "
                    f"the expectations file contains no rules."
                ),
                suggestions=[
                    "Add at least one expectation to the expectations file.",
                    "Or remove mode='quarantine' to use standard DQE mode.",
                ],
            )

        # Replace each character outside [A-Za-z0-9_] in the source view name
        # with "_"; the result suffixes generated names (_clean_<view>, ...).
        # A leading digit or a Python keyword is left as it is.
        source_view = self._extract_source_view(action.source)
        safe_source_view = replace_non_identifier_chars(source_view)

        inverse_filter = "NOT ({})".format(
            " AND ".join(f"({rule})" for rule in all_expectations.values())
        )

        failed_rule_data = [
            {"name": name, "rule": rule} for name, rule in all_expectations.items()
        ]

        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, flowgroup_config
        )

        from ...core.codegen.operational_metadata import OperationalMetadataService

        service = OperationalMetadataService()
        project_config = flowgroup_config.get("project_config")
        hash_exclude_columns = sorted(
            service.get_all_metadata_column_names(project_config)
        )

        template_context = {
            "target_view": action.target,
            "source_view": source_view,
            "safe_source_view": safe_source_view,
            "description": (
                action.description or f"Data quality checks for {action.source}"
            ),
            "expectations": all_expectations,
            "inverse_filter": inverse_filter,
            "failed_rule_data": failed_rule_data,
            "dlq_table": dlq_table,
            "source_table": source_table,
            "dlq_outbox_table": dlq_outbox_table,
            "recycled_expectations": recycled_expectations,
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
            "hash_exclude_columns": hash_exclude_columns,
        }

        return self.render_template(
            "transform/data_quality_quarantine.py.j2", template_context
        )

    def _extract_source_view(self, source) -> str:
        if isinstance(source, str):
            return source
        if isinstance(source, dict):
            return source.get("view", source.get("source", ""))
        return ""

    def _load_expectations(
        self, action: Action, spec_dir: Path | None = None
    ) -> Expectations:
        # validate_data_quality_transform has checked the rules, inline or in
        # the file; a malformed file still raises the loader's LHP-VAL-008
        # rather than generating a view with no rules.
        return load_action_expectations(action, spec_dir or Path.cwd())
