"""ForEachBatch sink generator."""

import logging
from pathlib import Path
from typing import Any, Dict

from ....models.config import Action
from ....utils.error_formatter import (
    ErrorCategory,
    ErrorFormatter,
    LHPValidationError,
)
from .base_sink import BaseSinkWriteGenerator


class ForEachBatchSinkWriteGenerator(BaseSinkWriteGenerator):
    """Generate ForEachBatch sink write actions.

    Supports two modes:
    - External file: User provides module_path with function body
    - Inline code: User provides batch_handler with function body

    In both cases, user provides ONLY the function body. LHP wraps it
    with the @dp.foreach_batch_sink decorator and generates the append_flow.
    """

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate ForEachBatch sink code.

        Args:
            action: Action configuration
            context: Context dictionary with flowgroup and project info

        Returns:
            Generated Python code for ForEachBatch sink

        Raises:
            FileNotFoundError: If module_path file doesn't exist
            ValueError: If configuration is invalid
        """
        sink_config = action.write_target

        # Extract configuration
        sink_name = sink_config.get("sink_name")
        module_path = sink_config.get("module_path")
        batch_handler = sink_config.get("batch_handler")
        handler_source = (
            "module_path" if module_path else ("inline" if batch_handler else "none")
        )
        self.logger.debug(
            f"Generating ForEachBatch sink for action '{action.name}': sink_name='{sink_name}', handler_source='{handler_source}'"
        )

        # Validate required fields (should already be validated, but double-check)
        if not sink_name:
            raise ErrorFormatter.missing_required_field(
                field_name="sink_name",
                component_type="ForEachBatch sink write action",
                component_name=action.name,
                field_description="This field specifies the unique name for the foreach_batch_sink.",
                example_config=f"""actions:
  - name: {action.name}
    type: write
    source: v_data
    write_target:
      type: sink
      sink_type: foreachbatch
      sink_name: "my_batch_sink"  # Required
      module_path: "batch_handlers/my_handler.py" """,
            )

        # Get batch handler code (from file or inline)
        if module_path:
            batch_handler_code = self._load_batch_handler_from_file(
                module_path, action, context
            )
        elif batch_handler:
            batch_handler_code = batch_handler
        else:
            raise ErrorFormatter.missing_required_field(
                field_name="module_path or batch_handler",
                component_type="ForEachBatch sink write action",
                component_name=action.name,
                field_description="ForEachBatch sinks require either 'module_path' (external file) or 'batch_handler' (inline code).",
                example_config=f"""write_target:
  type: sink
  sink_type: foreachbatch
  sink_name: "my_batch_sink"
  module_path: "batch_handlers/my_handler.py"  # Option 1
  # batch_handler: |                           # Option 2
  #   df.write.format("delta").save("/path")""",
            )

        # Apply substitutions to batch handler code (both inline and file-based)
        if batch_handler_code and context and "substitution_manager" in context:
            substitution_mgr = context["substitution_manager"]
            batch_handler_code = substitution_mgr._process_string(batch_handler_code)

            # Track secret references if they exist
            secret_refs = substitution_mgr.get_secret_references()
            if (
                "secret_references" in context
                and context["secret_references"] is not None
            ):
                context["secret_references"].update(secret_refs)

        # Extract source view (single source only)
        if not action.source:
            raise ErrorFormatter.missing_required_field(
                field_name="source",
                component_type="ForEachBatch sink write action",
                component_name=action.name,
                field_description="ForEachBatch sinks require a source view to read data from.",
                example_config=f"""actions:
  - name: {action.name}
    type: write
    source: v_data  # Required: source view name
    write_target:
      type: sink
      sink_type: foreachbatch
      sink_name: "my_batch_sink" """,
            )

        if not isinstance(action.source, str):
            raise ErrorFormatter.invalid_field_type(
                action_name=action.name,
                field_name="source",
                expected_type="a string (single view name)",
                actual_type=type(action.source).__name__,
                example="""source: v_data  # ForEachBatch sinks only support single source view""",
            )

        source_view = action.source

        # Get operational metadata configuration
        add_metadata, metadata_columns = self._get_operational_metadata(action, context)

        # Build comment
        comment = (
            sink_config.get("comment")
            or action.description
            or f"ForEachBatch sink: {action.name}"
        )

        # Build template context
        template_context = {
            "action_name": action.name,
            "sink_name": sink_name,
            "batch_handler_code": batch_handler_code,
            "source_view": source_view,
            "comment": comment,
            "description": action.description or f"ForEachBatch sink: {action.name}",
            "add_operational_metadata": add_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
        }

        return self.render_template(
            "write/sinks/foreachbatch_sink.py.j2", template_context
        )

    def _load_batch_handler_from_file(
        self, module_path: str, action: Action, context: Dict[str, Any]
    ) -> str:
        """Load batch handler code from external file.

        Args:
            module_path: Path to Python file containing batch handler body
            action: Action configuration
            context: Context dictionary

        Returns:
            Batch handler code (function body only)

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        project_root = context.get("spec_dir") or Path.cwd()
        handler_path = project_root / module_path

        if not handler_path.exists():
            raise ErrorFormatter.file_not_found(
                file_path=str(handler_path),
                search_locations=[
                    f"Relative to project root: {project_root / module_path}",
                ],
                file_type="ForEachBatch batch handler file",
            )

        return handler_path.read_text()
