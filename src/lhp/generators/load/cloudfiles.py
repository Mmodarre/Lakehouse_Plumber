"""CloudFiles load generator"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

from lhp.errors import ErrorFactory, LHPError, codes
from lhp.models import Action

from ...core.codegen.struct_type_emitter import emit_struct_type_code
from ...core.loaders.external_file_loader import (
    is_file_path,
    load_external_file_text,
    resolve_external_file_path,
)
from ...core.registry import BaseActionGenerator
from ...parsers.schema_parser import SchemaParser


class CloudFilesLoadGenerator(BaseActionGenerator):
    """Generate CloudFiles (Auto Loader) load actions."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")
        self.logger = logging.getLogger(__name__)
        self.schema_parser = SchemaParser()

        # Known cloudFiles options that require cloudFiles. prefix
        self.known_cloudfiles_options = {
            "format",
            "schemaLocation",
            "inferColumnTypes",
            "maxFilesPerTrigger",
            "maxBytesPerTrigger",
            "schemaEvolutionMode",
            "rescueDataColumn",
            "includeExistingFiles",
            "partitionColumns",
            "schemaHints",
            "allowOverwrites",
            "backfillInterval",
            "cleanSource",
            "cleanSource.retentionDuration",
            "cleanSource.moveDestination",
            "maxFileAge",
            "useIncrementalListing",
            "fetchParallelism",
            "pathRewrites",
            "resourceTag",
            "useManagedFileEvents",
            "useNotifications",
            "validateOptions",
            "useStrictGlobber",
        }

        # Mandatory cloudFiles options that must be present
        self.mandatory_options = {"format"}

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        source_config = action.source if isinstance(action.source, dict) else {}
        self.logger.debug(
            f"Generating CloudFiles load for target '{action.target}', action '{action.name}'"
        )

        readMode = action.readMode or source_config.get("readMode", "stream")
        if readMode != "stream":
            raise ErrorFactory.invalid_read_mode(
                action_name=action.name,
                action_type="cloudfiles",
                provided=readMode,
                valid_modes=["stream"],
            )

        path = source_config.get("path")
        file_format = source_config.get("format", "json")

        self.logger.debug(
            f"CloudFiles load '{action.name}': format='{file_format}', path='{path}'"
        )

        self._check_conflicts(source_config, action.name)

        schema_code_lines = []
        schema_variable = None
        schema_hints_value = None
        explicit_schema = source_config.get("schema")

        if explicit_schema:
            if isinstance(explicit_schema, str):
                if is_file_path(explicit_schema):
                    # Schema file path
                    schema_variable, schema_code_lines = self._process_schema_file(
                        explicit_schema, context.get("spec_dir")
                    )
                else:
                    # Inline DDL string (e.g. injected from a resolved contract).
                    schema_variable, schema_code_lines = self._process_inline_ddl(
                        explicit_schema, action.target
                    )
            elif isinstance(explicit_schema, dict) and "file" in explicit_schema:
                # Schema object with file
                schema_variable, schema_code_lines = self._process_schema_file(
                    explicit_schema["file"], context.get("spec_dir")
                )

        reader_options = {}
        if source_config.get("options"):
            options = source_config["options"]
            if not isinstance(options, dict):
                raise ErrorFactory.invalid_field_type(
                    action_name=action.name,
                    field_name="options",
                    expected_type="a dictionary (mapping)",
                    actual_type=type(options).__name__,
                    example="""options:
  cloudFiles.format: "json"
  cloudFiles.schemaLocation: "/mnt/schema" """,
                )
            reader_options.update(
                self._process_options(options, action.name, context.get("spec_dir"))
            )

            if "cloudFiles.schemaHints" in reader_options:
                schema_hints_value = reader_options["cloudFiles.schemaHints"]

        if source_config.get("reader_options"):
            reader_options.update(source_config["reader_options"])
        if source_config.get("format_options"):
            for key, value in source_config["format_options"].items():
                if not key.startswith(f"{file_format}."):
                    key = f"{file_format}.{key}"
                reader_options[key] = value

        if (
            source_config.get("schema_file")
            and not explicit_schema
            and not schema_hints_value
        ):
            # Default to explicit schema for backward compatibility
            schema_variable, schema_file_lines = self._process_schema_file(
                source_config["schema_file"], context.get("spec_dir")
            )
            schema_code_lines.extend(schema_file_lines)

        # Add legacy individual options for backward compatibility
        legacy_mappings = {
            "schema_location": "cloudFiles.schemaLocation",
            "schema_infer_column_types": "cloudFiles.inferColumnTypes",
            "max_files_per_trigger": "cloudFiles.maxFilesPerTrigger",
            "schema_evolution_mode": "cloudFiles.schemaEvolutionMode",
            "rescue_data_column": "cloudFiles.rescueDataColumn",
        }

        for legacy_key, cloudfiles_option in legacy_mappings.items():
            if source_config.get(legacy_key) is not None:
                if (
                    cloudfiles_option not in reader_options
                ):  # Don't override new options
                    value = source_config[legacy_key]
                    reader_options[cloudfiles_option] = (
                        str(value).lower() if isinstance(value, bool) else value
                    )

        self._validate_mandatory_options(reader_options, file_format)

        # Process schema hints into render data (template emits the code, §9.14)
        schema_hints_variable = None
        schema_hints_label = None
        schema_hints_columns = []
        if "cloudFiles.schemaHints" in reader_options:
            (
                schema_hints_variable,
                schema_hints_label,
                schema_hints_columns,
            ) = self._create_schema_hints_variable(
                reader_options["cloudFiles.schemaHints"], action.target
            )
            # Remove from reader_options since we'll use the variable instead
            del reader_options["cloudFiles.schemaHints"]

        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, context
        )

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "path": path,
            "readMode": readMode,
            "reader_options": reader_options,
            "schema_code_lines": schema_code_lines,
            "schema_variable": schema_variable,
            "schema_hints_variable": schema_hints_variable,
            "schema_hints_label": schema_hints_label,
            "schema_hints_columns": schema_hints_columns,
            "description": action.description
            or f"Load data from {file_format} files at {path}",
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
        }

        return self.render_template("load/cloudfiles.py.j2", template_context)

    def _process_options(
        self, options: Dict[str, Any], action_name: str, spec_dir: Path | None = None
    ) -> Dict[str, Any]:
        """Process the options field and validate cloudFiles options."""
        processed_options = {}

        for key, value in options.items():
            if (
                not key.startswith("cloudFiles.")
                and key in self.known_cloudfiles_options
            ):
                raise ErrorFactory.configuration_conflict(
                    action_name=action_name,
                    field_pairs=[(key, f"cloudFiles.{key}")],
                    preset_name=None,
                )

            if key == "cloudFiles.schemaHints":
                if isinstance(value, str):
                    if is_file_path(value):
                        project_root = spec_dir or Path.cwd()
                        file_ext = Path(value).suffix.lower()
                        resolved_path = resolve_external_file_path(
                            value, project_root, file_type="schema file"
                        )

                        if file_ext in [".yaml", ".yml", ".json"]:
                            schema_data = self.schema_parser.parse_schema_file(
                                resolved_path
                            )
                            processed_options[key] = self.schema_parser.to_schema_hints(
                                schema_data
                            )
                        elif file_ext in [".ddl", ".sql"]:
                            ddl_content = load_external_file_text(
                                value, project_root, file_type="DDL schema file"
                            ).strip()
                            processed_options[key] = ddl_content
                        else:
                            schema_data = self.schema_parser.parse_schema_file(
                                resolved_path
                            )
                            processed_options[key] = self.schema_parser.to_schema_hints(
                                schema_data
                            )
                    else:
                        processed_options[key] = str(value)
                else:
                    processed_options[key] = str(value)
            else:
                processed_options[key] = value

        return processed_options

    def _process_schema_file(
        self, schema_file_path: str, spec_dir: Path | None = None
    ) -> Tuple[str, List[str]]:
        """Process a schema file and generate StructType code."""
        try:
            schema_data = self.schema_parser.parse_schema_file(
                Path(schema_file_path), spec_dir
            )

            errors = self.schema_parser.validate_schema(schema_data)
            if errors:
                raise ErrorFactory.validation_error(
                    codes.VAL_011,
                    title="Schema validation failed",
                    details=f"Schema file '{schema_file_path}' failed validation: {'; '.join(errors)}",
                    suggestions=[
                        "Check the schema file syntax and column definitions",
                        "Ensure all column types are valid Spark SQL types",
                    ],
                    context={
                        "Schema File": str(schema_file_path),
                        "Errors": "; ".join(errors),
                    },
                )

            variable_name, code_lines = emit_struct_type_code(schema_data)

            for line in code_lines:
                if line.startswith("from pyspark.sql.types import"):
                    self.add_import(line)
                    break

            schema_def_lines = [
                line
                for line in code_lines
                if not line.startswith("from pyspark.sql.types import") and line.strip()
            ]

            return variable_name, schema_def_lines

        except FileNotFoundError as exc:
            search_locations = []
            if schema_file_path.startswith("/"):
                search_locations.append(f"Absolute path: {schema_file_path}")
            else:
                search_locations.append(
                    f"Relative to YAML: {spec_dir / schema_file_path}"
                )
                search_locations.append(
                    f"Project root: {Path.cwd() / schema_file_path}"
                )

            raise ErrorFactory.file_not_found(
                file_path=str(schema_file_path),
                search_locations=search_locations,
                file_type="schema file",
            ) from exc
        except LHPError:
            # Re-raise LHPError as-is (it's already well-formatted)
            raise
        except Exception as e:
            raise ErrorFactory.validation_error(
                codes.VAL_011,
                title=f"Error processing schema file '{schema_file_path}'",
                details=f"Failed to process schema file '{schema_file_path}': {e}",
                suggestions=[
                    "Check the schema file format (YAML, JSON, or DDL)",
                    "Ensure the file contains valid schema definitions",
                ],
                context={"Schema File": str(schema_file_path)},
            ) from e

    def _process_inline_ddl(self, ddl: str, target_view: str) -> Tuple[str, List[str]]:
        """Emit a read schema from an inline DDL string.

        ``DataStreamReader.schema()`` / ``DataFrameReader.schema()`` accept a
        DDL-formatted string directly, so we hand the DDL (e.g.
        ``"order_id BIGINT NOT NULL, tags ARRAY<STRING>"``, as injected by the
        contract-resolution pass) straight to Spark rather than parsing it into a
        ``StructType`` ourselves. Spark's own parser then handles every type —
        including complex types like ``ARRAY<…>`` / ``STRUCT<…>`` and ``NOT NULL``
        constraints — with no type-mapping gaps.
        """
        clean_target = target_view.replace("v_", "").replace("_raw", "")
        variable_name = f"{clean_target}_schema"
        return variable_name, [f'{variable_name} = "{ddl}"']

    def _check_conflicts(self, source_config: Dict[str, Any], action_name: str):
        """Check for conflicts between old and new configuration approaches."""
        options = source_config.get("options", {})

        conflicts = []

        legacy_to_new = {
            "schema_location": "cloudFiles.schemaLocation",
            "schema_infer_column_types": "cloudFiles.inferColumnTypes",
            "max_files_per_trigger": "cloudFiles.maxFilesPerTrigger",
            "schema_evolution_mode": "cloudFiles.schemaEvolutionMode",
            "rescue_data_column": "cloudFiles.rescueDataColumn",
        }

        for legacy_key, new_key in legacy_to_new.items():
            if source_config.get(legacy_key) is not None and new_key in options:
                conflicts.append(f"Both '{legacy_key}' and '{new_key}' specified")

        # ``schema`` and ``schema_file`` are mutually-exclusive *read* schema
        # sources. ``cloudFiles.schemaHints`` is an Auto Loader inference hint,
        # not a read schema, so it may legitimately accompany an explicit
        # read schema (e.g. a resolved contract injects both).
        read_schema_sources = []
        if source_config.get("schema_file"):
            read_schema_sources.append("schema_file")
        if source_config.get("schema"):
            read_schema_sources.append("schema")

        if len(read_schema_sources) > 1:
            conflicts.append(
                f"Multiple schema sources specified: {', '.join(read_schema_sources)}"
            )

        if conflicts:
            field_pairs = []
            for legacy_key, new_key in legacy_to_new.items():
                if source_config.get(legacy_key) is not None and new_key in options:
                    field_pairs.append((legacy_key, new_key))

            preset_name = source_config.get("preset")

            raise ErrorFactory.configuration_conflict(
                action_name=action_name,
                field_pairs=field_pairs,
                preset_name=preset_name,
            )

    def _create_schema_hints_variable(
        self, schema_hints: str, target_view: str
    ) -> Tuple[str, str, List[str]]:
        """Parse schema hints into the data the template needs to render them.

        Code generation itself lives in ``templates/load/cloudfiles.py.j2`` per
        constitution §9.14 / §2.10 — this method only performs data
        transformation (paren-aware column parsing + variable naming).
        """
        clean_target = target_view.replace("v_", "").replace("_raw", "")
        variable_name = f"{clean_target}_schema_hints"

        # Split by comma, but respect parentheses — types like DECIMAL(18,2) contain commas
        columns = []
        current_col = ""
        paren_count = 0

        for char in schema_hints:
            if char == "(":
                paren_count += 1
            elif char == ")":
                paren_count -= 1
            elif char == "," and paren_count == 0:
                if current_col.strip():
                    columns.append(current_col.strip())
                current_col = ""
                continue

            current_col += char

        if current_col.strip():
            columns.append(current_col.strip())

        return variable_name, clean_target, columns

    def _validate_mandatory_options(
        self, reader_options: Dict[str, Any], file_format: str
    ):
        """Validate that mandatory cloudFiles options are present."""
        if "cloudFiles.format" not in reader_options:
            reader_options["cloudFiles.format"] = file_format
