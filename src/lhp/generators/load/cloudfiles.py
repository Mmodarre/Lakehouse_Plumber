"""CloudFiles load generator - adapted from BurrowBuilder."""

import logging
from pathlib import Path
from typing import Dict, Any, List, Tuple
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.schema_parser import SchemaParser
from ...utils.operational_metadata import OperationalMetadata
from ...utils.error_formatter import ErrorFormatter, LHPError


class CloudFilesLoadGenerator(BaseActionGenerator):
    """Generate CloudFiles (Auto Loader) load actions."""

    def __init__(self):
        super().__init__()
        self.add_import("import dlt")
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
        """Generate CloudFiles load code."""
        source_config = action.source if isinstance(action.source, dict) else {}

        # CloudFiles requires stream mode
        readMode = action.readMode or source_config.get("readMode", "stream")
        if readMode != "stream":
            raise ValueError(
                f"CloudFiles action '{action.name}' requires readMode='stream', got '{readMode}'"
            )

        # Extract configuration
        path = source_config.get("path")
        file_format = source_config.get("format", "json")

        # Check for conflicts between old and new approaches
        self._check_conflicts(source_config, action.name)

        # Handle schema processing
        schema_code_lines = []
        schema_variable = None
        schema_hints_value = None
        explicit_schema = source_config.get("schema")

        # Process explicit schema field
        if explicit_schema:
            if isinstance(explicit_schema, str):
                # Schema file path
                schema_variable, schema_code_lines = self._process_schema_file(
                    explicit_schema, context.get("spec_dir")
                )
            elif isinstance(explicit_schema, dict) and "file" in explicit_schema:
                # Schema object with file
                schema_variable, schema_code_lines = self._process_schema_file(
                    explicit_schema["file"], context.get("spec_dir")
                )

        # Process options (new approach)
        reader_options = {}
        if source_config.get("options"):
            reader_options.update(
                self._process_options(
                    source_config["options"], action.name, context.get("spec_dir")
                )
            )

            # Extract schema hints if present in options
            if "cloudFiles.schemaHints" in reader_options:
                schema_hints_value = reader_options["cloudFiles.schemaHints"]

        # Process legacy options (old approach)
        if source_config.get("reader_options"):
            reader_options.update(source_config["reader_options"])
        if source_config.get("format_options"):
            for key, value in source_config["format_options"].items():
                if not key.startswith(f"{file_format}."):
                    key = f"{file_format}.{key}"
                reader_options[key] = value

        # Handle legacy schema_file
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

        # Validate mandatory options
        self._validate_mandatory_options(reader_options, file_format)

        # Process schema hints for better formatting
        schema_hints_variable = None
        schema_hints_lines = []
        if "cloudFiles.schemaHints" in reader_options:
            schema_hints_variable, schema_hints_lines = (
                self._create_schema_hints_variable(
                    reader_options["cloudFiles.schemaHints"], action.target
                )
            )
            # Remove from reader_options since we'll use the variable instead
            del reader_options["cloudFiles.schemaHints"]

        # Handle operational metadata
        flowgroup = context.get("flowgroup")
        preset_config = context.get("preset_config", {})
        project_config = context.get("project_config")

        # Initialize operational metadata handler
        operational_metadata = OperationalMetadata(
            project_config=(
                project_config.operational_metadata if project_config else None
            )
        )

        # Update context for substitutions
        if flowgroup:
            operational_metadata.update_context(flowgroup.pipeline, flowgroup.flowgroup)

        # Resolve metadata selection
        selection = operational_metadata.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        metadata_columns = operational_metadata.get_selected_columns(
            selection or {}, "view"
        )

        # Get required imports for metadata
        metadata_imports = operational_metadata.get_required_imports(metadata_columns)
        for import_stmt in metadata_imports:
            self.add_import(import_stmt)

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "path": path,
            "format": file_format,
            "readMode": readMode,
            "reader_options": reader_options,
            "schema_code_lines": schema_code_lines,
            "schema_variable": schema_variable,
            "schema_hints_variable": schema_hints_variable,
            "schema_hints_lines": schema_hints_lines,
            "description": action.description
            or f"Load data from {format} files at {path}",
            "add_operational_metadata": bool(metadata_columns),
            "metadata_columns": metadata_columns,
            "flowgroup": flowgroup,
        }

        return self.render_template("load/cloudfiles.py.j2", template_context)

    def _process_options(
        self, options: Dict[str, Any], action_name: str, spec_dir: Path = None
    ) -> Dict[str, Any]:
        """Process the options field and validate cloudFiles options.

        Args:
            options: Options dictionary from YAML
            action_name: Name of the action for error messages
            spec_dir: Base directory for relative paths

        Returns:
            Processed options dictionary
        """
        processed_options = {}

        for key, value in options.items():
            # Check if this looks like a cloudFiles option without prefix
            if (
                not key.startswith("cloudFiles.")
                and key in self.known_cloudfiles_options
            ):
                raise ErrorFormatter.configuration_conflict(
                    action_name=action_name,
                    field_pairs=[(key, f"cloudFiles.{key}")],
                    preset_name=None,
                )

            # Handle schema hints specially
            if key == "cloudFiles.schemaHints":
                if isinstance(value, str):
                    # Check if it's a file path (contains .yaml, .json, .yml, or path separators)
                    if (
                        ".yaml" in value.lower()
                        or ".yml" in value.lower()
                        or ".json" in value.lower()
                        or "/" in value
                        or "\\" in value
                    ):
                        # User provided schema file path
                        try:
                            schema_data = self.schema_parser.parse_schema_file(
                                Path(value), spec_dir
                            )
                            processed_options[key] = self.schema_parser.to_schema_hints(
                                schema_data
                            )
                        except FileNotFoundError as exc:
                            # Build search locations
                            search_locations = []
                            if value.startswith("/"):
                                search_locations.append(f"Absolute path: {value}")
                            else:
                                search_locations.append(
                                    f"Relative to YAML: {spec_dir / value}"
                                )
                                search_locations.append(
                                    f"Project root: {Path.cwd() / value}"
                                )

                            raise ErrorFormatter.file_not_found(
                                file_path=str(value),
                                search_locations=search_locations,
                                file_type="schema file",
                            ) from exc
                    else:
                        # User provided direct hints string
                        processed_options[key] = str(value)
                else:
                    # User provided direct hints string
                    processed_options[key] = str(value)
            else:
                # Preserve original type for all other options
                processed_options[key] = value

        return processed_options

    def _process_schema_file(
        self, schema_file_path: str, spec_dir: Path = None
    ) -> Tuple[str, List[str]]:
        """Process a schema file and generate StructType code.

        Args:
            schema_file_path: Path to schema file
            spec_dir: Base directory for relative paths

        Returns:
            Tuple of (variable_name, code_lines)
        """
        try:
            schema_data = self.schema_parser.parse_schema_file(
                Path(schema_file_path), spec_dir
            )

            # Validate schema
            errors = self.schema_parser.validate_schema(schema_data)
            if errors:
                raise ValueError(f"Schema validation failed: {'; '.join(errors)}")

            variable_name, code_lines = self.schema_parser.to_struct_type_code(
                schema_data
            )

            # Add the imports to the generator
            for line in code_lines:
                if line.startswith("from pyspark.sql.types import"):
                    self.add_import(line)
                    break

            # Return variable name and the schema definition lines (excluding imports)
            schema_def_lines = [
                line
                for line in code_lines
                if not line.startswith("from pyspark.sql.types import") and line.strip()
            ]

            return variable_name, schema_def_lines

        except FileNotFoundError as exc:
            # Build search locations
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

            raise ErrorFormatter.file_not_found(
                file_path=str(schema_file_path),
                search_locations=search_locations,
                file_type="schema file",
            ) from exc
        except LHPError:
            # Re-raise LHPError as-is (it's already well-formatted)
            raise
        except Exception as e:
            raise ValueError(f"Error processing schema file '{schema_file_path}': {e}")

    def _check_conflicts(self, source_config: Dict[str, Any], action_name: str):
        """Check for conflicts between old and new configuration approaches.

        Args:
            source_config: Source configuration dictionary
            action_name: Name of the action for error messages
        """
        options = source_config.get("options", {})

        # Check for conflicts with legacy options
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

        # Check schema conflicts
        schema_sources = []
        if source_config.get("schema_file"):
            schema_sources.append("schema_file")
        if source_config.get("schema"):
            schema_sources.append("schema")
        if "cloudFiles.schemaHints" in options:
            schema_sources.append("options.cloudFiles.schemaHints")

        if len(schema_sources) > 1:
            conflicts.append(
                f"Multiple schema sources specified: {', '.join(schema_sources)}"
            )

        if conflicts:
            # Extract field pairs from conflicts for better error formatting
            field_pairs = []
            for legacy_key, new_key in legacy_to_new.items():
                if source_config.get(legacy_key) is not None and new_key in options:
                    field_pairs.append((legacy_key, new_key))

            # Check for preset name in the source config
            preset_name = source_config.get("preset")

            raise ErrorFormatter.configuration_conflict(
                action_name=action_name,
                field_pairs=field_pairs,
                preset_name=preset_name,
            )

    def _create_schema_hints_variable(
        self, schema_hints: str, target_view: str
    ) -> Tuple[str, List[str]]:
        """Create a formatted schema hints variable for better readability.

        Args:
            schema_hints: The schema hints string (e.g., "col1 TYPE1, col2 TYPE2, ...")
            target_view: The target view name to use for variable naming

        Returns:
            Tuple of (variable_name, code_lines)
        """
        # Create variable name based on target view
        clean_target = target_view.replace("v_", "").replace("_raw", "")
        variable_name = f"{clean_target}_schema_hints"

        # Split schema hints by comma, but be careful with types like DECIMAL(18,2)
        # Use a more sophisticated parsing that respects parentheses
        columns = []
        current_col = ""
        paren_count = 0

        for char in schema_hints:
            if char == "(":
                paren_count += 1
            elif char == ")":
                paren_count -= 1
            elif char == "," and paren_count == 0:
                # Only split on comma if we're not inside parentheses
                if current_col.strip():
                    columns.append(current_col.strip())
                current_col = ""
                continue

            current_col += char

        # Don't forget the last column
        if current_col.strip():
            columns.append(current_col.strip())

        # Format each column with proper indentation
        code_lines = [
            f"# Schema hints for {clean_target} table",
            f'{variable_name} = """',
        ]

        # Add each column on its own line with indentation
        for i, column in enumerate(columns):
            if i == len(columns) - 1:  # Last column doesn't need comma
                code_lines.append(f"    {column}")
            else:
                code_lines.append(f"    {column},")

        code_lines.extend(
            ['""".strip().replace("\\n", " ")', ""]  # Empty line for separation
        )

        return variable_name, code_lines

    def _validate_mandatory_options(
        self, reader_options: Dict[str, Any], file_format: str
    ):
        """Validate that mandatory cloudFiles options are present.

        Args:
            reader_options: Combined reader options
            file_format: File format
        """
        # Check for mandatory cloudFiles.format
        if "cloudFiles.format" not in reader_options:
            reader_options["cloudFiles.format"] = file_format

        # Additional validation can be added here for other mandatory options
