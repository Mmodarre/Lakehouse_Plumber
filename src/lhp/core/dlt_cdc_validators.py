"""
Specialized validators for DLT table options and CDC configurations.
"""

from typing import List, Dict, Any
from ..models.config import Action


class DltTableOptionsValidator:
    """Validator for DLT table options (spark_conf, table_properties, schema, etc.)."""

    def validate(self, action: Action, prefix: str) -> List[str]:
        """Validate DLT table options."""
        errors = []

        if not action.write_target:
            return errors

        errors.extend(self._validate_spark_conf(action, prefix))
        errors.extend(self._validate_table_properties(action, prefix))
        errors.extend(self._validate_schema_options(action, prefix))
        errors.extend(self._validate_column_options(action, prefix))

        return errors

    def _validate_spark_conf(self, action: Action, prefix: str) -> List[str]:
        """Validate spark_conf configuration."""
        errors = []
        spark_conf = action.write_target.get("spark_conf")

        if spark_conf is not None:
            if not isinstance(spark_conf, dict):
                errors.append(f"{prefix}: 'spark_conf' must be a dictionary")
            else:
                # Validate spark_conf keys (should be strings)
                for key, value in spark_conf.items():
                    if not isinstance(key, str):
                        errors.append(
                            f"{prefix}: spark_conf key '{key}' must be a string"
                        )

        return errors

    def _validate_table_properties(self, action: Action, prefix: str) -> List[str]:
        """Validate table_properties configuration."""
        errors = []
        table_properties = action.write_target.get("table_properties")

        if table_properties is not None:
            if not isinstance(table_properties, dict):
                errors.append(f"{prefix}: 'table_properties' must be a dictionary")
            else:
                # Validate table_properties keys (should be strings)
                for key, value in table_properties.items():
                    if not isinstance(key, str):
                        errors.append(
                            f"{prefix}: table_properties key '{key}' must be a string"
                        )

        return errors

    def _validate_schema_options(self, action: Action, prefix: str) -> List[str]:
        """Validate schema-related options."""
        errors = []

        # Validate schema
        schema = action.write_target.get("table_schema") or action.write_target.get(
            "schema"
        )
        if schema is not None:
            if not isinstance(schema, str):
                errors.append(
                    f"{prefix}: 'table_schema' (or 'schema') must be a string (SQL DDL or StructType)"
                )

        # Validate row_filter
        row_filter = action.write_target.get("row_filter")
        if row_filter is not None:
            if not isinstance(row_filter, str):
                errors.append(f"{prefix}: 'row_filter' must be a string")

        # Validate temporary
        temporary = action.write_target.get("temporary")
        if temporary is not None:
            if not isinstance(temporary, bool):
                errors.append(f"{prefix}: 'temporary' must be a boolean")

        return errors

    def _validate_column_options(self, action: Action, prefix: str) -> List[str]:
        """Validate column-related options."""
        errors = []

        # Validate partition_columns
        partition_columns = action.write_target.get("partition_columns")
        if partition_columns is not None:
            if not isinstance(partition_columns, list):
                errors.append(f"{prefix}: 'partition_columns' must be a list")
            else:
                for i, col in enumerate(partition_columns):
                    if not isinstance(col, str):
                        errors.append(
                            f"{prefix}: partition_columns[{i}] must be a string"
                        )

        # Validate cluster_columns
        cluster_columns = action.write_target.get("cluster_columns")
        if cluster_columns is not None:
            if not isinstance(cluster_columns, list):
                errors.append(f"{prefix}: 'cluster_columns' must be a list")
            else:
                for i, col in enumerate(cluster_columns):
                    if not isinstance(col, str):
                        errors.append(
                            f"{prefix}: cluster_columns[{i}] must be a string"
                        )

        return errors


class CdcConfigValidator:
    """Validator for CDC configuration parameters."""

    def validate(self, action: Action, prefix: str) -> List[str]:
        """Validate CDC configuration."""
        errors = []

        if not action.write_target:
            return errors

        cdc_config = action.write_target.get("cdc_config")
        if not cdc_config:
            errors.append(f"{prefix}: cdc mode requires 'cdc_config'")
            return errors

        if not isinstance(cdc_config, dict):
            errors.append(f"{prefix}: 'cdc_config' must be a dictionary")
            return errors

        errors.extend(self._validate_required_fields(cdc_config, prefix))
        errors.extend(self._validate_sequence_options(cdc_config, prefix))
        errors.extend(self._validate_scd_options(cdc_config, prefix))
        errors.extend(self._validate_column_lists(cdc_config, prefix))
        errors.extend(self._validate_other_options(cdc_config, prefix))

        return errors

    def _validate_required_fields(
        self, cdc_config: Dict[str, Any], prefix: str
    ) -> List[str]:
        """Validate required CDC fields."""
        errors = []

        # Validate required keys parameter
        keys = cdc_config.get("keys")
        if not keys:
            errors.append(f"{prefix}: cdc_config must have 'keys'")
        elif not isinstance(keys, list):
            errors.append(f"{prefix}: 'keys' must be a list")
        elif not keys:  # Empty list
            errors.append(f"{prefix}: 'keys' cannot be empty")
        else:
            for i, key in enumerate(keys):
                if not isinstance(key, str):
                    errors.append(f"{prefix}: keys[{i}] must be a string")

        return errors

    def _validate_sequence_options(
        self, cdc_config: Dict[str, Any], prefix: str
    ) -> List[str]:
        """Validate sequence_by and related options."""
        errors = []

        # Validate sequence_by (optional but recommended)
        sequence_by = cdc_config.get("sequence_by")
        if sequence_by is not None:
            if isinstance(sequence_by, str):
                # String format is valid
                pass
            elif isinstance(sequence_by, list):
                # List format for struct() is valid
                if not sequence_by:  # Empty list
                    errors.append(f"{prefix}: 'sequence_by' list cannot be empty")
                else:
                    for i, col in enumerate(sequence_by):
                        if not isinstance(col, str):
                            errors.append(
                                f"{prefix}: sequence_by[{i}] must be a string"
                            )
            else:
                errors.append(
                    f"{prefix}: 'sequence_by' must be a string or list of strings"
                )

        return errors

    def _validate_scd_options(
        self, cdc_config: Dict[str, Any], prefix: str
    ) -> List[str]:
        """Validate SCD-related options."""
        errors = []

        # Validate stored_as_scd_type
        scd_type = cdc_config.get("scd_type")
        if scd_type is not None:
            if not isinstance(scd_type, int) or scd_type not in [1, 2]:
                errors.append(f"{prefix}: 'scd_type' must be 1 or 2")

        # Validate ignore_null_updates
        ignore_null_updates = cdc_config.get("ignore_null_updates")
        if ignore_null_updates is not None:
            if not isinstance(ignore_null_updates, bool):
                errors.append(f"{prefix}: 'ignore_null_updates' must be a boolean")

        # Validate apply_as_deletes
        apply_as_deletes = cdc_config.get("apply_as_deletes")
        if apply_as_deletes is not None:
            if not isinstance(apply_as_deletes, str):
                errors.append(
                    f"{prefix}: 'apply_as_deletes' must be a string expression"
                )

        # Validate apply_as_truncates
        apply_as_truncates = cdc_config.get("apply_as_truncates")
        if apply_as_truncates is not None:
            if not isinstance(apply_as_truncates, str):
                errors.append(
                    f"{prefix}: 'apply_as_truncates' must be a string expression"
                )
            # Validate that it's only used with SCD Type 1
            if cdc_config.get("scd_type") == 2:
                errors.append(
                    f"{prefix}: 'apply_as_truncates' is not supported with SCD Type 2"
                )

        # Validate track_history_columns for SCD Type 2
        track_history_columns = cdc_config.get("track_history_columns")
        if track_history_columns is not None:
            if not isinstance(track_history_columns, list):
                errors.append(f"{prefix}: 'track_history_columns' must be a list")
            else:
                for i, col in enumerate(track_history_columns):
                    if not isinstance(col, str):
                        errors.append(
                            f"{prefix}: track_history_columns[{i}] must be a string"
                        )

        return errors

    def _validate_column_lists(
        self, cdc_config: Dict[str, Any], prefix: str
    ) -> List[str]:
        """Validate column list options."""
        errors = []

        # Validate column_list and except_column_list (mutually exclusive)
        has_column_list = cdc_config.get("column_list") is not None
        has_except_column_list = cdc_config.get("except_column_list") is not None

        if has_column_list and has_except_column_list:
            errors.append(
                f"{prefix}: cannot have both 'column_list' and 'except_column_list'"
            )

        # Validate column_list
        if has_column_list:
            column_list = cdc_config["column_list"]
            if not isinstance(column_list, list):
                errors.append(f"{prefix}: 'column_list' must be a list")
            else:
                for i, col in enumerate(column_list):
                    if not isinstance(col, str):
                        errors.append(f"{prefix}: column_list[{i}] must be a string")

        # Validate except_column_list
        if has_except_column_list:
            except_column_list = cdc_config["except_column_list"]
            if not isinstance(except_column_list, list):
                errors.append(f"{prefix}: 'except_column_list' must be a list")
            else:
                for i, col in enumerate(except_column_list):
                    if not isinstance(col, str):
                        errors.append(
                            f"{prefix}: except_column_list[{i}] must be a string"
                        )

        return errors

    def _validate_other_options(
        self, cdc_config: Dict[str, Any], prefix: str
    ) -> List[str]:
        """Validate other CDC options."""
        # This method can be extended for additional CDC validations
        return []


class SnapshotCdcConfigValidator:
    """Validator for snapshot CDC configuration."""

    def validate(self, action: Action, prefix: str) -> List[str]:
        """Validate snapshot CDC configuration."""
        errors = []

        if not action.write_target:
            return errors

        snapshot_cdc_config = action.write_target.get("snapshot_cdc_config")
        if not snapshot_cdc_config:
            errors.append(f"{prefix}: snapshot_cdc mode requires 'snapshot_cdc_config'")
            return errors

        if not isinstance(snapshot_cdc_config, dict):
            errors.append(f"{prefix}: 'snapshot_cdc_config' must be a dictionary")
            return errors

        errors.extend(self._validate_source_configuration(snapshot_cdc_config, prefix))
        errors.extend(self._validate_keys_configuration(snapshot_cdc_config, prefix))
        errors.extend(self._validate_scd_configuration(snapshot_cdc_config, prefix))
        errors.extend(
            self._validate_track_history_configuration(snapshot_cdc_config, prefix)
        )

        return errors

    def _validate_source_configuration(
        self, config: Dict[str, Any], prefix: str
    ) -> List[str]:
        """Validate source configuration (mutually exclusive)."""
        errors = []

        has_source = config.get("source") is not None
        has_source_function = config.get("source_function") is not None

        if not has_source and not has_source_function:
            errors.append(
                f"{prefix}: snapshot_cdc_config must have either 'source' or 'source_function'"
            )
        elif has_source and has_source_function:
            errors.append(
                f"{prefix}: snapshot_cdc_config cannot have both 'source' and 'source_function'"
            )

        # Validate source_function if provided
        if has_source_function:
            source_function = config["source_function"]
            if not isinstance(source_function, dict):
                errors.append(f"{prefix}: 'source_function' must be a dictionary")
            else:
                if not source_function.get("file"):
                    errors.append(f"{prefix}: source_function must have 'file'")
                if not source_function.get("function"):
                    errors.append(f"{prefix}: source_function must have 'function'")

        return errors

    def _validate_keys_configuration(
        self, config: Dict[str, Any], prefix: str
    ) -> List[str]:
        """Validate keys configuration."""
        errors = []

        keys = config.get("keys")
        if not keys:
            errors.append(f"{prefix}: snapshot_cdc_config must have 'keys'")
        elif not isinstance(keys, list):
            errors.append(f"{prefix}: 'keys' must be a list")
        elif not keys:  # Empty list
            errors.append(f"{prefix}: 'keys' cannot be empty")
        else:
            for i, key in enumerate(keys):
                if not isinstance(key, str):
                    errors.append(f"{prefix}: keys[{i}] must be a string")

        return errors

    def _validate_scd_configuration(
        self, config: Dict[str, Any], prefix: str
    ) -> List[str]:
        """Validate SCD type configuration."""
        errors = []

        scd_type = config.get("stored_as_scd_type")
        if scd_type is not None:
            if not isinstance(scd_type, int) or scd_type not in [1, 2]:
                errors.append(f"{prefix}: 'stored_as_scd_type' must be 1 or 2")

        return errors

    def _validate_track_history_configuration(
        self, config: Dict[str, Any], prefix: str
    ) -> List[str]:
        """Validate track history configuration."""
        errors = []

        # Validate track history options (mutually exclusive)
        has_track_list = config.get("track_history_column_list") is not None
        has_track_except = config.get("track_history_except_column_list") is not None

        if has_track_list and has_track_except:
            errors.append(
                f"{prefix}: cannot have both 'track_history_column_list' and 'track_history_except_column_list'"
            )

        # Validate track_history_column_list
        if has_track_list:
            track_list = config["track_history_column_list"]
            if not isinstance(track_list, list):
                errors.append(f"{prefix}: 'track_history_column_list' must be a list")
            else:
                for i, col in enumerate(track_list):
                    if not isinstance(col, str):
                        errors.append(
                            f"{prefix}: track_history_column_list[{i}] must be a string"
                        )

        # Validate track_history_except_column_list
        if has_track_except:
            except_list = config["track_history_except_column_list"]
            if not isinstance(except_list, list):
                errors.append(
                    f"{prefix}: 'track_history_except_column_list' must be a list"
                )
            else:
                for i, col in enumerate(except_list):
                    if not isinstance(col, str):
                        errors.append(
                            f"{prefix}: track_history_except_column_list[{i}] must be a string"
                        )

        return errors


class CdcSchemaValidator:
    """Validator for CDC schema requirements."""

    def validate(self, action: Action, prefix: str) -> List[str]:
        """Validate CDC schema includes required __START_AT and __END_AT columns."""
        errors = []

        if not action.write_target:
            return errors

        schema = action.write_target.get("table_schema") or action.write_target.get(
            "schema"
        )
        if not schema:
            return errors

        # Check for required CDC columns
        if "__START_AT" not in schema:
            errors.append(
                f"{prefix}: CDC schema must include '__START_AT' column with same type as sequence_by"
            )

        if "__END_AT" not in schema:
            errors.append(
                f"{prefix}: CDC schema must include '__END_AT' column with same type as sequence_by"
            )

        # If we have sequence_by, we could validate type compatibility
        # but that would require parsing the schema DDL which is complex
        # For now, just check presence of columns

        return errors
