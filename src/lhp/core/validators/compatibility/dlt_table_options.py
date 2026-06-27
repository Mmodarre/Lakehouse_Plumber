import logging
from typing import List

from lhp.models import Action

logger = logging.getLogger(__name__)


class DltTableOptionsValidator:
    def validate(self, action: Action, prefix: str) -> List[str]:
        logger.debug(f"Validating DLT table options for {prefix}")
        errors = []

        if not action.write_target:
            return errors

        errors.extend(self._validate_spark_conf(action, prefix))
        errors.extend(self._validate_table_properties(action, prefix))
        errors.extend(self._validate_tags(action, prefix))
        errors.extend(self._validate_schema_options(action, prefix))
        errors.extend(self._validate_column_options(action, prefix))
        errors.extend(self._validate_refresh_options(action, prefix))

        return errors

    def _validate_spark_conf(self, action: Action, prefix: str) -> List[str]:
        errors = []
        spark_conf = action.write_target.get("spark_conf")

        if spark_conf is not None:
            if not isinstance(spark_conf, dict):
                errors.append(f"{prefix}: 'spark_conf' must be a dictionary")
            else:
                for key, _value in spark_conf.items():
                    if not isinstance(key, str):
                        errors.append(
                            f"{prefix}: spark_conf key '{key}' must be a string"
                        )

        return errors

    def _validate_table_properties(self, action: Action, prefix: str) -> List[str]:
        errors = []
        table_properties = action.write_target.get("table_properties")

        if table_properties is not None:
            if not isinstance(table_properties, dict):
                errors.append(f"{prefix}: 'table_properties' must be a dictionary")
            else:
                for key, _value in table_properties.items():
                    if not isinstance(key, str):
                        errors.append(
                            f"{prefix}: table_properties key '{key}' must be a string"
                        )

        return errors

    def _validate_tags(self, action: Action, prefix: str) -> List[str]:
        """Validate Unity Catalog ``tags``.

        ``tags`` absent (None) means the entity is unmanaged; an explicit empty
        ``{}`` is a valid managed-with-empty-set signal — both pass here. Keys
        must be strings; values must be strings or None (key-only tags).
        """
        errors = []
        tags = action.write_target.get("tags")

        if tags is not None:
            if not isinstance(tags, dict):
                errors.append(f"{prefix}: 'tags' must be a dictionary")
            else:
                for key, value in tags.items():
                    if not isinstance(key, str):
                        errors.append(f"{prefix}: tags key '{key}' must be a string")
                    elif not key.strip():
                        errors.append(f"{prefix}: tags key must not be empty")
                    if value is not None and not isinstance(value, (str, int, float, bool)):
                        errors.append(
                            f"{prefix}: tags value for '{key}' must be a scalar or null"
                        )

        return errors

    def _validate_schema_options(self, action: Action, prefix: str) -> List[str]:
        errors = []

        schema = action.write_target.get("table_schema")
        if schema is not None:
            if not isinstance(schema, str):
                errors.append(
                    f"{prefix}: 'table_schema' must be a string (SQL DDL or StructType)"
                )

        row_filter = action.write_target.get("row_filter")
        if row_filter is not None:
            if not isinstance(row_filter, str):
                errors.append(f"{prefix}: 'row_filter' must be a string")

        temporary = action.write_target.get("temporary")
        if temporary is not None:
            if not isinstance(temporary, bool):
                errors.append(f"{prefix}: 'temporary' must be a boolean")

        return errors

    def _validate_column_options(self, action: Action, prefix: str) -> List[str]:
        errors = []

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

        cluster_by_auto = action.write_target.get("cluster_by_auto")
        if cluster_by_auto is not None:
            if not isinstance(cluster_by_auto, bool):
                errors.append(f"{prefix}: 'cluster_by_auto' must be a boolean")

        if cluster_columns and cluster_by_auto is True:
            errors.append(
                f"{prefix}: 'cluster_columns' and 'cluster_by_auto' are mutually exclusive"
            )

        return errors

    def _validate_refresh_options(self, action: Action, prefix: str) -> List[str]:
        errors = []

        valid_policies = ["auto", "incremental", "incremental_strict", "full"]
        refresh_policy = action.write_target.get("refresh_policy")
        if refresh_policy is not None:
            if not isinstance(refresh_policy, str):
                errors.append(f"{prefix}: 'refresh_policy' must be a string")
            elif refresh_policy not in valid_policies:
                errors.append(
                    f"{prefix}: Invalid refresh_policy '{refresh_policy}'. "
                    f"Valid values are: {', '.join(valid_policies)}"
                )

        return errors
