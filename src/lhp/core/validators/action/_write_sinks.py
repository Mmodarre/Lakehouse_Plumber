"""Sink validation for write actions.

Module-level functions split out of :class:`WriteActionValidator` to
keep that class small (constitution §3.3).
"""

from typing import List

from lhp.models import Action

from ._name_checks import require_three_part_name


def validate_sink(action: Action, prefix: str) -> List[str]:
    errors = []
    sink_config = action.write_target

    if not sink_config.get("sink_type"):
        errors.append(f"{prefix}: Sink must have 'sink_type'")
        return errors

    if not sink_config.get("sink_name"):
        errors.append(f"{prefix}: Sink must have 'sink_name'")

    if not action.source:
        errors.append(f"{prefix}: Sink must have 'source' to read from")
    elif not isinstance(action.source, (str, list)):
        errors.append(f"{prefix}: Sink source must be a string or list of view names")

    sink_type = sink_config["sink_type"]

    if sink_type == "delta":
        errors.extend(validate_delta_sink(action, prefix))
    elif sink_type == "kafka":
        errors.extend(validate_kafka_sink(action, prefix))
    elif sink_type == "custom":
        errors.extend(validate_custom_sink(action, prefix))
    elif sink_type == "foreachbatch":
        errors.extend(validate_foreachbatch_sink(action, prefix))
    else:
        errors.append(f"{prefix}: Unknown sink_type '{sink_type}'")

    return errors


def validate_delta_sink(action: Action, prefix: str) -> List[str]:
    """Delta sinks require either 'tableName' OR 'path' (not both)."""
    errors = []
    sink_config = action.write_target

    if not sink_config.get("options"):
        errors.append(
            f"{prefix}: Delta sink requires 'options' with either 'tableName' or 'path'"
        )
        return errors

    options = sink_config["options"]
    has_table_name = "tableName" in options
    has_path = "path" in options

    if not has_table_name and not has_path:
        errors.append(
            f"{prefix}: Delta sink options must include either 'tableName' or 'path'"
        )
    elif has_table_name and has_path:
        errors.append(
            f"{prefix}: Delta sink options cannot have both 'tableName' and 'path'. Use one or the other."
        )

    if has_table_name:
        table_name_val = options["tableName"]
        err = require_three_part_name(table_name_val, "Delta sink 'tableName'", prefix)
        if err:
            errors.append(err)

    # Other options are intentionally passed through unvalidated.

    return errors


def validate_kafka_sink(action: Action, prefix: str) -> List[str]:
    errors = []
    sink_config = action.write_target

    if not sink_config.get("bootstrap_servers"):
        errors.append(f"{prefix}: Kafka sink must have 'bootstrap_servers'")

    if not sink_config.get("topic"):
        errors.append(f"{prefix}: Kafka sink must have 'topic'")

    if sink_config.get("options"):
        try:
            from ..field.kafka_options import KafkaOptionsValidator

            validator = KafkaOptionsValidator()
            validator.process_options(
                sink_config["options"], action.name, is_source=False
            )
        except Exception as e:
            errors.append(f"{prefix}: {str(e)}")

    return errors


def validate_custom_sink(action: Action, prefix: str) -> List[str]:
    errors = []
    sink_config = action.write_target

    if not sink_config.get("module_path"):
        errors.append(f"{prefix}: Custom sink must have 'module_path'")

    if not sink_config.get("custom_sink_class"):
        errors.append(f"{prefix}: Custom sink must have 'custom_sink_class'")

    return errors


def validate_foreachbatch_sink(action: Action, prefix: str) -> List[str]:
    errors = []
    sink_config = action.write_target

    if action.source and not isinstance(action.source, str):
        errors.append(
            f"{prefix}: ForEachBatch sink only supports single source view (string), not list or dict"
        )

    has_module_path = bool(sink_config.get("module_path"))
    has_batch_handler = bool(sink_config.get("batch_handler"))

    if has_module_path and has_batch_handler:
        errors.append(
            f"{prefix}: ForEachBatch sink must have either 'module_path' or 'batch_handler', not both"
        )
    elif not has_module_path and not has_batch_handler:
        errors.append(
            f"{prefix}: ForEachBatch sink must have either 'module_path' or 'batch_handler'"
        )

    if has_batch_handler:
        batch_handler = sink_config.get("batch_handler", "").strip()
        if not batch_handler:
            errors.append(
                f"{prefix}: ForEachBatch sink 'batch_handler' cannot be empty"
            )

    return errors
