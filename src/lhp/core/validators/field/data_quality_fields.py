"""Reject data-quality fields on actions that are not data_quality transforms.

Only a transform with ``transform_type: data_quality`` reads the action-level
``expectations_file``, ``quarantine`` and ``mode`` fields; any other action
would silently ignore them. :func:`check_data_quality_fields` raises
``LHP-CFG-073`` instead, from the same action-field check that raises
``LHP-CFG-001`` for an unknown field.
"""

import logging
from typing import Any, Dict

from ....errors import ErrorFactory, codes

logger = logging.getLogger(__name__)

#: The action-level fields only a data_quality transform reads.
DATA_QUALITY_FIELDS = ("expectations_file", "quarantine", "mode")

_READ_MODES = ("stream", "batch")


def check_data_quality_fields(action_dict: Dict[str, Any], action_name: str) -> None:
    """Raise ``LHP-CFG-073`` when a non-data_quality action sets a data-quality field.

    ``action_dict`` is the action as a plain mapping (``Action.model_dump()``).
    A field counts as set when its value is not ``None``.
    """
    action_type = _plain(action_dict.get("type"))
    transform_type = _plain(action_dict.get("transform_type"))
    if action_type == "transform" and transform_type == "data_quality":
        return

    misplaced = [f for f in DATA_QUALITY_FIELDS if action_dict.get(f) is not None]
    if not misplaced:
        return

    kind = (
        f"transform (transform_type '{transform_type}')"
        if action_type == "transform"
        else str(action_type)
    )
    logger.debug(f"Data-quality fields {misplaced} on {kind} action '{action_name}'")
    quoted = ", ".join(f"'{f}'" for f in misplaced)
    many = len(misplaced) > 1
    raise ErrorFactory.config_error(
        codes.CFG_073,
        title=f"Data-quality field{'s' if many else ''} on a {kind} action: {quoted}",
        details=(
            f"Action '{action_name}' is a {kind} action but sets {quoted}. Only a "
            "transform with transform_type 'data_quality' reads "
            f"{'these fields' if many else 'this field'}, so this action would "
            f"silently ignore {'them' if many else 'it'}."
        ),
        suggestions=[
            _hint(field, action_dict.get(field), action_type) for field in misplaced
        ],
        example=(
            "- name: check_quality\n"
            "  type: transform\n"
            "  transform_type: data_quality\n"
            "  source: v_raw\n"
            "  target: v_checked\n"
            "  expectations_file: expectations/quality.yaml"
        ),
        context={"Action": action_name, "Action Type": kind, "Fields": misplaced},
    )


def _hint(field: str, value: Any, action_type: Any) -> str:
    if field == "expectations_file":
        if action_type == "write":
            return (
                "Move 'expectations_file' to write_target.expectations_file "
                "(materialized_view writes)"
            )
        return "Put 'expectations_file' on a data_quality transform"
    if field == "quarantine":
        return "Put 'quarantine' on a data_quality transform with mode: quarantine"
    if isinstance(value, str) and value.lower() in _READ_MODES:
        return "Use 'readMode' to choose a stream or batch read"
    if action_type == "write":
        return "Set the streaming-table mode as write_target.mode"
    return "'mode' (dqe or quarantine) only applies to a data_quality transform"


def _plain(value: Any) -> Any:
    """``ActionType.WRITE`` -> ``"write"``; plain strings pass through."""
    return getattr(value, "value", value)
