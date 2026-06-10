"""Unknown-field error reporting for config validation.

The known-field catalogs these helpers reason about live in
``_field_catalog`` and are passed in via the ``expected_fields`` parameter.
"""

import logging
from typing import Optional, Set

from ....errors import ErrorFactory, codes

logger = logging.getLogger(__name__)


def raise_unknown_fields_error(
    action_name: str,
    config_type: str,
    unknown_fields: Set[str],
    expected_fields: Set[str],
    config_section: str,
) -> None:
    """Raise a user-friendly error for unknown configuration fields."""
    unknown_list = sorted(unknown_fields)

    suggestions = []
    for unknown_field in unknown_list:
        if unknown_field == "mode":
            suggestions.append(f"'{unknown_field}' → 'readMode'")
        elif unknown_field == "read_mode":
            suggestions.append(f"'{unknown_field}' → 'readMode'")
        elif unknown_field == "partitions":
            suggestions.append(f"'{unknown_field}' → 'partition_columns'")
        elif unknown_field == "sub_type":
            suggestions.append(
                f"'{unknown_field}' → use 'type' field in {config_section}"
            )
        else:
            best_match = _find_best_match(unknown_field, expected_fields)
            if best_match:
                suggestions.append(f"'{unknown_field}' → '{best_match}'")

    if len(unknown_list) == 1:
        unknown_text = f"Unknown field '{unknown_list[0]}'"
    else:
        unknown_text = f"Unknown fields: {', '.join(unknown_list)}"

    example_lines = []
    if suggestions:
        example_lines.append("Fix:")
        for suggestion in suggestions[:3]:
            example_lines.append(f"  {suggestion}")

    if len(suggestions) > 3:
        example_lines.append(f"  ... and {len(suggestions) - 3} more")

    example = (
        "\n".join(example_lines)
        if example_lines
        else "Check field names in documentation"
    )

    raise ErrorFactory.config_error(
        codes.CFG_001,
        title=f"{unknown_text} in {config_type}",
        details=f"Action '{action_name}' has invalid configuration",
        suggestions=[
            "Use the field name corrections shown below",
            "Check the documentation for valid field names",
        ],
        example=example,
        context={
            "Action": action_name,
            "Section": config_section,
            "Unknown": unknown_list,
            "Type": config_type,
        },
    )


def _find_best_match(unknown_field: str, expected_fields: Set[str]) -> Optional[str]:
    best_match = None
    best_score = 0

    for field in expected_fields:
        score = _calculate_similarity(unknown_field, field)
        if score > best_score and score > 0.6:
            best_score = score
            best_match = field

    if best_match:
        logger.debug(
            f"Similarity match for '{unknown_field}': '{best_match}' (score={best_score:.2f})"
        )
    else:
        logger.debug(
            f"No similar field found for '{unknown_field}' (best score below 0.6 threshold)"
        )

    return best_match


def _calculate_similarity(str1: str, str2: str) -> float:
    if not str1 or not str2:
        return 0.0

    if str1 in str2 or str2 in str1:
        return 0.8

    common_chars = sum(1 for c1, c2 in zip(str1, str2, strict=False) if c1 == c2)
    max_len = max(len(str1), len(str2))
    return common_chars / max_len if max_len > 0 else 0.0
