"""Read data-quality ``expectations_file`` files.

The data_quality transform generator and the expectations-file validator both
read an action's file through :func:`load_expectations_file`, so ``lhp
validate`` and ``lhp generate`` resolve, parse and unwrap it the same way.
"""

from pathlib import Path
from typing import Any, Dict, List, Union

from lhp.errors import ErrorFactory, codes

from ...parsers import load_yaml_file
from .external_file_loader import resolve_external_file_path

#: The two expectation formats, as they come out of the file.
Expectations = Union[List[Any], Dict[str, Any]]

_FORMAT_EXAMPLE = """\
id IS NOT NULL:
  action: drop
  name: valid_id
amount > 0:
  action: warn
  name: positive_amount"""


def load_expectations_file(
    expectations_file: Union[str, Path],
    project_root: Path,
    error_context: str = "expectations file",
) -> Expectations:
    """Return the expectations in ``expectations_file``, in the file's own format.

    ``expectations_file`` resolves against ``project_root``. Its single YAML or
    JSON document is one of:

    * a list of rules (``name``, ``constraint`` or ``expression``,
      ``failureAction`` or ``type``), returned as it is;
    * a mapping from each constraint to its ``action`` and ``name``, returned
      as it is;
    * a mapping with a top-level ``expectations`` key (the older wrapped
      format), whose value is returned without checking its type.

    ``error_context`` names the file in YAML-parse and document-count errors.

    :raises LHPFileError: ``LHP-IO-001`` when the file does not exist;
        ``LHP-IO-002`` when it cannot be read.
    :raises MultiDocumentError: ``LHP-IO-003`` when the file holds no document
        or more than one.
    :raises LHPConfigError: ``LHP-CFG-009`` when the file is not valid YAML.
    :raises LHPValidationError: ``LHP-VAL-008`` when the document is neither a
        mapping nor a list; ``context["Actual Type"]`` names the type found.
    """
    resolved_path = resolve_external_file_path(
        expectations_file, project_root, file_type="expectations file"
    )
    data: object = load_yaml_file(resolved_path, error_context=error_context)

    if isinstance(data, dict):
        if "expectations" in data:
            # Not type-checked: both readers accept whatever the key holds
            # (an empty or null value means no rules).
            wrapped: Expectations = data["expectations"]
            return wrapped
        return data
    if isinstance(data, list):
        return data

    actual_type = type(data).__name__
    raise ErrorFactory.validation_error(
        codes.VAL_008,
        title="Invalid expectations file format",
        details=(
            f"Expectations file '{expectations_file}' must hold a mapping or a "
            f"list of expectations, but it holds a {actual_type}."
        ),
        suggestions=[
            "Map each constraint to its 'action' and 'name' (see the example)",
            "Or list the rules, each with 'name', 'constraint' and 'failureAction'",
        ],
        example=_FORMAT_EXAMPLE,
        context={
            "File": str(resolved_path),
            "Expected Type": "mapping or list",
            "Actual Type": actual_type,
        },
    )
