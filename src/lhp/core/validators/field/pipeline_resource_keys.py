"""Check ``pipeline_config`` keys LHP also writes into the pipeline resource (#351).

LHP writes ``name``, ``libraries`` and ``root_path`` into every pipeline
resource and passes every other ``pipeline_config`` key through verbatim. A
user value for one of those keys used to come out as a second key of the same
name, and YAML readers keep the last one, so LHP's value was silently
discarded. ``libraries`` entries are now appended after LHP's glob library, so
their shape is checked here; ``name`` and ``root_path`` are LHP-owned and
rejected. Every failure is ``LHP-CFG-072``.
"""

import logging
from typing import Any, Mapping, Optional

from lhp.errors import LHPConfigError

from ._resource_key_errors import owned_key_error, resource_key_error

logger = logging.getLogger(__name__)

#: Keys pipeline_resource.yml.j2 writes itself. BundleManager keeps them out of
#: the template's pass-through loop, so a user value is never emitted twice.
LHP_WRITTEN_PIPELINE_KEYS = frozenset({"name", "libraries", "root_path"})

_GLOB_EXAMPLE = (
    "libraries:\n  - glob:\n      include: ${workspace.file_path}/pipeline_extras/**"
)

# Why each LHP-owned key cannot be set, and what to do instead.
_OWNED_KEYS = {
    "name": (
        "LHP names every pipeline resource '<pipeline>_pipeline' after the "
        "'pipeline' field of its flowgroups.",
        "Remove 'name'. To change the name, rename the pipeline in the "
        "flowgroups' 'pipeline' field.",
    ),
    "root_path": (
        "LHP sets 'root_path' to the pipeline's generated code folder. "
        "Databricks adds it to sys.path, and the generated code's imports "
        "of its helper modules rely on it.",
        "Remove 'root_path'.",
    ),
}


def validate_pipeline_resource_keys(config: Mapping[str, Any]) -> None:
    """Check one ``pipeline_config`` document's LHP-written keys.

    Raises:
        LHPConfigError: ``LHP-CFG-072`` if the document sets ``name`` or
            ``root_path``, or a ``libraries`` value that is not a list of
            ``glob: {include: <non-empty path>}`` entries.
    """
    for key, (reason, fix) in _OWNED_KEYS.items():
        if key in config:
            raise owned_key_error("pipeline_config", key, reason, fix)
    if "libraries" in config:
        _check_libraries(config["libraries"])


def _check_libraries(libraries: Any) -> None:
    if not isinstance(libraries, list):
        raise resource_key_error(
            title="Pipeline 'libraries' must be a list of glob entries",
            details=(
                f"'libraries' must be a list, got {type(libraries).__name__} "
                f"({libraries!r}). LHP appends these entries after the glob "
                f"library it generates for the pipeline's code, so it cannot "
                f"merge a single value or a bundle variable reference."
            ),
            suggestions=["List each extra library as a '- glob:' entry"],
            example=_GLOB_EXAMPLE,
        )
    for index, entry in enumerate(libraries):
        if not isinstance(entry, dict) or list(entry) != ["glob"]:
            raise _non_glob_library_error(index, entry)
        glob = entry["glob"]
        include = glob.get("include") if isinstance(glob, dict) else None
        if not isinstance(include, str) or not include.strip():
            raise resource_key_error(
                title=f"Library entry {index} has no 'include' path",
                details=(
                    f"Library entry {index} must be 'glob: {{include: <path>}}' "
                    f"with a non-empty path, got {entry!r}."
                ),
                suggestions=[
                    "Set 'include' to a file, a notebook, or a folder ending '/**'"
                ],
                example=_GLOB_EXAMPLE,
                context={"Entry": index},
            )
        # Databricks' glob library (PathPattern) accepts only ``include``.
        extra = ", ".join(repr(key) for key in glob if key != "include")
        if extra:
            raise resource_key_error(
                title=f"Library entry {index} glob has unsupported key {extra}",
                details=(
                    f"A glob library takes only 'include', but library entry "
                    f"{index} also sets {extra}."
                ),
                suggestions=[f"Remove {extra} from library entry {index}"],
                example=_GLOB_EXAMPLE,
                context={"Entry": index},
            )


def _non_glob_library_error(index: int, entry: Any) -> LHPConfigError:
    if not isinstance(entry, dict):
        title = f"Library entry {index} is not a glob library"
        found = f"is {entry!r}, not a mapping"
    elif not entry:
        title = f"Library entry {index} is empty"
        found = "is an empty mapping"
    elif "glob" in entry:
        others = ", ".join(f"'{kind}'" for kind in entry if kind != "glob")
        title = f"Library entry {index} combines 'glob' with {others}"
        found = f"combines 'glob' with {others}"
    else:
        kinds = ", ".join(f"'{kind}'" for kind in entry)
        title = f"Library entry {index} uses {kinds}, not 'glob'"
        found = f"uses {kinds}"
    path = _library_path(entry)
    example = f"libraries:\n  - glob:\n      include: {path}" if path else _GLOB_EXAMPLE
    return resource_key_error(
        title=title,
        details=(
            f"Library entry {index} {found}. Every pipeline already has the "
            f"glob library LHP generates for its code, and Databricks does not "
            f"allow glob libraries together with notebook or file libraries, "
            f"so each extra library must be a single 'glob' entry. A glob "
            f"'include' can point to one file or notebook."
        ),
        suggestions=["Rewrite the entry as a 'glob' library with an 'include' path"],
        example=example,
        context={"Entry": index},
    )


def _library_path(entry: Any) -> Optional[str]:
    """Return the path of a ``file``/``notebook`` entry, for a glob rewrite."""
    if not isinstance(entry, dict):
        return None
    for kind in ("file", "notebook"):
        spec = entry.get(kind)
        path = spec.get("path") if isinstance(spec, dict) else None
        if isinstance(path, str):
            return path
    return None
