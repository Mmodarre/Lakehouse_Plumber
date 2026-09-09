"""Read-only, path-based template inspection shared by the authoring API."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment, TemplateError, meta

from lhp.errors import LHPError
from lhp.models import Template
from lhp.parsers.yaml_loader import SAFE_LOADER
from lhp.parsers.yaml_parser import YAMLParser

MAX_SOURCE_BYTES = 524288
MAX_NODES = 20000
MAX_DEPTH = 50


def project_path(root: Path, path: str, *, template: bool = False) -> Path:
    """Guard both lexical identity and resolved symlinks, including missing paths."""
    relative = Path(path)
    if relative.is_absolute() or ".." in relative.parts or "\\" in path:
        raise PermissionError("Use a relative path inside the project.")
    if template and (
        not relative.parts
        or relative.parts[0] != "templates"
        or relative.suffix not in {".yaml", ".yml"}
    ):
        raise ValueError("Choose a .yaml or .yml file under templates/.")
    resolved = (root / relative).resolve()
    if not resolved.is_relative_to(root):
        raise PermissionError("Template authoring paths must stay inside the project.")
    if template and not resolved.is_relative_to((root / "templates").resolve()):
        raise PermissionError("Template sources must stay inside templates/.")
    return resolved


def bounded_read(path: Path) -> str:
    with path.open("rb") as source:
        raw = source.read(MAX_SOURCE_BYTES + 1)
    if len(raw) > MAX_SOURCE_BYTES:
        raise ValueError(
            "This file exceeds the 512 KiB authoring limit. Open it in Code."
        )
    return raw.decode("utf-8")


def diagnostic(
    path: str,
    message: str,
    *,
    code: str = "LHP-TEMPLATE-INSPECT",
    stage: str = "inspect",
    severity: str = "error",
    field: tuple[Any, ...] | None = None,
    locations: dict[tuple[Any, ...], tuple[int, int]] | None = None,
    suggestion: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "severity": severity,
        "code": code,
        "stage": stage,
        "message": message,
        "source_path": path,
        "suggestion": suggestion,
    }
    if field is not None:
        result["field_path"] = list(field)
        location = (locations or {}).get(field)
        if location:
            result["line"], result["column"] = location
    return result


def _source_locations(source: str) -> dict[tuple[Any, ...], tuple[int, int]]:
    locations: dict[tuple[Any, ...], tuple[int, int]] = {}
    tree = yaml.compose(source, Loader=SAFE_LOADER)
    count = 0

    def visit(node: yaml.Node, field: tuple[Any, ...], ancestors: set[int]) -> None:
        nonlocal count
        count += 1
        if count > MAX_NODES or len(field) > MAX_DEPTH or id(node) in ancestors:
            raise ValueError(
                "Template nesting, aliases or size exceed the authoring limit."
            )
        locations[field] = (node.start_mark.line + 1, node.start_mark.column + 1)
        ancestors = ancestors | {id(node)}
        if isinstance(node, yaml.MappingNode):
            for key, value in node.value:
                visit(value, (*field, str(key.value)), ancestors)
        elif isinstance(node, yaml.SequenceNode):
            for index, value in enumerate(node.value):
                visit(value, (*field, index), ancestors)

    if tree:
        visit(tree, (), set())
    return locations


def inspect_source(
    source: str, path: str
) -> tuple[Template | None, dict[str, Any], list[dict[str, Any]]]:
    """Check authoring structure and eligible Jinja without requiring sample values."""
    diagnostics: list[dict[str, Any]] = []
    raw: dict[str, Any] = {}
    locations: dict[tuple[Any, ...], tuple[int, int]] = {}
    try:
        if len(source.encode("utf-8")) > MAX_SOURCE_BYTES:
            raise ValueError("Template source exceeds the 512 KiB authoring limit.")
        locations = _source_locations(source)
        value = yaml.load(source, Loader=SAFE_LOADER)  # nosec B506
        if not isinstance(value, dict):
            raise TypeError("A template must contain one YAML mapping.")
        raw = value
        # Samples/results have a JSON contract. Do not silently stringify dates,
        # sets, nonfinite floats or mappings with non-string keys.
        _check_json_value(raw)
        model = YAMLParser.parse_template_data(raw)
    except (ValueError, TypeError, RecursionError, yaml.YAMLError, LHPError) as exc:
        item = diagnostic(path, str(exc), locations=locations, field=())
        mark = getattr(exc, "problem_mark", None)
        if mark is not None:
            item.update(line=mark.line + 1, column=mark.column + 1)
        diagnostics.append(item)
        return None, raw, diagnostics

    def report(message: str, field: tuple[Any, ...], *, warning: bool = False) -> None:
        diagnostics.append(
            diagnostic(
                path,
                message,
                field=field,
                locations=locations,
                severity="warning" if warning else "error",
            )
        )

    names: set[str] = set()
    for index, param in enumerate(model.parameters):
        field = ("parameters", index)
        name = param.get("name")
        if not isinstance(name, str) or not name.strip():
            report("Give this parameter a nonempty name.", (*field, "name"))
        elif name in names:
            report(f"Parameter '{name}' is declared more than once.", (*field, "name"))
        else:
            names.add(name)
        if "required" in param and not isinstance(param["required"], bool):
            report("Required must be true or false.", (*field, "required"))
        if "type" in param:
            report(
                "Declared type is advisory metadata; the renderer does not enforce it.",
                (*field, "type"),
                warning=True,
            )
        if param.get("required") is True and "default" in param:
            report(
                "Required parameters must be supplied explicitly, even with a default.",
                (*field, "default"),
                warning=True,
            )

    env = Environment()  # nosec B701 -- inspects YAML/SQL patterns, not HTML
    for field, value in _string_fields(raw.get("actions", []), ("actions",)):
        if "{{" in value and "}}" in value:
            try:
                ast = env.parse(value)
                env.from_string(value)
                unknown = sorted(
                    meta.find_undeclared_variables(ast) - names - set(env.globals)
                )
                if unknown:
                    report(
                        "References undeclared parameter(s): "
                        + ", ".join(unknown)
                        + ". The current renderer permits these; check sample values.",
                        field,
                        warning=True,
                    )
            except TemplateError as exc:
                report(f"Invalid Jinja expression: {exc}", field)
        elif "{%" in value:
            report(
                "Block-only Jinja is not rendered. The current engine renders a scalar only when it also contains {{ ... }}.",
                field,
                warning=True,
            )
        elif "{{" in value or "}}" in value:
            report(
                "Unpaired template delimiters remain literal in the current renderer.",
                field,
                warning=True,
            )
    for field, key in _templated_keys(raw.get("actions", []), ("actions",)):
        report(
            f"Mapping key {key!r} is literal: the renderer substitutes values only.",
            field,
            warning=True,
        )
    return model, raw, diagnostics


def _check_json_value(value: Any, depth: int = 0) -> None:
    if depth > MAX_DEPTH:
        raise ValueError("Template value nesting exceeds the authoring limit.")
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise ValueError(
                "Preview requires string mapping keys; preserve other YAML in Code."
            )
        for item in value.values():
            _check_json_value(item, depth + 1)
    elif isinstance(value, list):
        for item in value:
            _check_json_value(item, depth + 1)
    else:
        try:
            json.dumps(value, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Preview requires JSON-compatible values (finite numbers, strings, booleans, null, lists and mappings). Other YAML values remain editable in Code."
            ) from exc


def _string_fields(value: Any, field: tuple[Any, ...]) -> Any:
    if isinstance(value, str):
        yield field, value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from _string_fields(item, (*field, key))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _string_fields(item, (*field, index))


def _templated_keys(value: Any, field: tuple[Any, ...]) -> Any:
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(key, str) and ("{{" in key or "{%" in key):
                yield (*field, key), key
            yield from _templated_keys(item, (*field, key))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _templated_keys(item, (*field, index))


def source_entry(root: Path, path: str) -> dict[str, Any]:
    candidate = project_path(root, path, template=True)
    relative = Path(path).as_posix()
    reference = (
        Path(path).relative_to("templates").with_suffix("").as_posix()
        if Path(path).suffix == ".yaml"
        else None
    )
    entry: dict[str, Any] = {
        "source_path": relative,
        "reference": reference,
        "declared_name": None,
        "version": None,
        "description": None,
        "state": "invalid",
        "parameters": [],
        "presets": [],
        "action_count": None,
        "diagnostics": [],
    }
    try:
        model, raw, issues = inspect_source(bounded_read(candidate), relative)
    except (OSError, ValueError) as exc:
        entry["diagnostics"] = [diagnostic(relative, str(exc))]
        return entry
    entry["diagnostics"] = issues
    for key, target in (
        ("name", "declared_name"),
        ("version", "version"),
        ("description", "description"),
    ):
        if isinstance(raw.get(key), str):
            entry[target] = raw[key]
    if model:
        entry.update(
            version=model.version,
            action_count=len(model.actions),
            presets=model.presets,
        )
        entry["parameters"] = [
            {
                "name": p.get("name") if isinstance(p.get("name"), str) else "",
                "required": p.get("required") is True,
                "has_default": "default" in p,
                "default": p.get("default"),
                "declared_type": p.get("type")
                if isinstance(p.get("type"), str)
                else None,
                "description": p.get("description")
                if isinstance(p.get("description"), str)
                else None,
            }
            for p in model.parameters
        ]
        if not any(d["severity"] == "error" for d in issues):
            entry["state"] = "ready" if reference else "unsupported_extension"
    if not reference:
        entry["diagnostics"].append(
            diagnostic(
                relative,
                "This .yml file can be edited, but use_template currently loads .yaml files only.",
                severity="warning",
            )
        )
    return entry


def catalog(root: Path) -> dict[str, Any]:
    root = root.resolve()
    directory = project_path(root, "templates")
    if not directory.is_dir():
        return {"templates": [], "total": 0}
    entries = []
    for path in sorted(directory.rglob("*")):
        if path.suffix not in {".yaml", ".yml"} or not path.is_file():
            continue
        relative = "templates/" + path.relative_to(directory).as_posix()
        try:
            entries.append(source_entry(root, relative))
        except PermissionError:
            # Retain the unsafe entry without following or reading its target.
            entries.append(
                {
                    "source_path": relative,
                    "reference": None,
                    "declared_name": None,
                    "version": None,
                    "description": None,
                    "state": "invalid",
                    "parameters": [],
                    "presets": [],
                    "action_count": None,
                    "diagnostics": [
                        diagnostic(
                            relative,
                            "This symlink points outside the project and cannot be opened.",
                        )
                    ],
                }
            )
    return {"templates": entries, "total": len(entries)}


def source_hash(source: str) -> str:
    return hashlib.sha256(source.encode("utf-8")).hexdigest()
