"""Shared Jinja filters for YAML serialization."""

from typing import Any

import yaml


def dict_to_yaml(value: Any) -> str:
    """Serialize a Python value to YAML for pass-through rendering in Jinja templates.

    Used as a Jinja filter (``{{ {key: value} | toyaml | indent(6) }}``) so
    unknown config keys flow into generated resource YAML with correct structure.

    ``sort_keys=False`` preserves author-specified order; ``default_flow_style=False``
    forces block style so nested dicts look natural next to hand-written Jinja blocks.
    The trailing newline is stripped so the rendered block integrates cleanly with
    surrounding Jinja whitespace controls.
    """
    return yaml.safe_dump(value, default_flow_style=False, sort_keys=False).rstrip("\n")
