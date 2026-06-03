"""Presenter for the ``lhp list`` command family (presets / templates / blueprints).

Renders the frozen inspection views (:class:`lhp.api.PresetView`,
:class:`lhp.api.TemplateView`, :class:`lhp.api.BlueprintView`) as Rich tables on
stdout, with per-item detail blocks for descriptions, template parameters, and —
in instance mode — the pipelines each blueprint instance would materialise.

Pure presentation: this module imports only the shared ``_layout`` / ``_style``
primitives and the ``lhp.api`` view types. It MUST NOT import ``lhp.errors``
(constitution §5.2 / §9.5 — error formatting is single-sourced in
``cli/error_panel.py``); domain errors are raised by the command and rendered by
``cli_error_boundary``.

The stdout console is read from ``lhp.cli.console`` at call time (never bound at
import) so the per-test console swap in ``tests/conftest.py`` is honored.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Tuple

from rich.console import Console
from rich.text import Text

from lhp.cli import console as _console_module
from lhp.cli.presenters._layout import (
    ColumnSpec,
    render_empty_state,
    render_listing_table,
)

if TYPE_CHECKING:
    from lhp.api import BlueprintView, PresetView, TemplateView

logger = logging.getLogger(__name__)


def _sink() -> Console:
    """Resolve the stdout console at call time (honors per-test swap)."""
    return _console_module.console


def render_presets(presets: "Tuple[PresetView, ...]") -> None:
    """Render the presets table plus a description block for any described preset.

    Empty input renders a "No presets found" notice; the command still exits 0.
    """
    sink = _sink()
    if not presets:
        render_empty_state(
            "No presets found.",
            "Create a preset file in the 'presets' directory "
            "(e.g. presets/bronze_layer.yaml).",
            sink=sink,
        )
        return

    rows = [
        (
            preset.name,
            preset.file_path.name,
            str(preset.version),
            str(preset.extends or "-"),
        )
        for preset in presets
    ]
    render_listing_table(
        "Available presets",
        [
            ColumnSpec("Name", style="bold"),
            ColumnSpec("File"),
            ColumnSpec("Version"),
            ColumnSpec("Extends"),
        ],
        rows,
        total_label="presets",
        sink=sink,
    )
    _render_preset_descriptions(presets, sink)


def render_templates(templates: "Tuple[TemplateView, ...]") -> None:
    """Render the templates table plus a per-template parameter detail block.

    Empty input renders a "No templates found" notice; the command still exits 0.
    """
    sink = _sink()
    if not templates:
        render_empty_state(
            "No templates found.",
            "Create a template file in the 'templates' directory "
            "(e.g. templates/standard_ingestion.yaml).",
            sink=sink,
        )
        return

    rows = [
        (
            template.name,
            template.file_path.name,
            str(template.version),
            f"{template.required_parameter_count}/{template.parameter_count}",
            str(template.action_count),
        )
        for template in templates
    ]
    render_listing_table(
        "Available templates",
        [
            ColumnSpec("Name", style="bold"),
            ColumnSpec("File"),
            ColumnSpec("Version"),
            ColumnSpec("Params"),
            ColumnSpec("Actions"),
        ],
        rows,
        total_label="templates",
        sink=sink,
    )
    _render_template_details(templates, sink)


def render_blueprints(
    blueprints: "Tuple[BlueprintView, ...]",
    *,
    instances: bool,
) -> None:
    """Render the blueprints table; with ``instances`` show per-instance pipelines.

    Empty input renders a "No blueprints found" notice; the command still
    exits 0. With ``instances=True`` each blueprint expands to its instance
    files and the resolved pipeline names produced from each.
    """
    sink = _sink()
    if not blueprints:
        render_empty_state(
            "No blueprints found.",
            "Create a blueprint file under blueprints/ "
            "(or your configured blueprint_include patterns).",
            sink=sink,
        )
        return

    rows = [
        (
            bp.name,
            str(bp.version),
            str(bp.description or "-"),
            str(bp.parameter_count),
            str(bp.flowgroup_count),
            str(bp.instance_count),
        )
        for bp in blueprints
    ]
    render_listing_table(
        "Blueprints",
        [
            ColumnSpec("Name", style="bold"),
            ColumnSpec("Version"),
            ColumnSpec("Description"),
            ColumnSpec("Params"),
            ColumnSpec("Specs"),
            ColumnSpec("Instances"),
        ],
        rows,
        total_label="blueprints",
        sink=sink,
    )

    if instances:
        _render_blueprint_instances(blueprints, sink)


def _render_preset_descriptions(
    presets: "Tuple[PresetView, ...]", sink: Console
) -> None:
    """Print a description line for each preset that carries a real description."""
    described = [
        preset
        for preset in presets
        if preset.description and preset.description != "No description"
    ]
    if not described:
        return
    sink.print(Text("Descriptions", style="bold dim"))
    for preset in described:
        sink.print(Text(f"{preset.name}:", style="bold"))
        sink.print(f"   {preset.description}")


def _render_template_details(
    templates: "Tuple[TemplateView, ...]", sink: Console
) -> None:
    """Print the description and parameter detail for each template."""
    sink.print(Text("Template Details", style="bold dim"))
    for template in templates:
        sink.print(Text(f"{template.name}:", style="bold"))
        if template.description:
            sink.print(f"   Description: {template.description}")
        if not template.parameters:
            continue
        sink.print("   Parameters:")
        for param in template.parameters:
            requiredness = "required" if param.required else "optional"
            sink.print(f"      - {param.name} ({param.type_}, {requiredness})")
            if param.description:
                sink.print(f"        {param.description}")
            if param.default is not None:
                sink.print(f"        Default: {param.default}")


def _render_blueprint_instances(
    blueprints: "Tuple[BlueprintView, ...]", sink: Console
) -> None:
    """Print each blueprint's instance files and the pipelines they produce."""
    sink.print(Text("Instances", style="bold dim"))
    for bp in blueprints:
        sink.print(
            Text.assemble(
                ("  ", ""),
                (bp.name, "bold"),
                (f" ({bp.file_path}):", "dim"),
            )
        )
        for instance in bp.instances:
            sink.print(
                f"    - {instance.instance_file_path.name}: "
                f"{instance.flowgroup_count} flowgroup(s) -> "
                f"pipeline(s) {list(instance.pipelines)}"
            )
