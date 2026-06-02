"""List commands implementation for LakehousePlumber CLI.

Renders ``lhp list-presets``, ``lhp list-templates`` and
``lhp list-blueprints``. Everything domain-shaped is sourced through
:class:`lhp.api.LakehousePlumberApplicationFacade` —
``inspection.list_presets()`` / ``list_templates()`` /
``list_blueprints(include_instances=...)``. No internal-module imports
live here per constitution §2 + §9.13.
"""

import logging

from rich.text import Text

from lhp.api import (
    BlueprintView,
    LakehousePlumberApplicationFacade,
    PresetView,
    TemplateView,
)
from lhp.errors import ErrorFactory, codes

from .. import console as _console_module
from ..render import (
    ColumnSpec,
    render_command_header,
    render_empty_state,
    render_listing_table,
)
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


class ListCommand(BaseCommand):
    """List presets, templates, and blueprints."""

    def list_presets(self) -> None:
        """List all available presets with detailed information."""
        render_command_header("lhp list-presets")
        self.setup_from_context()
        project_root = self.ensure_project_root()
        presets_dir = project_root / "presets"

        if not presets_dir.exists():
            raise ErrorFactory.config_error(
                codes.CFG_016,
                title="No presets directory found",
                details="The presets/ directory does not exist in this project.",
                suggestions=[
                    "Create a 'presets' directory in your project root",
                    "Run 'lhp init <name>' to create a project with the correct structure",
                ],
            )

        facade = LakehousePlumberApplicationFacade.for_project(project_root)
        presets = facade.inspection.list_presets()

        if not presets:
            render_empty_state(
                "No presets found.",
                "Create a preset file in the 'presets' directory "
                "(e.g. presets/bronze_layer.yaml).",
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
        )

        self._display_preset_descriptions(presets)

    def list_templates(self) -> None:
        """List all available templates with detailed parameter information."""
        render_command_header("lhp list-templates")
        self.setup_from_context()
        project_root = self.ensure_project_root()
        templates_dir = project_root / "templates"

        if not templates_dir.exists():
            raise ErrorFactory.config_error(
                codes.CFG_017,
                title="No templates directory found",
                details="The templates/ directory does not exist in this project.",
                suggestions=[
                    "Create a 'templates' directory in your project root",
                    "Run 'lhp init <name>' to create a project with the correct structure",
                ],
            )

        facade = LakehousePlumberApplicationFacade.for_project(project_root)
        templates = facade.inspection.list_templates()

        if not templates:
            render_empty_state(
                "No templates found.",
                "Create a template file in the 'templates' directory "
                "(e.g. templates/standard_ingestion.yaml).",
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
        )

        self._display_template_details(templates)

        # Usage hint -> stderr (help text, not primary data).
        _console_module.err_console.print(
            Text(
                "Use templates in your flowgroup configuration: "
                "use_template: template_name; template_parameters: param1: value1",
                style="dim",
            )
        )

    def list_blueprints(self, verbose: bool = False) -> None:
        """List blueprints with parameter and instance counts.

        With ``--verbose``, lists each blueprint's instances and the resolved
        pipeline names that would be produced from each.
        """
        render_command_header("lhp list-blueprints")
        self.setup_from_context()
        project_root = self.ensure_project_root()

        facade = LakehousePlumberApplicationFacade.for_project(project_root)
        blueprints = facade.inspection.list_blueprints(include_instances=verbose)

        if not blueprints:
            render_empty_state(
                "No blueprints found.",
                "Create a blueprint file under blueprints/ "
                "(or your configured blueprint_include patterns).",
            )
            return

        rows = [
            (
                bp.name,
                str(bp.version),
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
                ColumnSpec("Params"),
                ColumnSpec("Specs"),
                ColumnSpec("Instances"),
            ],
            rows,
            total_label="blueprints",
        )

        total_instances = sum(bp.instance_count for bp in blueprints)

        if verbose:
            _console_module.console.print(Text("Verbose:", style="bold dim"))
            for bp in blueprints:
                _console_module.console.print(
                    Text.assemble(
                        ("  ", ""),
                        (bp.name, "bold"),
                        (f" ({bp.file_path}):", "dim"),
                    )
                )
                for instance in bp.instances:
                    _console_module.console.print(
                        f"    - {instance.instance_file_path.name}: "
                        f"{instance.flowgroup_count} flowgroup(s) -> "
                        f"pipeline(s) {list(instance.pipelines)}"
                    )

        _console_module.console.print(f"Total instances: {total_instances}")

    def _display_preset_descriptions(self, presets: "tuple[PresetView, ...]") -> None:
        described = [
            preset
            for preset in presets
            if preset.description and preset.description != "No description"
        ]
        if not described:
            return
        _console_module.console.print(Text("Descriptions", style="bold dim"))
        for preset in described:
            _console_module.console.print(Text(f"{preset.name}:", style="bold"))
            _console_module.console.print(f"   {preset.description}")

    def _display_template_details(self, templates: "tuple[TemplateView, ...]") -> None:
        _console_module.console.print(Text("Template Details", style="bold dim"))
        for template in templates:
            _console_module.console.print(Text(f"{template.name}:", style="bold"))
            if template.description:
                _console_module.console.print(f"   Description: {template.description}")

            if template.parameters:
                _console_module.console.print("   Parameters:")
                for param in template.parameters:
                    param_required = "required" if param.required else "optional"
                    _console_module.console.print(
                        f"      - {param.name} ({param.type_}, {param_required})"
                    )
                    if param.description:
                        _console_module.console.print(f"        {param.description}")
                    if param.default is not None:
                        _console_module.console.print(
                            f"        Default: {param.default}"
                        )


# Re-export the View types — the public CLI module imports them above as
# part of constraining itself to ``lhp.api`` per constitution §2 / §9.13;
# downstream tests may want to type-check the helpers without re-importing
# from the api package.
__all__ = ["ListCommand", "BlueprintView", "PresetView", "TemplateView"]
