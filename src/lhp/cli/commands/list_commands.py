"""List commands implementation for LakehousePlumber CLI."""

import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

from rich.text import Text

from ...parsers.yaml_parser import YAMLParser
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
            from ...utils.error_formatter import ErrorCategory, LHPConfigError

            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="016",
                title="No presets directory found",
                details="The presets/ directory does not exist in this project.",
                suggestions=[
                    "Create a 'presets' directory in your project root",
                    "Run 'lhp init <name>' to create a project with the correct structure",
                ],
            )

        preset_files = list(presets_dir.glob("*.yaml")) + list(
            presets_dir.glob("*.yml")
        )

        if not preset_files:
            render_empty_state(
                "No presets found.",
                "Create a preset file in the 'presets' directory "
                "(e.g. presets/bronze_layer.yaml).",
            )
            return

        presets_info = self._parse_preset_information(preset_files)

        rows = [
            (
                preset["name"],
                preset["file"],
                str(preset["version"]),
                str(preset["extends"] or "-"),
            )
            for preset in presets_info
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

        self._display_preset_descriptions(presets_info)

    def list_templates(self) -> None:
        """List all available templates with detailed parameter information."""
        render_command_header("lhp list-templates")
        self.setup_from_context()
        project_root = self.ensure_project_root()
        templates_dir = project_root / "templates"

        if not templates_dir.exists():
            from ...utils.error_formatter import ErrorCategory, LHPConfigError

            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="017",
                title="No templates directory found",
                details="The templates/ directory does not exist in this project.",
                suggestions=[
                    "Create a 'templates' directory in your project root",
                    "Run 'lhp init <name>' to create a project with the correct structure",
                ],
            )

        template_files = list(templates_dir.glob("*.yaml")) + list(
            templates_dir.glob("*.yml")
        )

        if not template_files:
            render_empty_state(
                "No templates found.",
                "Create a template file in the 'templates' directory "
                "(e.g. templates/standard_ingestion.yaml).",
            )
            return

        templates_info = self._parse_template_information(template_files)

        rows = [
            (
                template["name"],
                template["file"],
                str(template["version"]),
                str(template["params"]),
                str(template["actions"]),
            )
            for template in templates_info
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

        self._display_template_details(template_files)

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

        from ...core.project_config_loader import ProjectConfigLoader
        from ...core.services.blueprint_discoverer import BlueprintDiscoverer
        from ...parsers.blueprint_parser import BlueprintParser

        project_config_loader = ProjectConfigLoader(project_root)
        project_config = project_config_loader.load_project_config()

        discoverer = BlueprintDiscoverer(
            project_root,
            project_config=project_config,
            blueprint_parser=BlueprintParser(),
        )
        blueprints = discoverer.discover_blueprints()
        if not blueprints:
            render_empty_state(
                "No blueprints found.",
                "Create a blueprint file under blueprints/ "
                "(or your configured blueprint_include patterns).",
            )
            return

        instances = discoverer.discover_instances(blueprints)
        instances_by_blueprint: Dict[str, List[Tuple[Any, Path]]] = {
            name: [] for name in blueprints
        }
        for instance, instance_path in instances:
            instances_by_blueprint.setdefault(instance.blueprint_name, []).append(
                (instance, instance_path)
            )

        rows = [
            (
                name,
                str(bp.version),
                str(len(bp.parameters)),
                str(len(bp.flowgroups)),
                str(len(instances_by_blueprint.get(name, []))),
            )
            for name, (bp, _path) in sorted(blueprints.items())
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

        if verbose:
            from ...core.services.blueprint_expander import BlueprintExpander

            expander = BlueprintExpander()
            _console_module.console.print(Text("Verbose:", style="bold dim"))
            for name, (bp, bp_path) in sorted(blueprints.items()):
                _console_module.console.print(
                    Text.assemble(
                        ("  ", ""),
                        (name, "bold"),
                        (f" ({bp_path}):", "dim"),
                    )
                )
                for instance, instance_path in instances_by_blueprint.get(name, []):
                    contexts, _ = expander.expand_single_instance(
                        instance, instance_path, blueprints
                    )
                    pipelines = sorted({ctx.flowgroup.pipeline for ctx in contexts})
                    _console_module.console.print(
                        f"    - {instance_path.name}: "
                        f"{len(contexts)} flowgroup(s) -> pipeline(s) {pipelines}"
                    )

        _console_module.console.print(f"Total instances: {len(instances)}")

    def _parse_preset_information(
        self, preset_files: List[Path]
    ) -> List[Dict[str, Any]]:
        parser = YAMLParser()
        presets_info = []

        for preset_file in sorted(preset_files):
            try:
                preset = parser.parse_preset(preset_file)
                presets_info.append(
                    {
                        "name": preset.name,
                        "file": preset_file.name,
                        "version": preset.version,
                        "extends": preset.extends,
                        "description": preset.description or "No description",
                    }
                )
            except Exception as e:
                logger.warning(f"Could not parse preset {preset_file}: {e}")
                from ...utils.error_formatter import LHPError

                error_desc = (
                    e.title if isinstance(e, LHPError) else f"{type(e).__name__}"
                )
                presets_info.append(
                    {
                        "name": preset_file.stem,
                        "file": preset_file.name,
                        "version": "?",
                        "extends": "?",
                        "description": f"Error: {error_desc}",
                    }
                )

        return presets_info

    def _parse_template_information(
        self, template_files: List[Path]
    ) -> List[Dict[str, Any]]:
        parser = YAMLParser()
        templates_info = []

        for template_file in sorted(template_files):
            try:
                template = parser.parse_template_raw(template_file)
                required_params = sum(
                    1 for p in template.parameters if p.get("required", False)
                )
                total_params = len(template.parameters)

                templates_info.append(
                    {
                        "name": template.name,
                        "file": template_file.name,
                        "version": template.version,
                        "params": f"{required_params}/{total_params}",
                        "actions": len(template.actions),
                        "description": template.description or "No description",
                    }
                )
            except Exception as e:
                logger.warning(f"Could not parse template {template_file}: {e}")
                from ...utils.error_formatter import LHPError

                error_desc = (
                    e.title if isinstance(e, LHPError) else f"{type(e).__name__}"
                )
                templates_info.append(
                    {
                        "name": template_file.stem,
                        "file": template_file.name,
                        "version": "?",
                        "params": "?",
                        "actions": "?",
                        "description": f"Error: {error_desc}",
                    }
                )

        return templates_info

    def _display_preset_descriptions(self, presets_info: List[Dict[str, Any]]) -> None:
        described = [
            preset
            for preset in presets_info
            if preset["description"] != "No description"
        ]
        if not described:
            return
        _console_module.console.print(Text("Descriptions", style="bold dim"))
        for preset in described:
            _console_module.console.print(Text(f"{preset['name']}:", style="bold"))
            _console_module.console.print(f"   {preset['description']}")

    def _display_template_details(self, template_files: List[Path]) -> None:
        parser = YAMLParser()

        _console_module.console.print(Text("Template Details", style="bold dim"))
        for template_file in sorted(template_files):
            try:
                template = parser.parse_template_raw(template_file)
                _console_module.console.print(Text(f"{template.name}:", style="bold"))
                if template.description:
                    _console_module.console.print(
                        f"   Description: {template.description}"
                    )

                if template.parameters:
                    _console_module.console.print("   Parameters:")
                    for param in template.parameters:
                        param_name = param.get("name", "unknown")
                        param_type = param.get("type", "string")
                        param_required = (
                            "required" if param.get("required", False) else "optional"
                        )
                        param_desc = param.get("description", "")
                        default = param.get("default")

                        _console_module.console.print(
                            f"      - {param_name} ({param_type}, {param_required})"
                        )
                        if param_desc:
                            _console_module.console.print(f"        {param_desc}")
                        if default is not None:
                            _console_module.console.print(f"        Default: {default}")

            except Exception as e:
                logger.debug(
                    f"Skipping template detail display for {template_file}: {e}"
                )
                pass
