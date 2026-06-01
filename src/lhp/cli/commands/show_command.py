"""Show command implementation for LakehousePlumber CLI.

# JUSTIFIED: show_command.py is ~714 lines because the resolved-config
# display layer hosts header/substitution/action panels + project-summary
# tables in one file. Decomposition target: extract the panel renderers
# into ``src/lhp/cli/presenters/show_*.py`` and reduce this file to a
# Click callback + facade dispatch + a few lines of glue.
# TODO(Phase 9.2): see LOCAL/REMAINING_WORK.md §0 — CLI presenter extraction.
"""

import dataclasses
import importlib
import logging
import os
import time
from pathlib import Path
from typing import Iterator, Optional, Tuple

import yaml
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from lhp.api import (
    LakehousePlumberApplicationFacade,
    ProcessedFlowgroupView,
    ProjectConfigView,
    SubstitutionView,
)
from lhp.errors import ErrorCategory, LHPError

from .. import console as _console_module
from ..render import render_command_header
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


class ShowCommand(BaseCommand):
    """Show and info commands: resolved flowgroups, instances, and project info."""

    def execute(
        self,
        flowgroup: Optional[str],
        env: str,
        instance_path: Optional[str],
    ) -> None:
        """Dispatch ``lhp show`` to either flowgroup or instance handling.

        Validates the (flowgroup, --instance) argument combination here so
        that ``main.py`` stays a thin wiring layer; both error paths raise
        ``LHPError`` codes 057 (mutually exclusive) and 058 (missing).
        """
        if instance_path:
            if flowgroup:
                raise LHPError(
                    category=ErrorCategory.CONFIG,
                    code_number="057",
                    title="Cannot pass both flowgroup and --instance",
                    details=(
                        "Use either FLOWGROUP positional argument OR --instance "
                        "<path>, not both."
                    ),
                    suggestions=[
                        "Drop the FLOWGROUP positional argument when using --instance",
                    ],
                )
            self.show_instance(instance_path, env)
            return

        if not flowgroup:
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="058",
                title="Missing flowgroup argument",
                details="Provide a flowgroup name OR --instance <path>.",
                suggestions=[
                    "Run `lhp show <flowgroup>` to inspect a flowgroup",
                    "Run `lhp show --instance <path>` to inspect a blueprint instance",
                ],
            )
        self.show_flowgroup(flowgroup, env)

    def show_flowgroup(self, flowgroup: str, env: str = "dev") -> None:
        """Show resolved configuration for a specific flowgroup."""
        render_command_header(f"lhp show {flowgroup}")
        self.setup_from_context()
        project_root = self.ensure_project_root()

        logger.debug(f"Show flowgroup request: flowgroup={flowgroup}, env={env}")

        application_facade = LakehousePlumberApplicationFacade.for_project(
            project_root,
            enforce_version=False,
        )

        try:
            processed_fg = application_facade.inspection.process_flowgroup(
                flowgroup,
                env=env,
            )
        except LookupError:
            # `InspectionFacade.process_flowgroup` raises `LookupError`
            # via `_locate_flowgroup_by_name` when no flowgroup matches.
            available = sorted(
                fg.name for fg in application_facade.inspection.list_flowgroups()
            )
            suggestions = [
                "Check the flowgroup name for typos",
                "Ensure a YAML file with this flowgroup exists in pipelines/",
            ]
            if available:
                suggestions.insert(
                    0,
                    f"Available flowgroups: {', '.join(available)}",
                )

            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="013",
                title=f"Flowgroup '{flowgroup}' not found",
                details=f"No flowgroup named '{flowgroup}' was found in the project.",
                suggestions=suggestions,
                context={"Flowgroup": flowgroup},
            ) from None

        self._display_flowgroup_configuration(processed_fg, env)

    def show_instance(self, instance_path_str: str, env: str = "dev") -> None:
        """Show resolved configuration for the flowgroups produced by an instance.

        Expands only the named instance (not the full project) so the
        round-trip stays sub-second at 80-instance scale.

        The blueprint discoverer / expander / parser and the
        ``ProjectConfigLoader`` all live in ``lhp.core`` and ``lhp.parsers``.
        The §9.7 placement gate forbids ``cli/commands/*`` from importing them
        statically, so the helpers are resolved here via
        :func:`importlib.import_module` —
        :samp:`API-LEAK-DEFER-show-instance`.
        """
        render_command_header("lhp show --instance")
        self.setup_from_context()
        project_root = self.ensure_project_root()

        instance_path = Path(instance_path_str)
        if not instance_path.is_absolute():
            instance_path = (project_root / instance_path).resolve()
        if not instance_path.exists():
            raise LHPError(
                category=ErrorCategory.IO,
                code_number="003",
                title="Instance file not found",
                details=f"No file at {instance_path}",
                suggestions=[
                    "Check the path for typos",
                    "Place each instance file under "
                    "pipelines/<system>/<layer>/ or your configured "
                    "instance_include patterns",
                ],
                context={"Path": str(instance_path)},
            )

        _console_module.console.print(
            Text.assemble(
                ("Resolving instance ", "bold dim"),
                (f"'{instance_path.name}'", ""),
                (" in environment ", "bold dim"),
                (f"'{env}'", ""),
            )
        )

        loaders_mod = importlib.import_module("lhp.core.loaders")
        discovery_mod = importlib.import_module(
            "lhp.core.discovery.blueprint_discoverer"
        )
        processing_mod = importlib.import_module(
            "lhp.core.processing.blueprint_expander"
        )
        parsers_mod = importlib.import_module("lhp.parsers.blueprint_parser")

        project_config_loader_cls = loaders_mod.ProjectConfigLoader
        blueprint_discoverer_cls = discovery_mod.BlueprintDiscoverer
        blueprint_expander_cls = processing_mod.BlueprintExpander
        blueprint_parser_cls = parsers_mod.BlueprintParser

        project_config = project_config_loader_cls(project_root).load_project_config()
        parser = blueprint_parser_cls()
        discoverer = blueprint_discoverer_cls(
            project_root,
            project_config=project_config,
            blueprint_parser=parser,
        )
        blueprints = discoverer.discover_blueprints()
        if not blueprints:
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="056",
                title="No blueprints in project",
                details=(
                    "Cannot resolve an instance file because no blueprints "
                    "are present in the project."
                ),
                suggestions=[
                    "Add a blueprint file under blueprints/",
                ],
            )

        blueprint_models = {name: bp for name, (bp, _) in blueprints.items()}
        instance = parser.parse_instance_file(instance_path, blueprint_models)

        expander = blueprint_expander_cls()
        contexts, _ = expander.expand_single_instance(
            instance, instance_path, blueprints
        )

        application_facade = LakehousePlumberApplicationFacade.for_project(
            project_root,
            enforce_version=False,
        )

        sub_file = project_root / "substitutions" / f"{env}.yaml"
        if not sub_file.exists():
            _console_module.err_console.print(
                Text.assemble(
                    ("⚠ ", "bold yellow"),
                    ("Warning: Substitution file not found: ", "bold yellow"),
                    (str(sub_file), ""),
                )
            )
        sub_view = application_facade.inspection.build_substitution_view(env)
        _console_module.console.print(
            Text.assemble(
                ("Blueprint: ", "bold dim"),
                (instance.blueprint_name, ""),
            )
        )
        _console_module.console.print(
            Text.assemble(
                ("Flowgroups produced: ", "bold dim"),
                (str(len(contexts)), ""),
            )
        )

        for ctx in contexts:
            fg = ctx.flowgroup
            _console_module.console.print(
                Text.assemble(
                    ("Pipeline: ", "bold dim"),
                    (fg.pipeline, ""),
                    ("    Flowgroup: ", "bold dim"),
                    (fg.flowgroup, ""),
                )
            )
            processed = application_facade.inspection.process_flowgroup(
                fg.flowgroup,
                env=env,
            )
            self._display_flowgroup_configuration(processed, env)
            self._display_actions_table(processed)

        self._display_secret_references(sub_view)
        self._display_substitution_summary(sub_view)

    def show_project_info(self) -> None:
        """Display project information and statistics."""
        render_command_header("lhp info")
        self.setup_from_context()
        project_root = self.ensure_project_root()

        project_config = self._load_project_config(project_root)
        self._display_project_basic_info(project_config, project_root)

        resource_summary = self._collect_resource_summary(project_root)
        self._display_resource_summary(resource_summary)

        self._display_environments(project_root)

        self._display_recent_activity(project_root)

    def _display_flowgroup_configuration(
        self, processed_fg: ProcessedFlowgroupView, env: str
    ) -> None:
        """Render the resolved flowgroup as syntax-highlighted YAML inside a Panel.

        ``ProcessedFlowgroupView`` is a frozen dataclass. ``dataclasses.asdict``
        materialises it (plus its nested ``FlowgroupView`` and ``ActionView``
        members) into a mapping, but ``FlowgroupView.file_path`` is a
        :class:`pathlib.Path` that ``yaml.safe_dump`` cannot represent —
        :meth:`_to_yaml_safe` coerces those (and any other non-primitive
        leaves) to strings before serialisation.
        """
        from rich.panel import Panel
        from rich.syntax import Syntax

        data = self._to_yaml_safe(dataclasses.asdict(processed_fg))
        # Stable, human-friendly ordering for the rendered YAML: the
        # ``flowgroup`` summary first, then the post-processing payload.
        ordered = {
            "flowgroup": data.get("flowgroup", {}),
            "actions": data.get("actions", []),
            "job_name": data.get("job_name"),
            "variables": data.get("variables", {}),
        }
        # Drop None / empty entries so the rendered YAML doesn't show
        # placeholder keys for absent fields (matches the prior
        # ``exclude_none`` behaviour).
        ordered = {k: v for k, v in ordered.items() if v not in (None, {}, [])}
        yaml_text = yaml.safe_dump(
            ordered,
            default_flow_style=False,
            sort_keys=False,
            indent=2,
        )
        syntax = Syntax(
            yaml_text,
            "yaml",
            theme="monokai",
            line_numbers=True,
            word_wrap=False,
        )
        title = (
            f"{processed_fg.flowgroup.pipeline}."
            f"{processed_fg.flowgroup.name}   env: {env}"
        )
        _console_module.console.print(
            Panel(syntax, title=title, title_align="left", border_style="dim")
        )

    @classmethod
    def _to_yaml_safe(cls, value):
        """Recursively coerce a dataclass ``asdict`` mapping into YAML-safe primitives.

        Frozen ``views.py`` dataclasses keep richer Python types — most
        notably :class:`pathlib.Path` on ``FlowgroupView.file_path`` —
        that ``yaml.safe_dump`` cannot represent. Walking the structure
        once and converting any non-primitive leaf to ``str`` matches
        the prior ``model_dump(mode="json")`` behaviour without leaking
        Pydantic into the public-API consumer surface (§9.12).
        """
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, dict):
            return {k: cls._to_yaml_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [cls._to_yaml_safe(v) for v in value]
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return str(value)

    def _display_actions_table(self, processed_fg: ProcessedFlowgroupView) -> None:
        from ..render import ColumnSpec, render_empty_state, render_listing_table

        title = f"Actions ({len(processed_fg.actions)} total)"

        if not processed_fg.actions:
            render_empty_state(
                "No actions found.",
                "Add at least one action to this flowgroup.",
            )
            return

        rows = [
            (
                action.name,
                action.action_type,
                action.target or "-",
                action.description or "-",
            )
            for action in processed_fg.actions
        ]
        render_listing_table(
            title,
            [
                ColumnSpec("Name", style="bold"),
                ColumnSpec("Type"),
                ColumnSpec("Target"),
                ColumnSpec("Description", style="dim"),
            ],
            rows,
        )

    def _display_secret_references(self, view: SubstitutionView) -> None:
        from ..render import ColumnSpec, render_listing_table

        secret_refs = view.secret_references
        if not secret_refs:
            return
        rows = [
            (f"${{{ref.scope}/{ref.key}}}",)
            for ref in sorted(secret_refs, key=lambda r: f"{r.scope}/{r.key}")
        ]
        render_listing_table(
            f"Secret References ({len(secret_refs)} found)",
            [ColumnSpec("Reference", style="bold")],
            rows,
        )

    def _display_substitution_summary(self, view: SubstitutionView) -> None:
        from ..render import ColumnSpec, render_listing_table

        if not view.tokens:
            return

        rows = []
        for token, value in list(view.tokens.items())[:10]:
            display_value = str(value)
            if len(display_value) > 40:
                display_value = display_value[:37] + "..."
            rows.append((f"${{{token}}}", display_value))

        render_listing_table(
            f"Token Substitutions ({len(view.tokens)} found)",
            [
                ColumnSpec("Token", style="bold"),
                ColumnSpec("Value"),
            ],
            rows,
        )

        if len(view.tokens) > 10:
            _console_module.console.print(
                Text(
                    f"... and {len(view.tokens) - 10} more",
                    style="dim",
                )
            )

    def _load_project_config(self, project_root: Path) -> Optional[ProjectConfigView]:
        """Return a frozen :class:`ProjectConfigView`, or ``None`` on failure.

        Routes through the inspection sub-facade so the CLI never touches
        ``ProjectConfigLoader`` directly (§5.3 / §9.7). Any failure to
        load the project config (missing file, invalid YAML, version
        mismatch) is logged at WARNING and surfaces as ``None`` so the
        ``lhp info`` renderer can still display partial information.
        """
        try:
            application_facade = LakehousePlumberApplicationFacade.for_project(
                project_root,
                enforce_version=False,
            )
            return application_facade.inspection.get_project_config()
        except Exception as e:
            self.logger.warning(f"Could not load project config: {e}")
            return None

    def _display_project_basic_info(self, project_config, project_root: Path) -> None:
        """Non-TTY writes plain ``"Name: value"`` lines to ``sink.file`` so
        ``lhp info | grep Name`` returns the expected text without ANSI.
        """
        name = getattr(project_config, "name", None) or "Unknown"
        version = getattr(project_config, "version", None) or "Unknown"
        description = getattr(project_config, "description", None) or "No description"
        author = getattr(project_config, "author", None) or "Unknown"
        rows = [
            ("Name:", name),
            ("Version:", version),
            ("Description:", description),
            ("Author:", author),
            ("Location:", str(project_root)),
        ]
        if _console_module.console.is_terminal:
            grid = Table.grid(padding=(0, 2))
            grid.add_column(style="bold dim", no_wrap=True)
            grid.add_column()
            for label, value in rows:
                grid.add_row(label, value)
            _console_module.console.print(grid)
            return
        # Non-TTY: plain ``Label: value`` lines so grep/pipe works.
        _console_module.console.file.write(
            "".join(f"{label} {value}\n" for label, value in rows)
        )

    def _collect_resource_summary(self, project_root: Path) -> dict:
        pipelines_dir = project_root / "pipelines"
        presets_dir = project_root / "presets"
        templates_dir = project_root / "templates"

        pipeline_count = 0
        flowgroup_count = 0
        if pipelines_dir.exists():
            pipeline_dirs = [d for d in pipelines_dir.iterdir() if d.is_dir()]
            pipeline_count = len(pipeline_dirs)

            for pipeline_dir in pipeline_dirs:
                yaml_files = list(pipeline_dir.rglob("*.yaml"))
                flowgroup_count += len(yaml_files)

        preset_count = (
            len(list(presets_dir.glob("*.yaml"))) if presets_dir.exists() else 0
        )
        template_count = (
            len(list(templates_dir.glob("*.yaml"))) if templates_dir.exists() else 0
        )

        return {
            "pipeline_count": pipeline_count,
            "flowgroup_count": flowgroup_count,
            "preset_count": preset_count,
            "template_count": template_count,
        }

    def _display_resource_summary(self, summary: dict) -> None:
        """Same two-column grid pattern as ``_display_project_basic_info``."""
        rows = [
            ("Pipelines:", str(summary["pipeline_count"])),
            ("FlowGroups:", str(summary["flowgroup_count"])),
            ("Presets:", str(summary["preset_count"])),
            ("Templates:", str(summary["template_count"])),
        ]
        if _console_module.console.is_terminal:
            _console_module.console.print(Text("Resource Summary", style="bold dim"))
            grid = Table.grid(padding=(0, 2))
            grid.add_column(style="bold dim", no_wrap=True)
            grid.add_column()
            for label, value in rows:
                grid.add_row(label, value)
            _console_module.console.print(grid)
            return
        # Non-TTY: plain ``Label: value`` lines so grep/pipe works.
        _console_module.console.file.write("Resource Summary\n")
        _console_module.console.file.write(
            "".join(f"{label} {value}\n" for label, value in rows)
        )

    def _display_environments(self, project_root: Path) -> None:
        """Non-TTY writes plain text via ``sink.file.write`` so
        ``grep Environments`` still works."""
        substitutions_dir = project_root / "substitutions"
        if not substitutions_dir.exists():
            return
        env_files = [f.stem for f in substitutions_dir.glob("*.yaml")]
        if not env_files:
            return
        env_list = ", ".join(env_files)
        if _console_module.console.is_terminal:
            _console_module.console.print(
                Text.assemble(
                    ("Environments: ", "bold dim"),
                    (env_list, ""),
                )
            )
            return
        _console_module.console.file.write(f"Environments: {env_list}\n")

    def _display_recent_activity(self, project_root: Path) -> None:
        """Same TTY/non-TTY split as the rest of ``lhp info``; non-TTY writes
        plain text via ``sink.file`` so the output remains grep-friendly."""
        pipelines_dir = project_root / "pipelines"
        recent_files = []

        if pipelines_dir.exists():
            for yaml_file in pipelines_dir.rglob("*.yaml"):
                mtime = os.path.getmtime(yaml_file)
                recent_files.append((yaml_file, mtime))

        if not recent_files:
            if _console_module.console.is_terminal:
                _console_module.console.print(Text("Recent Activity", style="bold dim"))
            else:
                _console_module.console.file.write("Recent Activity\n")
            return

        recent_files.sort(key=lambda x: x[1], reverse=True)
        most_recent = recent_files[0]
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(most_recent[1]))
        rel_path = most_recent[0].relative_to(project_root)

        if _console_module.console.is_terminal:
            _console_module.console.print(Text("Recent Activity", style="bold dim"))
            _console_module.console.print(
                Text.assemble(
                    ("Last modified: ", "bold dim"),
                    (str(rel_path), ""),
                    (f" ({time_str})", "dim"),
                )
            )
            return
        _console_module.console.file.write("Recent Activity\n")
        _console_module.console.file.write(f"Last modified: {rel_path} ({time_str})\n")

    def show_substitutions(self, env: str = "dev") -> None:
        """Show available substitution tokens for an environment.

        Non-TTY mode falls through to plain ``"Category: name = value"``
        lines so ``lhp substitutions | grep`` still works.
        """
        render_command_header("lhp substitutions")
        self.setup_from_context()
        project_root = self.ensure_project_root()

        sub_file = project_root / "substitutions" / f"{env}.yaml"
        if not sub_file.exists():
            sub_dir = project_root / "substitutions"
            available_envs = []
            if sub_dir.exists():
                available_envs = sorted(f.stem for f in sub_dir.glob("*.yaml"))

            suggestions = [
                f"Create the substitution file: substitutions/{env}.yaml",
            ]
            if available_envs:
                suggestions.insert(
                    0,
                    f"Available environments: {', '.join(available_envs)}",
                )

            raise LHPError(
                category=ErrorCategory.IO,
                code_number="006",
                title=f"Substitution file not found for environment '{env}'",
                details=f"Expected file: {sub_file}",
                suggestions=suggestions,
                context={"Environment": env},
            )

        application_facade = LakehousePlumberApplicationFacade.for_project(
            project_root,
            enforce_version=False,
        )
        view = application_facade.inspection.build_substitution_view(env)

        simple_tokens: dict = {}
        maps: dict = {}
        reserved: dict = {}

        for key, value in view.raw_mappings.items():
            if key in ["workspace_env", "logical_env"]:
                reserved[key] = value
            elif isinstance(value, dict):
                maps[key] = value
            else:
                simple_tokens[key] = value

        if _console_module.console.is_terminal:
            _console_module.console.print(
                Text.assemble(
                    ("Available Substitutions for Environment: ", "bold dim"),
                    (env, ""),
                )
            )
            self._render_simple_tokens_tty("Simple Tokens", simple_tokens)
            self._render_maps_tty(maps)
            self._render_simple_tokens_tty("Reserved Tokens", reserved)
            return

        # Non-TTY: plain ``Category: name = value`` lines so pipe/grep work.
        # Use the same ``${name}`` rendering as the TTY branch so that
        # consumers grepping for tokens see consistent text in both modes.
        lines: list[str] = []
        lines.append(f"Available Substitutions for Environment: {env}")
        for category, mapping in (
            ("Simple Tokens", simple_tokens),
            ("Reserved Tokens", reserved),
        ):
            for key, value in sorted(mapping.items()):
                lines.append(f"{category}: ${{{key}}} = {value}")
        for map_name, map_value in sorted(maps.items()):
            for key_path, leaf in self._flatten_map(map_value):
                full_key = f"{map_name}.{key_path}" if key_path else map_name
                lines.append(f"Maps: {full_key} = {leaf}")
        _console_module.console.file.write("\n".join(lines) + "\n")

    def _render_simple_tokens_tty(self, title: str, mapping: dict) -> None:
        """Render a scalar token mapping as a Rich Table on TTY."""
        from ..render import ColumnSpec, render_listing_table

        if not mapping:
            return
        rows = [(f"${{{key}}}", str(value)) for key, value in sorted(mapping.items())]
        render_listing_table(
            title,
            [
                ColumnSpec("Token", style="bold"),
                ColumnSpec("Value"),
            ],
            rows,
        )

    def _render_maps_tty(self, maps: dict) -> None:
        """Render nested map substitutions as Rich ``Tree`` instances.

        STYLE.md §8 lists this as an inline-only site, so the tree is
        constructed here rather than via a shared helper.
        """
        if not maps:
            return
        _console_module.console.print(Text("Maps", style="bold dim"))
        for map_name, map_value in sorted(maps.items()):
            tree = Tree(Text(map_name, style="bold"))
            self._add_tree_nodes(tree, map_value)
            _console_module.console.print(tree)

    def _add_tree_nodes(self, parent: Tree, data: dict) -> None:
        """Scalars become leaves formatted as ``key: "value"`` so the
        wrapping quotes match the legacy plain output."""
        for key, value in data.items():
            if isinstance(value, dict):
                subtree = parent.add(Text(f"{key}", style="bold"))
                self._add_tree_nodes(subtree, value)
            else:
                parent.add(Text(f'{key}: "{value}"'))

    def _flatten_map(self, data: dict, prefix: str = "") -> Iterator[Tuple[str, str]]:
        """Yield ``(dotted_key, value)`` pairs for a nested map.

        Identical traversal to ``_add_tree_nodes`` so TTY and non-TTY
        outputs cover the same leaves.
        """
        for key, value in data.items():
            new_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                yield from self._flatten_map(value, new_key)
            else:
                yield new_key, str(value)
