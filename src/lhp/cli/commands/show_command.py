"""Show command implementation for LakehousePlumber CLI."""

import logging
import os
import time
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

import yaml
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from ...core.orchestrator import ActionOrchestrator
from ...parsers.yaml_parser import YAMLParser
from ...utils.substitution import EnhancedSubstitutionManager
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
        from ...utils.error_formatter import ErrorCategory, LHPError

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

        logger.debug(f"Searching for flowgroup file: {flowgroup}")
        flowgroup_file = self._find_flowgroup_file(flowgroup, project_root)
        if not flowgroup_file:
            from ...utils.error_formatter import ErrorCategory, LHPError

            available = self._collect_available_flowgroups(project_root)
            suggestions = [
                "Check the flowgroup name for typos",
                "Ensure a YAML file with this flowgroup exists in pipelines/",
            ]
            if available:
                suggestions.insert(
                    0,
                    f"Available flowgroups: {', '.join(sorted(available))}",
                )

            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="013",
                title=f"Flowgroup '{flowgroup}' not found",
                details=f"No flowgroup named '{flowgroup}' was found in the project.",
                suggestions=suggestions,
                context={"Flowgroup": flowgroup},
            )

        logger.debug(f"Found flowgroup file: {flowgroup_file}")
        fg = self._parse_flowgroup(flowgroup_file)
        substitution_mgr = self._load_substitution_manager(project_root, env)
        processed_fg = self._process_flowgroup(fg, substitution_mgr, project_root)

        self._display_flowgroup_configuration(processed_fg, env)

    def show_instance(self, instance_path_str: str, env: str = "dev") -> None:
        """Show resolved configuration for the flowgroups produced by an instance.

        Expands only the named instance (not the full project) so the
        round-trip stays sub-second at 80-instance scale.
        """
        render_command_header("lhp show --instance")
        self.setup_from_context()
        project_root = self.ensure_project_root()

        from ...core.project_config_loader import ProjectConfigLoader
        from ...core.services.blueprint_discoverer import BlueprintDiscoverer
        from ...core.services.blueprint_expander import BlueprintExpander
        from ...parsers.blueprint_parser import BlueprintParser

        instance_path = Path(instance_path_str)
        if not instance_path.is_absolute():
            instance_path = (project_root / instance_path).resolve()
        if not instance_path.exists():
            from ...utils.error_formatter import ErrorCategory, LHPError

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

        project_config = ProjectConfigLoader(project_root).load_project_config()
        parser = BlueprintParser()
        discoverer = BlueprintDiscoverer(
            project_root,
            project_config=project_config,
            blueprint_parser=parser,
        )
        blueprints = discoverer.discover_blueprints()
        if not blueprints:
            from ...utils.error_formatter import ErrorCategory, LHPError

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

        expander = BlueprintExpander()
        contexts, _ = expander.expand_single_instance(
            instance, instance_path, blueprints
        )

        substitution_mgr = self._load_substitution_manager(project_root, env)
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
            processed = self._process_flowgroup(fg, substitution_mgr, project_root)
            self._display_flowgroup_configuration(processed, env)
            self._display_actions_table(processed)

        self._display_secret_references(substitution_mgr)
        self._display_substitution_summary(substitution_mgr)

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

    def _iter_yaml_flowgroups(self, project_root: Path) -> Iterator[Tuple[Path, str]]:
        """Yield ``(yaml_file, flowgroup_name)`` pairs for every flowgroup
        declared under ``pipelines/``.

        Walks the include-pattern-filtered YAML files, parses each as a
        multi-document YAML stream, and emits both:

        - regular single-document files with a top-level ``flowgroup`` field
        - array syntax (a ``flowgroups`` list per document)

        YAML parse errors and file read errors are tolerated (logged at
        DEBUG and skipped) so callers see a best-effort view of the project.
        """
        pipelines_dir = project_root / "pipelines"
        if not pipelines_dir.exists():
            return

        include_patterns = self._get_include_patterns(project_root)
        yaml_files = self._discover_yaml_files_with_include(
            pipelines_dir, include_patterns
        )

        for yaml_file in yaml_files:
            try:
                with open(yaml_file, "r", encoding="utf-8") as f:
                    documents = list(yaml.safe_load_all(f))
            except (yaml.YAMLError, OSError) as e:
                logger.debug(f"Could not parse {yaml_file}: {e}")
                continue

            for doc in documents:
                if doc is None:
                    continue

                fg_name = doc.get("flowgroup")
                if fg_name:
                    yield yaml_file, fg_name

                if "flowgroups" in doc:
                    for fg_config in doc["flowgroups"]:
                        nested_name = fg_config.get("flowgroup")
                        if nested_name:
                            yield yaml_file, nested_name

    def _find_flowgroup_file(
        self, flowgroup: str, project_root: Path
    ) -> Optional[Path]:
        """Supports multi-document (---) and flowgroups array syntax."""
        for yaml_file, fg_name in self._iter_yaml_flowgroups(project_root):
            if fg_name == flowgroup:
                return yaml_file
        return None

    def _collect_available_flowgroups(self, project_root: Path) -> List[str]:
        return [fg_name for _, fg_name in self._iter_yaml_flowgroups(project_root)]

    def _parse_flowgroup(self, flowgroup_file: Path):
        parser = YAMLParser()
        return parser.parse_flowgroup(flowgroup_file)

    def _load_substitution_manager(self, project_root: Path, env: str):
        substitution_file = project_root / "substitutions" / f"{env}.yaml"
        if not substitution_file.exists():
            _console_module.err_console.print(
                Text.assemble(
                    ("⚠ ", "bold yellow"),
                    ("Warning: Substitution file not found: ", "bold yellow"),
                    (str(substitution_file), ""),
                )
            )
            return EnhancedSubstitutionManager(env=env)
        else:
            return EnhancedSubstitutionManager(substitution_file, env)

    def _process_flowgroup(self, fg, substitution_mgr, project_root: Path):
        orchestrator = ActionOrchestrator(project_root, enforce_version=False)
        return orchestrator.process_flowgroup(fg, substitution_mgr)

    def _display_flowgroup_configuration(self, processed_fg, env: str) -> None:
        """Render the resolved flowgroup as syntax-highlighted YAML inside a Panel.

        ``model_dump(mode="json")`` is used so enum values (e.g.
        ``ActionType``) serialize as primitive strings that
        ``yaml.safe_dump`` can handle.
        """
        from rich.panel import Panel
        from rich.syntax import Syntax

        if hasattr(processed_fg, "model_dump"):
            data = processed_fg.model_dump(mode="json", exclude_none=True)
        elif hasattr(processed_fg, "dict"):
            # Pydantic v1 fallback.
            data = processed_fg.dict(exclude_none=True)
        else:
            data = dict(processed_fg)

        yaml_text = yaml.safe_dump(
            data,
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
        title = f"{processed_fg.pipeline}.{processed_fg.flowgroup}   env: {env}"
        _console_module.console.print(
            Panel(syntax, title=title, title_align="left", border_style="dim")
        )

    def _display_actions_table(self, processed_fg) -> None:
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
                action.type.value,
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

    def _display_secret_references(self, substitution_mgr) -> None:
        from ..render import ColumnSpec, render_listing_table

        secret_refs = substitution_mgr.secret_references
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

    def _display_substitution_summary(self, substitution_mgr) -> None:
        from ..render import ColumnSpec, render_listing_table

        if not substitution_mgr.mappings:
            return

        rows = []
        for token, value in list(substitution_mgr.mappings.items())[:10]:
            display_value = str(value)
            if len(display_value) > 40:
                display_value = display_value[:37] + "..."
            rows.append((f"${{{token}}}", display_value))

        render_listing_table(
            f"Token Substitutions ({len(substitution_mgr.mappings)} found)",
            [
                ColumnSpec("Token", style="bold"),
                ColumnSpec("Value"),
            ],
            rows,
        )

        if len(substitution_mgr.mappings) > 10:
            _console_module.console.print(
                Text(
                    f"... and {len(substitution_mgr.mappings) - 10} more",
                    style="dim",
                )
            )

    def _load_project_config(self, project_root: Path):
        """Returns the ``ProjectConfig`` model, or ``None`` on missing/invalid file."""
        try:
            from ...core.project_config_loader import ProjectConfigLoader

            return ProjectConfigLoader(project_root).load_project_config()
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
            from ...utils.error_formatter import ErrorCategory, LHPError

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

        mgr = EnhancedSubstitutionManager(sub_file, env=env)

        simple_tokens: dict = {}
        maps: dict = {}
        reserved: dict = {}

        for key, value in mgr.mappings.items():
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
