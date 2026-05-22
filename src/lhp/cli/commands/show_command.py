"""Show command implementation for LakehousePlumber CLI."""

import logging
import os
import time
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

import click
import yaml

from ...core.orchestrator import ActionOrchestrator
from ...parsers.yaml_parser import YAMLParser
from ...utils.substitution import EnhancedSubstitutionManager
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


class ShowCommand(BaseCommand):
    """
    Handles show and info commands for LakehousePlumber CLI.

    Provides detailed information about flowgroups, project configuration,
    and resolved configurations with substitutions applied.
    """

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
        """
        Show resolved configuration for a specific flowgroup.

        Args:
            flowgroup: Name of the flowgroup to show
            env: Environment to resolve configuration for
        """
        self.setup_from_context()
        project_root = self.ensure_project_root()

        logger.debug(f"Show flowgroup request: flowgroup={flowgroup}, env={env}")

        # Find the flowgroup file
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

        # Parse and process flowgroup
        logger.debug(f"Found flowgroup file: {flowgroup_file}")
        fg = self._parse_flowgroup(flowgroup_file)
        substitution_mgr = self._load_substitution_manager(project_root, env)
        processed_fg = self._process_flowgroup(fg, substitution_mgr, project_root)

        # Render resolved flowgroup as YAML in a syntax-highlighted panel.
        self._display_flowgroup_configuration(processed_fg, env)

    def show_instance(self, instance_path_str: str, env: str = "dev") -> None:
        """Show resolved configuration for the flowgroups produced by an instance.

        M4 fix: at 80-instance scale, debugging a misconfigured instance would
        otherwise require a full ``lhp generate`` run or mentally applying the
        instance's parameters to the blueprint by hand. This command expands
        only the named instance and prints the resolved flowgroups in the
        existing show-flowgroup format, so the round-trip is sub-second.
        """
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

        click.echo(
            f"🔍 Resolving instance '{instance_path.name}' in environment '{env}'"
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
        click.echo(f"\n📐 Blueprint: {instance.blueprint_name}")
        click.echo(f"📊 Flowgroups produced: {len(contexts)}")

        for ctx in contexts:
            fg = ctx.flowgroup
            click.echo("")
            click.echo("=" * 70)
            click.echo(f"Pipeline: {fg.pipeline}    Flowgroup: {fg.flowgroup}")
            click.echo("=" * 70)
            processed = self._process_flowgroup(fg, substitution_mgr, project_root)
            self._display_flowgroup_configuration(processed, env)
            self._display_actions_table(processed)

        self._display_secret_references(substitution_mgr)
        self._display_substitution_summary(substitution_mgr)

    def show_project_info(self) -> None:
        """Display comprehensive project information and statistics."""
        self.setup_from_context()
        project_root = self.ensure_project_root()

        click.echo("📊 LakehousePlumber Project Information")
        click.echo("=" * 60)

        # Load and display project configuration
        project_config = self._load_project_config(project_root)
        self._display_project_basic_info(project_config, project_root)

        # Display resource summary
        resource_summary = self._collect_resource_summary(project_root)
        self._display_resource_summary(resource_summary)

        # Display environments
        self._display_environments(project_root)

        # Display recent activity
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
        """Find the YAML file containing the specified flowgroup.

        Supports multi-document (---) and flowgroups array syntax.
        """
        for yaml_file, fg_name in self._iter_yaml_flowgroups(project_root):
            if fg_name == flowgroup:
                return yaml_file
        return None

    def _collect_available_flowgroups(self, project_root: Path) -> List[str]:
        """Collect available flowgroup names from the project."""
        return [fg_name for _, fg_name in self._iter_yaml_flowgroups(project_root)]

    def _parse_flowgroup(self, flowgroup_file: Path):
        """Parse flowgroup file."""
        parser = YAMLParser()
        return parser.parse_flowgroup(flowgroup_file)

    def _load_substitution_manager(self, project_root: Path, env: str):
        """Load substitution manager for environment."""
        substitution_file = project_root / "substitutions" / f"{env}.yaml"
        if not substitution_file.exists():
            click.echo(f"⚠ Warning: Substitution file not found: {substitution_file}")
            return EnhancedSubstitutionManager(env=env)
        else:
            return EnhancedSubstitutionManager(substitution_file, env)

    def _process_flowgroup(self, fg, substitution_mgr, project_root: Path):
        """Process flowgroup with templates and presets."""
        orchestrator = ActionOrchestrator(project_root, enforce_version=False)
        return orchestrator.process_flowgroup(fg, substitution_mgr)

    def _display_flowgroup_configuration(self, processed_fg, env: str) -> None:
        """Render the resolved flowgroup as syntax-highlighted YAML inside a Panel.

        The flowgroup is dumped with Pydantic ``model_dump(mode="json")`` so
        enum values (e.g. ``ActionType``) serialize as primitive strings that
        ``yaml.safe_dump`` can handle. The result is wrapped in a Rich
        ``Syntax`` (YAML, monokai theme, line numbers) and a ``Panel`` whose
        title identifies the pipeline, flowgroup and environment.
        """
        from rich.panel import Panel
        from rich.syntax import Syntax

        from .. import console as _console_module

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
        """Display actions in table format."""
        click.echo(f"\n📊 Actions ({len(processed_fg.actions)} total)")
        click.echo("─" * 80)

        if not processed_fg.actions:
            click.echo("No actions found")
            return

        # Calculate column widths
        name_width = max(len(a.name) for a in processed_fg.actions) + 2
        type_width = 12
        target_width = max(len(a.target or "-") for a in processed_fg.actions) + 2

        # Header
        click.echo(
            f"{'Name':<{name_width}} │ {'Type':<{type_width}} │ "
            f"{'Target':<{target_width}} │ Description"
        )
        click.echo("─" * 80)

        # Actions
        for action in processed_fg.actions:
            name = action.name
            action_type = action.type.value
            target = action.target or "-"
            description = action.description or "-"

            # Truncate description if too long
            max_desc_width = 80 - name_width - type_width - target_width - 9
            if len(description) > max_desc_width:
                description = description[: max_desc_width - 3] + "..."

            click.echo(
                f"{name:<{name_width}} │ {action_type:<{type_width}} │ "
                f"{target:<{target_width}} │ {description}"
            )

        click.echo("─" * 80)

    def _display_secret_references(self, substitution_mgr) -> None:
        """Display secret references found in configuration."""
        secret_refs = substitution_mgr.secret_references
        if secret_refs:
            click.echo(f"\n🔐 Secret References ({len(secret_refs)} found)")
            click.echo("─" * 60)
            for ref in sorted(secret_refs, key=lambda r: f"{r.scope}/{r.key}"):
                click.echo(f"   ${{{ref.scope}/{ref.key}}}")

    def _display_substitution_summary(self, substitution_mgr) -> None:
        """Display substitution token summary."""
        if substitution_mgr.mappings:
            click.echo(
                f"\n🔄 Token Substitutions ({len(substitution_mgr.mappings)} found)"
            )
            click.echo("─" * 60)
            for token, value in list(substitution_mgr.mappings.items())[:10]:
                # Truncate long values for display
                display_value = str(value)
                if len(display_value) > 40:
                    display_value = display_value[:37] + "..."
                click.echo(f"   ${{{token}}} → {display_value}")

            if len(substitution_mgr.mappings) > 10:
                click.echo(f"   ... and {len(substitution_mgr.mappings) - 10} more")

    def _load_project_config(self, project_root: Path):
        """Load project configuration via the canonical ProjectConfigLoader.

        Returns the ``ProjectConfig`` Pydantic model (or ``None`` if the file
        is missing or fails to load — matching the previous best-effort
        behaviour of ``lhp info``).
        """
        try:
            from ...core.project_config_loader import ProjectConfigLoader

            return ProjectConfigLoader(project_root).load_project_config()
        except Exception as e:
            self.logger.warning(f"Could not load project config: {e}")
            return None

    def _display_project_basic_info(self, project_config, project_root: Path) -> None:
        """Display basic project information."""
        name = getattr(project_config, "name", None) or "Unknown"
        version = getattr(project_config, "version", None) or "Unknown"
        description = getattr(project_config, "description", None) or "No description"
        author = getattr(project_config, "author", None) or "Unknown"
        click.echo(f"Name:        {name}")
        click.echo(f"Version:     {version}")
        click.echo(f"Description: {description}")
        click.echo(f"Author:      {author}")
        click.echo(f"Location:    {project_root}")

    def _collect_resource_summary(self, project_root: Path) -> dict:
        """Collect summary statistics about project resources."""
        pipelines_dir = project_root / "pipelines"
        presets_dir = project_root / "presets"
        templates_dir = project_root / "templates"

        # Count pipelines and flowgroups
        pipeline_count = 0
        flowgroup_count = 0
        if pipelines_dir.exists():
            pipeline_dirs = [d for d in pipelines_dir.iterdir() if d.is_dir()]
            pipeline_count = len(pipeline_dirs)

            for pipeline_dir in pipeline_dirs:
                yaml_files = list(pipeline_dir.rglob("*.yaml"))
                flowgroup_count += len(yaml_files)

        # Count other resources
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
        """Display resource summary statistics."""
        click.echo("\n📈 Resource Summary:")
        click.echo(f"   Pipelines:  {summary['pipeline_count']}")
        click.echo(f"   FlowGroups: {summary['flowgroup_count']}")
        click.echo(f"   Presets:    {summary['preset_count']}")
        click.echo(f"   Templates:  {summary['template_count']}")

    def _display_environments(self, project_root: Path) -> None:
        """Display available environments."""
        substitutions_dir = project_root / "substitutions"
        if substitutions_dir.exists():
            env_files = [f.stem for f in substitutions_dir.glob("*.yaml")]
            if env_files:
                click.echo(f"\n🌍 Environments: {', '.join(env_files)}")

    def _display_recent_activity(self, project_root: Path) -> None:
        """Display recent activity information."""
        click.echo("\n📅 Recent Activity:")

        # Find most recently modified flowgroup
        pipelines_dir = project_root / "pipelines"
        recent_files = []

        if pipelines_dir.exists():
            for yaml_file in pipelines_dir.rglob("*.yaml"):
                mtime = os.path.getmtime(yaml_file)
                recent_files.append((yaml_file, mtime))

        if recent_files:
            recent_files.sort(key=lambda x: x[1], reverse=True)
            most_recent = recent_files[0]
            time_str = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(most_recent[1])
            )
            rel_path = most_recent[0].relative_to(project_root)
            click.echo(f"   Last modified: {rel_path} ({time_str})")

    def show_substitutions(self, env: str = "dev") -> None:
        """
        Show available substitution tokens for an environment.

        Args:
            env: Environment to show substitutions for
        """
        self.setup_from_context()
        project_root = self.ensure_project_root()

        # Load substitutions
        sub_file = project_root / "substitutions" / f"{env}.yaml"
        if not sub_file.exists():
            from ...utils.error_formatter import ErrorCategory, LHPError

            # Discover available environments for suggestions
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

        # Display
        click.echo(f"\n📋 Available Substitutions for Environment: {env}")
        click.echo("═" * 60)

        # Separate simple tokens and maps
        simple_tokens = {}
        maps = {}
        reserved = {}

        for key, value in mgr.mappings.items():
            if key in ["workspace_env", "logical_env"]:
                reserved[key] = value
            elif isinstance(value, dict):
                maps[key] = value
            else:
                simple_tokens[key] = value

        # Display simple tokens
        if simple_tokens:
            click.echo("\n✨ Simple Tokens:")
            for key, value in sorted(simple_tokens.items()):
                click.echo(f'  ${{<KEY>}}: "{value}"'.replace("<KEY>", key))

        # Display maps (with tree structure)
        if maps:
            click.echo("\n📦 Maps:")
            for map_name, map_value in sorted(maps.items()):
                click.echo(f"  {map_name}:")
                self._display_dict_tree(map_value, indent=4)

        # Display reserved
        if reserved:
            click.echo("\n🔒 Reserved Tokens:")
            for key, value in sorted(reserved.items()):
                click.echo(f'  ${{<KEY>}}: "{value}"'.replace("<KEY>", key))

        click.echo("")

    def _display_dict_tree(
        self, data: dict, indent: int = 0, is_last: bool = True
    ) -> None:
        """Display dict as tree structure."""
        items = list(data.items())
        for i, (key, value) in enumerate(items):
            is_last_item = i == len(items) - 1
            prefix = " " * indent + ("└─ " if is_last_item else "├─ ")

            if isinstance(value, dict):
                click.echo(f"{prefix}{key}:")
                self._display_dict_tree(value, indent + 2, is_last_item)
            else:
                click.echo(f'{prefix}{key}: "{value}"')
