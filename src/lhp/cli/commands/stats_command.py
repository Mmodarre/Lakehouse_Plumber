"""Stats command implementation for LakehousePlumber CLI."""

import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, Optional, Sequence, Tuple

from rich.table import Table
from rich.text import Text

from ...parsers.yaml_parser import YAMLParser
from .. import console as _console_module
from ..render import render_command_header
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


class StatsCommand(BaseCommand):
    """Pipeline statistics and complexity metrics."""

    def execute(self, pipeline: Optional[str] = None) -> None:
        """Run the stats command for one pipeline or all of them."""
        render_command_header("lhp stats")
        self.setup_from_context()
        project_root = self.ensure_project_root()
        parser = YAMLParser()

        logger.debug(f"Stats request: pipeline={pipeline}")

        pipeline_dirs = self._get_pipeline_directories(project_root, pipeline)
        if not pipeline_dirs:
            return

        total_stats = self._initialize_stats(len(pipeline_dirs))

        include_patterns = self._get_include_patterns(project_root)

        # YAML walk is the only potentially-slow phase; wrap it in a stderr
        # spinner so stdout stays clean for piping.
        with _console_module.err_console.status("Analyzing pipelines…"):
            for pipeline_dir in pipeline_dirs:
                self._analyze_pipeline(
                    pipeline_dir,
                    parser,
                    include_patterns,
                    total_stats,
                    single_pipeline=len(pipeline_dirs) == 1,
                )

        logger.info(
            f"Stats collected: {total_stats['pipelines']} pipelines, "
            f"{total_stats['flowgroups']} flowgroups, {total_stats['actions']} actions"
        )
        self._display_statistics_summary(total_stats)

    def _get_pipeline_directories(
        self, project_root: Path, pipeline: Optional[str]
    ) -> list[Path]:
        pipelines_dir = project_root / "pipelines"
        if not pipelines_dir.exists():
            _console_module.err_console.print(
                Text.assemble(("✗ ", "bold red"), "No pipelines directory found")
            )
            return []

        if pipeline:
            pipeline_dir = pipelines_dir / pipeline
            if not pipeline_dir.exists():
                _console_module.err_console.print(
                    Text.assemble(
                        ("✗ ", "bold red"),
                        f"Pipeline '{pipeline}' not found",
                    )
                )
                return []
            return [pipeline_dir]
        else:
            pipeline_dirs = [d for d in pipelines_dir.iterdir() if d.is_dir()]
            if not pipeline_dirs:
                _console_module.err_console.print(
                    Text.assemble(("✗ ", "bold red"), "No pipeline directories found")
                )
                return []
            return pipeline_dirs

    def _initialize_stats(self, pipeline_count: int) -> Dict:
        return {
            "pipelines": pipeline_count,
            "flowgroups": 0,
            "actions": 0,
            "load_actions": 0,
            "transform_actions": 0,
            "write_actions": 0,
            "secret_refs": 0,
            "templates_used": set(),
            "presets_used": set(),
            "action_types": defaultdict(int),
            # Per-pipeline rows captured for the single-pipeline detail block.
            # Cleared on entry; populated only when ``single_pipeline=True``.
            "per_pipeline_flowgroups": [],
            "single_pipeline_name": None,
            "single_pipeline_action_count": 0,
        }

    def _analyze_pipeline(
        self,
        pipeline_dir: Path,
        parser: YAMLParser,
        include_patterns: list[str],
        total_stats: Dict,
        single_pipeline: bool = False,
    ) -> None:
        pipeline_name = pipeline_dir.name
        flowgroup_files = self._discover_yaml_files_with_include(
            pipeline_dir, include_patterns
        )

        if single_pipeline:
            total_stats["single_pipeline_name"] = pipeline_name

        pipeline_actions = 0

        for yaml_file in flowgroup_files:
            try:
                flowgroup = parser.parse_flowgroup(yaml_file)
                total_stats["flowgroups"] += 1

                for action in flowgroup.actions:
                    total_stats["actions"] += 1
                    pipeline_actions += 1

                    if action.type.value == "load":
                        total_stats["load_actions"] += 1
                    elif action.type.value == "transform":
                        total_stats["transform_actions"] += 1
                    elif action.type.value == "write":
                        total_stats["write_actions"] += 1

                    if action.type.value == "load" and isinstance(action.source, dict):
                        subtype = action.source.get("type", "unknown")
                        total_stats["action_types"][f"load_{subtype}"] += 1
                    elif action.type.value == "transform" and action.transform_type:
                        total_stats["action_types"][
                            f"transform_{action.transform_type}"
                        ] += 1

                if flowgroup.presets:
                    for preset in flowgroup.presets:
                        total_stats["presets_used"].add(preset)

                if flowgroup.use_template:
                    total_stats["templates_used"].add(flowgroup.use_template)

                if single_pipeline:
                    total_stats["per_pipeline_flowgroups"].append(
                        (flowgroup.flowgroup, len(flowgroup.actions))
                    )

            except Exception as e:
                logger.warning(f"Could not parse {yaml_file}: {e}")
                # Surface parse failures on stderr so callers piping stdout
                # still see them.
                _console_module.err_console.print(
                    Text.assemble(
                        ("⚠ ", "bold yellow"),
                        f"Could not parse {yaml_file}: {e}",
                    )
                )
                continue

        if single_pipeline:
            total_stats["single_pipeline_action_count"] = pipeline_actions

    # Rendering helpers (inline per STYLE.md §8 — not promoted to render.py).

    @staticmethod
    def _emit_kv_lines(pairs: Sequence[Tuple[str, str]]) -> None:
        """Emit ``Key: value`` lines. Non-TTY bypasses Rich so no ANSI leaks."""
        if _console_module.console.is_terminal:
            for key, value in pairs:
                _console_module.console.print(
                    Text.assemble((f"{key}: ", "bold"), value)
                )
            return

        _console_module.console.file.write(
            "".join(f"{key}: {value}\n" for key, value in pairs)
        )

    @staticmethod
    def _emit_section_header(title: str) -> None:
        if _console_module.console.is_terminal:
            _console_module.console.print("")
            _console_module.console.print(Text(title, style="bold dim"))
            return
        _console_module.console.file.write(f"\n{title}\n")

    @staticmethod
    def _emit_table(
        title: str,
        columns: Sequence[Tuple[str, str]],
        rows: Iterable[Sequence[str]],
    ) -> None:
        """Inline table renderer (STYLE.md §8).

        ``columns`` is a sequence of ``(header, justify)`` pairs. Non-TTY
        emits tab-separated rows written directly to
        ``_console_module.console.file`` so embedded tabs survive.
        """
        materialized = [list(row) for row in rows]
        if _console_module.console.is_terminal:
            table = Table(
                title=title,
                border_style="dim",
                header_style="bold dim",
            )
            for header, justify in columns:
                table.add_column(header, justify=justify)  # type: ignore[arg-type]
            for row in materialized:
                table.add_row(*(str(cell) for cell in row))
            _console_module.console.print(table)
            return

        lines = [title, "\t".join(header for header, _ in columns)]
        for row in materialized:
            lines.append("\t".join(str(cell) for cell in row))
        _console_module.console.file.write("\n".join(lines) + "\n")

    def _display_statistics_summary(self, total_stats: Dict) -> None:
        if total_stats.get("single_pipeline_name"):
            self._display_single_pipeline_detail(total_stats)

        # ``Key: value`` lines so non-TTY consumers
        # (``lhp stats | grep Total``) see plain text.
        self._emit_section_header("Summary Statistics")
        self._emit_kv_lines(
            [
                ("Total pipelines", str(total_stats["pipelines"])),
                ("Total flowgroups", str(total_stats["flowgroups"])),
                ("Total actions", str(total_stats["actions"])),
                ("Load actions", str(total_stats["load_actions"])),
                ("Transform actions", str(total_stats["transform_actions"])),
                ("Write actions", str(total_stats["write_actions"])),
            ]
        )

        if total_stats["action_types"]:
            self._emit_section_header("Action Type Breakdown")
            self._emit_kv_lines(
                [
                    (action_type, str(count))
                    for action_type, count in sorted(
                        total_stats["action_types"].items()
                    )
                ]
            )

        if total_stats["presets_used"]:
            self._emit_section_header("Presets Used")
            self._emit_kv_lines(
                [("Presets", ", ".join(sorted(total_stats["presets_used"])))]
            )

        if total_stats["templates_used"]:
            self._emit_section_header("Templates Used")
            self._emit_kv_lines(
                [("Templates", ", ".join(sorted(total_stats["templates_used"])))]
            )

        if total_stats["flowgroups"] > 0:
            self._display_complexity_metrics(total_stats)

    def _display_single_pipeline_detail(self, total_stats: Dict) -> None:
        name = total_stats["single_pipeline_name"]
        self._emit_section_header(f"Pipeline: {name}")
        if total_stats["per_pipeline_flowgroups"]:
            self._emit_kv_lines(
                [
                    (f"FlowGroup {fg_name}", f"{action_count} actions")
                    for fg_name, action_count in total_stats["per_pipeline_flowgroups"]
                ]
            )
        self._emit_kv_lines(
            [("Total actions", str(total_stats["single_pipeline_action_count"]))]
        )

    def _display_complexity_metrics(self, total_stats: Dict) -> None:
        """Per STYLE.md §8 the complexity table is an inline one-off."""
        avg_actions_per_flowgroup = total_stats["actions"] / total_stats["flowgroups"]

        if avg_actions_per_flowgroup < 3:
            complexity = "Low"
        elif avg_actions_per_flowgroup < 7:
            complexity = "Medium"
        else:
            complexity = "High"

        # Spacer between the preceding kv block and the table.
        if _console_module.console.is_terminal:
            _console_module.console.print("")
        else:
            _console_module.console.file.write("\n")

        self._emit_table(
            "Complexity Metrics",
            columns=[("Metric", "left"), ("Value", "right")],
            rows=[
                ("Average actions per flowgroup", f"{avg_actions_per_flowgroup:.1f}"),
                ("Overall complexity", complexity),
            ],
        )
