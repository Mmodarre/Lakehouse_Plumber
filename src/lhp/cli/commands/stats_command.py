"""Stats command implementation for LakehousePlumber CLI."""

import logging
from collections import defaultdict
from typing import Dict, Iterable, Mapping, Optional, Sequence, Tuple

from rich.table import Table
from rich.text import Text

from lhp.api import (
    FlowgroupView,
    LakehousePlumberApplicationFacade,
    PipelineStats,
    StatsResult,
)

from .. import console as _console_module
from ..render import render_command_header
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


class StatsCommand(BaseCommand):
    """Pipeline statistics and complexity metrics."""

    def execute(self, pipeline: Optional[str] = None) -> None:
        """Run the stats command for one pipeline or all of them.

        Routes every domain call through
        :class:`LakehousePlumberApplicationFacade` so the CLI never
        touches internal ``lhp.core`` / ``lhp.parsers`` modules
        (constitution §1.10 / §9.13). When ``pipeline`` is ``None``
        the global :meth:`InspectionFacade.compute_stats` result is
        rendered directly; when scoped to a single pipeline the
        per-flowgroup view tuple from
        :meth:`InspectionFacade.list_flowgroups` is re-aggregated
        into a single-pipeline :class:`StatsResult` for uniform
        rendering downstream.
        """
        render_command_header("lhp stats")
        self.setup_from_context()
        project_root = self.ensure_project_root()

        logger.debug(f"Stats request: pipeline={pipeline}")

        # YAML walk is the only potentially-slow phase; wrap the facade
        # call in a stderr spinner so stdout stays clean for piping.
        with _console_module.err_console.status("Analyzing pipelines…"):
            application_facade = LakehousePlumberApplicationFacade.for_project(
                project_root
            )

            if pipeline is None:
                stats = application_facade.inspection.compute_stats()
                single_pipeline_breakdown: Tuple[Tuple[str, int], ...] = ()
            else:
                flowgroups = application_facade.inspection.list_flowgroups(
                    pipeline_filter=pipeline
                )
                if not flowgroups:
                    _console_module.err_console.print(
                        Text.assemble(
                            ("✗ ", "bold red"),
                            f"Pipeline '{pipeline}' not found",
                        )
                    )
                    return
                stats, single_pipeline_breakdown = self._aggregate_single_pipeline(
                    pipeline, flowgroups
                )

        if stats.pipeline_count == 0:
            _console_module.err_console.print(
                Text.assemble(("✗ ", "bold red"), "No pipelines found")
            )
            return

        logger.info(
            f"Stats collected: {stats.pipeline_count} pipelines, "
            f"{stats.flowgroup_count} flowgroups, {stats.total_actions} actions"
        )
        self._display_statistics_summary(
            stats,
            single_pipeline_name=pipeline,
            single_pipeline_breakdown=single_pipeline_breakdown,
        )

    @staticmethod
    def _aggregate_single_pipeline(
        pipeline_name: str,
        flowgroups: Sequence[FlowgroupView],
    ) -> Tuple[StatsResult, Tuple[Tuple[str, int], ...]]:
        """Re-aggregate filtered flowgroups into a single-pipeline ``StatsResult``.

        Sub-type granularity (``load_<source>`` / ``transform_<kind>``)
        is intentionally omitted in single-pipeline mode — the public
        :class:`FlowgroupView` deliberately summarises actions by type
        only. The returned breakdown tuple of ``(flowgroup_name,
        action_count)`` rows powers the inline per-pipeline detail
        block emitted just above the summary.
        """
        action_counts: Dict[str, int] = defaultdict(int)
        templates_used: set[str] = set()
        presets_used: set[str] = set()
        breakdown_rows: list[Tuple[str, int]] = []
        total_actions = 0

        for fg in flowgroups:
            fg_actions = (
                fg.load_action_count
                + fg.transform_action_count
                + fg.write_action_count
                + fg.test_action_count
            )
            total_actions += fg_actions
            action_counts["load"] += fg.load_action_count
            action_counts["transform"] += fg.transform_action_count
            action_counts["write"] += fg.write_action_count
            if fg.test_action_count:
                action_counts["test"] += fg.test_action_count
            if fg.template:
                templates_used.add(fg.template)
            for preset in fg.presets:
                presets_used.add(preset)
            breakdown_rows.append((fg.name, fg_actions))

        stats = StatsResult(
            pipeline_count=1,
            flowgroup_count=len(flowgroups),
            total_actions=total_actions,
            action_counts_by_type=dict(action_counts),
            pipeline_breakdown=(
                PipelineStats(
                    pipeline_name=pipeline_name,
                    flowgroup_count=len(flowgroups),
                    total_actions=total_actions,
                ),
            ),
            templates_used=tuple(sorted(templates_used)),
            presets_used=tuple(sorted(presets_used)),
        )
        return stats, tuple(breakdown_rows)

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

    def _display_statistics_summary(
        self,
        stats: StatsResult,
        *,
        single_pipeline_name: Optional[str] = None,
        single_pipeline_breakdown: Tuple[Tuple[str, int], ...] = (),
    ) -> None:
        if single_pipeline_name is not None:
            self._display_single_pipeline_detail(
                single_pipeline_name,
                single_pipeline_breakdown,
                stats.total_actions,
            )

        load_count = stats.action_counts_by_type.get("load", 0)
        transform_count = stats.action_counts_by_type.get("transform", 0)
        write_count = stats.action_counts_by_type.get("write", 0)

        # ``Key: value`` lines so non-TTY consumers
        # (``lhp stats | grep Total``) see plain text.
        self._emit_section_header("Summary Statistics")
        self._emit_kv_lines(
            [
                ("Total pipelines", str(stats.pipeline_count)),
                ("Total flowgroups", str(stats.flowgroup_count)),
                ("Total actions", str(stats.total_actions)),
                ("Load actions", str(load_count)),
                ("Transform actions", str(transform_count)),
                ("Write actions", str(write_count)),
            ]
        )

        action_breakdown = self._collect_action_type_breakdown(
            stats.action_counts_by_type
        )
        if action_breakdown:
            self._emit_section_header("Action Type Breakdown")
            self._emit_kv_lines(action_breakdown)

        if stats.presets_used:
            self._emit_section_header("Presets Used")
            self._emit_kv_lines([("Presets", ", ".join(stats.presets_used))])

        if stats.templates_used:
            self._emit_section_header("Templates Used")
            self._emit_kv_lines([("Templates", ", ".join(stats.templates_used))])

        if stats.flowgroup_count > 0:
            self._display_complexity_metrics(stats)

    @staticmethod
    def _collect_action_type_breakdown(
        action_counts_by_type: Mapping[str, int],
    ) -> list[Tuple[str, str]]:
        """Surface only sub-type rows (``load_<src>``, ``transform_<kind>``).

        Top-level ``load`` / ``transform`` / ``write`` / ``test`` totals
        are emitted in the Summary block above; the breakdown panel
        repeats only the finer-grained sub-type keys that the API
        layer attaches to :attr:`StatsResult.action_counts_by_type`
        for the global aggregate.
        """
        top_level = {"load", "transform", "write", "test"}
        return [
            (action_type, str(count))
            for action_type, count in sorted(action_counts_by_type.items())
            if action_type not in top_level
        ]

    def _display_single_pipeline_detail(
        self,
        pipeline_name: str,
        breakdown: Tuple[Tuple[str, int], ...],
        total_actions: int,
    ) -> None:
        self._emit_section_header(f"Pipeline: {pipeline_name}")
        if breakdown:
            self._emit_kv_lines(
                [
                    (f"FlowGroup {fg_name}", f"{action_count} actions")
                    for fg_name, action_count in breakdown
                ]
            )
        self._emit_kv_lines([("Total actions", str(total_actions))])

    def _display_complexity_metrics(self, stats: StatsResult) -> None:
        """Per STYLE.md §8 the complexity table is an inline one-off."""
        avg_actions_per_flowgroup = stats.total_actions / stats.flowgroup_count

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
