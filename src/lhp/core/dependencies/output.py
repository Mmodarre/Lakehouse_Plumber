"""Pure serializers for dependency analysis results.

This module exposes the pure, side-effect-free rendering surface for
dependency analysis results: three module-level serializers
(``export_to_dot``, ``export_to_json``, ``export_to_text``) plus the
shared text renderers (``_generate_text_representation`` /
``_generate_dependency_tree_text``). The :class:`DependencyOutputFormatter`
class wraps these as instance methods so the disk-writing
:class:`~lhp.core.dependencies.output_writer.DependencyOutputWriter` can
compose them. No file I/O lives here.
"""

from datetime import datetime
from typing import Any, Dict, List

from ...models.dependencies import (
    AffectedAction,
    DependencyAnalysisResult,
    DependencyGraphs,
    DependencyWarning,
)


def export_to_dot(graphs: DependencyGraphs, level: str = "pipeline") -> str:
    """Export a dependency graph to DOT format.

    Args:
        graphs: Dependency graphs (action / flowgroup / pipeline triple)
        level: Graph level to export (``"action"``, ``"flowgroup"``, or ``"pipeline"``)

    Returns:
        DOT format string representation of the requested graph level.
    """
    graph = graphs.get_graph_by_level(level)

    # Use networkx built-in DOT export with custom attributes
    dot_lines = [f"digraph {level}_dependencies {{"]
    dot_lines.append("  rankdir=LR;")
    dot_lines.append("  node [shape=box];")

    for node in graph.nodes():
        node_attrs = graph.nodes[node]
        label = node

        if level == "pipeline" and "flowgroup_count" in node_attrs:
            label = f"{node}\\n({node_attrs['flowgroup_count']} flowgroups)"
        elif level == "flowgroup" and "action_count" in node_attrs:
            label = f"{node}\\n({node_attrs['action_count']} actions)"

        dot_lines.append(f'  "{node}" [label="{label}"];')

    for source, target in graph.edges():
        dot_lines.append(f'  "{source}" -> "{target}";')

    dot_lines.append("}")

    return "\n".join(dot_lines)


def export_to_json(result: DependencyAnalysisResult) -> Dict[str, Any]:
    """Export a dependency analysis result to a structured JSON-ready dict.

    Args:
        result: Complete dependency analysis result

    Returns:
        Structured dictionary suitable for ``json.dump``. The top-level
        ``"warnings"`` key is always present (empty list when no extraction
        warnings were recorded) so the schema stays stable for consumers.
        Warnings are aggregated per unresolved read SITE:
        ``metadata.total_warnings`` counts distinct sites while
        ``metadata.total_warning_occurrences`` counts (site × affected
        action) pairs — the pre-aggregation total.
    """
    return {
        "metadata": {
            "total_pipelines": result.total_pipelines,
            "total_external_sources": result.total_external_sources,
            "total_stages": len(result.execution_stages),
            "has_circular_dependencies": len(result.circular_dependencies) > 0,
            "total_warnings": len(result.warnings),
            "total_warning_occurrences": sum(
                warning.affected_count for warning in result.warnings
            ),
        },
        "pipelines": {
            name: {
                "depends_on": dep.depends_on,
                "flowgroup_count": dep.flowgroup_count,
                "action_count": dep.action_count,
                "external_sources": dep.external_sources,
                "can_run_parallel": dep.can_run_parallel,
                "stage": dep.stage,
            }
            for name, dep in result.pipeline_dependencies.items()
        },
        "execution_stages": result.execution_stages,
        "external_sources": result.external_sources,
        "circular_dependencies": result.circular_dependencies,
        "warnings": [
            {
                "code": warning.code,
                "message": warning.message,
                "flowgroup": warning.flowgroup,
                "action": warning.action,
                "suggestion": warning.suggestion,
                "file_path": warning.file_path,
                "line": warning.line,
                "edit_yaml_path": warning.edit_yaml_path,
                "affected_actions": [
                    {
                        "flowgroup": affected.flowgroup,
                        "action": affected.action,
                        "edit_yaml_path": affected.edit_yaml_path,
                    }
                    for affected in warning.affected_actions
                ],
                "affected_count": warning.affected_count,
            }
            for warning in result.warnings
        ],
    }


def export_to_text(result: DependencyAnalysisResult) -> str:
    """Render a dependency analysis result as a human-readable text report.

    Thin wrapper around the shared :func:`_generate_text_representation`
    used internally by the disk-writing writer's file-save path.
    """
    return _generate_text_representation(result)


def _generate_text_representation(result: DependencyAnalysisResult) -> str:
    """Generate human-readable text representation of dependency analysis.

    Shared by :func:`export_to_text` and
    :meth:`DependencyOutputFormatter._generate_text_representation`.
    """
    lines = []

    lines.append("=" * 80)
    lines.append("LAKEHOUSE PLUMBER - PIPELINE DEPENDENCY ANALYSIS")
    lines.append("=" * 80)
    lines.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    lines.append("SUMMARY")
    lines.append("-" * 40)
    lines.append(f"Total Pipelines: {result.total_pipelines}")
    lines.append(f"Total Execution Stages: {len(result.execution_stages)}")
    lines.append(f"External Sources: {result.total_external_sources}")
    lines.append(f"Circular Dependencies: {len(result.circular_dependencies)}")
    lines.append("")

    lines.append("EXECUTION ORDER")
    lines.append("-" * 40)
    if result.execution_stages:
        for stage_idx, stage_pipelines in enumerate(result.execution_stages, 1):
            if len(stage_pipelines) == 1:
                lines.append(f"Stage {stage_idx}: {stage_pipelines[0]}")
            else:
                lines.append(
                    f"Stage {stage_idx}: {', '.join(stage_pipelines)} (parallel)"
                )
    else:
        lines.append(
            "No pipelines found or circular dependencies prevent execution order."
        )
    lines.append("")

    lines.append("PIPELINE DETAILS")
    lines.append("-" * 40)
    for pipeline_name in sorted(result.pipeline_dependencies.keys()):
        dep = result.pipeline_dependencies[pipeline_name]
        lines.append(f"Pipeline: {pipeline_name}")
        lines.append(f"  Flowgroups: {dep.flowgroup_count}")
        lines.append(f"  Actions: {dep.action_count}")
        lines.append(
            f"  Depends on: {', '.join(dep.depends_on) if dep.depends_on else 'None'}"
        )
        lines.append(f"  Stage: {dep.stage if dep.stage is not None else 'N/A'}")
        lines.append(f"  Can run parallel: {dep.can_run_parallel}")
        if dep.external_sources:
            lines.append(f"  External sources: {', '.join(dep.external_sources[:5])}")
            if len(dep.external_sources) > 5:
                lines.append(f"    ... and {len(dep.external_sources) - 5} more")
        lines.append("")

    if result.external_sources:
        lines.append("EXTERNAL SOURCES")
        lines.append("-" * 40)
        for ext_source in sorted(result.external_sources):
            lines.append(f"  {ext_source}")
        lines.append("")

    if result.circular_dependencies:
        lines.append("CIRCULAR DEPENDENCIES")
        lines.append("-" * 40)
        lines.append("⚠️  WARNING: Circular dependencies detected!")
        lines.append("These must be resolved before pipeline execution:")
        for cycle in result.circular_dependencies:
            lines.append(f"  {cycle[0]}")
        lines.append("")

    if result.warnings:
        total_occurrences = sum(w.affected_count for w in result.warnings)
        lines.append("DEPENDENCY EXTRACTION WARNINGS")
        lines.append("-" * 40)
        lines.append(
            f"{len(result.warnings)} unresolved site(s) affecting "
            f"{total_occurrences} action(s)"
        )
        for warning in result.warnings:
            lines.extend(_warning_site_lines(warning))
        lines.append("")

    lines.append("DEPENDENCY TREE")
    lines.append("-" * 40)
    lines.extend(_generate_dependency_tree_text(result))

    return "\n".join(lines)


_MAX_AFFECTED_SHOWN = 5


def _warning_site_lines(warning: "DependencyWarning") -> List[str]:
    """Render one aggregated warning site as an indented text block.

    Shows the site location, the message, up to
    :data:`_MAX_AFFECTED_SHOWN` affected ``flowgroup.action`` pairs with an
    overflow count, and the distinct YAML file(s) a ``depends_on`` fix
    belongs in. Leaf records that never went through the builder's
    aggregation (empty ``affected_actions``) fall back to their own
    ``flowgroup``/``action``/``edit_yaml_path`` fields.
    """
    header = f"  {warning.code}"
    if warning.file_path and warning.line is not None:
        header += f" {warning.file_path}:{warning.line}"
    elif warning.file_path:
        header += f" {warning.file_path}"

    affected = warning.affected_actions or (
        AffectedAction(
            flowgroup=warning.flowgroup,
            action=warning.action,
            edit_yaml_path=warning.edit_yaml_path,
        ),
    )
    shown = [f"{a.flowgroup}.{a.action}" for a in affected[:_MAX_AFFECTED_SHOWN]]
    overflow = len(affected) - _MAX_AFFECTED_SHOWN
    if overflow > 0:
        shown.append(f"+{overflow} more")
    edit_paths = list(
        dict.fromkeys(a.edit_yaml_path for a in affected if a.edit_yaml_path)
    )

    lines = [
        header,
        f"    {warning.message}",
        f"    Affected ({warning.affected_count}): {', '.join(shown)}",
    ]
    if edit_paths:
        lines.append(f"    Add depends_on in: {', '.join(edit_paths[:3])}")
    lines.append(f"    Suggestion: {warning.suggestion}")
    return lines


def _generate_dependency_tree_text(
    result: DependencyAnalysisResult,
) -> List[str]:
    """Generate ASCII tree representation of pipeline dependencies."""
    lines: List[str] = []

    if not result.pipeline_dependencies:
        lines.append("No pipelines found.")
        return lines

    root_pipelines = [
        name for name, dep in result.pipeline_dependencies.items() if not dep.depends_on
    ]

    if not root_pipelines:
        lines.append("⚠️  No root pipelines found (possible circular dependencies)")
        return lines

    visited: set = set()

    def add_pipeline_tree(pipeline: str, indent: str = "", is_last: bool = True):
        if pipeline in visited:
            lines.append(
                f"{indent}{'└── ' if is_last else '├── '}{pipeline} (already shown)"
            )
            return

        visited.add(pipeline)
        dep = result.pipeline_dependencies.get(pipeline)
        if dep:
            info = f"{pipeline} ({dep.flowgroup_count} flowgroups, {dep.action_count} actions)"
        else:
            info = pipeline

        lines.append(f"{indent}{'└── ' if is_last else '├── '}{info}")

        dependents = [
            name
            for name, dep in result.pipeline_dependencies.items()
            if pipeline in dep.depends_on
        ]

        child_indent = indent + ("    " if is_last else "│   ")
        for i, dependent in enumerate(sorted(dependents)):
            is_last_child = i == len(dependents) - 1
            add_pipeline_tree(dependent, child_indent, is_last_child)

    for i, root_pipeline in enumerate(sorted(root_pipelines)):
        is_last_root = i == len(root_pipelines) - 1
        add_pipeline_tree(root_pipeline, "", is_last_root)

    return lines


class DependencyOutputFormatter:
    """Pure formatter for dependency analysis results (DOT/JSON/text).

    Wraps the module-level serializers as instance methods so the
    disk-writing
    :class:`~lhp.core.dependencies.output_writer.DependencyOutputWriter`
    can compose them. Holds no state and performs no I/O.
    """

    def export_to_dot(self, graphs: DependencyGraphs, level: str = "pipeline") -> str:
        return export_to_dot(graphs, level)

    def export_to_json(self, result: DependencyAnalysisResult) -> Dict[str, Any]:
        return export_to_json(result)

    def export_to_text(self, result: DependencyAnalysisResult) -> str:
        return export_to_text(result)

    def _generate_text_representation(self, result: DependencyAnalysisResult) -> str:
        return _generate_text_representation(result)

    def _generate_dependency_tree_text(
        self, result: DependencyAnalysisResult
    ) -> List[str]:
        return _generate_dependency_tree_text(result)
