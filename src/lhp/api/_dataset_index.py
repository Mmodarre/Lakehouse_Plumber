"""Private implementation of the project dataset index (table→table lineage).

Underscore-prefixed: not part of :mod:`lhp.api`'s public surface. External
callers reach this only through :meth:`DependencyFacade.build_dataset_index`.

Builds a per-environment index of every produced dataset (table / sink) with
its lineage by joining the env-resolved write :class:`~lhp.api.views.ActionView`
objects (from :meth:`InspectionFacade.process_flowgroup`) onto the
substitution-agnostic action dependency graph (``core.dependencies``) by action
id ``{pipeline}.{flowgroup}.{action.name}``.

The dependency graph never resolves ``${tokens}`` (the dep service builds its
substitution manager with ``substitution_file=None`` — see
``core/dependencies/service.py``), so an action name that embeds a token misses
the id join. A positional fallback matches the flowgroup's env-resolved write
actions onto its graph write nodes in definition order and records a warning
(backend risk 1). Per-flowgroup resolution failures are collected as warnings
and skipped, mirroring ``GET /api/tables``.

A delta sink that writes a real table via ``options.tableName`` is indexed
under its ``sink:<type>/<id>`` id, never the underlying table FQN, so lineage
lookups by that table's FQN miss it (documented v1 limitation).

:stability: internal
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional, cast

from lhp.api._inspection_facade import InspectionFacade
from lhp.api.responses import DatasetIndexResult
from lhp.api.views import (
    ActionView,
    DatasetConsumerView,
    DatasetView,
    LineageEdgeView,
    LineageNodeView,
)

logger = logging.getLogger(__name__)

# Stable ordering for the lineage node projection: sources first, then the
# action chain in execution order. Keeps the DTO (and its fingerprint)
# deterministic regardless of graph iteration order.
_KIND_ORDER = {
    "external": 0,
    "dataset": 1,
    "load": 2,
    "transform": 3,
    "write": 4,
    "test": 5,
}


@dataclass(frozen=True)
class _WriteBinding:
    """One env-resolved write action bound to its action-graph node id."""

    node_id: str
    view: ActionView
    pipeline: str
    flowgroup: str
    source_file: str
    fqn: str


def _build_dataset_index(
    orchestrator: Any, *, env: str, force_rebuild: bool = False
) -> DatasetIndexResult:
    """Build the per-env dataset lineage index (see module docstring).

    ``orchestrator`` is the facade's internal orchestrator (typed ``Any`` so
    no internal type is named on this seam, per §1.10). The action graph is
    fetched through the memoized / serve-stale ``analyze_project`` path;
    ``force_rebuild`` bypasses both the in-process memo and the on-disk cache.
    """
    internal = orchestrator.dependencies.analyze_project(force_rebuild=force_rebuild)
    action_graph = internal.graphs.action_graph
    inspection = InspectionFacade(orchestrator)
    project_root: Path = orchestrator.project_root

    warnings: list[str] = []
    bindings, fg_write_fqns = _resolve_write_bindings(
        inspection, action_graph, project_root, env, warnings
    )

    datasets = [
        _build_dataset_view(binding, action_graph, bindings, fg_write_fqns)
        for binding in bindings.values()
    ]
    datasets.sort(key=lambda d: (d.fqn, d.pipeline, d.flowgroup, d.action_name))

    return DatasetIndexResult(
        env=env,
        datasets=tuple(datasets),
        warnings=tuple(_dedup(warnings)),
        fingerprint=_compute_fingerprint(env, datasets),
    )


def _resolve_write_bindings(
    inspection: InspectionFacade,
    action_graph: Any,
    project_root: Path,
    env: str,
    warnings: list[str],
) -> tuple[dict[str, _WriteBinding], dict[tuple[str, str], list[str]]]:
    """Env-resolve every flowgroup and join its write actions onto graph nodes.

    Returns ``(bindings, fg_write_fqns)`` where ``bindings`` maps a graph
    write-node id to its resolved binding and ``fg_write_fqns`` maps a
    ``(pipeline, flowgroup)`` key to that flowgroup's resolved write FQNs (used
    to name downstream consumers' own datasets).
    """
    bindings: dict[str, _WriteBinding] = {}
    fg_write_fqns: dict[tuple[str, str], list[str]] = {}

    for fgv in inspection.list_flowgroups():
        try:
            processed = inspection.process_flowgroup(fgv.name, env=env)
        except Exception:
            # Full traceback stays server-side (exc_info); the collected
            # warning is generic so internal error text never leaks.
            logger.warning(
                f"Failed to resolve flowgroup '{fgv.name}' for env '{env}'",
                exc_info=True,
            )
            warnings.append(f"Failed to resolve flowgroup '{fgv.name}'")
            continue

        source_file = _relative_source(fgv.file_path, project_root)
        resolved_writes = [a for a in processed.actions if a.action_type == "write"]
        graph_writes = _graph_write_nodes(action_graph, fgv.pipeline, fgv.name)

        fqns: list[str] = []
        used_fallback = False
        for idx, view in enumerate(resolved_writes):
            direct_id = f"{fgv.pipeline}.{fgv.name}.{view.name}"
            if direct_id in action_graph:
                node_id = direct_id
            elif idx < len(graph_writes):
                node_id = graph_writes[idx]
                used_fallback = True
            else:
                warnings.append(
                    f"Could not map write action '{view.name}' in flowgroup "
                    f"'{fgv.name}' to a dependency-graph node"
                )
                continue
            fqn = view.target_full_name or view.name
            bindings[node_id] = _WriteBinding(
                node_id=node_id,
                view=view,
                pipeline=fgv.pipeline,
                flowgroup=fgv.name,
                source_file=source_file,
                fqn=fqn,
            )
            fqns.append(fqn)

        if used_fallback:
            warnings.append(
                f"Action-name join fell back to positional matching in flowgroup "
                f"'{fgv.name}' (a write action name contains an unresolved token)"
            )
        fg_write_fqns[(fgv.pipeline, fgv.name)] = fqns

    return bindings, fg_write_fqns


def _graph_write_nodes(action_graph: Any, pipeline: str, flowgroup: str) -> list[str]:
    """Write-action node ids for one flowgroup, in graph insertion order."""
    return [
        str(node_id)
        for node_id, attrs in action_graph.nodes(data=True)
        if attrs.get("type") == "write"
        and attrs.get("pipeline") == pipeline
        and attrs.get("flowgroup") == flowgroup
    ]


def _build_dataset_view(
    binding: _WriteBinding,
    action_graph: Any,
    bindings: dict[str, _WriteBinding],
    fg_write_fqns: dict[tuple[str, str], list[str]],
) -> DatasetView:
    """Project one write binding into a :class:`DatasetView` with its lineage."""
    pipeline, flowgroup = binding.pipeline, binding.flowgroup
    chain_ids = _same_flowgroup_upstream(
        action_graph, binding.node_id, pipeline, flowgroup
    )

    nodes: dict[str, LineageNodeView] = {}
    edges: set[tuple[str, str]] = set()

    for cid in chain_ids:
        attrs = action_graph.nodes[cid]
        if cid == binding.node_id:
            nodes[cid] = LineageNodeView(
                id=cid,
                kind="write",
                label=binding.fqn,
                pipeline=pipeline,
                flowgroup=flowgroup,
                dataset_fqn=binding.fqn,
            )
        else:
            # ``attrs`` comes off the ``Any``-typed action graph; a chain
            # node's ``type`` is always an action kind, so cast the narrowed
            # value to the LineageNodeView.kind Literal.
            nodes[cid] = LineageNodeView(
                id=cid,
                kind=cast(
                    Literal[
                        "load", "transform", "write", "test", "dataset", "external"
                    ],
                    str(attrs.get("type") or "transform"),
                ),
                label=str(attrs.get("target") or attrs.get("action_name") or cid),
                pipeline=pipeline,
                flowgroup=flowgroup,
            )

    for cid in chain_ids:
        attrs = action_graph.nodes[cid]
        for ext in attrs.get("external_sources", []) or []:
            ext_id = f"external:{ext}"
            nodes.setdefault(
                ext_id,
                LineageNodeView(id=ext_id, kind="external", label=str(ext)),
            )
            edges.add((ext_id, cid))
        for pred in action_graph.predecessors(cid):
            pattrs = action_graph.nodes[pred]
            same_fg = (
                pattrs.get("pipeline") == pipeline
                and pattrs.get("flowgroup") == flowgroup
            )
            if same_fg:
                if pred in chain_ids:
                    edges.add((str(pred), cid))
            elif pattrs.get("type") == "write":
                # Inbound cross-flowgroup write edge → upstream dataset node.
                up_fqn = _upstream_fqn(pred, pattrs, bindings)
                ds_id = f"dataset:{up_fqn}"
                nodes.setdefault(
                    ds_id,
                    LineageNodeView(
                        id=ds_id,
                        kind="dataset",
                        label=up_fqn,
                        pipeline=str(pattrs.get("pipeline") or ""),
                        flowgroup=str(pattrs.get("flowgroup") or ""),
                        dataset_fqn=up_fqn,
                    ),
                )
                edges.add((ds_id, cid))
            # A foreign, non-write predecessor is a cross-flowgroup session
            # view (no FQN); dropped as a documented v1 limitation.

    consumers = _downstream_consumers(
        action_graph, binding.node_id, pipeline, flowgroup, fg_write_fqns
    )

    node_list = sorted(
        nodes.values(), key=lambda n: (_KIND_ORDER.get(n.kind, 99), n.id)
    )
    edge_list = [LineageEdgeView(source=s, target=t) for s, t in sorted(edges)]

    return DatasetView(
        fqn=binding.fqn,
        kind="sink" if binding.fqn.startswith("sink:") else "table",
        pipeline=pipeline,
        flowgroup=flowgroup,
        action_name=binding.view.name,
        write_mode=binding.view.write_mode or "",
        scd_type=binding.view.scd_type,
        source_file=binding.source_file,
        nodes=tuple(node_list),
        edges=tuple(edge_list),
        consumers=tuple(consumers),
    )


def _same_flowgroup_upstream(
    action_graph: Any, node_id: str, pipeline: str, flowgroup: str
) -> set[str]:
    """Reverse-reachable action nodes within the write's own flowgroup."""
    chain: set[str] = set()
    stack: list[str] = [node_id]
    while stack:
        cur = stack.pop()
        if cur in chain:
            continue
        chain.add(cur)
        for pred in action_graph.predecessors(cur):
            pattrs = action_graph.nodes[pred]
            if (
                pattrs.get("pipeline") == pipeline
                and pattrs.get("flowgroup") == flowgroup
            ):
                stack.append(str(pred))
    return chain


def _upstream_fqn(pred: Any, pattrs: Any, bindings: dict[str, _WriteBinding]) -> str:
    """Resolved FQN for a cross-flowgroup upstream write node."""
    binding = bindings.get(str(pred))
    if binding is not None:
        return binding.fqn
    # Producer flowgroup failed env resolution: fall back to the graph target.
    return str(pattrs.get("target") or pred)


def _downstream_consumers(
    action_graph: Any,
    node_id: str,
    pipeline: str,
    flowgroup: str,
    fg_write_fqns: dict[tuple[str, str], list[str]],
) -> list[DatasetConsumerView]:
    """Foreign-flowgroup actions that read this dataset, deduped and sorted."""
    consumers: list[DatasetConsumerView] = []
    seen: set[tuple[str, str, str, str]] = set()
    for succ in action_graph.successors(node_id):
        sattrs = action_graph.nodes[succ]
        s_pipeline = str(sattrs.get("pipeline") or "")
        s_flowgroup = str(sattrs.get("flowgroup") or "")
        if s_pipeline == pipeline and s_flowgroup == flowgroup:
            continue
        s_action = str(sattrs.get("action_name") or succ)
        target_fqns = fg_write_fqns.get((s_pipeline, s_flowgroup), [])
        consumer_fqn = target_fqns[0] if target_fqns else ""
        key = (consumer_fqn, s_pipeline, s_flowgroup, s_action)
        if key in seen:
            continue
        seen.add(key)
        consumers.append(
            DatasetConsumerView(
                dataset_fqn=consumer_fqn,
                pipeline=s_pipeline,
                flowgroup=s_flowgroup,
                action_name=s_action,
            )
        )
    consumers.sort(key=lambda c: (c.pipeline, c.flowgroup, c.action_name))
    return consumers


def _relative_source(file_path: Optional[Path], project_root: Path) -> str:
    """Return ``file_path`` relative to ``project_root`` as POSIX, or ``""``."""
    if file_path is None:
        return ""
    try:
        return file_path.relative_to(project_root).as_posix()
    except ValueError:
        return ""


def _dedup(items: list[str]) -> list[str]:
    """Order-preserving de-duplication of warning strings."""
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _compute_fingerprint(env: str, datasets: list[DatasetView]) -> str:
    """Content hash over the (deterministically ordered) index for freshness."""
    hasher = hashlib.sha256()
    hasher.update(env.encode("utf-8"))
    for d in datasets:
        hasher.update(
            f"\x00{d.fqn}|{d.kind}|{d.pipeline}|{d.flowgroup}|{d.action_name}"
            f"|{d.write_mode}|{d.scd_type}|{d.source_file}".encode("utf-8")
        )
        for n in d.nodes:
            hasher.update(f"|n:{n.id}:{n.kind}:{n.dataset_fqn}".encode("utf-8"))
        for e in d.edges:
            hasher.update(f"|e:{e.source}>{e.target}".encode("utf-8"))
        for c in d.consumers:
            hasher.update(
                f"|c:{c.dataset_fqn}:{c.pipeline}:{c.flowgroup}:{c.action_name}".encode(
                    "utf-8"
                )
            )
    return hasher.hexdigest()[:16]
