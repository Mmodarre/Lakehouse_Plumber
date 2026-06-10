"""Package-private graph primitives on the shared NetworkX DiGraph contract.

Both the dependency analyzer and the action resolver operate on the same
``nx.DiGraph`` contract carried by ``DependencyGraphs``. These two pure
functions are the single source of topological-ordering and cycle-detection
behaviour for the sub-package; callers add their own formatting, capping and
error-raising on top.
"""

from itertools import islice
from typing import Optional

import networkx as nx


def topological_generations(graph: "nx.DiGraph") -> list[list[str]]:
    """Return the topological generations of ``graph`` as a list of node lists.

    Each generation is a list of node names with no dependencies among them;
    generations are ordered so producers precede consumers. Returns ``[]`` when
    the graph is empty or is not a DAG (i.e. contains a cycle).
    """
    if not graph.nodes():
        return []

    try:
        return [list(generation) for generation in nx.topological_generations(graph)]
    except nx.NetworkXError:
        return []


def find_cycle(graph: "nx.DiGraph") -> Optional[list[str]]:
    """Return one cycle of ``graph`` as an ordered, closed list of node names.

    The returned list traverses the cycle in edge order and repeats the start
    node at the end (e.g. ``["a", "b", "a"]``). Returns ``None`` when the graph
    is acyclic. No formatting is applied and no exception is raised for the
    acyclic case.
    """
    try:
        edges = nx.find_cycle(graph)
    except nx.NetworkXNoCycle:
        return None

    return [u for u, _ in edges] + [edges[-1][1]]


def enumerate_cycles(
    graph: "nx.DiGraph", limit: Optional[int] = None
) -> list[list[str]]:
    """Enumerate elementary cycles of ``graph`` as raw lists of node names.

    Wraps ``nx.simple_cycles(graph)`` and preserves its native iteration
    order. Each returned cycle is the list of node names that form it, with
    no formatting and no closing repeat of the start node. When ``limit`` is
    set, returns at most the first ``limit`` cycles (``itertools.islice``
    semantics); when ``limit`` is ``None``, returns every cycle. The caller
    owns any capping policy and result shaping built on top of this.
    """
    cycles = nx.simple_cycles(graph)
    if limit is not None:
        cycles = islice(cycles, limit)
    return [list(cycle_nodes) for cycle_nodes in cycles]
