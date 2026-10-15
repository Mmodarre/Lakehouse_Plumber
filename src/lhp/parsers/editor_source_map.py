"""Bounded YAML-node positions for editor navigation.

Coordinates are zero-based UTF-16 code units, matching text editors. Paths
are document-relative; the document index distinguishes YAML streams.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

import yaml

from .yaml_loader import SAFE_LOADER

YamlPath = Tuple[str | int, ...]


@dataclass(frozen=True)
class SourceSpan:
    document_index: int
    path: YamlPath
    line: int
    column: int
    end_line: int
    end_column: int


def _utf16_column(lines: list[str], line: int, column: int) -> int:
    if line >= len(lines):
        return column
    return len(lines[line][:column].encode("utf-16-le")) // 2


def source_spans(text: str) -> Dict[Tuple[int, YamlPath], SourceSpan]:
    """Map values to spans without executing tags or following alias cycles."""
    if len(text.encode("utf-8")) > 2 * 1024 * 1024:
        raise ValueError("YAML document exceeds the 2 MiB editor limit")
    lines = text.splitlines(keepends=True)
    result: Dict[Tuple[int, YamlPath], SourceSpan] = {}
    visited = 0

    def visit(node: yaml.Node, doc: int, path: YamlPath, ancestors: set[int]) -> None:
        nonlocal visited
        visited += 1
        if visited > 40000 or len(path) > 64 or id(node) in ancestors:
            raise ValueError("YAML nesting or aliases exceed the editor limit")
        result[(doc, path)] = SourceSpan(
            doc,
            path,
            node.start_mark.line,
            _utf16_column(lines, node.start_mark.line, node.start_mark.column),
            node.end_mark.line,
            _utf16_column(lines, node.end_mark.line, node.end_mark.column),
        )
        lineage = ancestors | {id(node)}
        if isinstance(node, yaml.MappingNode):
            for key, value in node.value:
                visit(value, doc, (*path, str(key.value)), lineage)
        elif isinstance(node, yaml.SequenceNode):
            for index, value in enumerate(node.value):
                visit(value, doc, (*path, index), lineage)

    for document_index, node in enumerate(yaml.compose_all(text, Loader=SAFE_LOADER)):
        if node is not None:
            visit(node, document_index, (), set())
    return result
