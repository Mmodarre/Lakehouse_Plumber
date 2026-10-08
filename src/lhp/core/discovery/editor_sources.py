"""Index raw flowgroup, template and blueprint YAML source ownership."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Tuple

import yaml

from lhp.parsers import SAFE_LOADER, SourceSpan, YamlPath, source_spans


@dataclass(frozen=True)
class RawSourceEntry:
    raw: Mapping[str, Any]
    document_index: int
    yaml_path: YamlPath


class EditorYamlIndex:
    """One bounded parse shared by identity lookup and field navigation."""

    def __init__(self, text: str) -> None:
        self.spans = source_spans(text)
        self.documents = tuple(yaml.load_all(text, Loader=SAFE_LOADER))  # nosec B506

    def span(self, document_index: int, yaml_path: YamlPath) -> SourceSpan | None:
        return self.spans.get((document_index, yaml_path))

    def flowgroup(self, pipeline: str, name: str) -> RawSourceEntry | None:
        for document_index, doc in enumerate(self.documents):
            if not isinstance(doc, Mapping):
                continue
            entries = doc.get("flowgroups")
            if isinstance(entries, list):
                for index, child in enumerate(entries):
                    if not isinstance(child, Mapping):
                        continue
                    effective_pipeline = child.get("pipeline", doc.get("pipeline"))
                    if (
                        effective_pipeline == pipeline
                        and child.get("flowgroup") == name
                    ):
                        return RawSourceEntry(
                            child, document_index, ("flowgroups", index)
                        )
            elif doc.get("pipeline") == pipeline and doc.get("flowgroup") == name:
                return RawSourceEntry(doc, document_index, ())
        return None

    def blueprint_spec(self, index: int) -> RawSourceEntry | None:
        for document_index, doc in enumerate(self.documents):
            if not isinstance(doc, Mapping):
                continue
            entries = doc.get("flowgroups")
            if isinstance(entries, list) and 0 <= index < len(entries):
                child = entries[index]
                if isinstance(child, Mapping):
                    return RawSourceEntry(child, document_index, ("flowgroups", index))
        return None

    def first_mapping(self) -> RawSourceEntry | None:
        for document_index, doc in enumerate(self.documents):
            if isinstance(doc, Mapping):
                return RawSourceEntry(doc, document_index, ())
        return None

    def action_entries(self, parent: RawSourceEntry) -> Tuple[RawSourceEntry, ...]:
        actions = parent.raw.get("actions")
        if not isinstance(actions, list):
            return ()
        return tuple(
            RawSourceEntry(
                value, parent.document_index, (*parent.yaml_path, "actions", index)
            )
            for index, value in enumerate(actions)
            if isinstance(value, Mapping)
        )
