"""Declared resource references for every template, including unused definitions."""

from pathlib import Path
from typing import Mapping, Sequence, Tuple

from lhp.api.editor_views import EditorRelatedFileView, EditorSourceView
from lhp.api.views import TemplateView


def _template_file_references(
    root: Path, templates: Sequence[TemplateView]
) -> Mapping[str, Tuple[EditorRelatedFileView, ...]]:
    """Index declared fields; unresolved parameter paths remain non-existing refs."""
    from lhp.core.discovery import EditorYamlIndex, action_file_references

    result: dict[str, Tuple[EditorRelatedFileView, ...]] = {}
    for template in templates:
        source = template.file_path.resolve()
        if not source.is_relative_to(root.resolve()):
            continue
        path = source.relative_to(root.resolve()).as_posix()
        index = EditorYamlIndex(source.read_text(encoding="utf-8"))
        entry = index.first_mapping()
        refs: list[EditorRelatedFileView] = []
        if entry is not None:
            for action in index.action_entries(entry):
                for ref in action_file_references(action.raw, root):
                    if Path(ref.path).is_absolute() or ".." in Path(ref.path).parts:
                        continue
                    yaml_path = (*action.yaml_path, *ref.field_path)
                    span = index.span(action.document_index, yaml_path)
                    refs.append(
                        EditorRelatedFileView(
                            kind=ref.kind,
                            path=ref.path,
                            exists=ref.exists,
                            action_name=ref.action_name,
                            source=EditorSourceView(
                                path=path,
                                document_index=action.document_index,
                                yaml_path=yaml_path,
                                line=span.line if span else None,
                                column=span.column if span else None,
                                end_line=span.end_line if span else None,
                                end_column=span.end_column if span else None,
                            ),
                        )
                    )
        result[path] = tuple(refs)
    return result
