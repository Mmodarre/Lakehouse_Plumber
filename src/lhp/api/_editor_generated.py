"""Project synthetic flowgroups without inventing authored YAML locations."""

from __future__ import annotations

from typing import Mapping, cast

from lhp.api.editor_views import EditorActionView, EditorFlowgroupView, EditorSourceView
from lhp.api.responses import JSONValue
from lhp.models import FlowGroup


def generated_flowgroup_view(
    authored: FlowGroup, resolved: FlowGroup, source: EditorSourceView
) -> EditorFlowgroupView:
    """Expose a synthetic node as visible and noneditable. :stability: provisional"""
    # Pydantic mode=json recursively converts the model to JSON primitives.
    resolved_raw = cast(
        Mapping[str, JSONValue], resolved.model_dump(mode="json", exclude_none=True)
    )
    actions = tuple(
        EditorActionView(
            name=action.name,
            action_type=str(action.type.value),
            source=source,
            origin="generated",
            raw={},
            resolved=cast(
                Mapping[str, JSONValue],
                action.model_dump(mode="json", exclude_none=True),
            ),
            editable=False,
        )
        for action in resolved.actions
    )
    return EditorFlowgroupView(
        pipeline=authored.pipeline,
        name=authored.flowgroup,
        source=source,
        origin="generated",
        raw={},
        definition_raw=None,
        resolved=resolved_raw,
        actions=actions,
        editable=False,
    )
