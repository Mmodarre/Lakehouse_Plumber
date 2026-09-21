"""Request schema and bounded vocabularies for the UI-surface telemetry route.

The SPA reports WHICH part of the IDE was opened, toggled or created —
never what it contained. Every value that can reach an event is therefore
drawn from a closed set declared here: :data:`UI_SURFACES`,
:data:`UI_ACTIONS` and :data:`UI_VIA`. The pydantic models bound only the
SHAPE of the request (a well-formed session id, at most
:data:`MAX_UI_EVENTS` events, string lengths); membership of the closed sets
is checked by the router, which drops an unrecognised event and still
succeeds. That split is deliberate: a browser tab running a newer or older
bundle than the server must never see its telemetry post fail, and a
structural rejection is the only thing worth a ``422``.

The string-length caps matter even though unknown values are dropped: they
bound the work an oversized body can cost before the router rejects its
contents.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from lhp.webapp.services.telemetry_sessions import SESSION_ID_PATTERN

#: Every UI surface the SPA may report (DESIGN §E7). The registry derives its
#: ``dag_views`` / ``lineage_views`` / ``sandbox_toggles`` counters from the
#: labels built out of these names, so a rename here is a wire change.
UI_SURFACES: frozenset[str] = frozenset(
    {
        "file_editor",
        "flowgroup_graph",
        "flowgroup_code",
        "template_graph",
        "template_code",
        "config_form_project",
        "config_form_pipeline",
        "config_form_job",
        "config_yaml_project",
        "config_yaml_pipeline",
        "config_yaml_job",
        "project_map",
        "pipeline_dag",
        "table_detail",
        "resource_preset",
        "resource_template",
        "resource_blueprint",
        "resource_environment",
        "files_lens",
        "structure_lens",
        "tables_lens",
        "inspector_validation",
        "inspector_help",
        "problems",
        "run_stream",
        "run_history",
        "assistant_panel",
        "viewer_mode",
        "create_flowgroup_dialog",
        "sandbox_control",
        "sandbox_picker",
        "init_wizard",
    }
)

#: What happened to the surface. ``created`` is the only action carrying a ``via``.
UI_ACTIONS: frozenset[str] = frozenset({"opened", "toggled", "created"})

#: How a ``created`` surface was created.
UI_VIA: frozenset[str] = frozenset({"blank", "template", "blueprint"})

#: Events one request may carry; the SPA chunks a longer queue itself.
MAX_UI_EVENTS = 100


class UiEvent(BaseModel):
    """One observation of a UI surface, rendered as ``surface.action[.via]``.

    The fields are plain bounded strings rather than enums so that a value
    the server does not know costs the event, not the request.
    """

    surface: str = Field(..., max_length=64)
    action: str = Field(..., max_length=32)
    via: Optional[str] = Field(None, max_length=32)


class UiEventsRequest(BaseModel):
    """Body of ``POST /api/telemetry/ui``.

    ``session_id`` is the tab's own id in the same lowercase-uuid spelling
    the ``X-LHP-Session`` header uses. Carrying it in the body as well makes
    a batch self-describing, so it can still be attributed to its tab when
    the request itself presents no usable header.
    """

    session_id: str = Field(..., pattern=SESSION_ID_PATTERN)
    events: list[UiEvent] = Field(..., max_length=MAX_UI_EVENTS)
