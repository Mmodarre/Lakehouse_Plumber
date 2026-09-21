"""Unit tests for :class:`InitTemplateContext`.

The context carries the one identifier a scaffolded project is born with:
``create()`` mints a single UUID v4 and hands the same value to both
``bundle_uuid`` (rendered into ``databricks.yml``) and ``project_id``
(rendered into ``lhp.yaml``), so a bundle project has one identity rather
than two.
"""

from __future__ import annotations

import dataclasses
import re
import uuid

import pytest

from lhp.core.loaders import InitTemplateContext

pytestmark = pytest.mark.unit

UUID4_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


def test_create_mints_one_uuid_shared_by_bundle_uuid_and_project_id() -> None:
    """``bundle_uuid`` and ``project_id`` are the same freshly minted UUID v4."""
    context = InitTemplateContext.create(project_name="demo_project")

    assert UUID4_RE.match(context.project_id), (
        f"project_id must be a UUID v4, got {context.project_id!r}"
    )
    assert context.project_id == context.bundle_uuid
    assert uuid.UUID(context.project_id).version == 4


def test_create_mints_a_distinct_identity_per_project() -> None:
    """Two scaffolds never share an identity."""
    first = InitTemplateContext.create(project_name="demo_project")
    second = InitTemplateContext.create(project_name="demo_project")

    assert first.project_id != second.project_id


def test_project_id_defaults_to_empty_string() -> None:
    """Direct construction (no ``create()``) leaves the identity unset."""
    context = InitTemplateContext(
        project_name="demo_project", current_date="2026-01-01"
    )

    assert context.project_id == ""


def test_context_is_frozen() -> None:
    """The context is a value object — rendering never mutates it."""
    context = InitTemplateContext.create(project_name="demo_project")

    with pytest.raises(dataclasses.FrozenInstanceError):
        context.project_id = "tampered"  # type: ignore[misc]
