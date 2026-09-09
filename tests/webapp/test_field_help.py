"""Guidance is packaged locally and cannot use arbitrary request paths."""

import pytest
from fastapi import HTTPException

from lhp.webapp.routers.help import get_field_help


def test_packaged_contextual_help_has_real_examples():
    catalog = get_field_help("flowgroup")
    assert catalog["version"] == 1
    cloudfiles = [
        entry
        for entry in catalog["entries"]
        if any(
            binding.get("subtype") == "load:cloudfiles" for binding in entry["bindings"]
        )
    ]
    delta = [
        entry
        for entry in catalog["entries"]
        if any(binding.get("subtype") == "load:delta" for binding in entry["bindings"])
    ]
    assert cloudfiles and delta
    assert any(entry.get("examples") for entry in cloudfiles)
    assert all(entry["sources"] for entry in cloudfiles)


@pytest.mark.parametrize(
    "kind",
    ["../project", "project.json", "flowgroup/../../etc/passwd", "missing", "PROJECT"],
)
def test_unknown_or_unsafe_help_category_is_not_read(kind):
    with pytest.raises(HTTPException) as caught:
        get_field_help(kind)
    assert caught.value.status_code == 404


def test_guidance_route_is_registered(client):
    response = client.get("/api/help/template")
    assert response.status_code == 200
    assert response.json()["entries"]
