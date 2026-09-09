"""HTTP authoring contracts, route precedence and native-value preservation."""

import pytest


@pytest.mark.parametrize(
    "path",
    [
        "templates/ingestion/csv_ingestion_template.yaml",
        "templates/json_ingestion_template.yaml",
    ],
)
def test_source_route_handles_nested_identity(client, path):
    response = client.get("/api/templates/source", params={"path": path})
    assert response.status_code == 200
    entry = response.json()["template"]
    assert entry["source_path"] == path
    assert entry["reference"] == path.removeprefix("templates/").removesuffix(".yaml")


def test_catalog_route_keeps_nested_sources(client):
    response = client.get("/api/templates/catalog")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == len(data["templates"])
    assert any(
        e["reference"] == "ingestion/csv_ingestion_template" for e in data["templates"]
    )


def test_source_preserves_explicit_null_default(mutable_project, mutable_client):
    path = mutable_project / "templates/presence.yaml"
    path.write_text(
        "name: presence\nparameters:\n - name: absent\n - name: explicit\n   default: null\n"
    )
    response = mutable_client.get(
        "/api/templates/source", params={"path": "templates/presence.yaml"}
    )
    parameters = response.json()["template"]["parameters"]
    assert parameters[0]["has_default"] is False
    assert parameters[1]["has_default"] is True
    assert "default" in parameters[1] and parameters[1]["default"] is None


def test_legacy_duplicate_name_is_ambiguous(mutable_project, mutable_client):
    path = mutable_project / "templates/duplicate.yaml"
    path.write_text("name: json_ingestion_template\n")
    response = mutable_client.get("/api/templates/json_ingestion_template")
    assert response.status_code == 409
    assert "source path" in response.json()["detail"]


def test_preview_handles_inspection_and_missing_values(client):
    body = {
        "source_path": "templates/new.yaml",
        "source_yaml": "name: t\nparameters:\n - name: value\n   required: true\n   default: 0\n",
        "request_revision": "r2",
        "stage": "inspect",
        "sample_parameters": {},
    }
    inspected = client.post("/api/templates/preview", json=body)
    assert inspected.status_code == 200
    assert inspected.json()["status"] == "ready"
    assert inspected.json()["request_revision"] == "r2"
    expanded = client.post("/api/templates/preview", json={**body, "stage": "expanded"})
    assert expanded.json()["status"] == "needs_parameters"
    assert expanded.json()["missing_parameters"] == ["value"]


def test_preview_path_and_payload_guards(client):
    body = {
        "source_path": "../outside.yaml",
        "source_yaml": "name: t\n",
        "request_revision": "r",
        "stage": "inspect",
    }
    assert client.post("/api/templates/preview", json=body).status_code == 403
    assert (
        client.post(
            "/api/templates/preview",
            json={
                **body,
                "source_path": "templates/t.yaml",
                "source_yaml": "x" * 524289,
            },
        ).status_code
        == 422
    )
    assert (
        client.get(
            "/api/templates/source", params={"path": "templates/missing.yaml"}
        ).status_code
        == 404
    )


def test_preview_uses_existing_token_and_origin_guards(e2e_project_path, monkeypatch):
    from fastapi.testclient import TestClient

    from lhp.webapp.app import create_app

    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(e2e_project_path))
    monkeypatch.setenv("LHP_WEBAPP_TOKEN", "template-test-session")
    body = {
        "source_path": "templates/draft.yaml",
        "source_yaml": "name: t\n",
        "request_revision": "r",
        "stage": "inspect",
    }
    with TestClient(create_app(), base_url="http://127.0.0.1") as client:
        assert client.post("/api/templates/preview", json=body).status_code == 401
        headers = {"X-LHP-Token": "template-test-session"}
        assert (
            client.post(
                "/api/templates/preview",
                json=body,
                headers={**headers, "Origin": "https://elsewhere.example"},
            ).status_code
            == 403
        )
        assert (
            client.post(
                "/api/templates/preview", json=body, headers=headers
            ).status_code
            == 200
        )
