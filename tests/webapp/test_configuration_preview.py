"""Read-only HTTP configuration-preview contract and path guards."""


def test_saved_preview_returns_resolved_values(client):
    response = client.get(
        "/api/configuration/preview",
        params={
            "path": "config/pipeline_config.yaml",
            "kind": "pipeline",
            "env": "dev",
        },
    )
    assert response.status_code == 200
    assert response.json()["source"] == "saved"
    assert "serverless" in response.json()["values"]


def test_preview_rejects_path_escape(client):
    response = client.get(
        "/api/configuration/preview",
        params={"path": "../outside.yaml", "kind": "pipeline", "env": "dev"},
    )
    assert response.status_code == 403


def test_preview_reports_missing_file(client):
    response = client.get(
        "/api/configuration/preview",
        params={"path": "config/missing.yaml", "kind": "job", "env": "dev"},
    )
    assert response.status_code == 404


def test_preview_rejects_invalid_kind_and_environment(client):
    for kind, env in [("project", "dev"), ("job", "../prod")]:
        response = client.get(
            "/api/configuration/preview",
            params={"path": "config/pipeline_config.yaml", "kind": kind, "env": env},
        )
        assert response.status_code == 422
