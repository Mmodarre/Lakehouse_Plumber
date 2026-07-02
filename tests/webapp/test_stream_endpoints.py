"""Endpoint contract tests for the NDJSON streaming router.

Covers the two POST stream endpoints wired in
:mod:`lhp.webapp.routers.streaming`:

* ``POST /api/validate/stream``
* ``POST /api/generate/stream``

These tests exercise the FULL stack (router → facade → core engine) over the
shared E2E fixture project, so they are heavier than the self-sufficient
:mod:`tests.webapp.test_stream_adapter` unit tests. They assert the §5.7 frame
ordering as observed by an HTTP client, the terminal frame per operation, the
write side effect for generate, request-body validation (422), and that the
NDJSON body is consumable incrementally under ``TestClient``.

The fixture project IS a bundle project (``databricks.yml`` present) but the
router deliberately runs with ``bundle_enabled=False`` (it imports no
``lhp.bundle`` helper, and the DI facade carries no pipeline-config path), so
the streams run as a pure code-generation / validation preview.

Substitution environments available in the fixture project (from
``substitutions/*.yaml``): ``dev``, ``prod``, ``tst``. ``dev`` is used here.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.webapp

# The fixture project's substitution env used by every test below. Enumerated
# from tests/e2e/fixtures/testing_project/substitutions/{dev,prod,tst}.yaml.
ENV = "dev"


def _parse_ndjson(raw: bytes) -> list[dict[str, Any]]:
    """Decode an NDJSON byte body into a list of frame dicts.

    Every non-empty line must be a JSON object carrying a ``type`` key (the
    pinned frame protocol — see ``stream_adapter``).
    """
    frames: list[dict[str, Any]] = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        frame = json.loads(line)
        assert isinstance(frame, dict), f"frame is not a JSON object: {frame!r}"
        assert "type" in frame, f"frame missing 'type' key: {frame!r}"
        frames.append(frame)
    return frames


# --- 1. VALIDATE happy path (read-only client) ------------------------------


def test_validate_stream_contract_happy_path(client: TestClient) -> None:
    """POST /api/validate/stream → 200 NDJSON; §5.7 frame ordering holds."""
    resp = client.post("/api/validate/stream", json={"env": ENV})

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/x-ndjson")

    frames = _parse_ndjson(resp.content)
    types = [f["type"] for f in frames]

    # §5.7: exactly one OperationStarted, first.
    assert types[0] == "OperationStarted"
    assert types.count("OperationStarted") == 1
    assert frames[0]["operation_name"] == "validate_pipelines"
    assert frames[0]["env"] == ENV

    # Terminal frame is the single ValidationCompleted, last.
    assert types[-1] == "ValidationCompleted"
    assert types.count("ValidationCompleted") == 1
    # The pristine fixture validates clean.
    assert frames[-1]["response"]["success"] is True

    # Phase frames appear strictly between the start and the terminal.
    assert "PhaseStarted" in types
    assert "PhaseCompleted" in types
    assert types.index("OperationStarted") < types.index("PhaseStarted")
    assert types.index("PhaseStarted") < types.index("ValidationCompleted")


# --- 2. GENERATE happy path (mutable client) --------------------------------


def test_generate_stream_writes_files_and_completes(
    mutable_client: TestClient, mutable_project: Any
) -> None:
    """POST /api/generate/stream → terminal GenerationCompleted(success); files written."""
    resp = mutable_client.post("/api/generate/stream", json={"env": ENV})

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/x-ndjson")

    frames = _parse_ndjson(resp.content)
    types = [f["type"] for f in frames]

    assert types[0] == "OperationStarted"
    assert frames[0]["operation_name"] == "generate_pipelines"

    # Terminal GenerationCompleted carrying a successful response.
    assert types[-1] == "GenerationCompleted"
    assert types.count("GenerationCompleted") == 1
    response = frames[-1]["response"]
    assert response["success"] is True
    assert response["total_files_written"] > 0

    # The write side effect landed in the tmp copy under generated/<env>.
    generated_dir = mutable_project / "generated" / ENV
    assert generated_dir.is_dir()
    py_files = list(generated_dir.rglob("*.py"))
    assert py_files, f"no generated .py files under {generated_dir}"


# --- 3. FAILURE path (mutable client — corrupts a flowgroup) ----------------


def test_validate_stream_failure_emits_terminal_error_frame(
    mutable_client: TestClient, mutable_project: Any
) -> None:
    """A structural failure (unknown action type) aborts the validate stream.

    An invalid ``type:`` survives YAML parse but fails discovery with
    ``LHP-ACT-001`` BEFORE the per-pipeline validate phase. The facade's
    validate stream raises this as a bare ``LHPError`` (no preceding
    ``ErrorEmitted`` rendezvous — it escapes ``discover_all_flowgroups()``),
    which the adapter renders as the PINNED TERMINAL ERROR FRAME.

    This failure surfaces as a terminal
    ``{"type": "error", "code": "LHP-ACT-001", ...}`` frame — NOT as a
    ``ValidationCompleted(success=false)``. The "fold into ValidationCompleted"
    path is reserved for per-pipeline FINDINGS in otherwise-parseable YAML;
    a structural/config error that aborts discovery is the §1.4 error-frame
    path instead.
    """
    # Corrupt one flowgroup in the isolated COPY: an unknown action type. This
    # parses as valid YAML but is rejected by the action registry at discovery.
    fg = mutable_project / "pipelines" / "02_bronze" / "customer_bronze.yaml"
    original = fg.read_text()
    corrupted = original.replace(
        "    type: load\n", "    type: not_a_real_action_type\n", 1
    )
    assert corrupted != original, "expected to corrupt the load action type"
    fg.write_text(corrupted)

    resp = mutable_client.post("/api/validate/stream", json={"env": ENV})

    # The stream itself is a 200 (the response started before the failure was
    # known); the failure surfaces as the terminal in-band error frame.
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/x-ndjson")

    frames = _parse_ndjson(resp.content)
    types = [f["type"] for f in frames]

    # OperationStarted first; a single terminal error frame last, nothing after.
    assert types[0] == "OperationStarted"
    assert types[-1] == "error"
    assert types.count("error") == 1

    err = frames[-1]
    assert err["code"] == "LHP-ACT-001"
    # The pinned error-frame shape (built by the adapter from LHPError attrs).
    assert set(err) >= {
        "type",
        "code",
        "title",
        "details",
        "suggestions",
        "context",
        "doc_link",
    }

    # No ValidationCompleted was emitted — the run aborted before the terminal.
    assert "ValidationCompleted" not in types


# --- 3b. Request-body validation → 422 --------------------------------------


@pytest.mark.parametrize("path", ["/api/validate/stream", "/api/generate/stream"])
def test_stream_missing_env_is_422(client: TestClient, path: str) -> None:
    """A body without the required ``env`` field is rejected with 422."""
    resp = client.post(path, json={"pipeline": "acmi_edw_bronze"})
    assert resp.status_code == 422


@pytest.mark.parametrize("path", ["/api/validate/stream", "/api/generate/stream"])
def test_stream_empty_env_is_422(client: TestClient, path: str) -> None:
    """An empty ``env`` (min_length=1) is rejected with 422 before any run."""
    resp = client.post(path, json={"env": ""})
    assert resp.status_code == 422


# --- 4. Incremental consumption under TestClient ----------------------------


def test_validate_stream_is_consumable_incrementally(client: TestClient) -> None:
    """``client.stream(...)`` + ``iter_lines`` proves NDJSON streams line-by-line."""
    first_type: str | None = None
    line_count = 0

    with client.stream("POST", "/api/validate/stream", json={"env": ENV}) as resp:
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("application/x-ndjson")
        for line in resp.iter_lines():
            if not line.strip():
                continue
            frame = json.loads(line)
            line_count += 1
            if first_type is None:
                first_type = frame["type"]

    # We consumed multiple discrete frames incrementally, the first being the
    # OperationStarted marker (§5.7).
    assert first_type == "OperationStarted"
    assert line_count >= 2
