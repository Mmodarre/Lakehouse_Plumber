"""Shared fixtures for the webapp (FastAPI backend) test suite.

``create_app`` is a *zero-argument* uvicorn factory; all configuration flows
from the ``LHP_WEBAPP_*`` environment (see :mod:`lhp.webapp.settings`). Every
fixture here that builds an app therefore
``monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", ...)`` *before* calling
``create_app()``, because ``get_settings()`` reads and resolves
``project_root`` eagerly at construction time.

There are no auth / git / workspace / prod-mode / state fixtures: the local
IDE is same-origin, single user, no auth, no hosted workspace provisioning.

Scope choice — everything here is **function-scoped**. Because configuration
is injected by mutating ``os.environ`` (via ``monkeypatch``), and ``monkeypatch``
is itself function-scoped, a session/module-scoped client would either leak env
state across tests or require manual teardown. Function scope keeps each test's
env mutation isolated and torn down automatically. The deep-copy cost is modest
(the fixture project is small), so the safety is worth it.
"""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from lhp.webapp.app import create_app

# Resolved relative to THIS file so it works regardless of the pytest invocation
# cwd: tests/webapp/conftest.py -> tests/ -> tests/e2e/fixtures/testing_project.
_FIXTURE_SOURCE = (
    Path(__file__).resolve().parent.parent / "e2e" / "fixtures" / "testing_project"
)

# Recognizable marker text embedded in the fake ``index.html`` served by the
# ``with_static`` fixture, so tests can assert the SPA shell (not the API) was
# returned without depending on the real built bundle.
WITH_STATIC_INDEX_MARKER = "SPA_TEST_INDEX_MARKER"

# TestClient's default base_url is http://testserver, whose Host header the
# app's TrustedHostMiddleware (loopback-only) rejects with 400 — every client
# over ``create_app()`` must present a loopback Host instead.
LOOPBACK_BASE_URL = "http://127.0.0.1"


@pytest.fixture(autouse=True)
def _no_parse_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable the default-on parse cache for every webapp test.

    Several tests serve the tracked e2e fixture project in place; the cache
    would otherwise write ``.lhp/cache/`` shards into it, polluting the
    fixture for the e2e suite. Parse-cache behavior has its own coverage in
    ``tests/core/coordination`` and ``tests/integration``.
    """
    monkeypatch.setenv("LHP_NO_CACHE", "1")


def _clear_ambient_webapp_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Drop ambient LHP_WEBAPP_* vars so a developer's env can't perturb tests.

    ``LHP_WEBAPP_TOKEN`` matters most: if it leaked in, every ``/api`` request
    from these token-less fixtures would 401.
    """
    monkeypatch.delenv("LHP_WEBAPP_PORT", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_LOG_LEVEL", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_TOKEN", raising=False)


def _client_for(monkeypatch: pytest.MonkeyPatch, project_root: Path) -> TestClient:
    """Inject the env contract for ``project_root`` and build a ``TestClient``.

    Sets ``LHP_WEBAPP_PROJECT_ROOT``, clears the other ambient ``LHP_WEBAPP_*``
    vars, then calls ``create_app()`` (which reads them eagerly). The returned
    client is NOT yet entered — fixtures use it as a context manager so the
    lifespan (startup log + project-state resolution) runs per test. Any
    ``resolve_static_dir`` monkeypatching must happen BEFORE this call.
    """
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(project_root))
    _clear_ambient_webapp_env(monkeypatch)
    return TestClient(create_app(), base_url=LOOPBACK_BASE_URL)


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Auto-apply the ``webapp`` marker to every test in this directory tree.

    Idempotent with the per-module ``pytestmark = pytest.mark.webapp``
    declarations.
    """
    conftest_dir = Path(__file__).resolve().parent
    for item in items:
        try:
            item_path = Path(item.fspath).resolve()
        except Exception:
            continue
        if conftest_dir in item_path.parents:
            item.add_marker(pytest.mark.webapp)


@pytest.fixture(autouse=True)
def _clean_shared_fixture_run_history() -> Iterator[None]:
    """Keep the SHARED e2e fixture free of run-history DB files.

    The run recorder persists every validate/generate stream into
    ``<project_root>/.lhp/webapp.db`` — including streams driven through the
    read-only ``client`` fixture, whose project root IS the shared fixture
    project. That tree must stay pristine (nothing may touch
    ``tests/e2e/fixtures`` in place), so the webapp DB files are removed
    before AND after every test; the ``.lhp/`` directory itself is removed
    only when the cleanup leaves it empty (it does not exist in git).
    """

    def _remove() -> None:
        lhp_dir = _FIXTURE_SOURCE / ".lhp"
        for path in lhp_dir.glob("webapp.db*"):
            path.unlink(missing_ok=True)
        if lhp_dir.is_dir() and not any(lhp_dir.iterdir()):
            lhp_dir.rmdir()

    _remove()
    yield
    _remove()


@pytest.fixture
def e2e_project_path() -> Path:
    """Path to the CURRENT repo's read-only E2E fixture project.

    This points directly at the shared fixture and must be treated as
    read-only. Tests that mutate the project should use
    :func:`mutable_project` / :func:`mutable_client` instead.
    """
    assert _FIXTURE_SOURCE.is_dir(), (
        f"E2E fixture project not found at {_FIXTURE_SOURCE}; "
        "the webapp test fixtures depend on tests/e2e/fixtures/testing_project."
    )
    return _FIXTURE_SOURCE


@pytest.fixture
def client(
    e2e_project_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """Read-only ``TestClient`` over ``create_app()``.

    Function-scoped (see module docstring): the project root is injected via
    ``monkeypatch.setenv`` before the app is built, and ``monkeypatch`` is
    function-scoped. The other ambient ``LHP_WEBAPP_*`` vars (port, log level,
    token) are cleared so a developer's env can't perturb the test app.

    Bound to the SHARED fixture project — do not use for write/delete/generate
    tests; use :func:`mutable_client` for those.
    """
    # ``with`` triggers the lifespan; raise_server_exceptions is left at its
    # default so genuine 500s surface in tests.
    with _client_for(monkeypatch, e2e_project_path) as test_client:
        yield test_client


@pytest.fixture
def mutable_project(tmp_path: Path) -> Path:
    """Deep copy of the fixture project into ``tmp_path`` for write tests.

    Each test gets its own isolated copy, so write/delete/generate side effects
    never touch the shared read-only fixture and never leak between tests.
    """
    assert _FIXTURE_SOURCE.is_dir(), (
        f"E2E fixture project not found at {_FIXTURE_SOURCE}."
    )
    dest = tmp_path / "testing_project"
    shutil.copytree(_FIXTURE_SOURCE, dest)
    return dest


@pytest.fixture
def mutable_client(
    mutable_project: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """``TestClient`` bound to the deep-copied (mutable) project.

    For write / delete / generate tests. Same function-scoped env-injection
    contract as :func:`client`, but points the app at the isolated copy so the
    shared fixture stays pristine.
    """
    with _client_for(monkeypatch, mutable_project) as test_client:
        yield test_client


@pytest.fixture
def no_static(
    e2e_project_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """``TestClient`` where the SPA is forced *not built* (hermetic).

    Monkeypatches :func:`lhp.webapp.static_app.resolve_static_dir` to ``None``
    BEFORE ``create_app()`` so the result is identical whether or not a real
    static bundle exists on disk: ``GET /`` yields the plain-text build message
    and unmatched non-``/api`` paths yield JSON ``404`` (no HTML fallback).
    """
    monkeypatch.setattr("lhp.webapp.static_app.resolve_static_dir", lambda: None)
    with _client_for(monkeypatch, e2e_project_path) as test_client:
        yield test_client


@pytest.fixture
def with_static(
    e2e_project_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """``TestClient`` where the SPA is forced *built* against a tmp bundle.

    Creates a minimal ``index.html`` (carrying :data:`WITH_STATIC_INDEX_MARKER`)
    plus ``assets/app.js`` in a tmp dir and monkeypatches
    :func:`lhp.webapp.static_app.resolve_static_dir` to return it BEFORE
    ``create_app()``. Independent of the real (gitignored) built bundle.
    """
    static_dir = tmp_path / "static"
    (static_dir / "assets").mkdir(parents=True)
    (static_dir / "index.html").write_text(
        "<!doctype html><html><head><title>SPA</title></head>"
        f'<body><div id="root"></div><!-- {WITH_STATIC_INDEX_MARKER} --></body>'
        "</html>\n",
        encoding="utf-8",
    )
    (static_dir / "assets" / "app.js").write_text(
        "export const marker = 1;\n", encoding="utf-8"
    )
    monkeypatch.setattr("lhp.webapp.static_app.resolve_static_dir", lambda: static_dir)
    with _client_for(monkeypatch, e2e_project_path) as test_client:
        yield test_client
