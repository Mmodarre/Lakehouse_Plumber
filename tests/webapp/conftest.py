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
    function-scoped. ``LHP_WEBAPP_PORT`` / ``LHP_WEBAPP_LOG_LEVEL`` are cleared
    so a developer's ambient env can't perturb the test app.

    Bound to the SHARED fixture project — do not use for write/delete/generate
    tests; use :func:`mutable_client` for those.
    """
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(e2e_project_path))
    monkeypatch.delenv("LHP_WEBAPP_PORT", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_LOG_LEVEL", raising=False)

    app = create_app()
    # ``with`` triggers the (logging-only) lifespan; raise_server_exceptions is
    # left at its default so genuine 500s surface in tests.
    with TestClient(app) as test_client:
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
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(mutable_project))
    monkeypatch.delenv("LHP_WEBAPP_PORT", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_LOG_LEVEL", raising=False)

    app = create_app()
    with TestClient(app) as test_client:
        yield test_client
