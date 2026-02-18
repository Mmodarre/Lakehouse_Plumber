import shutil

import pytest
from pathlib import Path
from fastapi.testclient import TestClient

from lhp.api.app import create_app
from lhp.api.config import APISettings

_FIXTURE_SOURCE = Path(__file__).parent.parent / "e2e" / "fixtures" / "testing_project"


# ---------------------------------------------------------------------------
# Read-only fixtures (each test gets its own isolated deep copy)
# ---------------------------------------------------------------------------


@pytest.fixture
def e2e_project_path(tmp_path: Path) -> Path:
    """Deep copy of the E2E testing project fixture for test isolation."""
    dest = tmp_path / "testing_project"
    shutil.copytree(_FIXTURE_SOURCE, dest)
    return dest


@pytest.fixture
def api_settings(e2e_project_path: Path) -> APISettings:
    """API settings pointing to the isolated fixture project."""
    return APISettings(
        project_root=e2e_project_path,
        dev_mode=True,
        log_level="DEBUG",
    )


@pytest.fixture
def app(api_settings: APISettings):
    """FastAPI app configured for testing."""
    return create_app(settings=api_settings)


@pytest.fixture
def client(app) -> TestClient:
    """Test client for making HTTP requests."""
    return TestClient(app)


# ---------------------------------------------------------------------------
# Mutable fixtures (session-scoped deep copy for write endpoints)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def mutable_project_path(tmp_path_factory) -> Path:
    """Deep-copy of the fixture project for tests that write files.

    Session-scoped so the expensive copy only happens once per test run.
    Write tests should be designed to be independent of each other.
    """
    dest = tmp_path_factory.mktemp("api_mutable") / "testing_project"
    shutil.copytree(_FIXTURE_SOURCE, dest)
    return dest


@pytest.fixture(scope="session")
def mutable_settings(mutable_project_path: Path) -> APISettings:
    """API settings pointing to the mutable project copy."""
    return APISettings(
        project_root=mutable_project_path,
        dev_mode=True,
        log_level="DEBUG",
    )


@pytest.fixture(scope="session")
def mutable_app(mutable_settings: APISettings):
    """FastAPI app using the mutable project copy."""
    return create_app(settings=mutable_settings)


@pytest.fixture(scope="session")
def mutable_client(mutable_app) -> TestClient:
    """Test client for write endpoints (generate, config update, cleanup)."""
    return TestClient(mutable_app)


# ---------------------------------------------------------------------------
# Production mode fixtures (dev_mode=False for auth testing)
# ---------------------------------------------------------------------------


@pytest.fixture
def prod_settings(e2e_project_path: Path) -> APISettings:
    """API settings in production mode (auth headers required)."""
    return APISettings(
        project_root=e2e_project_path,
        dev_mode=False,
        log_level="DEBUG",
    )


@pytest.fixture
def prod_app(prod_settings: APISettings):
    """FastAPI app in production mode."""
    return create_app(settings=prod_settings)


@pytest.fixture
def prod_client(prod_app) -> TestClient:
    """Test client in production mode (no dev user fallback)."""
    return TestClient(prod_app)
