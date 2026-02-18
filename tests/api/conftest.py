import os
import shutil
import subprocess

import pytest
from pathlib import Path
from fastapi.testclient import TestClient

from lhp.api.app import create_app
from lhp.api.auth import UserContext
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


# ---------------------------------------------------------------------------
# Phase 2 fixtures: Git, workspace, and auth for CRUD endpoints
# ---------------------------------------------------------------------------

_GIT_ENV = {
    **os.environ,
    "GIT_AUTHOR_NAME": "Test",
    "GIT_AUTHOR_EMAIL": "test@test.com",
    "GIT_COMMITTER_NAME": "Test",
    "GIT_COMMITTER_EMAIL": "test@test.com",
}


@pytest.fixture
def mock_git_remote(tmp_path: Path) -> Path:
    """Create a bare git repo to act as a remote.

    Avoids network calls while exercising real git clone/push/pull.
    Contains a minimal LHP project structure (lhp.yaml + pipelines/).
    """
    bare_repo = tmp_path / "remote.git"
    bare_repo.mkdir()
    subprocess.run(["git", "init", "--bare", str(bare_repo)], check=True)

    # Create initial commit in a working copy
    work_dir = tmp_path / "setup_work"
    work_dir.mkdir()
    subprocess.run(["git", "clone", str(bare_repo), str(work_dir)], check=True)

    # Add minimal LHP project structure
    (work_dir / "lhp.yaml").write_text("project_name: test\n")
    (work_dir / "pipelines").mkdir()
    subprocess.run(["git", "add", "-A"], cwd=str(work_dir), check=True)
    subprocess.run(
        ["git", "commit", "-m", "Initial commit"],
        cwd=str(work_dir),
        check=True,
        env=_GIT_ENV,
    )
    subprocess.run(["git", "push"], cwd=str(work_dir), check=True)
    return bare_repo


@pytest.fixture
def mock_workspace_root(tmp_path: Path) -> Path:
    """Temporary workspace root directory for workspace manager tests."""
    workspace_root = tmp_path / "workspaces"
    workspace_root.mkdir()
    return workspace_root


@pytest.fixture
def test_user() -> UserContext:
    """Test UserContext for workspace operations."""
    return UserContext(
        email="test@test.com",
        username="testuser",
        user_id="test-user-123",
    )


@pytest.fixture
def auth_headers() -> dict:
    """Auth headers for production-mode requests (matches test_user)."""
    return {
        "X-Forwarded-Email": "test@test.com",
        "X-Forwarded-User": "testuser",
        "X-Forwarded-User-Id": "test-user-123",
    }


@pytest.fixture
def workspace_project_source(tmp_path: Path, mock_git_remote: Path) -> Path:
    """A working-copy clone of mock_git_remote for use as project source.

    Returns a git-initialized project root that the WorkspaceManager can
    clone from when creating user workspaces.
    """
    work_dir = tmp_path / "source_project"
    subprocess.run(
        ["git", "clone", str(mock_git_remote), str(work_dir)], check=True
    )
    return work_dir
