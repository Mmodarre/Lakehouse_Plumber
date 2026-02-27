"""Tests for OpenCodeProcess and OpenCodeProcessPool.

Covers port allocation, max processes, idle reaper, dev mode collapse,
get_or_create idempotency, shared skills deployment, and env-var-based
process configuration.
"""

import asyncio
import json
import os
import stat
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from lhp.api.services.ai_config import AIConfig
from lhp.api.services.opencode_manager import (
    OpenCodeProcess,
    OpenCodeProcessPool,
    _PORT_RANGE_SIZE,
    _PORT_RANGE_START,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(**overrides) -> AIConfig:
    defaults = {
        "max_processes": 3,
        "idle_timeout_minutes": 0.01,  # Very short for testing (0.6s)
    }
    defaults.update(overrides)
    return AIConfig(**defaults)


def _mock_process(*, returncode=None, stderr_data=b""):
    proc = MagicMock()
    proc.returncode = returncode
    proc.pid = 12345
    stderr_stream = AsyncMock()
    stderr_stream.read = AsyncMock(return_value=stderr_data)
    proc.stderr = stderr_stream
    proc.wait = AsyncMock()
    proc.send_signal = MagicMock()
    proc.kill = MagicMock()
    return proc


# ---------------------------------------------------------------------------
# OpenCodeProcess tests
# ---------------------------------------------------------------------------


class TestOpenCodeProcess:
    """Tests for the single-process wrapper."""

    def test_url_property(self):
        proc = OpenCodeProcess(
            user_id="test", workspace_path=Path("/tmp"), port=4096
        )
        assert proc.url == "http://localhost:4096"

    def test_auth_headers_with_password(self):
        proc = OpenCodeProcess(
            user_id="test", workspace_path=Path("/tmp"), port=4096, password="secret"
        )
        assert proc.auth_headers() == {"Authorization": "Bearer secret"}

    def test_auth_headers_without_password(self):
        proc = OpenCodeProcess(
            user_id="test", workspace_path=Path("/tmp"), port=4096
        )
        assert proc.auth_headers() == {}

    def test_touch_updates_activity(self):
        proc = OpenCodeProcess(
            user_id="test", workspace_path=Path("/tmp"), port=4096
        )
        old_activity = proc.last_activity
        import time as _time
        _time.sleep(0.01)
        proc.touch()
        assert proc.last_activity > old_activity

    def test_env_stored(self):
        env = {"FOO": "bar"}
        proc = OpenCodeProcess(
            user_id="test", workspace_path=Path("/tmp"), port=4096, env=env
        )
        assert proc._env is env

    def test_env_default_none(self):
        proc = OpenCodeProcess(
            user_id="test", workspace_path=Path("/tmp"), port=4096
        )
        assert proc._env is None

    @pytest.mark.asyncio
    async def test_try_restart_skipped_when_restarting(self):
        """_try_restart() is a no-op while _restarting is True."""
        proc = OpenCodeProcess(
            user_id="test", workspace_path=Path("/tmp"), port=4096
        )
        proc._restarting = True
        proc._process = _mock_process(returncode=1)
        initial = proc._restart_count
        await proc._try_restart()
        assert proc._restart_count == initial

    @pytest.mark.asyncio
    async def test_try_restart_skipped_when_stopping(self):
        proc = OpenCodeProcess(
            user_id="test", workspace_path=Path("/tmp"), port=4096
        )
        proc._stopping = True
        proc._process = _mock_process(returncode=1)
        await proc._try_restart()
        assert proc._restart_count == 0

    @pytest.mark.asyncio
    async def test_try_restart_increments_count(self):
        proc = OpenCodeProcess(
            user_id="test", workspace_path=Path("/tmp"), port=4096
        )
        proc._process = _mock_process(returncode=1)
        with patch.object(proc, "_spawn_process", new_callable=AsyncMock):
            await proc._try_restart()
        assert proc._restart_count == 1

    @pytest.mark.asyncio
    async def test_wait_for_healthy_returns_false_on_early_exit(self):
        proc = OpenCodeProcess(
            user_id="test", workspace_path=Path("/tmp"), port=4096
        )
        proc._process = _mock_process(returncode=1, stderr_data=b"port in use")
        result = await proc._wait_for_healthy()
        assert result is False

    @pytest.mark.asyncio
    async def test_wait_for_healthy_returns_true(self):
        proc = OpenCodeProcess(
            user_id="test", workspace_path=Path("/tmp"), port=4096
        )
        proc._process = _mock_process(returncode=None)
        with patch.object(proc, "health_check", new_callable=AsyncMock, return_value=True):
            result = await proc._wait_for_healthy()
        assert result is True

    @pytest.mark.asyncio
    async def test_ensure_workspace_noop_when_same(self):
        proc = OpenCodeProcess(
            user_id="test", workspace_path=Path("/workspace"), port=4096
        )
        proc._available = True
        with patch.object(proc, "_stop_process", new_callable=AsyncMock) as mock_stop:
            await proc.ensure_workspace(Path("/workspace"))
            mock_stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_workspace_restarts_on_change(self):
        proc = OpenCodeProcess(
            user_id="test", workspace_path=Path("/old"), port=4096
        )
        proc._available = True

        with (
            patch.object(proc, "_stop_process", new_callable=AsyncMock),
            patch.object(proc, "_spawn_process", new_callable=AsyncMock),
        ):
            await proc.ensure_workspace(Path("/new"))

        assert proc.workspace_path == Path("/new")
        assert proc._restarting is False


# ---------------------------------------------------------------------------
# OpenCodeProcessPool tests
# ---------------------------------------------------------------------------


class TestOpenCodeProcessPool:
    """Tests for the process pool."""

    def test_port_allocation_sequential(self):
        pool = OpenCodeProcessPool(
            ai_config=_make_config(), dev_mode=False
        )
        ports = [pool._allocate_port() for _ in range(5)]
        assert ports == list(range(_PORT_RANGE_START, _PORT_RANGE_START + 5))

    def test_port_allocation_exhaustion(self):
        pool = OpenCodeProcessPool(
            ai_config=_make_config(), dev_mode=False
        )
        pool._allocated_ports = set(
            range(_PORT_RANGE_START, _PORT_RANGE_START + _PORT_RANGE_SIZE)
        )
        with pytest.raises(RuntimeError, match="No available ports"):
            pool._allocate_port()

    @pytest.mark.asyncio
    async def test_get_or_create_idempotent(self, tmp_path):
        """Calling get_or_create twice returns the same process."""
        pool = OpenCodeProcessPool(
            ai_config=_make_config(), dev_mode=False
        )
        with (
            patch.object(pool, "_build_process_env", return_value={}),
            patch.object(
                OpenCodeProcess, "start", new_callable=AsyncMock
            ),
            patch.object(
                OpenCodeProcess, "ensure_workspace", new_callable=AsyncMock
            ),
        ):
            p1 = await pool.get_or_create("user-abc", tmp_path)
            p2 = await pool.get_or_create("user-abc", tmp_path)
        assert p1 is p2

    @pytest.mark.asyncio
    async def test_max_processes_enforced(self, tmp_path):
        """Pool raises RuntimeError when at capacity."""
        config = _make_config(max_processes=2)
        pool = OpenCodeProcessPool(ai_config=config, dev_mode=False)

        with (
            patch.object(pool, "_build_process_env", return_value={}),
            patch.object(OpenCodeProcess, "start", new_callable=AsyncMock),
            patch.object(
                OpenCodeProcess, "ensure_workspace", new_callable=AsyncMock
            ),
        ):
            await pool.get_or_create("user-1", tmp_path)
            await pool.get_or_create("user-2", tmp_path)
            with pytest.raises(RuntimeError, match="at capacity"):
                await pool.get_or_create("user-3", tmp_path)

    @pytest.mark.asyncio
    async def test_dev_mode_collapses_to_single(self, tmp_path):
        """In dev mode, all users share the dev-local process."""
        pool = OpenCodeProcessPool(
            ai_config=_make_config(),
            dev_mode=True,
            project_root=tmp_path,
            default_port=4096,
        )

        with (
            patch.object(pool, "_build_process_env", return_value={}),
            patch.object(OpenCodeProcess, "start", new_callable=AsyncMock),
            patch.object(
                OpenCodeProcess, "ensure_workspace", new_callable=AsyncMock
            ),
        ):
            p1 = await pool.get_or_create("user-1", tmp_path)
            p2 = await pool.get_or_create("user-2", tmp_path)
        assert p1 is p2
        assert p1.user_id == "dev-local"

    @pytest.mark.asyncio
    async def test_release_stops_and_removes(self, tmp_path):
        pool = OpenCodeProcessPool(
            ai_config=_make_config(), dev_mode=False
        )

        with (
            patch.object(pool, "_build_process_env", return_value={}),
            patch.object(OpenCodeProcess, "start", new_callable=AsyncMock),
            patch.object(
                OpenCodeProcess, "ensure_workspace", new_callable=AsyncMock
            ),
        ):
            proc = await pool.get_or_create("user-abc", tmp_path)

        with patch.object(proc, "stop", new_callable=AsyncMock) as mock_stop:
            await pool.release("user-abc")
            mock_stop.assert_called_once()

        assert "user-abc" not in pool._processes

    @pytest.mark.asyncio
    async def test_get_status_dev_mode(self, tmp_path):
        pool = OpenCodeProcessPool(
            ai_config=_make_config(),
            dev_mode=True,
            project_root=tmp_path,
            default_port=4096,
        )
        status = pool.get_status()
        assert status["mode"] == "dev"
        assert status["available"] is False  # No process started yet

    @pytest.mark.asyncio
    async def test_get_status_production(self):
        config = _make_config(max_processes=10)
        pool = OpenCodeProcessPool(ai_config=config, dev_mode=False)
        status = pool.get_status()
        assert status["mode"] == "production"
        assert status["available"] is True
        assert status["max_processes"] == 10

    @pytest.mark.asyncio
    async def test_get_status_external(self):
        pool = OpenCodeProcessPool(
            ai_config=_make_config(),
            external_url="http://external:4096",
        )
        status = pool.get_status()
        assert status["mode"] == "external"
        assert status["url"] == "http://external:4096"

    @pytest.mark.asyncio
    async def test_stop_stops_all_processes(self, tmp_path):
        pool = OpenCodeProcessPool(
            ai_config=_make_config(), dev_mode=False
        )

        with (
            patch.object(pool, "_build_process_env", return_value={}),
            patch.object(OpenCodeProcess, "start", new_callable=AsyncMock),
            patch.object(
                OpenCodeProcess, "ensure_workspace", new_callable=AsyncMock
            ),
        ):
            await pool.get_or_create("user-1", tmp_path)
            await pool.get_or_create("user-2", tmp_path)

        with patch.object(OpenCodeProcess, "stop", new_callable=AsyncMock):
            await pool.stop()

        assert len(pool._processes) == 0
        assert len(pool._allocated_ports) == 0

    @pytest.mark.asyncio
    async def test_idle_reaper_removes_idle_processes(self, tmp_path):
        """Idle reaper should remove processes past the timeout."""
        config = _make_config(idle_timeout_minutes=0.0001)  # ~0.006s
        pool = OpenCodeProcessPool(ai_config=config, dev_mode=False)

        # Manually add a process with old activity time
        proc = OpenCodeProcess(
            user_id="idle-user", workspace_path=tmp_path, port=4096
        )
        proc.last_activity = time.monotonic() - 3600  # 1 hour ago
        pool._processes["idle-user"] = proc
        pool._allocated_ports.add(4096)

        original_sleep = asyncio.sleep

        async def fast_sleep(duration):
            """Replace 60s sleep with 0.05s so the reaper fires quickly."""
            await original_sleep(min(duration, 0.05))

        with (
            patch.object(proc, "stop", new_callable=AsyncMock),
            patch("asyncio.sleep", side_effect=fast_sleep),
        ):
            pool._stopping = False
            reaper = asyncio.create_task(pool._idle_reaper())
            await original_sleep(0.3)  # Wait for reaper to run at least once
            pool._stopping = True
            reaper.cancel()
            try:
                await reaper
            except asyncio.CancelledError:
                pass

        assert "idle-user" not in pool._processes


# ---------------------------------------------------------------------------
# Shared skills deployment
# ---------------------------------------------------------------------------


class TestSharedSkillsDeployment:
    """Test _deploy_shared_skills creates a temp dir with clean skill files."""

    @pytest.mark.asyncio
    async def test_deploys_skill_without_managed_header(self):
        """SKILL.md is copied verbatim — no managed header prepended."""
        pool = OpenCodeProcessPool(
            ai_config=_make_config(), dev_mode=True, project_root=Path("/tmp")
        )
        shared_dir = await pool._deploy_shared_skills()
        try:
            skill_file = shared_dir / "skills" / "lhp" / "SKILL.md"
            assert skill_file.exists()
            content = skill_file.read_text()
            # Should start with YAML frontmatter, not LHP-MANAGED header
            assert content.startswith("---")
            assert "LHP-MANAGED" not in content
        finally:
            # Make writable for cleanup
            for dirpath, dirnames, filenames in os.walk(shared_dir):
                for fname in filenames:
                    os.chmod(os.path.join(dirpath, fname), 0o644)
                os.chmod(dirpath, 0o755)
            import shutil
            shutil.rmtree(shared_dir)

    @pytest.mark.asyncio
    async def test_deploys_reference_files(self):
        """Reference .md files are deployed under skills/lhp/references/."""
        pool = OpenCodeProcessPool(
            ai_config=_make_config(), dev_mode=True, project_root=Path("/tmp")
        )
        shared_dir = await pool._deploy_shared_skills()
        try:
            refs_dir = shared_dir / "skills" / "lhp" / "references"
            assert refs_dir.exists()
            ref_files = list(refs_dir.glob("*.md"))
            assert len(ref_files) > 0
            # None should have managed headers
            for ref_file in ref_files:
                content = ref_file.read_text()
                assert "LHP-MANAGED" not in content
        finally:
            for dirpath, dirnames, filenames in os.walk(shared_dir):
                for fname in filenames:
                    os.chmod(os.path.join(dirpath, fname), 0o644)
                os.chmod(dirpath, 0o755)
            import shutil
            shutil.rmtree(shared_dir)

    @pytest.mark.asyncio
    async def test_directory_is_read_only(self):
        """Deployed directory and files are made read-only (0o555/0o444)."""
        pool = OpenCodeProcessPool(
            ai_config=_make_config(), dev_mode=True, project_root=Path("/tmp")
        )
        shared_dir = await pool._deploy_shared_skills()
        try:
            # Check directory permissions
            dir_mode = stat.S_IMODE(shared_dir.stat().st_mode)
            assert dir_mode == 0o555

            # Check file permissions
            skill_file = shared_dir / "skills" / "lhp" / "SKILL.md"
            file_mode = stat.S_IMODE(skill_file.stat().st_mode)
            assert file_mode == 0o444
        finally:
            for dirpath, dirnames, filenames in os.walk(shared_dir):
                for fname in filenames:
                    os.chmod(os.path.join(dirpath, fname), 0o644)
                os.chmod(dirpath, 0o755)
            import shutil
            shutil.rmtree(shared_dir)

    @pytest.mark.asyncio
    async def test_temp_dir_prefix(self):
        """Temp directory uses the lhp-opencode- prefix."""
        pool = OpenCodeProcessPool(
            ai_config=_make_config(), dev_mode=True, project_root=Path("/tmp")
        )
        shared_dir = await pool._deploy_shared_skills()
        try:
            assert "lhp-opencode-" in shared_dir.name
        finally:
            for dirpath, dirnames, filenames in os.walk(shared_dir):
                for fname in filenames:
                    os.chmod(os.path.join(dirpath, fname), 0o644)
                os.chmod(dirpath, 0o755)
            import shutil
            shutil.rmtree(shared_dir)


# ---------------------------------------------------------------------------
# Process environment builder
# ---------------------------------------------------------------------------


class TestProcessEnvBuilder:
    """Test _build_process_env creates the correct env dict."""

    def test_sets_config_content(self):
        """OPENCODE_CONFIG_CONTENT contains valid JSON matching AIConfig."""
        pool = OpenCodeProcessPool(
            ai_config=_make_config(), dev_mode=True, project_root=Path("/tmp")
        )
        env = pool._build_process_env()
        assert "OPENCODE_CONFIG_CONTENT" in env
        config = json.loads(env["OPENCODE_CONFIG_CONTENT"])
        assert "$schema" in config
        assert config["model"] == "anthropic/databricks-claude-sonnet-4-6"

    def test_sets_config_dir(self):
        """OPENCODE_CONFIG_DIR is set when _shared_config_dir is not None."""
        pool = OpenCodeProcessPool(
            ai_config=_make_config(), dev_mode=True, project_root=Path("/tmp")
        )
        pool._shared_config_dir = Path("/some/shared/dir")
        env = pool._build_process_env()
        assert env["OPENCODE_CONFIG_DIR"] == "/some/shared/dir"

    def test_omits_config_dir_when_none(self):
        """OPENCODE_CONFIG_DIR is not set when _shared_config_dir is None."""
        pool = OpenCodeProcessPool(
            ai_config=_make_config(), dev_mode=True, project_root=Path("/tmp")
        )
        assert pool._shared_config_dir is None
        env = pool._build_process_env()
        # Should not be set unless it was already in os.environ
        if "OPENCODE_CONFIG_DIR" not in os.environ:
            assert "OPENCODE_CONFIG_DIR" not in env

    def test_sets_disable_project_config(self):
        """OPENCODE_DISABLE_PROJECT_CONFIG is always set to '1'."""
        pool = OpenCodeProcessPool(
            ai_config=_make_config(), dev_mode=True, project_root=Path("/tmp")
        )
        env = pool._build_process_env()
        assert env["OPENCODE_DISABLE_PROJECT_CONFIG"] == "1"

    def test_inherits_os_environ(self):
        """Env dict includes existing environment variables."""
        pool = OpenCodeProcessPool(
            ai_config=_make_config(), dev_mode=True, project_root=Path("/tmp")
        )
        env = pool._build_process_env()
        # PATH should always be present from os.environ
        assert "PATH" in env
