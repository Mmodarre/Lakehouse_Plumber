"""OpenCode process pool for AI assistant integration.

Manages per-user OpenCode server processes:

- **Dev mode**: a single shared process keyed ``"dev-local"``, running at
  a fixed port with ``cwd=project_root``.
- **Production mode**: each user gets their own process (keyed by
  ``user_id_hash``), dynamically allocated a port from a pool, with
  ``cwd=workspace_path``.  An idle reaper background task cleans up
  processes that exceed the configured timeout.

Replaces the former single-process ``OpenCodeManager`` with a pool-based
architecture that supports multi-user deployments.
"""

import asyncio
import json
import logging
import os
import shutil
import signal
import tempfile
import time
from pathlib import Path
from typing import Optional

import httpx

from lhp.api.services.ai_config import AIConfig

logger = logging.getLogger(__name__)

# OpenCode health-check and restart constants
_HEALTH_INTERVAL_S = 5.0
_HEALTH_TIMEOUT_S = 3.0
_MAX_RESTART_ATTEMPTS = 3
_SHUTDOWN_GRACE_S = 5.0
_STARTUP_WAIT_S = 10.0
_STARTUP_POLL_S = 0.5

# Port range for dynamically allocated processes
_PORT_RANGE_START = 4096
_PORT_RANGE_SIZE = 100

# Maximum process lifetime — safety margin before 1h OAuth token expiry
_MAX_LIFETIME_S = 50 * 60


def _get_databricks_oauth_token() -> Optional[str]:
    """Get fresh OAuth token via databricks-sdk (zero-config in Databricks Apps).

    Returns None if SDK not installed or auth fails.
    """
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        headers = w.config.authenticate()
        bearer = headers.get("Authorization", "")
        if bearer.startswith("Bearer "):
            return bearer[7:]
        return None
    except ImportError:
        return None
    except Exception:
        logger.warning("Failed to obtain Databricks OAuth token", exc_info=True)
        return None


class OpenCodeProcess:
    """Manages a single OpenCode subprocess.

    Encapsulates spawning, stopping, health checking, and auto-restart
    for one OpenCode server instance.
    """

    def __init__(
        self,
        *,
        user_id: str,
        workspace_path: Path,
        port: int,
        password: Optional[str] = None,
        cors_origin: str = "http://localhost:5173",
        env: Optional[dict[str, str]] = None,
    ) -> None:
        self.user_id = user_id
        self.workspace_path = workspace_path
        self.port = port
        self._password = password
        self._cors_origin = cors_origin
        self._env = env

        # Runtime state
        self._process: Optional[asyncio.subprocess.Process] = None
        self._health_task: Optional[asyncio.Task] = None
        self._restart_count = 0
        self._available = False
        self._stopping = False
        self._restarting = False
        self.last_activity: float = time.monotonic()
        self.created_at: float = time.monotonic()

    # ── Properties ────────────────────────────────────────────

    @property
    def available(self) -> bool:
        return self._available

    @property
    def url(self) -> str:
        return f"http://localhost:{self.port}"

    @property
    def auth_required(self) -> bool:
        return self._password is not None

    def auth_headers(self) -> dict[str, str]:
        """Build authorization headers for proxied requests."""
        if self._password:
            return {"Authorization": f"Bearer {self._password}"}
        return {}

    def touch(self) -> None:
        """Update last_activity timestamp (called on each user interaction)."""
        self.last_activity = time.monotonic()

    # ── Lifecycle ─────────────────────────────────────────────

    async def start(self) -> None:
        """Spawn the OpenCode subprocess and start health monitoring."""
        await self._spawn_process()
        self._health_task = asyncio.create_task(self._health_monitor())

    async def stop(self) -> None:
        """Gracefully shut down the subprocess and cancel health monitor."""
        self._stopping = True
        self._available = False

        if self._health_task and not self._health_task.done():
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass

        await self._stop_process()

    async def ensure_workspace(self, workspace_path: Path) -> None:
        """Restart the process if the workspace has changed.

        In dev mode (single process), this is typically a no-op since
        workspace_path doesn't change.  In production, this handles the
        case where a user's workspace may have been recreated.
        """
        target_cwd = str(workspace_path)
        current_cwd = str(self.workspace_path)

        if current_cwd == target_cwd and self._available:
            logger.debug(f"Process {self.user_id}: already in correct workspace")
            return

        logger.info(f"Process {self.user_id}: workspace change {current_cwd} → {target_cwd}")
        self.workspace_path = workspace_path

        self._restarting = True
        try:
            await self._stop_process()
            self._restart_count = 0
            await self._spawn_process()
        finally:
            self._restarting = False

    async def health_check(self) -> bool:
        """Single health check against the OpenCode server."""
        try:
            async with httpx.AsyncClient(timeout=_HEALTH_TIMEOUT_S) as client:
                resp = await client.get(
                    f"{self.url}/global/health",
                    headers=self.auth_headers(),
                )
                return resp.status_code == 200
        except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPError):
            return False

    # ── Subprocess management (private) ───────────────────────

    async def _spawn_process(self) -> None:
        """Spawn the ``opencode serve`` subprocess."""
        binary = shutil.which("opencode")
        if not binary:
            node_bin = Path("./node_modules/.bin/opencode")
            if node_bin.exists():
                binary = str(node_bin.resolve())
        if not binary:
            logger.error(
                "opencode binary not found on PATH. "
                "Install with: brew install opencode  or  npm install -g opencode"
            )
            self._available = False
            return

        # Ensure the binary path is absolute — shutil.which() can return
        # relative paths, which fail when the subprocess CWD differs from
        # the main process CWD (e.g. user workspace vs deploy directory).
        binary_path = Path(binary)
        if not binary_path.is_absolute():
            binary = str(binary_path.resolve())
            logger.debug(f"Resolved relative binary path to: {binary}")

        cwd = str(self.workspace_path)
        cmd = [binary, "serve", "--port", str(self.port)]
        logger.info(f"Spawning OpenCode: {' '.join(cmd)} (cwd={cwd}, user={self.user_id})")

        # Log presence of critical AI env vars (not values) for diagnostics.
        env_source = self._env or os.environ
        _AI_ENV_VARS = [
            "ANTHROPIC_BASE_URL",
            "ANTHROPIC_API_KEY",
            "ANTHROPIC_MODEL",
            "ANTHROPIC_CUSTOM_HEADERS",
            "OPENCODE_CONFIG_DIR",
            "OPENCODE_CONFIG_CONTENT",
            "OPENCODE_DISABLE_PROJECT_CONFIG",
        ]
        for var in _AI_ENV_VARS:
            logger.debug(f"AI env var {var}: {'set' if var in env_source else 'NOT set'}")
        if "ANTHROPIC_BASE_URL" not in env_source:
            logger.warning(
                "ANTHROPIC_BASE_URL is not set — "
                "OpenCode may fail to connect to Databricks Model Serving"
            )

        self._process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self._env,
        )

        self._available = await self._wait_for_healthy()
        if self._available:
            logger.info(f"OpenCode ready at {self.url} (user={self.user_id})")
            self._restart_count = 0
        else:
            logger.error(f"OpenCode failed to start (user={self.user_id})")

    async def _stop_process(self) -> None:
        """Stop the current subprocess if running."""
        if self._process and self._process.returncode is None:
            logger.info(f"Stopping OpenCode subprocess (user={self.user_id})")
            try:
                self._process.send_signal(signal.SIGTERM)
                try:
                    await asyncio.wait_for(
                        self._process.wait(), timeout=_SHUTDOWN_GRACE_S
                    )
                except asyncio.TimeoutError:
                    logger.warning("OpenCode did not exit gracefully, sending SIGKILL")
                    self._process.kill()
                    await self._process.wait()
            except ProcessLookupError:
                pass
            logger.info(f"OpenCode subprocess stopped (user={self.user_id})")
        self._process = None
        self._available = False

    async def _wait_for_healthy(self) -> bool:
        """Poll health endpoint until ready or timeout."""
        elapsed = 0.0
        while elapsed < _STARTUP_WAIT_S:
            if self._process and self._process.returncode is not None:
                stderr_snippet = ""
                if self._process.stderr:
                    try:
                        raw = await asyncio.wait_for(
                            self._process.stderr.read(2048), timeout=1.0
                        )
                        stderr_snippet = raw.decode(errors="replace").strip()
                    except Exception:
                        pass
                logger.error(
                    f"OpenCode exited immediately (rc={self._process.returncode})"
                    + (f": {stderr_snippet}" if stderr_snippet else "")
                )
                return False

            if await self.health_check():
                return True
            await asyncio.sleep(_STARTUP_POLL_S)
            elapsed += _STARTUP_POLL_S
        return False

    async def _try_restart(self) -> None:
        """Attempt to restart a crashed subprocess."""
        if self._stopping or self._restarting:
            return

        self._restart_count += 1
        if self._restart_count > _MAX_RESTART_ATTEMPTS:
            logger.error(
                f"OpenCode crashed {_MAX_RESTART_ATTEMPTS} times "
                f"(user={self.user_id}) — giving up"
            )
            self._available = False
            return

        logger.warning(
            f"OpenCode crashed (user={self.user_id}), "
            f"restart attempt {self._restart_count}/{_MAX_RESTART_ATTEMPTS}"
        )
        if self._process:
            try:
                await self._process.wait()
            except Exception:
                pass

        await self._spawn_process()

    async def _health_monitor(self) -> None:
        """Periodically check server health, restart if needed."""
        while not self._stopping:
            await asyncio.sleep(_HEALTH_INTERVAL_S)
            if self._stopping:
                break

            healthy = await self.health_check()
            if healthy:
                if not self._available:
                    logger.info(f"OpenCode recovered (user={self.user_id})")
                self._available = True
            else:
                if self._available:
                    logger.warning(f"OpenCode health check failed (user={self.user_id})")
                self._available = False

                if self._process and self._process.returncode is not None:
                    await self._try_restart()


class OpenCodeProcessPool:
    """Manages a pool of per-user OpenCode processes.

    In dev mode, all requests collapse to a single shared process.
    In production mode, each user gets their own process with dynamic
    port allocation and idle timeout reaping.
    """

    def __init__(
        self,
        *,
        ai_config: AIConfig,
        dev_mode: bool = False,
        project_root: Optional[Path] = None,
        default_port: int = 4096,
        password: Optional[str] = None,
        external_url: Optional[str] = None,
    ) -> None:
        self._ai_config = ai_config
        self._dev_mode = dev_mode
        self._project_root = project_root
        self._default_port = default_port
        self._password = password
        self._external_url = external_url

        # Process pool keyed by user_id or "dev-local"
        self._processes: dict[str, OpenCodeProcess] = {}
        self._allocated_ports: set[int] = set()
        self._lock = asyncio.Lock()
        self._reaper_task: Optional[asyncio.Task] = None
        self._session_reaper_task: Optional[asyncio.Task] = None
        self._stopping = False

        # External mode state
        self._external_available = False

        # Shared config directory for skills (created in start())
        self._shared_config_dir: Optional[Path] = None

    # ── Properties ────────────────────────────────────────────

    @property
    def is_external(self) -> bool:
        return self._external_url is not None

    @property
    def external_url(self) -> Optional[str]:
        if self._external_url:
            return self._external_url.rstrip("/")
        return None

    # ── Lifecycle ─────────────────────────────────────────────

    async def start(self) -> None:
        """Start the pool.

        In dev mode: spawns the single shared process immediately.
        In external mode: verifies connectivity.
        In production mode: starts the idle reaper only (processes
        are spawned lazily on first request).
        """
        if self.is_external:
            logger.info(f"AI pool: connecting to external OpenCode at {self.external_url}")
            self._external_available = await self._external_health_check()
            if not self._external_available:
                logger.warning(
                    f"External OpenCode at {self.external_url} is not reachable"
                )
            return

        # Deploy shared skills directory (used by all processes)
        self._shared_config_dir = await self._deploy_shared_skills()

        if self._dev_mode and self._project_root:
            # Dev mode: spawn single process immediately
            process = OpenCodeProcess(
                user_id="dev-local",
                workspace_path=self._project_root,
                port=self._default_port,
                password=self._password,
                env=self._build_process_env(),
            )
            await process.start()
            self._processes["dev-local"] = process
            self._allocated_ports.add(self._default_port)
            logger.info("AI pool: dev-mode single process started")
        else:
            # Production mode: just verify binary exists
            binary = shutil.which("opencode")
            if not binary:
                node_bin = Path("./node_modules/.bin/opencode")
                if node_bin.exists():
                    binary = str(node_bin.resolve())
            if not binary:
                logger.error(
                    "opencode binary not found on PATH. "
                    "Install with: brew install opencode  or  npm install -g opencode"
                )

        # Start idle reaper (production mode only)
        if not self._dev_mode:
            self._reaper_task = asyncio.create_task(self._idle_reaper())

        # Start session reaper (all modes — dev-local sessions accumulate too)
        self._session_reaper_task = asyncio.create_task(self._session_reaper())

    async def stop(self) -> None:
        """Stop all processes and the reaper task."""
        self._stopping = True

        for task in (self._reaper_task, self._session_reaper_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Stop all processes concurrently
        if self._processes:
            await asyncio.gather(
                *(p.stop() for p in self._processes.values()),
                return_exceptions=True,
            )
            self._processes.clear()
            self._allocated_ports.clear()

        # Clean up shared config directory
        if self._shared_config_dir and self._shared_config_dir.exists():
            # Make writable first (we set it read-only in _deploy_shared_skills)
            for dirpath, dirnames, filenames in os.walk(self._shared_config_dir):
                for fname in filenames:
                    os.chmod(os.path.join(dirpath, fname), 0o644)
                os.chmod(dirpath, 0o755)
            shutil.rmtree(self._shared_config_dir)
            self._shared_config_dir = None

        logger.info("AI pool: all processes stopped")

    # ── Process access ────────────────────────────────────────

    async def get_or_create(
        self,
        user_id_hash: str,
        workspace_path: Path,
    ) -> OpenCodeProcess:
        """Get an existing process for the user, or create a new one.

        In dev mode, ``user_id_hash`` is ignored and the shared process
        is returned.  In production, a new process is spawned if none
        exists, with dynamic port allocation.

        Raises ``RuntimeError`` if the pool is at max capacity.
        """
        if self.is_external:
            raise RuntimeError("get_or_create not applicable in external mode")

        key = "dev-local" if self._dev_mode else user_id_hash

        async with self._lock:
            # Return existing process
            if key in self._processes:
                proc = self._processes[key]
                proc.touch()
                # Ensure workspace is correct (may have changed)
                await proc.ensure_workspace(workspace_path)
                return proc

            # Check capacity (production only)
            if not self._dev_mode:
                if len(self._processes) >= self._ai_config.max_processes:
                    raise RuntimeError(
                        f"AI process pool at capacity ({self._ai_config.max_processes})"
                    )

            # Allocate port
            port = self._allocate_port()

            # Create and start new process
            process = OpenCodeProcess(
                user_id=key,
                workspace_path=workspace_path,
                port=port,
                password=self._password,
                env=self._build_process_env(),
            )
            await process.start()
            self._processes[key] = process
            logger.info(
                f"AI pool: spawned process for user={key} "
                f"on port={port} (total={len(self._processes)})"
            )
            return process

    async def release(self, user_id_hash: str) -> None:
        """Stop and remove a user's process from the pool."""
        key = "dev-local" if self._dev_mode else user_id_hash
        async with self._lock:
            process = self._processes.pop(key, None)
            if process:
                self._allocated_ports.discard(process.port)
                await process.stop()
                logger.info(f"AI pool: released process for user={key}")

    def get_status(self) -> dict:
        """Return pool status for the /api/ai/status endpoint."""
        if self.is_external:
            return {
                "available": self._external_available,
                "mode": "external",
                "url": self.external_url,
            }

        if self._dev_mode:
            dev_proc = self._processes.get("dev-local")
            return {
                "available": dev_proc.available if dev_proc else False,
                "mode": "dev",
                "active_processes": len(self._processes),
            }

        return {
            "available": True,  # Pool itself is available; processes are lazy
            "mode": "production",
            "active_processes": len(self._processes),
            "max_processes": self._ai_config.max_processes,
        }

    # ── Port allocation ───────────────────────────────────────

    def _allocate_port(self) -> int:
        """Find the next available port in the range."""
        for port in range(_PORT_RANGE_START, _PORT_RANGE_START + _PORT_RANGE_SIZE):
            if port not in self._allocated_ports:
                self._allocated_ports.add(port)
                return port
        raise RuntimeError("No available ports in the allocation range")

    # ── Idle reaper (production only) ─────────────────────────

    async def _idle_reaper(self) -> None:
        """Periodically stop processes that have been idle too long."""
        timeout_s = self._ai_config.idle_timeout_minutes * 60
        while not self._stopping:
            await asyncio.sleep(60)  # Check every minute
            if self._stopping:
                break

            now = time.monotonic()
            to_reap: list[tuple[str, str]] = []

            for key, proc in self._processes.items():
                if key == "dev-local":
                    continue
                idle_s = now - proc.last_activity
                age_s = now - proc.created_at
                if idle_s > timeout_s:
                    to_reap.append((key, f"idle {idle_s / 60:.1f}m"))
                elif age_s > _MAX_LIFETIME_S:
                    to_reap.append((key, f"max-lifetime {age_s / 60:.1f}m"))

            for key, reason in to_reap:
                logger.info(f"AI pool: reaping process for user={key} ({reason})")
                await self.release(key)

    async def _session_reaper(self) -> None:
        """Periodically delete old sessions from running OpenCode processes.

        Runs every 5 minutes.  For each process, fetches sessions, filters
        to root-only (no ``parentID``), and deletes any older than
        ``session_max_age_hours``.  OpenCode's ``DELETE /session/:id``
        cascade-deletes children, messages, and parts automatically.
        """
        max_age_s = self._ai_config.session_max_age_hours * 3600
        while not self._stopping:
            await asyncio.sleep(300)  # Every 5 minutes
            if self._stopping:
                break

            now_ms = time.time() * 1000  # OpenCode uses epoch milliseconds

            for key, proc in list(self._processes.items()):
                if not proc.available:
                    continue
                try:
                    async with httpx.AsyncClient() as client:
                        resp = await client.get(
                            f"{proc.url}/session",
                            headers=proc.auth_headers(),
                            timeout=10.0,
                        )
                    if resp.status_code != 200:
                        continue
                    sessions = resp.json()
                    if not isinstance(sessions, list):
                        continue

                    for sess in sessions:
                        # Skip child/subagent sessions (deleted via cascade)
                        if sess.get("parentID"):
                            continue
                        created_ms = (sess.get("time") or {}).get("created", 0)
                        age_s = (now_ms - created_ms) / 1000
                        if age_s > max_age_s:
                            sess_id = sess.get("id")
                            if not sess_id:
                                continue
                            try:
                                async with httpx.AsyncClient() as client:
                                    await client.delete(
                                        f"{proc.url}/session/{sess_id}",
                                        headers=proc.auth_headers(),
                                        timeout=10.0,
                                    )
                                logger.debug(
                                    f"AI pool: reaped session {sess_id} "
                                    f"for user={key} (age {age_s / 3600:.1f}h)"
                                )
                            except Exception:
                                logger.debug(
                                    f"AI pool: failed to delete session {sess_id}",
                                    exc_info=True,
                                )
                except Exception:
                    logger.debug(
                        f"AI pool: session reaper error for user={key}",
                        exc_info=True,
                    )

    # ── External mode health check ────────────────────────────

    async def _external_health_check(self) -> bool:
        """Check health of an external OpenCode server."""
        try:
            headers = {}
            if self._password:
                headers["Authorization"] = f"Bearer {self._password}"
            async with httpx.AsyncClient(timeout=_HEALTH_TIMEOUT_S) as client:
                resp = await client.get(
                    f"{self.external_url}/global/health",
                    headers=headers,
                )
                return resp.status_code == 200
        except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPError):
            return False

    async def _deploy_shared_skills(self) -> Path:
        """Deploy LHP skill files to a shared temp directory.

        Creates a directory structure under a temp path::

            <tmpdir>/skills/lhp/SKILL.md
            <tmpdir>/skills/lhp/references/*.md

        Files are copied verbatim from the bundled package — no managed
        headers.  The directory is made read-only to prevent OpenCode
        from creating ``package.json``/``node_modules`` inside it.

        Returns the path to the created temp directory.
        """
        from importlib import resources

        skill_pkg = "lhp.api.services.opencode_templates.skills.lhp"
        refs_pkg = f"{skill_pkg}.references"

        base = Path(tempfile.mkdtemp(prefix="lhp-opencode-"))
        skill_dir = base / "skills" / "lhp"
        refs_dir = skill_dir / "references"
        refs_dir.mkdir(parents=True)

        # Copy SKILL.md
        skill_content = (
            resources.files(skill_pkg).joinpath("SKILL.md").read_text("utf-8")
        )
        (skill_dir / "SKILL.md").write_text(skill_content)

        # Copy all reference .md files
        try:
            refs_source = resources.files(refs_pkg)
            for item in refs_source.iterdir():
                if item.name.endswith(".md") and not item.name.startswith("__"):
                    ref_content = item.read_text("utf-8")
                    (refs_dir / item.name).write_text(ref_content)
        except Exception:
            logger.warning(
                "Could not enumerate skill reference files", exc_info=True
            )

        # Make read-only to prevent OpenCode from writing into it
        for dirpath, dirnames, filenames in os.walk(base):
            for fname in filenames:
                os.chmod(os.path.join(dirpath, fname), 0o444)
            os.chmod(dirpath, 0o555)

        logger.info(f"Deployed shared skills to {base}")
        return base

    def _build_process_env(self) -> dict[str, str]:
        """Build the environment dict for OpenCode subprocesses.

        Sets ``OPENCODE_CONFIG_CONTENT`` (inline JSON config),
        ``OPENCODE_CONFIG_DIR`` (shared skills directory), and
        ``OPENCODE_DISABLE_PROJECT_CONFIG`` to prevent OpenCode from
        reading/writing config in the workspace.
        """
        env = os.environ.copy()

        # Add node_modules/.bin to PATH so the opencode binary (and any
        # Node helpers it needs) can be found regardless of subprocess CWD.
        node_bin_dir = Path("./node_modules/.bin").resolve()
        if node_bin_dir.is_dir():
            env["PATH"] = str(node_bin_dir) + os.pathsep + env.get("PATH", "")

        # When inside Databricks Apps (DATABRICKS_CLIENT_ID present),
        # fetch fresh OAuth token and derive ANTHROPIC_BASE_URL
        if "DATABRICKS_CLIENT_ID" in os.environ:
            token = _get_databricks_oauth_token()
            if token:
                env["ANTHROPIC_AUTH_TOKEN"] = token
                env["ANTHROPIC_API_KEY"] = "unused"
            if "ANTHROPIC_BASE_URL" not in env:
                dbx_host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
                if dbx_host:
                    if not dbx_host.startswith(("https://", "http://")):
                        dbx_host = f"https://{dbx_host}"
                    env["ANTHROPIC_BASE_URL"] = (
                        f"{dbx_host}/serving-endpoints/anthropic"
                    )

        env["OPENCODE_CONFIG_CONTENT"] = json.dumps(
            self._ai_config.to_opencode_json()
        )
        if self._shared_config_dir is not None:
            env["OPENCODE_CONFIG_DIR"] = str(self._shared_config_dir)
        env["OPENCODE_DISABLE_PROJECT_CONFIG"] = "1"
        return env
