"""OpenCode subprocess manager for AI assistant integration.

Manages the OpenCode server lifecycle — spawning it as a subprocess in
'subprocess mode', or connecting to an external instance in 'external mode'.
Provides health checking, auto-restart, and clean shutdown.

Production mode vs dev mode:
- **Dev mode**: project_root is known at startup → OpenCode subprocess
  starts with ``cwd=project_root``, config files deployed once there.
- **Production mode**: project_root is None at startup. When a session
  is requested, ``ensure_workspace()`` (re)starts the OpenCode subprocess
  with ``cwd=workspace_root`` so the AI operates on the correct files.
  Config files are deployed to the workspace before restarting.
"""

import asyncio
import json
import logging
import shutil
import signal
import tempfile
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# OpenCode health-check and restart constants
_HEALTH_INTERVAL_S = 5.0
_HEALTH_TIMEOUT_S = 3.0
_MAX_RESTART_ATTEMPTS = 3
_SHUTDOWN_GRACE_S = 5.0
_STARTUP_WAIT_S = 10.0
_STARTUP_POLL_S = 0.5


class OpenCodeManager:
    """Manages the OpenCode server for AI assistant features.

    Two modes of operation:
    - **Subprocess mode** (default): spawns ``opencode serve`` as a child
      process. Uses ``project_root`` as cwd if provided (dev mode), or
      defers startup until a workspace is known (production mode).
    - **External mode**: connects to a pre-existing OpenCode server at
      a user-supplied URL (set via ``LHP_OPENCODE_URL``).
    """

    def __init__(
        self,
        *,
        project_root: Optional[Path] = None,
        port: int = 4096,
        external_url: Optional[str] = None,
        password: Optional[str] = None,
        cors_origin: str = "http://localhost:5173",
    ) -> None:
        self._project_root = project_root
        self._port = port
        self._external_url = external_url
        self._password = password
        self._cors_origin = cors_origin

        # Runtime state
        self._process: Optional[asyncio.subprocess.Process] = None
        self._health_task: Optional[asyncio.Task] = None
        self._restart_count = 0
        self._available = False
        self._stopping = False
        self._current_cwd: Optional[str] = None  # Track which dir OpenCode runs in
        self._restarting = False  # Suppresses health monitor during workspace transitions

    # --- Public properties ---

    @property
    def available(self) -> bool:
        """Whether the OpenCode server is reachable and healthy."""
        return self._available

    @property
    def url(self) -> str:
        """Base URL of the OpenCode server."""
        if self._external_url:
            return self._external_url.rstrip("/")
        return f"http://localhost:{self._port}"

    @property
    def is_external(self) -> bool:
        return self._external_url is not None

    @property
    def auth_required(self) -> bool:
        return self._password is not None

    # --- Lifecycle ---

    async def start(self) -> None:
        """Start the OpenCode server (or verify external connectivity)."""
        if self.is_external:
            logger.info(f"AI assistant: connecting to external OpenCode at {self.url}")
            self._available = await self._health_check()
            if not self._available:
                logger.warning(
                    f"External OpenCode at {self.url} is not reachable — "
                    "AI features will be unavailable"
                )
        else:
            if self._project_root:
                # Dev mode: known project root → deploy config and start immediately
                await self._ensure_opencode_config(self._project_root)
                await self._spawn_process(str(self._project_root))
            else:
                # Production mode: defer spawning until a workspace is known.
                # Start with temp dir so /api/ai/status can report available=true
                # once the binary is verified.
                binary = shutil.which("opencode")
                if binary:
                    await self._spawn_process(tempfile.gettempdir())
                else:
                    logger.error(
                        "opencode binary not found on PATH. "
                        "Install with: brew install opencode  or  npm install -g opencode"
                    )

        # Start periodic health monitoring
        self._health_task = asyncio.create_task(self._health_monitor())

    async def stop(self) -> None:
        """Gracefully shut down the OpenCode server and health monitor."""
        self._stopping = True
        self._available = False

        # Cancel health monitor
        if self._health_task and not self._health_task.done():
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass

        # Stop subprocess (not applicable in external mode)
        await self._stop_process()

    # --- Workspace scoping (production mode) ---

    async def ensure_workspace(self, workspace_root: Path) -> None:
        """Ensure OpenCode is running with cwd=workspace_root.

        Called from the AI router before the frontend creates a session.
        In dev mode (project_root already set), this is a no-op.
        In production mode, this:
        1. Deploys config files (.opencode/, opencode.json) to the workspace
        2. Restarts the OpenCode subprocess with cwd=workspace_root if needed
        """
        logger.info(
            f"ensure_workspace called: workspace_root={workspace_root}, "
            f"is_external={self.is_external}, current_cwd={self._current_cwd}, "
            f"available={self._available}"
        )

        if self.is_external:
            # External mode: can't control cwd, just deploy config
            await self._ensure_opencode_config(workspace_root)
            return

        target_cwd = str(workspace_root)

        # Already running with the right directory?
        if self._current_cwd == target_cwd and self._available:
            logger.info("OpenCode already running in correct directory")
            return

        logger.info(f"AI workspace change: {self._current_cwd} → {target_cwd}")

        # Deploy config files into workspace
        await self._ensure_opencode_config(workspace_root)

        # Prevent health monitor from triggering _try_restart during transition
        self._restarting = True
        try:
            await self._stop_process()
            self._restart_count = 0
            await self._spawn_process(target_cwd)
        finally:
            self._restarting = False

    async def deploy_workspace_config(self, workspace_root: Path) -> None:
        """Deploy OpenCode config files into a user's workspace directory.

        Called from the AI router when creating a session in production mode.
        In dev mode, this is done once at startup via ``_ensure_opencode_config``.
        """
        await self._ensure_opencode_config(workspace_root)

    # --- Subprocess management ---

    async def _spawn_process(self, cwd: str) -> None:
        """Spawn the ``opencode serve`` subprocess with the given cwd."""
        binary = shutil.which("opencode")
        if not binary:
            logger.error(
                "opencode binary not found on PATH. "
                "Install with: brew install opencode  or  npm install -g opencode"
            )
            self._available = False
            return

        cmd = [
            binary,
            "serve",
            "--port",
            str(self._port),
        ]

        logger.info(f"Spawning OpenCode: {' '.join(cmd)} (cwd={cwd})")

        self._process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self._current_cwd = cwd

        # Wait for the server to become healthy
        self._available = await self._wait_for_healthy()
        if self._available:
            logger.info(f"OpenCode server ready at {self.url} (cwd={cwd})")
            self._restart_count = 0
        else:
            logger.error("OpenCode server failed to start within timeout")

    async def _stop_process(self) -> None:
        """Stop the current OpenCode subprocess if running."""
        if self._process and self._process.returncode is None:
            logger.info("Stopping OpenCode subprocess")
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
                pass  # Already exited
            logger.info("OpenCode subprocess stopped")
        self._process = None
        self._available = False

    async def _wait_for_healthy(self) -> bool:
        """Poll health endpoint until ready or timeout.

        Also checks whether the spawned process is still alive on each
        iteration.  If the process has already exited (e.g. port conflict),
        we return *False* immediately instead of polling for the full
        timeout against a potentially stale/orphaned endpoint.
        """
        elapsed = 0.0
        while elapsed < _STARTUP_WAIT_S:
            # Bail early if our process already exited (e.g. port conflict)
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

            if await self._health_check():
                return True
            await asyncio.sleep(_STARTUP_POLL_S)
            elapsed += _STARTUP_POLL_S
        return False

    async def _try_restart(self) -> None:
        """Attempt to restart a crashed subprocess."""
        if self._stopping or self.is_external or self._restarting:
            return

        self._restart_count += 1
        if self._restart_count > _MAX_RESTART_ATTEMPTS:
            logger.error(
                f"OpenCode crashed {_MAX_RESTART_ATTEMPTS} times — "
                "giving up, AI features disabled"
            )
            self._available = False
            return

        logger.warning(
            f"OpenCode crashed, restart attempt {self._restart_count}/{_MAX_RESTART_ATTEMPTS}"
        )
        # Clean up dead process
        if self._process:
            try:
                await self._process.wait()
            except Exception:
                pass

        # Restart with the same cwd
        cwd = self._current_cwd or tempfile.gettempdir()
        await self._spawn_process(cwd)

    # --- Health monitoring ---

    async def _health_check(self) -> bool:
        """Single health check against the OpenCode server."""
        try:
            async with httpx.AsyncClient(timeout=_HEALTH_TIMEOUT_S) as client:
                headers = {}
                if self._password:
                    headers["Authorization"] = f"Bearer {self._password}"
                resp = await client.get(
                    f"{self.url}/global/health", headers=headers
                )
                return resp.status_code == 200
        except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPError):
            return False

    async def _health_monitor(self) -> None:
        """Periodically check server health, restart if needed."""
        while not self._stopping:
            await asyncio.sleep(_HEALTH_INTERVAL_S)
            if self._stopping:
                break

            healthy = await self._health_check()
            if healthy:
                if not self._available:
                    logger.info("OpenCode server recovered")
                self._available = True
            else:
                if self._available:
                    logger.warning("OpenCode health check failed")
                self._available = False

                # For subprocess mode, check if process died and try restart
                if not self.is_external and self._process:
                    if self._process.returncode is not None:
                        await self._try_restart()

    # --- Config generation ---

    async def _ensure_opencode_config(self, target_root: Path) -> None:
        """Generate opencode.json and deploy agent/instruction files into target_root."""
        config_path = target_root / "opencode.json"

        config = {
            "$schema": "https://opencode.ai/config.json",
            "provider": {
                "anthropic": {},
            },
            "model": "anthropic/claude-sonnet-4-20250514",
        }

        # Only write if missing — don't overwrite user customizations
        if not config_path.exists():
            await asyncio.to_thread(
                config_path.write_text, json.dumps(config, indent=2) + "\n"
            )
            logger.info(f"Generated {config_path}")

        # Deploy bundled agent definition and instructions
        await self._deploy_templates(target_root)

    async def _deploy_templates(self, target_root: Path) -> None:
        """Copy bundled agent/instruction files into target_root/.opencode/ dir.

        Uses importlib.resources to locate the template files shipped with the
        lhp package. Files are only written if missing (preserves user edits).
        """
        from importlib import resources

        dest_base = target_root / ".opencode"

        template_pkg = "lhp.api.services.opencode_templates"
        mappings = [
            ("agents", "lhp-assistant.md"),
            ("instructions", "lhp-yaml-reference.md"),
        ]

        for subdir, filename in mappings:
            dest_dir = dest_base / subdir
            await asyncio.to_thread(dest_dir.mkdir, parents=True, exist_ok=True)
            dest_file = dest_dir / filename

            if dest_file.exists():
                continue  # Don't overwrite user customizations

            try:
                pkg = f"{template_pkg}.{subdir}"
                content = resources.files(pkg).joinpath(filename).read_text("utf-8")
                await asyncio.to_thread(dest_file.write_text, content)
                logger.info(f"Deployed {subdir}/{filename} to {target_root}")
            except Exception:
                logger.warning(f"Could not deploy {subdir}/{filename}", exc_info=True)
