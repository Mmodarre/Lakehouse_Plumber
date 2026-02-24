import logging
import os
import shutil
import sys
from pathlib import Path

import click

logger = logging.getLogger(__name__)


class ServeCommand:
    """Start the LHP API server for local development."""

    def execute(
        self,
        port: int = 8000,
        host: str = "0.0.0.0",
        repo: str = ".",
        reload: bool = False,
        with_ai: bool = False,
    ) -> None:
        """Start the API server."""
        # Validate API dependencies are installed
        try:
            import fastapi  # noqa: F401
            import uvicorn
        except ImportError:
            click.echo(
                "Error: API dependencies not installed.\n"
                "Install with: pip install 'lakehouse-plumber[api]'",
                err=True,
            )
            sys.exit(1)

        # Resolve project root
        project_root = Path(repo).resolve()
        if not (project_root / "lhp.yaml").exists():
            click.echo(
                f"Error: No lhp.yaml found at {project_root}\n"
                "Use --repo to point to your LHP project directory.",
                err=True,
            )
            sys.exit(1)

        # Validate opencode binary if AI requested
        if with_ai and not shutil.which("opencode"):
            click.echo(
                "Error: opencode binary not found on PATH.\n"
                "Install with: brew install opencode  or  npm install -g opencode",
                err=True,
            )
            sys.exit(1)

        # Set environment for the API
        os.environ["LHP_PROJECT_ROOT"] = str(project_root)
        os.environ["LHP_DEV_MODE"] = "true"
        if with_ai:
            os.environ["LHP_AI_ENABLED"] = "true"

        click.echo("Starting LHP API server...")
        click.echo(f"  Project: {project_root}")
        click.echo(f"  URL:     http://{host}:{port}/api/docs")
        click.echo(f"  Mode:    Development (auth disabled)")
        if with_ai:
            opencode_port = int(os.environ.get("LHP_OPENCODE_PORT", "4096"))
            click.echo(f"  AI:      Enabled (OpenCode on port {opencode_port})")

        uvicorn.run(
            "lhp.api.app:create_app",
            host=host,
            port=port,
            reload=reload,
            factory=True,
        )
