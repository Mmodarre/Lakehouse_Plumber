import logging
import os
import sys
from pathlib import Path
from typing import Optional

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

        # Set environment for the API
        os.environ["LHP_PROJECT_ROOT"] = str(project_root)
        os.environ["LHP_DEV_MODE"] = "true"

        click.echo(f"Starting LHP API server...")
        click.echo(f"  Project: {project_root}")
        click.echo(f"  URL:     http://{host}:{port}/api/docs")
        click.echo(f"  Mode:    Development (auth disabled)")

        uvicorn.run(
            "lhp.api.app:create_app",
            host=host,
            port=port,
            reload=reload,
            factory=True,
        )
