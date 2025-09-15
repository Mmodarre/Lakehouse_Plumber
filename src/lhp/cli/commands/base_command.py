"""Base command class with shared utilities for LakehousePlumber CLI commands."""

import sys
import logging
from pathlib import Path
from typing import Optional
import click

logger = logging.getLogger(__name__)


class BaseCommand:
    """
    Base class for all CLI commands providing shared utilities.
    
    This class encapsulates common patterns used across CLI commands:
    - Project root validation and discovery
    - Error handling patterns
    - Logging setup coordination
    - Common validation checks
    """
    
    def __init__(self):
        """Initialize base command with shared state."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.verbose = False
        self.log_file = None
        self._project_root = None
    
    def setup_from_context(self) -> None:
        """Setup command state from Click context."""
        ctx = click.get_current_context()
        if ctx.obj:
            self.verbose = ctx.obj.get("verbose", False)
            self.log_file = ctx.obj.get("log_file", None)
    
    def ensure_project_root(self) -> Path:
        """
        Find and validate project root directory.
        
        Returns:
            Path to project root
            
        Raises:
            SystemExit: If not in a LakehousePlumber project
        """
        if self._project_root is None:
            self._project_root = self._find_project_root()
            
        if not self._project_root:
            click.echo("❌ Not in a LakehousePlumber project directory")
            click.echo("💡 Run 'lhp init <project_name>' to create a new project")
            click.echo("💡 Or navigate to an existing project directory")
            sys.exit(1)
            
        return self._project_root
    
    def _find_project_root(self) -> Optional[Path]:
        """
        Find the project root by looking for lhp.yaml.
        
        Searches current directory and parent directories for lhp.yaml file.
        
        Returns:
            Path to project root if found, None otherwise
        """
        current = Path.cwd().resolve()
        
        # Check current directory and parent directories  
        for path in [current] + list(current.parents):
            if (path / "lhp.yaml").exists():
                return path
        
        return None
    
    def check_substitution_file(self, env: str) -> Path:
        """
        Check that substitution file exists for the given environment.
        
        Args:
            env: Environment name
            
        Returns:
            Path to substitution file
            
        Raises:
            SystemExit: If substitution file doesn't exist
        """
        project_root = self.ensure_project_root()
        substitution_file = project_root / "substitutions" / f"{env}.yaml"
        
        if not substitution_file.exists():
            click.echo(f"❌ Substitution file not found: {substitution_file}")
            sys.exit(1)
            
        return substitution_file
    
    def echo_verbose_info(self, message: str) -> None:
        """Echo verbose information if verbose mode is enabled."""
        if self.verbose and self.log_file:
            click.echo(f"{message}")
            if "Detailed logs:" not in message:
                click.echo(f"Detailed logs: {self.log_file}")
    
    def handle_error(self, error: Exception, context: str, exit_code: int = 1) -> None:
        """
        Handle errors with consistent formatting and exit.
        
        Args:
            error: Exception that occurred
            context: Context description for the error
            exit_code: Exit code to use (default: 1)
        """
        if self.verbose:
            click.echo(f"{context}: {error}")
            if self.log_file:
                click.echo(f"See detailed logs: {self.log_file}")
        else:
            click.echo(f"{context}")
            
        sys.exit(exit_code)
