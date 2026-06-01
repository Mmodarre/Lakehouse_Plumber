"""Logging configuration for the LHP CLI."""

import logging
import sys
from pathlib import Path
from typing import Optional


def configure_logging(
    verbose: bool, project_root: Optional[Path] = None, log_to_file: bool = False
):
    """Configure logging for LakehousePlumber.

    A DEBUG log file is written only when ``log_to_file`` is true and a project
    root is found. Returns the log file path, or None when no file is written.
    """
    cleanup_logging()

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if (verbose or log_to_file) else logging.INFO)

    # Diagnostic logs go to stderr so they never collide with primary
    # command output on stdout. See STYLE.md section 2.
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging.WARNING if not verbose else logging.DEBUG)
    console_formatter = logging.Formatter("%(levelname)s: %(message)s")
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    log_file_path = None
    if log_to_file and project_root:
        log_dir = project_root / ".lhp" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file_path = log_dir / "lhp.log"

        file_handler = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    return str(log_file_path) if log_file_path else None


def cleanup_logging():
    """Clean up logging handlers."""
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        handler.close()
        root_logger.removeHandler(handler)
