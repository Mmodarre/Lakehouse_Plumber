"""Common utility for loading external files with consistent path resolution."""

from pathlib import Path
from typing import List, Optional, Union

from lhp.errors import ErrorFactory, codes


def is_file_path(value: str) -> bool:
    """Detect if a string is a file path vs inline content.

    Used primarily by cloudFiles.schemaHints which accepts both inline DDL and
    file paths in a single parameter (matching Databricks API). Detects by file
    extension (.yaml, .yml, .json, .ddl, .sql) or path separator (/ or \\).
    """
    if not value:
        return False

    value_lower = value.lower()

    file_extensions = [".yaml", ".yml", ".json", ".ddl", ".sql"]
    if any(ext in value_lower for ext in file_extensions):
        return True

    if "/" in value or "\\" in value:
        return True

    return False


def resolve_external_file_path(
    file_path: Union[str, Path],
    base_dir: Path,
    file_type: str = "file",
    search_additional: Optional[List[Path]] = None,
) -> Path:
    """Resolve external file path; raises LHPError with search locations when not found."""
    file_path = Path(file_path)

    if file_path.is_absolute():
        if file_path.exists():
            return file_path
        search_locations = [f"Absolute path: {file_path}"]
        raise ErrorFactory.file_not_found(str(file_path), search_locations, file_type)

    resolved_path = base_dir / file_path

    if resolved_path.exists():
        return resolved_path

    search_locations = [f"Relative to project root: {resolved_path}"]

    if search_additional:
        for additional_dir in search_additional:
            additional_path = additional_dir / file_path
            search_locations.append(f"Additional location: {additional_path}")

    raise ErrorFactory.file_not_found(str(file_path), search_locations, file_type)


def load_external_file_text(
    file_path: Union[str, Path],
    base_dir: Path,
    file_type: str = "file",
    encoding: str = "utf-8",
) -> str:
    """Load external file as text; combines path resolution and text loading."""
    resolved_path = resolve_external_file_path(file_path, base_dir, file_type)

    try:
        return resolved_path.read_text(encoding=encoding)
    except UnicodeDecodeError as e:
        raise ErrorFactory.io_error(
            codes.IO_004,
            title=f"File encoding error: {file_type}",
            details=f"Could not read '{resolved_path}' as {encoding} text: {e}",
            suggestions=[
                f"Ensure the file is saved with {encoding} encoding",
                "Check that the file is a text file, not a binary file",
            ],
            context={"File": str(resolved_path), "Encoding": encoding},
        ) from e
    except PermissionError as e:
        raise ErrorFactory.io_error(
            codes.IO_005,
            title=f"Permission denied: {file_type}",
            details=f"Cannot read '{resolved_path}': permission denied.",
            suggestions=[
                "Check file permissions",
                f"Ensure the file is readable: {resolved_path}",
            ],
            context={"File": str(resolved_path)},
        ) from e
