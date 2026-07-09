"""Shared YAML loading utilities.

Uses PyYAML only — does NOT interfere with ruamel.yaml usage for
databricks.yml structure preservation where needed.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

from ..errors import ErrorFactory, LHPError, MultiDocumentError, codes

logger = logging.getLogger(__name__)

SAFE_LOADER = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
if SAFE_LOADER is yaml.SafeLoader:
    logger.debug("libyaml CSafeLoader unavailable; using pure-Python SafeLoader")


def load_yaml_file(
    file_path: Union[Path, str],
    allow_empty: bool = True,
    error_context: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Single-document YAML loading; raises MultiDocumentError (LHP-IO-003) if the
    file contains 0 or more than 1 document. Use load_yaml_documents_all() for
    multi-document files.
    """
    file_path = Path(file_path)
    logger.debug(f"Loading YAML file: {file_path}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            documents = list(yaml.load_all(f, Loader=SAFE_LOADER))

        if len(documents) != 1:
            raise MultiDocumentError(file_path, len(documents), error_context)

        content = documents[0]

        if content is None:
            return {} if allow_empty else None

        return content

    except yaml.YAMLError as e:
        raise ErrorFactory.yaml_parse_error(
            file_path=str(file_path),
            error_message=str(e),
            context=error_context,
        ) from e
    except FileNotFoundError as e:
        if isinstance(e, LHPError):
            raise
        raise ErrorFactory.file_not_found(
            file_path=str(file_path),
            search_locations=[str(file_path.parent)],
            file_type="YAML file",
        ) from e
    except Exception as e:
        if isinstance(e, LHPError):
            raise

        context = error_context or f"file {file_path}"
        raise ErrorFactory.io_error(
            codes.IO_002,
            title="Error reading YAML file",
            details=f"Error reading {context}: {e}",
            suggestions=[
                "Check the file exists and is readable",
                "Verify the file encoding is UTF-8",
                "Ensure the file is valid YAML",
            ],
            context={"file": str(file_path)},
        ) from e


def safe_load_yaml_with_fallback(
    file_path: Union[Path, str],
    fallback_value: Dict[str, Any] | None = None,
    error_context: Optional[str] = None,
    log_errors: bool = True,
) -> Dict[str, Any]:
    """Load YAML; return fallback_value (default ``{}``) on any error."""
    if fallback_value is None:
        fallback_value = {}

    try:
        return load_yaml_file(file_path, error_context=error_context)
    except Exception as e:
        if log_errors:
            context = error_context or f"YAML file {file_path}"
            logger.warning(f"Could not load {context}: {e}")
        return fallback_value


def load_yaml_documents_all(
    file_path: Union[Path, str], error_context: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Load all YAML documents from a file; None/empty documents are filtered out."""
    file_path = Path(file_path)
    logger.debug(f"Loading all YAML documents from: {file_path}")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            documents = list(yaml.load_all(f, Loader=SAFE_LOADER))

        documents = [doc for doc in documents if doc is not None]

        logger.debug(f"Loaded {len(documents)} document(s) from {file_path}")

        return documents

    except yaml.YAMLError as e:
        raise ErrorFactory.yaml_parse_error(
            file_path=str(file_path),
            error_message=str(e),
            context=error_context,
        ) from e
    except FileNotFoundError as e:
        if isinstance(e, LHPError):
            raise
        raise ErrorFactory.file_not_found(
            file_path=str(file_path),
            search_locations=[str(file_path.parent)],
            file_type="YAML file",
        ) from e
    except Exception as e:
        if isinstance(e, LHPError):
            raise

        context = error_context or f"file {file_path}"
        raise ErrorFactory.io_error(
            codes.IO_002,
            title="Error reading YAML file",
            details=f"Error reading {context}: {e}",
            suggestions=[
                "Check the file exists and is readable",
                "Verify the file encoding is UTF-8",
                "Ensure the file is valid YAML",
            ],
            context={"file": str(file_path)},
        ) from e
