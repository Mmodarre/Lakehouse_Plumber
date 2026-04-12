"""Per-run file checksum cache for LakehousePlumber.

Each file is read and hashed at most once per generation run.
Thread-safe for use during parallel flowgroup generation.
"""

import hashlib
import logging
import threading
from pathlib import Path

logger = logging.getLogger(__name__)


class ChecksumCache:
    """Per-run file checksum cache.

    Each file is read and hashed at most once. Thread-safe via threading.Lock
    for use during parallel flowgroup generation.
    """

    def __init__(self):
        self._cache: dict[str, str] = {}  # resolved_path_str -> hex_digest
        self._lock = threading.Lock()

    def get(self, file_path: Path) -> str:
        """Get or compute the SHA256 checksum for a file.

        Args:
            file_path: Path to the file (absolute or relative).

        Returns:
            SHA256 hexdigest string, or empty string if computation fails.
        """
        key = str(file_path.resolve())
        with self._lock:
            if key in self._cache:
                return self._cache[key]
        # Compute outside lock to avoid holding it during I/O
        digest = self._compute(file_path)
        with self._lock:
            # Another thread may have computed it; use first result
            if key not in self._cache:
                self._cache[key] = digest
            return self._cache[key]

    def _compute(self, file_path: Path) -> str:
        """Compute SHA256 checksum for a file.

        Args:
            file_path: Path to the file.

        Returns:
            SHA256 hexdigest string, or empty string on failure.
        """
        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
            return sha256_hash.hexdigest()
        except Exception as e:
            logger.warning(f"Failed to calculate checksum for {file_path}: {e}")
            return ""

    @property
    def size(self) -> int:
        """Number of cached entries."""
        with self._lock:
            return len(self._cache)
