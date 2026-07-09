"""Persistent on-disk parse cache for pipeline YAML files.

Cross-invocation companion to the in-memory ``CachingYAMLParser`` caches:
each source file gets one pickle shard under ``<project>/.lhp/cache/parse/``
keyed by (resolved path, mtime_ns, size, version tag). The cache is
delete-safe at any time; every failure path — unwritable directory, corrupt
shard, key mismatch, pickle errors — degrades to a fresh parse and never
raises into a caller.

Concurrency model (deliberate — no locking, no SQLite, no index file):
per-source-file shards written via temp-file + ``os.replace`` (atomic on
POSIX and Windows) mean concurrent writers (a long-lived ``lhp web`` process
and short-lived CLI runs) exchange only complete shards, last-write-wins;
readers observe old-or-new content, never a partial write.
"""

import hashlib
import json
import logging
import os
import pickle
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from lhp.models import FlowGroup

from ..utils.version import get_version

logger = logging.getLogger(__name__)

#: Bump on any shard payload-format or parse-semantics change.
CACHE_SCHEMA_VERSION = 1


def _compute_version_tag() -> str:
    """Version tag embedded in every shard key.

    Combines the installed LHP version, the cache schema version, and a hash
    of the :class:`FlowGroup` JSON schema. The model-shape hash invalidates
    shards on pydantic model changes even when ``get_version()`` reads stale
    editable-install metadata.
    """
    schema_hash = hashlib.sha256(
        json.dumps(FlowGroup.model_json_schema(), sort_keys=True).encode()
    ).hexdigest()[:16]
    return f"{get_version()}|{CACHE_SCHEMA_VERSION}|{schema_hash}"


class PersistentParseCache:
    """One pickle shard per source file; atomic replace; never raises.

    A payload is a plain dict ``{"key": ..., "documents": ..., "flowgroups":
    ...}`` where ``flowgroups`` is either a ``list[FlowGroup]`` or ``None``
    for a doc-only payload.
    """

    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        self._version_tag = _compute_version_tag()
        self._enabled = True
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.debug(f"Persistent parse cache disabled ({cache_dir}): {e}")
            self._enabled = False

    @staticmethod
    def _shard_name(resolved_path: Path) -> str:
        """Windows-safe shard filename derived from the resolved source path."""
        digest = hashlib.sha256(str(resolved_path).encode("utf-8")).hexdigest()
        return digest[:32] + ".pkl"

    def _key(
        self, resolved_path: Path, mtime_ns: int, size: int
    ) -> Tuple[str, int, int, str]:
        return (str(resolved_path), mtime_ns, size, self._version_tag)

    def load(
        self, resolved_path: Path, mtime_ns: int, size: int
    ) -> Optional[Dict[str, Any]]:
        """Return the shard payload for ``resolved_path`` or ``None``.

        ``None`` on any miss: absent shard, unreadable/corrupt pickle, or an
        embedded key that does not match (file changed, LHP upgraded, model
        shape changed). Never raises.
        """
        if not self._enabled:
            return None
        shard = self.cache_dir / self._shard_name(resolved_path)
        try:
            with open(shard, "rb") as f:
                payload = pickle.load(f)
            if (
                not isinstance(payload, dict)
                or payload.get("key") != self._key(resolved_path, mtime_ns, size)
                or "documents" not in payload
                or "flowgroups" not in payload
            ):
                logger.debug(f"Parse cache stale/invalid for {resolved_path}")
                return None
            return payload
        except FileNotFoundError:
            return None
        except Exception as e:
            logger.debug(f"Parse cache read failed for {resolved_path}: {e}")
            return None

    def save(
        self,
        resolved_path: Path,
        mtime_ns: int,
        size: int,
        documents: List[Dict[str, Any]],
        flowgroups: Optional[List[FlowGroup]],
    ) -> None:
        """Atomically write the shard for ``resolved_path``.

        The temp file is created inside ``cache_dir`` so ``os.replace`` stays
        on one volume. All I/O errors are swallowed with a debug log.
        """
        if not self._enabled:
            return
        payload = {
            "key": self._key(resolved_path, mtime_ns, size),
            "documents": documents,
            "flowgroups": flowgroups,
        }
        tmp_name: Optional[str] = None
        try:
            data = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
            with tempfile.NamedTemporaryFile(
                dir=self.cache_dir, suffix=".tmp", delete=False
            ) as tmp:
                tmp.write(data)
                tmp_name = tmp.name
            os.replace(tmp_name, self.cache_dir / self._shard_name(resolved_path))
        except Exception as e:
            logger.debug(f"Parse cache write failed for {resolved_path}: {e}")
            if tmp_name is not None:
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass

    def sweep(self, live_paths: Iterable[Path]) -> None:
        """Delete shards for files not in ``live_paths``, plus leftover temp files.

        ``live_paths`` must be resolved paths (same form the shard names were
        derived from). Errors are swallowed per entry so a single locked or
        vanished file cannot abort the sweep.
        """
        if not self._enabled:
            return
        live_names = {self._shard_name(path) for path in live_paths}
        try:
            entries = list(self.cache_dir.iterdir())
        except OSError as e:
            logger.debug(f"Parse cache sweep skipped ({self.cache_dir}): {e}")
            return
        removed = 0
        for entry in entries:
            name = entry.name
            orphan_shard = name.endswith(".pkl") and name not in live_names
            leftover_tmp = name.endswith(".tmp")
            if not (orphan_shard or leftover_tmp):
                continue
            try:
                entry.unlink()
                removed += 1
            except OSError:
                continue
        if removed:
            logger.debug(f"Parse cache sweep removed {removed} stale file(s)")
