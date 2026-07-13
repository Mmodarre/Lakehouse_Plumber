"""Persistent on-disk build-cache for the assembled dependency graph.

Cross-invocation companion to the in-process analysis memo in
:class:`~lhp.core.dependencies.service.DependencyAnalysisService`: the whole
:class:`~lhp.models.dependencies.DependencyAnalysisResult` for one
``(pipeline_filter, blueprint_filter, trust_depends_on)`` option triple is
pickled into a single shard under ``<project>/.lhp/cache/graph/`` so a fresh
process (or a restarted ``lhp web``) skips the discovery + parse + graph build
that dominates ``lhp dag`` wall time on a cache HIT.

Correctness (the load-bearing invariant): a HIT must NEVER serve a stale
graph. Dependency extraction reads external ``.py`` / ``.sql`` transform
bodies, not just YAML, so a shard records EVERY file the build depended on:

- a *structural* manifest of all project YAML (re-globbed and compared on
  ``load`` — catches added / removed / renamed / edited YAML), and
- a *body* manifest of every ``.py`` / ``.sql`` file the build read via the
  dependencies :class:`~lhp.core.dependencies._parse_cache.ParseCache`, each
  stored with ``(mtime_ns, size)`` — the same per-file discipline
  :class:`~lhp.parsers.parse_cache.PersistentParseCache` uses.

Both manifests use ``(mtime_ns, size)`` rather than a content hash: with
thousands of flowgroup YAML files a ``stat`` sweep is ~40x cheaper than
reading every byte, and a real edit always bumps ``mtime_ns`` (writes update
the mtime; git checkout does not preserve it), so it is as robust as the
existing parse cache for the edit-then-rerun workflow.

Any drift in either manifest, a version-tag mismatch, an unreadable/corrupt
shard, or an unwritable cache directory degrades to a rebuild and NEVER
raises into a caller.

Concurrency: one pickle shard per ``(version_tag, option_triple)`` slot,
written via temp-file + ``os.replace`` (atomic on POSIX and Windows). A stale
build overwrites its own slot; readers observe old-or-new content, never a
partial write.
"""

import hashlib
import logging
import os
import pickle
import tempfile
from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from ...models.dependencies import (
    AffectedAction,
    DependencyAnalysisResult,
    DependencyGraphs,
    DependencyWarning,
    PipelineDependency,
)
from ...utils.version import get_version

logger = logging.getLogger(__name__)

#: Bump MANUALLY on any change to the pickled payload shape / the dataclass
#: fields of :class:`DependencyAnalysisResult` and its nested models. The
#: model-field hash below is a best-effort guard, but this constant is the
#: authoritative invalidation mechanism.
CACHE_SCHEMA_VERSION = 1

#: Directory names never included in the structural YAML manifest: caches,
#: VCS metadata, and generated output (``generated/`` Python, ``resources/``
#: bundle YAML). Including generated output would bust the cache on every
#: ``lhp generate`` even when no source changed.
_EXCLUDED_DIR_NAMES = frozenset(
    {
        ".lhp",
        ".git",
        ".hg",
        ".svn",
        ".databricks",
        ".ruff_cache",
        ".mypy_cache",
        "__pycache__",
        "node_modules",
        "generated",
        "resources",
        "dist",
        "build",
    }
)

#: One (relpath-or-abspath) -> (mtime_ns, size) map. Structural manifests key
#: on project-relative POSIX paths; body manifests key on absolute paths.
Manifest = Dict[str, Tuple[int, int]]

#: (pipeline_filter, blueprint_filter, trust_depends_on) — mirrors the
#: in-process memo key in ``DependencyAnalysisService`` exactly.
OptionTriple = Tuple[Optional[str], Optional[str], bool]


def _model_schema_hash() -> str:
    """Best-effort hash of the result dataclass tree's field names.

    Invalidates shards when a field is added/removed/renamed even if the
    author forgets to bump :data:`CACHE_SCHEMA_VERSION`. Field *names* only
    (not types) keep this stable against harmless annotation reprs.
    """
    parts = []
    for cls in (
        DependencyAnalysisResult,
        DependencyGraphs,
        PipelineDependency,
        DependencyWarning,
        AffectedAction,
    ):
        if is_dataclass(cls):
            parts.append(f"{cls.__name__}:{','.join(f.name for f in fields(cls))}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]


def _compute_version_tag() -> str:
    """Version tag embedded in every shard slot and filename prefix.

    Combines the installed LHP version, :data:`CACHE_SCHEMA_VERSION`, and the
    model-field hash. The last two invalidate shards even when
    ``get_version()`` reads stale editable-install metadata.
    """
    return f"{get_version()}|{CACHE_SCHEMA_VERSION}|{_model_schema_hash()}"


class PersistentGraphCache:
    """One pickle shard per ``(version_tag, option_triple)``; never raises.

    A payload is a plain dict with keys ``key`` (the ``(version_tag,
    option_triple)`` slot identity), ``structural`` (the project-YAML
    manifest), ``bodies`` (the ``.py`` / ``.sql`` body manifest), and
    ``result`` (the pickled :class:`DependencyAnalysisResult`).
    """

    def __init__(self, cache_dir: Path, project_root: Path) -> None:
        self.cache_dir = cache_dir
        self.project_root = project_root
        self._version_tag = _compute_version_tag()
        self._version_prefix = hashlib.sha256(
            self._version_tag.encode("utf-8")
        ).hexdigest()[:16]
        self._enabled = True
        try:
            cache_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.debug(f"Persistent graph cache disabled ({cache_dir}): {e}")
            self._enabled = False

    def _shard_name(self, option_triple: OptionTriple) -> str:
        """Filename ``<version-prefix>_<triple-hash>.pkl`` for a slot.

        The version prefix lets :meth:`sweep` evict shards from other LHP
        versions by filename alone (no read).
        """
        triple_hash = hashlib.sha256(repr(option_triple).encode("utf-8")).hexdigest()[
            :32
        ]
        return f"{self._version_prefix}_{triple_hash}.pkl"

    def _structural_manifest(self) -> Manifest:
        """``{relpath: (mtime_ns, size)}`` for all project YAML.

        Walks ``project_root`` with in-place directory pruning so the ~2800
        parse-cache shards under ``.lhp/`` and generated output are never
        descended into. ``stat``-only — no file contents are read.

        Unlike the body manifest (captured at read time), this ``stat`` sweep
        runs at SAVE time, leaving a residual discovery->save window in which a
        YAML edited mid-build is stamped with its post-edit stat. This is the
        accepted parse-cache race model and is bounded by the (~seconds) build;
        snapshotting at discovery time would need invasive service plumbing for
        no material gain, so it is deliberately left as-is.
        """
        manifest: Manifest = {}
        root = self.project_root
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in _EXCLUDED_DIR_NAMES]
            for name in filenames:
                if not name.endswith((".yaml", ".yml")):
                    continue
                abs_path = Path(dirpath) / name
                try:
                    st = abs_path.stat()
                except OSError:
                    continue
                try:
                    rel = abs_path.relative_to(root).as_posix()
                except ValueError:
                    rel = str(abs_path)
                manifest[rel] = (st.st_mtime_ns, st.st_size)
        return manifest

    @staticmethod
    def _body_manifest(body_stats: Dict[Path, Tuple[int, int]]) -> Manifest:
        """``{abspath: (mtime_ns, size)}`` from the stats the build captured.

        Consumes the ``(mtime_ns, size)`` the ParseCache recorded when it
        actually READ each body -- it does NOT re-stat here. Re-statting at
        save time would capture any edit that landed during the (~seconds)
        build and stamp the shard with post-edit stats over a graph built from
        pre-edit bytes, so a later ``load`` would match those stats and serve a
        stale graph. Read-time capture makes the manifest attest to the exact
        bytes the build parsed, so such an edit drifts the manifest and MISSES.
        """
        return {str(path): stat for path, stat in body_stats.items()}

    @staticmethod
    def _bodies_drifted(bodies: Manifest) -> bool:
        """True if any recorded body file is missing or changed on disk."""
        for path_str, (mtime_ns, size) in bodies.items():
            try:
                st = Path(path_str).stat()
            except OSError:
                return True
            if st.st_mtime_ns != mtime_ns or st.st_size != size:
                return True
        return False

    def load(self, option_triple: OptionTriple) -> Optional[DependencyAnalysisResult]:
        """Return the cached result for ``option_triple`` or ``None``.

        ``None`` on any miss: absent/corrupt shard, version-tag mismatch,
        drifted structural manifest (YAML added/removed/renamed/edited), or a
        drifted/missing body file. Never raises.
        """
        if not self._enabled:
            return None
        shard = self.cache_dir / self._shard_name(option_triple)
        try:
            with open(shard, "rb") as f:
                # Trusted-local data: shards live in the project's own cache
                # dir and are written only by LHP; anything foreign fails the
                # key/type checks below and degrades to a miss.
                payload = pickle.load(f)  # nosec B301
            if (
                not isinstance(payload, dict)
                or payload.get("key") != (self._version_tag, option_triple)
                or not isinstance(payload.get("structural"), dict)
                or not isinstance(payload.get("bodies"), dict)
                or not isinstance(payload.get("result"), DependencyAnalysisResult)
            ):
                logger.debug(f"Graph cache stale/invalid for {option_triple}")
                return None
            if payload["structural"] != self._structural_manifest():
                logger.debug(f"Graph cache YAML drift for {option_triple}")
                return None
            if self._bodies_drifted(payload["bodies"]):
                logger.debug(f"Graph cache body drift for {option_triple}")
                return None
            result: DependencyAnalysisResult = payload["result"]
            return result
        except FileNotFoundError:
            return None
        except Exception as e:
            logger.debug(f"Graph cache read failed for {option_triple}: {e}")
            return None

    def save(
        self,
        option_triple: OptionTriple,
        result: DependencyAnalysisResult,
        body_stats: Dict[Path, Tuple[int, int]],
    ) -> None:
        """Atomically write the shard for ``option_triple``.

        ``body_stats`` is the ``{path: (mtime_ns, size)}`` the build recorded
        AT READ TIME (via the ParseCache); it is stored verbatim rather than
        re-statted here (see :meth:`_body_manifest`).

        The temp file is created inside ``cache_dir`` so ``os.replace`` stays
        on one volume. All I/O and pickling errors are swallowed with a debug
        log — a failed save just means the next run rebuilds.
        """
        if not self._enabled:
            return
        payload: Dict[str, Any] = {
            "key": (self._version_tag, option_triple),
            "structural": self._structural_manifest(),
            "bodies": self._body_manifest(body_stats),
            "result": result,
        }
        tmp_name: Optional[str] = None
        try:
            data = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
            with tempfile.NamedTemporaryFile(
                dir=self.cache_dir, suffix=".tmp", delete=False
            ) as tmp:
                # Capture the name before writing so the except-branch unlink
                # also covers a failed write, not just a failed replace.
                tmp_name = tmp.name
                tmp.write(data)
            os.replace(tmp_name, self.cache_dir / self._shard_name(option_triple))
        except Exception as e:
            logger.debug(f"Graph cache write failed for {option_triple}: {e}")
            if tmp_name is not None:
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass

    def describe(self, option_triple: OptionTriple) -> Tuple[Optional[str], str]:
        """Cheap ``(built_at, fingerprint)`` metadata for a shard slot.

        ``built_at`` is the shard file's mtime as an ISO-8601 UTC string, or
        ``None`` when no shard exists for ``option_triple`` (never built, or
        the cache is disabled). ``fingerprint`` is the cache version tag — the
        build identity (LHP version + schema version + model-field hash) that
        keys every slot; it lives in-memory and needs no read.

        O(1) by construction: one ``os.stat`` on the shard file, NEVER an
        unpickle of the (multi-MB) result nor a content re-hash — the staleness
        poll must stay cheap. Whether the served graph has DRIFTED from disk is
        answered by the webapp's watcher-set stale flag, not here.
        """
        if not self._enabled:
            return None, self._version_tag
        shard = self.cache_dir / self._shard_name(option_triple)
        try:
            st = shard.stat()
        except OSError:
            return None, self._version_tag
        built_at = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat()
        return built_at, self._version_tag

    def sweep(self) -> None:
        """Delete shards from other LHP versions, plus leftover temp files.

        A version bump changes the filename prefix, orphaning every prior
        shard; this evicts them by filename alone (no read). Errors are
        swallowed per entry so one locked/vanished file cannot abort the
        sweep.
        """
        if not self._enabled:
            return
        keep_prefix = f"{self._version_prefix}_"
        try:
            entries = list(self.cache_dir.iterdir())
        except OSError as e:
            logger.debug(f"Graph cache sweep skipped ({self.cache_dir}): {e}")
            return
        removed = 0
        for entry in entries:
            name = entry.name
            stale_shard = name.endswith(".pkl") and not name.startswith(keep_prefix)
            leftover_tmp = name.endswith(".tmp")
            if not (stale_shard or leftover_tmp):
                continue
            try:
                entry.unlink()
                removed += 1
            except OSError:
                continue
        if removed:
            logger.debug(f"Graph cache sweep removed {removed} stale file(s)")
