"""Tolerant project-identity reader over ``lhp.yaml`` and ``databricks.yml``.

Constitution §2.4 carve-out: this reader returns primitives (a hash, an
enum, counts), never raises, and is never a source of behaviour-affecting
configuration. It sits below ``lhp.parsers``, so it reads YAML with
``yaml.safe_load`` directly and validates nothing.

``bundle_present`` is a fact about the disk (a ``databricks.yml`` exists)
and is deliberately distinct from a command's ``bundle_enabled`` flag: this
package cannot import ``lhp.bundle.detection`` to ask the real question,
and ``--no-bundle`` disables support while the file still exists.
``env_class`` keys off the disk fact.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple

import yaml

logger = logging.getLogger(__name__)

# Public so the documented pseudonymity claim can be checked: the id sent is
# sha256(salt + normalised raw id)[:32], for every source alike.
PROJECT_ID_SALT = "lhp-project:"

_LHP_YAML = "lhp.yaml"
# ``.yml`` only, matching the bundle detector's own spelling rule.
_DATABRICKS_YML = "databricks.yml"
_SUBSTITUTIONS_DIR = "substitutions"
_SUBSTITUTION_SUFFIXES = (".yaml", ".yml")
_KNOWN_MODES = frozenset({"development", "production"})


@dataclass(frozen=True)
class ProjectIdentity:
    """What the envelope may say about a project: a hash and coarse facts.

    ``target_modes`` maps bundle target names to their ``mode`` and is used
    locally to derive ``env_class``; the names themselves are never sent.
    """

    project_id: Optional[str]
    source: str
    bundle_present: bool
    target_modes: Mapping[str, str]
    environment_count: int


def hash_project_id(raw: str) -> str:
    """Salted, normalised SHA-256 prefix of a raw project identifier."""
    normalised = raw.strip().lower()
    digest = hashlib.sha256(f"{PROJECT_ID_SALT}{normalised}".encode("utf-8"))
    return digest.hexdigest()[:32]


def _load_mapping(path: Path) -> Optional[Dict[str, Any]]:
    """The top-level mapping of a YAML file.

    ``None`` when the file is absent; ``{}`` when it exists but cannot be
    read as a mapping, so presence and content are reported separately.
    """
    try:
        if not path.is_file():
            return None
        document = yaml.safe_load(path.read_text("utf-8"))
    except (OSError, yaml.YAMLError, ValueError):  # unreadable means "no facts"
        logger.debug("Could not read a project file for telemetry", exc_info=True)
        return {}
    return document if isinstance(document, dict) else {}


def _non_empty_string(value: Any) -> Optional[str]:
    return value if isinstance(value, str) and value.strip() else None


def _pick_identity(
    project: Mapping[str, Any], bundle: Mapping[str, Any]
) -> Tuple[Optional[str], str]:
    """The raw identifier and its source, in the documented precedence."""
    project_id = _non_empty_string(project.get("project_id"))
    if project_id:
        return project_id, "lhp_yaml"
    block = bundle.get("bundle")
    uuid = _non_empty_string(block.get("uuid")) if isinstance(block, dict) else None
    if uuid:
        return uuid, "bundle_uuid"
    name = _non_empty_string(project.get("name"))
    if name:
        return name, "name_hash"
    return None, "none"


def _target_modes(bundle: Mapping[str, Any]) -> Dict[str, str]:
    targets = bundle.get("targets")
    if not isinstance(targets, dict):
        return {}
    return {
        str(name): str(target["mode"])
        for name, target in targets.items()
        if isinstance(target, dict) and target.get("mode") is not None
    }


def _environment_count(root: Path) -> int:
    folder = root / _SUBSTITUTIONS_DIR
    try:
        if not folder.is_dir():
            return 0
        return sum(
            1
            for entry in folder.iterdir()
            if entry.suffix in _SUBSTITUTION_SUFFIXES and entry.is_file()
        )
    except OSError:  # a folder that cannot be listed contributes nothing
        logger.debug("Could not list the substitutions folder", exc_info=True)
        return 0


def read_project_identity(project_root: Optional[Path]) -> ProjectIdentity:
    """Resolve the project's pseudonymous id and coarse facts; never raises.

    Resolution: ``lhp.yaml`` ``project_id``, then ``databricks.yml``
    ``bundle.uuid``, then ``lhp.yaml`` ``name``, else none. Every id is
    hashed the same way; ``source`` says which one was found.
    """
    if project_root is None:
        return ProjectIdentity(None, "none", False, {}, 0)
    project = _load_mapping(project_root / _LHP_YAML) or {}
    bundle = _load_mapping(project_root / _DATABRICKS_YML)
    raw, source = _pick_identity(project, bundle or {})
    return ProjectIdentity(
        project_id=None if raw is None else hash_project_id(raw),
        source=source,
        bundle_present=bundle is not None,
        target_modes=_target_modes(bundle or {}),
        environment_count=_environment_count(project_root),
    )


def env_class(project_root: Optional[Path], env: Optional[str]) -> str:
    """Classify the bundle target ``env`` without naming it.

    ``none`` when the project has no ``databricks.yml``; the target's
    ``mode`` when it is ``development`` or ``production``; ``unspecified``
    for a missing target, a missing mode or any other value.
    """
    identity = read_project_identity(project_root)
    if not identity.bundle_present:
        return "none"
    mode = identity.target_modes.get(env) if env else None
    return mode if mode in _KNOWN_MODES else "unspecified"
