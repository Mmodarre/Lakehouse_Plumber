"""Classify a project-relative path into the LHP file kind it represents.

This table is the CANONICAL one. ``web_app/src/lib/monaco-setup.ts`` mirrors
the eight kinds that also select a JSON schema in the editor — exactly the
entries of :data:`FRONTEND_MIRROR_GLOBS`. A glob changed here must be changed
there in the same commit; the parity tests on both sides compare the literal
glob strings and fail until it is.

Matching is structural rather than one ``fnmatch`` over the whole path,
because ``fnmatch``'s ``*`` also matches ``/``: ``presets/*.yaml`` would
wrongly accept ``presets/nested/x.yaml`` and ``pipelines/**/*.yaml`` would
wrongly reject ``pipelines/x.yaml``. Each rule therefore pins its leading
directory segments and whether intermediate directories are allowed, and
applies ``fnmatchcase`` to the file name alone — case-sensitively on every
platform, including Windows, where plain ``fnmatch`` would fold case.

Classification is a pure naming judgement: nothing here touches the disk, so
a path need not exist to be classified.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from fnmatch import fnmatchcase
from pathlib import PurePosixPath

from lhp.webapp.services.file_io import _normalize_relative


class FileKind(StrEnum):
    """What a project-relative path represents in an LHP project.

    A ``StrEnum`` so a member can be used directly wherever the bare kind
    string is expected, with no conversion step to forget.
    """

    FLOWGROUP = "flowgroup"
    PRESET = "preset"
    TEMPLATE = "template"
    SUBSTITUTION = "substitution"
    PROJECT_CONFIG = "project_config"
    PIPELINE_CONFIG = "pipeline_config"
    JOB_CONFIG = "job_config"
    BLUEPRINT = "blueprint"
    SCHEMA = "schema"
    SANDBOX_PROFILE = "sandbox_profile"
    SQL = "sql"
    PYTHON = "python"
    OTHER = "other"


@dataclass(frozen=True)
class _Rule:
    """One entry of the ordered classification table.

    ``directory`` is the exact leading path segments the file must sit under
    (empty = the project root). ``nested`` says whether further directory
    segments may appear between ``directory`` and the file: ``False`` means
    the file sits *directly* in ``directory``. ``name_globs`` are matched
    case-sensitively against the file name alone.
    """

    kind: FileKind
    directory: tuple[str, ...]
    nested: bool
    name_globs: tuple[str, ...]


# Both YAML spellings are accepted everywhere LHP reads YAML.
_YAML_NAMES: tuple[str, ...] = ("*.yaml", "*.yml")

# Evaluated top to bottom; the first match wins, so the narrow root-anchored
# rules precede the broad extension-only ones. ``schemas/`` deliberately
# claims every extension: a schema tree holds no flowgroups or scripts. The
# sandbox-profile rule carries the one exact filename the profile loader
# reads, so ``.lhp/profile.yml`` names nothing and stays ``other``.
_RULES: tuple[_Rule, ...] = (
    _Rule(FileKind.PROJECT_CONFIG, (), False, ("lhp.yaml", "lhp.yml")),
    _Rule(
        FileKind.PIPELINE_CONFIG,
        ("config",),
        False,
        ("pipeline_config*.yaml", "pipeline_config*.yml"),
    ),
    _Rule(
        FileKind.JOB_CONFIG,
        ("config",),
        False,
        (
            "job_config*.yaml",
            "job_config*.yml",
            "monitoring_job_config*.yaml",
            "monitoring_job_config*.yml",
        ),
    ),
    _Rule(FileKind.FLOWGROUP, ("pipelines",), True, _YAML_NAMES),
    _Rule(FileKind.PRESET, ("presets",), False, _YAML_NAMES),
    _Rule(FileKind.TEMPLATE, ("templates",), True, _YAML_NAMES),
    _Rule(FileKind.SUBSTITUTION, ("substitutions",), False, _YAML_NAMES),
    _Rule(FileKind.BLUEPRINT, ("blueprints",), True, _YAML_NAMES),
    _Rule(FileKind.SCHEMA, ("schemas",), True, ("*",)),
    _Rule(FileKind.SANDBOX_PROFILE, (".lhp",), False, ("profile.yaml",)),
    _Rule(FileKind.SQL, (), True, ("*.sql", "*.ddl")),
    _Rule(FileKind.PYTHON, (), True, ("*.py",)),
)

# The glob strings the frontend mirror uses for the eight kinds it also
# recognizes, kept as literals so a drift test can compare them verbatim
# against ``web_app/src/lib/monaco-setup.ts``. What is mirrored is the glob
# STRINGS, not the matcher: monaco hands these to the yaml-language-server,
# whose ``*`` crosses path separators, so a nested ``presets/`` file still
# gets the preset schema there while it classifies as ``other`` here. Two
# further deliberate narrowings on the frontend side: it has no entry for the
# kinds it cannot schema-validate (blueprint, sandbox_profile, sql, python),
# and its ``schema`` globs cover YAML only, whereas the rule above classifies
# anything under ``schemas/`` as a schema file.
FRONTEND_MIRROR_GLOBS: dict[FileKind, tuple[str, ...]] = {
    FileKind.FLOWGROUP: ("pipelines/**/*.yaml", "pipelines/**/*.yml"),
    FileKind.PRESET: ("presets/*.yaml", "presets/*.yml"),
    FileKind.TEMPLATE: ("templates/**/*.yaml", "templates/**/*.yml"),
    FileKind.SUBSTITUTION: ("substitutions/*.yaml", "substitutions/*.yml"),
    FileKind.PROJECT_CONFIG: ("lhp.yaml", "lhp.yml"),
    FileKind.PIPELINE_CONFIG: (
        "config/pipeline_config*.yaml",
        "config/pipeline_config*.yml",
    ),
    FileKind.JOB_CONFIG: (
        "config/job_config*.yaml",
        "config/job_config*.yml",
        "config/monitoring_job_config*.yaml",
        "config/monitoring_job_config*.yml",
    ),
    FileKind.SCHEMA: ("schemas/**/*.yaml", "schemas/**/*.yml"),
}


def _matches(rule: _Rule, parts: tuple[str, ...]) -> bool:
    """Return ``True`` when ``parts`` (a split POSIX path) satisfies ``rule``."""
    directories = parts[:-1]
    if rule.nested:
        if directories[: len(rule.directory)] != rule.directory:
            return False
    elif directories != rule.directory:
        return False
    return any(fnmatchcase(parts[-1], glob) for glob in rule.name_globs)


def classify_path(relative_path: str) -> FileKind:
    """Return the :class:`FileKind` a project-relative path represents.

    ``relative_path`` is normalized the same way every other file-I/O prefix
    check normalizes it (Windows separators, duplicate slashes, leading ``/``
    and ``./`` segments), then matched against the ordered rule table.
    Anything unrecognized — an empty path or a bare directory included — is
    :attr:`FileKind.OTHER`, so this never raises.
    """
    parts = PurePosixPath(_normalize_relative(relative_path)).parts
    if not parts:
        return FileKind.OTHER
    for rule in _RULES:
        if _matches(rule, parts):
            return rule.kind
    return FileKind.OTHER
