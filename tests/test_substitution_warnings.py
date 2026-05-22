"""Unit tests for the per-instance deprecation flag on the substitution manager.

The legacy module-level ``_DEPRECATED_BARE_TOKEN_WARNED`` flag (and the
``logger.warning`` call that flipped it) was removed in Phase C of the
LHP CLI UX Hardening Phase 2 plan. Per-instance ``has_deprecated_bare_tokens``
is the replacement; downstream callers query it after construction and
forward a single entry to the per-run :class:`WarningCollector`.
"""

import logging

import yaml

from lhp.utils.substitution import EnhancedSubstitutionManager


def _write_substitution_file(path, mapping):
    """Write a minimal env-keyed substitutions YAML and return the path."""
    path.write_text(yaml.safe_dump({"dev": mapping}), encoding="utf-8")
    return path


def test_substitution_manager_sets_bool_flag_on_bare_tokens(tmp_path) -> None:
    """Encountering the bare ``{token}`` syntax flips the per-instance flag."""
    sub_file = _write_substitution_file(
        tmp_path / "dev.yaml",
        {
            "catalog": "main",
            # A value that itself references another mapping using the
            # deprecated bare-{token} syntax. The recursive-expand pass
            # in __init__ runs _replace_tokens_in_string over each
            # mapping value, which is the only place the per-instance
            # flag is flipped.
            "table_path": "/Volumes/{catalog}/legacy/data",
        },
    )

    manager = EnhancedSubstitutionManager(substitution_file=sub_file, env="dev")
    assert manager.has_deprecated_bare_tokens is True


def test_substitution_manager_flag_false_when_clean(tmp_path) -> None:
    """A substitution file using only ``${token}`` keeps the flag at ``False``."""
    sub_file = _write_substitution_file(
        tmp_path / "dev.yaml",
        {
            "catalog": "main",
            "table_path": "/Volumes/${catalog}/data",
        },
    )

    manager = EnhancedSubstitutionManager(substitution_file=sub_file, env="dev")
    assert manager.has_deprecated_bare_tokens is False


def test_substitution_manager_flag_starts_false_with_no_file(tmp_path) -> None:
    """A bare manager with no substitution file starts with the flag unset."""
    manager = EnhancedSubstitutionManager(substitution_file=None, env="dev")
    assert manager.has_deprecated_bare_tokens is False


def test_no_logger_warning_emitted_from_substitution(tmp_path, caplog) -> None:
    """``substitution.py`` no longer emits ``logger.warning`` for bare tokens.

    The runtime ``logger.warning(...)`` was silenced inside worker
    processes (NullHandler-only); the orchestrator now consults the
    per-instance bool flag and forwards a single warning to the
    per-run :class:`WarningCollector`. There must be no remaining
    ``logger.warning`` emission from ``lhp.utils.substitution`` for
    deprecation.
    """
    sub_file = _write_substitution_file(
        tmp_path / "dev.yaml",
        {
            "catalog": "main",
            "table_path": "/Volumes/{catalog}/legacy/data",
        },
    )

    with caplog.at_level(logging.WARNING, logger="lhp.utils.substitution"):
        manager = EnhancedSubstitutionManager(substitution_file=sub_file, env="dev")
        # Trigger an additional substitution pass to ensure no warning
        # emits when the manager's user-facing methods are called.
        manager.substitute_yaml({"path": "/Volumes/{catalog}/legacy/data"})

    deprecation_records = [
        r
        for r in caplog.records
        if r.name == "lhp.utils.substitution" and "deprecat" in r.message.lower()
    ]
    assert deprecation_records == []
    # And the per-instance flag is set, which is the supported signal.
    assert manager.has_deprecated_bare_tokens is True
