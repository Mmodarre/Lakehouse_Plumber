"""Pre-pool YAML deprecation-scan invariants for the shared helper.

Both ``lhp generate`` and ``lhp validate`` invoke the same pre-pool YAML
scan helper (:func:`lhp.cli.yaml_scanner.emit_deprecation_warning_if_needed`).
Workers are silenced via a ``NullHandler`` (see Phase 3) so the bare-
``{token}`` deprecation warning that ``SubstitutionManager`` normally
produces would never reach the user. The pre-pool main-thread scan
compensates by recording the warning on a per-run
:class:`WarningCollector` when any discovered YAML uses the bare
``{token}`` syntax.

These tests pin the contract for the shared helper. The legacy
module-level ``_DEPRECATED_BARE_TOKEN_WARNED`` flag has been deleted —
dedup is handled by the collector itself via ``(category, message)``.
"""

from pathlib import Path

import pytest

from lhp.cli.warning_collector import WarningCollector
from lhp.cli.yaml_scanner import emit_deprecation_warning_if_needed


def test_pre_pool_scan_records_warning_once_when_bare_token_present(
    tmp_path: Path,
) -> None:
    """A bare-``{token}`` YAML records exactly one deprecation entry."""
    yaml_a = tmp_path / "fg_a.yaml"
    yaml_a.write_text("source: {legacy_token}\n", encoding="utf-8")
    yaml_b = tmp_path / "fg_b.yaml"
    yaml_b.write_text("source: ${proper_token}\n", encoding="utf-8")

    collector = WarningCollector()
    emit_deprecation_warning_if_needed(collector, [yaml_a, yaml_b])
    # Second call must remain a no-op because the collector dedups by
    # ``(category, message)``.
    emit_deprecation_warning_if_needed(collector, [yaml_a])

    assert collector.count == 1


def test_pre_pool_scan_records_nothing_when_no_bare_token(tmp_path: Path) -> None:
    """Clean YAMLs (only ``${token}`` syntax) record zero warnings."""
    yaml_a = tmp_path / "fg_a.yaml"
    yaml_a.write_text("source: ${proper_token}\n", encoding="utf-8")

    collector = WarningCollector()
    emit_deprecation_warning_if_needed(collector, [yaml_a])

    assert collector.count == 0


def test_pre_pool_scan_ignores_missing_files(tmp_path: Path) -> None:
    """Missing file paths are silently skipped, not surfaced as errors."""
    missing = tmp_path / "does_not_exist.yaml"

    collector = WarningCollector()
    emit_deprecation_warning_if_needed(collector, [missing])

    assert collector.count == 0


def test_pre_pool_scan_skips_none_paths() -> None:
    """``None`` entries in the iterable are skipped defensively."""
    collector = WarningCollector()
    emit_deprecation_warning_if_needed(collector, [None])

    assert collector.count == 0


def test_pre_pool_scan_ignores_dollar_token_syntax(tmp_path: Path) -> None:
    """``${X}`` is the supported syntax; its ``{X}`` suffix must not match."""
    yaml_a = tmp_path / "fg_a.yaml"
    yaml_a.write_text(
        "source: ${proper_token}\ntarget: ${another_token}\n", encoding="utf-8"
    )

    collector = WarningCollector()
    emit_deprecation_warning_if_needed(collector, [yaml_a])

    assert collector.count == 0


@pytest.mark.parametrize(
    "yaml_body",
    [
        "source: {{ schema_file }}\n",
        "source: {{schema_file}}\n",
    ],
)
def test_pre_pool_scan_ignores_jinja2_template(yaml_body: str, tmp_path: Path) -> None:
    """``{{ X }}`` / ``{{X}}`` is Jinja2 — the lookahead must reject both forms."""
    yaml_a = tmp_path / "fg_a.yaml"
    yaml_a.write_text(yaml_body, encoding="utf-8")

    collector = WarningCollector()
    emit_deprecation_warning_if_needed(collector, [yaml_a])

    assert collector.count == 0


def test_pre_pool_scan_ignores_local_var_syntax(tmp_path: Path) -> None:
    """``%{X}`` is the local-variable syntax documented in CLAUDE.md; must not match."""
    yaml_a = tmp_path / "fg_a.yaml"
    yaml_a.write_text("source: %{local_var}\n", encoding="utf-8")

    collector = WarningCollector()
    emit_deprecation_warning_if_needed(collector, [yaml_a])

    assert collector.count == 0
