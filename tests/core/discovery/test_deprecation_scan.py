"""Bare-``{token}`` deprecation scan on the core flowgroup-read path (C4).

Pins the C4 contract: scan EVERY file (no first-match early return), emit
EXACTLY ONE :class:`DeprecationWarningRecord` (code ``LHP-DEPR-001``, correct
``file``) per offending file, and reject every non-bare syntax
(``${...}`` / ``%{...}`` / ``{{...}}``).
"""

from pathlib import Path

import pytest

from lhp.core.discovery.deprecation_scanner import scan_bare_token_deprecations
from lhp.core.discovery.flowgroup_discoverer import FlowgroupDiscoveryService
from lhp.errors import codes

_DEPR_001 = "LHP-DEPR-001"


@pytest.mark.unit
def test_one_record_per_offending_file_across_many_files(tmp_path: Path) -> None:
    """N files each with a bare ``{token}`` yield N records (one per file)."""
    files = []
    for i in range(4):
        f = tmp_path / f"fg_{i}.yaml"
        f.write_text(f"source: {{legacy_token_{i}}}\n", encoding="utf-8")
        files.append(f)

    records = scan_bare_token_deprecations(files)

    assert len(records) == 4
    assert {r.file for r in records} == set(files)
    assert all(r.code == _DEPR_001 for r in records)
    assert all(r.code == codes.DEPR_001.code for r in records)
    assert all(r.flowgroup is None for r in records)
    assert all(
        "bare {token} substitution syntax is deprecated" in r.message for r in records
    )


@pytest.mark.unit
def test_multiple_bare_tokens_in_one_file_dedupe_to_one_record(
    tmp_path: Path,
) -> None:
    """Several bare tokens in a single file collapse to exactly ONE record."""
    f = tmp_path / "fg.yaml"
    f.write_text(
        "catalog: {one}\nschema: {two}\ntable: {three}\n",
        encoding="utf-8",
    )

    records = scan_bare_token_deprecations([f])

    assert len(records) == 1
    assert records[0].file == f
    assert records[0].code == _DEPR_001


@pytest.mark.unit
def test_dollar_local_var_and_jinja_syntax_yield_no_record(tmp_path: Path) -> None:
    """``${X}`` / ``%{X}`` / ``{{ X }}`` / ``{{X}}`` are all valid — never flagged."""
    f = tmp_path / "clean.yaml"
    f.write_text(
        "dollar: ${proper_token}\n"
        "local: %{local_var}\n"
        "jinja_spaced: {{ schema_file }}\n"
        "jinja_tight: {{schema_file}}\n",
        encoding="utf-8",
    )

    assert scan_bare_token_deprecations([f]) == ()


@pytest.mark.unit
def test_clean_file_with_no_braces_yields_no_record(tmp_path: Path) -> None:
    """A file with no brace syntax at all yields nothing."""
    f = tmp_path / "clean.yaml"
    f.write_text("source: plain_table\ntarget: other_table\n", encoding="utf-8")

    assert scan_bare_token_deprecations([f]) == ()


@pytest.mark.unit
def test_mixed_set_reports_only_offending_files(tmp_path: Path) -> None:
    """A mix of offending and clean files reports exactly the offenders."""
    bad_a = tmp_path / "bad_a.yaml"
    bad_a.write_text("source: {bare_a}\n", encoding="utf-8")
    clean = tmp_path / "clean.yaml"
    clean.write_text("source: ${ok}\nlocal: %{lv}\n", encoding="utf-8")
    bad_b = tmp_path / "bad_b.yaml"
    bad_b.write_text("a: {bare_b1}\nb: {bare_b2}\n", encoding="utf-8")

    records = scan_bare_token_deprecations([bad_a, clean, bad_b])

    assert {r.file for r in records} == {bad_a, bad_b}
    assert len(records) == 2


@pytest.mark.unit
def test_missing_or_unreadable_file_is_skipped(tmp_path: Path) -> None:
    """A path that does not exist is skipped, not surfaced as an error."""
    missing = tmp_path / "does_not_exist.yaml"

    assert scan_bare_token_deprecations([missing]) == ()


@pytest.mark.unit
def test_same_file_listed_twice_dedupes_by_resolved_path(tmp_path: Path) -> None:
    """The same file surfaced twice by the glob collapses to one record."""
    f = tmp_path / "fg.yaml"
    f.write_text("source: {bare}\n", encoding="utf-8")

    records = scan_bare_token_deprecations([f, f])

    assert len(records) == 1


@pytest.mark.unit
def test_empty_input_yields_empty_tuple() -> None:
    """No files in, no records out."""
    assert scan_bare_token_deprecations([]) == ()


def _write_fg(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


@pytest.mark.integration
def test_service_scans_all_pipeline_files_one_record_per_offender(
    tmp_path: Path,
) -> None:
    """The service globs ``pipelines/**`` and reports exactly one record per offending file."""
    pipelines = tmp_path / "pipelines"
    _write_fg(pipelines / "ingest" / "a.yaml", "source: {bare_a}\n")
    _write_fg(
        pipelines / "ingest" / "b.yaml",
        "catalog: {c}\nschema: {s}\n",  # multiple bare tokens -> ONE record
    )
    _write_fg(pipelines / "reference" / "c.yaml", "path: {bare_c}\n")
    _write_fg(
        pipelines / "reference" / "clean.yaml",
        "dollar: ${ok}\nlocal: %{lv}\njinja: {{ tmpl }}\n",
    )

    service = FlowgroupDiscoveryService(tmp_path)
    records = service.scan_deprecation_warnings()

    assert len(records) == 3
    assert {r.file.name for r in records} == {"a.yaml", "b.yaml", "c.yaml"}
    assert all(r.code == _DEPR_001 for r in records)
    assert all(r.flowgroup is None for r in records)


@pytest.mark.integration
def test_service_clean_project_yields_no_records(tmp_path: Path) -> None:
    """A project whose YAML uses only valid syntax yields no records."""
    pipelines = tmp_path / "pipelines"
    _write_fg(pipelines / "ingest" / "a.yaml", "source: ${ok}\nlocal: %{lv}\n")
    _write_fg(pipelines / "ingest" / "b.yaml", "jinja: {{ schema_file }}\n")

    service = FlowgroupDiscoveryService(tmp_path)

    assert service.scan_deprecation_warnings() == ()


@pytest.mark.integration
def test_service_no_pipelines_dir_yields_no_records(tmp_path: Path) -> None:
    """A project with no ``pipelines/`` directory yields no records (no crash)."""
    service = FlowgroupDiscoveryService(tmp_path)

    assert service.scan_deprecation_warnings() == ()
