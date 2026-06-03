"""Deprecation warnings surface as ``WarningEmitted`` in the facade streams.

Two deprecation-warning sources feed the §5.7 event stream:

* the MAIN-THREAD bare-``{token}`` scan
  (``orchestrator.discovery.scan_deprecation_warnings()`` → ``LHP-DEPR-001``),
  emitted in/after the ``discover`` phase, and
* the WORKER warnings merged off the engine's per-pipeline results
  (the ``database`` field → ``LHP-DEPR-002``; the schema-transform
  ``enforcement`` key → ``LHP-DEPR-003``; the preset ``database_suffix`` key →
  ``LHP-DEPR-004``), emitted in/after the ``generate`` / ``validate`` phase.

Both are merged + deduped by ``(code, file)`` into ONE ordered sequence the
facade re-emits. These tests pin:

1. The worker ``database`` deprecation SURFACES via the validate stream as a
   ``WarningEmitted`` with ``code='LHP-DEPR-002'`` and the offending file.
2. The ``database_suffix`` preset deprecation surfaces (``LHP-DEPR-004``).
3. The main-thread bare-``{token}`` deprecation surfaces (``LHP-DEPR-001``),
   carrying the source file and no flowgroup.
4. Dedup: the same ``(code, file)`` produced by two flowgroups collapses to ONE
   ``WarningEmitted`` (tested directly on the shared emission helper, which both
   the generate and validate streams use, for all four codes + deterministic
   order).
5. The §13.4 locked field-order call ``WarningEmitted(rec.message,
   code=rec.code, ...)`` type-checks and constructs.

Tests import strictly from :mod:`lhp.api` for the facade-driven cases; the
emission-helper unit test reaches the underscore-prefixed
``lhp.api._converters_common`` (an internal seam, exercised here in-process).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from lhp.api import (
    LakehousePlumberApplicationFacade,
    ValidationCompleted,
    WarningEmitted,
    collect_response,
)


def _base_project(root: Path) -> None:
    for sub in ("presets", "templates", "substitutions"):
        (root / sub).mkdir(parents=True, exist_ok=True)
    (root / "lhp.yaml").write_text("name: warning_stream\nversion: '1.0'\n")
    with open(root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(
            {
                "dev": {
                    "catalog": "dev_catalog",
                    "bronze_schema": "bronze",
                    "landing_path": "/mnt/dev/landing",
                }
            },
            f,
        )


def _write_flowgroup(root: Path, pipeline: str, fg: dict, *, name: str) -> Path:
    pdir = root / "pipelines" / pipeline
    pdir.mkdir(parents=True, exist_ok=True)
    path = pdir / f"{name}.yaml"
    with open(path, "w") as f:
        yaml.dump(fg, f)
    return path


@pytest.fixture
def facade_in(tmp_path: Path):
    created: list[str] = []

    def _build() -> LakehousePlumberApplicationFacade:
        created.append(os.getcwd())
        os.chdir(tmp_path)
        return LakehousePlumberApplicationFacade.for_project(
            tmp_path, enforce_version=False
        )

    yield _build
    if created:
        os.chdir(created[0])


def _drain_warnings(events: list) -> list[WarningEmitted]:
    return [e for e in events if isinstance(e, WarningEmitted)]


@pytest.mark.integration
class TestWorkerDeprecationsSurface:
    """Worker-side deprecations ride back and surface as ``WarningEmitted``."""

    def test_database_field_deprecation_now_surfaces(
        self, tmp_path: Path, facade_in
    ) -> None:
        """The ``database`` worker deprecation surfaces as DEPR-002 on the validate stream."""
        _base_project(tmp_path)
        src_file = _write_flowgroup(
            tmp_path,
            "p_db",
            {
                "pipeline": "p_db",
                "flowgroup": "p_db_fg",
                "actions": [
                    {
                        "name": "load_db",
                        "type": "load",
                        "target": "v_db_raw",
                        # Deprecated ``database`` field (catalog.schema) → DEPR-002.
                        "source": {
                            "type": "delta",
                            "database": "dev_catalog.bronze",
                            "table": "src",
                        },
                    },
                    {
                        "name": "write_db",
                        "type": "write",
                        "source": "v_db_raw",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "dev_catalog",
                            "schema": "bronze",
                            "table": "db_table",
                            "create_table": True,
                        },
                    },
                ],
            },
            name="p_db_fg",
        )
        facade = facade_in()

        events = list(
            facade.validation.validate_pipelines(pipeline_fields=["p_db"], env="dev")
        )

        warnings = _drain_warnings(events)
        depr_002 = [w for w in warnings if w.code == "LHP-DEPR-002"]
        assert len(depr_002) == 1, (
            f"expected exactly one LHP-DEPR-002 WarningEmitted; got "
            f"{[(w.code, w.file) for w in warnings]}"
        )
        (warning,) = depr_002
        assert warning.category == "deprecation"
        assert warning.flowgroup == "p_db_fg"
        assert warning.file == src_file
        assert "database" in warning.message.lower()

    def test_database_suffix_preset_deprecation_surfaces(
        self, tmp_path: Path, facade_in
    ) -> None:
        _base_project(tmp_path)
        (tmp_path / "presets" / "legacy_suffix.yaml").write_text(
            yaml.safe_dump(
                {
                    "name": "legacy_suffix",
                    "version": "1.0",
                    "defaults": {
                        "write_actions": {
                            "streaming_table": {"database_suffix": "_dev"}
                        }
                    },
                }
            )
        )
        _write_flowgroup(
            tmp_path,
            "p_suffix",
            {
                "pipeline": "p_suffix",
                "flowgroup": "p_suffix_fg",
                "presets": ["legacy_suffix"],
                "actions": [
                    {
                        "name": "load_s",
                        "type": "load",
                        "target": "v_s",
                        "source": {
                            "type": "cloudfiles",
                            "path": "${landing_path}/s",
                            "format": "json",
                        },
                    },
                    {
                        "name": "write_s",
                        "type": "write",
                        "source": "v_s",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "dev_catalog",
                            "schema": "bronze",
                            "table": "s_table",
                            "create_table": True,
                        },
                    },
                ],
            },
            name="p_suffix_fg",
        )
        facade = facade_in()

        events = list(
            facade.validation.validate_pipelines(
                pipeline_fields=["p_suffix"], env="dev"
            )
        )

        warnings = _drain_warnings(events)
        depr_004 = [w for w in warnings if w.code == "LHP-DEPR-004"]
        assert len(depr_004) == 1, (
            f"expected one LHP-DEPR-004 WarningEmitted; got "
            f"{[(w.code, w.file) for w in warnings]}"
        )
        assert depr_004[0].category == "deprecation"
        assert "database_suffix" in depr_004[0].message


@pytest.mark.integration
class TestMainThreadDeprecationSurfaces:
    def test_bare_token_deprecation_surfaces_with_file(
        self, tmp_path: Path, facade_in
    ) -> None:
        _base_project(tmp_path)
        src_file = _write_flowgroup(
            tmp_path,
            "p_bare",
            {
                "pipeline": "p_bare",
                "flowgroup": "p_bare_fg",
                "actions": [
                    {
                        "name": "load_bare",
                        "type": "load",
                        "target": "v_bare",
                        # Bare ``{landing_path}`` (deprecated) — use ``${...}``.
                        "source": {
                            "type": "cloudfiles",
                            "path": "{landing_path}/bare",
                            "format": "json",
                        },
                    },
                    {
                        "name": "write_bare",
                        "type": "write",
                        "source": "v_bare",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "dev_catalog",
                            "schema": "bronze",
                            "table": "bare_table",
                            "create_table": True,
                        },
                    },
                ],
            },
            name="p_bare_fg",
        )
        facade = facade_in()

        events = list(
            facade.validation.validate_pipelines(pipeline_fields=["p_bare"], env="dev")
        )

        warnings = _drain_warnings(events)
        depr_001 = [w for w in warnings if w.code == "LHP-DEPR-001"]
        assert len(depr_001) == 1, (
            f"expected one LHP-DEPR-001 WarningEmitted; got "
            f"{[(w.code, w.file) for w in warnings]}"
        )
        (warning,) = depr_001
        assert warning.category == "deprecation"
        # Whole-file scan: file attributed, no single owning flowgroup.
        assert warning.file == src_file
        assert warning.flowgroup is None

    def test_warnings_drain_via_collect_response_sink(
        self, tmp_path: Path, facade_in
    ) -> None:
        _base_project(tmp_path)
        _write_flowgroup(
            tmp_path,
            "p_bare2",
            {
                "pipeline": "p_bare2",
                "flowgroup": "p_bare2_fg",
                "actions": [
                    {
                        "name": "load_b2",
                        "type": "load",
                        "target": "v_b2",
                        "source": {
                            "type": "cloudfiles",
                            "path": "{landing_path}/b2",
                            "format": "json",
                        },
                    },
                    {
                        "name": "write_b2",
                        "type": "write",
                        "source": "v_b2",
                        "write_target": {
                            "type": "streaming_table",
                            "catalog": "dev_catalog",
                            "schema": "bronze",
                            "table": "b2_table",
                            "create_table": True,
                        },
                    },
                ],
            },
            name="p_bare2_fg",
        )
        facade = facade_in()

        sink: list[WarningEmitted] = []
        response = collect_response(
            facade.validation.validate_pipelines(
                pipeline_fields=["p_bare2"], env="dev"
            ),
            warnings_sink=sink,
        )
        from lhp.api import BatchValidationResponse

        assert isinstance(response, BatchValidationResponse)
        assert any(w.code == "LHP-DEPR-001" for w in sink)


@pytest.mark.unit
class TestEmissionHelperDedupAndOrder:
    """The shared emission helper: union-dedup by ``(code, file)``, ordered.

    Both facade streams thread a SHARED ``(code, file)`` ``seen`` set through
    :func:`lhp.api._converters_common._emit_deprecation_warnings` across the
    discover-phase (main) and generate/validate-phase (worker) emission points,
    so the union surfaces once, in deterministic caller-iteration order.
    """

    def _record(self, code: str, file: Path | None, flowgroup: str | None):
        from lhp.models.processing import DeprecationWarningRecord

        return DeprecationWarningRecord(
            code=code, message=f"msg for {code}", file=file, flowgroup=flowgroup
        )

    def test_same_code_file_from_two_flowgroups_dedups_to_one(self) -> None:
        """Two records sharing ``(code, file)`` (e.g. two flowgroups in one file)
        collapse to a single ``WarningEmitted``."""
        from lhp.api._converters_common import _emit_deprecation_warnings

        shared_file = Path("pipelines/p/shared.yaml")
        records = [
            self._record("LHP-DEPR-002", shared_file, "fg_a"),
            self._record("LHP-DEPR-002", shared_file, "fg_b"),
        ]
        seen: set = set()
        emitted = list(_emit_deprecation_warnings(records, seen=seen))

        assert len(emitted) == 1
        assert emitted[0].code == "LHP-DEPR-002"
        assert emitted[0].file == shared_file
        # First-seen payload kept verbatim (fg_a, not fg_b).
        assert emitted[0].flowgroup == "fg_a"

    def test_union_dedup_across_two_emission_points(self) -> None:
        """A shared ``seen`` set dedups across the discover + worker calls."""
        from lhp.api._converters_common import _emit_deprecation_warnings

        f = Path("pipelines/p/fg.yaml")
        seen: set = set()
        main = list(
            _emit_deprecation_warnings(
                [self._record("LHP-DEPR-001", f, None)], seen=seen
            )
        )
        # Worker-phase emission: a DISTINCT code on the same file survives, but a
        # repeat of the already-seen ``(LHP-DEPR-001, f)`` is suppressed.
        worker = list(
            _emit_deprecation_warnings(
                [
                    self._record("LHP-DEPR-001", f, "fg"),  # dup → suppressed
                    self._record("LHP-DEPR-002", f, "fg"),  # distinct code → kept
                ],
                seen=seen,
            )
        )

        assert [w.code for w in main] == ["LHP-DEPR-001"]
        assert [w.code for w in worker] == ["LHP-DEPR-002"]

    def test_deterministic_first_seen_order_all_codes(self) -> None:
        """Emission order follows caller-iteration order across all four codes."""
        from lhp.api._converters_common import _emit_deprecation_warnings

        f = Path("pipelines/p/fg.yaml")
        records = [
            self._record("LHP-DEPR-001", f, None),
            self._record("LHP-DEPR-002", f, "fg"),
            self._record("LHP-DEPR-003", f, "fg"),
            self._record("LHP-DEPR-004", f, "fg"),
        ]
        seen: set = set()
        emitted = list(_emit_deprecation_warnings(records, seen=seen))

        assert [w.code for w in emitted] == [
            "LHP-DEPR-001",
            "LHP-DEPR-002",
            "LHP-DEPR-003",
            "LHP-DEPR-004",
        ]
        assert all(w.category == "deprecation" for w in emitted)
        assert all(w.file == f for w in emitted)


@pytest.mark.unit
class TestLockedFieldOrderConstruction:
    """§13.4: ``WarningEmitted(rec.message, code=rec.code, ...)`` type-checks."""

    def test_record_to_warning_emitted_field_order(self) -> None:
        from lhp.models.processing import DeprecationWarningRecord

        rec = DeprecationWarningRecord(
            code="LHP-DEPR-002",
            message="the database field is deprecated",
            file=Path("pipelines/p/fg.yaml"),
            flowgroup="fg",
        )
        # message POSITIONAL, code/file/flowgroup keyword (locked order).
        warning = WarningEmitted(
            rec.message,
            code=rec.code,
            category="deprecation",
            file=rec.file,
            flowgroup=rec.flowgroup,
        )
        assert warning.message == rec.message
        assert warning.code == "LHP-DEPR-002"
        assert warning.file == rec.file
        assert warning.flowgroup == "fg"


@pytest.mark.unit
class TestEventBufferSoftCap:
    """C6 / §13.4: the event-buffer soft cap wraps every public facade stream.

    The shared wrapper :func:`lhp.api._stream_guard._cap_event_stream` counts
    the events of the stream it wraps and emits a single
    ``WarningEmitted("event buffer near limit", code="LHP-EVT-SOFT-CAP")`` when
    the count reaches the soft cap (999), then keeps forwarding. These tests
    pin the synthetic wrapper directly (a >999-event stream → exactly one cap
    warning; a <999-event stream → none) plus the §5.7 ordering guarantees the
    injection must preserve.
    """

    @staticmethod
    def _fake_stream(n: int):
        """Yield ``n`` non-terminal placeholder events, then one terminal.

        The first ``n`` events are ``PhaseStarted`` (non-terminal); the final
        event is a terminal ``OperationCompleted`` subclass so the stream looks
        like a real §5.7 stream whose last event is its terminal.
        """
        from lhp.api import BatchGenerationResponse, GenerationCompleted
        from lhp.api.events import PhaseStarted

        for _ in range(n):
            yield PhaseStarted(phase="generate")
        yield GenerationCompleted(
            response=BatchGenerationResponse(
                success=True,
                pipeline_responses={},
                total_files_written=0,
                aggregate_generated_filenames=(),
                output_location=None,
            )
        )

    def _caps(self, events: list) -> list[WarningEmitted]:
        return [
            e
            for e in events
            if isinstance(e, WarningEmitted) and e.code == "LHP-EVT-SOFT-CAP"
        ]

    def test_stream_over_cap_emits_exactly_one_soft_cap_warning(self) -> None:
        """A stream of >999 events emits exactly ONE LHP-EVT-SOFT-CAP warning."""
        from lhp.api._stream_guard import _cap_event_stream

        # 1500 placeholders + 1 terminal = 1501 inner events (well over 999).
        out = list(_cap_event_stream(self._fake_stream(1500)))

        caps = self._caps(out)
        assert len(caps) == 1, f"expected exactly one soft-cap; got {len(caps)}"
        cap = caps[0]
        # §13.4 locked emission: message positional, code keyword.
        assert cap.message == "event buffer near limit"
        assert cap.code == "LHP-EVT-SOFT-CAP"
        # Injected immediately BEFORE the 999th inner event (index 998), so the
        # inner event that tripped the cap still follows it.
        assert out.index(cap) == 998
        # The wrapper forwards every inner event unchanged AND injects one
        # warning: 1501 inner + 1 cap = 1502 total.
        assert len(out) == 1502

    def test_stream_under_cap_emits_no_warning(self) -> None:
        """A stream shorter than the cap emits NO soft-cap warning."""
        from lhp.api._stream_guard import _cap_event_stream

        # 997 placeholders + 1 terminal = 998 inner events (< 999).
        out = list(_cap_event_stream(self._fake_stream(997)))

        assert self._caps(out) == []
        # Nothing injected: the stream passes through unchanged.
        assert len(out) == 998

    def test_soft_cap_preserves_terminal_last_at_exactly_999(self) -> None:
        """At exactly 999 inner events the cap is emitted, terminal stays last.

        The 999th inner event is the terminal ``GenerationCompleted``; the
        wrapper injects the warning BEFORE it, so "exactly one terminal event
        is the last event" (§5.7) still holds.
        """
        from lhp.api import GenerationCompleted
        from lhp.api._stream_guard import _cap_event_stream

        # 998 placeholders + 1 terminal = 999 inner events (== soft cap).
        out = list(_cap_event_stream(self._fake_stream(998)))

        assert len(self._caps(out)) == 1
        # Terminal is still the very last event (warning landed before it).
        assert isinstance(out[-1], GenerationCompleted)
        assert not isinstance(out[-1], WarningEmitted)

    def test_soft_cap_is_emitted_only_once_across_a_long_stream(self) -> None:
        """The cap fires once and never re-triggers as the stream continues."""
        from lhp.api._stream_guard import _cap_event_stream

        out = list(_cap_event_stream(self._fake_stream(5000)))
        assert len(self._caps(out)) == 1
