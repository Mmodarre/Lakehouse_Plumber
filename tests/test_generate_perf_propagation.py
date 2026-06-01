"""Cross-process perf-propagation proof for ``lhp --perf generate``.

The per-flowgroup work runs inside a ``ProcessPoolExecutor(mp_context="spawn")``.
Spawned workers re-import the perf module with ``_enabled = False``, so before
the worker→parent perf channel landed, every in-worker ``perf_timer(...)`` call
hit the disabled fast-path and recorded nothing — the "Per-category aggregate
stats" table in ``perf.log`` showed only coordinator-side categories.

This test proves the channel now works end-to-end: a ``--perf`` generate run
over a ≥2-flowgroup project produces a perf.log whose per-category table now
contains the previously dead WORKER-side rows ``resolve_dependencies`` and
``assemble_code``, each with ``cnt == flowgroup count``. Those categories are
recorded ONLY inside the worker (``core/codegen/coordinator.py``), so their
presence here is direct evidence that the worker's in-memory aggregate crossed
the spawn boundary and was merged into the coordinator singleton that
``log_perf_summary()`` renders.

This file also carries (T9):

* the *consolidated* worker-visibility proof — every per-category timing row
  that a ``--perf generate`` lights up over a presets-using fixture, with the
  observed counts pinned (notably ``preset_resolve`` at 2x the flowgroup count,
  which exposes the double preset-chain resolution); and
* the zero-overhead / output-neutral guard — WITHOUT ``--perf``, no ``perf.log``
  is written and the worker envelope returns ``FlowgroupOutcome.perf is None``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli
from lhp.core.coordination import _flowgroup_pool as fp
from lhp.core.coordination._flowgroup_pool import (
    _FlowgroupWorkerState,
    _process_one_flowgroup,
)
from lhp.utils import performance_timer as pt
from lhp.utils.performance_timer import is_perf_enabled
from tests.fakes import (
    FakeCodeFormatter,
    FakeCodeGenerator,
    FakeFlowgroupResolutionService,
    FakeSubstitutionManager,
)
from tests.helpers.contexts import wrap_in_ctx

# Worker-side categories recorded ONLY inside the spawn worker
# (core/codegen/coordinator.py). Their appearance in the coordinator's
# perf.log is the cross-process propagation proof.
_WORKER_CATEGORIES = ("resolve_dependencies", "assemble_code")


class _MinimalFlowGroup:
    """Smallest FlowGroup stand-in the worker reads in ``validate`` mode.

    ``_process_one_flowgroup_impl`` only reads ``.pipeline`` / ``.flowgroup``
    off the (un)resolved flowgroup before short-circuiting at the end of the
    resolve step in validate mode; the fake resolver returns the ctx
    unchanged, so this object also doubles as the resolved flowgroup.
    """

    def __init__(self, *, pipeline: str, flowgroup: str) -> None:
        self.pipeline = pipeline
        self.flowgroup = flowgroup


def _build_multipipeline_project(
    project_root: Path, pipeline_names, *, with_presets: bool = False
) -> None:
    """Project with one small flowgroup per pipeline (load + transform + write).

    Mirrors ``tests/test_generate_command_parallel.py``'s builder so the worker
    path exercised here is identical to the existing parallel-generate tests.

    ``with_presets`` adds one shared preset file and references it from every
    flowgroup. That lights the preset-resolution path (``preset_resolve`` /
    ``fg_presets`` categories), which is otherwise dead because the bare
    fixture declares no presets — ``resolve_preset_chain`` is gated on
    ``flowgroup.presets`` at all three call sites.
    """
    (project_root / "presets").mkdir(parents=True, exist_ok=True)
    (project_root / "templates").mkdir(parents=True, exist_ok=True)
    (project_root / "substitutions").mkdir(parents=True, exist_ok=True)
    for name in pipeline_names:
        (project_root / "pipelines" / name).mkdir(parents=True, exist_ok=True)

    (project_root / "lhp.yaml").write_text(
        "name: test_perf_propagation_project\nversion: '1.0'\n"
    )

    subs = {
        "dev": {
            "catalog": "dev_catalog",
            "bronze_schema": "bronze",
            "landing_path": "/mnt/dev/landing",
        }
    }
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(subs, f)

    if with_presets:
        (project_root / "presets" / "bronze_props.yaml").write_text(
            "name: bronze_props\n"
            "version: '1.0'\n"
            "defaults:\n"
            "  write_actions:\n"
            "    streaming_table:\n"
            "      table_properties:\n"
            "        delta.enableRowTracking: 'true'\n"
        )

    for name in pipeline_names:
        flowgroup = {
            "pipeline": name,
            "flowgroup": f"{name}_fg",
            "actions": [
                {
                    "name": f"load_{name}",
                    "type": "load",
                    "target": f"v_{name}_raw",
                    "source": {
                        "type": "cloudfiles",
                        "path": "${landing_path}/" + name,
                        "format": "json",
                    },
                },
                {
                    "name": f"clean_{name}",
                    "type": "transform",
                    "transform_type": "sql",
                    "source": f"v_{name}_raw",
                    "target": f"v_{name}_clean",
                    "sql": f"SELECT * FROM v_{name}_raw",
                },
                {
                    "name": f"write_{name}",
                    "type": "write",
                    "source": f"v_{name}_clean",
                    "write_target": {
                        "type": "streaming_table",
                        "catalog": "${catalog}",
                        "schema": "${bronze_schema}",
                        "table": name,
                        "create_table": True,
                    },
                },
            ],
        }
        if with_presets:
            flowgroup["presets"] = ["bronze_props"]
        with open(project_root / "pipelines" / name / f"{name}_fg.yaml", "w") as f:
            yaml.dump(flowgroup, f)


def _parse_category_counts(perf_log_text: str) -> dict:
    """Parse the ``Per-category aggregate stats`` rows into ``{category: cnt}``.

    The renderer (``PerfSummary.log_summary``) emits each row as::

        [PERF]   <category>            cnt=<n>    avg=...s  min=...  max=...  total=...
    """
    counts: dict = {}
    row = re.compile(r"\[PERF\]\s+(\S+)\s+cnt=(\d+)\b")
    in_table = False
    for line in perf_log_text.splitlines():
        if "Per-category aggregate stats:" in line:
            in_table = True
            continue
        if not in_table:
            continue
        # The table ends at the next section header / trailing rule.
        if "Event counts:" in line or "====" in line:
            break
        m = row.search(line)
        if m:
            counts[m.group(1)] = int(m.group(2))
    return counts


class TestGeneratePerfPropagation:
    PIPELINES = ["p_alpha", "p_beta"]

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture(autouse=True)
    def _reset_perf_state(self):
        """Disable + clear the module-global perf singleton around each test.

        A ``--perf`` run flips ``performance_timer._enabled`` True and adds a
        file handler in THIS process; without a reset that state leaks into
        sibling tests (notably the zero-overhead proof, which asserts perf is
        OFF). Mirrors ``tests/unit/test_performance_timer.py``'s fixture.
        """
        self._reset_perf_module()
        yield
        self._reset_perf_module()

    @staticmethod
    def _reset_perf_module() -> None:
        pt._enabled = False
        pt._start_wall_clock = None
        pt._summary.reset()
        for handler in pt._perf_logger.handlers[:]:
            handler.close()
            pt._perf_logger.removeHandler(handler)

    def test_worker_category_stats_propagate_to_parent_perf_log(
        self, runner, tmp_path, monkeypatch
    ):
        project_root = tmp_path
        _build_multipipeline_project(project_root, self.PIPELINES)
        flowgroup_count = len(self.PIPELINES)  # one flowgroup per pipeline

        monkeypatch.chdir(project_root)
        # ``--perf`` is a GROUP-level option, so it precedes the subcommand.
        result = runner.invoke(
            cli,
            [
                "--perf",
                "generate",
                "--env",
                "dev",
                "--max-workers",
                "2",
                "--no-bundle",
            ],
        )
        assert result.exit_code == 0, f"CLI exited {result.exit_code}: {result.output}"

        perf_log = project_root / ".lhp" / "logs" / "perf.log"
        assert (
            perf_log.exists()
        ), f"Expected perf.log at {perf_log}; CLI output:\n{result.output}"
        text = perf_log.read_text(encoding="utf-8")
        assert (
            "Per-category aggregate stats:" in text
        ), f"perf.log missing the per-category table:\n{text}"

        counts = _parse_category_counts(text)

        # The worker-side categories must now be present (cross-process
        # propagation), each counted once per flowgroup.
        for category in _WORKER_CATEGORIES:
            assert category in counts, (
                f"Worker-side category '{category}' absent from the per-category "
                f"table — cross-process perf propagation is not working.\n"
                f"Parsed categories: {sorted(counts)}\n\nperf.log:\n{text}"
            )
            assert counts[category] == flowgroup_count, (
                f"Category '{category}' has cnt={counts[category]}, expected "
                f"{flowgroup_count} (one per flowgroup).\n\nperf.log:\n{text}"
            )

    def test_all_worker_categories_visible_with_expected_counts(
        self, runner, tmp_path, monkeypatch
    ):
        """Consolidated worker-visibility: every per-category row a ``--perf``
        generate lights up appears with the count the fixture dictates.

        Built WITH presets (``with_presets=True``) so the preset-resolution
        path is alive — without it ``preset_resolve`` / ``fg_presets`` never
        fire (``resolve_preset_chain`` is gated on ``flowgroup.presets`` at
        every call site).

        Observed counts for this 2-flowgroup, 3-action-per-flowgroup fixture:

        * ``resolve_dependencies`` cnt=2  (once per flowgroup; coordinator)
        * ``assemble_code``        cnt=2  (once per flowgroup; coordinator)
        * ``get_generator``        cnt=6  (one per action: load+transform+write)
        * ``jinja_render``         cnt=6  (one render per action generator)
        * ``black_format``         cnt=2  (one format per flowgroup's code)
        * ``preset_resolve``       cnt=4  == ``2 * flowgroup_count``

        The ``preset_resolve`` ratio is the headline finding: each flowgroup's
        preset chain is resolved TWICE per run — once in the resolver
        (``flowgroup_resolver.py``, the ``fg_presets`` path, cnt=2 here) and a
        second time in the codegen coordinator (``coordinator.py`` line ~160).
        The assertion pins the observed ``== 2 * flowgroup_count`` relationship
        to lock that double-resolution exposure in place.
        """
        project_root = tmp_path
        _build_multipipeline_project(project_root, self.PIPELINES, with_presets=True)
        flowgroup_count = len(self.PIPELINES)  # one flowgroup per pipeline

        monkeypatch.chdir(project_root)
        result = runner.invoke(
            cli,
            [
                "--perf",
                "generate",
                "--env",
                "dev",
                "--max-workers",
                "2",
                "--no-bundle",
            ],
        )
        assert result.exit_code == 0, f"CLI exited {result.exit_code}: {result.output}"

        perf_log = project_root / ".lhp" / "logs" / "perf.log"
        assert (
            perf_log.exists()
        ), f"Expected perf.log at {perf_log}; CLI output:\n{result.output}"
        text = perf_log.read_text(encoding="utf-8")
        counts = _parse_category_counts(text)

        def _present(category: str) -> int:
            assert category in counts, (
                f"Category '{category}' absent from the per-category table.\n"
                f"Parsed categories: {sorted(counts)}\n\nperf.log:\n{text}"
            )
            return counts[category]

        # Categories pinned to exactly one occurrence per flowgroup.
        for category in ("resolve_dependencies", "assemble_code", "black_format"):
            assert _present(category) == flowgroup_count, (
                f"Category '{category}' has cnt={counts[category]}, expected "
                f"{flowgroup_count} (one per flowgroup).\n\nperf.log:\n{text}"
            )

        # Action-count-driven categories: non-zero (one per action generator).
        for category in ("get_generator", "jinja_render"):
            assert _present(category) > 0, (
                f"Category '{category}' has cnt={counts[category]}, expected "
                f"> 0 (one per action generated).\n\nperf.log:\n{text}"
            )

        # preset_resolve: each flowgroup's chain is resolved twice (resolver +
        # codegen coordinator), so cnt is exactly 2x the flowgroup count.
        preset_cnt = _present("preset_resolve")
        assert preset_cnt > flowgroup_count, (
            f"preset_resolve cnt={preset_cnt} should exceed the flowgroup "
            f"count ({flowgroup_count}) — the double-resolution is not "
            f"exposed.\n\nperf.log:\n{text}"
        )
        assert preset_cnt == 2 * flowgroup_count, (
            f"preset_resolve cnt={preset_cnt}, expected {2 * flowgroup_count} "
            f"(== 2 * flowgroup_count: resolved once in the resolver and once "
            f"in the codegen coordinator per flowgroup).\n\nperf.log:\n{text}"
        )

    def test_no_perf_flag_writes_no_log_and_outcome_perf_is_none(
        self, runner, tmp_path, monkeypatch
    ):
        """Zero-overhead / output-neutral guard for a run WITHOUT ``--perf``.

        Two independent proofs:

        1. *Output-neutral.* A plain ``generate`` (no ``--perf``) over the same
           fixture writes NO ``perf.log`` — nothing is timed or rendered.
        2. *Envelope returns ``perf=None``.* An in-process call to
           :func:`_process_one_flowgroup` with perf disabled (the default;
           asserted via :func:`is_perf_enabled`) returns an outcome whose
           ``perf`` field is ``None``. This proves the perf envelope's
           disabled fast-path directly, without depending on the CLI. The
           worker reads its collaborators from the ``_flowgroup_state`` global
           (set here exactly as ``_init_flowgroup_worker`` would in a spawned
           worker), and ``validate`` mode exercises only the resolver — so the
           empty-surface ``FakeCodeGenerator`` / ``FakeCodeFormatter`` are
           never called.
        """
        # --- Proof 1: no --perf => no perf.log written ----------------------
        project_root = tmp_path
        _build_multipipeline_project(project_root, self.PIPELINES)

        monkeypatch.chdir(project_root)
        result = runner.invoke(
            cli,
            ["generate", "--env", "dev", "--max-workers", "2", "--no-bundle"],
        )
        assert result.exit_code == 0, f"CLI exited {result.exit_code}: {result.output}"

        perf_log = project_root / ".lhp" / "logs" / "perf.log"
        assert not perf_log.exists(), (
            f"perf.log was written at {perf_log} despite no --perf flag.\n"
            f"CLI output:\n{result.output}"
        )

        # --- Proof 2: envelope returns perf=None when perf is disabled -------
        # Perf is off by default in this (parent) process.
        assert not is_perf_enabled()

        state = _FlowgroupWorkerState(
            processor=FakeFlowgroupResolutionService(),
            substitution_managers={"pipe_x": FakeSubstitutionManager()},
            include_tests=False,
            code_generator=FakeCodeGenerator(),
            formatter=FakeCodeFormatter(),
            pipeline_output_dirs={"pipe_x": None},
            environment="dev",
        )
        monkeypatch.setattr(fp, "_flowgroup_state", state)

        ctx = wrap_in_ctx(_MinimalFlowGroup(pipeline="pipe_x", flowgroup="fg_x"))
        outcome = _process_one_flowgroup(ctx, mode="validate")

        assert outcome.success is True, outcome
        assert outcome.perf is None, (
            "Worker envelope attached a perf export with perf disabled — the "
            f"zero-overhead contract is broken: perf={outcome.perf!r}"
        )
