"""Integration: sandbox generate over the REAL e2e fixture project.

Copies ``tests/e2e/fixtures/testing_project`` (§8.5 — the fixture itself is
never touched) into tmp, writes a personal ``.lhp/profile.yaml`` scoping the
run to ``acmi_edw_silver`` plus the ``gold_*`` glob, appends the team
``sandbox:`` policy to ``lhp.yaml``, and drives the REAL generation facade
with ``sandbox=True``. The fixture's own producer/consumer graph carries the
assertions:

* ``acmi_edw_silver`` PRODUCES ``acme_edw_dev.edw_silver.{customer_dim,
  nation_dim, supplier_dim, partsupp_dim, dim_sfcc_cust}`` and READS bronze
  tables produced by the OUT-of-scope ``acmi_edw_bronze`` (shared reads —
  untouched: read-shared/write-own).
* ``gold_load`` PRODUCES the gold MVs and READS the silver dims inside
  generated ``spark.sql("...")`` literals (``customer_lifetime_value.py``
  joins in-scope ``customer_dim`` / ``nation_dim`` against the
  never-produced ``orders_fct`` — renamed and untouched refs in ONE body).

One probe flowgroup + user module is ADDED TO THE TMP COPY ONLY (never the
shared fixture): a ``gold_load`` python transform whose copied module holds
an in-scope table literal (rewritten), an out-of-scope literal (untouched),
and a variable-bound in-scope read (the ``LHP-VAL-066`` case).

The gate-abort test swaps the spawn ``ProcessPoolExecutor`` for an
in-process stand-in (the established ``_SyncExecutor`` precedent from
``tests/core/coordination/test_flowgroup_pool.py``, here additionally
invoking the REAL initializer so the REAL worker runs in-process) so a
monkeypatched ``lhp.core.sandbox.rewrite_python_table_literals`` reaches the
worker — spawn isolation would swallow the patch. Commit-gate contract under
test (``executor._iter_generate_deltas`` → ``gate_or_raise`` →
``commit_generate_results``): the gate raises BEFORE the commit generator is
created, and the env-tree wipe inside ``commit_generate_results`` is lazy —
so a failed run performs ZERO wipes and ZERO writes, leaving
``generated/<env>/`` byte-identical.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
from concurrent.futures import Future
from pathlib import Path

import pytest

import lhp.core.sandbox as sandbox_pkg
from lhp.api import (
    ErrorEmitted,
    GenerationCompleted,
    LakehousePlumberApplicationFacade,
    PhaseStarted,
    PipelineCompleted,
    PipelineFailed,
    PipelineStarted,
    WarningEmitted,
)
from lhp.core.coordination import _flowgroup_pool as fp
from lhp.core.coordination import _pool as fe
from lhp.errors import LHPError

_E2E_FIXTURE = (
    Path(__file__).resolve().parents[1] / "e2e" / "fixtures" / "testing_project"
)

_ENV = "dev"

# Scope: one exact name + one glob (the glob expands to gold_load only).
_PROFILE = """sandbox:
  namespace: alice
  pipelines:
    - acmi_edw_silver
    - gold_*
"""

# Gate-abort scope: gold_load only (smaller worklist for the two extra runs).
_PROFILE_GOLD_ONLY = """sandbox:
  namespace: alice
  pipelines:
    - gold_load
"""

# Team policy appended to the fixture's lhp.yaml: the DEFAULT pattern stated
# explicitly, plus allowed_envs so the run also exercises the CFG-065 gate
# positively (dev IS sandbox-enabled).
_SANDBOX_POLICY = """
sandbox:
  strategy: table
  table_pattern: "{namespace}_{table}"
  allowed_envs:
    - dev
"""

# Probe flowgroup for the tmp copy: load an IN-SCOPE silver table (structured
# read -> renamed), python transform via a copied user module, write a gold
# streaming table (renamed). Mirrors the fixture's own
# 19_dependency_bindings/opaque_read_flow.yaml action shape.
_PROBE_FLOWGROUP = """pipeline: gold_load
flowgroup: sandbox_module_probe

actions:
  - name: load_probe_seed
    type: load
    readMode: stream
    source:
      type: delta
      database: "${catalog}.${silver_schema}"
      table: customer_dim
    target: v_probe_seed

  - name: probe_enrich
    type: transform
    transform_type: python
    source: v_probe_seed
    module_path: "py_functions/sandbox_probe_transform.py"
    function_name: "enrich_with_dims"
    readMode: stream
    parameters:
      note: "sandbox_probe"
    target: v_probe_out

  - name: write_probe_out
    type: write
    source: v_probe_out
    write_target:
      type: streaming_table
      database: "${catalog}.${gold_schema}"
      table: "sandbox_probe_out"
"""

# Copied user module: one rewritable in-scope literal, one out-of-scope
# literal (shared read), one variable-bound in-scope read (-> LHP-VAL-066).
# Hardcoded resolved names mirror py_functions/dep_bindings_opaque_transform.py.
_PROBE_MODULE = '''from pyspark.sql import DataFrame

# Variable-bound IN-SCOPE read: the rewriter recognizes the site but cannot
# rewrite a non-literal argument -> one LHP-VAL-066 for this file; the
# assignment literal itself is NOT a read site and stays unrenamed.
_VARIABLE_BOUND_DIM = "acme_edw_dev.edw_silver.nation_dim"


def enrich_with_dims(df: DataFrame, spark, parameters) -> DataFrame:
    """Join the probe stream against silver/bronze lookups (never executed)."""
    in_scope_dim = spark.read.table("acme_edw_dev.edw_silver.customer_dim")
    shared_bronze = spark.read.table("acme_edw_dev.edw_bronze.customer")
    bound_dim = spark.read.table(_VARIABLE_BOUND_DIM)
    out = df.join(in_scope_dim, ["customer_id"], "left")
    out = out.unionByName(shared_bronze, allowMissingColumns=True)
    return out.unionByName(bound_dim, allowMissingColumns=True)
'''


def _copy_fixture(dest: Path, profile: str) -> Path:
    """Deep-copy the e2e fixture and apply the sandbox additions to the COPY."""
    shutil.copytree(_E2E_FIXTURE, dest, ignore=shutil.ignore_patterns("__pycache__"))
    (dest / ".lhp").mkdir()
    (dest / ".lhp" / "profile.yaml").write_text(profile)
    lhp_yaml = dest / "lhp.yaml"
    lhp_yaml.write_text(lhp_yaml.read_text() + _SANDBOX_POLICY)
    (dest / "pipelines" / "04_gold" / "sandbox_module_probe.yaml").write_text(
        _PROBE_FLOWGROUP
    )
    (dest / "py_functions" / "sandbox_probe_transform.py").write_text(_PROBE_MODULE)
    return dest


def _drain(gen):
    """Collect events until exhaustion or an LHPError raise (§1.4)."""
    events: list = []
    raised: LHPError | None = None
    try:
        for event in gen:
            events.append(event)
    except LHPError as exc:
        raised = exc
    return events, raised


def _generate(project_root: Path, *, max_workers: int):
    """One REAL sandbox generate run; formatting off (no ruff dependency)."""
    facade = LakehousePlumberApplicationFacade.for_project(
        project_root, enforce_version=False
    )
    return _drain(
        facade.generation.generate_pipelines(
            env=_ENV,
            output_dir=project_root / "generated" / _ENV,
            sandbox=True,
            max_workers=max_workers,
            apply_formatting=False,
        )
    )


def _tree_digest(root: Path) -> dict[str, str]:
    """{relative file path: sha256} over every file under ``root``."""
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


@pytest.fixture(scope="module")
def sandbox_run(tmp_path_factory):
    """ONE shared successful sandbox run; the read-only tests assert off it."""
    project_root = _copy_fixture(
        tmp_path_factory.mktemp("sandbox_w3") / "proj", _PROFILE
    )
    with pytest.MonkeyPatch.context() as mp:
        mp.chdir(project_root)
        events, raised = _generate(project_root, max_workers=2)
    assert raised is None, f"shared sandbox run raised: {raised}"
    return project_root, events


# Scope


@pytest.mark.integration
def test_scope_generates_exactly_the_profile_pipelines(sandbox_run):
    """generated/<env>/ holds EXACTLY the in-scope pipeline dirs; the event
    stream ran exactly those pipelines (glob ``gold_*`` -> gold_load)."""
    project_root, events = sandbox_run
    output_dir = project_root / "generated" / _ENV

    pipeline_dirs = sorted(p.name for p in output_dir.iterdir() if p.is_dir())
    assert pipeline_dirs == ["acmi_edw_silver", "gold_load"]

    started = sorted(e.pipeline for e in events if isinstance(e, PipelineStarted))
    completed = sorted(e.pipeline for e in events if isinstance(e, PipelineCompleted))
    assert started == ["acmi_edw_silver", "gold_load"]
    assert completed == ["acmi_edw_silver", "gold_load"]
    assert not [e for e in events if isinstance(e, PipelineFailed)]

    terminal = events[-1]
    assert isinstance(terminal, GenerationCompleted)
    assert terminal.response.success is True


# Write renames: tables PRODUCED by in-scope pipelines


@pytest.mark.integration
def test_write_targets_renamed_through_namespace_pattern(sandbox_run):
    """Every in-scope write target carries the ``alice_<table>`` leaf — the
    silver CDC streaming table + flow, and the gold MV decorator."""
    project_root, _ = sandbox_run
    output_dir = project_root / "generated" / _ENV

    silver = (output_dir / "acmi_edw_silver" / "customer_silver_dim.py").read_text()
    assert 'name="acme_edw_dev.edw_silver.alice_customer_dim"' in silver
    assert 'target="acme_edw_dev.edw_silver.alice_customer_dim"' in silver
    # The unrenamed fully-qualified name is GONE from the producing file.
    assert "acme_edw_dev.edw_silver.customer_dim" not in silver

    gold = (output_dir / "gold_load" / "customer_lifetime_value.py").read_text()
    assert 'name="acme_edw_dev.edw_gold.alice_customer_lifetime_value_mv"' in gold

    probe = (output_dir / "gold_load" / "sandbox_module_probe.py").read_text()
    assert "acme_edw_dev.edw_gold.alice_sandbox_probe_out" in probe
    # The probe's structured load of the in-scope silver table is renamed too
    # (reads of in-scope-produced tables rewritten everywhere in scope).
    assert (
        'spark.readStream.table("acme_edw_dev.edw_silver.alice_customer_dim")' in probe
    )


# spark.sql body renames + shared reads untouched


@pytest.mark.integration
def test_spark_sql_bodies_rename_in_scope_refs_only(sandbox_run):
    """Inside ONE generated ``spark.sql`` literal, in-scope-produced refs are
    renamed while refs without an in-scope producer stay byte-identical."""
    project_root, _ = sandbox_run
    output_dir = project_root / "generated" / _ENV

    clv = (output_dir / "gold_load" / "customer_lifetime_value.py").read_text()
    # In-scope silver dims renamed INSIDE the SQL text.
    assert "FROM acme_edw_dev.edw_silver.alice_customer_dim c" in clv
    assert "JOIN acme_edw_dev.edw_silver.alice_nation_dim n" in clv
    # orders_fct has NO in-scope producer -> untouched in the same body.
    assert "JOIN acme_edw_dev.edw_silver.orders_fct o" in clv

    dashboard = (output_dir / "gold_load" / "executive_dashboard_mv.py").read_text()
    # gold_load's own MV (in-scope produced) renamed where it is READ...
    assert "FROM acme_edw_dev.edw_gold.alice_customer_lifetime_value_mv" in dashboard
    # ...while a ref no in-scope pipeline produces stays untouched.
    assert "FROM acme_edw_dev.edw_gold.sales_summary_monthly_mv" in dashboard


@pytest.mark.integration
def test_reads_of_out_of_scope_tables_stay_untouched(sandbox_run):
    """Shared reads (tables produced by OUT-of-scope pipelines) keep their
    original names in structured loads, SQL bodies, and copied modules."""
    project_root, _ = sandbox_run
    output_dir = project_root / "generated" / _ENV

    # Structured load: silver streams from bronze (acmi_edw_bronze is out of
    # scope) — the read survives unrenamed.
    silver = (output_dir / "acmi_edw_silver" / "customer_silver_dim.py").read_text()
    assert 'spark.readStream.table("acme_edw_dev.edw_bronze.customer")' in silver

    # spark.sql body: order_summary_mv reads edw_silver.orders, which nothing
    # in scope produces — untouched.
    summary = (output_dir / "gold_load" / "order_summary_mv.py").read_text()
    assert "FROM acme_edw_dev.edw_silver.orders o" in summary

    # Copied module shipped by the fixture itself: bronze refs inside its
    # spark.sql text survive unrenamed.
    snapshot = (
        output_dir
        / "acmi_edw_silver"
        / "custom_python_functions"
        / "partsupp_snapshot_func.py"
    ).read_text()
    assert "acme_edw_dev.edw_bronze.partsupp" in snapshot
    assert "alice_partsupp" not in snapshot


# Copied-module renames + LHP-VAL-066


@pytest.mark.integration
def test_copied_module_literals_renamed_in_scope_only(sandbox_run):
    """The COPIED user module on disk carries the renamed in-scope literal,
    keeps the out-of-scope literal, and keeps the variable-bound name."""
    project_root, _ = sandbox_run
    module = (
        project_root
        / "generated"
        / _ENV
        / "gold_load"
        / "custom_python_functions"
        / "sandbox_probe_transform.py"
    ).read_text()

    # In-scope literal rewritten in place (quote style preserved).
    assert 'spark.read.table("acme_edw_dev.edw_silver.alice_customer_dim")' in module
    # Out-of-scope literal untouched.
    assert 'spark.read.table("acme_edw_dev.edw_bronze.customer")' in module
    # The variable BINDING is not a read site — its literal stays unrenamed
    # (the read through it is the LHP-VAL-066 case asserted below).
    assert '_VARIABLE_BOUND_DIM = "acme_edw_dev.edw_silver.nation_dim"' in module


@pytest.mark.integration
def test_unrewritable_in_scope_read_emits_val_066(sandbox_run):
    """The variable-bound in-scope read folds into EXACTLY ONE WarningEmitted
    (category='sandbox', LHP-VAL-066) pointing at the copied module; the run
    has no mixed-producer sinks, so no LHP-VAL-065 appears."""
    _, events = sandbox_run

    sandbox_warnings = [
        e for e in events if isinstance(e, WarningEmitted) and e.category == "sandbox"
    ]
    val_066 = [w for w in sandbox_warnings if w.code == "LHP-VAL-066"]
    assert len(val_066) == 1
    warning = val_066[0]
    assert warning.flowgroup == "sandbox_module_probe"
    assert warning.file is not None
    assert str(warning.file).endswith(
        "custom_python_functions/sandbox_probe_transform.py"
    )
    # The message names the still-original in-scope table at its read site.
    assert "acme_edw_dev.edw_silver.nation_dim" in warning.message

    assert not [w for w in sandbox_warnings if w.code == "LHP-VAL-065"]


# Monitoring phase guard


@pytest.mark.integration
def test_sandbox_run_emits_no_monitoring_phase(sandbox_run):
    """A sandbox run NEVER opens the monitoring phase (the finalizer would
    clobber shared committed artifacts with a sandbox-scoped worklist)."""
    _, events = sandbox_run
    phases = [e.phase for e in events if isinstance(e, PhaseStarted)]
    assert "monitoring" not in phases
    assert phases.count("generate") == 1


# Gate abort: injected rewriter failure leaves the output tree untouched


class _InProcessExecutor:
    """``ProcessPoolExecutor`` stand-in running the REAL worker in-process.

    Unlike the fake-worker ``_SyncExecutor`` precedent, this one INVOKES the
    engine's initializer (``_init_flowgroup_worker``) so the genuine
    ``_process_one_flowgroup`` finds its module-global worker state in THIS
    process — which is what lets a monkeypatched
    ``lhp.core.sandbox.rewrite_python_table_literals`` reach the worker's
    call-time import (a spawn boundary would discard the patch). ``submit``
    resolves eagerly into an already-completed Future, keeping the engine's
    submit / as_completed / result call shape intact.
    """

    def __init__(self, *args, initializer=None, initargs=(), **kwargs) -> None:
        if initializer is not None:
            initializer(*initargs)

    def __enter__(self) -> "_InProcessExecutor":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def submit(self, fn, /, *args, **kwargs) -> Future:
        fut: Future = Future()
        try:
            fut.set_result(fn(*args, **kwargs))
        except Exception as exc:  # pragma: no cover - worker never raises
            fut.set_exception(exc)
        return fut


@pytest.mark.integration
def test_gate_abort_on_rewriter_failure_leaves_tree_untouched(tmp_path, monkeypatch):
    """Injected rewriter failure -> every flowgroup fails -> the
    all-or-nothing gate raises BEFORE commit -> generated/<env>/ stays
    byte-identical to the previous successful run (no wipe, no writes)."""
    project_root = _copy_fixture(tmp_path / "proj", _PROFILE_GOLD_ONLY)
    output_dir = project_root / "generated" / _ENV
    monkeypatch.chdir(project_root)

    # In-process pool (see _InProcessExecutor) + restore registrations for
    # the worker-state module global the real initializer overwrites and the
    # root-logger handlers `_init_worker_logger` strips in-process.
    monkeypatch.setattr(fe, "ProcessPoolExecutor", _InProcessExecutor)
    monkeypatch.setattr(fp, "_flowgroup_state", fp._flowgroup_state)
    root_logger = logging.getLogger()
    saved_handlers = list(root_logger.handlers)
    saved_level = root_logger.level

    try:
        # Run 1 — seed: an untampered run populates the tree.
        events_ok, raised_ok = _generate(project_root, max_workers=1)
        assert raised_ok is None, f"seed run raised: {raised_ok}"
        assert isinstance(events_ok[-1], GenerationCompleted)
        before = _tree_digest(output_dir)
        assert before, "seed run wrote no files"

        # Run 2 — inject: the worker's call-time import of the rewriter now
        # raises; the catch-all converts it into a failure DTO (§5.6).
        def _explode(source, renames):
            raise ValueError("injected sandbox rewriter failure (W3 gate-abort)")

        monkeypatch.setattr(sandbox_pkg, "rewrite_python_table_literals", _explode)
        events_fail, raised_fail = _generate(project_root, max_workers=1)
    finally:
        root_logger.handlers[:] = saved_handlers
        root_logger.setLevel(saved_level)

    # §1.4 rendezvous: exactly one ErrorEmitted carrying the raised gate
    # error; the raise closes the stream (no terminal GenerationCompleted).
    assert isinstance(raised_fail, LHPError)
    emitted = [e for e in events_fail if isinstance(e, ErrorEmitted)]
    assert len(emitted) == 1
    assert emitted[0].lhp_error is raised_fail
    assert not [e for e in events_fail if isinstance(e, GenerationCompleted)]
    # The failure delta surfaced per pipeline BEFORE the gate raised.
    assert [e.pipeline for e in events_fail if isinstance(e, PipelineFailed)] == [
        "gold_load"
    ]

    # Commit-gate contract: gate precedes commit and the wipe is lazy inside
    # the commit generator — a failed run leaves the tree byte-identical.
    assert _tree_digest(output_dir) == before
