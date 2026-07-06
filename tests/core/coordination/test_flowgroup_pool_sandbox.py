"""Worker-side sandbox rewrite hooks: table renames through the flat engine.

Drives :meth:`ActionOrchestrator.generate_pipelines` /
:meth:`ActionOrchestrator.validate_pipelines` with a ``sandbox_plan`` against
a real on-disk project (construction idiom copied from
``test_sandbox_prepass``) and proves, through the REAL spawn pool
(``max_workers=2`` — the rename set pickles across the boundary):

* generate — committed output carries the renamed tables on BOTH sides
  (producer write targets and consumer reads), out-of-scope reads stay
  untouched, copied user modules are rewritten on disk, and the
  ``StopIteration.value`` warning channel carries the folded ``LHP-VAL-066``
  record for the variable-bound (unrewritable) read;
* validate — the cross-flowgroup barrier sees the REWRITTEN resolved set
  (structured pass only; no text pass, no ``LHP-VAL-066`` — v1 limitation);
* ``sandbox_plan=None`` — byte-identical legacy behavior.

:stability: provisional
"""

from pathlib import Path
from typing import Any, List, Tuple

import pytest
import yaml

from lhp.core.coordination.layers import build_facade_orchestrator
from lhp.models.processing import SandboxRunConfig, SandboxWarningRecord

ENV = "dev"
PIPELINE = "sbx_pipe"
ORDERS = "dev_catalog.bronze.orders"
ORDERS_RENAMED = "dev_catalog.bronze.alice__orders"
CUSTOMERS = "dev_catalog.bronze.customers"
CONSUMER_OUT_RENAMED = "dev_catalog.bronze.alice__consumer_out"

USER_MODULE = '''"""User transform reading the in-scope orders table."""


def read_orders(df, spark, parameters):
    renamable = spark.table("dev_catalog.bronze.orders")
    bound = "dev_catalog.bronze.orders"
    unrenamable = spark.read.table(bound)
    runtime_name = fetch_table_name()
    shimmed = spark.read.table(runtime_name)
    dynamic = spark.sql(build_query())
    return renamable.union(unrenamable).union(shimmed).union(dynamic)
'''


def _producer_flowgroup() -> dict:
    """In-scope producer: cloudfiles load -> tokenized streaming_table write."""
    return {
        "pipeline": PIPELINE,
        "flowgroup": "producer_fg",
        "actions": [
            {
                "name": "load_orders_raw",
                "type": "load",
                "target": "v_orders_raw",
                "source": {
                    "type": "cloudfiles",
                    "path": "${landing_path}/orders",
                    "format": "json",
                },
            },
            {
                "name": "write_orders",
                "type": "write",
                "source": "v_orders_raw",
                "write_target": {
                    "type": "streaming_table",
                    "catalog": "${catalog}",
                    "schema": "${bronze_schema}",
                    "table": "orders",
                    "create_table": True,
                },
            },
        ],
    }


def _consumer_flowgroup() -> dict:
    """Consumer: in-scope read, out-of-scope read, python transform, write."""
    return {
        "pipeline": PIPELINE,
        "flowgroup": "consumer_fg",
        "actions": [
            {
                "name": "load_orders",
                "type": "load",
                "target": "v_orders",
                "source": {
                    "type": "delta",
                    "catalog": "${catalog}",
                    "schema": "${bronze_schema}",
                    "table": "orders",
                },
            },
            {
                "name": "load_customers",
                "type": "load",
                "target": "v_customers",
                "source": {
                    "type": "delta",
                    "catalog": "${catalog}",
                    "schema": "${bronze_schema}",
                    "table": "customers",
                },
            },
            {
                "name": "enrich_orders",
                "type": "transform",
                "transform_type": "python",
                "source": "v_orders",
                "target": "v_enriched",
                "module_path": "py_functions/orders_reader.py",
                "function_name": "read_orders",
            },
            {
                "name": "write_consumer_out",
                "type": "write",
                "source": "v_enriched",
                "write_target": {
                    "type": "streaming_table",
                    "catalog": "${catalog}",
                    "schema": "${bronze_schema}",
                    "table": "consumer_out",
                    "create_table": True,
                },
            },
        ],
    }


def _build_project(tmp_path: Path) -> Path:
    """Build an on-disk project (idiom from test_sandbox_prepass)."""
    project_root = tmp_path / "project"
    (project_root / "presets").mkdir(parents=True)
    (project_root / "templates").mkdir()
    (project_root / "substitutions").mkdir()
    substitutions = {
        ENV: {
            "catalog": "dev_catalog",
            "bronze_schema": "bronze",
            "landing_path": "/mnt/dev/landing",
        }
    }
    with open(project_root / "substitutions" / f"{ENV}.yaml", "w") as f:
        yaml.dump(substitutions, f)
    (project_root / "py_functions").mkdir()
    (project_root / "py_functions" / "orders_reader.py").write_text(USER_MODULE)
    for spec in (_producer_flowgroup(), _consumer_flowgroup()):
        pipeline_dir = project_root / "pipelines" / spec["pipeline"]
        pipeline_dir.mkdir(parents=True, exist_ok=True)
        with open(pipeline_dir / f"{spec['flowgroup']}.yaml", "w") as f:
            yaml.dump(spec, f)
    return project_root


def _build_plan(orchestrator, project_root: Path):
    """The E8 seam: scope = the one pipeline, pattern {namespace}__{table}."""
    run = SandboxRunConfig(
        namespace="alice",
        table_pattern="{namespace}__{table}",
        strategy="table",
        pipelines=(PIPELINE,),
    )
    discovered = list(orchestrator.bootstrap.discover_all_flowgroups())
    return orchestrator.build_sandbox_rewrite_plan(ENV, run, discovered)


def _drain(gen) -> Tuple[List[Any], Any]:
    """Fully drain a generator, capturing ``StopIteration.value``."""
    items: List[Any] = []
    while True:
        try:
            items.append(next(gen))
        except StopIteration as stop:
            return items, stop.value


def _generate(orchestrator, output_dir: Path, **kwargs) -> Tuple[List[Any], Any]:
    stream = orchestrator.generate_pipelines(
        pipeline_fields=[PIPELINE],
        env=ENV,
        output_dir=output_dir,
        max_workers=2,
        **kwargs,
    )
    return _drain(stream)


@pytest.mark.integration
def test_generate_with_sandbox_plan_rewrites_committed_output(tmp_path):
    """(a) Committed output carries the D7 renames; VAL_066 rides the channel.

    ``max_workers=2`` runs the REAL spawn pool, so this is also the
    spawn-boundary smoke: ``SandboxTableRenames`` pickles onto the worker
    state and back-channels its warnings through ``StopIteration.value``.
    """
    project_root = _build_project(tmp_path)
    orchestrator = build_facade_orchestrator(project_root, enforce_version=False)
    plan = _build_plan(orchestrator, project_root)
    output_dir = tmp_path / "generated"

    deltas, warnings = _generate(orchestrator, output_dir, sandbox_plan=plan)

    assert [d.pipeline_name for d in deltas] == [PIPELINE]
    assert all(d.success for d in deltas)
    producer_text = (output_dir / PIPELINE / "producer_fg.py").read_text()
    consumer_text = (output_dir / PIPELINE / "consumer_fg.py").read_text()

    # Producer WRITE side renamed; the original spelling is gone.
    assert ORDERS_RENAMED in producer_text
    assert ORDERS not in producer_text
    # Consumer READ side (structured delta load) renamed too.
    assert ORDERS_RENAMED in consumer_text
    assert ORDERS not in consumer_text
    # The consumer's own write target is in scope as well.
    assert CONSUMER_OUT_RENAMED in consumer_text
    # Out-of-scope read untouched.
    assert CUSTOMERS in consumer_text

    # Copied user module rewritten ON DISK: the literal read renamed, the
    # variable-bound read left alone (it is the LHP-VAL-066 site).
    module_files = list(output_dir.rglob("orders_reader.py"))
    assert len(module_files) == 1
    module_text = module_files[0].read_text()
    assert f'spark.table("{ORDERS_RENAMED}")' in module_text
    assert f'bound = "{ORDERS}"' in module_text
    assert "spark.read.table(bound)" in module_text

    # The unrewritable read folded into ONE LHP-VAL-066 record per file,
    # transported on the merge chain to StopIteration.value.
    val_066 = [w for w in warnings if w.code == "LHP-VAL-066"]
    assert len(val_066) == 1
    record = val_066[0]
    assert isinstance(record, SandboxWarningRecord)
    assert record.flowgroup == "consumer_fg"
    assert record.file is not None and record.file.name == "orders_reader.py"
    assert ORDERS in record.message
    assert "table_read" in record.message


@pytest.mark.integration
def test_generate_with_sandbox_plan_shims_and_advises(tmp_path):
    """Opaque reads are shim-wrapped on disk; opaque spark.sql rides VAL-067.

    Same REAL spawn pool as the VAL-066 smoke: the mixed rewriter warning
    stream (UnrewritableTableRead + UnverifiableSqlRead) pickles back and folds
    per-file into the two distinct codes.
    """
    project_root = _build_project(tmp_path)
    orchestrator = build_facade_orchestrator(project_root, enforce_version=False)
    plan = _build_plan(orchestrator, project_root)
    output_dir = tmp_path / "generated"

    deltas, warnings = _generate(orchestrator, output_dir, sandbox_plan=plan)
    assert all(d.success for d in deltas)

    module_text = next(output_dir.rglob("orders_reader.py")).read_text()
    # Opaque runtime read wrapped in the shim; ONE helper for the module.
    assert "spark.read.table(__lhp_sandbox_table(runtime_name))" in module_text
    assert module_text.count("def __lhp_sandbox_table(") == 1
    # Opaque spark.sql is SQL text, not a name argument — never wrapped.
    assert "spark.sql(build_query())" in module_text
    # The variable-bound in-scope read is still the untouched VAL-066 site.
    assert "spark.read.table(bound)" in module_text

    # Opaque spark.sql folds into ONE LHP-VAL-067 advisory for the file; the
    # VAL-066 site rides alongside it (distinct (code, file) keys).
    val_067 = [w for w in warnings if w.code == "LHP-VAL-067"]
    assert len(val_067) == 1
    record = val_067[0]
    assert isinstance(record, SandboxWarningRecord)
    assert record.flowgroup == "consumer_fg"
    assert record.file is not None and record.file.name == "orders_reader.py"
    assert "runtime" in record.message
    assert any(w.code == "LHP-VAL-066" for w in warnings)


@pytest.mark.integration
def test_validate_with_sandbox_plan_resolves_rewritten_set(tmp_path, monkeypatch):
    """(b) Validate mode: the cross-flowgroup barrier sees sandbox names.

    The barrier runs on the coordinator over the workers' RESOLVED
    flowgroups, so spying on ``validate_cross_flowgroup`` observes exactly
    the resolved set the rewrite hooks produced.
    """
    project_root = _build_project(tmp_path)
    orchestrator = build_facade_orchestrator(project_root, enforce_version=False)
    plan = _build_plan(orchestrator, project_root)

    seen: List[Any] = []
    original = orchestrator.validation.validate_cross_flowgroup

    def spy(flowgroups, **kwargs):
        seen.extend(flowgroups)
        return original(flowgroups, **kwargs)

    monkeypatch.setattr(orchestrator.validation, "validate_cross_flowgroup", spy)

    outcomes, warnings = _drain(
        orchestrator.validate_pipelines(
            pipeline_fields=[PIPELINE],
            env=ENV,
            max_workers=2,
            sandbox_plan=plan,
        )
    )

    assert [o.pipeline for o in outcomes] == [PIPELINE]
    assert all(o.success for o in outcomes)
    assert len(seen) == 2  # producer_fg + consumer_fg, resolved.
    # Catalog/schema/table ride as SEPARATE model fields, so assert on the
    # quoted table LEAF in each resolved flowgroup's dump.
    dumped = [str(fg.model_dump()) for fg in seen]
    # Write side and structured read side both carry the sandbox name.
    assert sum(text.count("'alice__orders'") for text in dumped) == 2
    assert not any("'orders'" in text for text in dumped)
    # Out-of-scope read untouched in the resolved set.
    assert any("'customers'" in text for text in dumped)
    # Structured pass only: validate never emits LHP-VAL-066 (v1 limitation).
    assert not any(w.code == "LHP-VAL-066" for w in warnings)


@pytest.mark.integration
def test_generate_without_sandbox_plan_is_legacy_identical(tmp_path):
    """(c) ``sandbox_plan=None`` is byte-identical to omitting the kwarg."""
    project_root = _build_project(tmp_path)
    orchestrator = build_facade_orchestrator(project_root, enforce_version=False)

    out_default = tmp_path / "out_default"
    out_none = tmp_path / "out_none"
    _generate(orchestrator, out_default)
    _generate(orchestrator, out_none, sandbox_plan=None)

    files_default = sorted(
        p.relative_to(out_default) for p in out_default.rglob("*.py")
    )
    files_none = sorted(p.relative_to(out_none) for p in out_none.rglob("*.py"))
    assert files_default == files_none
    for rel in files_default:
        assert (out_default / rel).read_bytes() == (out_none / rel).read_bytes()
    # And no sandbox spelling anywhere on the legacy path.
    for rel in files_default:
        assert "alice__" not in (out_default / rel).read_text()
