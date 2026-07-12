"""
End-to-end tests for dependency extraction from externalized SQL/Python inside
``write_target``.

These tests exist to prevent a regression of the bug where ``lhp dag`` under-
reported dependencies for materialized views (``write_target.sql_path``),
custom sinks (``write_target.module_path``), and ForEachBatch handlers
(``write_target.batch_handler``). See the V0.8.6 changelog entry.

Each test gets an isolated deep copy of the ``testing_project`` fixture and
may add per-test flowgroups/files before running ``lhp dag``. The full
fixture project is left undisturbed between tests.
"""

import json
import os
import shutil
import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestDepsExtraction:
    """E2E tests validating lhp dag extraction from write_target bodies."""

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        """Set up fresh test project for each test method."""
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        yield

        os.chdir(self.original_cwd)

    def run_dag_command(self, *args) -> tuple:
        """Run ``lhp dag`` and return (exit_code, output)."""
        runner = CliRunner()
        result = runner.invoke(cli, ["dag", *args])
        return result.exit_code, result.output

    def _load_json_output(self) -> dict:
        """Load ``.lhp/dependencies/pipeline_dependencies.json`` from the project."""
        json_path = (
            self.project_root / ".lhp" / "dependencies" / "pipeline_dependencies.json"
        )
        assert json_path.exists(), f"Expected JSON output at {json_path}"
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _load_flowgroup_dot(self) -> str:
        """Load the generated flowgroup dependency graph in DOT format."""
        dot_path = (
            self.project_root / ".lhp" / "dependencies" / "flowgroup_dependencies.dot"
        )
        assert dot_path.exists(), f"Expected DOT output at {dot_path}"
        return dot_path.read_text(encoding="utf-8")

    def test_deps_extracts_write_target_sql_path_upstream(self):
        """Materialized-view SQL in write_target.sql_path must contribute to
        external_sources in pipeline_dependencies.json.

        The fixture includes ``04_gold/order_summary_mv.yaml`` with
        ``write_target.sql_path: sql/gold/order_summary.sql`` — that SQL
        reads ``acme_edw_dev.edw_silver.orders``. Before the V0.8.6 fix,
        the analyzer skipped ``write_target.sql_path`` entirely and the
        upstream silver table never surfaced.
        """
        exit_code, output = self.run_dag_command("--format", "json")
        assert exit_code == 0, f"lhp dag failed: {output}"

        data = self._load_json_output()
        gold = data["pipelines"].get("gold_load")
        assert gold is not None, "gold_load pipeline missing from deps output"

        external_sources = set(gold["external_sources"])
        assert "acme_edw_dev.edw_silver.orders" in external_sources, (
            "Expected silver.orders to surface as an external source from "
            "order_summary_mv.yaml's write_target.sql_path. "
            f"Actual external_sources: {external_sources}"
        )

    def test_deps_extracts_write_target_python_upstream(self):
        """Custom-sink Python in write_target.module_path must be parsed for
        table references."""
        # Drop a custom-sink flowgroup that uses write_target.module_path.
        pipeline_dir = self.project_root / "pipelines" / "04_gold"
        sinks_dir = self.project_root / "sinks"
        sinks_dir.mkdir(parents=True, exist_ok=True)

        sink_py = sinks_dir / "custom_audit_sink.py"
        sink_py.write_text(
            textwrap.dedent('''\
                """Custom audit sink that references an upstream silver table."""

                def write(df, epoch_id):
                    audit = spark.read.table("acme_edw_dev.edw_silver.customer_dim")
                    return df.join(audit, "customer_key", "left")
                '''),
            encoding="utf-8",
        )

        yaml = pipeline_dir / "audit_sink_flowgroup.yaml"
        yaml.write_text(
            textwrap.dedent("""\
                pipeline: gold_load
                flowgroup: audit_sink_flow
                actions:
                  - name: load_sink_input
                    type: load
                    readMode: batch
                    source:
                      type: delta
                      database: "{catalog}.{silver_schema}"
                      table: customer_dim
                    target: v_audit_sink_input
                  - name: write_audit_sink
                    type: write
                    source: v_audit_sink_input
                    write_target:
                      type: custom
                      sink_type: custom
                      sink_name: audit_sink
                      module_path: sinks/custom_audit_sink.py
                      custom_sink_class: AuditSink
                """),
            encoding="utf-8",
        )

        exit_code, output = self.run_dag_command("--format", "json")
        assert exit_code == 0, f"lhp dag failed: {output}"

        data = self._load_json_output()
        gold = data["pipelines"].get("gold_load")
        assert gold is not None

        external_sources = set(gold["external_sources"])
        assert "acme_edw_dev.edw_silver.customer_dim" in external_sources, (
            "Expected parser to extract silver.customer_dim from sinks/custom_audit_sink.py. "
            f"Actual: {external_sources}"
        )

    def test_deps_flowgroup_graph_has_incoming_edges_on_mv_flowgroup(self):
        """The exact invariant the BWH bug violated: a flowgroup whose sole
        body is externalized via ``write_target.sql_path`` should have ≥1
        incoming edge in the flowgroup DOT graph once an upstream silver
        flowgroup exists.

        Added here so CI catches any future regression where the analyzer
        silently stops parsing write_target bodies. The test wires up:
          * a silver flowgroup that writes ``{catalog}.{silver_schema}.e2e_orders``
          * a gold flowgroup with ``write_target.sql_path`` whose SQL reads that
            exact token-qualified table.

        Using the token form matches how real projects write SQL — the
        dependency analyzer doesn't apply runtime substitution, so tokens
        on both sides let the edge land.
        """
        silver_dir = self.project_root / "pipelines" / "03_silver" / "dim"
        silver_yaml = silver_dir / "e2e_orders_silver.yaml"
        silver_yaml.write_text(
            textwrap.dedent("""\
                pipeline: acmi_edw_silver
                flowgroup: e2e_orders_silver
                presets:
                  - default_delta_properties
                actions:
                  - name: load_bronze_orders_for_e2e
                    type: load
                    readMode: stream
                    source:
                      type: delta
                      database: "{catalog}.{bronze_schema}"
                      table: orders
                    target: v_e2e_orders_bronze_src
                  - name: write_e2e_orders_silver
                    type: write
                    source: v_e2e_orders_bronze_src
                    write_target:
                      type: streaming_table
                      database: "{catalog}.{silver_schema}"
                      table: e2e_orders
                """),
            encoding="utf-8",
        )

        gold_dir = self.project_root / "pipelines" / "04_gold"
        sql_dir = self.project_root / "sql" / "gold"
        sql_dir.mkdir(parents=True, exist_ok=True)
        sql_file = sql_dir / "e2e_orders_summary.sql"
        sql_file.write_text(
            "SELECT order_year, COUNT(*) AS n\n"
            "FROM {catalog}.{silver_schema}.e2e_orders\n"
            "GROUP BY order_year\n",
            encoding="utf-8",
        )

        gold_yaml = gold_dir / "e2e_orders_summary_mv.yaml"
        gold_yaml.write_text(
            textwrap.dedent("""\
                pipeline: gold_load
                flowgroup: e2e_orders_summary_mv
                actions:
                  - name: write_e2e_orders_summary_mv
                    type: write
                    write_target:
                      type: materialized_view
                      database: "{catalog}.{gold_schema}"
                      table: e2e_orders_summary_mv
                      sql_path: "sql/gold/e2e_orders_summary.sql"
                """),
            encoding="utf-8",
        )

        exit_code, output = self.run_dag_command()
        assert exit_code == 0, f"lhp dag failed: {output}"

        dot = self._load_flowgroup_dot()

        # Edges are serialized as:   "src" -> "dst";
        import re

        edges = re.findall(r'"([^"]+)"\s*->\s*"([^"]+)"', dot)
        # Flowgroup graph node ids are pipeline-qualified (``pipeline.flowgroup``),
        # so the mv flowgroup surfaces as ``gold_load.e2e_orders_summary_mv``.
        incoming_to_mv = [e for e in edges if e[1] == "gold_load.e2e_orders_summary_mv"]

        assert len(incoming_to_mv) >= 1, (
            "Regression sentinel: e2e_orders_summary_mv must have ≥1 incoming "
            "edge in flowgroup_dependencies.dot once the upstream silver "
            "flowgroup produces {catalog}.{silver_schema}.e2e_orders. "
            f"Observed edges into this flowgroup: {incoming_to_mv}"
        )

    def test_deps_union_of_python_parse_and_explicit_source(self):
        """Python actions: parser output is UNIONED with explicit source:,
        not replaced. This is the Python-specific escape hatch the V0.8.6
        fix introduces.

        We wire up a Python transform whose parsed sources yield one table,
        while its explicit ``source:`` list names a different table. Both
        should appear in the pipeline's external_sources.

        The parser recognizes ``spark.read.table(...)`` but not
        ``spark_session.read.table(...)``, so the Python code uses the
        literal ``spark`` name.
        """
        py_funcs_dir = self.project_root / "py_functions"
        py_funcs_dir.mkdir(parents=True, exist_ok=True)
        py_file = py_funcs_dir / "union_test_transform.py"
        py_file.write_text(
            textwrap.dedent('''\
                """Python transform whose parser-visible source is distinct from the
                declared explicit source."""

                def transform_union_test(spark):
                    parser_seen = spark.read.table("acme_edw_dev.edw_silver.parser_seen_table")
                    return parser_seen
                '''),
            encoding="utf-8",
        )

        pipeline_dir = self.project_root / "pipelines" / "04_gold"
        yaml = pipeline_dir / "union_test_flowgroup.yaml"
        yaml.write_text(
            textwrap.dedent("""\
                pipeline: gold_load
                flowgroup: union_test_flow
                actions:
                  - name: explicit_source_load
                    type: load
                    readMode: batch
                    source:
                      type: delta
                      database: "{catalog}.{silver_schema}"
                      table: customer_dim
                    target: v_explicit_src_customer_dim
                  - name: transform_union
                    type: transform
                    transform_type: python
                    source:
                      - "acme_edw_dev.edw_silver.explicit_declared_table"
                    module_path: py_functions/union_test_transform.py
                    function_name: transform_union_test
                    target: v_union_result
                  - name: write_union_output
                    type: write
                    source: v_union_result
                    write_target:
                      type: streaming_table
                      database: "{catalog}.{gold_schema}"
                      table: union_test_output
                """),
            encoding="utf-8",
        )

        exit_code, output = self.run_dag_command("--format", "json")
        assert exit_code == 0, f"lhp dag failed: {output}"

        data = self._load_json_output()
        gold = data["pipelines"].get("gold_load")
        assert gold is not None

        external_sources = set(gold["external_sources"])

        # Parser-visible table must surface even though explicit source: is set.
        assert "acme_edw_dev.edw_silver.parser_seen_table" in external_sources, (
            "Parser-visible table missing; union semantics regressed. "
            f"external_sources: {external_sources}"
        )
        # Explicit source must ALSO surface — union, not replace.
        assert "acme_edw_dev.edw_silver.explicit_declared_table" in external_sources, (
            "Explicit source dropped; union semantics regressed. "
            f"external_sources: {external_sources}"
        )

    def test_deps_sql_mid_segment_token_read_round_trips_byte_exactly(self):
        """A SQL read whose table name carries a MID-SEGMENT substitution token
        (``...orders${order_suffix}``) must surface with its token bytes intact.

        This pins the byte-fidelity contract the legacy regex parser broke: it
        silently truncated ``FROM cat.sch.tbl${suffix}`` to ``cat.sch.tbl``, so
        suffixed readers could never match suffix-carrying producers.
        """
        sql_dir = self.project_root / "sql" / "gold"
        sql_dir.mkdir(parents=True, exist_ok=True)
        sql_file = sql_dir / "e2e_suffixed_read.sql"
        sql_file.write_text(
            "SELECT order_year, COUNT(*) AS n\n"
            "FROM {catalog}.{silver_schema}.orders${order_suffix}\n"
            "GROUP BY order_year\n",
            encoding="utf-8",
        )

        gold_yaml = self.project_root / "pipelines" / "04_gold" / "e2e_suffixed_mv.yaml"
        gold_yaml.write_text(
            textwrap.dedent("""\
                pipeline: gold_load
                flowgroup: e2e_suffixed_mv
                actions:
                  - name: write_e2e_suffixed_mv
                    type: write
                    write_target:
                      type: materialized_view
                      database: "{catalog}.{gold_schema}"
                      table: e2e_suffixed_mv
                      sql_path: "sql/gold/e2e_suffixed_read.sql"
                """),
            encoding="utf-8",
        )

        exit_code, output = self.run_dag_command("--format", "json")
        assert exit_code == 0, f"lhp dag failed: {output}"

        data = self._load_json_output()
        gold = data["pipelines"].get("gold_load")
        assert gold is not None

        external_sources = set(gold["external_sources"])
        assert "{catalog}.{silver_schema}.orders${order_suffix}" in external_sources, (
            "Mid-segment token bytes must round-trip exactly through SQL "
            f"extraction. Actual external_sources: {external_sources}"
        )
        # The truncated form must NOT appear — that was the legacy bug.
        assert "{catalog}.{silver_schema}.orders" not in external_sources, (
            "Truncated table name resurfaced — token masking regressed"
        )

    def test_deps_parameter_bound_reads_and_extraction_warnings(self):
        """The shared fixture pipeline ``19_dependency_bindings`` pins the
        dependency-extraction overhaul end-to-end:

        * ``param_snapshot_flow`` — a snapshot_cdc ``source_function`` whose
          keyword-only ``source_table`` parameter carries ``${token}`` bytes
          naming a table the ``helper_imports`` pipeline produces with the
          same spelling → INTERNAL pipeline edge, no ``depends_on`` needed.
        * ``configured_union_flow`` — a python transform looping over
          ``parameters["tables"]``; static loop unrolling yields one read per
          element, token bytes preserved exactly.
        * ``opaque_read_flow`` — a read whose table name only exists in a
          runtime environment variable (genuinely dynamic — the analyzer
          now follows helper calls and parameter bindings, so the fixture
          must be beyond static reach); it emits exactly one aggregated
          LHP-DEP-002 site record with stamped context and the depends_on
          edit path instead of a silent missing edge.
        """
        exit_code, output = self.run_dag_command("--format", "json")
        assert exit_code == 0, f"lhp dag failed: {output}"

        data = self._load_json_output()
        dep = data["pipelines"].get("dep_bindings")
        assert dep is not None, "dep_bindings pipeline missing from deps output"

        # (i) kwonly snapshot parameter forms an internal edge: the producing
        # pipeline appears in depends_on and the token-qualified table does
        # NOT leak into external_sources.
        assert "helper_imports" in set(dep["depends_on"]), (
            "Parameter-bound snapshot read failed to form the internal edge "
            f"to helper_imports. depends_on: {dep['depends_on']}"
        )
        external_sources = set(dep["external_sources"])
        assert (
            "${catalog}.${bronze_schema}.helper_snapshot_dim" not in external_sources
        ), "Internally-matched snapshot table leaked into external_sources"

        # (ii) loop unrolling: one read per parameters["tables"] element,
        # token bytes byte-exact.
        assert "${catalog}.${bronze_schema}.dep_loop_alpha" in external_sources, (
            f"Loop element alpha missing. external_sources: {external_sources}"
        )
        assert "${catalog}.${bronze_schema}.dep_loop_beta" in external_sources, (
            f"Loop element beta missing. external_sources: {external_sources}"
        )

        # (iii) genuinely-dynamic read → exactly one aggregated LHP-DEP-002
        # site record with the enriched message and actionable fields.
        warnings = data["warnings"]
        assert data["metadata"]["total_warnings"] == len(warnings)
        assert data["metadata"]["total_warning_occurrences"] == sum(
            w["affected_count"] for w in warnings
        )
        opaque = [
            w
            for w in warnings
            if w["code"] == "LHP-DEP-002" and w["flowgroup"] == "opaque_read_flow"
        ]
        assert len(opaque) == 1, (
            f"Expected exactly one DEP-002 for opaque_read_flow, got: {opaque}"
        )
        warning = opaque[0]
        assert warning["action"] == "opaque_union_lookup"
        assert warning["file_path"], "warning must carry the originating file"
        assert "\\" not in warning["file_path"], (
            "warning file_path must use POSIX separators in JSON output"
        )
        assert warning["file_path"].endswith(
            "py_functions/dep_bindings_opaque_transform.py"
        )
        assert isinstance(warning["line"], int)
        assert "depends_on" in warning["suggestion"]
        # Enriched message names the unresolved argument expression.
        assert "os.environ['DEP_BINDINGS_LOOKUP_TABLE']" in warning["message"]
        # Site aggregation fields: one affected action, pointing at the
        # flowgroup YAML as the place to add depends_on.
        assert warning["affected_count"] == 1
        assert [(a["flowgroup"], a["action"]) for a in warning["affected_actions"]] == [
            ("opaque_read_flow", "opaque_union_lookup")
        ]
        assert warning["edit_yaml_path"].endswith(
            "pipelines/19_dependency_bindings/opaque_read_flow.yaml"
        )

    def test_deps_interprocedural_resolution_zero_warnings(self):
        """Cases A/B/C from the customer gap analysis, end-to-end on disk:
        kwonly-seeded snapshot functions with nested closures, return-value
        folding and ``dict.fromkeys`` loops (A); module-level sibling callees
        with positional args (B); ``parameters or {}`` coercion chains with
        helper calls inside f-strings (C). Every read resolves statically —
        the tables surface and NO LHP-DEP-002 is emitted for these flows.
        """
        py_dir = self.project_root / "py_functions"
        flow_dir = self.project_root / "pipelines" / "20_dep_cases"
        flow_dir.mkdir(parents=True, exist_ok=True)

        (py_dir / "case_a_snapshot_func.py").write_text(
            textwrap.dedent('''\
                from typing import Optional, Tuple
                from pyspark.sql import DataFrame


                def next_case_a_snapshot(
                    latest_snapshot_version: Optional[int],
                    *,
                    catalog: str,
                    schema: str,
                ) -> Optional[Tuple[DataFrame, int]]:
                    """Kwonly-seeded reader with closures + folded candidates."""

                    def _table_exists(fqn):
                        return spark.catalog.tableExists(fqn)

                    def _resolve_stg_fqn():
                        return f"{catalog}.{schema}_stg.case_a_orders"

                    stg_table_fqn = _resolve_stg_fqn()
                    if latest_snapshot_version is None and _table_exists(stg_table_fqn):
                        return (spark.read.table(stg_table_fqn), 1)
                    candidates = [
                        f"{catalog}.{schema}.case_a_t1",
                        f"{catalog}.{schema}.case_a_t2",
                    ]
                    for fqn in dict.fromkeys(candidates):
                        if _table_exists(fqn):
                            return (spark.read.table(fqn), 1)
                    return None
                '''),
            encoding="utf-8",
        )
        (flow_dir / "case_a_snapshot_flow.yaml").write_text(
            textwrap.dedent("""\
                pipeline: dep_cases
                flowgroup: case_a_snapshot_flow
                actions:
                  - name: load_case_a_seed
                    type: load
                    readMode: stream
                    source:
                      type: delta
                      database: "${catalog}.${raw_schema}"
                      table: dep_bindings_seed
                    target: v_case_a_seed
                  - name: write_case_a_snapshot
                    type: write
                    source: v_case_a_seed
                    write_target:
                      type: streaming_table
                      database: "${catalog}.${silver_schema}"
                      table: "case_a_dim"
                      mode: "snapshot_cdc"
                      snapshot_cdc_config:
                        source_function:
                          file: "py_functions/case_a_snapshot_func.py"
                          function: "next_case_a_snapshot"
                          parameters:
                            catalog: "acme_edw_dev"
                            schema: "edw_case"
                        keys: ["id"]
                        stored_as_scd_type: 1
                """),
            encoding="utf-8",
        )

        (py_dir / "case_b_deid_transform.py").write_text(
            textwrap.dedent('''\
                from pyspark.sql import DataFrame


                def _deid(df: DataFrame, table_name):
                    ref = spark.read.table(table_name)
                    return df


                def transform_case_b(df: DataFrame, spark, parameters) -> DataFrame:
                    """Sibling callee receives the table name positionally."""
                    return _deid(df, "acme_edw_dev.edw_ref.case_b_patients")
                '''),
            encoding="utf-8",
        )
        (py_dir / "case_c_sites_transform.py").write_text(
            textwrap.dedent('''\
                from pyspark.sql import DataFrame


                def _schema_for(site):
                    return f"edw_{site}"


                def _coerce_sites(raw):
                    return list(raw)


                def transform_case_c(df: DataFrame, spark, parameters) -> DataFrame:
                    """`parameters or {}` + coercion + f-string helper call."""
                    p = parameters or {}
                    sites = _coerce_sites(p["sites"])
                    for site in sites:
                        lookup = spark.read.table(
                            f"acme_edw_dev.{_schema_for(site)}.case_c_orders"
                        )
                        df = df.unionByName(lookup, allowMissingColumns=True)
                    return df
                '''),
            encoding="utf-8",
        )
        (flow_dir / "case_bc_transforms.yaml").write_text(
            textwrap.dedent("""\
                pipeline: dep_cases
                flowgroup: case_bc_transform_flow
                actions:
                  - name: load_case_bc_seed
                    type: load
                    readMode: stream
                    source:
                      type: delta
                      database: "${catalog}.${raw_schema}"
                      table: dep_bindings_seed
                    target: v_case_bc_seed
                  - name: transform_case_b
                    type: transform
                    transform_type: python
                    source: v_case_bc_seed
                    module_path: "py_functions/case_b_deid_transform.py"
                    function_name: "transform_case_b"
                    readMode: stream
                    parameters:
                      unused: "x"
                    target: v_case_b_out
                  - name: transform_case_c
                    type: transform
                    transform_type: python
                    source: v_case_b_out
                    module_path: "py_functions/case_c_sites_transform.py"
                    function_name: "transform_case_c"
                    readMode: stream
                    parameters:
                      sites:
                        - "syd"
                        - "mel"
                    target: v_case_c_out
                  - name: write_case_bc
                    type: write
                    source: v_case_c_out
                    write_target:
                      type: streaming_table
                      database: "${catalog}.${silver_schema}"
                      table: "case_bc_out"
                """),
            encoding="utf-8",
        )

        exit_code, output = self.run_dag_command("--format", "json")
        assert exit_code == 0, f"lhp dag failed: {output}"

        data = self._load_json_output()
        cases = data["pipelines"].get("dep_cases")
        assert cases is not None, "dep_cases pipeline missing from deps output"

        external_sources = set(cases["external_sources"])
        expected = {
            # Case A: return folding + closure param + dict.fromkeys loop.
            "acme_edw_dev.edw_case_stg.case_a_orders",
            "acme_edw_dev.edw_case.case_a_t1",
            "acme_edw_dev.edw_case.case_a_t2",
            # Case B: positional arg to a sibling callee.
            "acme_edw_dev.edw_ref.case_b_patients",
            # Case C: parameters-or-{} + coerced loop + f-string helper call.
            "acme_edw_dev.edw_syd.case_c_orders",
            "acme_edw_dev.edw_mel.case_c_orders",
        }
        missing = expected - external_sources
        assert not missing, (
            f"Statically-resolvable reads failed to surface: {missing}. "
            f"external_sources: {external_sources}"
        )

        # Everything resolved — no advisory may reference these flowgroups.
        case_warnings = [
            w
            for w in data["warnings"]
            if any(
                a["flowgroup"].startswith("case_")
                for a in (w["affected_actions"] or [w])
            )
            or w["flowgroup"].startswith("case_")
        ]
        assert case_warnings == [], (
            f"Cases A/B/C must resolve with zero warnings, got: {case_warnings}"
        )

    def test_deps_blueprint_pipeline_identity_full_expansion(self):
        """REGRESSION (dedup-loss): a blueprint that parameterizes
        ``pipeline:`` per instance must contribute EVERY instance's pipeline
        to the DAG — the old default view collapsed synthetics to one
        representative per spec and silently dropped all but the first
        instance's pipeline from the JSON and the job orchestration YAML.
        """
        blueprint = self.project_root / "blueprints" / "bp11_per_site.yaml"
        blueprint.write_text(
            textwrap.dedent("""\
                name: bp11_per_site
                version: "1.0"
                description: "Per-site pipeline identity (dedup-loss regression)."

                parameters:
                  - name: site_name
                    required: true
                    description: "Site identifier; becomes part of the pipeline name."

                flowgroups:
                  - pipeline: "bp11_%{site_name}_pipeline"
                    flowgroup: "%{site_name}_bp11_ingest"
                    actions:
                      - name: load_%{site_name}_bp11_seed
                        type: load
                        readMode: stream
                        source:
                          type: delta
                          database: "${catalog}.${raw_schema}"
                          table: dep_bindings_seed
                        target: "v_%{site_name}_bp11_seed"
                      - name: write_%{site_name}_bp11
                        type: write
                        source: "v_%{site_name}_bp11_seed"
                        write_target:
                          type: streaming_table
                          database: "${catalog}.${silver_schema}"
                          table: "bp11_%{site_name}_out"
                """),
            encoding="utf-8",
        )
        instance_dir = self.project_root / "pipelines" / "20_bp11_sites"
        instance_dir.mkdir(parents=True, exist_ok=True)
        for site in ("site_a", "site_b"):
            (instance_dir / f"{site}.yaml").write_text(
                textwrap.dedent(f"""\
                    use_blueprint: bp11_per_site
                    parameters:
                      site_name: {site}
                    """),
                encoding="utf-8",
            )

        exit_code, output = self.run_dag_command("--format", "json,job")
        assert exit_code == 0, f"lhp dag failed: {output}"

        data = self._load_json_output()
        pipelines = set(data["pipelines"])
        assert {"bp11_site_a_pipeline", "bp11_site_b_pipeline"} <= pipelines, (
            "Both per-site pipelines must appear in the DAG JSON. "
            f"Got: {sorted(p for p in pipelines if p.startswith('bp11'))}"
        )

        deps_dir = self.project_root / ".lhp" / "dependencies"
        job_files = list(deps_dir.rglob("*.job.yml"))
        assert job_files, f"Expected job YAML under {deps_dir}"
        job_text = "\n".join(f.read_text(encoding="utf-8") for f in job_files)
        for pipeline in ("bp11_site_a_pipeline", "bp11_site_b_pipeline"):
            assert pipeline in job_text, (
                f"{pipeline} missing from the job orchestration YAML — "
                "blueprint instances were dropped from job output"
            )
