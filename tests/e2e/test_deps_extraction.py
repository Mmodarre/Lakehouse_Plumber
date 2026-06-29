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
        incoming_to_mv = [e for e in edges if e[1] == "e2e_orders_summary_mv"]

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
        * ``opaque_read_flow`` — a read routed through a helper call; the
          analyzer does not follow helper calls, so it emits exactly one
          LHP-DEP-002 warning with stamped context instead of a silent
          missing edge.
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

        # (iii) opaque helper-routed read → exactly one stamped LHP-DEP-002.
        warnings = data["warnings"]
        assert data["metadata"]["total_warnings"] == len(warnings)
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
