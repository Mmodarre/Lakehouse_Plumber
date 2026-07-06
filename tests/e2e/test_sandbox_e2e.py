"""End-to-end tests for developer sandbox mode (`lhp generate --sandbox`).

Scenario: the personal ``.lhp/profile.yaml`` scopes the run to the
``acmi_edw_bronze`` pipeline (exact entry) plus the ``acmi_edw_silv*`` glob
(expands to ``acmi_edw_silver``), namespace ``alice``. The team policy in
``lhp.yaml`` states the default ``{namespace}_{table}`` pattern explicitly,
and the project-level ``event_log:`` block is uncommented so the bundle sync
exercises the event-log name namespacing.

Verified behaviours:

* ``generated/dev/`` holds EXACTLY the two in-scope pipeline dirs.
* every generated file matches the hand-written
  ``generated_baseline_sandbox/dev`` baseline byte-for-byte: write targets
  and in-scope reads carry ``alice_<leaf>`` names, while reads of tables no
  in-scope pipeline produces (``edw_raw.*``, ``edw_old.*``,
  ``edw_bronze.bronze_sfcc_cust``, ``edw_bronze.nation``, quarantine DLQ
  tables) stay byte-identical.
* ``resources/lhp/`` holds EXACTLY the two scoped resource files, each
  matching ``resources_baseline_sandbox/lhp``, with the composed event-log
  table name namespaced (``alice_<pipeline>_event_log``).

Second scenario (``TestSandboxComplexE2E``): namespace ``bob`` with the
NON-DEFAULT SUFFIX pattern ``{table}_{namespace}`` and ``allowed_envs:
[dev]``. The profile scopes the run to exactly ten pipelines by exact name
(no globs): ``sample_python_func_pipeline``, ``helper_imports``,
``dep_bindings``, ``13_sinks``, ``custom_datasource``, ``15_python_load``,
``acme_edw_bp_bronze``, ``acme_edw_bp_silver``, ``acmi_edw_silver``,
``acmi_edw_modelled``. This scope exercises the complex carriers: Python
function transforms, Python helper-module closures, Python custom data
sources, Python loads, delta/kafka/foreachbatch sinks, blueprint-expanded
pipelines (``acme_edw_bp_raw`` is deliberately OUT of scope so the
blueprint-sibling read stays unrenamed), snapshot-CDC parameter bindings,
and inline-SQL bodies. ``dep_bindings`` additionally carries the dynamic-SQL
probes: an inline-SQL ``table_changes()`` CDF read, an f-string ``spark.sql``
rewrite, a name-bound container read (``LHP-VAL-066``), the runtime
table-read shim, and an opaque ``spark.sql`` advisory (``LHP-VAL-067``). The
event-log leaf is routed through the suffix pattern
(``<pipeline>_event_log_bob``), and rename semantics are probed one carrier
at a time against ``generated_baseline_sandbox_complex/dev`` (58 files) and
``resources_baseline_sandbox_complex/lhp`` (10 files).
"""

import hashlib
import os
import shutil
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli

# Scope: one exact pipeline name + one glob (expands to acmi_edw_silver only).
SANDBOX_PROFILE = """sandbox:
  namespace: alice
  pipelines:
    - acmi_edw_bronze
    - acmi_edw_silv*
"""

# Team policy appended to the copy's lhp.yaml: the DEFAULT pattern stated
# explicitly, plus allowed_envs so dev is positively sandbox-enabled.
SANDBOX_POLICY = """
sandbox:
  strategy: table
  table_pattern: "{namespace}_{table}"
  allowed_envs:
    - dev
"""


@pytest.mark.e2e
class TestSandboxE2E:
    """E2E tests for sandbox-mode generation against hand-written baselines."""

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"
        self.generated_baseline_dir = (
            self.project_root / "generated_baseline_sandbox" / "dev"
        )
        self.resources_baseline_dir = (
            self.project_root / "resources_baseline_sandbox" / "lhp"
        )

        self._init_bundle_project()
        self._arrange_sandbox()

        yield

        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        # Baselines are only used for comparison, never copied into working dirs.
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)

        self.generated_dir.mkdir(parents=True, exist_ok=True)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    def _arrange_sandbox(self):
        """Apply the sandbox additions to the isolated COPY (never the fixture).

        Writes the personal ``.lhp/profile.yaml``, appends the team
        ``sandbox:`` policy, and uncomments the project ``event_log:`` block
        in the copy's ``lhp.yaml`` (same uncomment as TestEventLogE2E).
        """
        lhp_dir = self.project_root / ".lhp"
        lhp_dir.mkdir()
        (lhp_dir / "profile.yaml").write_text(SANDBOX_PROFILE)

        lhp_yaml = self.project_root / "lhp.yaml"
        content = lhp_yaml.read_text()
        content = content.replace(
            '#event_log:\n#  catalog: "{catalog}"\n#  schema: _meta\n'
            '#  name_suffix: "_event_log"',
            'event_log:\n  catalog: "{catalog}"\n  schema: _meta\n'
            '  name_suffix: "_event_log"',
        )
        assert "\nevent_log:" in content, "event_log uncomment did not apply"
        lhp_yaml.write_text(content + SANDBOX_POLICY)

    def run_sandbox_generate(self) -> tuple:
        """Run `lhp generate --env dev --sandbox` with the project pipeline config."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--verbose",
                "generate",
                "--env",
                "dev",
                "--pipeline-config",
                "config/pipeline_config.yaml",
                "--sandbox",
            ],
        )
        return result.exit_code, result.output

    @staticmethod
    def _compare_file_hashes(file1: Path, file2: Path) -> str:
        """Compare two files by SHA-256. Returns '' if identical."""

        def get_hash(f):
            return hashlib.sha256(f.read_bytes()).hexdigest()

        h1, h2 = get_hash(file1), get_hash(file2)
        if h1 != h2:
            return (
                f"Hash mismatch: {file1.name} "
                f"(generated={h1[:12]}) != (baseline={h2[:12]})"
            )
        return ""

    def _compare_directory_hashes(
        self, generated_dir: Path, baseline_dir: Path
    ) -> list:
        """Compare directories recursively. Returns list of differences."""
        differences = []
        # Skip __pycache__ dirs and .pyc files: bytecode is not LHP-generated
        # output, so a stray .pyc in a baseline dir must not register as a diff.
        gen_files = {
            f.relative_to(generated_dir): f
            for f in generated_dir.rglob("*")
            if f.is_file() and "__pycache__" not in f.parts and f.suffix != ".pyc"
        }
        base_files = {
            f.relative_to(baseline_dir): f
            for f in baseline_dir.rglob("*")
            if f.is_file() and "__pycache__" not in f.parts and f.suffix != ".pyc"
        }

        for rel in sorted(gen_files.keys() - base_files.keys()):
            differences.append(f"EXTRA: {rel}")
        for rel in sorted(base_files.keys() - gen_files.keys()):
            differences.append(f"MISSING: {rel}")
        for rel in sorted(gen_files.keys() & base_files.keys()):
            h1 = hashlib.sha256(gen_files[rel].read_bytes()).hexdigest()
            h2 = hashlib.sha256(base_files[rel].read_bytes()).hexdigest()
            if h1 != h2:
                differences.append(f"CHANGED: {rel}")
        return differences

    def test_sandbox_scope_generates_exactly_profile_pipelines(self):
        """D6: generated/dev/ holds EXACTLY the two in-scope pipeline dirs."""
        exit_code, output = self.run_sandbox_generate()
        assert exit_code == 0, f"Sandbox generation should succeed: {output}"

        pipeline_dirs = sorted(
            p.name for p in self.generated_dir.iterdir() if p.is_dir()
        )
        assert pipeline_dirs == ["acmi_edw_bronze", "acmi_edw_silver"], (
            f"Sandbox run must generate exactly the profile scope, got {pipeline_dirs}"
        )

    def test_sandbox_generated_output_matches_baseline(self):
        """D7: every generated file matches the hand-written sandbox baseline."""
        exit_code, output = self.run_sandbox_generate()
        assert exit_code == 0, f"Sandbox generation should succeed: {output}"

        differences = self._compare_directory_hashes(
            self.generated_dir, self.generated_baseline_dir
        )
        assert not differences, (
            f"{len(differences)} difference(s) vs generated_baseline_sandbox:\n"
            + "\n".join(differences)
        )

    def test_sandbox_in_scope_read_renamed_out_of_scope_read_untouched(self):
        """D7 read-shared/write-own on the two probe reads.

        acmi_edw_silver READS ``edw_bronze.customer`` (produced by the
        in-scope ``acmi_edw_bronze``) -> renamed; it also READS
        ``edw_bronze.bronze_sfcc_cust`` (produced by NOTHING in-project, an
        external/shared source) -> byte-identical.
        """
        exit_code, output = self.run_sandbox_generate()
        assert exit_code == 0, f"Sandbox generation should succeed: {output}"

        silver_dim = (
            self.generated_dir / "acmi_edw_silver" / "customer_silver_dim.py"
        ).read_text()
        assert (
            'spark.readStream.table("acme_edw_dev.edw_bronze.alice_customer")'
            in silver_dim
        ), "In-scope read of the bronze-produced table must carry the alice_ leaf"
        assert "acme_edw_dev.edw_bronze.customer" not in silver_dim, (
            "The unrenamed in-scope-produced name must be gone from the consumer"
        )

        sfcc = (
            self.generated_dir / "acmi_edw_silver" / "customer_dim_sfcc.py"
        ).read_text()
        assert (
            'spark.readStream.table("acme_edw_dev.edw_bronze.bronze_sfcc_cust")' in sfcc
        ), "Read of a table with no in-scope producer must stay untouched"
        assert "alice_bronze_sfcc_cust" not in sfcc, (
            "A table no in-scope pipeline produces must NOT be namespaced"
        )

    def test_sandbox_resources_exactly_scoped_with_namespaced_event_log(self):
        """D6+D9: resources/lhp/ holds exactly the scoped resource files and
        the injected project event-log table name is namespaced."""
        exit_code, output = self.run_sandbox_generate()
        assert exit_code == 0, f"Sandbox generation should succeed: {output}"

        resource_files = sorted(f.name for f in self.resources_dir.iterdir())
        assert resource_files == [
            "acmi_edw_bronze.pipeline.yml",
            "acmi_edw_silver.pipeline.yml",
        ], f"Sandbox sync must emit exactly the scoped resources, got {resource_files}"

        for name in resource_files:
            hash_diff = self._compare_file_hashes(
                self.resources_dir / name, self.resources_baseline_dir / name
            )
            assert hash_diff == "", f"Resource baseline mismatch: {hash_diff}"

        # Semantic spot-check on top of the hash: the composed
        # <pipeline>_event_log leaf is routed through {namespace}_{table}.
        for pipeline in ("acmi_edw_bronze", "acmi_edw_silver"):
            parsed = yaml.safe_load(
                (self.resources_dir / f"{pipeline}.pipeline.yml").read_text()
            )
            event_log = parsed["resources"]["pipelines"][f"{pipeline}_pipeline"][
                "event_log"
            ]
            assert event_log["name"] == f"alice_{pipeline}_event_log"
            assert event_log["schema"] == "_meta"
            assert event_log["catalog"] == "acme_edw_dev"


# Complex scope: ten exact pipeline names (no globs), namespace bob.
SANDBOX_COMPLEX_PROFILE = """sandbox:
  namespace: bob
  pipelines:
    - sample_python_func_pipeline
    - helper_imports
    - dep_bindings
    - 13_sinks
    - custom_datasource
    - 15_python_load
    - acme_edw_bp_bronze
    - acme_edw_bp_silver
    - acmi_edw_silver
    - acmi_edw_modelled
"""

# Team policy appended to the copy's lhp.yaml: the NON-DEFAULT SUFFIX
# pattern (leaf first, namespace last), plus allowed_envs so dev is
# positively sandbox-enabled.
SANDBOX_COMPLEX_POLICY = """
sandbox:
  strategy: table
  table_pattern: "{table}_{namespace}"
  allowed_envs:
    - dev
"""


@pytest.mark.e2e
class TestSandboxComplexE2E:
    """E2E tests for sandbox mode across the complex carriers (bob namespace).

    The profile scopes the run to exactly ten pipelines by exact name (no
    globs) under the NON-DEFAULT suffix pattern ``{table}_{namespace}``. This
    exercises Python function transforms, Python helper-module closures,
    Python custom data sources, Python loads, delta/kafka/foreachbatch sinks,
    blueprint-expanded pipelines (``acme_edw_bp_raw`` out of scope),
    snapshot-CDC parameter bindings, inline-SQL bodies, and — on
    ``dep_bindings`` — the dynamic-SQL probes (``table_changes()`` CDF read,
    f-string ``spark.sql`` rewrite, name-bound container read / ``LHP-VAL-066``,
    runtime table-read shim, and opaque ``spark.sql`` / ``LHP-VAL-067``). Output
    is checked against the hand-written
    ``generated_baseline_sandbox_complex/dev`` (58 files) and
    ``resources_baseline_sandbox_complex/lhp`` (10 files).
    """

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"
        self.generated_baseline_dir = (
            self.project_root / "generated_baseline_sandbox_complex" / "dev"
        )
        self.resources_baseline_dir = (
            self.project_root / "resources_baseline_sandbox_complex" / "lhp"
        )

        self._init_bundle_project()
        self._arrange_sandbox()

        yield

        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        # Baselines are only used for comparison, never copied into working dirs.
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)

        self.generated_dir.mkdir(parents=True, exist_ok=True)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    def _arrange_sandbox(self):
        """Apply the sandbox additions to the isolated COPY (never the fixture).

        Writes the personal ``.lhp/profile.yaml``, appends the team
        ``sandbox:`` policy, and uncomments the project ``event_log:`` block
        in the copy's ``lhp.yaml`` (same uncomment as TestEventLogE2E).
        """
        lhp_dir = self.project_root / ".lhp"
        lhp_dir.mkdir()
        (lhp_dir / "profile.yaml").write_text(SANDBOX_COMPLEX_PROFILE)

        lhp_yaml = self.project_root / "lhp.yaml"
        content = lhp_yaml.read_text()
        content = content.replace(
            '#event_log:\n#  catalog: "{catalog}"\n#  schema: _meta\n'
            '#  name_suffix: "_event_log"',
            'event_log:\n  catalog: "{catalog}"\n  schema: _meta\n'
            '  name_suffix: "_event_log"',
        )
        assert "\nevent_log:" in content, "event_log uncomment did not apply"
        lhp_yaml.write_text(content + SANDBOX_COMPLEX_POLICY)

    def run_sandbox_generate(self) -> tuple:
        """Run `lhp generate --env dev --sandbox` with the project pipeline config."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--verbose",
                "generate",
                "--env",
                "dev",
                "--pipeline-config",
                "config/pipeline_config.yaml",
                "--sandbox",
            ],
        )
        return result.exit_code, result.output

    @staticmethod
    def _compare_file_hashes(file1: Path, file2: Path) -> str:
        """Compare two files by SHA-256. Returns '' if identical."""

        def get_hash(f):
            return hashlib.sha256(f.read_bytes()).hexdigest()

        h1, h2 = get_hash(file1), get_hash(file2)
        if h1 != h2:
            return (
                f"Hash mismatch: {file1.name} "
                f"(generated={h1[:12]}) != (baseline={h2[:12]})"
            )
        return ""

    def _compare_directory_hashes(
        self, generated_dir: Path, baseline_dir: Path
    ) -> list:
        """Compare directories recursively. Returns list of differences."""
        differences = []
        # Skip __pycache__ dirs and .pyc files: bytecode is not LHP-generated
        # output, so a stray .pyc in a baseline dir must not register as a diff.
        gen_files = {
            f.relative_to(generated_dir): f
            for f in generated_dir.rglob("*")
            if f.is_file() and "__pycache__" not in f.parts and f.suffix != ".pyc"
        }
        base_files = {
            f.relative_to(baseline_dir): f
            for f in baseline_dir.rglob("*")
            if f.is_file() and "__pycache__" not in f.parts and f.suffix != ".pyc"
        }

        for rel in sorted(gen_files.keys() - base_files.keys()):
            differences.append(f"EXTRA: {rel}")
        for rel in sorted(base_files.keys() - gen_files.keys()):
            differences.append(f"MISSING: {rel}")
        for rel in sorted(gen_files.keys() & base_files.keys()):
            h1 = hashlib.sha256(gen_files[rel].read_bytes()).hexdigest()
            h2 = hashlib.sha256(base_files[rel].read_bytes()).hexdigest()
            if h1 != h2:
                differences.append(f"CHANGED: {rel}")
        return differences

    def test_sandbox_complex_scope_generates_exactly_profile_pipelines(self):
        """generated/dev/ holds EXACTLY the ten in-scope pipeline dirs."""
        exit_code, output = self.run_sandbox_generate()
        assert exit_code == 0, f"Sandbox generation should succeed: {output}"

        pipeline_dirs = sorted(
            p.name for p in self.generated_dir.iterdir() if p.is_dir()
        )
        assert pipeline_dirs == [
            "13_sinks",
            "15_python_load",
            "acme_edw_bp_bronze",
            "acme_edw_bp_silver",
            "acmi_edw_modelled",
            "acmi_edw_silver",
            "custom_datasource",
            "dep_bindings",
            "helper_imports",
            "sample_python_func_pipeline",
        ], f"Sandbox run must generate exactly the profile scope, got {pipeline_dirs}"
        assert "acme_edw_bp_raw" not in pipeline_dirs, (
            "The blueprint sibling acme_edw_bp_raw is out of scope and must be absent"
        )

    def test_sandbox_complex_generated_output_matches_baseline(self):
        """Every generated file matches the hand-written complex baseline."""
        exit_code, output = self.run_sandbox_generate()
        assert exit_code == 0, f"Sandbox generation should succeed: {output}"

        differences = self._compare_directory_hashes(
            self.generated_dir, self.generated_baseline_dir
        )
        assert not differences, (
            f"{len(differences)} difference(s) vs generated_baseline_sandbox_complex:\n"
            + "\n".join(differences)
        )

    def test_sandbox_complex_resources_scoped_with_suffix_event_logs(self):
        """resources/lhp/ holds exactly the ten scoped resource files and the
        injected project event-log table name carries the suffix namespace."""
        exit_code, output = self.run_sandbox_generate()
        assert exit_code == 0, f"Sandbox generation should succeed: {output}"

        resource_files = sorted(f.name for f in self.resources_dir.iterdir())
        assert resource_files == [
            "13_sinks.pipeline.yml",
            "15_python_load.pipeline.yml",
            "acme_edw_bp_bronze.pipeline.yml",
            "acme_edw_bp_silver.pipeline.yml",
            "acmi_edw_modelled.pipeline.yml",
            "acmi_edw_silver.pipeline.yml",
            "custom_datasource.pipeline.yml",
            "dep_bindings.pipeline.yml",
            "helper_imports.pipeline.yml",
            "sample_python_func_pipeline.pipeline.yml",
        ], f"Sandbox sync must emit exactly the scoped resources, got {resource_files}"

        for name in resource_files:
            hash_diff = self._compare_file_hashes(
                self.resources_dir / name, self.resources_baseline_dir / name
            )
            assert hash_diff == "", f"Resource baseline mismatch: {hash_diff}"

        # Semantic spot-check on top of the hash: the composed
        # <pipeline>_event_log leaf is routed through {table}_{namespace}.
        for pipeline in (
            "sample_python_func_pipeline",
            "helper_imports",
            "dep_bindings",
            "13_sinks",
            "custom_datasource",
            "15_python_load",
            "acme_edw_bp_bronze",
            "acme_edw_bp_silver",
            "acmi_edw_silver",
            "acmi_edw_modelled",
        ):
            parsed = yaml.safe_load(
                (self.resources_dir / f"{pipeline}.pipeline.yml").read_text()
            )
            event_log = parsed["resources"]["pipelines"][f"{pipeline}_pipeline"][
                "event_log"
            ]
            assert event_log["name"] == f"{pipeline}_event_log_bob"
            assert event_log["schema"] == "_meta"
            assert event_log["catalog"] == "acme_edw_dev"

    def test_sandbox_complex_parameter_bound_read_renamed(self):
        """Pins the parameter-binding rename (snapshot-CDC source_function).

        The consumer's ``source_function.parameters`` value (a parameter-bound
        read) is renamed to the producer's namespaced leaf, and the producer
        side writes the same namespaced name.
        """
        exit_code, output = self.run_sandbox_generate()
        assert exit_code == 0, f"Sandbox generation should succeed: {output}"

        consumer = (
            self.generated_dir / "dep_bindings" / "param_snapshot_flow.py"
        ).read_text()
        assert (
            'source_table="acme_edw_dev.edw_bronze.helper_snapshot_dim_bob"' in consumer
        ), "The parameter-bound snapshot read must carry the namespaced producer leaf"
        assert "acme_edw_dev.edw_bronze.helper_snapshot_dim" not in consumer.replace(
            "acme_edw_dev.edw_bronze.helper_snapshot_dim_bob", ""
        ), "The unrenamed parameter-bound name must be gone from the consumer"

        producer = (
            self.generated_dir / "helper_imports" / "helper_snapshot_flow.py"
        ).read_text()
        assert 'name="acme_edw_dev.edw_bronze.helper_snapshot_dim_bob"' in producer, (
            "The in-scope producer must write the namespaced snapshot-dim name"
        )

    def test_sandbox_complex_rename_semantics_across_carriers(self):
        """One probe per carrier — rename applies to write targets and to
        reads of in-scope-produced tables, never to reads of tables no in-scope
        pipeline produces.
        """
        exit_code, output = self.run_sandbox_generate()
        assert exit_code == 0, f"Sandbox generation should succeed: {output}"

        # Delta sink: the write target leaf is namespaced.
        sk_delta = (self.generated_dir / "13_sinks" / "sk_delta.py").read_text()
        assert (
            '"tableName": "acme_edw_dev.edw_analytics.daily_order_metrics_bob"'
            in sk_delta
        ), "Delta sink write target must carry the _bob suffix"

        # foreachbatch MERGE target untouched (sinks register no producer).
        sk_foreach = (
            self.generated_dir / "13_sinks" / "sk_foreach_batch.py"
        ).read_text()
        assert "MERGE INTO acme_edw_dev.edw_gold.dim_customer_merged" in sk_foreach, (
            "foreachbatch MERGE target must remain the unrenamed name"
        )
        assert "dim_customer_merged_bob" not in sk_foreach, (
            "A sink registers no producer, so its MERGE target must NOT be namespaced"
        )

        # Inline SQL join: one statement, mixed outcomes by producer scope.
        partsupp = (
            self.generated_dir / "acmi_edw_modelled" / "partsupp_modelled_fct.py"
        ).read_text()
        assert "acme_edw_dev.edw_silver.partsupp_dim_bob" in partsupp, (
            "Inline-SQL read of an in-scope-produced table must be namespaced"
        )
        assert "acme_edw_dev.edw_silver.supplier_dim_bob" in partsupp, (
            "Inline-SQL read of an in-scope-produced table must be namespaced"
        )
        assert "acme_edw_dev.edw_silver.part_dim" in partsupp, (
            "Inline-SQL read of a table with no in-scope producer must stay untouched"
        )
        assert "part_dim_bob" not in partsupp, (
            "part_dim has no in-scope producer, so it must NOT be namespaced"
        )

        # Blueprint asymmetry: read of an out-of-scope-produced raw table is
        # untouched, while the in-scope write target is namespaced.
        bp_bronze = (
            self.generated_dir / "acme_edw_bp_bronze" / "site_alpha_customer_bronze.py"
        ).read_text()
        assert (
            'spark.readStream.table("acme_edw_dev.edw_raw.site_alpha_customer_raw")'
            in bp_bronze
        ), (
            "Read of acme_edw_bp_raw's output must stay untouched (producer out of scope)"
        )
        assert "acme_edw_dev.edw_bronze.site_alpha_customer_bob" in bp_bronze, (
            "The in-scope blueprint bronze write target must carry the _bob suffix"
        )

        # Inverse of the alice scenario: this read of edw_bronze.customer is
        # UNRENAMED here because its producer (acmi_edw_bronze) is out of scope.
        silver_dim = (
            self.generated_dir / "acmi_edw_silver" / "customer_silver_dim.py"
        ).read_text()
        assert (
            'spark.readStream.table("acme_edw_dev.edw_bronze.customer")' in silver_dim
        ), "Read of customer must stay unrenamed (acmi_edw_bronze out of scope)"

    def test_sandbox_complex_table_changes_and_identifier_rewrite(self):
        """Feature 1: the single-quoted in-scope FQN inside both
        ``IDENTIFIER(...)`` and ``table_changes(...)`` is text-rewritten to the
        producer's namespaced leaf.

        ``cdf_probe_flow`` reads ``helper_snapshot_dim`` (produced by the
        in-scope ``helper_imports`` pipeline) through both constructs; each
        single-quoted FQN carries the ``_bob`` suffix, and the in-scope write
        target is namespaced too. The unrenamed FQN (``helper_snapshot_dim'``,
        which ``helper_snapshot_dim_bob'`` never matches) is gone.
        """
        exit_code, output = self.run_sandbox_generate()
        assert exit_code == 0, f"Sandbox generation should succeed: {output}"

        cdf = (self.generated_dir / "dep_bindings" / "cdf_probe_flow.py").read_text()
        assert "IDENTIFIER('acme_edw_dev.edw_bronze.helper_snapshot_dim_bob')" in cdf, (
            "The in-scope FQN inside IDENTIFIER() must be rewritten to the _bob leaf"
        )
        assert (
            "table_changes('acme_edw_dev.edw_bronze.helper_snapshot_dim_bob', 0)" in cdf
        ), "The in-scope FQN inside table_changes() must be rewritten to the _bob leaf"
        assert "helper_snapshot_dim'" not in cdf, (
            "No unrenamed single-quoted FQN may remain in either construct"
        )
        assert 'name="acme_edw_dev.edw_silver.cdf_probe_out_bob"' in cdf, (
            "The in-scope CDF write target must carry the _bob suffix"
        )

    def test_sandbox_complex_python_read_carriers(self):
        """Features 2-5 on one copied module (``dep_bindings_union_transform.py``).

        A single module exercises four carriers at once:

        * opaque loop read -> wrapped by the runtime shim (feature 4);
        * f-string ``spark.sql`` with the in-scope FQN fully inside the literal
          segment -> rewritten, the ``{run_id}`` interpolation preserved
          (feature 2);
        * name-bound dict read -> left untouched, ``LHP-VAL-066`` (feature 3);
        * opaque ``spark.sql(query)`` -> left unchanged, ``LHP-VAL-067``
          (feature 5).
        """
        exit_code, output = self.run_sandbox_generate()
        assert exit_code == 0, f"Sandbox generation should succeed: {output}"

        mod = (
            self.generated_dir
            / "dep_bindings"
            / "custom_python_functions"
            / "dep_bindings_union_transform.py"
        ).read_text()

        # Feature 4: the runtime shim is emitted exactly once and wraps the
        # fully-opaque loop read.
        assert mod.count("def __lhp_sandbox_table(") == 1, (
            "The runtime shim helper must be emitted exactly once per module"
        )
        assert "spark.read.table(__lhp_sandbox_table(t))" in mod, (
            "The opaque loop read must be wrapped by the runtime shim"
        )

        # Feature 2: the in-scope FQN in the f-string literal segment is
        # rewritten; the {run_id} interpolation is preserved verbatim.
        assert (
            'f"SELECT * FROM acme_edw_dev.edw_bronze.helper_snapshot_dim_bob '
            'WHERE v = {run_id}"' in mod
        ), "The in-scope FQN in the f-string literal segment must be rewritten"

        # Feature 3: the name-bound dict read is left untouched (VAL-066) — the
        # dict literal keeps the original FQN and the read is NOT shim-wrapped.
        assert (
            'sources = {"helper": "acme_edw_dev.edw_bronze.helper_snapshot_dim"}' in mod
        ), "The dict-bound FQN must stay the original (non-sandbox) name"
        assert 'spark.read.table(sources["helper"])' in mod, (
            "The name-bound container read must stay bare (VAL-066, no rewrite)"
        )
        assert "__lhp_sandbox_table(sources" not in mod, (
            "The VAL-066 container read must NOT be wrapped by the runtime shim"
        )

        # Feature 5: the opaque spark.sql body is left unchanged (VAL-067).
        assert "spark.sql(query)" in mod, (
            "The opaque spark.sql(query) body must be left unchanged"
        )

    def _collect_sandbox_warnings(self):
        """Drive the real generation stream and return its sandbox warnings.

        Uses the public facade plus the ``warnings_sink`` recovery path so the
        assertion is on structured, uncolored :class:`WarningEmitted` records
        rather than the width-wrapped, ANSI-styled renderer text.
        """
        from lhp.api import (
            LakehousePlumberApplicationFacade,
            ProgressSink,
            collect_response,
            should_enable_bundle_support,
        )

        facade = LakehousePlumberApplicationFacade.for_project(
            self.project_root, pipeline_config_path="config/pipeline_config.yaml"
        )
        events = facade.generation.generate_pipelines(
            env="dev",
            output_dir=self.generated_dir,
            bundle_enabled=should_enable_bundle_support(
                self.project_root, cli_no_bundle=False
            ),
            sandbox=True,
            progress=ProgressSink(),
        )
        sink = []
        collect_response(events, warnings_sink=sink)
        return [w for w in sink if w.code in ("LHP-VAL-066", "LHP-VAL-067")]

    def test_sandbox_complex_warning_stream_val066_val067(self):
        """The container (``LHP-VAL-066``) and opaque-SQL (``LHP-VAL-067``)
        advisories surface on the generate warning stream, folded per file.

        ``LHP-VAL-066`` folds once for the dict-bound read in
        ``dep_bindings_union_transform.py``. ``LHP-VAL-067`` folds once per file
        across the two opaque-SQL carriers: the same module's
        ``spark.sql(query)`` and the existing
        ``supplier_param_snapshot_func.py`` fully-interpolated f-string window.
        """
        warnings = self._collect_sandbox_warnings()
        val_066 = [w for w in warnings if w.code == "LHP-VAL-066"]
        val_067 = [w for w in warnings if w.code == "LHP-VAL-067"]

        assert len(val_066) == 1, (
            f"Expected exactly one LHP-VAL-066 fold, got "
            f"{[(w.code, w.file) for w in val_066]}"
        )
        assert val_066[0].category == "sandbox"
        assert val_066[0].file is not None
        assert val_066[0].file.name == "dep_bindings_union_transform.py"

        assert len(val_067) == 2, (
            f"Expected exactly two LHP-VAL-067 folds, got "
            f"{[(w.code, w.file) for w in val_067]}"
        )
        assert all(w.category == "sandbox" for w in val_067)
        assert {w.file.name for w in val_067 if w.file} == {
            "dep_bindings_union_transform.py",
            "supplier_param_snapshot_func.py",
        }
