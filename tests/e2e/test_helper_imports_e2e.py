"""E2E tests for transitive helper-module copying.

Proves the helper-import contract end-to-end: when a user entry function imports
a LOCAL helper sub-package, LHP copies the entry AND its transitive local helpers
into ``custom_python_functions/`` (preserving sub-package structure) and:

  * prefix-rewrites absolute-local imports on the entry to
    ``custom_python_functions.helpers.transforms`` ;
  * preserves intra-package relative imports (``from .util import clean``)
    verbatim inside the copied helper ;
  * leaves external/stdlib imports (pyspark) untouched ;
  * carries an ``LHP-SOURCE:`` provenance header on every copied file.

The same entry-importing-a-helper pattern is wired into ALL FIVE entry funnels,
each a flowgroup in the single ``helper_imports`` pipeline:

  * ``helper_transform_flow`` — python-TRANSFORM action;
  * ``helper_source_flow`` — custom DATA SOURCE (load) action;
  * ``helper_load_flow`` — python LOAD action;
  * ``helper_sink_flow`` — custom SINK (write) action;
  * ``helper_snapshot_flow`` — snapshot-CDC ``source_function`` (write) action.

The two by-value funnels (custom source + custom sink) additionally emit
``register_pickle_by_value(custom_python_functions)`` so the class — and the
embedded helper closure — travel by value to the executor.

Richer import shapes are exercised across the helper closure:

  * a NESTED sub-package (``helpers/sub/deep.py``) referenced by the load entry
    — copied with structure preserved;
  * a transitive ABSOLUTE helper->helper chain (``helpers/aggregate.py`` does
    ``from helpers.other import tag``) — prefix-rewritten *inside* the copied
    helper, distinct from the existing relative ``from .util`` case;
  * external/stdlib imports (pyspark) in helpers — left untouched.

Because the closure copies a top-level package whole (spec §3.8), the single
``custom_python_functions/`` tree under the pipeline holds the union of every
flowgroup's closure (all five entries + the shared ``helpers/`` package).

The fixture lives under
``tests/e2e/fixtures/testing_project/pipelines/18_helper_imports/`` with the
entries + helper sub-package under ``py_functions/`` (``helper_demo_transform.py``,
``helper_demo_source.py``, ``helper_demo_load.py``, ``helper_demo_sink.py``,
``helper_demo_snapshot.py``, ``helpers/{__init__,transforms,util,aggregate,
other}.py``, ``helpers/sub/{__init__,deep}.py``). Baselines were authored
manually and verified against the feature contract — never formatted.
"""

import hashlib
import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestHelperImportsE2E:
    """E2E test for transitive helper-module copying (entry + closure)."""

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        """Create isolated copy of fixture project for each test."""
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"
        self.pipeline_dir = self.generated_dir / "helper_imports"
        self.baseline_dir = (
            self.project_root / "generated_baseline" / "dev" / "helper_imports"
        )

        self._init_bundle_project()

        yield
        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)
        self.resources_dir.mkdir(parents=True, exist_ok=True)

    def run_generate(self) -> tuple:
        """Run 'lhp generate --env dev' with the bundle pipeline config."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "generate",
                "--env",
                "dev",
                "--pipeline-config",
                "config/pipeline_config.yaml",
            ],
        )
        return result.exit_code, result.output

    def _compare_file_hashes(self, file1: Path, file2: Path) -> str:
        """Compare two files by SHA-256. Returns '' if identical, error otherwise."""

        def get_hash(f: Path) -> str:
            return hashlib.sha256(f.read_bytes()).hexdigest()

        h1, h2 = get_hash(file1), get_hash(file2)
        if h1 != h2:
            return (
                f"Hash mismatch: {file1.name} ({h1[:12]}) != {file2.name} ({h2[:12]})"
            )
        return ""

    def test_helper_closure_matches_baseline(self):
        """Entry + helper closure copied, rewritten, and structure-preserved.

        Verifies every generated artifact of the helper-import generation against
        the manually-authored baseline:
          1. All five pipeline .py files (one per funnel).
          2. All five copied entry modules (absolute-local imports prefix-rewritten).
          3. The copied helper sub-package — flat helpers (__init__, transforms,
             util, aggregate, other) with both the relative import (transforms ->
             .util) and the absolute helper->helper import (aggregate ->
             helpers.other) handled, plus the NESTED sub-package (sub/__init__,
             sub/deep) with structure preserved.
          4. The top-level custom_python_functions/__init__.py.
        """
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        artifacts = [
            "helper_transform_flow.py",
            "helper_source_flow.py",
            "helper_load_flow.py",
            "helper_sink_flow.py",
            "helper_snapshot_flow.py",
            "custom_python_functions/__init__.py",
            "custom_python_functions/helper_demo_transform.py",
            "custom_python_functions/helper_demo_source.py",
            "custom_python_functions/helper_demo_load.py",
            "custom_python_functions/helper_demo_sink.py",
            "custom_python_functions/helper_demo_snapshot.py",
            "custom_python_functions/helpers/__init__.py",
            "custom_python_functions/helpers/transforms.py",
            "custom_python_functions/helpers/util.py",
            "custom_python_functions/helpers/aggregate.py",
            "custom_python_functions/helpers/other.py",
            "custom_python_functions/helpers/sub/__init__.py",
            "custom_python_functions/helpers/sub/deep.py",
        ]
        for rel_path in artifacts:
            generated = self.pipeline_dir / rel_path
            baseline = self.baseline_dir / rel_path
            assert generated.exists(), (
                f"{rel_path} should be generated under helper_imports/"
            )
            assert baseline.exists(), f"Baseline {rel_path} should exist"
            diff = self._compare_file_hashes(generated, baseline)
            assert diff == "", f"Baseline mismatch for {rel_path}: {diff}"

    def test_entry_absolute_local_import_is_prefix_rewritten(self):
        """The entry's absolute-local import is rewritten to custom_python_functions.*."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        for entry in ("helper_demo_transform.py", "helper_demo_source.py"):
            content = (
                self.pipeline_dir / "custom_python_functions" / entry
            ).read_text()
            assert (
                "from custom_python_functions.helpers.transforms import enrich"
                in content
            ), f"{entry} should have the absolute-local import prefix-rewritten"
            assert "from helpers.transforms import" not in content, (
                f"{entry} should not retain the un-rewritten absolute-local import"
            )

    def test_helper_relative_import_is_preserved_verbatim(self):
        """The intra-package relative import inside the helper is left unchanged."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        helper = (
            self.pipeline_dir / "custom_python_functions" / "helpers" / "transforms.py"
        )
        content = helper.read_text()
        assert "from .util import clean" in content, (
            "Intra-package relative import must be preserved verbatim"
        )
        assert "from custom_python_functions.helpers.util" not in content, (
            "Relative import must not be prefix-rewritten"
        )
        # External imports are left untouched.
        assert "from pyspark.sql.functions import lit" in content

    def test_custom_data_source_registers_pickle_by_value(self):
        """The custom-data-source flow emits register_pickle_by_value for the package."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        content = (self.pipeline_dir / "helper_source_flow.py").read_text()
        assert (
            "_lhp_cloudpickle.register_pickle_by_value(custom_python_functions)"
            in content
        ), "Custom data source must register the package by value"

    def test_all_five_entry_imports_prefix_rewritten(self):
        """Every funnel's copied entry has its absolute-local helper import rewritten.

        One assertion per entry funnel: load / sink / snapshot reach
        ``helpers.aggregate``; transform / source reach ``helpers.transforms``.
        All five must end up under the ``custom_python_functions.`` prefix and
        none may retain the bare ``from helpers.`` form.
        """
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        expected = {
            "helper_demo_transform.py": (
                "from custom_python_functions.helpers.transforms import enrich"
            ),
            "helper_demo_source.py": (
                "from custom_python_functions.helpers.transforms import enrich"
            ),
            "helper_demo_load.py": (
                "from custom_python_functions.helpers.aggregate import aggregate_rows"
            ),
            "helper_demo_sink.py": (
                "from custom_python_functions.helpers.aggregate import aggregate_rows"
            ),
            "helper_demo_snapshot.py": (
                "from custom_python_functions.helpers.aggregate import aggregate_rows"
            ),
        }
        for entry, rewritten in expected.items():
            content = (
                self.pipeline_dir / "custom_python_functions" / entry
            ).read_text()
            assert rewritten in content, (
                f"{entry} should have its absolute-local import prefix-rewritten"
            )
            # Check actual import STATEMENTS (line-start after strip), not the
            # docstring prose that quotes the bare form inside backticks.
            bare_stmts = [
                ln
                for ln in content.splitlines()
                if ln.strip().startswith("from helpers.")
            ]
            assert not bare_stmts, (
                f"{entry} should not retain any un-rewritten 'from helpers.' "
                f"import statement; found: {bare_stmts}"
            )

    def test_nested_subpackage_copied_with_structure_preserved(self):
        """A nested helper sub-package is copied under its sub-dir, structure intact.

        ``helpers/sub/deep.py`` (referenced by the load entry via
        ``from helpers.sub.deep import deepen``) is mirrored at
        ``custom_python_functions/helpers/sub/deep.py`` with a synthesized/real
        ``sub/__init__.py`` alongside it, and the load entry's two-segment
        absolute-local import is prefix-rewritten.
        """
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        cpf = self.pipeline_dir / "custom_python_functions"
        nested = cpf / "helpers" / "sub" / "deep.py"
        nested_init = cpf / "helpers" / "sub" / "__init__.py"
        assert nested.exists(), (
            "Nested helper must be copied under helpers/sub/ (structure preserved)"
        )
        assert nested_init.exists(), "Nested sub-package must have an __init__.py"
        assert "def deepen(" in nested.read_text(), "Nested helper body must be copied"

        entry = (cpf / "helper_demo_load.py").read_text()
        assert "from custom_python_functions.helpers.sub.deep import deepen" in entry, (
            "The nested two-segment absolute-local import must be prefix-rewritten"
        )

    def test_transitive_absolute_helper_to_helper_import_is_rewritten(self):
        """A helper's ABSOLUTE import of a sibling helper is prefix-rewritten in-copy.

        ``helpers/aggregate.py`` reaches its sibling via the absolute-local
        ``from helpers.other import tag`` — distinct from the relative
        ``from .util import clean`` case. Inside the copied helper this becomes
        ``from custom_python_functions.helpers.other import tag``; the external
        pyspark import is left untouched and ``helpers/other.py`` is copied too.
        """
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        cpf = self.pipeline_dir / "custom_python_functions"
        aggregate = (cpf / "helpers" / "aggregate.py").read_text()
        assert "from custom_python_functions.helpers.other import tag" in aggregate, (
            "The absolute helper->helper import must be prefix-rewritten in the copy"
        )
        # Check actual import STATEMENTS (line-start after strip), not the
        # docstring prose that quotes the bare form inside backticks.
        bare_stmts = [
            ln
            for ln in aggregate.splitlines()
            if ln.strip().startswith("from helpers.")
        ]
        assert not bare_stmts, (
            f"The un-rewritten absolute helper->helper import statement must not "
            f"survive; found: {bare_stmts}"
        )
        # External import in the same helper is left untouched.
        assert "from pyspark.sql.functions import lit" in aggregate
        # The absolute-imported sibling is part of the copied closure.
        assert (cpf / "helpers" / "other.py").exists(), (
            "The absolute-imported sibling helper must be copied into the closure"
        )

    def test_custom_sink_registers_pickle_by_value(self):
        """The custom-sink (write) funnel also registers the package by value."""
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        content = (self.pipeline_dir / "helper_sink_flow.py").read_text()
        assert (
            "_lhp_cloudpickle.register_pickle_by_value(custom_python_functions)"
            in content
        ), "Custom sink must register the package by value"
        assert (
            "from custom_python_functions.helper_demo_sink import HelperBackedSink"
            in content
        ), "Custom sink driver must import the entry from custom_python_functions"

    def test_snapshot_source_function_imports_copied_entry(self):
        """The snapshot-CDC driver imports the copied entry under custom_python_functions.

        The snapshot ``source_function`` funnel does not use
        ``register_pickle_by_value`` (it is invoked by reference inside the
        pipeline); instead the driver aliases the copied entry module and
        passes its function to ``create_auto_cdc_from_snapshot_flow``.
        """
        exit_code, output = self.run_generate()
        assert exit_code == 0, f"Generation failed: {output}"

        content = (self.pipeline_dir / "helper_snapshot_flow.py").read_text()
        assert (
            "import custom_python_functions.helper_demo_snapshot "
            "as _snap_helper_demo_snapshot" in content
        ), "Snapshot driver must alias-import the copied entry module"
        assert "source=_snap_helper_demo_snapshot.next_helper_snapshot" in content, (
            "Snapshot driver must reference the copied source function"
        )
