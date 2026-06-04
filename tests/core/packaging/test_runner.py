"""Contract tests for ``lhp.core.packaging.runner.build_runner_code``.

The runner is the single source file synced for a wheel-mode pipeline
(WHEEL_PACKAGING_SPEC §6.6, R9). Two properties are load-bearing and pinned
here:

* the rendered runner is valid Python that bootstraps ``builtins`` from the
  ambient globals and imports the flowgroup package's submodules; and
* it is **content-stable** — it references ONLY the import-package name, never
  the distribution name, content hash, or wheel version — so re-syncing it
  after pipeline content changes is a no-op.
"""

from __future__ import annotations

import ast
import re

import pytest

from lhp.core.packaging.runner import build_runner_code

_IMPORT_PKG = "p_15_python_load"


def _ambient_globals_tuple(code: str) -> tuple[str, ...]:
    """Extract the ``_AMBIENT_GLOBALS`` tuple literal from rendered runner code.

    Parsing the assignment via AST (rather than substring matching) is the
    precise way to assert *exactly* which ambient names the runner republishes.
    """
    tree = ast.parse(code)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "_AMBIENT_GLOBALS" for t in node.targets
        ):
            value = ast.literal_eval(node.value)
            return tuple(value)
    raise AssertionError("runner does not assign _AMBIENT_GLOBALS")


@pytest.mark.unit
class TestRunnerIsValidPython:
    def test_ast_parse_succeeds(self) -> None:
        ast.parse(build_runner_code(import_package=_IMPORT_PKG))

    def test_render_is_deterministic(self) -> None:
        """Same inputs render byte-identical output (no embedded clock/order)."""
        assert build_runner_code(import_package=_IMPORT_PKG) == build_runner_code(
            import_package=_IMPORT_PKG
        )


@pytest.mark.unit
class TestRunnerBootstrapMechanics:
    def test_contains_required_stdlib_machinery(self) -> None:
        code = build_runner_code(import_package=_IMPORT_PKG)
        # builtins republish + pkgutil submodule enumeration are the mechanism.
        assert "builtins" in code
        assert "pkgutil" in code

    def test_references_the_import_package_name(self) -> None:
        code = build_runner_code(import_package=_IMPORT_PKG)
        assert _IMPORT_PKG in code

    def test_default_ambient_globals_are_exactly_spark_and_dbutils(self) -> None:
        code = build_runner_code(import_package=_IMPORT_PKG)
        # spark and dbutils are present...
        assert "spark" in code
        assert "dbutils" in code
        # ...and they are the ONLY ambient names republished into builtins.
        assert _ambient_globals_tuple(code) == ("spark", "dbutils")

    def test_no_other_ambient_names_present(self) -> None:
        """Common SDP/notebook ambient names must NOT leak into the runner."""
        code = build_runner_code(import_package=_IMPORT_PKG)
        for forbidden in ("sqlContext", "displayHTML", "getArgument", "sc"):
            assert forbidden not in code

    def test_ambient_globals_override_is_honored(self) -> None:
        code = build_runner_code(
            import_package=_IMPORT_PKG, ambient_globals=("spark", "dbutils", "table")
        )
        assert _ambient_globals_tuple(code) == ("spark", "dbutils", "table")
        ast.parse(code)


@pytest.mark.unit
class TestRunnerContentStability:
    """The runner bytes must not encode any pipeline-content-derived identity."""

    def test_no_wheel_filename_reference(self) -> None:
        code = build_runner_code(import_package=_IMPORT_PKG)
        assert ".whl" not in code

    def test_no_twelve_hex_content_hash(self) -> None:
        code = build_runner_code(import_package=_IMPORT_PKG)
        # A 12-hex run is the content-hash shape (identity.content_hash).
        assert re.search(r"\b[0-9a-f]{12}\b", code) is None

    def test_no_version_string(self) -> None:
        code = build_runner_code(import_package=_IMPORT_PKG)
        # No digits-dot-digits anywhere (would catch an LHP version like 0.9.0).
        assert re.search(r"\d+\.\d+", code) is None

    def test_no_distribution_name(self) -> None:
        """The dist name (<pipeline>_<env>_<hash>) must not appear."""
        code = build_runner_code(import_package=_IMPORT_PKG)
        # The env-qualified dist form and any hash-bearing dist are absent;
        # the only pipeline token present is the bare import-package name.
        assert "preprod" not in code
        assert "dist-info" not in code
        assert ".dist-info" not in code

    def test_stable_across_distinct_pipelines_modulo_pkg_name(self) -> None:
        """Two different pipelines differ ONLY by their import-package token."""
        code_a = build_runner_code(import_package="p_alpha")
        code_b = build_runner_code(import_package="p_beta")
        normalized_a = code_a.replace("p_alpha", "<PKG>")
        normalized_b = code_b.replace("p_beta", "<PKG>")
        assert normalized_a == normalized_b
