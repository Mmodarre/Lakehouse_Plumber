"""Regression guard for GitHub issue #217 — resolver must not drop bindings a wildcard cannot supply.

``resolver.py`` once carried a rule #2 ("submodule wildcard imports take
precedence over parent-module-as-alias imports"). It was unsound: ``from
pyspark.sql.functions import *`` never binds the name ``F``, so dropping ``from
pyspark.sql import functions as F`` in its favour produced a module whose
``F.`` references were unbound — a ``NameError`` raised when SDP evaluated the
flow, at pipeline start-up. The rule has been removed; these tests guard
against its return.

The trigger was ``lhp generate --include-tests``: every test-action generator
contributes the wildcard (``BaseTestActionGenerator``) while any
``F.``-emitting generator in the same flowgroup contributes the alias
(``SchemaTransformGenerator``, ``BaseSinkGenerator``, and operational
metadata's ``F.current_timestamp()``).
"""

import ast

import pytest

from lhp.core.codegen.imports import ImportManager

pytestmark = pytest.mark.unit


def _resolve(*imports: str) -> list[str]:
    manager = ImportManager()
    for statement in imports:
        manager.add_import(statement)
    return manager.get_consolidated_imports()


def _bound_names(import_block: list[str]) -> set[str]:
    """Local names an import block actually binds, per Python semantics."""
    bound: set[str] = set()
    for statement in import_block:
        for node in ast.walk(ast.parse(statement)):
            if isinstance(node, ast.ImportFrom):
                bound |= {a.asname or a.name for a in node.names if a.name != "*"}
            elif isinstance(node, ast.Import):
                bound |= {a.asname or a.name.split(".")[0] for a in node.names}
    return bound


class TestAliasedParentSurvivesSubmoduleWildcard:
    """An aliased parent import binds a name the child wildcard cannot supply."""

    def test_alias_and_wildcard_both_kept(self):
        resolved = _resolve(
            "from pyspark.sql import functions as F",
            "from pyspark.sql.functions import *",
        )

        assert "from pyspark.sql.functions import *" in resolved
        assert "from pyspark.sql import functions as F" in resolved
        assert "F" in _bound_names(resolved)

    def test_issue_217_flowgroup_import_set_binds_F(self):
        """The exact import set a schema transform + uniqueness test produces."""
        resolved = _resolve(
            # from generators/test/_base.py
            "from pyspark import pipelines as dp",
            "from pyspark.sql.functions import *",
            # from generators/transform/schema.py
            "from pyspark.sql import functions as F",
            "from pyspark.sql.types import StructType",
        )

        assert "F" in _bound_names(resolved), (
            "generated module references F.col(...) but no import binds 'F'"
        )

    def test_sibling_parent_imports_are_not_collateral_damage(self):
        """Unrelated imports from the parent module must survive too.

        The removed rule excluded the whole ``pyspark.sql`` module *group*, so
        ``DataFrame`` (added by ``MaterializedViewGenerator``) disappeared as
        well whenever the alias conflict was recorded.
        """
        resolved = _resolve(
            "from pyspark.sql import DataFrame",
            "from pyspark.sql import functions as F",
            "from pyspark.sql.functions import *",
        )

        assert "from pyspark.sql import DataFrame" in resolved
        assert {"DataFrame", "F"} <= _bound_names(resolved)

    def test_types_alias_survives_types_wildcard(self):
        resolved = _resolve(
            "from pyspark.sql import types as T",
            "from pyspark.sql.types import *",
        )

        assert "T" in _bound_names(resolved)


class TestSoundCollapsesStillHappen:
    """The one sound rule — same-module wildcard beats same-module names — stays."""

    def test_same_module_wildcard_beats_specific_names(self):
        resolved = _resolve(
            "from pyspark.sql.functions import col, lit",
            "from pyspark.sql.functions import *",
        )

        assert resolved == ["from pyspark.sql.functions import *"]

    def test_unrelated_imports_untouched(self):
        resolved = _resolve(
            "import os",
            "from pyspark import pipelines as dp",
            "from pyspark.sql.functions import *",
        )

        assert set(resolved) == {
            "import os",
            "from pyspark import pipelines as dp",
            "from pyspark.sql.functions import *",
        }
