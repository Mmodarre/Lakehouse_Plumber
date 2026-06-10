"""Tests for ImportManager module."""

import tempfile
from unittest.mock import Mock, patch

import pytest

from lhp.core.codegen.imports import ImportManager, extract_future_imports
from lhp.core.codegen.imports.categorizer import (
    categorize_import,
    extract_module_name,
    is_wildcard_import,
)
from lhp.core.registry import BaseActionGenerator
from lhp.generators.load.custom_datasource import CustomDataSourceLoadGenerator
from lhp.models import Action, ActionType


class TestImportManagerBasics:
    """Test basic ImportManager functionality."""

    def setup_method(self):
        self.manager = ImportManager()

    def test_initialization(self):
        assert self.manager is not None
        assert len(self.manager.get_consolidated_imports()) == 0

    def test_manual_import_addition(self):
        self.manager.add_import("import os")
        self.manager.add_import("from pathlib import Path")

        imports = self.manager.get_consolidated_imports()
        assert "import os" in imports
        assert "from pathlib import Path" in imports
        assert len(imports) == 2

    def test_manual_import_duplicates(self):
        self.manager.add_import("import os")
        self.manager.add_import("import os")  # Duplicate
        self.manager.add_import("from pathlib import Path")

        imports = self.manager.get_consolidated_imports()
        assert imports.count("import os") == 1  # Should only appear once
        assert len(imports) == 2

    def test_manual_import_whitespace_handling(self):
        self.manager.add_import("  import os  ")
        self.manager.add_import("\tfrom pathlib import Path\n")
        self.manager.add_import("")  # Empty string
        self.manager.add_import("   ")  # Whitespace only

        imports = self.manager.get_consolidated_imports()
        assert "import os" in imports
        assert "from pathlib import Path" in imports
        assert len(imports) == 2  # Empty/whitespace should be ignored

    def test_expression_import_detection(self):
        self.manager.add_imports_from_expression("F.current_timestamp()")
        self.manager.add_imports_from_expression("F.col('name').alias('column_name')")
        self.manager.add_imports_from_expression(
            "F.when(F.col('age') > 18, F.lit('adult'))"
        )

        imports = self.manager.get_consolidated_imports()
        # Should detect F (functions) import from expressions
        assert any("functions" in imp for imp in imports)

    def test_expression_import_invalid_expressions(self):
        self.manager.add_imports_from_expression("invalid_syntax((")
        self.manager.add_imports_from_expression("")
        self.manager.add_imports_from_expression(None)

        # Should still work normally
        imports = self.manager.get_consolidated_imports()
        assert isinstance(imports, list)

    def test_mixed_sources(self):
        self.manager.add_import("import custom_module")
        self.manager.add_import("from pathlib import Path")
        self.manager.add_imports_from_expression("F.lit('test')")

        imports = self.manager.get_consolidated_imports()

        # Check for specific imports from each source
        assert "import custom_module" in imports
        assert any("pathlib" in imp for imp in imports)
        assert any("functions" in imp for imp in imports)

    def test_clear_functionality(self):
        self.manager.add_import("import os")
        self.manager.add_import("from pathlib import Path")
        self.manager.add_imports_from_expression("F.current_timestamp()")

        assert len(self.manager.get_consolidated_imports()) > 0

        self.manager.clear()
        imports = self.manager.get_consolidated_imports()

        assert len(imports) == 0


class TestConflictResolution:
    def setup_method(self):
        self.manager = ImportManager()

    def test_basic_wildcard_precedence(self):
        self.manager.add_import("from pyspark.sql.functions import col, lit")
        self.manager.add_import("from pyspark.sql.functions import *")

        imports = self.manager.get_consolidated_imports()

        assert "from pyspark.sql.functions import *" in imports
        assert "from pyspark.sql.functions import col, lit" not in imports
        assert len([imp for imp in imports if "pyspark.sql.functions" in imp]) == 1

    def test_submodule_conflict_resolution(self):
        self.manager.add_import("from pyspark.sql import functions as F")
        self.manager.add_import("from pyspark.sql.functions import *")

        imports = self.manager.get_consolidated_imports()

        assert "from pyspark.sql.functions import *" in imports
        assert "from pyspark.sql import functions as F" not in imports

    def test_multiple_submodule_conflicts(self):
        self.manager.add_import("from pyspark.sql import functions as F")
        self.manager.add_import("from pyspark.sql import types as T")
        self.manager.add_import("from pyspark.sql.functions import *")
        self.manager.add_import("from pyspark.sql.types import *")

        imports = self.manager.get_consolidated_imports()

        assert "from pyspark.sql.functions import *" in imports
        assert "from pyspark.sql.types import *" in imports
        assert "from pyspark.sql import functions as F" not in imports
        assert "from pyspark.sql import types as T" not in imports

    def test_non_conflicting_submodules(self):
        self.manager.add_import("from pyspark.sql import SparkSession")
        self.manager.add_import(
            "from pyspark.sql.functions import col, lit"
        )  # No wildcard

        imports = self.manager.get_consolidated_imports()

        assert "from pyspark.sql import SparkSession" in imports
        assert "from pyspark.sql.functions import col, lit" in imports

    def test_complex_conflict_scenario(self):
        self.manager.add_import("from pyspark.sql import functions as F")
        self.manager.add_import("import os")

        self.manager.add_imports_from_expression("F.current_timestamp()")

        self.manager.add_import("from pyspark.sql.functions import *")
        self.manager.add_import("from pyspark.sql.types import *")

        imports = self.manager.get_consolidated_imports()

        assert "import os" in imports
        assert any("from pyspark.sql.functions import *" in imp for imp in imports)

    def test_same_module_wildcard_duplicates(self):
        self.manager.add_import("from pyspark.sql.functions import *")
        self.manager.add_import("from pyspark.sql.functions import *")  # Duplicate

        imports = self.manager.get_consolidated_imports()

        wildcard_imports = [
            imp for imp in imports if "from pyspark.sql.functions import *" in imp
        ]
        assert len(wildcard_imports) == 1

    def test_no_conflicts_preserved(self):
        self.manager.add_import("import os")
        self.manager.add_import("from pathlib import Path")
        self.manager.add_import("from pyspark import pipelines as dp")
        self.manager.add_import("from typing import Dict, List")

        imports = self.manager.get_consolidated_imports()

        assert "import os" in imports
        assert "from pathlib import Path" in imports
        assert "from pyspark import pipelines as dp" in imports
        assert "from typing import Dict, List" in imports
        assert len(imports) == 4

    def test_partial_conflicts(self):
        self.manager.add_import("import os")  # No conflict
        self.manager.add_import(
            "from pyspark.sql import functions as F"
        )  # Will conflict
        self.manager.add_import("from pathlib import Path")  # No conflict
        self.manager.add_import(
            "from pyspark.sql.functions import *"
        )  # Conflicts with F import
        self.manager.add_import("from pyspark import pipelines as dp")  # No conflict

        imports = self.manager.get_consolidated_imports()

        assert "import os" in imports
        assert "from pathlib import Path" in imports
        assert "from pyspark import pipelines as dp" in imports
        assert "from pyspark.sql.functions import *" in imports
        assert "from pyspark.sql import functions as F" not in imports

    def test_different_alias_patterns(self):
        self.manager.add_import("from pyspark.sql import functions as F")
        self.manager.add_import(
            "from pyspark.sql import functions as pyspark_functions"
        )
        self.manager.add_import("from pyspark.sql import functions")  # No alias
        self.manager.add_import("from pyspark.sql.functions import *")

        imports = self.manager.get_consolidated_imports()

        assert "from pyspark.sql.functions import *" in imports
        assert not any(
            "from pyspark.sql import functions" in imp
            for imp in imports
            if "import *" not in imp
        )


class TestImportSorting:
    def setup_method(self):
        self.manager = ImportManager()

    def test_import_categorization(self):
        imports_by_category = {
            "standard": ["import os", "from pathlib import Path", "import json"],
            "third_party": ["import pandas", "import numpy", "import requests"],
            "pyspark": ["from pyspark.sql import SparkSession", "import pyspark"],
            "dlt": ["from pyspark import pipelines as dp"],
            "custom": ["from mymodule import helper", "import custom_package"],
        }

        for _category, imports in imports_by_category.items():
            for imp in imports:
                self.manager.add_import(imp)

        sorted_imports = self.manager.get_consolidated_imports()

        total_expected = sum(len(imports) for imports in imports_by_category.values())
        assert len(sorted_imports) == total_expected

    def test_import_order_standard_first(self):
        self.manager.add_import("import custom_module")  # custom (last)
        self.manager.add_import("from pyspark import pipelines as dp")  # dlt (4th)
        self.manager.add_import("import os")  # standard (1st)
        self.manager.add_import("import pandas")  # third-party (2nd)
        self.manager.add_import("import pyspark")  # pyspark (3rd)

        imports = self.manager.get_consolidated_imports()

        os_pos = imports.index("import os")
        pandas_pos = imports.index("import pandas")
        pyspark_pos = imports.index("import pyspark")
        dlt_pos = imports.index("from pyspark import pipelines as dp")
        custom_pos = imports.index("import custom_module")

        assert os_pos < pandas_pos < pyspark_pos < dlt_pos < custom_pos

    def test_alphabetical_within_categories(self):
        standard_imports = [
            "import sys",
            "import os",
            "import json",
            "from pathlib import Path",
        ]
        for imp in standard_imports:
            self.manager.add_import(imp)

        imports = self.manager.get_consolidated_imports()

        standard_in_result = [
            imp
            for imp in imports
            if any(std in imp for std in ["os", "sys", "json", "pathlib"])
        ]

        expected_order = [
            "from pathlib import Path",
            "import json",
            "import os",
            "import sys",
        ]
        assert standard_in_result == expected_order

    def test_mixed_import_styles_sorting(self):
        self.manager.add_import("import os")
        self.manager.add_import("from os import path")
        self.manager.add_import("from pathlib import Path")
        self.manager.add_import("import sys")

        imports = self.manager.get_consolidated_imports()

        expected = [
            "from os import path",
            "from pathlib import Path",
            "import os",
            "import sys",
        ]
        assert imports == expected

    def test_pyspark_specific_categorization(self):
        pyspark_imports = [
            "import pyspark",
            "from pyspark.sql import SparkSession",
            "from pyspark.sql import functions as F",
            "from pyspark.sql.functions import col",
            "from pyspark.sql.types import StructType",
        ]

        for imp in pyspark_imports:
            self.manager.add_import(imp)

        imports = self.manager.get_consolidated_imports()

        assert len(imports) == len(pyspark_imports)

        expected_order = [
            "from pyspark import sql",  # Would be detected if present
            "from pyspark.sql import SparkSession",
            "from pyspark.sql import functions as F",
            "from pyspark.sql.functions import col",
            "from pyspark.sql.types import StructType",
            "import pyspark",
        ]

        for imp in pyspark_imports:
            assert imp in imports

    def test_dlt_categorization(self):
        dlt_imports = [
            "from pyspark import pipelines as dp",
        ]

        for imp in dlt_imports:
            self.manager.add_import(imp)

        imports = self.manager.get_consolidated_imports()

        for imp in dlt_imports:
            assert imp in imports

    def test_complete_sorting_workflow(self):
        all_imports = [
            "import custom_module",  # custom
            "import os",  # standard
            "from pyspark.sql import functions as F",  # pyspark
            "import requests",  # third_party
            "from pathlib import Path",  # standard
            "from pyspark import pipelines as dp",  # dlt
            "import pandas",  # third_party
            "from pyspark.sql.functions import *",  # pyspark
            "import json",  # standard
        ]

        for imp in all_imports:
            self.manager.add_import(imp)

        imports = self.manager.get_consolidated_imports()

        assert len(imports) >= 7  # Some may be removed due to conflicts

        first_third_party = next(
            (
                i
                for i, imp in enumerate(imports)
                if "requests" in imp or "pandas" in imp
            ),
            -1,
        )
        first_pyspark = next(
            (
                i
                for i, imp in enumerate(imports)
                if "pyspark" in imp and "pipelines" not in imp
            ),
            -1,
        )
        first_dlt = next((i for i, imp in enumerate(imports) if "pipelines" in imp), -1)
        first_custom = next(
            (i for i, imp in enumerate(imports) if "custom_module" in imp), -1
        )

        standard_imports = [
            imp
            for imp in imports
            if any(std in imp for std in ["os", "pathlib", "json"])
        ]
        assert len(standard_imports) >= 2  # At least os and pathlib

        if first_third_party >= 0 and first_pyspark >= 0:
            assert first_third_party < first_pyspark
        if first_pyspark >= 0 and first_dlt >= 0:
            assert first_pyspark < first_dlt
        if first_dlt >= 0 and first_custom >= 0:
            assert first_dlt < first_custom

    def test_unknown_modules_categorized_as_custom(self):
        unknown_imports = [
            "import unknown_module",
            "from mysterious import function",
            "import project_specific",
        ]

        for imp in unknown_imports:
            self.manager.add_import(imp)

        imports = self.manager.get_consolidated_imports()

        for imp in unknown_imports:
            assert imp in imports


class TestExtractFutureImports:
    """Tests for the AST-based ``extract_future_imports`` helper.

    The helper underpins the PEP 236 chokepoint in the code-assembly step
    (``CodeAssembler.assemble``): any source string passed through assembly
    must surrender its ``from __future__`` lines so they can be hoisted to the
    top of the assembled module.
    """

    def test_single_future_import_extracted(self):
        source = "from __future__ import annotations\n\nx = 1\n"
        future_lines, cleaned = extract_future_imports(source)

        assert future_lines == ["from __future__ import annotations"]
        # Original line is blanked but preserved (line numbering intact).
        assert cleaned.split("\n")[0] == ""
        assert "x = 1" in cleaned
        assert "from __future__" not in cleaned

    def test_multiple_future_imports_in_declaration_order(self):
        source = (
            "from __future__ import annotations\n"
            "from __future__ import division\n"
            "\n"
            "y = 2\n"
        )
        future_lines, cleaned = extract_future_imports(source)

        assert future_lines == [
            "from __future__ import annotations",
            "from __future__ import division",
        ]
        assert "from __future__" not in cleaned
        assert "y = 2" in cleaned

    def test_future_import_inside_string_not_extracted(self):
        # AST guarantee: a literal "from __future__" inside a triple-quoted
        # string is just data, not an import statement, and must NOT be
        # extracted. This is the AST-vs-regex correctness check.
        source = 'DOCS = """\nfrom __future__ import annotations\n"""\nz = 3\n'
        future_lines, cleaned = extract_future_imports(source)

        assert future_lines == []
        # Source returned unchanged — nothing was blanked.
        assert cleaned == source

    def test_no_future_imports(self):
        source = "import os\n\ndef f():\n    pass\n"
        future_lines, cleaned = extract_future_imports(source)

        assert future_lines == []
        assert cleaned == source

    def test_invalid_python_falls_through(self):
        # Caller (assembly) may pass fragments that are not standalone
        # parseable modules. Helper must degrade gracefully.
        source = "def broken(\n"
        future_lines, cleaned = extract_future_imports(source)

        assert future_lines == []
        assert cleaned == source

    def test_multiline_future_import_extracted(self):
        # Parenthesized future imports span multiple lines.
        source = (
            "from __future__ import (\n"
            "    annotations,\n"
            "    division,\n"
            ")\n"
            "\n"
            "value = 42\n"
        )
        future_lines, cleaned = extract_future_imports(source)

        assert len(future_lines) == 1
        assert "from __future__" in future_lines[0]
        assert "annotations" in future_lines[0]
        assert "division" in future_lines[0]
        assert "from __future__" not in cleaned
        assert "value = 42" in cleaned


class TestIntegration:
    def setup_method(self):
        self.manager = ImportManager()

    def test_base_generator_integration(self):
        class TestGenerator(BaseActionGenerator):
            def __init__(self):
                super().__init__(use_import_manager=True)

            def generate(self, action, context):
                return "test_code"

        generator = TestGenerator()

        generator.add_import("import os")
        generator.add_imports_from_expression("F.current_timestamp()")

        import_manager = generator.get_import_manager()
        assert import_manager is not None

        imports = generator.imports
        assert "import os" in imports
        assert len(imports) >= 1

    def test_custom_datasource_integration(self):
        self.manager.add_import("from pyspark.sql.functions import *")
        self.manager.add_import("from pyspark.sql.types import *")
        self.manager.add_import("from pyspark import pipelines as dp")

        self.manager.add_imports_from_expression("F.current_timestamp()")
        self.manager.add_imports_from_expression("F.col('_metadata')")

        imports = self.manager.get_consolidated_imports()

        assert len(imports) > 0
        assert any("from pyspark.sql.functions import *" in imp for imp in imports)


class TestErrorHandling:
    def setup_method(self):
        self.manager = ImportManager()

    def test_none_inputs(self):
        self.manager.add_import(None)
        self.manager.add_imports_from_expression(None)

        imports = self.manager.get_consolidated_imports()
        assert isinstance(imports, list)

    def test_empty_inputs(self):
        self.manager.add_import("")
        self.manager.add_import("   ")
        self.manager.add_imports_from_expression("")

        imports = self.manager.get_consolidated_imports()
        assert len(imports) == 0

    def test_malformed_import_statements(self):
        malformed_imports = [
            "import",  # Incomplete
            "from import",  # Invalid syntax
            "import 123invalid",  # Invalid module name
            "from . import",  # Incomplete relative import
        ]

        for imp in malformed_imports:
            self.manager.add_import(imp)

        imports = self.manager.get_consolidated_imports()
        assert isinstance(imports, list)

    def test_expression_parsing_errors(self):
        invalid_expressions = [
            "F.function_with_unclosed_paren(",
            "invalid.syntax..with..dots",
            "F.('malformed')",
            "invalid_identifier_with_$pecial_chars",
        ]

        for expr in invalid_expressions:
            self.manager.add_imports_from_expression(expr)

        imports = self.manager.get_consolidated_imports()
        assert isinstance(imports, list)


class TestUtilityMethods:
    def setup_method(self):
        self.manager = ImportManager()

    def test_extract_module_name(self):
        test_cases = [
            ("import os", "os"),
            ("from pathlib import Path", "pathlib"),
            ("from pyspark.sql import functions as F", "pyspark.sql"),
            ("from pyspark.sql.functions import *", "pyspark.sql.functions"),
            ("invalid import statement", None),
        ]

        for import_stmt, expected in test_cases:
            result = extract_module_name(import_stmt)
            assert result == expected

    def test_is_wildcard_import(self):
        wildcard_cases = [
            ("from pyspark.sql.functions import *", True),
            ("from os import *", True),
            ("from pyspark.sql.functions import col", False),
            ("import os", False),
        ]

        for import_stmt, expected in wildcard_cases:
            result = is_wildcard_import(import_stmt)
            assert result == expected

    def test_categorize_import(self):
        categorization_cases = [
            ("import os", "standard"),
            ("from pathlib import Path", "standard"),
            ("import pandas", "third_party"),
            ("import requests", "third_party"),
            ("from pyspark.sql import SparkSession", "pyspark"),
            ("from pyspark import pipelines as dp", "dlt"),
            ("import unknown_module", "custom"),
        ]

        for import_stmt, expected in categorization_cases:
            result = categorize_import(import_stmt)
            assert result == expected


class TestRealWorldScenarios:
    def setup_method(self):
        self.manager = ImportManager()

    def test_custom_datasource_complete_scenario(self):
        self.manager.add_import("from pyspark.sql import functions as F")
        self.manager.add_import("from pyspark.sql.functions import *")
        self.manager.add_import("from pyspark.sql.types import *")

        self.manager.add_imports_from_expression("F.current_timestamp()")
        self.manager.add_imports_from_expression("F.col('_processing_timestamp')")

        self.manager.add_import("from pyspark import pipelines as dp")

        imports = self.manager.get_consolidated_imports()

        wildcard_imports = [
            imp for imp in imports if "from pyspark.sql.functions import *" in imp
        ]
        f_alias_imports = [
            imp for imp in imports if "from pyspark.sql import functions as F" in imp
        ]

        assert len(wildcard_imports) == 1
        assert len(f_alias_imports) == 0

    def test_operational_metadata_integration(self):
        metadata_expressions = [
            "F.current_timestamp()",
            "F.lit('pipeline_name')",
            "F.col('_metadata.file_path')",
            "F.when(F.col('status') == 'active', F.lit('valid'))",
        ]

        for expr in metadata_expressions:
            self.manager.add_imports_from_expression(expr)

        imports = self.manager.get_consolidated_imports()

        assert len(imports) > 0
        assert any("functions" in imp for imp in imports)

    def test_mixed_generator_scenario(self):
        self.manager.add_import("from pyspark.sql import functions as F")

        self.manager.add_import("from pyspark.sql.functions import *")
        self.manager.add_import("from pyspark.sql.types import StructType")
        self.manager.add_import("from pyspark.sql.types import *")

        self.manager.add_imports_from_expression("F.input_file_name()")

        self.manager.add_import("from pyspark import pipelines as dp")

        imports = self.manager.get_consolidated_imports()

        assert len(imports) > 0
        assert any("from pyspark.sql.functions import *" in imp for imp in imports)
        assert "from pyspark import pipelines as dp" in imports


class TestImportNameCollision:
    """Test ImportManager.add_import collision detection.

    The check fires when two ``from … import …`` statements bind the same
    local name to different source modules — the silent-shadowing risk that
    exists for python load/transform and (post-refactor) for custom
    datasource/sink imports.
    """

    def test_same_name_different_modules_raises(self):
        """``from a import X`` then ``from b import X`` → LHPValidationError."""
        from lhp.errors import LHPValidationError

        mgr = ImportManager()
        mgr.add_import("from a import Conflict")
        with pytest.raises(LHPValidationError) as excinfo:
            mgr.add_import("from b import Conflict")
        msg = str(excinfo.value)
        assert "Conflict" in msg
        assert "collision" in msg.lower()

    def test_same_name_same_module_dedupes(self):
        """Identical ``from … import …`` is silently deduped (set behavior)."""
        mgr = ImportManager()
        mgr.add_import("from a import Same")
        mgr.add_import("from a import Same")
        assert "from a import Same" in mgr.get_consolidated_imports()
        assert (
            sum(
                1
                for imp in mgr.get_consolidated_imports()
                if imp == "from a import Same"
            )
            == 1
        )

    def test_different_aliases_coexist(self):
        """``from a import X as Y`` and ``from b import X as Z`` → no collision."""
        mgr = ImportManager()
        mgr.add_import("from a import X as Y")
        mgr.add_import("from b import X as Z")
        consolidated = mgr.get_consolidated_imports()
        assert "from a import X as Y" in consolidated
        assert "from b import X as Z" in consolidated

    def test_alias_vs_unaliased_different_names(self):
        """``from a import X`` and ``from b import Y as X`` collide on local name X."""
        from lhp.errors import LHPValidationError

        mgr = ImportManager()
        mgr.add_import("from a import X")
        with pytest.raises(LHPValidationError):
            mgr.add_import("from b import Y as X")

    def test_plain_import_does_not_trigger_check(self):
        """``import x`` form has no binding-name extraction; never raises."""
        mgr = ImportManager()
        mgr.add_import("import a")
        mgr.add_import("import b")
        consolidated = mgr.get_consolidated_imports()
        assert "import a" in consolidated
        assert "import b" in consolidated

    def test_wildcard_import_skipped(self):
        """``from a import *`` doesn't bind named symbols; never collides."""
        mgr = ImportManager()
        mgr.add_import("from a import *")
        mgr.add_import("from b import X")
        consolidated = mgr.get_consolidated_imports()
        assert "from b import X" in consolidated
