"""Tests for snapshot-CDC ``source_function`` extraction (PEP 236 safety).

Two layers of protection are exercised here:

1. ``source_function_loader._extract_function_code`` strips
   ``from __future__`` lines from the inlined block. Without this, a
   user's snapshot-source file's future import would land mid-body of the
   generated module — beneath ``from pyspark import pipelines as dp`` —
   and trigger ``SyntaxError: from __future__ imports must occur at the
   beginning of the file`` when Lakeflow imports it.

2. ``CodeGenerationService._assemble_final_code`` is the chokepoint that hoists
   any leftover ``from __future__`` line from the rendered template body
   to the top of the assembled module — defense in depth.
"""

import ast

from lhp.core.codegen.coordinator import CodeGenerationService
from lhp.generators.write.source_function_loader import _extract_function_code
from lhp.models.config import FlowGroup


class TestExtractFunctionCodeStripsFuture:
    """``_extract_function_code`` must skip ``from __future__`` imports."""

    def test_future_import_dropped_from_inlined_block(self):
        source = (
            "from __future__ import annotations\n"
            "from typing import Optional, Tuple\n"
            "from pyspark.sql import DataFrame\n"
            "\n"
            "def my_snapshot(latest_version: Optional[int]):\n"
            "    return None\n"
        )
        tree = ast.parse(source)

        extracted = _extract_function_code(source, tree, "my_snapshot")

        # __future__ removed; the body and surviving imports preserved.
        assert "from __future__" not in extracted
        assert "from typing import Optional, Tuple" in extracted
        assert "def my_snapshot" in extracted


class TestAssemblyChokepointHoistsFutureFromCompleteCode:
    """Defense-in-depth: even if ``complete_code`` contains a ``__future__``
    line embedded mid-body (which would only happen if the extractor's
    strip filter ever regresses), the assembly chokepoint must hoist it
    to the top so the resulting module compiles."""

    def test_future_in_complete_code_is_hoisted(self):
        # Simulate a rendered streaming_table template body where the
        # ``source_function_code`` block carried a ``__future__`` line
        # through. PEP 236 requires this gets surfaced to the top.
        complete_code = (
            "# ============================================================\n"
            "# TARGET TABLES\n"
            "# ============================================================\n"
            "\n"
            "from __future__ import annotations\n"
            "from typing import Optional\n"
            "\n"
            "def my_snapshot(latest_version: Optional[int]):\n"
            "    return None\n"
            "\n"
            "@dp.create_streaming_table(name='t')\n"
            "def t():\n"
            "    pass\n"
        )
        all_imports = {"from pyspark import pipelines as dp"}
        flowgroup = FlowGroup(pipeline="p_test", flowgroup="fg_test")

        cg = CodeGenerationService()
        assembled = cg._assemble_final_code(flowgroup, all_imports, [], complete_code)

        # Hard contract: assembled module must parse cleanly.
        compile(assembled, "<string>", "exec")

        # The future line appears exactly once and immediately follows the
        # LHP comment header (with only blank lines between).
        assert assembled.count("from __future__ import annotations") == 1
        future_pos = assembled.index("from __future__ import annotations")
        # Hoisted ahead of `dp` import.
        dp_pos = assembled.index("from pyspark import pipelines as dp")
        assert future_pos < dp_pos
        # Hoisted ahead of the typing import that originally accompanied it.
        typing_pos = assembled.index("from typing import Optional")
        assert future_pos < typing_pos

        # The header banner still leads the file.
        header_end = assembled.index(f"# FlowGroup: fg_test")
        assert header_end < future_pos
