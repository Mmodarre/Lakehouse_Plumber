"""Tests for the ``__future__`` hoisting chokepoint in code assembly.

PEP 236 requires future imports to precede all other statements; without this
hoist a ``from __future__`` line embedded mid-body would trigger
``SyntaxError: from __future__ imports must occur at the beginning of the file``.
"""

from lhp.core.codegen.coordinator import CodeGenerationService
from lhp.models import FlowGroup


class TestAssemblyChokepointHoistsFutureFromCompleteCode:
    def test_future_in_complete_code_is_hoisted(self):
        # A rendered template body that carries a ``from __future__`` line
        # mid-body. PEP 236 requires this gets surfaced to the top.
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
        assembled = cg._assembler.assemble(flowgroup, all_imports, [], complete_code)

        # Hard contract: assembled module must parse cleanly.
        compile(assembled, "<string>", "exec")

        # The future line appears exactly once and immediately follows the
        # LHP comment header (with only blank lines between).
        assert assembled.count("from __future__ import annotations") == 1
        future_pos = assembled.index("from __future__ import annotations")
        dp_pos = assembled.index("from pyspark import pipelines as dp")
        assert future_pos < dp_pos
        typing_pos = assembled.index("from typing import Optional")
        assert future_pos < typing_pos

        header_end = assembled.index("# FlowGroup: fg_test")
        assert header_end < future_pos
