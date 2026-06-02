"""Tests for the ``__future__`` hoisting chokepoint in code assembly.

``CodeAssembler.assemble`` is the chokepoint that hoists any ``from
__future__`` line found mid-body in a rendered template to the top of the
assembled module. PEP 236 requires future imports to precede all other
statements, so without this hoist a ``from __future__`` line embedded inside a
generated flowgroup body — beneath ``from pyspark import pipelines as dp`` —
would trigger ``SyntaxError: from __future__ imports must occur at the
beginning of the file`` when Lakeflow imports the module.
"""

from lhp.core.codegen.coordinator import CodeGenerationService
from lhp.models import FlowGroup


class TestAssemblyChokepointHoistsFutureFromCompleteCode:
    """The assembly chokepoint must hoist a ``__future__`` line embedded
    mid-body in ``complete_code`` to the top so the resulting module
    compiles."""

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
        # Hoisted ahead of `dp` import.
        dp_pos = assembled.index("from pyspark import pipelines as dp")
        assert future_pos < dp_pos
        # Hoisted ahead of the typing import that originally accompanied it.
        typing_pos = assembled.index("from typing import Optional")
        assert future_pos < typing_pos

        # The header banner still leads the file.
        header_end = assembled.index("# FlowGroup: fg_test")
        assert header_end < future_pos
