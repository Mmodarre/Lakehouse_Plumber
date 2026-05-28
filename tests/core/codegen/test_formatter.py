"""Unit tests for the CodeFormatter wrapper around Black."""

import pytest

from lhp.core.codegen.formatter import CodeFormatter
from lhp.errors import LHPConfigError


@pytest.mark.unit
def test_format_code_raises_on_invalid_python():
    formatter = CodeFormatter()
    with pytest.raises(LHPConfigError) as exc_info:
        formatter.format_code("def x(:\n  pass")
    assert exc_info.value.code_number == "031"
    assert "Black" in exc_info.value.title or "parsing" in exc_info.value.title


@pytest.mark.unit
def test_format_code_succeeds_on_valid_python():
    formatter = CodeFormatter()
    result = formatter.format_code("x=1\ny  =  2")
    assert "x = 1" in result
    assert "y = 2" in result


@pytest.mark.unit
def test_format_code_preserves_imports():
    formatter = CodeFormatter()
    result = formatter.format_code("import os\n\nimport sys\nprint(os, sys)")
    assert "import os" in result
    assert "import sys" in result
