"""Tests for the local-closure resolver and its sibling import rewriter.

Closure tests build on-disk module trees under ``tmp_path`` and exercise
:func:`resolve_local_closure`. They are tagged so ``-k closure`` selects
them. Rewrite tests exercise
:func:`lhp.core.codegen.python_import_rewriter.rewrite_local_imports`
(``-k rewrite``); both halves share the on-disk locality predicate, so they
agree on what counts as a local import.
"""

import ast
from pathlib import Path

import pytest

from lhp.core.codegen.python_dependency_resolver import (
    ResolvedModule,
    resolve_local_closure,
)
from lhp.core.codegen.python_file_copier import _build_module_content
from lhp.core.codegen.python_import_rewriter import rewrite_local_imports
from lhp.errors import LHPError


def _write(path: Path, content: str = "") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _rel_paths(records: list[ResolvedModule]) -> set[str]:
    return {str(r.rel_path) for r in records}


def test_closure_sibling_module(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    entry = _write(root / "entry.py", "from helper import do_thing\n")
    _write(root / "helper.py", "def do_thing():\n    return 1\n")

    records = resolve_local_closure(entry, root, cache=None)

    assert _rel_paths(records) == {"helper.py"}
    helper = records[0]
    assert helper.rel_path == Path("helper.py")
    assert helper.content_source == (root / "helper.py").resolve()
    assert helper.is_synthesized_init is False
    # Entry is excluded from the returned closure.
    assert all(r.rel_path != Path("entry.py") for r in records)


def test_closure_sibling_module_not_imported_is_skipped(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    entry = _write(root / "entry.py", "from helper import do_thing\n")
    _write(root / "helper.py", "def do_thing():\n    return 1\n")
    _write(root / "unused.py", "def nope():\n    return 0\n")

    records = resolve_local_closure(entry, root, cache=None)

    assert _rel_paths(records) == {"helper.py"}


def test_closure_single_subpackage_includes_init(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    entry = _write(
        root / "entry.py", "from helpers.date_change import to_snapshot_date\n"
    )
    _write(root / "helpers" / "__init__.py", "")
    _write(
        root / "helpers" / "date_change.py",
        "import datetime\n\n\ndef to_snapshot_date():\n    return datetime.date.today()\n",
    )

    records = resolve_local_closure(entry, root, cache=None)

    assert _rel_paths(records) == {
        "helpers/__init__.py",
        "helpers/date_change.py",
    }
    init = next(r for r in records if r.rel_path == Path("helpers/__init__.py"))
    # The on-disk __init__.py is copied (real source), not synthesized.
    assert init.is_synthesized_init is False
    assert init.content_source == (root / "helpers" / "__init__.py").resolve()


def test_closure_whole_subpackage_copies_unimported_siblings(tmp_path: Path) -> None:
    """A package reference drags ALL its .py in (§3.8), even unimported ones."""
    root = tmp_path / "py_functions"
    entry = _write(root / "entry.py", "from helpers.a import x\n")
    _write(root / "helpers" / "__init__.py", "")
    _write(root / "helpers" / "a.py", "def x():\n    return 1\n")
    _write(root / "helpers" / "b.py", "def y():\n    return 2\n")

    records = resolve_local_closure(entry, root, cache=None)

    assert _rel_paths(records) == {
        "helpers/__init__.py",
        "helpers/a.py",
        "helpers/b.py",
    }


def test_closure_nested_subpackage(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    entry = _write(root / "entry.py", "from pkg.sub.deep import f\n")
    _write(root / "pkg" / "__init__.py", "")
    _write(root / "pkg" / "sub" / "__init__.py", "")
    _write(root / "pkg" / "sub" / "deep.py", "def f():\n    return 1\n")

    records = resolve_local_closure(entry, root, cache=None)

    assert _rel_paths(records) == {
        "pkg/__init__.py",
        "pkg/sub/__init__.py",
        "pkg/sub/deep.py",
    }


def test_closure_relative_import_in_package_followed(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    entry = _write(root / "entry.py", "from helpers.a import x\n")
    _write(root / "helpers" / "__init__.py", "")
    _write(
        root / "helpers" / "a.py", "from .b import y\n\n\ndef x():\n    return y()\n"
    )
    _write(root / "helpers" / "b.py", "def y():\n    return 2\n")

    records = resolve_local_closure(entry, root, cache=None)

    # Whole-package copy already pulls b.py; the relative import is consistent.
    assert _rel_paths(records) == {
        "helpers/__init__.py",
        "helpers/a.py",
        "helpers/b.py",
    }


def test_closure_transitive_across_two_packages(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    entry = _write(root / "entry.py", "from pkg_a.mod import a\n")
    _write(root / "pkg_a" / "__init__.py", "")
    _write(
        root / "pkg_a" / "mod.py",
        "from pkg_b.other import b\n\n\ndef a():\n    return b()\n",
    )
    _write(root / "pkg_b" / "__init__.py", "")
    _write(root / "pkg_b" / "other.py", "def b():\n    return 2\n")

    records = resolve_local_closure(entry, root, cache=None)

    assert _rel_paths(records) == {
        "pkg_a/__init__.py",
        "pkg_a/mod.py",
        "pkg_b/__init__.py",
        "pkg_b/other.py",
    }


def test_closure_import_cycle_terminates(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    entry = _write(root / "entry.py", "from a import fa\n")
    _write(root / "a.py", "from b import fb\n\n\ndef fa():\n    return fb()\n")
    _write(root / "b.py", "from a import fa\n\n\ndef fb():\n    return 1\n")

    records = resolve_local_closure(entry, root, cache=None)

    assert _rel_paths(records) == {"a.py", "b.py"}


def test_closure_self_import_cycle_terminates(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    entry = _write(root / "entry.py", "from helper import h\n")
    # helper imports itself — pathological but must not loop forever.
    _write(root / "helper.py", "from helper import h\n\n\ndef h():\n    return 1\n")

    records = resolve_local_closure(entry, root, cache=None)

    assert _rel_paths(records) == {"helper.py"}


def test_closure_external_imports_not_copied(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    entry = _write(
        root / "entry.py",
        "import os\nfrom pyspark.sql import functions as F\nfrom helper import h\n",
    )
    _write(root / "helper.py", "import json\n\n\ndef h():\n    return 1\n")

    records = resolve_local_closure(entry, root, cache=None)

    # Only the local helper; os / pyspark / json never appear.
    assert _rel_paths(records) == {"helper.py"}


def test_closure_no_local_imports_returns_empty(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    entry = _write(
        root / "entry.py",
        "import os\nfrom pyspark.sql import functions as F\n\n\ndef f():\n    return 1\n",
    )

    records = resolve_local_closure(entry, root, cache=None)

    assert records == []


def test_closure_namespace_init_synthesized(tmp_path: Path) -> None:
    """A flat module imported via a dotted path under a dir without __init__."""
    root = tmp_path / "py_functions"
    entry = _write(root / "entry.py", "from helpers.thing import t\n")
    # helpers/ has NO __init__.py on disk (namespace package).
    _write(root / "helpers" / "thing.py", "def t():\n    return 1\n")

    records = resolve_local_closure(entry, root, cache=None)

    rels = _rel_paths(records)
    assert "helpers/thing.py" in rels
    assert "helpers/__init__.py" in rels
    synth = next(r for r in records if r.rel_path == Path("helpers/__init__.py"))
    assert synth.is_synthesized_init is True
    assert synth.content_source is None


def test_closure_nested_namespace_init_synthesized(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    entry = _write(root / "entry.py", "from pkg.sub.deep import f\n")
    _write(root / "pkg" / "__init__.py", "")  # on disk
    # pkg/sub has NO __init__.py — must be synthesized.
    _write(root / "pkg" / "sub" / "deep.py", "def f():\n    return 1\n")

    records = resolve_local_closure(entry, root, cache=None)

    pkg_init = next(r for r in records if r.rel_path == Path("pkg/__init__.py"))
    sub_init = next(r for r in records if r.rel_path == Path("pkg/sub/__init__.py"))
    assert pkg_init.is_synthesized_init is False
    assert sub_init.is_synthesized_init is True
    assert sub_init.content_source is None


def test_closure_rule_a_root_is_package_raises_val_023(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    entry = _write(root / "entry.py", "from helper import h\n")
    _write(root / "helper.py", "def h():\n    return 1\n")
    _write(root / "__init__.py", "")  # root itself is a package -> Rule A

    with pytest.raises(LHPError) as exc_info:
        resolve_local_closure(entry, root, cache=None)

    assert exc_info.value.code == "LHP-VAL-023"


def test_closure_missing_helper_raises_val_025(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    # helper.py exists so 'helper' classifies as local-absolute...
    entry = _write(
        root / "entry.py", "from helper import h\nfrom helper.gone import g\n"
    )
    _write(root / "helper.py", "def h():\n    return 1\n")

    with pytest.raises(LHPError) as exc_info:
        resolve_local_closure(entry, root, cache=None)

    assert exc_info.value.code == "LHP-VAL-025"


def test_closure_broken_sibling_in_subpackage_raises_io_003(tmp_path: Path) -> None:
    """Whole-sub-package copy (§3.8) parses every .py — a broken, unimported
    sibling in the package surfaces as LHP-IO-003 at closure time. Asserted so
    this consequence is deliberate, not accidental.
    """
    root = tmp_path / "py_functions"
    entry = _write(root / "entry.py", "from helpers.good import g\n")
    _write(root / "helpers" / "__init__.py", "")
    _write(root / "helpers" / "good.py", "def g():\n    return 1\n")
    # Unrelated, unimported, syntactically broken sibling in the same package.
    _write(root / "helpers" / "broken.py", "def oops(:\n    return\n")

    with pytest.raises(LHPError) as exc_info:
        resolve_local_closure(entry, root, cache=None)

    assert exc_info.value.code == "LHP-IO-003"


def test_closure_broken_entry_raises_io_003(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    entry = _write(root / "entry.py", "def broken(:\n    return\n")

    with pytest.raises(LHPError) as exc_info:
        resolve_local_closure(entry, root, cache=None)

    assert exc_info.value.code == "LHP-IO-003"


def test_closure_uses_provided_cache(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    entry = _write(root / "entry.py", "from helper import h\n")
    _write(root / "helper.py", "def h():\n    return 1\n")

    cache: dict = {}
    resolve_local_closure(entry, root, cache=cache)

    # Both files were parsed and their trees cached by resolved path.
    assert str(entry.resolve()) in cache
    assert str((root / "helper.py").resolve()) in cache


def _rewrite(source: str, root: Path) -> str:
    return rewrite_local_imports(source, ast.parse(source), root)


def test_rewrite_absolute_local_from_subpackage_module(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    _write(root / "helpers" / "__init__.py", "")
    _write(root / "helpers" / "date_change.py", "def f():\n    return 1\n")

    out = _rewrite("from helpers.date_change import to_snapshot_date\n", root)

    assert "from custom_python_functions.helpers.date_change import" in out
    assert "to_snapshot_date" in out


def test_rewrite_absolute_local_from_package(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    _write(root / "helpers" / "__init__.py", "")
    _write(root / "helpers" / "date_change.py", "def f():\n    return 1\n")

    out = _rewrite("from helpers import date_change\n", root)

    assert "from custom_python_functions.helpers import date_change" in out


def test_rewrite_absolute_local_sibling_module(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    _write(root / "helper.py", "def h():\n    return 1\n")

    out = _rewrite("from helper import h\n", root)

    assert "from custom_python_functions.helper import h" in out


def test_rewrite_preserves_aliases(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    _write(root / "helpers" / "__init__.py", "")
    _write(root / "helpers" / "mod.py", "def a():\n    return 1\n")

    out = _rewrite("from helpers.mod import a as alpha, b\n", root)

    assert "from custom_python_functions.helpers.mod import a as alpha, b" in out


def test_rewrite_relative_import_preserved(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    _write(root / "helpers" / "__init__.py", "")
    _write(root / "helpers" / "sibling.py", "def y():\n    return 1\n")

    out = _rewrite("from .sibling import y\n", root)

    assert "from .sibling import y" in out
    assert "custom_python_functions" not in out


def test_rewrite_relative_multi_level_preserved(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    out = _rewrite("from ..pkg.sub import z\n", root)

    assert "from ..pkg.sub import z" in out
    assert "custom_python_functions" not in out


def test_rewrite_external_imports_preserved(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    source = (
        "import os\nimport datetime as dt\nfrom pyspark.sql import functions as F\n"
    )

    out = _rewrite(source, root)

    assert out == source
    assert "custom_python_functions" not in out


def test_rewrite_namespace_package_member_still_prefixed(tmp_path: Path) -> None:
    """``helpers/`` has NO ``__init__.py`` so the substrate classifies the
    import ``is_local=False`` — yet the closure copies it and the rewriter
    must prefix it, because both re-resolve on disk under ``root``."""
    root = tmp_path / "py_functions"
    # helpers/ is a namespace package: directory exists, no __init__.py.
    _write(root / "helpers" / "thing.py", "def t():\n    return 1\n")

    out = _rewrite("from helpers.thing import t\n", root)

    assert "from custom_python_functions.helpers.thing import t" in out


def test_rewrite_plain_dotted_local_raises_val_024(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    _write(root / "helpers" / "__init__.py", "")
    _write(root / "helpers" / "date_change.py", "def f():\n    return 1\n")

    with pytest.raises(LHPError) as exc_info:
        _rewrite("import helpers.date_change\n", root)

    assert exc_info.value.code == "LHP-VAL-024"


def test_rewrite_plain_dotted_local_with_alias_raises_val_024(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    _write(root / "helpers" / "__init__.py", "")
    _write(root / "helpers" / "date_change.py", "def f():\n    return 1\n")

    with pytest.raises(LHPError) as exc_info:
        _rewrite("import helpers.date_change as c\n", root)

    assert exc_info.value.code == "LHP-VAL-024"


def test_rewrite_plain_dotted_external_not_raised(tmp_path: Path) -> None:
    """A plain dotted import of a non-local module is left untouched, no error."""
    root = tmp_path / "py_functions"

    out = _rewrite("import os.path\n", root)

    assert out == "import os.path\n"


def test_rewrite_preserves_non_import_text_and_handles_multiline(
    tmp_path: Path,
) -> None:
    root = tmp_path / "py_functions"
    _write(root / "helpers" / "__init__.py", "")
    _write(root / "helpers" / "mod.py", "def a():\n    return 1\n")

    source = (
        "# leading comment with unicode: café\n"
        "\n"
        "import os  # external, keep as-is\n"
        "\n"
        "from helpers.mod import (\n"
        "    a,\n"
        "    b,  # trailing comment inside parens\n"
        ")\n"
        "\n"
        "\n"
        "def use():\n"
        "    # body comment\n"
        "    return a() + b()\n"
    )

    out = _rewrite(source, root)

    # The multi-line local import is collapsed to a single prefixed line.
    assert "from custom_python_functions.helpers.mod import a, b" in out
    # Every non-import line survives byte-for-byte.
    assert "# leading comment with unicode: café\n" in out
    assert "import os  # external, keep as-is\n" in out
    assert "def use():\n" in out
    assert "    # body comment\n" in out
    assert "    return a() + b()\n" in out
    # The external import is untouched; only the local one was rewritten.
    assert out.count("custom_python_functions") == 1


def test_rewrite_no_local_imports_is_identity(tmp_path: Path) -> None:
    root = tmp_path / "py_functions"
    source = "import os\nx = 1\n\n\ndef f():\n    return x\n"

    out = _rewrite(source, root)

    assert out == source


def test_rewrite_lazy_function_body_local_import_prefixed(tmp_path: Path) -> None:
    """Local imports inside a function body are rewritten too (whole-tree walk),
    matching closure discovery; the surrounding indentation is preserved."""
    root = tmp_path / "py_functions"
    _write(root / "helper.py", "def h():\n    return 1\n")

    source = "def use():\n    from helper import h  # lazy\n    return h()\n"

    out = _rewrite(source, root)

    assert "    from custom_python_functions.helper import h  # lazy\n" in out


# --- Codec-alignment invariant for the byte-offset import surgery -------------
#
# rewrite_local_imports slices the import span using the AST node's byte
# offsets (lineno/col_offset/end_*), which CPython measures as UTF-8 *byte*
# columns relative to the string the tree was parsed from. The function
# re-encodes its ``source`` argument to UTF-8 internally
# (``data = source.encode("utf-8")``) and recomputes line-start offsets from
# those bytes. The invariant the copier must uphold is therefore:
#
#     the ``source`` string and the string the ``tree`` was parsed from must be
#     the SAME decode (UTF-8) of the SAME bytes.
#
# Pre-fix, the copier read the body with ``read_text()`` (locale codec → cp1252
# on Windows) while ``parse_user_module`` parsed the tree from a UTF-8 read.
# A non-ASCII byte before the import on its line then made the two strings
# disagree on byte length, shifting the slice and corrupting the rewrite.
#
# These two tests pin the contrast directly at the rewriter boundary. Building
# both ``tree`` and ``source`` from the same UTF-8 string would be a tautology
# (the already-fixed state) and is deliberately avoided: the mismatched case
# below decodes the SAME bytes as latin-1 to simulate the pre-fix locale read.

# A non-ASCII char (U+2014 EM DASH: 3 bytes in UTF-8, but 3 *separate* chars
# when those bytes are decoded as latin-1, re-encoding to 4 UTF-8 bytes) placed
# in a string literal BEFORE the local import on the SAME physical line, so it
# falls inside the import node's col_offset prefix and skews the slice start.
_DESYNC_MODULE_BYTES = 'x = "—"; from helper import thing\n'.encode("utf-8")
_DESYNC_ROOT_HELPER = "helper.py"
_DESYNC_EXPECTED_ALIGNED = 'x = "—"; from custom_python_functions.helper import thing\n'


def test_rewrite_aligned_utf8_source_rewrites_correctly(tmp_path: Path) -> None:
    """With ``source`` decoded the same way the tree was parsed (UTF-8), the
    byte-offset slice lands exactly and the local import is prefixed cleanly,
    the leading unicode string literal preserved verbatim."""
    root = tmp_path / "py_functions"
    _write(root / _DESYNC_ROOT_HELPER, "def thing():\n    return 1\n")

    # Parse the tree from the UTF-8 decode, matching source_parser.py:75.
    tree = ast.parse(_DESYNC_MODULE_BYTES.decode("utf-8"))
    # Pass source as the SAME UTF-8 decode (simulates the fixed read).
    out = rewrite_local_imports(_DESYNC_MODULE_BYTES.decode("utf-8"), tree, root)

    assert out == _DESYNC_EXPECTED_ALIGNED


def test_rewrite_mismatched_codec_source_corrupts_slice(tmp_path: Path) -> None:
    """With ``source`` decoded under a DIFFERENT codec (latin-1) than the tree
    was parsed (UTF-8), the byte-offset slice misaligns and the rewrite is
    corrupted. This reproduces the pre-fix Windows desync: the AST offsets are
    UTF-8 byte columns, but the latin-1 ``source`` re-encodes the leading
    em-dash to a different byte length, shifting the import span."""
    root = tmp_path / "py_functions"
    _write(root / _DESYNC_ROOT_HELPER, "def thing():\n    return 1\n")

    # Tree parsed from UTF-8 (as the real parser does)...
    tree = ast.parse(_DESYNC_MODULE_BYTES.decode("utf-8"))
    # ...but source decoded as latin-1 from the SAME bytes (pre-fix locale read).
    out = rewrite_local_imports(_DESYNC_MODULE_BYTES.decode("latin-1"), tree, root)

    # The rewrite is corrupted: it is neither the correct aligned output nor a
    # clean prefixed import. The em-dash byte-length skew makes the slice start
    # too early (eating the closing quote) and end too early (leaving a
    # duplicated 'ing' tail), so the result is not even valid Python.
    assert out != _DESYNC_EXPECTED_ALIGNED
    with pytest.raises(SyntaxError):
        ast.parse(out)


def test_build_module_content_reads_source_as_utf8_to_match_parser(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The copier must read the body with the SAME codec the parser uses (UTF-8).

    This is the cross-platform regression guard for the copier's
    ``_build_module_content`` read. ``parse_user_module`` always reads the tree
    with ``encoding="utf-8"`` (``source_parser.py:75``); the rewriter then slices
    that source at the AST's UTF-8 byte offsets. If the copier reads the *body*
    string with a different codec, the slice and the parse disagree on byte
    lengths and the rewrite is corrupted.

    On macOS this cannot be exercised by a plain on-disk read: the platform
    default codec is already UTF-8, so the pre-fix ``read_text()`` (no
    ``encoding``) and the fixed ``read_text(encoding="utf-8")`` decode the same
    UTF-8 file identically -- a real-read test is a no-op locally. The Windows
    bug is that ``read_text()`` with no ``encoding`` decodes the on-disk UTF-8
    bytes under the locale codec (cp1252). This test rebinds ``Path.read_text``
    so that an *unencoded* call returns the latin-1 decode (simulating that
    Windows locale read) while an explicit ``encoding="utf-8"`` call decodes
    correctly -- isolating the single thing the fix changed: passing
    ``encoding="utf-8"`` at the ``_build_module_content`` read.

    Against the fixed code (``read_text(encoding="utf-8")``) the patch returns
    UTF-8, the slice aligns, and the output equals the aligned expectation
    (green). Against the pre-fix code (``read_text()``) the patch returns
    latin-1, the byte offsets skew, and the output is corrupted/unparseable
    (red).
    """
    root = tmp_path / "py_functions"
    _write(root / "helper.py", "def thing():\n    return 1\n")

    # Entry module written to disk as real UTF-8 bytes, with a multibyte char
    # (em-dash) inside a string literal BEFORE the local import on the SAME line.
    entry = root / "entry.py"
    entry.write_bytes(_DESYNC_MODULE_BYTES)

    real_read_text = Path.read_text

    def simulated_locale_read_text(self, *args, encoding=None, **kwargs):
        # No explicit encoding (pre-fix path) -> simulate a cp1252-style locale
        # decode via latin-1; explicit encoding (the fix + the parser) decodes
        # faithfully. Either way the on-disk bytes are the single source of truth.
        raw = self.read_bytes()
        return raw.decode("latin-1") if encoding is None else raw.decode(encoding)

    monkeypatch.setattr(Path, "read_text", simulated_locale_read_text)

    out = _build_module_content(
        entry,
        header_path="entry.py",
        root=root,
        context={},
        build_header=lambda _hp: "",
    )

    # Fixed read keeps body and tree on the same UTF-8 codec: aligned + valid.
    assert out == _DESYNC_EXPECTED_ALIGNED
    ast.parse(out)
