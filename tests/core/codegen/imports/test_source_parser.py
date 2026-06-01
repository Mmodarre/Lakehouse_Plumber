"""Unit tests for the shared parse substrate (``source_parser``).

Covers the single parse contract (``parse_user_module``: parse-OK, the
generic ``LHP-IO-003`` syntax-error path, and the per-pipeline tree
cache) and the import classifier (``local_import_targets``: relative,
absolute-local, external, and plain-dotted-local imports).
"""

import ast
from pathlib import Path

import pytest

from lhp.core.codegen.imports import (
    ImportTarget,
    local_import_targets,
    parse_user_module,
)
from lhp.errors import LHPError


@pytest.mark.unit
def test_parse_ok_returns_module(tmp_path):
    """A syntactically-valid file parses to an ``ast.Module``."""
    path = tmp_path / "good.py"
    path.write_text("import os\n\n\ndef f():\n    return os.getcwd()\n")

    tree = parse_user_module(path, cache=None)

    assert isinstance(tree, ast.Module)


@pytest.mark.unit
def test_syntax_error_raises_generic_io_003_with_file_path(tmp_path):
    """Invalid syntax raises LHP-IO-003 parameterized by the file path."""
    path = tmp_path / "broken.py"
    path.write_text("def f(:\n    pass\n")

    with pytest.raises(LHPError) as exc_info:
        parse_user_module(path, cache=None)

    err = exc_info.value
    assert err.code == "LHP-IO-003"
    # Message/details must reference the offending file path generically...
    assert str(path) in err.details
    # ...and must NOT carry the snapshot-flavored "valid snapshot signature"
    # example (this is the generic syntax-error contract, not the CDC one).
    assert err.example is None
    assert "latest_version" not in (err.example or "")
    assert "latest_version" not in err.details


@pytest.mark.unit
def test_cache_parses_once_for_two_requests_same_path(tmp_path, monkeypatch):
    """A shared cache parses a path once; the second request is a hit."""
    path = tmp_path / "cached.py"
    path.write_text("x = 1\n")

    call_count = {"n": 0}
    real_parse = ast.parse

    def counting_parse(*args, **kwargs):
        call_count["n"] += 1
        return real_parse(*args, **kwargs)

    monkeypatch.setattr(ast, "parse", counting_parse)

    events: list[str] = []
    monkeypatch.setattr(
        "lhp.core.codegen.imports.source_parser.incr_event",
        lambda name, n=1: events.append(name),
    )

    cache: dict = {}
    tree_one = parse_user_module(path, cache=cache)
    tree_two = parse_user_module(path, cache=cache)

    assert call_count["n"] == 1
    assert tree_one is tree_two  # same cached object
    assert str(path.resolve()) in cache
    assert events == ["source_parse_miss", "source_parse_hit"]


@pytest.mark.unit
def test_cache_none_counts_miss_and_does_not_cache(tmp_path, monkeypatch):
    """With ``cache=None`` every call parses and counts a miss, no hit."""
    path = tmp_path / "uncached.py"
    path.write_text("y = 2\n")

    call_count = {"n": 0}
    real_parse = ast.parse

    def counting_parse(*args, **kwargs):
        call_count["n"] += 1
        return real_parse(*args, **kwargs)

    monkeypatch.setattr(ast, "parse", counting_parse)

    events: list[str] = []
    monkeypatch.setattr(
        "lhp.core.codegen.imports.source_parser.incr_event",
        lambda name, n=1: events.append(name),
    )

    parse_user_module(path, cache=None)
    parse_user_module(path, cache=None)

    assert call_count["n"] == 2
    assert events == ["source_parse_miss", "source_parse_miss"]


def _build_classification_tree(tmp_path: Path) -> ast.Module:
    """Build an on-disk root with all four import kinds and parse the entry."""
    # Local sub-package: helpers/ with __init__.py and a module.
    helpers = tmp_path / "helpers"
    helpers.mkdir()
    (helpers / "__init__.py").write_text("")
    (helpers / "date_change.py").write_text("def to_snapshot_date():\n    return 1\n")

    # Local sibling module (flat under root).
    (tmp_path / "sibling.py").write_text("VALUE = 1\n")

    entry = tmp_path / "entry.py"
    entry.write_text(
        "import os\n"  # external
        "from helpers.date_change import to_snapshot_date\n"  # absolute-local
        "from sibling import VALUE\n"  # absolute-local (sibling .py)
        "from . import something\n"  # relative
        "from .pkgmod import other\n"  # relative with module
        "import helpers.date_change\n"  # plain-dotted local
        "import helpers.date_change as hd\n"  # plain-dotted local with asname
        "from pyspark.sql import DataFrame\n"  # external
    )
    return parse_user_module(entry, cache=None)


@pytest.mark.unit
def test_classification_covers_all_four_kinds(tmp_path):
    """Relative / absolute-local / external / plain-dotted are distinguished."""
    tree = _build_classification_tree(tmp_path)

    targets = local_import_targets(tree, tmp_path)
    by_module: dict = {}
    for t in targets:
        by_module.setdefault((t.module, t.level, t.is_plain_dotted), t)

    # External: `import os` and `from pyspark.sql import DataFrame`.
    os_t = next(t for t in targets if t.module == "os" and t.level == 0)
    assert os_t.is_local is False
    assert os_t.is_plain_dotted is False

    pyspark_t = next(t for t in targets if t.module == "pyspark.sql")
    assert pyspark_t.is_local is False
    assert pyspark_t.level == 0
    assert pyspark_t.names == ("DataFrame",)

    # Absolute-local from-import of a sub-package module.
    helpers_from = next(
        t
        for t in targets
        if t.module == "helpers.date_change" and t.level == 0 and not t.is_plain_dotted
    )
    assert helpers_from.is_local is True
    assert helpers_from.names == ("to_snapshot_date",)

    # Absolute-local from-import of a flat sibling module.
    sibling_t = next(t for t in targets if t.module == "sibling")
    assert sibling_t.is_local is True
    assert sibling_t.is_plain_dotted is False

    # Relative imports: level > 0, not local-absolute, level recorded.
    relative_targets = [t for t in targets if t.level > 0]
    assert len(relative_targets) == 2
    for rel in relative_targets:
        assert rel.is_local is False
        assert rel.level == 1

    # Plain-dotted-local: `import helpers.date_change` (+ the aliased form).
    plain_dotted = [t for t in targets if t.is_plain_dotted]
    assert len(plain_dotted) == 2
    for pd in plain_dotted:
        assert pd.is_local is True
        assert pd.level == 0
        assert pd.module == "helpers.date_change"
    # The aliased form records its asname in ``names``.
    aliased = next(t for t in plain_dotted if t.names == ("hd",))
    assert aliased.module == "helpers.date_change"


@pytest.mark.unit
def test_lazy_function_body_import_is_classified(tmp_path):
    """Imports inside a function body are walked and classified, not skipped."""
    helpers = tmp_path / "helpers"
    helpers.mkdir()
    (helpers / "__init__.py").write_text("")
    (helpers / "util.py").write_text("def go():\n    return 1\n")

    entry = tmp_path / "lazy.py"
    entry.write_text(
        "def loader():\n    from helpers.util import go\n    return go()\n"
    )
    tree = parse_user_module(entry, cache=None)

    targets = local_import_targets(tree, tmp_path)

    lazy = next(t for t in targets if t.module == "helpers.util")
    assert lazy.is_local is True
    assert lazy.names == ("go",)
    assert lazy.lineno > 1  # located inside the function body


@pytest.mark.unit
def test_import_target_is_frozen():
    """``ImportTarget`` is immutable (frozen dataclass)."""
    target = ImportTarget(
        module="os",
        level=0,
        names=("os",),
        is_local=False,
        is_plain_dotted=False,
        lineno=1,
        col_offset=0,
    )
    with pytest.raises(AttributeError):
        target.is_local = True  # type: ignore[misc]
