"""Tests for the webapp file-I/O service — the security boundary.

Covers the path-traversal guard, the write/delete protection list, PUT
write semantics (parent-dir creation), unrestricted reads under
``generated/``, post-write YAML syntax diagnostics, and the recursive tree
shape with directory exclusions. Each test builds a self-contained project
under ``tmp_path``.
"""

from pathlib import Path

import pytest

from lhp.webapp.services.file_io import (
    FileIOService,
    PathTraversalError,
    WriteProtectedError,
    WriteResult,
    YamlSyntaxError,
)

pytestmark = pytest.mark.webapp


@pytest.fixture
def project_root(tmp_path: Path) -> Path:
    """A minimal project tree exercising readable/writable/protected areas."""
    root = tmp_path / "proj"
    root.mkdir()

    # Normal, writable areas.
    (root / "pipelines").mkdir()
    (root / "pipelines" / "bronze.yaml").write_text("flowgroup: bronze\n")
    (root / "lhp.yaml").write_text("name: demo\n")

    # Read-only-but-readable: generated/.
    (root / "generated").mkdir()
    (root / "generated" / "bronze.py").write_text("# generated\n")

    # Protected mutation areas.
    (root / ".git").mkdir()
    (root / ".git" / "HEAD").write_text("ref: refs/heads/main\n")
    (root / ".lhp").mkdir()
    (root / ".lhp" / "logs").mkdir()
    (root / ".lhp" / "logs" / "run.log").write_text("log\n")
    (root / ".lhp" / "dependencies").mkdir()
    (root / ".lhp" / "dependencies" / "graph.json").write_text("{}\n")
    (root / ".lhp" / "profile.yaml").write_text("active: dev\n")

    # Noise dirs that must be excluded from the tree.
    (root / "__pycache__").mkdir()
    (root / "__pycache__" / "x.pyc").write_text("")
    (root / ".venv").mkdir()
    (root / ".venv" / "pyvenv.cfg").write_text("")

    return root


@pytest.fixture
def service(project_root: Path) -> FileIOService:
    return FileIOService(project_root)


def test_traversal_relative_escape_raises(service: FileIOService) -> None:
    with pytest.raises(PathTraversalError):
        service.read_file("../../etc/passwd")


def test_traversal_absolute_outside_root_raises(
    service: FileIOService, tmp_path: Path
) -> None:
    outside = tmp_path / "outside.txt"
    outside.write_text("secret\n")
    # An absolute path joined to root via ``/`` discards the root in pathlib,
    # so this is the canonical absolute-escape attempt.
    with pytest.raises(PathTraversalError):
        service.read_file(str(outside))


def test_traversal_guard_applies_to_writes(service: FileIOService) -> None:
    with pytest.raises(PathTraversalError):
        service.write_file("../escape.txt", "x")


def test_traversal_guard_applies_to_deletes(service: FileIOService) -> None:
    with pytest.raises(PathTraversalError):
        service.delete_file("../escape.txt")


def test_write_under_generated_blocked(service: FileIOService) -> None:
    with pytest.raises(WriteProtectedError):
        service.write_file("generated/new.py", "x")


def test_write_under_git_blocked(service: FileIOService) -> None:
    with pytest.raises(WriteProtectedError):
        service.write_file(".git/config", "x")


def test_write_under_lhp_logs_blocked(service: FileIOService) -> None:
    with pytest.raises(WriteProtectedError):
        service.write_file(".lhp/logs/new.log", "x")


def test_write_under_lhp_dependencies_blocked(service: FileIOService) -> None:
    with pytest.raises(WriteProtectedError):
        service.write_file(".lhp/dependencies/new.json", "x")


def test_write_lhp_profile_allowed(service: FileIOService, project_root: Path) -> None:
    # .lhp/profile.yaml and everything else under .lhp/ (except logs/deps) stays
    # writable.
    result = service.write_file(".lhp/profile.yaml", "active: prod\n")
    assert isinstance(result, WriteResult)
    assert (project_root / ".lhp" / "profile.yaml").read_text() == "active: prod\n"


def test_write_under_pipelines_creates_dirs(
    service: FileIOService, project_root: Path
) -> None:
    result = service.write_file("pipelines/silver/orders.yaml", "flowgroup: orders\n")
    written = project_root / "pipelines" / "silver" / "orders.yaml"
    assert written.exists()
    assert written.read_text() == "flowgroup: orders\n"
    assert result.yaml_error is None


def test_delete_under_git_blocked(service: FileIOService, project_root: Path) -> None:
    with pytest.raises(WriteProtectedError):
        service.delete_file(".git/HEAD")
    # Still present.
    assert (project_root / ".git" / "HEAD").exists()


def test_delete_under_generated_blocked(service: FileIOService) -> None:
    with pytest.raises(WriteProtectedError):
        service.delete_file("generated/bronze.py")


def test_delete_normal_file_ok(service: FileIOService, project_root: Path) -> None:
    target = project_root / "pipelines" / "bronze.yaml"
    assert target.exists()
    service.delete_file("pipelines/bronze.yaml")
    assert not target.exists()


def test_delete_missing_file_raises(service: FileIOService) -> None:
    with pytest.raises(FileNotFoundError):
        service.delete_file("pipelines/does_not_exist.yaml")


def test_read_under_generated_ok(service: FileIOService) -> None:
    # generated/ is write-protected but MUST be readable (UI shows generated
    # code).
    content = service.read_file("generated/bronze.py")
    assert content == "# generated\n"


def test_read_under_git_ok(service: FileIOService) -> None:
    # Reads are unrestricted even under .git/ — only the traversal guard gates
    # reads.
    content = service.read_file(".git/HEAD")
    assert content == "ref: refs/heads/main\n"


def test_read_missing_file_raises(service: FileIOService) -> None:
    with pytest.raises(FileNotFoundError):
        service.read_file("pipelines/nope.yaml")


def test_write_bad_yaml_persists_and_reports(
    service: FileIOService, project_root: Path
) -> None:
    bad = "a:\n  b: [1, 2\n c: 3\n"  # unterminated flow sequence
    result = service.write_file("pipelines/broken.yaml", bad)

    # The write STILL persists despite the syntax error.
    persisted = project_root / "pipelines" / "broken.yaml"
    assert persisted.exists()
    assert persisted.read_text() == bad

    # Diagnostic carries 1-based line/column + a message.
    assert result.yaml_error is not None
    assert isinstance(result.yaml_error, YamlSyntaxError)
    assert result.yaml_error.line >= 1
    assert result.yaml_error.column >= 1
    assert result.yaml_error.message


def test_write_bad_yaml_line_column_one_based(
    service: FileIOService,
) -> None:
    # PyYAML reports this mark at 0-based (2, 2); the service emits 1-based.
    bad = "a:\n  b: [1, 2\n c: 3\n"
    result = service.write_file("pipelines/broken2.yaml", bad)
    assert result.yaml_error is not None
    assert result.yaml_error.line == 3
    assert result.yaml_error.column == 3


def test_write_good_yaml_no_error(service: FileIOService) -> None:
    result = service.write_file("pipelines/good.yaml", "key: value\n")
    assert result.yaml_error is None


def test_write_non_yaml_skips_check(service: FileIOService) -> None:
    # A .py file with content that is not valid YAML must NOT be checked.
    result = service.write_file("pipelines/script.py", "def f(: pass\n")
    assert result.yaml_error is None


def test_tree_shape_and_exclusions(service: FileIOService) -> None:
    tree = service.list_tree()

    # Root node shape.
    assert tree["type"] == "directory"
    assert tree["path"] == ""
    assert "children" in tree

    top_names = {c["name"] for c in tree["children"]}
    # Excluded noise dirs absent.
    assert "__pycache__" not in top_names
    assert ".venv" not in top_names
    assert ".git" not in top_names
    # Real dirs/files present (including dotfiles that are not excluded).
    assert "pipelines" in top_names
    assert "generated" in top_names
    assert "lhp.yaml" in top_names
    assert ".lhp" in top_names

    # Node field shape: every node carries name/path/type; dirs carry children.
    by_name = {c["name"]: c for c in tree["children"]}
    pipelines = by_name["pipelines"]
    assert pipelines["type"] == "directory"
    assert pipelines["path"] == "pipelines"
    assert "children" in pipelines

    lhp_yaml = by_name["lhp.yaml"]
    assert lhp_yaml["type"] == "file"
    assert lhp_yaml["path"] == "lhp.yaml"
    assert "children" not in lhp_yaml

    # Recursive: nested file path is project-relative with "/" separators.
    child_paths = {c["path"] for c in pipelines["children"]}
    assert "pipelines/bronze.yaml" in child_paths
