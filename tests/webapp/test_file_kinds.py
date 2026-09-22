"""Tests for the project-relative path -> file-kind classifier.

Covers the ordered rule table (one case per kind), nesting depth, Windows
separators and other normalization inputs, case sensitivity, and the parity
contract with the frontend mirror (``web_app/src/lib/monaco-setup.ts``).
``FileIOService.file_exists`` is covered here too because it is the
existence probe the same call sites pair with the classifier.
"""

from pathlib import Path

import pytest

from lhp.webapp.services.file_io import FileIOService, PathTraversalError
from lhp.webapp.services.file_kinds import (
    FRONTEND_MIRROR_GLOBS,
    FileKind,
    classify_path,
)

pytestmark = pytest.mark.webapp

# tests/webapp/test_file_kinds.py -> tests/ -> repo root -> web_app/...
_MONACO_SETUP = (
    Path(__file__).resolve().parents[2] / "web_app" / "src" / "lib" / "monaco-setup.ts"
)


# ---------------------------------------------------------------- classify_path


@pytest.mark.parametrize(
    ("relative_path", "expected"),
    [
        # project_config — root only, both YAML spellings.
        ("lhp.yaml", FileKind.PROJECT_CONFIG),
        ("lhp.yml", FileKind.PROJECT_CONFIG),
        ("config/lhp.yaml", FileKind.OTHER),
        # pipeline_config / job_config — name-prefixed files directly in config/.
        ("config/pipeline_config.yaml", FileKind.PIPELINE_CONFIG),
        ("config/pipeline_config_dev.yml", FileKind.PIPELINE_CONFIG),
        ("config/job_config.yaml", FileKind.JOB_CONFIG),
        ("config/job_config_dev.yml", FileKind.JOB_CONFIG),
        ("config/monitoring_job_config.yaml", FileKind.JOB_CONFIG),
        ("config/monitoring_job_config_dev.yml", FileKind.JOB_CONFIG),
        ("config/nested/pipeline_config.yaml", FileKind.OTHER),
        ("config/pipeline_config.json", FileKind.OTHER),
        # flowgroup — any depth under pipelines/, including no nesting at all.
        ("pipelines/x.yaml", FileKind.FLOWGROUP),
        ("pipelines/a/b/c.yml", FileKind.FLOWGROUP),
        ("pipelines/x.sql", FileKind.SQL),
        # preset / substitution — top level only.
        ("presets/bronze.yaml", FileKind.PRESET),
        ("presets/bronze.yml", FileKind.PRESET),
        ("presets/nested/x.yaml", FileKind.OTHER),
        ("substitutions/dev.yaml", FileKind.SUBSTITUTION),
        ("substitutions/nested/dev.yaml", FileKind.OTHER),
        # template / blueprint — any depth.
        ("templates/t.yaml", FileKind.TEMPLATE),
        ("templates/a/b/t.yml", FileKind.TEMPLATE),
        ("blueprints/b.yaml", FileKind.BLUEPRINT),
        ("blueprints/a/b.yml", FileKind.BLUEPRINT),
        # schema — any depth, any extension.
        ("schemas/customers.yaml", FileKind.SCHEMA),
        ("schemas/a/b/customers.json", FileKind.SCHEMA),
        ("schemas/a/b/loader.py", FileKind.SCHEMA),
        # sandbox_profile — the one recognized file under .lhp/.
        (".lhp/profile.yaml", FileKind.SANDBOX_PROFILE),
        (".lhp/profile.yml", FileKind.OTHER),
        (".lhp/logs/run.log", FileKind.OTHER),
        # sql / python — anywhere, by extension.
        ("queries/customers.sql", FileKind.SQL),
        ("customers.ddl", FileKind.SQL),
        ("generated/bronze/customers.py", FileKind.PYTHON),
        ("setup.py", FileKind.PYTHON),
        # other — anything unrecognized, including empty input.
        ("README.md", FileKind.OTHER),
        ("", FileKind.OTHER),
        ("pipelines", FileKind.OTHER),
    ],
)
def test_classify_path_table(relative_path: str, expected: FileKind) -> None:
    assert classify_path(relative_path) == expected


@pytest.mark.parametrize(
    ("relative_path", "expected"),
    [
        ("pipelines\\a\\b.yaml", FileKind.FLOWGROUP),
        ("config\\job_config_dev.yaml", FileKind.JOB_CONFIG),
        (".lhp\\profile.yaml", FileKind.SANDBOX_PROFILE),
        ("/pipelines/bronze.yaml", FileKind.FLOWGROUP),
        ("./presets/bronze.yaml", FileKind.PRESET),
        ("pipelines//a//b.yaml", FileKind.FLOWGROUP),
        (".\\.\\lhp.yaml", FileKind.PROJECT_CONFIG),
    ],
)
def test_classify_path_normalizes_input(relative_path: str, expected: FileKind) -> None:
    assert classify_path(relative_path) == expected


@pytest.mark.parametrize(
    "relative_path",
    [
        "LHP.YAML",
        "Lhp.yaml",
        "Pipelines/bronze.yaml",
        "pipelines/bronze.YAML",
        "PRESETS/bronze.yaml",
        "config/Pipeline_Config.yaml",
        "queries/customers.SQL",
        "generated/bronze.PY",
    ],
)
def test_classify_path_is_case_sensitive(relative_path: str) -> None:
    """Case is never folded — on Windows either, where ``fnmatch`` would."""
    assert classify_path(relative_path) == FileKind.OTHER


def test_file_kind_members_and_order() -> None:
    """The member order is the canonical kind order shared with the design."""
    assert [kind.value for kind in FileKind] == [
        "flowgroup",
        "preset",
        "template",
        "substitution",
        "project_config",
        "pipeline_config",
        "job_config",
        "blueprint",
        "schema",
        "sandbox_profile",
        "sql",
        "python",
        "other",
    ]


def test_file_kind_is_a_str_enum() -> None:
    """Members serialize as their bare value, so props need no conversion."""
    assert isinstance(FileKind.FLOWGROUP, str)
    assert FileKind.FLOWGROUP == "flowgroup"
    assert f"{FileKind.FLOWGROUP}" == "flowgroup"


# ------------------------------------------------------- frontend mirror parity


def test_frontend_mirror_covers_the_shared_kinds() -> None:
    assert set(FRONTEND_MIRROR_GLOBS) == {
        FileKind.FLOWGROUP,
        FileKind.PRESET,
        FileKind.TEMPLATE,
        FileKind.SUBSTITUTION,
        FileKind.PROJECT_CONFIG,
        FileKind.PIPELINE_CONFIG,
        FileKind.JOB_CONFIG,
        FileKind.SCHEMA,
    }


def test_frontend_mirror_globs_appear_in_monaco_setup() -> None:
    """Every mirrored glob must be a literal in the frontend's match table.

    The Python table is canonical; this fails the moment the two drift. The
    quotes are part of the needle so a glob cannot be satisfied by prose in
    the comment block above the frontend's table, which names several of
    these paths.
    """
    assert _MONACO_SETUP.is_file(), (
        f"Frontend mirror not found at {_MONACO_SETUP}; the parity contract "
        "in file_kinds.py depends on it."
    )
    source = _MONACO_SETUP.read_text(encoding="utf-8")
    missing = [
        glob
        for globs in FRONTEND_MIRROR_GLOBS.values()
        for glob in globs
        if f"'{glob}'" not in source
    ]
    assert missing == []


@pytest.mark.parametrize(
    ("kind", "glob"),
    [(kind, glob) for kind, globs in FRONTEND_MIRROR_GLOBS.items() for glob in globs],
)
def test_frontend_mirror_globs_classify_to_their_kind(
    kind: FileKind, glob: str
) -> None:
    """A concrete path built from each mirrored glob classifies to that kind.

    Guards the mirror constant against drifting away from the rule table it
    documents.
    """
    sample = glob.replace("**/", "nested/").replace("*", "x")
    assert classify_path(sample) == kind


# ----------------------------------------------------- FileIOService.file_exists


@pytest.fixture
def project_root(tmp_path: Path) -> Path:
    root = tmp_path / "proj"
    (root / "pipelines" / "bronze").mkdir(parents=True)
    (root / "pipelines" / "bronze" / "customers.yaml").write_text("flowgroup: b\n")
    (root / "lhp.yaml").write_text("name: demo\n")
    return root


@pytest.fixture
def service(project_root: Path) -> FileIOService:
    return FileIOService(project_root)


def test_file_exists_true_for_existing_file(service: FileIOService) -> None:
    assert service.file_exists("lhp.yaml") is True
    assert service.file_exists("pipelines/bronze/customers.yaml") is True


def test_file_exists_false_for_missing_file(service: FileIOService) -> None:
    assert service.file_exists("pipelines/bronze/orders.yaml") is False
    assert service.file_exists("nope/at/all.yaml") is False


def test_file_exists_false_for_directory(service: FileIOService) -> None:
    assert service.file_exists("pipelines") is False
    assert service.file_exists("pipelines/bronze") is False


def test_file_exists_traversal_raises(service: FileIOService, tmp_path: Path) -> None:
    """Same guard contract as ``read_bytes_if_exists``: escapes raise."""
    outside = tmp_path / "outside.yaml"
    outside.write_text("x: 1\n")
    with pytest.raises(PathTraversalError):
        service.file_exists("../outside.yaml")
    with pytest.raises(PathTraversalError):
        service.file_exists(str(outside))
