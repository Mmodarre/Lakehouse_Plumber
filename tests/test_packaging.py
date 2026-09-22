"""Wheel-content regression test.

Guards against ``package-data`` drift in ``pyproject.toml``: every
version-controlled file under ``src/lhp/templates/`` must be present in the
built wheel. The v0.8.2 incident (TemplateNotFound for
``monitoring/union_event_logs.py.j2`` on Windows) was caused by such drift.

The expected-file set is derived from ``git ls-files``, not the working tree,
so developer-local noise (e.g. editor ``.vscode/`` dirs, ``.DS_Store``) does
not generate false positives.
"""

import json
import pathlib
import subprocess
import sys
import zipfile

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent


def _tracked_template_files() -> set[str]:
    result = subprocess.run(
        ["git", "ls-files", "src/lhp/templates"],
        cwd=REPO,
        check=True,
        capture_output=True,
        text=True,
    )
    tracked = set()
    for line in result.stdout.splitlines():
        # Rewrite the repo-relative path (src/lhp/templates/...) to the
        # wheel-relative path (lhp/templates/...).
        wheel_path = line.removeprefix("src/")
        tracked.add(wheel_path)
    return tracked


@pytest.fixture(scope="module")
def built_wheel(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    """Build the wheel once and share it across the slow packaging tests.

    Module-scoped so ``python -m build`` runs a single time even though more
    than one test inspects the resulting wheel contents.
    """
    outdir = tmp_path_factory.mktemp("wheel")
    subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", str(outdir)],
        cwd=REPO,
        check=True,
    )
    wheels = list(outdir.glob("lakehouse_plumber-*.whl"))
    assert len(wheels) == 1, f"Expected exactly one wheel, got {wheels}"
    return wheels[0]


@pytest.mark.slow
def test_wheel_contains_every_template_file(built_wheel: pathlib.Path) -> None:
    with zipfile.ZipFile(built_wheel) as zf:
        names = set(zf.namelist())

    expected = _tracked_template_files()
    assert expected, (
        "Git found no tracked files under src/lhp/templates/ — refusing to trust an empty expectation set."
    )

    missing = expected - names
    assert not missing, (
        f"{len(missing)} template file(s) missing from wheel distribution:\n"
        + "\n".join(f"  - {m}" for m in sorted(missing))
    )


@pytest.mark.slow
def test_wheel_contains_webapp_static_assets(built_wheel: pathlib.Path) -> None:
    """The built wheel must ship the SPA assets under ``lhp/webapp/static/``.

    The frontend is built by ``scripts/build_webapp.sh`` into
    ``src/lhp/webapp/static/`` (gitignored). CI runs that script before the
    wheel build, so the assets are always present there. Locally the static
    dir is usually absent (mid-refactor / no Node toolchain), so this test
    skips cleanly rather than failing.
    """
    static_index = REPO / "src" / "lhp" / "webapp" / "static" / "index.html"
    if not static_index.is_file():
        pytest.skip(
            "src/lhp/webapp/static/index.html is absent — run "
            "scripts/build_webapp.sh to build the SPA assets first "
            "(CI builds them before the wheel, so this never skips in CI)."
        )

    with zipfile.ZipFile(built_wheel) as zf:
        names = set(zf.namelist())

    assert "lhp/webapp/static/index.html" in names, (
        "lhp/webapp/static/index.html is missing from the built wheel — "
        "check the 'lhp.webapp' package-data globs in pyproject.toml."
    )

    asset_files = [
        n
        for n in names
        if n.startswith("lhp/webapp/static/assets/") and not n.endswith("/")
    ]
    assert asset_files, (
        "No files found under lhp/webapp/static/assets/ in the built wheel — "
        "check the 'lhp.webapp' package-data globs in pyproject.toml."
    )


@pytest.mark.slow
def test_wheel_contains_field_help_and_template_authoring_assets(
    built_wheel: pathlib.Path,
) -> None:
    """Verify actual packaged help/schema bytes, including the new subdirectory."""
    categories = {"project", "pipeline_config", "job_config", "flowgroup", "template"}
    help_paths = {f"lhp/schemas/help/{kind}.json" for kind in categories}
    expected = help_paths | {
        "lhp/schemas/help/source-review.json",
        "lhp/schemas/template.schema.json",
        "lhp/webapp/schemas/template_authoring.py",
    }
    with zipfile.ZipFile(built_wheel) as wheel:
        missing = expected - set(wheel.namelist())
        assert not missing, (
            "Field guidance/template authoring assets missing from the wheel:\n"
            + "\n".join(sorted(missing))
        )
        for path in expected:
            assert wheel.read(path) == (REPO / "src" / path).read_bytes(), (
                f"The wheel contains stale or altered authoring content: {path}"
            )
        for path in help_paths:
            catalog = json.loads(wheel.read(path))
            assert catalog["version"] == 1 and catalog["entries"], (
                f"The wheel's help catalog is empty or invalid: {path}"
            )


def test_py_typed_marker_ships_with_package() -> None:
    """Without py.typed, mypy/pyright treat the package as untyped. Required by §1.13 + TARGET §8."""
    import importlib.resources

    assert importlib.resources.files("lhp").joinpath("py.typed").is_file()
