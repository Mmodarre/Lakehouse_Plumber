"""Test configuration and shared fixtures."""

import contextlib
import io
import logging
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

# Rich and the LHP console module are imported defensively so the
# ``packaging-check`` CI job — which installs neither the project nor
# rich into its venv — can still collect ``tests/test_packaging.py``.
# Tests that actually exercise rich rendering live elsewhere and run
# under the unit-test job, which installs ``lhp[dev]`` and therefore
# has both rich and syrupy available.
try:
    from rich.console import Console

    import lhp.cli.console as _lhp_console_module

    _RICH_AVAILABLE = True
except ImportError:
    Console = None  # type: ignore[assignment,misc]
    _lhp_console_module = None  # type: ignore[assignment]
    _RICH_AVAILABLE = False


@contextlib.contextmanager
def capture_lhp_console(width: int = 80):
    """Capture rendered output from ``lhp.cli.console.console`` for a single test block.

    Swaps the module-level ``console`` singleton with a deterministic
    no-color in-memory Console at the given width, yields the underlying
    StringIO buffer, then restores the prior console on exit. Composes
    with the autouse ``_isolate_lhp_console`` fixture: the autouse fixture
    sets the per-test default, this helper temporarily overrides it
    within a ``with`` block, and exit restores the autouse default.

    Importable as ``from conftest import capture_lhp_console`` because
    pytest inserts the testpaths root onto ``sys.path`` when there is no
    ``tests/__init__.py``, making conftest a top-level module.
    """
    buf = io.StringIO()
    fake = Console(file=buf, force_terminal=False, no_color=True, width=width)
    saved = _lhp_console_module.console
    _lhp_console_module.console = fake
    try:
        yield buf
    finally:
        _lhp_console_module.console = saved


@pytest.fixture(autouse=True)
def _isolate_lhp_console(monkeypatch):
    """Per-test Rich Console with deterministic settings.

    Replaces the module-level ``console`` and ``err_console`` singletons with
    Consoles that emit plain text at a fixed wide width so assertions are not
    perturbed by terminal width, ANSI codes, or TTY detection.

    No-ops when rich (and therefore ``lhp.cli.console``) is not importable —
    needed so the ``packaging-check`` CI job, which runs against a minimal
    venv, can still collect ``tests/test_packaging.py``.
    """
    if not _RICH_AVAILABLE:
        yield
        return
    monkeypatch.setattr(
        _lhp_console_module,
        "console",
        Console(force_terminal=False, no_color=True, width=999),
    )
    monkeypatch.setattr(
        _lhp_console_module,
        "err_console",
        Console(stderr=True, force_terminal=False, no_color=True, width=999),
    )
    yield


def pytest_addoption(parser):
    parser.addoption(
        "--update-baselines",
        action="store_true",
        default=False,
        help="Update golden test baseline files with current generator output",
    )


@pytest.fixture
def golden(request):
    """Fixture for golden output test comparison.

    Usage: golden(actual_code, "load_cloudfiles")
    Run with --update-baselines to regenerate baseline files.
    """
    baseline_dir = Path(__file__).parent / "baselines"
    update = request.config.getoption("--update-baselines")

    def check(actual: str, name: str):
        path = baseline_dir / f"{name}.py"
        if update:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(actual)
        else:
            assert path.exists(), (
                f"Baseline {path} missing — run: pytest --update-baselines -k golden"
            )
            expected = path.read_text()
            assert actual == expected, (
                f"Output differs from baseline {path}. "
                f"Run --update-baselines if change is intentional."
            )

    return check


def force_close_all_log_handlers():
    """Force close all logging handlers to release file locks on Windows."""
    root_logger = logging.getLogger()

    for handler in root_logger.handlers[:]:
        try:
            handler.close()
        except Exception:
            pass
        root_logger.removeHandler(handler)

    logging.basicConfig(force=True)


@pytest.fixture(autouse=True)
def clean_logging():
    """Automatically clean up logging for all tests to prevent Windows file locking."""
    force_close_all_log_handlers()

    yield

    force_close_all_log_handlers()


@pytest.fixture
def isolated_project():
    """Create a completely isolated temporary project directory with proper cleanup."""
    temp_dir = None
    try:
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
    finally:
        if temp_dir and temp_dir.exists():
            force_close_all_log_handlers()

            try:
                shutil.rmtree(temp_dir)
            except PermissionError:
                if sys.platform == "win32":
                    import gc
                    import time

                    gc.collect()
                    time.sleep(0.1)

                    try:
                        shutil.rmtree(temp_dir)
                    except PermissionError:
                        try:
                            temp_dir.rename(temp_dir.with_suffix(".cleanup"))
                        except Exception:
                            pass
                else:
                    raise


@pytest.fixture
def mock_logging_config():
    """Mock the configure_logging function to prevent file creation during tests."""
    with patch("lhp.cli.main.configure_logging") as mock_config:
        mock_config.return_value = Path(tempfile.gettempdir()) / "mock_lhp.log"
        yield mock_config


@pytest.fixture
def temp_project_with_logging_cleanup():
    """Create a temporary project with proper logging cleanup."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_root = Path(tmpdir)

        force_close_all_log_handlers()

        logging.basicConfig(
            level=logging.WARNING,
            format="%(levelname)s: %(message)s",
            force=True,
        )

        yield project_root

        force_close_all_log_handlers()


@pytest.fixture
def windows_safe_tempdir():
    """Create a temporary directory with Windows-safe cleanup."""
    if sys.platform != "win32":
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    else:
        temp_dir = Path(tempfile.mkdtemp())
        try:
            yield temp_dir
        finally:
            force_close_all_log_handlers()

            for attempt in range(3):
                try:
                    shutil.rmtree(temp_dir)
                    break
                except PermissionError:
                    if attempt < 2:
                        import gc
                        import time

                        gc.collect()
                        time.sleep(0.1 * (attempt + 1))
                    else:
                        try:
                            temp_dir.rename(temp_dir.with_suffix(f".cleanup.{attempt}"))
                        except Exception:
                            pass


def pytest_configure(config):
    """Wire the per-action generators into the core ``ActionRegistry``.

    In production this is triggered lazily at the single composition point
    (``LakehousePlumberApplicationFacade.for_project``); ``import lhp`` no
    longer eagerly imports ``lhp.generators.registration``. Tests that
    construct ``ActionRegistry()`` directly never go through the facade, and
    some do so at module import time (e.g. ``tests/generators/test/``), which
    happens during collection — before any fixture runs. ``pytest_configure``
    runs *before* collection, so registration is in place when those
    module-level registries are built. ``register_all`` is idempotent (later
    calls update), so re-running it here is harmless.
    """
    try:
        from lhp.generators.registration import register_all
    except ImportError:
        # The ``packaging-check`` CI job installs neither the project nor its
        # runtime deps (pydantic) into its venv — it only builds a wheel in a
        # subprocess via ``tests/test_packaging.py``. There is no in-process
        # ``lhp`` to register against there, so skip. Mirrors the defensive
        # rich/console import guard at the top of this module.
        return

    register_all()


@pytest.fixture(autouse=True, scope="session")
def configure_test_logging():
    """Configure logging for the entire test session."""
    logging.getLogger("lhp").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.ERROR)

    yield

    force_close_all_log_handlers()


@pytest.fixture
def create_flowgroup():
    """Factory fixture to create FlowGroup with minimal valid structure."""
    from lhp.models import Action, ActionType, FlowGroup

    def _create(
        pipeline: str,
        flowgroup: str,
        job_name: str | None = None,
        source_table: str = "raw.source",
        target_table: str = "bronze.target",
    ):
        return FlowGroup(
            pipeline=pipeline,
            flowgroup=flowgroup,
            job_name=job_name,
            actions=[
                Action(
                    name=f"load_{flowgroup}",
                    type=ActionType.LOAD,
                    source=source_table,
                    target=f"v_{flowgroup}_raw",
                ),
                Action(
                    name=f"write_{flowgroup}",
                    type=ActionType.WRITE,
                    source=f"v_{flowgroup}_raw",
                    write_target={
                        "type": "streaming_table",
                        "database": "bronze",
                        "table": target_table,
                    },
                ),
            ],
        )

    return _create


@pytest.fixture
def sample_flowgroups_with_job_name(create_flowgroup):
    """3 flowgroups: 2 bronze, 1 silver."""
    return [
        create_flowgroup(
            "bronze_pipeline", "bronze_fg1", "bronze_job", "raw.table1", "bronze.table1"
        ),
        create_flowgroup(
            "bronze_pipeline", "bronze_fg2", "bronze_job", "raw.table2", "bronze.table2"
        ),
        create_flowgroup(
            "silver_pipeline",
            "silver_fg1",
            "silver_job",
            "bronze.table1",
            "silver.table1",
        ),
    ]


@pytest.fixture
def sample_flowgroups_mixed_job_name(create_flowgroup):
    """4 flowgroups: 2 with job_name, 2 without."""
    return [
        create_flowgroup("bronze_pipeline", "fg1", "bronze_job"),
        create_flowgroup("bronze_pipeline", "fg2", "bronze_job"),
        create_flowgroup("silver_pipeline", "fg3", None),
        create_flowgroup("silver_pipeline", "fg4", None),
    ]


@pytest.fixture
def sample_multi_doc_job_config(tmp_path):
    """Creates temp multi-doc job_config.yaml at tmp_path/config/job_config.yaml."""
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)

    config_content = """project_defaults:
  max_concurrent_runs: 1
  performance_target: STANDARD
  tags:
    env: dev
    managed_by: lhp
---
job_name: bronze_job
max_concurrent_runs: 2
tags:
  layer: bronze
---
job_name: silver_job
performance_target: PERFORMANCE_OPTIMIZED
tags:
  layer: silver
"""
    config_file = config_dir / "job_config.yaml"
    config_file.write_text(config_content)
    return tmp_path


@pytest.fixture
def mock_dependency_result():
    """Creates mock DependencyAnalysisResult with required attributes."""
    from unittest.mock import Mock

    from lhp.models.dependencies import DependencyAnalysisResult

    result = Mock(spec=DependencyAnalysisResult)
    result.total_pipelines = 2
    result.execution_stages = [["pipeline1"], ["pipeline2"]]
    result.pipeline_dependencies = {
        "pipeline1": Mock(
            depends_on=[],
            flowgroup_count=1,
            action_count=2,
            external_sources=[],
            stage=1,
        ),
        "pipeline2": Mock(
            depends_on=["pipeline1"],
            flowgroup_count=1,
            action_count=2,
            external_sources=[],
            stage=2,
        ),
    }
    result.external_sources = []
    result.circular_dependencies = []
    return result
