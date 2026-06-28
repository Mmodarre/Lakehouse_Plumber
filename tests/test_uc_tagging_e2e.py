"""End-to-end test: UC tagging hook is emitted by a real generate run."""

import os
import shutil
import tempfile
from pathlib import Path

import pytest

from lhp.api import LakehousePlumberApplicationFacade, collect_response


def _write_project(root: Path, uc_tagging_block: str = "") -> None:
    root.mkdir(parents=True)
    (root / "lhp.yaml").write_text(
        'name: test_project\nversion: "1.0"\n' + uc_tagging_block
    )

    subs = root / "substitutions"
    subs.mkdir()
    (subs / "dev.yaml").write_text("dev:\n  catalog: prod_cat\n")

    schemas = root / "schemas"
    schemas.mkdir()
    (schemas / "orders.yaml").write_text(
        "name: orders\n"
        "columns:\n"
        "  - name: id\n"
        "    type: BIGINT\n"
        "  - name: email\n"
        "    type: STRING\n"
        "    tags:\n"
        "      classification: pii\n"
    )

    pipes = root / "pipelines"
    pipes.mkdir()
    (pipes / "p.yaml").write_text(
        "pipeline: tagging_pipe\n"
        "flowgroup: fg\n"
        "actions:\n"
        "  - name: load_orders\n"
        "    type: load\n"
        "    source:\n"
        "      type: delta\n"
        "      database: '{catalog}.raw'\n"
        "      table: orders\n"
        "    target: v_orders\n"
        "  - name: write_orders\n"
        "    type: write\n"
        "    source: v_orders\n"
        "    write_target:\n"
        "      type: streaming_table\n"
        "      catalog: '{catalog}'\n"
        "      schema: bronze\n"
        "      table: orders\n"
        "      table_schema: schemas/orders.yaml\n"
        "      tags:\n"
        "        team: data-eng\n"
        "        pii: ''\n"
    )


def _generate(root: Path):
    facade = LakehousePlumberApplicationFacade.for_project(root, enforce_version=False)
    collect_response(
        facade.generation.generate_pipelines(
            pipeline_filter="tagging_pipe",
            env="dev",
            output_dir=root / "generated",
        )
    )
    return root / "generated" / "tagging_pipe" / "_uc_tagging_hook.py"


@pytest.fixture
def project_dir():
    tmp = Path(tempfile.mkdtemp())
    cwd = os.getcwd()
    try:
        yield tmp / "proj"
    finally:
        os.chdir(cwd)
        shutil.rmtree(tmp, ignore_errors=True)


@pytest.mark.e2e
def test_uc_tagging_hook_emitted_with_substituted_names(project_dir):
    _write_project(project_dir, uc_tagging_block="uc_tagging:\n  enabled: true\n")
    hook = _generate(project_dir)

    assert hook.exists(), "expected _uc_tagging_hook.py to be generated"
    content = hook.read_text()

    # Substituted, fully-qualified table name embedded (no {catalog} token left).
    # Quote-agnostic: the terminal ruff pass may normalize quote style.
    assert "prod_cat.bronze.orders" in content
    assert "{catalog}" not in content
    # Table tags (key-only normalized to empty string) and column tags present
    assert "data-eng" in content
    assert "classification" in content and "pii" in content
    # REST-based, additive by default
    assert "/api/2.1/unity-catalog/entity-tag-assignments" in content
    assert "_REMOVE_UNDECLARED_TAGS = False" in content
    # Trigger on update_progress pipeline-terminal states; existing state read once
    # from information_schema; 16-thread pool (default).
    assert 'event_type") != "update_progress"' in content
    assert "_TERMINAL_PIPELINE_STATES" in content
    assert "system.information_schema.table_tags" in content
    assert "ThreadPoolExecutor(max_workers=16)" in content


@pytest.mark.e2e
def test_no_hook_when_uc_tagging_disabled(project_dir):
    _write_project(
        project_dir, uc_tagging_block="uc_tagging:\n  enabled: false\n"
    )
    hook = _generate(project_dir)
    assert not hook.exists()


@pytest.mark.e2e
def test_hook_emitted_when_block_absent(project_dir):
    # On by default: tags declared with no uc_tagging block → hook IS generated.
    _write_project(project_dir)  # default: no uc_tagging block
    hook = _generate(project_dir)
    assert hook.exists()
