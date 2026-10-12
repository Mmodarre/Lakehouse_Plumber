"""Unresolved ``${name}`` in a UC ``tags_file`` raises LHP-CFG-010 (#298).

The tagging hook substitutes ``${token}`` references in the tag keys and values
it reads from a ``tags_file``. A plain ``${name}`` still left afterwards would
be written to Unity Catalog as literal text, so:

- ``build_uc_tagging_hook_files`` (the generate path) raises LHP-CFG-010;
- ``check_tags_file_tokens`` (the per-action check flowgroup resolution runs,
  so ``lhp validate`` reports the same error) raises it for exactly the write
  targets the hook tags, and is a no-op for the others.

Real substitution managers and on-disk tags files.
"""

from pathlib import Path

import pytest

from lhp.core.codegen import check_tags_file_tokens
from lhp.core.codegen.uc_tagging import build_uc_tagging_hook_files
from lhp.core.processing import EnhancedSubstitutionManager
from lhp.errors import LHPError
from lhp.models import Action, ActionType, FlowGroup, ProjectConfig, UCTaggingConfig

pytestmark = pytest.mark.unit

_TAGS_FILE = "schemas/orders.yaml"


def _mgr(tmp_path: Path, *, skip_validation: bool = False):
    sub_file = tmp_path / "dev.yaml"
    sub_file.write_text("dev:\n  owner: platform\n")
    return EnhancedSubstitutionManager(
        substitution_file=sub_file, env="dev", skip_validation=skip_validation
    )


def _tags_file(tmp_path: Path, body: str) -> str:
    path = tmp_path / _TAGS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return _TAGS_FILE


def _write(**target_extra) -> Action:
    return Action(
        name="write_orders",
        type=ActionType.WRITE,
        source="v_orders",
        write_target={
            "type": "streaming_table",
            "catalog": "c",
            "schema": "s",
            "table": "orders",
            "tags_file": _TAGS_FILE,
            **target_extra,
        },
    )


def _config(enabled: bool = True) -> ProjectConfig:
    return ProjectConfig(name="p", uc_tagging=UCTaggingConfig(enabled=enabled))


def _hook(tmp_path, mgr, action=None):
    return build_uc_tagging_hook_files(
        pipeline_name="p1",
        flowgroups=[
            FlowGroup(pipeline="p1", flowgroup="fg1", actions=[action or _write()])
        ],
        project_config=_config(),
        project_root=tmp_path,
        substitution_mgr=mgr,
    )


def _check(tmp_path, mgr, action=None, *, enabled: bool = True):
    check_tags_file_tokens(
        action or _write(),
        mgr,
        project_config=_config(enabled),
        project_root=tmp_path,
        pipeline="p1",
        flowgroup="fg1",
    )


_UNDEFINED = (
    "table: orders\n"
    "tags:\n"
    '  team: "${missing_team}"\n'
    "columns:\n"
    "  - name: email\n"
    "    tags:\n"
    '      "${missing_key}": pii\n'
)


class TestHookGeneratePath:
    def test_undefined_token_raises_cfg_010_naming_file_and_tokens(self, tmp_path):
        _tags_file(tmp_path, _UNDEFINED)

        with pytest.raises(LHPError) as excinfo:
            _hook(tmp_path, _mgr(tmp_path))

        error = excinfo.value
        assert error.code == "LHP-CFG-010"
        assert _TAGS_FILE in error.details
        assert error.context["File"] == _TAGS_FILE
        assert error.context["Unresolved"] == "${missing_team}, ${missing_key}"

    def test_defined_token_is_substituted(self, tmp_path):
        _tags_file(tmp_path, 'table: orders\ntags:\n  team: "${owner}"\n')

        files = _hook(tmp_path, _mgr(tmp_path))

        assert "'team': 'platform'" in next(iter(files.values()))

    def test_dotted_reference_passes_as_written(self, tmp_path):
        _tags_file(tmp_path, 'table: orders\ntags:\n  team: "${spark.team}"\n')

        files = _hook(tmp_path, _mgr(tmp_path))

        assert "'team': '${spark.team}'" in next(iter(files.values()))

    def test_skip_validation_leaves_token(self, tmp_path):
        _tags_file(tmp_path, _UNDEFINED)

        files = _hook(tmp_path, _mgr(tmp_path, skip_validation=True))

        assert "${missing_team}" in next(iter(files.values()))

    def test_without_a_manager_nothing_is_checked(self, tmp_path):
        _tags_file(tmp_path, _UNDEFINED)

        files = _hook(tmp_path, None)

        assert "${missing_team}" in next(iter(files.values()))


class TestCheckTagsFileTokens:
    def test_undefined_token_raises_with_action_context(self, tmp_path):
        _tags_file(tmp_path, _UNDEFINED)

        with pytest.raises(LHPError) as excinfo:
            _check(tmp_path, _mgr(tmp_path))

        error = excinfo.value
        assert error.code == "LHP-CFG-010"
        assert error.context["File"] == _TAGS_FILE
        assert error.context["Unresolved"] == "${missing_team}, ${missing_key}"
        assert error.context["Pipeline"] == "p1"
        assert error.context["FlowGroup"] == "fg1"
        assert error.context["Action"] == "write_orders"

    def test_defined_and_dotted_tokens_pass(self, tmp_path):
        _tags_file(
            tmp_path,
            'table: orders\ntags:\n  team: "${owner}"\n  env: "${spark.env}"\n',
        )

        _check(tmp_path, _mgr(tmp_path))

    def test_skip_validation_is_a_no_op(self, tmp_path):
        _tags_file(tmp_path, _UNDEFINED)

        _check(tmp_path, _mgr(tmp_path, skip_validation=True))

    def test_disabled_uc_tagging_is_a_no_op(self, tmp_path):
        """The hook never reads the file, so the check does not either."""
        _check(tmp_path, _mgr(tmp_path), enabled=False)

    @pytest.mark.parametrize(
        "action",
        [
            _write(temporary=True),
            _write(create_table=False),
            Action(name="load", type=ActionType.LOAD, source={"type": "sql"}),
            Action(
                name="sink",
                type=ActionType.WRITE,
                write_target={"type": "sink", "tags_file": _TAGS_FILE},
            ),
        ],
        ids=["temporary", "no_create_table", "load", "sink"],
    )
    def test_actions_the_hook_does_not_tag_are_not_checked(self, tmp_path, action):
        _check(tmp_path, _mgr(tmp_path), action)

    def test_inline_tags_are_not_read_here(self, tmp_path):
        action = _write(tags_file=None, tags={"team": "${missing}"})

        _check(tmp_path, _mgr(tmp_path), action)

    def test_missing_tags_file_raises_io_001(self, tmp_path):
        with pytest.raises(LHPError) as excinfo:
            _check(tmp_path, _mgr(tmp_path))

        assert excinfo.value.code == "LHP-IO-001"


_LOCATED_TITLE = (
    "Unresolved substitution token(s) ${missing_team}, ${missing_key} in " + _TAGS_FILE
)


class TestTagsFileErrorWording:
    """The title names the tags file and tokens, for ``lhp validate``'s one-line
    summary and the editor; tags are applied through the REST API, not SQL, so
    the details carry no Spark note."""

    def test_generate_path_title_and_details(self, tmp_path):
        _tags_file(tmp_path, _UNDEFINED)

        with pytest.raises(LHPError) as excinfo:
            _hook(tmp_path, _mgr(tmp_path))

        assert excinfo.value.title == _LOCATED_TITLE
        assert "Spark" not in excinfo.value.details
        assert not any("configuration:" in s for s in excinfo.value.suggestions)

    def test_validate_path_title_and_details(self, tmp_path):
        _tags_file(tmp_path, _UNDEFINED)

        with pytest.raises(LHPError) as excinfo:
            _check(tmp_path, _mgr(tmp_path))

        assert excinfo.value.title == _LOCATED_TITLE
        assert "Spark" not in excinfo.value.details
