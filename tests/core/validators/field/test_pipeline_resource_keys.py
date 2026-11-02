"""Unit tests for pipeline_config keys LHP also writes into the resource (#351).

LHP writes ``name``, ``libraries`` and ``root_path`` into every pipeline
resource. ``libraries`` entries are appended after LHP's glob library, so their
shape is checked; ``name`` and ``root_path`` are rejected. Every failure is
``LHP-CFG-072``.
"""

from __future__ import annotations

import pytest

from lhp.core.validators import validate_pipeline_resource_keys
from lhp.errors import LHPConfigError

pytestmark = pytest.mark.unit

_GLOB = {"glob": {"include": "${workspace.file_path}/hooks/**"}}


def _pipeline_error(config) -> LHPConfigError:
    with pytest.raises(LHPConfigError) as exc_info:
        validate_pipeline_resource_keys(config)
    assert exc_info.value.code == "LHP-CFG-072"
    return exc_info.value


class TestPipelineResourceKeys:
    def test_config_without_generated_keys_passes(self):
        validate_pipeline_resource_keys({"serverless": True, "run_as": {}})

    def test_glob_libraries_pass(self):
        validate_pipeline_resource_keys({"libraries": [_GLOB, _GLOB]})

    def test_empty_libraries_list_passes(self):
        validate_pipeline_resource_keys({"libraries": []})

    @pytest.mark.parametrize("key", ["root_path", "name"])
    def test_lhp_owned_key_rejected(self, key):
        error = _pipeline_error({key: "anything"})
        assert f"'{key}'" in str(error)

    def test_libraries_not_a_list_rejected(self):
        error = _pipeline_error({"libraries": "${var.my_libraries}"})
        assert "list" in str(error)

    @pytest.mark.parametrize("kind", ["file", "notebook"])
    def test_path_library_rejected_with_glob_rewrite(self, kind):
        error = _pipeline_error({"libraries": [{kind: {"path": "/Workspace/x.py"}}]})
        message = str(error)
        assert f"'{kind}'" in message
        # The fix is shown with the user's own path as a glob include.
        assert "include: /Workspace/x.py" in message

    @pytest.mark.parametrize("kind", ["jar", "maven", "whl"])
    def test_other_library_types_rejected(self, kind):
        error = _pipeline_error({"libraries": [{kind: "dbfs:/lib"}]})
        assert f"'{kind}'" in str(error)

    def test_entry_not_a_mapping_rejected(self):
        _pipeline_error({"libraries": ["${workspace.file_path}/hooks/**"]})

    def test_glob_combined_with_another_key_rejected(self):
        _pipeline_error({"libraries": [{**_GLOB, "file": {"path": "/Workspace/x.py"}}]})

    @pytest.mark.parametrize(
        "glob",
        [{}, {"include": ""}, {"include": "   "}, {"include": 3}, "path/**"],
    )
    def test_glob_without_usable_include_rejected(self, glob):
        error = _pipeline_error({"libraries": [{"glob": glob}]})
        assert "include" in str(error)

    def test_glob_with_key_other_than_include_rejected(self):
        """Databricks' glob (PathPattern) accepts only ``include``."""
        error = _pipeline_error(
            {"libraries": [{"glob": {"include": "/Workspace/x/**", "exclude": "y"}}]}
        )
        assert "'exclude'" in str(error)

    def test_non_glob_kind_named_in_title(self):
        """The CLI summary shows only the title, so it names the library kind."""
        error = _pipeline_error({"libraries": [{"notebook": {"path": "/W/nb"}}]})
        assert "'notebook'" in error.title

    def test_error_names_entry_index(self):
        error = _pipeline_error(
            {"libraries": [_GLOB, {"file": {"path": "/Workspace/x.py"}}]}
        )
        assert error.context.get("Entry") == 1

    def test_empty_entry_named_as_empty(self):
        error = _pipeline_error({"libraries": [{}]})
        assert error.title == "Library entry 0 is empty"
        assert "not a mapping" not in str(error)

    def test_glob_mixed_with_file_named_as_combination(self):
        error = _pipeline_error(
            {"libraries": [{**_GLOB, "file": {"path": "/Workspace/x.py"}}]}
        )
        assert error.title == "Library entry 0 combines 'glob' with 'file'"
