"""TDD tests for YAMLEditor -- round-trip YAML editing service.

These tests exercise the ``YAMLEditor`` service which uses ruamel.yaml for
comment/formatting-preserving reads and writes. The production module
``lhp.api.services.yaml_editor`` does not exist yet; these tests are
written first to drive the implementation.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lhp.api.services.yaml_editor import (
    FlowgroupFileFormat,
    FlowgroupUpdateResult,
    YAMLEditor,
)

pytestmark = pytest.mark.api


# ---------------------------------------------------------------------------
# TestReadWrite
# ---------------------------------------------------------------------------


class TestReadWrite:
    """Tests for basic ``read`` and ``write`` round-trip operations."""

    def test_round_trip_preserves_content(self, tmp_path: Path):
        editor = YAMLEditor()
        data = {"pipeline": "bronze", "flowgroup": "customers", "catalog": "main"}
        path = tmp_path / "test.yaml"

        editor.write(path, data)
        result = editor.read(path)

        assert result["pipeline"] == "bronze"
        assert result["flowgroup"] == "customers"
        assert result["catalog"] == "main"

    def test_write_creates_parent_directories(self, tmp_path: Path):
        editor = YAMLEditor()
        nested_path = tmp_path / "a" / "b" / "c" / "file.yaml"
        data = {"key": "value"}

        editor.write(nested_path, data)

        assert nested_path.exists()
        assert editor.read(nested_path)["key"] == "value"

    def test_read_nonexistent_raises(self, tmp_path: Path):
        editor = YAMLEditor()
        missing = tmp_path / "does_not_exist.yaml"

        with pytest.raises(FileNotFoundError):
            editor.read(missing)


# ---------------------------------------------------------------------------
# TestMultiDocument
# ---------------------------------------------------------------------------


class TestMultiDocument:
    """Tests for multi-document (``---`` separated) YAML operations."""

    def test_read_all_documents_parses_multi_doc(self, tmp_path: Path):
        path = tmp_path / "multi.yaml"
        path.write_text(
            "flowgroup: alpha\npipeline: bronze\n"
            "---\n"
            "flowgroup: beta\npipeline: silver\n"
        )
        editor = YAMLEditor()

        docs = editor.read_all_documents(path)

        assert len(docs) == 2
        assert docs[0]["flowgroup"] == "alpha"
        assert docs[1]["flowgroup"] == "beta"

    def test_write_all_documents_creates_multi_doc(self, tmp_path: Path):
        editor = YAMLEditor()
        path = tmp_path / "multi_out.yaml"
        docs = [
            {"flowgroup": "first", "pipeline": "p1"},
            {"flowgroup": "second", "pipeline": "p2"},
        ]

        editor.write_all_documents(path, docs)

        content = path.read_text()
        assert "---" in content
        # Verify round-trip
        reloaded = editor.read_all_documents(path)
        assert len(reloaded) == 2
        assert reloaded[0]["flowgroup"] == "first"
        assert reloaded[1]["flowgroup"] == "second"


# ---------------------------------------------------------------------------
# TestDetectFormat
# ---------------------------------------------------------------------------


class TestDetectFormat:
    """Tests for ``detect_file_format`` which identifies the flowgroup file layout."""

    def test_detects_single_format(self, tmp_path: Path):
        path = tmp_path / "single.yaml"
        path.write_text("flowgroup: my_fg\npipeline: bronze\n")
        editor = YAMLEditor()

        fmt = editor.detect_file_format(path)

        assert fmt == FlowgroupFileFormat.SINGLE

    def test_detects_multi_document_format(self, tmp_path: Path):
        path = tmp_path / "multi.yaml"
        path.write_text(
            "flowgroup: alpha\npipeline: p1\n"
            "---\n"
            "flowgroup: beta\npipeline: p2\n"
        )
        editor = YAMLEditor()

        fmt = editor.detect_file_format(path)

        assert fmt == FlowgroupFileFormat.MULTI_DOCUMENT

    def test_detects_array_syntax_format(self, tmp_path: Path):
        path = tmp_path / "array.yaml"
        path.write_text(
            "pipeline: shared_pipeline\n"
            "flowgroups:\n"
            "  - flowgroup: fg_a\n"
            "    catalog: main\n"
            "  - flowgroup: fg_b\n"
            "    catalog: main\n"
        )
        editor = YAMLEditor()

        fmt = editor.detect_file_format(path)

        assert fmt == FlowgroupFileFormat.ARRAY_SYNTAX


# ---------------------------------------------------------------------------
# TestFindFlowgroupInFile
# ---------------------------------------------------------------------------


class TestFindFlowgroupInFile:
    """Tests for ``find_flowgroup_in_file``."""

    def test_finds_in_single_doc(self, tmp_path: Path):
        path = tmp_path / "single.yaml"
        path.write_text("flowgroup: target\npipeline: p1\n")
        editor = YAMLEditor()

        doc_index, doc = editor.find_flowgroup_in_file(path, "target")

        assert doc_index == 0
        assert doc is not None
        assert doc["flowgroup"] == "target"

    def test_finds_in_multi_doc(self, tmp_path: Path):
        path = tmp_path / "multi.yaml"
        path.write_text(
            "flowgroup: first\npipeline: p1\n"
            "---\n"
            "flowgroup: second\npipeline: p2\n"
        )
        editor = YAMLEditor()

        doc_index, doc = editor.find_flowgroup_in_file(path, "second")

        assert doc_index == 1
        assert doc is not None
        assert doc["flowgroup"] == "second"

    def test_returns_none_when_not_found(self, tmp_path: Path):
        path = tmp_path / "single.yaml"
        path.write_text("flowgroup: other\npipeline: p1\n")
        editor = YAMLEditor()

        doc_index, doc = editor.find_flowgroup_in_file(path, "nonexistent")

        assert doc is None


# ---------------------------------------------------------------------------
# TestCreateFlowgroup
# ---------------------------------------------------------------------------


class TestCreateFlowgroup:
    """Tests for ``create_flowgroup``."""

    def test_creates_file_at_correct_path(self, tmp_path: Path):
        editor = YAMLEditor()
        config = {"catalog": "main", "target": "schema.table"}

        result_path = editor.create_flowgroup(
            project_root=tmp_path,
            pipeline="bronze_pipeline",
            flowgroup_name="customer_load",
            config=config,
        )

        expected = tmp_path / "pipelines" / "bronze_pipeline" / "customer_load.yaml"
        assert result_path == expected
        assert result_path.exists()

    def test_sets_pipeline_and_flowgroup(self, tmp_path: Path):
        editor = YAMLEditor()
        config = {"catalog": "main"}

        result_path = editor.create_flowgroup(
            project_root=tmp_path,
            pipeline="my_pipeline",
            flowgroup_name="my_fg",
            config=config,
        )

        data = editor.read(result_path)
        assert data["pipeline"] == "my_pipeline"
        assert data["flowgroup"] == "my_fg"

    def test_creates_parent_directories(self, tmp_path: Path):
        editor = YAMLEditor()
        config = {"catalog": "main"}

        editor.create_flowgroup(
            project_root=tmp_path,
            pipeline="new_pipeline",
            flowgroup_name="new_fg",
            config=config,
        )

        assert (tmp_path / "pipelines" / "new_pipeline").is_dir()


# ---------------------------------------------------------------------------
# TestUpdateFlowgroup
# ---------------------------------------------------------------------------


class TestUpdateFlowgroup:
    """Tests for ``update_flowgroup`` across all three file formats."""

    def test_single_format_overwrites_file(self, tmp_path: Path):
        editor = YAMLEditor()
        path = tmp_path / "fg.yaml"
        path.write_text("flowgroup: my_fg\npipeline: p1\ncatalog: old_catalog\n")

        editor.update_flowgroup(
            project_root=tmp_path,
            flowgroup_path=path,
            flowgroup_name="my_fg",
            config={"catalog": "new_catalog", "target": "schema.new_table"},
        )

        data = editor.read(path)
        assert data["catalog"] == "new_catalog"
        assert data["target"] == "schema.new_table"

    def test_multi_doc_updates_only_target(self, tmp_path: Path):
        editor = YAMLEditor()
        path = tmp_path / "multi.yaml"
        path.write_text(
            "flowgroup: alpha\npipeline: p1\ncatalog: cat_a\n"
            "---\n"
            "flowgroup: beta\npipeline: p1\ncatalog: cat_b\n"
        )

        editor.update_flowgroup(
            project_root=tmp_path,
            flowgroup_path=path,
            flowgroup_name="beta",
            config={"catalog": "updated_cat_b"},
        )

        docs = editor.read_all_documents(path)
        beta_doc = next(d for d in docs if d["flowgroup"] == "beta")
        assert beta_doc["catalog"] == "updated_cat_b"

    def test_multi_doc_preserves_sibling_documents(self, tmp_path: Path):
        editor = YAMLEditor()
        path = tmp_path / "multi.yaml"
        path.write_text(
            "flowgroup: alpha\npipeline: p1\ncatalog: cat_a\n"
            "---\n"
            "flowgroup: beta\npipeline: p1\ncatalog: cat_b\n"
        )

        editor.update_flowgroup(
            project_root=tmp_path,
            flowgroup_path=path,
            flowgroup_name="beta",
            config={"catalog": "changed"},
        )

        docs = editor.read_all_documents(path)
        alpha_doc = next(d for d in docs if d["flowgroup"] == "alpha")
        assert alpha_doc["catalog"] == "cat_a"
        assert alpha_doc["pipeline"] == "p1"

    def test_array_syntax_updates_only_target_entry(self, tmp_path: Path):
        editor = YAMLEditor()
        path = tmp_path / "array.yaml"
        path.write_text(
            "pipeline: shared\n"
            "flowgroups:\n"
            "  - flowgroup: fg_a\n"
            "    catalog: cat_a\n"
            "  - flowgroup: fg_b\n"
            "    catalog: cat_b\n"
        )

        editor.update_flowgroup(
            project_root=tmp_path,
            flowgroup_path=path,
            flowgroup_name="fg_b",
            config={"catalog": "new_cat_b"},
        )

        data = editor.read(path)
        fg_b = next(fg for fg in data["flowgroups"] if fg["flowgroup"] == "fg_b")
        assert fg_b["catalog"] == "new_cat_b"

    def test_array_syntax_preserves_shared_fields(self, tmp_path: Path):
        editor = YAMLEditor()
        path = tmp_path / "array.yaml"
        path.write_text(
            "pipeline: shared_pipeline\n"
            "catalog: shared_catalog\n"
            "flowgroups:\n"
            "  - flowgroup: fg_a\n"
            "    target: table_a\n"
            "  - flowgroup: fg_b\n"
            "    target: table_b\n"
        )

        editor.update_flowgroup(
            project_root=tmp_path,
            flowgroup_path=path,
            flowgroup_name="fg_a",
            config={"target": "new_table_a"},
        )

        data = editor.read(path)
        assert data["pipeline"] == "shared_pipeline"
        assert data["catalog"] == "shared_catalog"

    def test_not_found_in_multi_doc_raises_value_error(self, tmp_path: Path):
        editor = YAMLEditor()
        path = tmp_path / "multi.yaml"
        path.write_text(
            "flowgroup: alpha\npipeline: p1\n"
            "---\n"
            "flowgroup: beta\npipeline: p1\n"
        )

        with pytest.raises(ValueError):
            editor.update_flowgroup(
                project_root=tmp_path,
                flowgroup_path=path,
                flowgroup_name="nonexistent",
                config={"catalog": "x"},
            )

    def test_not_found_in_array_syntax_raises_value_error(self, tmp_path: Path):
        editor = YAMLEditor()
        path = tmp_path / "array.yaml"
        path.write_text(
            "pipeline: p1\n"
            "flowgroups:\n"
            "  - flowgroup: fg_a\n"
            "    catalog: cat_a\n"
        )

        with pytest.raises(ValueError):
            editor.update_flowgroup(
                project_root=tmp_path,
                flowgroup_path=path,
                flowgroup_name="missing_fg",
                config={"catalog": "x"},
            )

    def test_returns_update_result_metadata(self, tmp_path: Path):
        editor = YAMLEditor()
        path = tmp_path / "multi.yaml"
        path.write_text(
            "flowgroup: alpha\npipeline: p1\n"
            "---\n"
            "flowgroup: beta\npipeline: p1\n"
        )

        result = editor.update_flowgroup(
            project_root=tmp_path,
            flowgroup_path=path,
            flowgroup_name="alpha",
            config={"catalog": "updated"},
        )

        assert isinstance(result, FlowgroupUpdateResult)
        assert result.is_multi_flowgroup_file is True
        assert result.documents_in_file == 2
        assert result.file_format == FlowgroupFileFormat.MULTI_DOCUMENT


# ---------------------------------------------------------------------------
# TestDeleteFlowgroup
# ---------------------------------------------------------------------------


class TestDeleteFlowgroup:
    """Tests for ``delete_flowgroup`` across all three file formats."""

    def test_single_format_deletes_file(self, tmp_path: Path):
        editor = YAMLEditor()
        path = tmp_path / "pipelines" / "p1" / "fg.yaml"
        path.parent.mkdir(parents=True)
        path.write_text("flowgroup: fg\npipeline: p1\n")

        editor.delete_flowgroup(path, "fg", project_root=tmp_path)

        assert not path.exists()

    def test_multi_doc_removes_target_preserves_others(self, tmp_path: Path):
        editor = YAMLEditor()
        path = tmp_path / "multi.yaml"
        path.write_text(
            "flowgroup: alpha\npipeline: p1\n"
            "---\n"
            "flowgroup: beta\npipeline: p1\n"
            "---\n"
            "flowgroup: gamma\npipeline: p1\n"
        )

        editor.delete_flowgroup(path, "beta")

        docs = editor.read_all_documents(path)
        names = [d["flowgroup"] for d in docs]
        assert "beta" not in names
        assert "alpha" in names
        assert "gamma" in names

    def test_multi_doc_deletes_file_when_last_removed(self, tmp_path: Path):
        editor = YAMLEditor()
        path = tmp_path / "single_doc.yaml"
        path.write_text("flowgroup: only_one\npipeline: p1\n")

        editor.delete_flowgroup(path, "only_one")

        assert not path.exists()

    def test_array_syntax_removes_target_preserves_others(self, tmp_path: Path):
        editor = YAMLEditor()
        path = tmp_path / "array.yaml"
        path.write_text(
            "pipeline: shared\n"
            "flowgroups:\n"
            "  - flowgroup: fg_a\n"
            "    catalog: cat_a\n"
            "  - flowgroup: fg_b\n"
            "    catalog: cat_b\n"
            "  - flowgroup: fg_c\n"
            "    catalog: cat_c\n"
        )

        editor.delete_flowgroup(path, "fg_b")

        data = editor.read(path)
        names = [fg["flowgroup"] for fg in data["flowgroups"]]
        assert "fg_b" not in names
        assert "fg_a" in names
        assert "fg_c" in names

    def test_array_syntax_deletes_file_when_last_removed(self, tmp_path: Path):
        editor = YAMLEditor()
        path = tmp_path / "array.yaml"
        path.write_text(
            "pipeline: shared\n"
            "flowgroups:\n"
            "  - flowgroup: only_fg\n"
            "    catalog: cat\n"
        )

        editor.delete_flowgroup(path, "only_fg")

        assert not path.exists()

    def test_not_found_raises_value_error(self, tmp_path: Path):
        editor = YAMLEditor()
        path = tmp_path / "fg.yaml"
        path.write_text("flowgroup: real_fg\npipeline: p1\n")

        with pytest.raises(ValueError):
            editor.delete_flowgroup(path, "wrong_name")

    def test_cleans_up_empty_parent_directories(self, tmp_path: Path):
        editor = YAMLEditor()
        pipeline_dir = tmp_path / "pipelines" / "my_pipeline"
        pipeline_dir.mkdir(parents=True)
        path = pipeline_dir / "fg.yaml"
        path.write_text("flowgroup: fg\npipeline: my_pipeline\n")

        editor.delete_flowgroup(path, "fg", project_root=tmp_path)

        assert not path.exists()
        # The empty pipeline directory should be cleaned up
        assert not pipeline_dir.exists()
        # But pipelines/ (or project root) should still exist
        assert tmp_path.exists()


# ---------------------------------------------------------------------------
# TestCreatePresetTemplateEnv
# ---------------------------------------------------------------------------


class TestCreatePresetTemplateEnv:
    """Tests for creating presets, templates, and environment files."""

    def test_create_preset_at_correct_path(self, tmp_path: Path):
        editor = YAMLEditor()
        config = {"scd_type": 2, "merge_keys": ["id"]}

        result_path = editor.create_preset(
            project_root=tmp_path,
            name="scd_type2",
            config=config,
        )

        expected = tmp_path / "presets" / "scd_type2.yaml"
        assert result_path == expected
        assert result_path.exists()

        data = editor.read(result_path)
        assert data["scd_type"] == 2
        assert data["merge_keys"] == ["id"]

    def test_create_template_at_correct_path(self, tmp_path: Path):
        editor = YAMLEditor()
        config = {"pipeline": "{{ pipeline_name }}", "catalog": "{{ catalog }}"}

        result_path = editor.create_template(
            project_root=tmp_path,
            name="bronze_template",
            config=config,
        )

        expected = tmp_path / "templates" / "bronze_template.yaml"
        assert result_path == expected
        assert result_path.exists()

        data = editor.read(result_path)
        assert data["pipeline"] == "{{ pipeline_name }}"

    def test_create_environment_at_correct_path(self, tmp_path: Path):
        editor = YAMLEditor()
        tokens = {"catalog": "dev_catalog", "schema_prefix": "dev_"}

        result_path = editor.create_environment(
            project_root=tmp_path,
            env_name="dev",
            tokens=tokens,
        )

        expected = tmp_path / "substitutions" / "dev.yaml"
        assert result_path == expected
        assert result_path.exists()

        data = editor.read(result_path)
        assert data["catalog"] == "dev_catalog"
        assert data["schema_prefix"] == "dev_"
