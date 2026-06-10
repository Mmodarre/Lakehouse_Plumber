"""Isolated unit tests for :class:`OperationalMetadataService`.

Covers ``get_metadata_and_imports`` and ``get_all_metadata_column_names`` in
``lhp/core/codegen/operational_metadata/service.py``. This orchestration class
was only reached transitively through full pipeline generation. It is the sole
caller that wires ``adapt_expressions_for_imports`` into the catalog, so its
contract is asserted here directly.

These tests are fully isolated: they construct the service and in-memory
config models, with no pipeline generation.
"""

import pytest

from lhp.core.codegen.imports.manager import ImportManager
from lhp.core.codegen.operational_metadata import metadata as metadata_module
from lhp.core.codegen.operational_metadata.service import (
    OperationalMetadataService,
)
from lhp.models import (
    Action,
    FlowGroup,
    ProjectConfig,
)

WILDCARD_IMPORT = "from pyspark.sql.functions import *"


def _action() -> Action:
    return Action(
        name="a",
        type="load",
        target="t",
        source={"type": "sql", "sql": "select 1"},
    )


def _flowgroup_selecting(*columns: str) -> FlowGroup:
    return FlowGroup(
        pipeline="pl",
        flowgroup="fg",
        operational_metadata=list(columns) or None,
    )


def _wildcard_manager() -> ImportManager:
    im = ImportManager()
    im.add_import(WILDCARD_IMPORT)
    return im


def _f_prefix_manager() -> ImportManager:
    im = ImportManager()
    im.add_import("import pyspark.sql.functions as F")
    return im


@pytest.mark.unit
def test_returns_three_tuple_of_bool_dict_list():
    """Contract: (add_metadata: bool, columns: dict, imports: list)."""
    service = OperationalMetadataService()

    result = service.get_metadata_and_imports(
        _action(),
        _flowgroup_selecting("_ingestion_timestamp"),
        preset_config={},
        project_config=ProjectConfig(name="p"),
        target_type="view",
        import_manager=_f_prefix_manager(),
    )

    assert isinstance(result, tuple)
    assert len(result) == 3
    add_metadata, columns, imports = result
    assert isinstance(add_metadata, bool)
    assert isinstance(columns, dict)
    assert isinstance(imports, list)


@pytest.mark.unit
def test_add_metadata_true_when_columns_selected():
    service = OperationalMetadataService()

    add_metadata, columns, _ = service.get_metadata_and_imports(
        _action(),
        _flowgroup_selecting("_ingestion_timestamp"),
        preset_config={},
        project_config=ProjectConfig(name="p"),
        target_type="view",
        import_manager=_f_prefix_manager(),
    )

    assert add_metadata is True
    assert "_ingestion_timestamp" in columns


@pytest.mark.unit
def test_add_metadata_false_when_no_columns_selected():
    """With no operational_metadata selection anywhere, add_metadata is False
    and the columns/imports are empty."""
    service = OperationalMetadataService()

    add_metadata, columns, imports = service.get_metadata_and_imports(
        _action(),
        _flowgroup_selecting(),  # no selection
        preset_config={},
        project_config=ProjectConfig(name="p"),
        target_type="view",
        import_manager=_f_prefix_manager(),
    )

    assert add_metadata is False
    assert columns == {}
    assert imports == []


@pytest.mark.unit
def test_wildcard_import_manager_drives_expression_adaptation():
    """Behavioral evidence that adapt_expressions_for_imports ran: with a
    wildcard import the selected expression is the bare-call form and no
    ``functions as F`` import is required."""
    service = OperationalMetadataService()

    add_metadata, columns, imports = service.get_metadata_and_imports(
        _action(),
        _flowgroup_selecting("_ingestion_timestamp"),
        preset_config={},
        project_config=ProjectConfig(name="p"),
        target_type="view",
        import_manager=_wildcard_manager(),
    )

    assert add_metadata is True
    assert columns["_ingestion_timestamp"] == "current_timestamp()"
    # Bare call needs the wildcard, not an `as F` import.
    assert not any("functions as F" in imp for imp in imports)


@pytest.mark.unit
def test_f_prefix_import_manager_keeps_prefixed_expression():
    service = OperationalMetadataService()

    _, columns, imports = service.get_metadata_and_imports(
        _action(),
        _flowgroup_selecting("_ingestion_timestamp"),
        preset_config={},
        project_config=ProjectConfig(name="p"),
        target_type="view",
        import_manager=_f_prefix_manager(),
    )

    assert columns["_ingestion_timestamp"] == "F.current_timestamp()"
    assert any("functions as F" in imp for imp in imports)


@pytest.mark.unit
def test_adapt_expressions_called_exactly_when_import_manager_present(
    monkeypatch,
):
    """Spy on the catalog: adapt_expressions_for_imports is called once with the
    import_manager when one is supplied, and not at all when it is None."""
    calls = []
    original = metadata_module.OperationalMetadataCatalog.adapt_expressions_for_imports

    def _spy(self, import_manager=None):
        calls.append(import_manager)
        return original(self, import_manager)

    monkeypatch.setattr(
        metadata_module.OperationalMetadataCatalog,
        "adapt_expressions_for_imports",
        _spy,
    )
    service = OperationalMetadataService()
    im = _f_prefix_manager()

    service.get_metadata_and_imports(
        _action(),
        _flowgroup_selecting("_ingestion_timestamp"),
        preset_config={},
        project_config=ProjectConfig(name="p"),
        target_type="view",
        import_manager=im,
    )
    assert calls == [im]

    calls.clear()
    service.get_metadata_and_imports(
        _action(),
        _flowgroup_selecting("_ingestion_timestamp"),
        preset_config={},
        project_config=ProjectConfig(name="p"),
        target_type="view",
        import_manager=None,
    )
    assert calls == []


@pytest.mark.unit
def test_none_project_config_uses_defaults_without_crashing():
    """A None project_config must not crash; defaults are used and a selected
    default column resolves normally."""
    service = OperationalMetadataService()

    add_metadata, columns, imports = service.get_metadata_and_imports(
        _action(),
        _flowgroup_selecting("_ingestion_timestamp"),
        preset_config={},
        project_config=None,
        target_type="view",
        import_manager=_f_prefix_manager(),
    )

    assert add_metadata is True
    assert columns["_ingestion_timestamp"] == "F.current_timestamp()"
    assert isinstance(imports, list)


@pytest.mark.unit
def test_get_all_metadata_column_names_returns_builtin_defaults():
    """With no project config, the column-name set is exactly the five built-in
    defaults."""
    service = OperationalMetadataService()

    names = service.get_all_metadata_column_names(None)

    assert names == {
        "_ingestion_timestamp",
        "_source_file",
        "_pipeline_run_id",
        "_pipeline_name",
        "_flowgroup_name",
    }


@pytest.mark.unit
def test_get_all_metadata_column_names_unions_project_columns():
    from lhp.models import (
        MetadataColumnConfig,
        ProjectOperationalMetadataConfig,
    )

    proj = ProjectConfig(
        name="p",
        operational_metadata=ProjectOperationalMetadataConfig(
            columns={
                "_custom": MetadataColumnConfig(
                    expression="F.lit('x')", applies_to=["view"]
                )
            }
        ),
    )
    service = OperationalMetadataService()

    names = service.get_all_metadata_column_names(proj)

    assert "_custom" in names
    assert {"_ingestion_timestamp", "_pipeline_name"} <= names
