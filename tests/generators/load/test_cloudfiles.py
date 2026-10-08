"""CloudFiles explicit-schema emission: variable naming and pyspark.sql.types imports."""

import ast

import pytest
import yaml

from lhp.generators.load.cloudfiles import CloudFilesLoadGenerator
from lhp.models import Action


def _write_schema(directory, filename, schema):
    path = directory / filename
    path.write_text(yaml.safe_dump(schema, sort_keys=False), encoding="utf-8")
    return path


def _cloudfiles_action(target, schema_path, *, legacy_key=False):
    source = {"type": "cloudfiles", "path": "/data/in", "format": "csv"}
    source["schema_file" if legacy_key else "schema"] = str(schema_path)
    return Action(name=f"load_{target}", type="load", source=source, target=target)


def _types_imports(generator):
    return {
        imp for imp in generator.imports if imp.startswith("from pyspark.sql.types")
    }


@pytest.fixture
def prices_schema(tmp_path):
    return _write_schema(
        tmp_path,
        "prices.yaml",
        {
            "table": "prices",
            "columns": [
                {"name": "sku", "type": "STRING", "nullable": False},
                {"name": "price", "type": "DECIMAL(10,2)"},
            ],
        },
    )


@pytest.mark.parametrize("legacy_key", [False, True], ids=["schema", "schema_file"])
def test_imports_only_the_types_the_schema_uses_one_per_line(
    tmp_path, prices_schema, legacy_key
):
    generator = CloudFilesLoadGenerator()

    generator.generate(
        _cloudfiles_action("v_prices_raw", prices_schema, legacy_key=legacy_key),
        {"spec_dir": tmp_path},
    )

    assert _types_imports(generator) == {
        "from pyspark.sql.types import DecimalType",
        "from pyspark.sql.types import StringType",
        "from pyspark.sql.types import StructField",
        "from pyspark.sql.types import StructType",
    }


def test_schema_code_has_no_import_line(tmp_path, prices_schema):
    generator = CloudFilesLoadGenerator()

    code = generator.generate(
        _cloudfiles_action("v_prices_raw", prices_schema), {"spec_dir": tmp_path}
    )

    assert "import" not in code


def test_table_keyed_schema_variable_comes_from_target(tmp_path, prices_schema):
    generator = CloudFilesLoadGenerator()

    code = generator.generate(
        _cloudfiles_action("v_prices_raw", prices_schema), {"spec_dir": tmp_path}
    )

    assert "v_prices_raw_schema = StructType([" in code
    assert ".schema(v_prices_raw_schema)" in code
    assert "schema_schema" not in code


def test_hyphenated_schema_name_yields_valid_identifier(tmp_path):
    schema_path = _write_schema(
        tmp_path,
        "prices-file.yaml",
        {"name": "prices-file", "columns": [{"name": "sku", "type": "STRING"}]},
    )
    generator = CloudFilesLoadGenerator()

    code = generator.generate(
        _cloudfiles_action("v_prices_raw", schema_path), {"spec_dir": tmp_path}
    )

    ast.parse(code)
    assert "v_prices_raw_schema = StructType([" in code
    assert ".schema(v_prices_raw_schema)" in code


def test_target_is_sanitized_into_an_identifier(tmp_path, prices_schema):
    generator = CloudFilesLoadGenerator()

    code = generator.generate(
        _cloudfiles_action("v.prices-raw", prices_schema), {"spec_dir": tmp_path}
    )

    assert "v_prices_raw_schema = StructType([" in code
    assert ".schema(v_prices_raw_schema)" in code
