"""Unit tests for `CdcSchemaValidator` with a file-based ``table_schema``.

The inline-DDL behavior is covered by
``tests/test_dlt_cdc_validators_extended.py::TestCdcSchemaValidatorDirect``. These
tests cover the file-reference form: the validator resolves the schema file (relative
to ``project_root``) before checking for the SCD2 history columns ``__START_AT`` /
``__END_AT``, and defers (no false positives) when it cannot resolve.
"""

from lhp.core.validators.compatibility import CdcSchemaValidator
from lhp.models import Action, ActionType

_BOTH_COLUMNS = """\
name: customer_scd2_dim
columns:
  - name: customer_id
    type: BIGINT
  - name: __START_AT
    type: TIMESTAMP
  - name: __END_AT
    type: TIMESTAMP
"""

_MISSING_START_AT = """\
name: customer_scd2_dim
columns:
  - name: customer_id
    type: BIGINT
  - name: __END_AT
    type: TIMESTAMP
"""

_MISSING_BOTH = """\
name: customer_scd2_dim
columns:
  - name: customer_id
    type: BIGINT
  - name: c_name
    type: STRING
"""


def _write_schema(tmp_path, body):
    schemas = tmp_path / "schemas"
    schemas.mkdir(exist_ok=True)
    (schemas / "customer_scd2_dim.yaml").write_text(body)
    return "schemas/customer_scd2_dim.yaml"


def _action(schema_ref):
    return Action(
        name="write_customer_scd2",
        type=ActionType.WRITE,
        source="v_source",
        write_target={"type": "streaming_table", "table_schema": schema_ref},
    )


class TestCdcSchemaValidatorFileSchema:
    prefix = "Action[1] 'write_customer_scd2'"

    def test_file_schema_with_both_columns_no_errors(self, tmp_path):
        ref = _write_schema(tmp_path, _BOTH_COLUMNS)
        validator = CdcSchemaValidator(project_root=tmp_path)
        assert validator.validate(_action(ref), self.prefix) == []

    def test_file_schema_missing_start_at(self, tmp_path):
        ref = _write_schema(tmp_path, _MISSING_START_AT)
        validator = CdcSchemaValidator(project_root=tmp_path)
        errors = validator.validate(_action(ref), self.prefix)
        assert any("__START_AT" in e for e in errors)
        assert not any("__END_AT" in e for e in errors)

    def test_file_schema_missing_both_columns(self, tmp_path):
        ref = _write_schema(tmp_path, _MISSING_BOTH)
        validator = CdcSchemaValidator(project_root=tmp_path)
        errors = validator.validate(_action(ref), self.prefix)
        assert any("__START_AT" in e for e in errors)
        assert any("__END_AT" in e for e in errors)
        assert len(errors) == 2

    def test_file_schema_no_project_root_defers(self, tmp_path):
        """File path but no project_root: cannot resolve → defer, no false positive."""
        _write_schema(tmp_path, _MISSING_BOTH)
        validator = CdcSchemaValidator(project_root=None)
        assert validator.validate(_action("schemas/customer_scd2_dim.yaml"), self.prefix) == []

    def test_missing_schema_file_defers(self, tmp_path):
        """Referenced file does not exist: defer to the generator's IO error, do not raise."""
        validator = CdcSchemaValidator(project_root=tmp_path)
        assert validator.validate(_action("schemas/does_not_exist.yaml"), self.prefix) == []
