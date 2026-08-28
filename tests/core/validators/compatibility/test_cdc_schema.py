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


def _action(schema_ref, cdc_config=None):
    """Build a CDC write action referencing ``schema_ref``.

    The ``__START_AT``/``__END_AT`` presence check only applies to SCD Type 2, so
    the default ``cdc_config`` is Type 2; pass an explicit ``cdc_config`` (e.g. with
    ``scd_type: 1``) to exercise the gate.
    """
    if cdc_config is None:
        cdc_config = {
            "keys": ["customer_id"],
            "sequence_by": "last_modified_dt",
            "scd_type": 2,
        }
    return Action(
        name="write_customer_scd2",
        type=ActionType.WRITE,
        source="v_source",
        write_target={
            "type": "streaming_table",
            "table_schema": schema_ref,
            "cdc_config": cdc_config,
        },
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
        assert (
            validator.validate(_action("schemas/customer_scd2_dim.yaml"), self.prefix)
            == []
        )

    def test_missing_schema_file_defers(self, tmp_path):
        """Referenced file does not exist: defer to the generator's IO error, do not raise."""
        validator = CdcSchemaValidator(project_root=tmp_path)
        assert (
            validator.validate(_action("schemas/does_not_exist.yaml"), self.prefix)
            == []
        )


# Inline DDL with a PRIMARY KEY and no __START_AT/__END_AT — correct for SCD Type 1,
# where the validity columns must not exist. ``table_schema`` is the only way to
# register a PRIMARY KEY on a pipeline-managed table.
_SCD1_INLINE = (
    "customer_sk STRING NOT NULL, customer_id BIGINT NOT NULL, "
    "c_name STRING, CONSTRAINT dim_customer_pk PRIMARY KEY (customer_sk)"
)


class TestCdcSchemaValidatorScdType:
    """The ``__START_AT``/``__END_AT`` presence check applies to SCD Type 2 only.

    Type 1 targets must not carry the validity columns (the runtime rejects them),
    so declaring a PRIMARY KEY via ``table_schema`` on a Type 1 target must validate.
    """

    prefix = "Action[1] 'write_customer_scd2'"

    def test_scd1_inline_schema_with_primary_key_no_errors(self):
        """SCD1 + PRIMARY KEY, no validity columns: no error."""
        validator = CdcSchemaValidator()
        action = _action(
            _SCD1_INLINE,
            cdc_config={
                "keys": ["customer_id"],
                "sequence_by": "last_modified_dt",
                "scd_type": 1,
            },
        )
        assert validator.validate(action, self.prefix) == []

    def test_scd_type_omitted_defaults_to_type1_no_errors(self):
        """Omitted scd_type defaults to Type 1 (matching the generator): no error."""
        validator = CdcSchemaValidator()
        action = _action(
            _SCD1_INLINE,
            cdc_config={"keys": ["customer_id"], "sequence_by": "last_modified_dt"},
        )
        assert validator.validate(action, self.prefix) == []

    def test_scd1_file_schema_with_primary_key_no_errors(self, tmp_path):
        """SCD1 + a file-based schema with no validity columns: no error."""
        ref = _write_schema(tmp_path, _MISSING_BOTH)
        validator = CdcSchemaValidator(project_root=tmp_path)
        action = _action(
            ref,
            cdc_config={
                "keys": ["customer_id"],
                "sequence_by": "last_modified_dt",
                "scd_type": 1,
            },
        )
        assert validator.validate(action, self.prefix) == []

    def test_scd2_still_requires_validity_columns(self):
        """Regression guard: SCD2 missing both validity columns still errors."""
        validator = CdcSchemaValidator()
        action = _action(
            _SCD1_INLINE,
            cdc_config={
                "keys": ["customer_id"],
                "sequence_by": "last_modified_dt",
                "scd_type": 2,
            },
        )
        errors = validator.validate(action, self.prefix)
        assert any("__START_AT" in e for e in errors)
        assert any("__END_AT" in e for e in errors)
        assert len(errors) == 2
