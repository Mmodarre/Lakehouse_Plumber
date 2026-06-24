"""Validation of the ``contract`` action field (Slice 4).

The contract option/action-type checks (empty/missing ``file``; ``type`` other
than ``odcs``; ``schema_hints`` on a non-cloudfiles action; ``expectations_action``
on a non-data_quality action; a ``contract`` on an unsupported action type) are
enforced at resolver time, surfaced as ``LHPError`` (``LHP-CFG-064``) — there is
no separate static ``ConfigValidator`` surface for the ``contract`` field today.

Per the slice brief, when a check is resolver-time rather than static-validation
we assert the observable ``LHPError``. The static ``ConfigValidator`` is exercised
only to confirm it accepts a ``contract``-bearing action (the field is a valid
model field; resolution happens later in the pipeline).
"""

import textwrap
from pathlib import Path

import pytest

from lhp.core.processing.contract_resolver import ContractResolver
from lhp.core.validators import ConfigValidator
from lhp.errors import LHPError
from lhp.models import Action, ActionType, TransformType

# ---------------------------------------------------------------------------
# Fixtures (single-object ``orders`` contract, matching test_contract_resolution)
# ---------------------------------------------------------------------------

SINGLE_OBJECT_CONTRACT = textwrap.dedent(
    """
    version: "1.0.0"
    apiVersion: v3.0.2
    kind: DataContract
    id: 11111111-1111-1111-1111-111111111111
    status: active
    name: sales-contract
    schema:
      - name: orders
        physicalType: table
        properties:
          - name: order_id
            logicalType: integer
            physicalType: BIGINT
            required: true
            criticalDataElement: true
            logicalTypeOptions:
              minimum: 1
          - name: status
            logicalType: string
            logicalTypeOptions:
              minLength: 1
    """
).strip()

CONTRACT_FILE = "contracts/sales.odcs.yaml"


def _write_contract(root: Path) -> Path:
    contracts_dir = root / "contracts"
    contracts_dir.mkdir(parents=True, exist_ok=True)
    path = contracts_dir / "sales.odcs.yaml"
    path.write_text(SINGLE_OBJECT_CONTRACT, encoding="utf-8")
    return path


def _flowgroup(action: dict) -> dict:
    return {"pipeline": "sales", "flowgroup": "ingest", "actions": [action]}


def _cloudfiles_action(contract: dict) -> dict:
    return {
        "name": "load_orders",
        "type": "load",
        "target": "v_orders_raw",
        "source": {"type": "cloudfiles", "path": "/data/orders", "format": "json"},
        "contract": contract,
    }


def _data_quality_action(contract: dict) -> dict:
    return {
        "name": "validate_orders",
        "type": "transform",
        "transform_type": "data_quality",
        "source": "v_orders_typed",
        "target": "v_orders_validated",
        "readMode": "stream",
        "contract": contract,
    }


@pytest.fixture
def resolver() -> ContractResolver:
    return ContractResolver()


# ---------------------------------------------------------------------------
# contract.file validation
# ---------------------------------------------------------------------------


class TestContractFileValidation:
    def test_empty_file_raises(self, tmp_path, resolver):
        _write_contract(tmp_path)
        fg = _flowgroup(_cloudfiles_action({"file": "", "type": "odcs"}))

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"


# ---------------------------------------------------------------------------
# contract.type validation
# ---------------------------------------------------------------------------


class TestContractTypeValidation:
    def test_non_odcs_type_raises(self, tmp_path, resolver):
        _write_contract(tmp_path)
        fg = _flowgroup(
            _cloudfiles_action({"file": CONTRACT_FILE, "type": "snowflake"})
        )

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"


# ---------------------------------------------------------------------------
# option / action-type mismatches
# ---------------------------------------------------------------------------


class TestOptionActionTypeMismatch:
    def test_schema_hints_on_non_cloudfiles_action_raises(self, tmp_path, resolver):
        # ``schema_hints`` is cloudfiles-load-only; on a write it is invalid.
        _write_contract(tmp_path)
        action = {
            "name": "write_orders",
            "type": "write",
            "source": "v_orders_validated",
            "write_target": {
                "type": "streaming_table",
                "database": "cat.bronze",
                "table": "orders",
            },
            "contract": {
                "file": CONTRACT_FILE,
                "type": "odcs",
                "schema_hints": True,
            },
        }
        fg = _flowgroup(action)

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"

    def test_expectations_action_on_non_data_quality_action_raises(
        self, tmp_path, resolver
    ):
        # ``expectations_action`` is data_quality-only; on a cloudfiles load invalid.
        _write_contract(tmp_path)
        fg = _flowgroup(
            _cloudfiles_action(
                {
                    "file": CONTRACT_FILE,
                    "type": "odcs",
                    "expectations_action": "drop",
                }
            )
        )

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"


# ---------------------------------------------------------------------------
# contract on an unsupported action type
# ---------------------------------------------------------------------------


class TestUnsupportedActionType:
    def test_contract_on_sql_transform_raises(self, tmp_path, resolver):
        _write_contract(tmp_path)
        action = {
            "name": "sql_step",
            "type": "transform",
            "transform_type": "sql",
            "source": "v_orders_raw",
            "target": "v_orders_sql",
            "sql": "SELECT * FROM v_orders_raw",
            "contract": {"file": CONTRACT_FILE, "type": "odcs"},
        }
        fg = _flowgroup(action)

        with pytest.raises(LHPError) as exc:
            resolver.resolve(fg, project_root=tmp_path)
        assert exc.value.code == "LHP-CFG-064"


# ---------------------------------------------------------------------------
# Static validator accepts a contract-bearing action (resolution is deferred)
# ---------------------------------------------------------------------------


class TestStaticValidatorAcceptsContract:
    def test_validate_action_does_not_reject_contract_field(self):
        # The ``contract`` field is a valid model field; the static config
        # validator must not flag its mere presence (resolution happens later).
        validator = ConfigValidator()
        action = Action(
            name="load_orders",
            type=ActionType.LOAD,
            target="v_orders_raw",
            source={"type": "cloudfiles", "path": "/data/orders", "format": "json"},
            contract={"file": CONTRACT_FILE, "type": "odcs"},
        )
        errors = validator.validate_action(action, 0)
        assert not any("contract" in str(e).lower() for e in errors)

    def test_data_quality_contract_action_constructs(self):
        # Sanity: a data_quality action carrying a contract is a valid Action.
        action = Action(
            name="validate_orders",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_orders_typed",
            target="v_orders_validated",
            readMode="stream",
            contract={
                "file": CONTRACT_FILE,
                "type": "odcs",
                "expectations_action": "drop",
            },
        )
        assert action.contract is not None
        assert action.contract.file == CONTRACT_FILE
        assert action.contract.expectations_action == "drop"
