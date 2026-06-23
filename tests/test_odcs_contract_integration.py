"""Integration tests for ODCS contract translation wiring (Slice 1).

These pin the two integration seams the unit tests in
``tests/test_odcs_schema_translation.py`` deliberately do NOT cover:

1. ``LakehousePlumberApplicationFacade.for_project(...)`` must invoke
   ``ContractTranslationService.translate()`` during facade construction,
   so that merely building the facade for a project with a ``contracts/``
   directory materialises ``contracts/lhp/schemas/<stem>.<object>_schema.yaml`` files.

2. ``LakehousePlumberBootstrap.init_project(...)`` must scaffold a
   ``contracts/`` directory (alongside the existing ``schemas/``,
   ``expectations/`` etc.). The translated output lives under ``contracts/lhp/``,
   is regenerated each run, and is gitignored (commit ``50981ee``).
"""

import textwrap
from pathlib import Path

from lhp.api import LakehousePlumberBootstrap
from lhp.api.facade import LakehousePlumberApplicationFacade

# Reuse the VALID_CONTRACT_YAML *shape* from the unit suite (a minimal,
# schema-valid two-object ODCS contract: ``orders`` + ``customers``).
CONTRACT_YAML = textwrap.dedent(
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
        description: Order facts
        properties:
          - name: order_id
            logicalType: integer
            physicalType: BIGINT
            required: true
            criticalDataElement: true
            primaryKey: true
            primaryKeyPosition: 1
            description: Unique order id
          - name: amount
            logicalType: number
            physicalType: DECIMAL(18,2)
            logicalTypeOptions:
              minimum: 0
          - name: status
            logicalType: string
      - name: customers
        properties:
          - name: customer_id
            logicalType: integer
            required: true
    """
).strip()


# ---------------------------------------------------------------------------
# Seam 1: facade construction invokes contract translation
# ---------------------------------------------------------------------------


class TestFacadeInvokesContractTranslation:
    """``for_project(translate_contracts=True)`` translates ``contracts/``.

    Seam chosen: the *public* construction entry point
    ``LakehousePlumberApplicationFacade.for_project``, which wires
    ``ContractTranslationService.translate()`` in. Asserting on the observable
    side effect (emitted schema files) rather than the internal call keeps the
    test agnostic to the exact wiring point.

    ``translate_contracts`` defaults to ``False`` on the facade; the CLI
    ``generate`` / ``validate`` commands pass ``not no_contracts`` to opt in.
    This test opts in explicitly.

    A minimal-but-valid LHP project (``lhp.yaml`` with just ``name`` + ``version``,
    plus ``contracts/sales.yaml``) is built in ``tmp_path``;
    ``enforce_version=False`` avoids version-pinning friction.
    """

    def _build_minimal_project(self, root: Path) -> None:
        (root / "lhp.yaml").write_text(
            'name: sales_project\nversion: "1.0"\n', encoding="utf-8"
        )
        contracts_dir = root / "contracts"
        contracts_dir.mkdir()
        (contracts_dir / "sales.yaml").write_text(CONTRACT_YAML, encoding="utf-8")

    def test_for_project_translates_contracts_into_lhp_schemas(self, tmp_path):
        self._build_minimal_project(tmp_path)

        # Opting in (as the CLI does) must trigger contract translation.
        LakehousePlumberApplicationFacade.for_project(
            tmp_path, enforce_version=False, translate_contracts=True
        )

        schemas_dir = tmp_path / "contracts" / "lhp" / "schemas"
        assert (schemas_dir / "sales.orders_schema.yaml").exists(), (
            "for_project should translate contracts/sales.yaml -> "
            "contracts/lhp/schemas/sales.orders_schema.yaml during construction"
        )
        assert (schemas_dir / "sales.customers_schema.yaml").exists(), (
            "for_project should translate contracts/sales.yaml -> "
            "contracts/lhp/schemas/sales.customers_schema.yaml during construction"
        )

    def test_for_project_translates_contracts_into_lhp_expectations(self, tmp_path):
        # The constrained ``orders`` object (criticalDataElement order_id +
        # amount minimum) must also emit an expectations file alongside schemas.
        self._build_minimal_project(tmp_path)

        LakehousePlumberApplicationFacade.for_project(
            tmp_path, enforce_version=False, translate_contracts=True
        )

        exp_dir = tmp_path / "contracts" / "lhp" / "expectations"
        orders_exp = exp_dir / "sales.orders_expectations.yaml"
        assert orders_exp.exists(), (
            "for_project should translate constrained contract object orders -> "
            "contracts/lhp/expectations/sales.orders_expectations.yaml"
        )
        # customers has only a bare ``required`` constraint -> it DOES yield an
        # expectations file; but the constraint-free shape would not. The
        # orders file is the load-bearing assertion here.
        # Schemas must still be emitted (Slice 1 unaffected).
        assert (
            tmp_path / "contracts" / "lhp" / "schemas" / "sales.orders_schema.yaml"
        ).exists()

    def test_translate_contracts_false_opts_out(self, tmp_path):
        # The --no-contracts opt-out (translate_contracts=False) must skip
        # translation entirely: no contracts/lhp/ output is created.
        self._build_minimal_project(tmp_path)

        LakehousePlumberApplicationFacade.for_project(
            tmp_path, enforce_version=False, translate_contracts=False
        )

        assert not (tmp_path / "contracts" / "lhp").exists(), (
            "translate_contracts=False (--no-contracts) must not translate "
            "contracts or create contracts/lhp/"
        )


# ---------------------------------------------------------------------------
# Seam 2: project init scaffolds contracts/ and gitignores translated output
# ---------------------------------------------------------------------------


class TestInitScaffoldsContractsDir:
    """``init_project`` creates ``contracts/``; translated output is gitignored.

    Exercised through the public ``LakehousePlumberBootstrap`` (the same
    entry point ``lhp init`` routes to, per
    ``tests/test_cli_main_coverage.py``).

    Source contracts live in ``contracts/``; the translated artifacts under
    ``contracts/lhp/`` are regenerated on every run and gitignored (commit
    ``50981ee``), so the generated ``.gitignore`` excludes ``contracts/lhp/``.
    The ``.lhp/`` state/logs directory stays ignored independently.
    """

    def test_init_creates_contracts_directory(self, tmp_path):
        target = tmp_path / "proj"
        result = LakehousePlumberBootstrap().init_project(
            target, bundle=False, project_name="proj"
        )
        assert result.success, result.error_message

        # Existing scaffold dirs (sanity: confirms init ran as expected).
        assert (target / "schemas").is_dir()
        assert (target / "expectations").is_dir()

        # RED today: contracts/ is not yet part of the scaffold.
        assert (target / "contracts").is_dir(), (
            "init_project should scaffold a contracts/ directory for ODCS "
            "data contracts"
        )

    def test_init_gitignores_translated_output(self, tmp_path):
        target = tmp_path / "proj"
        result = LakehousePlumberBootstrap().init_project(
            target, bundle=False, project_name="proj"
        )
        assert result.success, result.error_message

        gitignore = (target / ".gitignore").read_text(encoding="utf-8")
        # ``.lhp/`` (state/logs) is ignored ...
        assert ".lhp/" in gitignore
        # ... and the regenerated translation output under ``contracts/lhp/``
        # is ignored too (it is rebuilt from the source contracts each run).
        assert "contracts/lhp/" in gitignore
