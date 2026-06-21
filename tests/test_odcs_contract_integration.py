"""Integration tests for ODCS contract translation wiring (Slice 1).

These pin the two integration seams the unit tests in
``tests/test_odcs_schema_translation.py`` deliberately do NOT cover:

1. ``LakehousePlumberApplicationFacade.for_project(...)`` must invoke
   ``ContractTranslationService.translate()`` during facade construction,
   so that merely building the facade for a project with a ``contracts/``
   directory materialises ``.lhp/contracts/schemas/<stem>.<object>_schema.yaml`` files.

2. ``LakehousePlumberBootstrap.init_project(...)`` must scaffold a
   ``contracts/`` directory (alongside the existing ``schemas/``,
   ``expectations/`` etc.) and gitignore the translated-schema output.

Both are written against the documented public surface while the wiring
is not yet implemented; they are expected to be RED until the feature is
wired (NOT failing on import/collection). RED reason for each is noted
inline.
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
            primaryKey: true
            primaryKeyPosition: 1
            description: Unique order id
          - name: amount
            logicalType: number
            physicalType: DECIMAL(18,2)
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
    """``for_project`` translates ``contracts/`` into ``.lhp/contracts/schemas/``.

    Seam chosen: the *public* construction entry point
    ``LakehousePlumberApplicationFacade.for_project`` (which the plan
    wires ``ContractTranslationService.translate()`` into, directly or
    via ``build_facade_orchestrator`` in
    ``lhp/core/coordination/layers.py``). Asserting on the observable
    side effect (emitted schema files) rather than the internal call
    keeps the test agnostic to *which* of those two functions the
    implementer wires it into — both satisfy the plan.

    A minimal-but-valid LHP project (``lhp.yaml`` with just ``name`` +
    ``version``, plus ``contracts/sales.yaml``) is built in ``tmp_path``;
    ``enforce_version=False`` avoids version-pinning friction.

    RED today because translation is not invoked during construction, so
    ``.lhp/contracts/schemas/`` is never created. The facade itself constructs
    fine (verified), so this fails on the missing-file assertion, NOT on
    import/collection.
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

        # Constructing the facade must trigger contract translation.
        LakehousePlumberApplicationFacade.for_project(
            tmp_path, enforce_version=False
        )

        schemas_dir = tmp_path / ".lhp" / "contracts" / "schemas"
        assert (schemas_dir / "sales.orders_schema.yaml").exists(), (
            "for_project should translate contracts/sales.yaml -> "
            ".lhp/contracts/schemas/sales.orders_schema.yaml during construction"
        )
        assert (schemas_dir / "sales.customers_schema.yaml").exists(), (
            "for_project should translate contracts/sales.yaml -> "
            ".lhp/contracts/schemas/sales.customers_schema.yaml during construction"
        )


# ---------------------------------------------------------------------------
# Seam 2: project init scaffolds contracts/ and gitignores translated output
# ---------------------------------------------------------------------------


class TestInitScaffoldsContractsDir:
    """``init_project`` creates ``contracts/`` and ignores ``.lhp/`` output.

    Exercised through the public ``LakehousePlumberBootstrap`` (the same
    entry point ``lhp init`` routes to, per
    ``tests/test_cli_main_coverage.py``).

    NOTE on the gitignore assertion: the generated ``.gitignore``
    (``src/lhp/templates/init/.gitignore.j2``) ALREADY ignores all of
    ``.lhp/`` broadly, which subsumes the plan's ``.lhp/contracts/schemas/`` intent
    — so asserting any ``.lhp/`` entry is already GREEN and would not
    pin new behaviour. The RED-able part of seam 2 is therefore the
    ``contracts/`` directory, which init does NOT yet create. We assert
    that (RED now, GREEN once ``_create_directory_tree`` adds it) and
    also assert the ``.lhp/`` ignore stays present as a regression guard.
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

    def test_init_gitignores_translated_schema_output(self, tmp_path):
        target = tmp_path / "proj"
        result = LakehousePlumberBootstrap().init_project(
            target, bundle=False, project_name="proj"
        )
        assert result.success, result.error_message

        gitignore = (target / ".gitignore").read_text(encoding="utf-8")
        # ``.lhp/`` is already broadly ignored, which covers the
        # translated ``.lhp/contracts/schemas/`` output the plan intends to keep
        # out of version control. Regression guard, not the RED pin.
        assert ".lhp/" in gitignore
