"""Integration test for ODCS contract project scaffolding.

Slice 4 replaced the file-output delivery (``ContractTranslationService`` +
``for_project(translate_contracts=...)`` + the ``contracts/lhp/`` gitignore) with
an in-memory, per-action resolution pass — see ``tests/test_contract_resolution.py``.
The only integration seam that survives here is project scaffolding: ``init`` must
create a ``contracts/`` directory for source ODCS contracts.
"""

from lhp.api import LakehousePlumberBootstrap


class TestInitScaffoldsContractsDir:
    """``init_project`` creates a ``contracts/`` directory for source contracts.

    Exercised through the public ``LakehousePlumberBootstrap`` (the same
    entry point ``lhp init`` routes to, per ``tests/test_cli_main_coverage.py``).
    Source ODCS contracts live in ``contracts/``; Slice 4 resolves them in
    memory, so there is no longer a generated ``contracts/lhp/`` output to ignore.
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

        assert (target / "contracts").is_dir(), (
            "init_project should scaffold a contracts/ directory for ODCS "
            "data contracts"
        )
