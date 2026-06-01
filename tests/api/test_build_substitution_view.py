"""Behavioral test for ``InspectionFacade.build_substitution_view``.

Closes a green-lie coverage gap: ``tests/api/test_substitution_view_contract.py``
exercises the :class:`SubstitutionView` *dataclass* in isolation (frozen
contract, pickle / JSON round-trips, field-type rules) but never drives the
facade method that actually reads ``substitutions/<env>.yaml``, constructs an
:class:`~lhp.core.processing.substitution.EnhancedSubstitutionManager`, and
projects its state onto the view.

This test builds a minimal real LHP project on disk, runs the project through
the public entrypoint
``LakehousePlumberApplicationFacade.for_project(...).inspection.build_substitution_view(env)``,
and asserts the two-field contract the manager is supposed to honour:

- ``tokens`` is a *flat* string projection — every value, including a
  nested-map token, is coerced via ``str(...)``.
- ``raw_mappings`` preserves the *un-coerced* nested structure — a nested-map
  token stays a real ``dict``.

It also pins the scalar token (present as a string) and the reserved
``workspace_env`` / ``logical_env`` tokens (auto-injected by the manager),
plus ``view.env``.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from lhp.api import LakehousePlumberApplicationFacade, SubstitutionView

ENV = "test"

# Token shapes the assertions depend on. Keep nested-dict values free of
# ``{token}`` / ``${token}`` syntax: the manager's recursive expansion only
# walks ``str`` values in ``mappings`` (nested dicts are left intact), but
# plain values keep the test independent of expansion behaviour.
NESTED_KEY = "catalog_map"
NESTED_VALUE = {"prod": "acme_prod", "dev": "acme_dev"}
SCALAR_STR_KEY = "catalog"
SCALAR_STR_VALUE = "acme_edw_test"
SCALAR_INT_KEY = "max_workers"
SCALAR_INT_VALUE = 4


def _write_minimal_project(root: Path) -> None:
    """Write the smallest project ``for_project`` will accept.

    ``ProjectConfig`` only requires ``name`` (all other fields default);
    ``enforce_version=False`` relaxes the version gate, and we omit
    ``required_lhp_version`` entirely. ``build_substitution_view`` reads only
    ``substitutions/<env>.yaml``, so no ``pipelines/`` / ``templates/`` /
    ``presets/`` content is needed for this path.
    """
    (root / "lhp.yaml").write_text(textwrap.dedent("""\
            name: substitution_view_test_project
            version: "1.0"
            """))

    subs_dir = root / "substitutions"
    subs_dir.mkdir(parents=True, exist_ok=True)
    # Nested-map value uses block style so YAML parses it as a real dict.
    (subs_dir / f"{ENV}.yaml").write_text(textwrap.dedent(f"""\
            {ENV}:
              {SCALAR_STR_KEY}: {SCALAR_STR_VALUE}
              {SCALAR_INT_KEY}: {SCALAR_INT_VALUE}
              {NESTED_KEY}:
                prod: {NESTED_VALUE["prod"]}
                dev: {NESTED_VALUE["dev"]}
            """))


@pytest.mark.unit
def test_build_substitution_view_flattens_tokens_and_preserves_raw_mappings(
    tmp_path: Path,
) -> None:
    _write_minimal_project(tmp_path)

    facade = LakehousePlumberApplicationFacade.for_project(
        tmp_path, enforce_version=False
    )
    view = facade.inspection.build_substitution_view(ENV)

    assert isinstance(view, SubstitutionView)

    # --- env -----------------------------------------------------------------
    assert view.env == ENV

    # --- nested-map token: flat string in tokens, real dict in raw_mappings --
    assert isinstance(view.tokens[NESTED_KEY], str)
    assert view.tokens[NESTED_KEY] == str(NESTED_VALUE)

    assert isinstance(view.raw_mappings[NESTED_KEY], dict)
    assert view.raw_mappings[NESTED_KEY] == NESTED_VALUE

    # --- scalar tokens: present as strings ------------------------------------
    assert view.tokens[SCALAR_STR_KEY] == SCALAR_STR_VALUE
    assert isinstance(view.tokens[SCALAR_STR_KEY], str)
    # Int scalar is coerced to its str repr for the flat field.
    assert view.tokens[SCALAR_INT_KEY] == str(SCALAR_INT_VALUE)
    assert isinstance(view.tokens[SCALAR_INT_KEY], str)

    # --- reserved tokens: auto-injected by the manager as the env name --------
    # The manager seeds ``workspace_env`` / ``logical_env`` into ``mappings``
    # (as plain strings), so they land in BOTH the flat ``tokens`` projection
    # and the structure-preserving ``raw_mappings``.
    assert view.tokens["workspace_env"] == ENV
    assert view.tokens["logical_env"] == ENV
    assert view.raw_mappings["workspace_env"] == ENV
    assert view.raw_mappings["logical_env"] == ENV
