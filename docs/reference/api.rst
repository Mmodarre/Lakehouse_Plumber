=============
Python API
=============

.. meta::
   :description: Python API reference for Lakehouse Plumber (LHP) — public classes, modules, and stability tiers for programmatic pipeline generation.

The Lakehouse Plumber (LHP) Python API exposes the same generation engine that
powers the ``lhp`` command-line interface. Import it to build custom tooling:
notebook integrations, CI/CD wrappers, programmatic validation in tests, or
embedding LHP inside a larger orchestrator. For day-to-day pipeline authoring,
prefer the CLI — the Python API trades convenience for control.

This page catalogs the public surface by stability tier and renders live
docstrings via Sphinx autodoc. The tier table is the contract; the autodoc
sections below it are the source-of-truth signatures pulled from the code at
build time.

Stability tiers
===============

LHP follows a four-tier stability model. The tier governs what level of change
to expect between minor and major versions.

.. list-table::
   :header-rows: 1
   :widths: 15 25 60

   * - Tier
     - Change policy
     - Definition
   * - **Stable**
     - Semantic versioning; breaking changes only at major releases.
     - Public API in active use by external integrations. Method signatures,
       return shapes, and observable behavior are versioned.
   * - **Beta**
     - May change between minor versions with a deprecation cycle of at least
       one release.
     - Public API still settling. Suitable for production use if you pin LHP
       to a minor version.
   * - **Experimental**
     - May change without notice at any release, including patch releases.
     - Public API exposed for evaluation. Do not depend on it from
       long-lived code.
   * - **Internal**
     - May change at any time. Not part of the API contract.
     - Implementation detail. Importing it from outside ``lhp.*`` is
       unsupported even when the import path resolves.

If a symbol is not listed on this page, treat it as Internal regardless of its
import path.

Public API by tier
==================

Stable
------

No symbol is currently Stable. LHP is pre-1.0; the CLI is the stable contract.
Python API consumers should treat the Beta tier as the recommended entry point
and pin to a minor version of LHP.

Beta
----

These symbols are the recommended entry points for programmatic use.

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Symbol
     - Summary
   * - ``lhp.api.LakehousePlumberApplicationFacade``
     - Public entry point; construct via ``.for_project(project_root)``. Drives
       discovery, validation, code generation, and bundle sync through its
       sub-facades.
   * - ``lhp.parsers.yaml_parser.YAMLParser``
     - Parses and validates LHP YAML files into Pydantic models. Use to load
       flowgroups, presets, and templates without invoking the full pipeline.
   * - ``lhp.parsers.yaml_parser.CachingYAMLParser``
     - Thread-safe caching wrapper around ``YAMLParser``. Suitable when
       parsing the same files repeatedly within one process.
   * - ``lhp.presets.preset_manager.PresetManager``
     - Loads presets from a directory and resolves inheritance chains.
   * - ``lhp.bundle.manager.BundleManager``
     - Synchronizes Databricks bundle resource files with generated pipeline
       code.
   * - ``lhp.models``
     - Pydantic models for the on-disk YAML schema: ``FlowGroup``, ``Action``,
       ``Preset``, ``Template``, ``ProjectConfig``, ``Blueprint``, and the
       associated enumerations (``ActionType``, ``LoadSourceType``,
       ``TransformType``, ``WriteTargetType``, ``TestActionType``).
   * - ``lhp.errors``
     - Public exception hierarchy: ``LHPError``, ``LHPConfigError``,
       ``LHPValidationError``, ``LHPFileError``. Catch these to handle LHP
       failures programmatically.

Experimental
------------

These symbols are exposed but may change without notice.

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Symbol
     - Summary
   * - ``lhp.utils.version.get_version``
     - Returns the installed LHP package version string.

Internal
--------

Everything else under ``lhp.*`` is Internal — including all of ``lhp.core.*``,
``lhp.generators.*``, ``lhp.cli.*``, ``lhp.schemas.*``, ``lhp.templates.*``,
``lhp.resources.*``, and any ``lhp.utils.*`` module not listed under Beta or
Experimental above. Drive LHP through the facade, not these modules.

.. important::

   Importing from an Internal namespace works today but provides no guarantees.
   Patch releases may move, rename, or remove these symbols without a
   deprecation notice.

Detailed reference
==================

The sections below render docstrings, type hints, and signatures directly from
the source code. Refer to the tier table above for stability guarantees on each
symbol.

Package root: ``lhp``
---------------------

.. automodule:: lhp
   :members:
   :undoc-members:
   :show-inheritance:

Application facade: ``lhp.api.facade``
--------------------------------------

.. automodule:: lhp.api.facade
   :no-members:

.. autoclass:: lhp.api.LakehousePlumberApplicationFacade
   :members:
   :show-inheritance:
   :member-order: bysource

YAML parsing: ``lhp.parsers.yaml_parser``
-----------------------------------------

.. automodule:: lhp.parsers.yaml_parser
   :no-members:

.. autoclass:: lhp.parsers.yaml_parser.YAMLParser
   :members:
   :show-inheritance:
   :member-order: bysource

.. autoclass:: lhp.parsers.yaml_parser.CachingYAMLParser
   :members:
   :show-inheritance:
   :member-order: bysource

Presets: ``lhp.presets.preset_manager``
---------------------------------------

.. automodule:: lhp.presets.preset_manager
   :no-members:

.. autoclass:: lhp.presets.preset_manager.PresetManager
   :members:
   :show-inheritance:
   :member-order: bysource

Bundle integration: ``lhp.bundle.manager``
------------------------------------------

.. automodule:: lhp.bundle.manager
   :no-members:

.. autoclass:: lhp.bundle.manager.BundleManager
   :members:
   :show-inheritance:
   :member-order: bysource

Configuration models: ``lhp.models``
------------------------------------

Pydantic models that mirror the on-disk YAML schema. Import these to type your
own loaders or to construct configurations programmatically.

.. automodule:: lhp.models
   :members:
   :show-inheritance:
   :member-order: bysource
   :exclude-members: model_config

Errors and exceptions: ``lhp.errors``
-------------------------------------

Catch ``LHPError`` for any LHP-originated failure. The concrete subclasses
disambiguate configuration, validation, and I/O failures and carry an error
code that maps to an entry in the error reference.

.. automodule:: lhp.errors
   :members: ErrorCategory, LHPError, LHPConfigError, LHPValidationError, LHPFileError
   :show-inheritance:
   :member-order: bysource

Version (Experimental): ``lhp.utils.version``
---------------------------------------------

.. automodule:: lhp.utils.version
   :members: get_version
   :show-inheritance:

Notes on usage
==============

- All public classes log through ``logging.getLogger("lhp.<module>")``.
  Configure the ``lhp`` logger to control LHP output independently of your
  application's logging.
- LHP raises ``LHPError`` (or a subclass) for every recoverable failure.
  Unexpected exceptions indicate a bug; file an issue with the traceback.
- Pydantic v2 powers ``lhp.models``. Treat the models as immutable once
  constructed; use ``model_copy(update=...)`` for derived instances.
- Construct one ``LakehousePlumberApplicationFacade`` per run; it is not
  thread-safe. ``CachingYAMLParser`` is thread-safe and shared internally.

Most users should drive LHP through the **CLI reference** rather than the Python
API. For the design rationale behind the engine, see the **How Lakehouse Plumber
works** concept page.
