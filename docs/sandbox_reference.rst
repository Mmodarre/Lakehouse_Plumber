Sandbox Mode Reference
======================

.. meta::
   :description: Configuration reference for Lakehouse Plumber developer sandbox mode — the --sandbox flag, the sandbox block in lhp.yaml, the .lhp/profile.yaml personal profile, scope resolution, rename semantics, run behavior, and error codes.

.. versionadded:: 0.9.1

Developer sandbox mode gives each developer a personal, namespaced copy of
their slice of a Lakehouse Plumber (LHP) project without touching shared
tables. Passing ``--sandbox`` scopes the run to the pipelines you declare in a
gitignored personal profile (``.lhp/profile.yaml``) and renames the tables
those pipelines produce through a team-configurable pattern.

The core invariant is read-shared / write-own: only tables produced by
in-scope pipelines are renamed — the write itself plus every in-scope read of
them. Reads of tables produced outside your scope stay pointed at the shared
tables. Renames apply only to tables produced in scope because those are the
only tables a sandbox run writes; everything else is shared input that must
remain shared.

This page catalogs the flag, both configuration files, scope and rename
semantics, run behavior, and every sandbox error code. For the task
walk-through, see :doc:`develop_in_a_sandbox`.

CLI Activation
--------------

``--sandbox`` is a boolean flag on ``lhp generate`` and ``lhp validate``
only. It is not available on any other command.

.. list-table::
   :header-rows: 1
   :widths: 42 58

   * - Invocation
     - Behavior
   * - ``lhp generate --env <env> --sandbox``
     - Generates only the pipelines in your profile scope, with sandbox
       renames applied.
   * - ``lhp validate --env <env> --sandbox``
     - Validates only the pipelines in your profile scope, with the
       structured renames applied.
   * - ``--sandbox`` with ``-p`` / ``--pipeline``
     - Mutually exclusive. Raises a Click usage error (exit code 2):
       ``--sandbox cannot be combined with -p/--pipeline: sandbox scope comes
       from .lhp/profile.yaml``.
   * - ``--sandbox`` with ``--dry-run``
     - Supported. Reports what a sandbox generate would produce without
       writing files.
   * - ``--sandbox`` with ``--strict``
     - Supported. The sandbox warning codes ``LHP-VAL-065`` and
       ``LHP-VAL-066`` become failures.

Team Policy (``lhp.yaml``)
--------------------------

The optional ``sandbox:`` block in ``lhp.yaml`` sets the team-wide rename
policy. When the block is absent, all defaults below apply. A block that is
not a YAML mapping fails with :ref:`LHP-CFG-062 <lhp-cfg-062>`.

.. list-table:: ``sandbox`` fields
   :header-rows: 1
   :widths: 18 14 24 44

   * - Key
     - Type
     - Default
     - Constraints / meaning
   * - ``strategy``
     - string
     - ``table``
     - Rename strategy. v1 accepts only ``table``. Any other value fails with
       :ref:`LHP-CFG-062 <lhp-cfg-062>`.
   * - ``table_pattern``
     - string
     - ``{namespace}_{table}``
     - Python ``str.format`` pattern applied to the table leaf. Placeholders
       must be exactly ``{namespace}`` and ``{table}``, both present and no
       others; no conversions (``!r``) or format specs (``:>10``); literal
       text limited to ``[A-Za-z0-9_]``. An invalid pattern fails with
       :ref:`LHP-CFG-063 <lhp-cfg-063>`.
   * - ``allowed_envs``
     - list of strings
     - absent (unrestricted)
     - Environments where sandbox runs are allowed. Absent or ``null`` means
       any environment. An empty list fails with
       :ref:`LHP-CFG-062 <lhp-cfg-062>`; running ``--sandbox`` against an
       environment not in the list fails with
       :ref:`LHP-CFG-065 <lhp-cfg-065>`.

The braces in ``table_pattern`` are ``str.format`` placeholders, not
``${token}`` substitution tokens.

Personal Profile (``.lhp/profile.yaml``)
----------------------------------------

Each developer's namespace and pipeline scope live in
``<project-root>/.lhp/profile.yaml``, nested under a top-level ``sandbox:``
key. The init template's ``.gitignore`` excludes the ``.lhp/`` directory, so
the profile never enters version control. Sandbox is explicit opt-in: the
namespace and scope are never auto-detected.

.. list-table:: profile ``sandbox`` fields
   :header-rows: 1
   :widths: 18 18 64

   * - Field
     - Type
     - Constraints / meaning
   * - ``namespace``
     - string
     - Required. Must match ``^[a-z][a-z0-9_]{0,63}$`` — a lowercase letter
       followed by up to 63 lowercase letters, digits, or underscores.
   * - ``pipelines``
     - list of strings
     - Required, non-empty. Exact pipeline names or case-sensitive
       ``fnmatch``-style globs.

.. code-block:: yaml
   :caption: .lhp/profile.yaml

   sandbox:
     namespace: alice
     pipelines:
       - raw_ingestions
       - silver_*

A missing ``.lhp/profile.yaml`` fails with :ref:`LHP-IO-025 <lhp-io-025>`. A
malformed profile — unreadable file, invalid YAML, non-mapping root, missing
``sandbox:`` key, a ``namespace`` that fails the regex, or an empty
``pipelines`` list — fails with :ref:`LHP-CFG-064 <lhp-cfg-064>`.

Scope Resolution
----------------

LHP expands the profile's ``pipelines`` entries against the project's
discovered pipelines:

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - Rule
     - Behavior
   * - Glob detection
     - An entry is a glob if it contains any of ``*``, ``?``, or ``[``;
       otherwise it is an exact pipeline name.
   * - Glob expansion
     - Globs expand with Python ``fnmatchcase`` — case-sensitive and
       deterministic across platforms.
   * - Zero-match entries
     - Every entry that matches zero pipelines is collected; all offenders
       fold into one :ref:`LHP-VAL-064 <lhp-val-064>` error listing each
       offending entry and the available pipeline names.
   * - Monitoring pipeline
     - Silently excluded from glob expansion. An exact entry naming the
       monitoring pipeline fails with :ref:`LHP-VAL-064 <lhp-val-064>`.

Rename Semantics
----------------

``table_pattern`` formats the table leaf only; catalog and schema pass
through unchanged. For namespace ``alice`` and the default pattern,
``catalog.schema.table`` becomes ``catalog.schema.alice_table``.

A table is "produced in scope" when an in-scope pipeline writes it: the
rename set is exactly the streaming-table and materialized-view write targets
plus the delta-sink ``tableName`` options of the scoped pipelines. Matching
against the rename set is case-insensitive and backtick-aware (Unity Catalog
name semantics); each rewrite formats the original leaf spelling at that
site, so the author's casing survives inside the new name.

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - Rewritten (table leaf only)
     - Not rewritten
   * - * Streaming-table and materialized-view write targets
         (``write_target`` catalog/schema/table).
       * Delta-sink ``write_target.options.tableName`` (``sink_type: delta``
         only).
       * Snapshot Change Data Capture (CDC) ``snapshot_cdc_config.source``
         (dotted table-reference form only; ``source_function`` files are
         untouched).
       * Delta-load ``source`` catalog/schema/table (``source.type: delta``
         only).
       * ``source`` entries (string or list) that match the rename set.
       * Test-action ``reference`` and ``lookup_table`` fields.
       * SQL bodies inside generated ``spark.sql`` string literals (table
         references inside SQL-quoted strings and SQL comments are exempt).
       * Table string literals in copied Python modules (custom data source
         and Python transform files), including ``spark.sql(...)`` constant
         bodies.
       * Table references passed as YAML parameter values into user Python
         code — the python-transform ``parameters`` dict, the python-load
         ``source.parameters`` dict, and the snapshot-CDC
         ``source_function.parameters`` kwargs. A parameter value is renamed
         only when the whole string canonically matches the rename set;
         arbitrary parameter strings are untouched, and nested lists and
         dicts are walked element-wise.
     - * Reads of tables produced outside the sandbox scope (read-shared).
       * ``depends_on`` entries — they shape dependency ordering only and
         never appear in generated code.
       * Non-delta sinks and non-delta loads (no table identity).
       * Bare one-part view names (the rename set holds only two- and
         three-part names).
       * Per-pipeline explicit ``event_log:`` dicts (only the project-level
         event-log table name is namespaced).

Run Behavior
------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Surface
     - Sandbox behavior
   * - ``generated/<env>/``
     - Contains the Lakeflow Spark Declarative Pipelines (SDP) source files
       for exactly the scoped pipelines.
   * - ``resources/lhp/``
     - The Declarative Automation Bundles resource YAML regenerates for the
       scoped pipelines only.
   * - Project event log
     - The project-level event-log table name is namespaced through the same
       ``table_pattern``.
   * - Monitoring phase
     - Skipped. A sandbox run never regenerates the shared
       ``monitoring/<env>`` artifacts.

Warnings and Errors
-------------------

.. list-table::
   :header-rows: 1
   :widths: 22 14 64

   * - Code
     - Severity
     - Trigger
   * - :ref:`LHP-IO-025 <lhp-io-025>`
     - Error
     - ``.lhp/profile.yaml`` not found.
   * - :ref:`LHP-CFG-062 <lhp-cfg-062>`
     - Error
     - Invalid ``sandbox:`` block in ``lhp.yaml``: not a mapping, unknown
       ``strategy``, or empty ``allowed_envs`` list.
   * - :ref:`LHP-CFG-063 <lhp-cfg-063>`
     - Error
     - Invalid ``table_pattern``.
   * - :ref:`LHP-CFG-064 <lhp-cfg-064>`
     - Error
     - Invalid personal profile: bad YAML, missing ``sandbox:`` key,
       ``namespace`` regex failure, or empty ``pipelines`` list.
   * - :ref:`LHP-CFG-065 <lhp-cfg-065>`
     - Error
     - The selected environment is not in ``sandbox.allowed_envs``.
   * - :ref:`LHP-VAL-064 <lhp-val-064>`
     - Error
     - A profile ``pipelines`` entry matched zero pipelines, or an exact
       entry names the monitoring pipeline.
   * - :ref:`LHP-VAL-065 <lhp-val-065>`
     - Warning
     - Mixed producer: a sandbox-rewritten table is also produced by an
       out-of-scope pipeline. The rewrite proceeds.
   * - :ref:`LHP-VAL-066 <lhp-val-066>`
     - Warning
     - An in-scope read in a copied Python module could not be rewritten
       because the table reference is not a plain string literal. Emitted by
       ``lhp generate`` only.

The two warning codes carry the warning category ``sandbox`` and never fail a
run on their own; ``--strict`` promotes them to failures. For the full
catalog, see :doc:`errors_reference`.

v1 Limitations
--------------

.. list-table::
   :header-rows: 1
   :widths: 28 72

   * - Limitation
     - Detail
   * - Rename strategy
     - Only ``strategy: table``. Catalog and schema rename strategies are
       planned.
   * - Teardown
     - No teardown command. The Declarative Automation Bundles ``bundle
       destroy`` command removes sandbox streaming tables and materialized
       views, but delta-sink tables created by sandbox runs require manual
       cleanup.
   * - Validate-mode Python warnings
     - ``lhp validate --sandbox`` applies the structured renames but does not
       emit :ref:`LHP-VAL-066 <lhp-val-066>`.

See also
--------

* :doc:`develop_in_a_sandbox` — how-to walk-through for setting up a profile
  and running a sandbox generate.
* :doc:`errors_reference` — full error code reference.
* :doc:`bundle_config_reference` — bundle integration and generated resource
  layout.
