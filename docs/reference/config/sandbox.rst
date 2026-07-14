Sandbox configuration
=====================

.. meta::
   :description: Reference for LHP developer sandbox mode — the --sandbox flag, the sandbox block in lhp.yaml, the .lhp/profile.yaml personal profile, their YAML fields and defaults, scope resolution, and rename semantics.

Developer sandbox mode scopes ``lhp generate`` / ``lhp validate`` to the
pipelines a developer declares in a personal profile and renames the tables
those pipelines produce. It draws on three surfaces: the ``--sandbox`` flag,
an optional ``sandbox:`` block in ``lhp.yaml`` (team policy), and a gitignored
``.lhp/profile.yaml`` (personal scope).

Activation flag
---------------

``--sandbox`` is a boolean flag on ``lhp generate`` and ``lhp validate`` only.

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Invocation
     - Behavior
   * - ``lhp generate --env <env> --sandbox``
     - Generates only the in-scope pipelines, with renames applied.
   * - ``lhp validate --env <env> --sandbox``
     - Validates only the in-scope pipelines, with the structured renames applied.
   * - ``--sandbox`` with ``-p`` / ``--pipeline``
     - Rejected with a Click usage error (exit code 2): sandbox scope comes from ``.lhp/profile.yaml``, so it cannot be narrowed with ``-p``.

Team policy — ``sandbox:`` block in ``lhp.yaml``
------------------------------------------------

Optional top-level block. When absent, all defaults below apply. A block that
is not a mapping fails with ``LHP-CFG-062``.

.. list-table::
   :header-rows: 1
   :widths: 20 18 20 42

   * - Field
     - Type
     - Default
     - Description
   * - ``strategy``
     - string
     - ``table``
     - Rename strategy. Only ``table`` is accepted in v1; any other value fails with ``LHP-CFG-062``.
   * - ``table_pattern``
     - string
     - ``{namespace}_{table}``
     - Python ``str.format`` pattern applied to the table leaf. Must contain exactly ``{namespace}`` and ``{table}``, both present and no others; no conversions (``!r``) or format specs (``:>10``); literal text limited to ``[A-Za-z0-9_]``. Invalid patterns fail with ``LHP-CFG-063``. These braces are ``str.format`` placeholders, not ``${token}`` substitution tokens.
   * - ``allowed_envs``
     - list[string]
     - — (unrestricted)
     - Environments where ``--sandbox`` is permitted. Absent or ``null`` allows any environment. An empty list fails with ``LHP-CFG-062``; running ``--sandbox`` against an environment not in the list fails with ``LHP-CFG-065``.

.. code-block:: yaml

   # lhp.yaml
   sandbox:
     strategy: table
     table_pattern: "{namespace}_{table}"
     allowed_envs: [dev, tst]

Personal profile — ``.lhp/profile.yaml``
-----------------------------------------

Per-developer namespace and pipeline scope, nested under a top-level
``sandbox:`` key in ``<project-root>/.lhp/profile.yaml``. The ``.lhp/``
directory is gitignored. A missing file fails with ``LHP-IO-025``; a malformed
profile (bad YAML, non-mapping root, missing ``sandbox:`` key, ``namespace``
regex failure, or empty ``pipelines`` list) fails with ``LHP-CFG-064``.

.. list-table::
   :header-rows: 1
   :widths: 18 18 12 52

   * - Field
     - Type
     - Required
     - Description
   * - ``namespace``
     - string
     - Yes
     - Personal identifier prepended to renamed tables. Must match ``^[a-z][a-z0-9_]{0,63}$`` — a lowercase letter followed by up to 63 lowercase letters, digits, or underscores.
   * - ``pipelines``
     - list[string]
     - Yes
     - Non-empty. Exact pipeline names or case-sensitive ``fnmatchcase`` globs. An entry is a glob if it contains ``*``, ``?``, or ``[``.

.. code-block:: yaml

   # .lhp/profile.yaml
   sandbox:
     namespace: alice
     pipelines:
       - raw_ingestions
       - silver_*

Scope resolution
----------------

Each ``pipelines`` entry is expanded against the project's discovered
pipelines.

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - Rule
     - Behavior
   * - Glob expansion
     - Globs expand with ``fnmatchcase`` — case-sensitive, deterministic across platforms.
   * - Zero-match entries
     - Every entry matching zero pipelines is collected; all offenders fold into one ``LHP-VAL-064`` error.
   * - Monitoring pipeline
     - Excluded from glob expansion. An exact entry naming the monitoring pipeline fails with ``LHP-VAL-064``.

Rename semantics
----------------

``table_pattern`` formats the table leaf only; catalog and schema pass through
unchanged. With namespace ``alice`` and the default pattern,
``catalog.schema.table`` becomes ``catalog.schema.alice_table``. The rename set
is exactly the tables the in-scope pipelines produce; matching is
case-insensitive and backtick-aware.

.. list-table::
   :header-rows: 1
   :widths: 55 45

   * - Element
     - Rename behavior
   * - Streaming-table / materialized-view write target
     - Leaf renamed.
   * - Delta-sink ``options.tableName`` (``sink_type: delta`` only)
     - Leaf renamed.
   * - Catalog and schema of a renamed table
     - Pass through unchanged.
   * - In-scope reads of a produced (in-scope) table
     - Leaf renamed to match the producer.
   * - Reads of tables produced outside the scope
     - Unchanged (read-shared).
   * - Non-delta sinks and non-delta loads
     - Unchanged (no table identity).
   * - ``depends_on`` entries
     - Unchanged (dependency ordering only; never emitted in code).
