Dependency Analysis & Job Generation
=====================================

.. meta::
   :description: Automatic pipeline dependency detection and orchestration job generation for multi-pipeline Databricks workflows.

The Dependency Analysis feature automatically analyzes your pipeline structure to understand 
data flow dependencies, execution order, and external data sources. This enables intelligent 
orchestration job generation for Databricks.


Overview
--------

Lakehouse Plumber analyzes your :term:`FlowGroup` YAML files to build a comprehensive dependency graph that shows:

- **Pipeline Dependencies**: Which pipelines depend on others
- **Execution Stages**: The optimal order for running pipelines
- **External Sources**: Data dependencies outside your LHP project
- **Parallel Opportunities**: Pipelines that can run simultaneously

This analysis powers orchestration job generation, enabling you to create Databricks jobs 
with proper task dependencies automatically.

**When to Use Dependency Analysis**

==================== ================================================================
Use Case             Description
==================== ================================================================
**Development**      Understand your pipeline architecture and data flow
**Validation**       Validate project structure for consistency
**Job Generation**   Create orchestration jobs with proper dependencies
**CI/CD**            Optimize build and deployment order
==================== ================================================================

.. note::
   Dependency analysis fully resolves flowgroups (templates, presets, and
   substitutions) but does not run per-flowgroup configuration validation.
   Run ``lhp validate`` to check flowgroup configurations.

Key Concepts
------------

Pipeline Dependencies
~~~~~~~~~~~~~~~~~~~~~

Dependencies are automatically detected by analyzing:

- **Table References**: SQL queries that reference tables from other pipelines
- **Python Functions**: Custom transformations that read from pipeline outputs
  (including ``spark.read``/``readStream`` table and relational-format reads)
- **CDC Snapshots**: :term:`Slowly Changing Dimension <SCD>` patterns with source functions
- **Delta Sinks**: a ``sink_type: delta`` write registers its target table, so
  downstream reads of it form internal edges
- **Explicit Declarations**: edges declared with the ``depends_on`` action field

External Sources
~~~~~~~~~~~~~~~~

External sources are data dependencies **outside** your LHP-managed pipelines:

- Source system tables (e.g., ``${catalog}.${migration_schema}.customers``)
- Legacy data sources (e.g., ``${catalog}.${old_schema}.orders``)
- Third-party data feeds

.. note::
   Internal pipeline outputs are **not** considered external sources - they're managed 
   dependencies within your LHP project.

Execution Stages
~~~~~~~~~~~~~~~~

Pipelines are organized into execution stages based on their dependencies:

+----------+---------------------------+----------------------------------------+
| Stage    | Pipelines                 | Dependencies                           |
+==========+===========================+========================================+
| Stage 1  | ``raw_ingestion``         | External sources only                  |
+----------+---------------------------+----------------------------------------+
| Stage 2  | ``bronze_layer``          | Depends on Stage 1                     |
+----------+---------------------------+----------------------------------------+
| Stage 3  | ``silver_layer``          | Depends on Stage 2                     |
+----------+---------------------------+----------------------------------------+
| Stage 4  | ``gold_layer``            | Depends on Stage 3                     |
+----------+---------------------------+----------------------------------------+

Pipelines within the same stage can run in **parallel**.

How Dependencies Are Resolved
------------------------------

Transforms may reference earlier views (or tables) via the ``source`` field.
LHP's resolver builds a DAG and checks for cycles.

**Dependency resolution process:**

1. **Parse source references** — Extract view/table dependencies from actions
2. **Build dependency graph** — Create directed acyclic graph (DAG) of dependencies
3. **Cycle detection** — Prevent circular dependencies that would cause runtime errors
4. **Topological ordering** — Generate actions in correct execution order

**Example dependency chain:**

.. code-block:: yaml
   :caption: Dependency example

   # raw_data.yaml - No dependencies (source)
   actions:
     - name: load_files
       type: load
       source: { type: cloudfiles, path: "/data/*.json" }
       target: v_raw_data

   # clean_data.yaml - Depends on v_raw_data
   actions:
     - name: clean_data
       type: transform
       source: v_raw_data  # ← Dependency
       target: v_clean_data

   # aggregated.yaml - Depends on v_clean_data
   actions:
     - name: aggregate
       type: transform
       source: v_clean_data  # ← Dependency
       target: v_aggregated

How Table References Are Matched
--------------------------------

The analyzer matches a read ``source`` against the table that another action
produces by **canonical value**, not by surface syntax. Before comparison, each
table reference is normalized: lowercased, with backticks and surrounding
whitespace stripped from every dotted part. So ``` `MyCat`.`MySchema`.`Orders` ```,
``MYCAT.MYSCHEMA.ORDERS``, and ``mycat.myschema.orders`` all match the same
producer.

**Two-part and three-part references.** A two-part ``schema.table`` source
matches a three-part ``catalog.schema.table`` producer only when the producing
catalog is **unambiguous**. If two or more catalogs each register a producer for
the same ``schema.table``, the reference is ambiguous and is treated as an
external source rather than forming a phantom cross-catalog edge. (Tables are
global; pipeline-scoped views are matched separately.)

**What registers as a producer.** A ``streaming_table`` or
``materialized_view`` write registers its ``write_target`` catalog / schema /
table as a producer. A ``type: sink`` write with ``sink_type: delta`` registers
its ``options.tableName``, so a downstream action that reads that table forms an
**internal** dependency edge. Sinks without a Delta table target —
``kafka``, ``custom`` (ForEachBatch), and Delta sinks writing to a ``path`` —
register **no** producer; they are terminal/external by design, and reads of
those destinations stay external.

.. note::
   Dependency analysis does **not** resolve substitution tokens. Matching is
   byte-level after canonicalization, and canonicalization only lowercases
   and strips backticks and surrounding whitespace — nothing else. A source
   written with a token (for example ``${catalog}.sales.orders``) does not
   match a producer written as a concrete literal
   (``acme_prod.sales.orders``), and vice versa — they are compared as the
   literal strings authored in the YAML. The same applies between token
   spellings: a producer whose table name carries a token is matched only by
   readers that use the **same token bytes**, so ``${catalog}.sales.orders``
   does not match ``{catalog}.sales.orders`` (``${token}`` is the
   recommended syntax; ``{token}`` is deprecated). To declare an edge across
   a token boundary, author both sides as literals or use :ref:`depends-on`
   below.

.. _depends-on:

Declaring Dependencies Explicitly with ``depends_on``
-----------------------------------------------------

Some edges cannot be parsed from SQL or Python sources — for example a table
referenced only through a token, built dynamically at runtime, or read by an
idiom the parser does not recognize. The optional ``depends_on`` field is the
escape hatch for these cases. It is available on **any** action and takes a list
of upstream table references:

.. code-block:: yaml
   :caption: Declaring an unparseable upstream edge

   actions:
     - name: build_summary
       type: transform
       transform_type: python
       source: v_orders
       module_path: "transforms/build_summary.py"
       function_name: run
       depends_on:
         - acme_prod.reference.exchange_rates   # catalog.schema.table
         - staging.late_arriving_dim            # schema.table
       target: v_summary

``depends_on`` is **additive**: its entries always contribute edges *on top of*
whatever the parser already extracts from the action's SQL or Python source — it
never replaces parsed dependencies.

A **non-empty** ``depends_on`` also **suppresses** that action's
:ref:`LHP-DEP-002 <lhp-dep-002>` / :ref:`LHP-DEP-003 <lhp-dep-003>` extraction
advisories: once you have declared the action's upstreams by hand, LHP stops
flagging the reads it could not resolve statically for that action. Suppression
is **per action, not per read** — matching a declared entry back to a specific
opaque read is uncomputable, so LHP cannot tell which unresolved read a given
``depends_on`` entry was meant to cover.

.. important::
   The trade-off is plain: an action with two opaque reads and only **one**
   declared upstream stops warning about the **second** read as well. When you
   add ``depends_on`` to silence an advisory, declare **every** upstream the
   action needs — not just the one the advisory named.

Each entry must be a well-formed table reference: a non-empty string of at most
three dot-separated parts (``catalog.schema.table``, ``schema.table``, or
``table``) with no blank parts. A malformed entry raises
:ref:`LHP-VAL-063 <lhp-val-063>`.

What Extraction Resolves Automatically
--------------------------------------

``lhp dag`` derives edges by statically parsing the SQL and Python bodies
your actions reference. The matrix below summarizes what each parser resolves
on its own and which reads need an explicit :ref:`depends_on <depends-on>`
declaration instead. The two sections that follow give the full rules.

.. list-table::
   :header-rows: 1
   :widths: 10 50 40

   * - Body
     - Resolved automatically
     - Needs ``depends_on``
   * - SQL
     - Table reads anywhere in a multi-statement body, parsed with sqlglot's
       Databricks dialect: ``FROM`` / ``JOIN`` references, the reads inside
       ``MERGE`` / ``INSERT`` / CTAS statements, ``stream()`` / ``live()`` /
       ``snapshot()``-wrapped names, quoted identifiers, and names carrying
       substitution tokens (``${token}``, ``${secret:scope/key}``, deprecated
       ``{token}``) — including mid-segment tokens such as
       ``cat.sch.tbl${suffix}``.
     - Any body sqlglot cannot parse. It contributes zero edges and emits one
       :ref:`LHP-DEP-003 <lhp-dep-003>` advisory.
   * - Python
     - Recognized Spark read calls whose table argument is statically known:
       string literals, module constants, conditional reassignment (union of
       candidates), f-strings over bound values, ``+`` concatenation,
       ``"{}.{}".format(...)``, the string methods ``.replace`` / ``.upper``
       / ``.lower`` / ``.strip`` / ``.lstrip`` / ``.rstrip`` /
       ``sep.join(...)``, and values bound from the action's YAML
       ``parameters`` (all three shapes). Resolution is **inter-procedural**
       within a file: function parameters (unioned across the same file's
       call sites, plus signature defaults), user-function return values
       (so ``spark.read.table(helper(x))`` now resolves), ``a or b`` /
       ``a and b`` folds, the collection builtins ``list`` / ``tuple`` /
       ``sorted`` / ``set`` / ``dict.fromkeys``, and ``for``-loops over any
       statically-foldable iterable (unrolled — one read per element).
     - Genuinely runtime-only values: ``os.environ`` and other runtime I/O,
       class attributes, a parameter fed a dynamic value at any call site,
       and anything past the depth (20) or value-set (256) caps. Each
       recognized-but-unresolvable read emits one
       :ref:`LHP-DEP-002 <lhp-dep-002>` advisory.

How ``lhp dag`` Extracts Dependencies from SQL
----------------------------------------------

.. versionchanged:: 0.9.0
   SQL extraction is parsed with sqlglot (Databricks dialect), replacing the
   earlier regex-based parser. Substitution tokens now survive extraction
   byte-for-byte even mid-segment: ``FROM cat.sch.tbl${suffix}`` extracts
   ``cat.sch.tbl${suffix}``, where the old parser silently truncated it to
   ``cat.sch.tbl``.

Every SQL body an action references — inline ``sql`` or an on-disk
``sql_path`` file — is parsed as Databricks SQL, multi-statement bodies
included. Extraction returns **reads only**:

- ``FROM`` / ``JOIN`` table references in any statement, including the reads
  inside ``MERGE``, ``INSERT``, and CTAS statements. The **write target** of
  ``MERGE`` / ``INSERT`` / ``CREATE`` / ``UPDATE`` / ``DELETE`` / ``DROP``
  statements is excluded — writing to a table does not make it an upstream
  dependency.
- Common table expression (CTE) names are excluded; only the real tables the
  CTEs read from are extracted.
- ``stream(...)``, ``live(...)``, and ``snapshot(...)`` wrappers are
  unwrapped (case-insensitively) to the table they name.
- Backtick-quoted identifiers are extracted unquoted.
- String literals are never mistaken for table references, even when they
  contain ``FROM``.
- The ``$source`` placeholder of SQL transforms is excluded: it refers to the
  action's own declared ``source`` view, which already carries that edge.

**Substitution tokens survive byte-for-byte.** Tokens are not valid SQL, so
``${token}``, ``${secret:scope/key}``, and the deprecated ``{token}`` form
are masked before parsing and restored afterwards, preserving the exact bytes
you authored — including mid-segment forms like ``cat.sch.tbl${suffix}``.
Tokens are never resolved at analysis time; see
`How Table References Are Matched`_.

**Parse failures never fail the run.** A body sqlglot cannot parse yields
zero table references and exactly one :ref:`LHP-DEP-003 <lhp-dep-003>`
advisory suggesting an explicit ``depends_on`` declaration — never an error.

How ``lhp dag`` Extracts Dependencies from Python Code
------------------------------------------------------

When an action carries Python code — a transform's ``module_path``, a python
load's ``source.module_path``, or code inside a ``write_target`` (custom
sinks, ForEachBatch handlers, or CDC snapshot functions) — ``lhp dag``
statically analyzes the Python source to extract table references from Spark
calls.

**Calls the parser recognizes:**

- ``spark.table("cat.sch.t")``
- ``spark.read.table("cat.sch.t")`` and ``spark.readStream.table("cat.sch.t")``
- ``spark.read.format("delta"|"iceberg"|"hive"|"unity_catalog").table("cat.sch.t")``
  and ``.load("cat.sch.t")`` — including the ``readStream`` variants — whenever
  the table name is statically resolvable.
- ``spark.catalog.tableExists("cat.sch.t")``
- ``spark.catalog.dropTempView("cat.sch.t")``
- ``spark.sql("...")`` — the SQL string is resolved through the same static
  machinery, then parsed with the sqlglot-based SQL extraction above. An
  argument that cannot be resolved emits one
  :ref:`LHP-DEP-002 <lhp-dep-002>` advisory; a resolved string that does not
  parse emits one :ref:`LHP-DEP-003 <lhp-dep-003>` advisory.

Only the relational-table formats ``delta``, ``iceberg``, ``hive``, and
``unity_catalog`` register a dependency. ``cloudFiles`` (Auto Loader) reads and
``custom_datasource`` reads remain external roots — they are entry points into
the project, not edges within it.

The parser also follows local variable bindings inside direct table calls::

    tbl = "cat.sch.orders"
    spark.read.table(tbl)        # resolves to "cat.sch.orders"

**YAML parameters resolve the way generated code applies them.**

.. versionadded:: 0.9.0

The values an action declares in YAML flow into the entry function's scope
exactly as the generated code passes them at runtime. The entry function is
looked up at the top level of the module by name; a signature mismatch binds
nothing — the parser never guesses.

.. list-table::
   :header-rows: 1
   :widths: 28 30 42

   * - Action shape
     - YAML parameters
     - How generated code passes them
   * - Python transform (``transform_type: python``)
     - ``parameters:`` (flat on the action)
     - The dict is one positional argument: third with at least one source
       view (``fn(df, spark, parameters)``), second with none
       (``fn(spark, parameters)``).
   * - Python load (``source.type: python``)
     - ``source.parameters``
     - The dict is the second positional argument
       (``fn(spark, parameters)``); ``function_name`` defaults to
       ``get_df``.
   * - Snapshot CDC ``source_function``
     - ``source_function.parameters``
     - Each entry is bound as a keyword argument via ``functools.partial``;
       the function receives them as keyword-only arguments.

Inside the entry function, ``parameters["key"]`` lookups and
``parameters.get("key", default)`` calls resolve to the declared values, so a
read like this needs no ``depends_on``:

.. code-block:: yaml
   :caption: Declared parameter feeding a table read

   - name: load_external
     type: load
     source:
       type: python
       module_path: "loaders/external.py"
       parameters:
         table: "${catalog}.bronze.orders"
     target: v_external

.. code-block:: python
   :caption: loaders/external.py

   def get_df(spark, parameters):
       # Resolves to "${catalog}.bronze.orders" — token bytes preserved.
       return spark.read.table(parameters["table"])

**What the parser can resolve:**

- Simple assignments: ``tbl = "literal"`` or ``tbl: str = "literal"``.
- Chained assignments: ``a = b = "literal"``.
- Tuple / list unpacking where both sides are parallel literals:
  ``a, b = "x", "y"``.
- Reassignments and conditional branches — every possible literal value is
  emitted (union semantics)::

      tbl = "cat.sch.a"
      if cond:
          tbl = "cat.sch.b"
      spark.table(tbl)         # emits both "cat.sch.a" and "cat.sch.b"

- Module-level constants referenced inside functions.
- Values bound from YAML ``parameters`` (see the table above), including
  ``parameters["key"]`` and ``parameters.get("key", default)`` lookups.
- ``for``-loops over statically-foldable iterables — the loop unrolls,
  emitting one read per element (including iteration over dict keys)::

      for t in ["cat.sch.a", "cat.sch.b"]:
          spark.read.table(t)  # emits both "cat.sch.a" and "cat.sch.b"

- f-strings whose interpolations resolve to bound values, for example
  ``f"{parameters['catalog']}.sales.orders"``. An unresolved interpolation
  whose name matches a well-known token name (``catalog``, ``schema``,
  ``table``, ``bronze_schema``, ``silver_schema``, ``gold_schema``,
  ``migration_schema``, ``old_schema``) is preserved literally as
  ``{name}``; any other unresolved interpolation leaves the whole f-string
  unresolved.
- String methods on statically-resolved values: ``.replace(a, b)``,
  ``.upper()`` / ``.lower()``, ``.strip()`` / ``.lstrip()`` / ``.rstrip()``,
  and ``sep.join(items)``.
- String concatenation via ``+`` and ``"{}.{}".format(...)`` chains, **when
  every operand is itself statically resolvable**. Each side is resolved
  recursively and combined; if any operand is unresolvable, the whole
  expression is dropped.

.. versionchanged:: 0.9.1
   Resolution is now **inter-procedural** within a single file. In addition to
   the intra-function cases above, the parser also resolves:

- **Function parameters** — a parameter's value set is the union of the
  argument values passed at every call site of that function in the same
  file, plus any value bound from YAML ``parameters`` and the signature
  default. If a call site passes a dynamic value (or spreads ``*args`` /
  ``**kwargs``), the parameter stays unresolved.
- **User-function return values** — a bare-name call to a function defined in
  the same file resolves to the union of its ``return`` expressions
  (``return None`` and bare ``return`` are skipped; any dynamic return leaves
  the call unresolved). A read routed through a helper —
  ``spark.read.table(helper(x))`` — therefore now resolves whenever the
  helper's returns and its arguments are all statically knowable.
- **Boolean folds** — ``a or b`` and ``a and b`` fold to the union of both
  operands' value sets, so ``parameters or {}`` resolves to the ``parameters``
  dict.
- **Collection builtins** — ``list(x)``, ``tuple(x)``, ``sorted(x)``,
  ``set(x)``, and ``dict.fromkeys(x)`` fold as identity on the element value
  set.

Inter-procedural resolution is memoized and cycle-guarded (a recursive
resolution yields "unknown", keeping the advisory), depth-capped at 20 and
value-set-capped at 256. It is never speculative: anything genuinely dynamic
(for example ``os.environ``) still emits an :ref:`LHP-DEP-002 <lhp-dep-002>`
advisory.

**What the parser cannot resolve:**

- Genuinely runtime-only values — ``os.environ`` and other runtime I/O, or a
  function parameter that receives a dynamic (non-static) value at any of its
  call sites.
- String concatenation or ``.format()`` where any operand is non-static.
- Class attributes (``self.tbl``, class-body bindings seen from methods).
- ``for``-loops over iterables that are not statically foldable.
- ``nonlocal`` / ``global`` declarations.
- Anything requiring resolution deeper than the depth cap (20) or wider than
  the value-set cap (256); recursive resolution resolves to "unknown".

A recognized read call whose argument cannot be resolved emits one
:ref:`LHP-DEP-002 <lhp-dep-002>` advisory. For these cases, declare the edge
explicitly with :ref:`depends_on <depends-on>`::

    - name: my_transform
      type: transform
      transform_type: python
      source: v_orders
      module_path: transforms/my_transform.py
      function_name: run
      depends_on:
        - acme_edw_dev.edw_silver.parameterized_table

**Precedence between parser output and explicit source:**

The analyzer treats SQL parsing as authoritative — if a SQL body produces any
extracted sources, the explicit ``source:`` declaration is not additionally
consulted. For Python, the analyzer takes the **union** of parser output and
explicit ``source:``, because Python parsing is best-effort:

+--------+---------------------------------------------+
| Body   | Behavior                                    |
+========+=============================================+
| SQL    | Parser wins. Explicit ``source:`` ignored   |
|        | when parser finds any references.           |
+--------+---------------------------------------------+
| Python | Parser ∪ explicit ``source:``.              |
+--------+---------------------------------------------+
| None   | Falls back to explicit ``source:`` only.    |
+--------+---------------------------------------------+

``depends_on`` entries sit outside this precedence: they are unioned on top
of the result in every case (see :ref:`depends-on`).

**Locations the parser inspects:**

- ``action.sql``, ``action.sql_path``
- ``action.source["sql"]``, ``action.source["sql_path"]``
- ``action.write_target["sql"]``, ``action.write_target["sql_path"]``
  (materialized views)
- ``action.module_path`` (Python transforms)
- ``action.source["module_path"]`` (python loads)
- ``action.write_target["module_path"]`` (custom sinks)
- ``action.write_target["batch_handler"]`` (inline ForEachBatch code)
- ``action.write_target["snapshot_cdc_config"]["source_function"]["file"]``
  (CDC snapshot functions)

Custom-sink modules and ForEachBatch handlers have no YAML parameters
mechanism in generated code, so those bodies parse without parameter
bindings.

.. _extraction-warnings:

Dependency Extraction Warnings
------------------------------

.. versionadded:: 0.9.0

Extraction emits two **advisory** warning codes. They never fail a run and
are never raised as errors — they tell you which reads the analyzer saw but
could not turn into dependency edges.

.. list-table::
   :header-rows: 1
   :widths: 18 50 32

   * - Code
     - Meaning
     - Remediation
   * - :ref:`LHP-DEP-002 <lhp-dep-002>`
     - A recognized Python table-read call whose table argument cannot be
       statically resolved — its value is only known at runtime. The message
       quotes the unresolved argument expression, e.g. *Cannot statically
       resolve the table argument of* ``spark.read.table(...)`` *— the value
       of* ``os.environ['TBL']`` *is only known at runtime.*
     - Declare the upstream table with :ref:`depends_on <depends-on>` on the
       action.
   * - :ref:`LHP-DEP-003 <lhp-dep-003>`
     - A SQL body could not be parsed for table extraction. One warning per
       unparseable body; the body contributes zero edges.
     - Fix the SQL, or declare the upstream tables with ``depends_on``.

``depends_on`` entries are themselves validated:
a malformed entry raises :ref:`LHP-VAL-063 <lhp-val-063>`. A non-empty
``depends_on`` also **suppresses** the action's advisories entirely — see
:ref:`depends-on`.

Warnings are aggregated per read site
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionchanged:: 0.9.1
   Warnings are grouped by unresolved read **site**, not per affected action.

A single opaque read — one helper module referenced by hundreds of actions,
say — used to emit one near-identical warning per action. Warnings are now
grouped by ``(code, file, line, message)``, so each record represents **one
unresolved read site** and lists every action it affects. A project that once
produced 11,451 near-identical warnings from four helper sites now reports
**four** records.

Each aggregated record carries:

- a representative ``flowgroup`` / ``action`` (the first affected action,
  sorted),
- ``edit_yaml_path`` — the YAML file a ``depends_on`` fix belongs in (the
  flowgroup's own YAML, or the **blueprint** YAML for a blueprint-synthetic
  flowgroup),
- ``affected_actions`` — the full list of affected actions, each with its own
  ``flowgroup`` / ``action`` / ``edit_yaml_path``, and
- ``affected_count`` — how many actions the site affects.

**Where warnings appear:**

- **Terminal (stderr).** ``lhp dag`` prints a count header
  (``N unresolved read site(s) affecting M action(s):``) followed by a
  grouped ``Extraction warnings`` table with columns **Code**, **Unresolved
  read** (``file:line``), **Reason**, **Affected** (the count plus the first
  three ``flowgroup.action`` entries, then ``+N more``), and **Add
  depends_on in** (the distinct YAML paths to edit). The table is capped at
  50 sites with an overflow row, and one ``depends_on`` hint follows.
- **JSON output.** The top-level ``warnings`` array is always present (empty
  when there are none). Each entry has ``code``, ``message``, ``flowgroup``,
  ``action``, ``suggestion``, ``file_path``, ``line``, plus the aggregation
  fields ``edit_yaml_path``, ``affected_actions`` (an array of
  ``{flowgroup, action, edit_yaml_path}`` objects), and ``affected_count``.
  For file-based bodies, ``file_path`` is the resolved ``.sql`` / ``.py``
  file; for inline bodies it is the flowgroup YAML file. In ``metadata``,
  ``total_warnings`` counts **distinct sites** and
  ``total_warning_occurrences`` counts the site × affected-action pairs.
- **Text report.** A ``DEPENDENCY EXTRACTION WARNINGS`` section renders one
  block per site, each with an ``Affected (N): ...`` line (the first five
  actions, then ``+N more``) and an ``Add depends_on in: ...`` line.

Warnings do **not** appear in the DOT output or in generated orchestration
job YAML.

In the Python API, ``lhp.api`` exposes the same records as
``DependencyWarningView`` entries on ``DependencyAnalysisResult.warnings``;
each view carries the ``edit_yaml_path`` / ``affected_actions`` /
``affected_count`` fields (``affected_actions`` is a tuple of
``AffectedActionView``). Both types are provisional stability.

For the per-code reference, see :doc:`errors_reference`.

Using the dag Command
---------------------

The ``lhp dag`` command runs dependency analysis and writes one or more
output formats.

.. versionchanged:: 0.9.0
   The command was renamed from ``lhp deps`` to ``lhp dag``. ``lhp deps``
   still works as a hidden alias but is deprecated and prints a deprecation
   notice.

**Basic Usage**

.. code-block:: bash

   # Full analysis with all formats
   lhp dag

   # Generate only orchestration job
   lhp dag --format job --job-name my_etl_job

   # Structured dependency graph only
   lhp dag --format json

   # Custom output directory
   lhp dag --output /path/to/analysis

Command Options
~~~~~~~~~~~~~~~

.. code-block:: text

   lhp dag [OPTIONS]

**Options:**

``--format``
    Output format(s), comma-separated: ``dot``, ``json``, ``text``, ``job``,
    ``all`` (default: ``all``)

    - ``dot``: GraphViz diagram for visualization
    - ``json``: Structured data for programmatic use
    - ``text``: Human-readable analysis report
    - ``job``: Databricks orchestration job YAML
    - ``all``: Generate all formats

``--job-name, -j``
    Custom name for generated orchestration job (only with ``job`` format)

``--job-config, -jc``
    Path to job configuration file

``--output, -o``
    Output directory (defaults to ``.lhp/dependencies/``)

``--bundle-output, -b``
    Save job file directly to ``resources/`` directory

``--expand-blueprints``
    **Deprecated, ignored no-op.** Blueprint synthetic flowgroups are always
    fully expanded — one graph node per instance — so this flag no longer
    changes anything. Passing it prints a deprecation notice and emits a
    ``DeprecationWarning``. It will be removed in a future release. (See
    :doc:`blueprints` for why full expansion is the only correct mode.)

``--blueprint``
    Restrict the analysis to one blueprint (unchanged).

Output Formats
--------------

Text Report
~~~~~~~~~~~

Human-readable analysis showing pipeline details, execution order, and dependency tree:

.. code-block:: text

   ================================================================================
   LAKEHOUSE PLUMBER - PIPELINE DEPENDENCY ANALYSIS
   ================================================================================
   Generated at: 2025-09-25 12:50:59

   SUMMARY
   ----------------------------------------
   Total Pipelines: 7
   Total Execution Stages: 6
   External Sources: 7
   Circular Dependencies: 0

   EXECUTION ORDER
   ----------------------------------------
   Stage 1: unirate_api_ingestion, acmi_edw_raw (can run in parallel)
   Stage 2: acmi_edw_bronze
   Stage 3: acmi_edw_silver
   Stage 4: acmi_edw_gold

JSON Data
~~~~~~~~~

Structured data perfect for integration with other tools:

.. code-block:: json

   {
     "metadata": {
       "total_pipelines": 7,
       "total_external_sources": 7,
       "total_stages": 6,
       "has_circular_dependencies": false,
       "total_warnings": 0,
       "total_warning_occurrences": 0
     },
     "pipelines": {
       "acmi_edw_bronze": {
         "depends_on": ["acmi_edw_raw"],
         "flowgroup_count": 14,
         "action_count": 80,
         "external_sources": [
           "${catalog}.${migration_schema}.customers"
         ],
         "stage": 1
       }
     },
     "execution_stages": [
       ["unirate_api_ingestion", "acmi_edw_raw"],
       ["acmi_edw_bronze"],
       ["acmi_edw_silver"]
     ],
     "warnings": []
   }

The top-level ``warnings`` array carries the
:ref:`extraction advisories <extraction-warnings>` and is always present,
even when empty.

GraphViz Diagram
~~~~~~~~~~~~~~~~

DOT format for creating visual dependency diagrams:

.. code-block:: text

   digraph pipeline_dependencies {
     rankdir=LR;
     node [shape=box];
     "acmi_edw_raw" [label="acmi_edw_raw\n(16 flowgroups)"];
     "acmi_edw_bronze" [label="acmi_edw_bronze\n(14 flowgroups)"];
     "acmi_edw_raw" -> "acmi_edw_bronze";
   }

.. tip::
   Use tools like Graphviz or online DOT viewers to visualize your pipeline dependencies as diagrams.

Orchestration Job Generation
----------------------------

The most powerful feature is automatic **orchestration job generation**. This creates a 
Databricks job YAML file with proper task dependencies based on your pipeline analysis.

Generating Jobs
~~~~~~~~~~~~~~~

.. code-block:: bash

   # Generate job with custom name
   lhp dag --format job --job-name data_warehouse_etl

   # Generate job and save directly to resources/
   lhp dag --format job --job-name data_warehouse_etl --bundle-output

   # Generate with custom configuration
   lhp dag --format job --job-config config/job_config.yaml --bundle-output

Generated Job Structure
~~~~~~~~~~~~~~~~~~~~~~~

The generated job YAML follows Declarative Automation Bundles format:

.. code-block:: yaml
   :caption: resources/data_warehouse_etl.job.yml

   resources:
     jobs:
       data_warehouse_etl:
         name: data_warehouse_etl
         max_concurrent_runs: 1
         tasks:
           - task_key: acmi_edw_raw_pipeline
             pipeline_task:
               pipeline_id: ${resources.pipelines.acmi_edw_raw_pipeline.id}
               full_refresh: false

           - task_key: acmi_edw_bronze_pipeline
             depends_on:
               - task_key: acmi_edw_raw_pipeline
             pipeline_task:
               pipeline_id: ${resources.pipelines.acmi_edw_bronze_pipeline.id}
               full_refresh: false

         queue:
           enabled: true
         performance_target: STANDARD

Key Features
~~~~~~~~~~~~

**Automatic Task Dependencies**
    Tasks are linked with ``depends_on`` clauses based on pipeline dependencies

**Pipeline Resource References**
    Uses ``${resources.pipelines.{name}_pipeline.id}`` for proper bundle integration

**Parallel Execution**
    Pipelines in the same stage have no interdependencies and can run in parallel

**Configurable Options**
    Customize with job configuration files (see below)

Customizing Job Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a ``job_config.yaml`` file to customize job settings:

.. code-block:: yaml
   :caption: config/job_config.yaml

   max_concurrent_runs: 2
   performance_target: PERFORMANCE_OPTIMIZED
   timeout_seconds: 7200
   
   queue:
     enabled: true
   
   tags:
     environment: production
     team: data-platform
   
   email_notifications:
     on_start:
       - admin@example.com
     on_success:
       - team@example.com
     on_failure:
       - oncall@example.com
   
   webhook_notifications:
     on_failure:
       - id: pagerduty-webhook
   
   permissions:
     - level: CAN_MANAGE
       user_name: admin@company.com
     - level: CAN_VIEW
       group_name: data-team
   
   schedule:
     quartz_cron_expression: "0 0 8 * * ?"
     timezone_id: America/New_York
     pause_status: UNPAUSED

**Using Custom Config:**

.. code-block:: bash

   # Use custom config file
   lhp dag --format job --job-config config/job_config.yaml --bundle-output

.. seealso::
   For complete job configuration options, see :doc:`bundle_config_reference`.

Integration with Databricks Bundles
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The generated job integrates seamlessly with Declarative Automation Bundles:

.. code-block:: bash

   # Generate job directly to resources/
   lhp dag --format job --job-name my_etl --bundle-output
   
   # Deploy with bundle commands
   databricks bundle deploy --target dev
   
   # Run the job
   databricks bundle run my_etl --target dev

Examples
--------

Simple ETL Pipeline
~~~~~~~~~~~~~~~~~~~

For a basic three-tier architecture:

.. code-block:: bash

   lhp dag --format job --job-name etl_pipeline --bundle-output

**Result**: Creates tasks for Raw → Bronze → Silver → Gold with proper dependencies.

**Generated Task Structure:**

.. code-block:: text

   etl_pipeline
   ├── raw_ingestion_pipeline (Stage 1, no dependencies)
   ├── bronze_layer_pipeline (Stage 2, depends on raw)
   ├── silver_layer_pipeline (Stage 3, depends on bronze)
   └── gold_layer_pipeline (Stage 4, depends on silver)

Complex Multi-Source Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For pipelines with multiple data sources and parallel processing:

.. code-block:: bash

   lhp dag --format all --job-name multi_source_etl

**Analysis shows:**

- Multiple Stage 1 pipelines (can run in parallel)
- Convergence in later stages
- Proper orchestration of dependent transformations

**Example Output:**

.. code-block:: text

   EXECUTION ORDER
   ----------------------------------------
   Stage 1: api_ingestion, sftp_ingestion, db_ingestion (parallel)
   Stage 2: bronze_consolidation (waits for all Stage 1)
   Stage 3: silver_transformations
   Stage 4: gold_aggregations

Troubleshooting
---------------

Circular Dependencies
~~~~~~~~~~~~~~~~~~~~~

If circular dependencies are detected:

.. code-block:: text

   ERROR: Circular dependencies detected:
   Pipeline A → Pipeline B → Pipeline C → Pipeline A

**Solution**: Review your FlowGroup SQL queries and break the circular reference by:

- Using temporary views instead of direct table references
- Restructuring data flow to eliminate cycles

Missing Dependencies
~~~~~~~~~~~~~~~~~~~~

If expected dependencies aren't detected:

**Check:**

- The :ref:`extraction warnings <extraction-warnings>` on the ``lhp dag``
  output — ``LHP-DEP-002`` / ``LHP-DEP-003`` name the exact action and
  ``file:line`` the analyzer could not resolve
- SQL table references use correct naming patterns
- Python functions properly reference source tables
- CDC snapshot configurations are correctly structured
- Producer and reader spell substitution tokens with the same bytes
  (``${catalog}.x`` does not match ``{catalog}.x``)

For any read the analyzer cannot resolve, declare the edge with
:ref:`depends_on <depends-on>`.

External Source Issues
~~~~~~~~~~~~~~~~~~~~~~

If too many external sources are detected:

.. code-block:: text

   WARNING: 50 external sources detected

**Review:**

- CTE names aren't being excluded (should be filtered automatically)
- Internal pipeline references are properly formatted
- Template variables are correctly structured

The dependency analyzer only considers table references in SQL queries and
Python functions. A table reference it recognizes but cannot resolve
statically surfaces as an ``LHP-DEP-002`` / ``LHP-DEP-003``
:ref:`advisory <extraction-warnings>` — declare those edges with
:ref:`depends_on <depends-on>`.

CLI Quick Reference
-------------------

.. code-block:: bash

   # Full analysis with all output formats
   lhp dag

   # Generate orchestration job
   lhp dag --format job --job-name my_etl

   # Save job directly to bundle resources
   lhp dag --format job --job-name my_etl --bundle-output

   # Use custom job configuration
   lhp dag -jc config/job_config.yaml --bundle-output

   # Structured dependency graph as JSON
   lhp dag --format json

   # Custom output directory
   lhp dag --output ./analysis

Related Documentation
---------------------

* :doc:`bundle_config_reference` - Bundle integration and configuration
* :doc:`architecture` - Understanding pipelines and flowgroups
* :doc:`cicd` - CI/CD patterns and deployment workflows
* :doc:`cli` - Complete CLI command reference
* :doc:`errors_reference` - Error and warning code reference (including ``LHP-DEP-002`` / ``LHP-DEP-003``)
