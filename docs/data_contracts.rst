Data Contracts (ODCS)
=====================

.. meta::
   :description: How to drive Lakehouse Plumber from Open Data Contract Standard (ODCS) YAML contracts using a single `contract` action field — schemas (Auto Loader read schema / hints, write table_schema, schema-transform casts) and data-quality expectations are resolved inline before code generation.

`Open Data Contract Standard <https://bitol.io/open-data-contract-standard/>`_ (ODCS)
is a YAML format for declaring a dataset's schema and data-quality rules. Lakehouse
Plumber (LHP) lets an action **reference** a contract via a single ``contract`` field;
a resolution pass then rewrites that action **before code generation** so it carries the
resolved schema or expectations **inline**. No intermediate files are written — the
contract is the source of truth, resolved in memory on every ``lhp validate`` / ``lhp
generate`` run.

What a contract drives is **implicit from the action type**:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Action
     - What the contract injects
   * - ``cloudfiles`` **load**
     - ``source.schema`` (enforced read schema, inline DDL) — or
       ``cloudFiles.schemaHints`` **instead** when ``schema_hints: true``
       (hints-only; never both)
   * - **write** (``streaming_table`` / ``materialized_view``)
     - ``write_target.table_schema`` (inline DDL)
   * - ``schema`` **transform**
     - ``schema_inline`` (cast-only ``<col>: <type>`` entries)
   * - ``data_quality`` **transform**
     - inline ``expectations`` (list form)
   * - ``test`` (no ``test_type``)
     - **expands 1→N** into concrete ``test`` actions, one per ODCS ``quality`` rule

Add a contract
--------------

Place an ODCS contract YAML anywhere in the project (``lhp init`` scaffolds a
``contracts/`` folder for this purpose; any path works). The ``contract.file`` path is
resolved relative to the project root.

.. code-block:: yaml
   :caption: contracts/customer.odcs.yaml

   version: "1.0.0"
   apiVersion: v3.0.2
   kind: DataContract
   id: 7b3d1e10-0001-4a00-9000-000000000001
   status: active
   name: customer-contract
   schema:
     - name: customer
       physicalType: table
       description: One row per customer
       quality:                          # dataset-level rule → a custom_sql test
         - name: customer_row_count
           type: library
           metric: rowCount
           mustBeGreaterThan: 0
           severity: warning
       properties:
         - name: customer_id
           logicalType: integer
           physicalType: BIGINT
           required: true
           criticalDataElement: true
           primaryKey: true
           primaryKeyPosition: 1
           quality:                      # column-level rule → a uniqueness test
             - name: customer_id_unique
               type: library
               metric: duplicateValues
               mustBe: 0
               severity: error
         - name: full_name
           logicalType: string
           required: true
           logicalTypeOptions:
             minLength: 1
         - name: lifetime_value
           logicalType: number
           physicalType: DECIMAL(18,2)
           logicalTypeOptions:
             minimum: 0

The top-level ``version``, ``apiVersion``, ``kind``, ``id``, and ``status`` fields are
required by the ODCS specification. LHP validates every contract against the bundled
ODCS JSON Schema and raises ``LHP-CFG-062`` if a file is not a valid contract.

Reference it from an action
---------------------------

Attach a ``contract`` block to an action. The only required key is ``file``; everything
else is optional and tunes resolution.

.. list-table:: ``contract`` fields
   :header-rows: 1
   :widths: 22 18 60

   * - Field
     - Default
     - Meaning
   * - ``file``
     - *(required)*
     - Path to the ODCS contract, relative to the project root.
   * - ``type``
     - ``odcs``
     - Contract format. Only ``odcs`` is supported.
   * - ``object``
     - sole object
     - Which schema object (table) to resolve. Optional when the contract has a single
       object; **required** when it has more than one.
   * - ``schema_hints``
     - ``false``
     - ``cloudfiles`` load only. When ``true``, emit ``cloudFiles.schemaHints``
       **instead of** an enforced ``source.schema`` — Auto Loader infers the schema
       and the hints pin the contract's declared types. Never both.
   * - ``expectations_action``
     - per-property
     - ``data_quality`` only. One of ``warn`` / ``drop`` / ``fail``, applied to every
       expectation. When omitted, each property's action is derived from its
       ``criticalDataElement`` flag.

In a ``cloudfiles`` load
~~~~~~~~~~~~~~~~~~~~~~~~~

Attaching a contract injects ``source.schema`` (the full read schema) so Auto Loader
accepts exactly the declared columns:

.. code-block:: yaml
   :caption: pipelines/bronze/customer.yaml

   actions:
     - name: load_customer
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: "${landing_volume}/customer/*.csv"
         format: csv
       target: v_customer_raw
       contract:
         file: contracts/customer.odcs.yaml

Set ``schema_hints: true`` to emit ``cloudFiles.schemaHints`` **instead** — Auto Loader
infers the schema while the hints pin the contract's declared types. No enforced
``source.schema`` is emitted (the two are mutually exclusive):

.. code-block:: yaml

       contract:
         file: contracts/customer.odcs.yaml
         schema_hints: true

**Where ``physicalName`` is used.** The cloudfiles **read** schema (``source.schema`` and
``cloudFiles.schemaHints``) reads the raw source files, so it uses each column's
``physicalName`` where present (falling back to ``name``). The ``schema`` transform then
renames those physical names to the contract ``name`` (above). The write ``table_schema``
defines the target table and **always** uses the contract ``name``. So a typical flow is:
cloudfiles load (physical names) → ``schema`` transform (rename + cast to contract names) →
write (contract names).

In a write action
~~~~~~~~~~~~~~~~~~

A contract on a ``streaming_table`` or ``materialized_view`` write injects
``write_target.table_schema``, so the contract defines the table's columns and types:

.. code-block:: yaml
   :caption: pipelines/silver/customer.yaml

   actions:
     - name: write_customer
       type: write
       source: v_customer_clean
       write_target:
         type: streaming_table
         database: "${silver_schema}"
         table: customer
       contract:
         file: contracts/customer.odcs.yaml

In a ``schema`` transform
~~~~~~~~~~~~~~~~~~~~~~~~~~~

A contract on a ``schema`` transform injects ``schema_inline`` as a ``type_casting`` map
(every contract column → its type). When a property declares an ODCS ``physicalName`` that
differs from its ``name``, a ``column_mapping`` entry (``physicalName`` → ``name``) is also
added so the transform renames that source column to the contract's name:

.. code-block:: yaml

     - name: cast_customer
       type: transform
       transform_type: schema
       source: v_customer_raw
       target: v_customer_typed
       contract:
         file: contracts/customer.odcs.yaml

For example, a property ``{name: customer_id, physicalName: cust id, physicalType: BIGINT}``
generates ``column_mapping: {"cust id": customer_id}`` + ``type_casting: {customer_id: BIGINT}``
→ ``df.withColumnRenamed("cust id", "customer_id")`` then a cast.

In a ``data_quality`` transform
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A contract on a ``data_quality`` transform injects inline ``expectations`` derived from
each property's ``logicalTypeOptions``:

.. code-block:: yaml
   :caption: pipelines/silver/customer.yaml

   actions:
     - name: customer_dq
       type: transform
       transform_type: data_quality
       source: v_customer_clean
       target: v_customer_dq
       readMode: stream
       contract:
         file: contracts/customer.odcs.yaml
         expectations_action: fail   # optional; else per-property criticalDataElement

**What becomes an expectation.** Each property contributes row-level checks derived from
its ``logicalTypeOptions``:

- string ``minLength`` / ``maxLength`` / ``pattern`` → ``length(<col>) >= n`` /
  ``length(<col>) <= n`` / ``<col> RLIKE '...'``
- integer/number ``minimum`` / ``maximum`` / ``exclusiveMinimum`` / ``exclusiveMaximum``
  / ``multipleOf`` → the matching comparison / ``<col> % n = 0``
- date/timestamp/time ``minimum`` / ``maximum`` (and exclusive variants) → quoted-literal
  comparisons
- array ``minItems`` / ``maxItems`` / ``uniqueItems`` → ``size(<col>) ...`` /
  ``size(<col>) = size(array_distinct(<col>))``
- object ``required: [field]`` → ``<col>.<field> IS NOT NULL``

**Severity.** With ``expectations_action`` set, every expectation uses that action.
Otherwise it comes from ``criticalDataElement``: a property marked
``criticalDataElement: true`` produces ``fail`` expectations (a violating row fails the
pipeline); otherwise ``warn`` (violations are logged, rows pass).

In a ``test`` action (contract-driven tests)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A contract on a ``type: test`` action with **no** ``test_type`` is **expanded into many
test actions** — one per ODCS ``quality`` rule (both dataset-level ``schema[].quality`` and
property-level ``schema[].properties[].quality``) on the selected entity. Set ``source`` to
the single table the rules run against; the resolver fills in each ``test_type``:

.. code-block:: yaml
   :caption: pipelines/tests/orders_tests.yaml

   actions:
     - name: orders_contract_tests
       type: test
       source: "${catalog}.${bronze_schema}.orders"
       contract:
         file: contracts/orders.odcs.yaml
       # NO test_type — supplying one alongside a contract raises LHP-CFG-064

Each ``quality`` rule maps to a single-table test:

.. list-table::
   :header-rows: 1
   :widths: 48 24 28

   * - ODCS ``quality`` rule
     - LHP ``test_type``
     - Notes
   * - ``library`` ``duplicateValues`` with ``mustBe: 0``
     - ``uniqueness``
     - on the bound column(s)
   * - ``library`` ``nullValues`` / ``missingValues`` with ``mustBe: 0``
     - ``completeness``
     - column must be ``NOT NULL``
   * - ``library`` ``rowCount`` (any operator)
     - ``custom_sql``
     - ``COUNT(*)`` vs the operator
   * - ``library`` ``nullValues`` / ``missingValues`` / ``duplicateValues`` (other operators)
     - ``custom_sql``
     - count + threshold from the operator
   * - ``library`` ``invalidValues`` with ``arguments.validValues``
     - ``custom_sql``
     - ``... NOT IN (…)`` count
   * - ``sql`` rule (``query`` + operator)
     - ``custom_sql``
     - ``${table}`` / ``${column}`` substituted
   * - ``custom`` / ``text`` rules
     - *(skipped, logged)*
     - not translatable here

The rule ``severity`` sets ``on_violation`` (``error`` → ``fail``; ``warning`` / ``info`` →
``warn``; absent → ``fail``). The rule ``name`` becomes the test's ``test_id`` and part of its
action name. **Scope (first cut):** only the ``quality`` array drives tests, against a single
``source`` table; Contract-driven tests, like all test actions, only materialise with
``lhp generate --include-tests``.

Multi-table contracts
----------------------

An ODCS contract's top-level ``schema`` is a list of objects, so one file can describe
several tables. When a contract has more than one object, set ``object`` to pick the
one an action resolves against:

.. code-block:: yaml

   contract:
     file: contracts/sales.odcs.yaml
     object: orders

Omitting ``object`` against a multi-object contract raises ``LHP-CFG-064``; so does
naming an object the contract does not define.

How resolution works
---------------------

On every ``lhp validate`` / ``lhp generate`` run, after substitution and before code
generation, LHP rewrites each contract-bearing action:

1. Validate the ``contract`` options and parse the referenced contract.
2. Select the entity object (``object`` or the sole object).
3. Inject the resolved artifact inline for the action's type (see the table above).
4. Strip the ``contract`` field so generators only ever see resolved config.

Resolution **never overwrites** config you set by hand. If an action already declares the
field a contract would inject — ``source.schema``, ``cloudFiles.schemaHints``,
``write_target.table_schema``, ``schema_inline``, or ``expectations`` /
``expectations_file`` — LHP raises ``LHP-CFG-064`` rather than silently replacing it.

.. seealso::

   - :doc:`ingest_with_autoloader` — full reference for ``cloudFiles.schemaHints``
     and ``source.schema`` on a ``cloudfiles`` load action.
   - :doc:`actions/write_actions` — full reference for ``table_schema`` on a write
     target.
   - :doc:`actions/transform_actions` — the ``schema`` and ``data_quality`` transforms.
   - `Open Data Contract Standard <https://bitol.io/open-data-contract-standard/>`_ —
     the ODCS specification.
