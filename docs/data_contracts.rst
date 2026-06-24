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
     - ``source.schema`` (read schema, inline DDL); also ``cloudFiles.schemaHints``
       when ``schema_hints: true``
   * - **write** (``streaming_table`` / ``materialized_view``)
     - ``write_target.table_schema`` (inline DDL)
   * - ``schema`` **transform**
     - ``schema_inline`` (cast-only ``<col>: <type>`` entries)
   * - ``data_quality`` **transform**
     - inline ``expectations`` (list form)

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
       properties:
         - name: customer_id
           logicalType: integer
           physicalType: BIGINT
           required: true
           criticalDataElement: true
           primaryKey: true
           primaryKeyPosition: 1
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
   * - ``entity_name``
     - sole object
     - Which schema object (table) to resolve. Optional when the contract has a single
       object; **required** when it has more than one.
   * - ``schema_hints``
     - ``false``
     - ``cloudfiles`` load only. When ``true``, also emit ``cloudFiles.schemaHints``
       (in addition to the read schema).
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

Set ``schema_hints: true`` to *also* emit ``cloudFiles.schemaHints`` — pinning the
declared types while Auto Loader infers any remaining columns:

.. code-block:: yaml

       contract:
         file: contracts/customer.odcs.yaml
         schema_hints: true

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

A contract on a ``schema`` transform injects ``schema_inline`` as **cast-only** entries
(one ``<col>: <type>`` per contract column — types only, no renames). The source view is
assumed to already use the contract's column names:

.. code-block:: yaml

     - name: cast_customer
       type: transform
       transform_type: schema
       source: v_customer_raw
       target: v_customer_typed
       contract:
         file: contracts/customer.odcs.yaml

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

Multi-table contracts
----------------------

An ODCS contract's top-level ``schema`` is a list of objects, so one file can describe
several tables. When a contract has more than one object, set ``entity_name`` to pick the
one an action resolves against:

.. code-block:: yaml

   contract:
     file: contracts/sales.odcs.yaml
     entity_name: orders

Omitting ``entity_name`` against a multi-object contract raises ``LHP-CFG-064``; so does
naming an object the contract does not define.

How resolution works
---------------------

On every ``lhp validate`` / ``lhp generate`` run, after substitution and before code
generation, LHP rewrites each contract-bearing action:

1. Validate the ``contract`` options and parse the referenced contract.
2. Select the entity object (``entity_name`` or the sole object).
3. Inject the resolved artifact inline for the action's type (see the table above).
4. Strip the ``contract`` field so generators only ever see resolved config.

Resolution **never overwrites** config you set by hand. If an action already declares the
field a contract would inject — ``source.schema``, ``cloudFiles.schemaHints``,
``write_target.table_schema``, ``schema_inline``, or ``expectations`` /
``expectations_file`` — LHP raises ``LHP-CFG-064`` rather than silently replacing it.

Errors
------

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - Code
     - Raised when
   * - ``LHP-CFG-062``
     - The referenced file is not a valid ODCS contract.
   * - ``LHP-CFG-063``
     - A property type cannot be mapped to a Spark type.
   * - ``LHP-CFG-064``
     - Contract resolution fails: missing ``file``; ``type`` other than ``odcs``;
       ``schema_hints`` off a cloudfiles load; ``expectations_action`` off a
       data_quality transform; a ``contract`` on an unsupported action type; a missing
       contract file; an unknown or ambiguous ``entity_name``; or a conflict with a
       field already set on the action.

.. seealso::

   - :doc:`ingest_with_autoloader` — full reference for ``cloudFiles.schemaHints``
     and ``source.schema`` on a ``cloudfiles`` load action.
   - :doc:`actions/write_actions` — full reference for ``table_schema`` on a write
     target.
   - :doc:`actions/transform_actions` — the ``schema`` and ``data_quality`` transforms.
   - `Open Data Contract Standard <https://bitol.io/open-data-contract-standard/>`_ —
     the ODCS specification.
