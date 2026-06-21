Data Contracts (ODCS)
=====================

.. meta::
   :description: How to use Open Data Contract Standard (ODCS) YAML files in a contracts/ folder to drive Lakehouse Plumber — starting with table schemas for Auto Loader schema hints and full schema enforcement.

`Open Data Contract Standard <https://bitol.io/open-data-contract-standard/>`_ (ODCS)
is a YAML format for declaring a dataset's schema and data-quality rules. Place ODCS
contracts into a ``contracts/`` folder and Lakehouse Plumber (LHP) translates them
into the artifacts it generates pipelines from. This feature currently covers
**schemas** — LHP translates each contract's schema into an LHP schema file you point
a ``cloudfiles`` load action at, to guide Auto Loader's schema inference or to enforce
the full schema.

.. versionadded:: 0.9.0

Add a contract to ``contracts/``
--------------------------------

Place an ODCS contract YAML in the project's ``contracts/`` directory. ``lhp init``
scaffolds this folder; create it yourself in an existing project. The feature is
opt-in by folder presence — with no ``contracts/`` directory, nothing changes.

.. code-block:: yaml
   :caption: contracts/customer.contract.yaml

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
           primaryKey: true
           primaryKeyPosition: 1
           description: Surrogate key for the customer
         - name: full_name
           logicalType: string
           required: true
         - name: lifetime_value
           logicalType: number
           physicalType: DECIMAL(18,2)
         - name: is_active
           logicalType: boolean
           required: true

The top-level ``version``, ``apiVersion``, ``kind``, ``id``, and ``status`` fields are
required by the ODCS specification. LHP validates every contract against the bundled
ODCS JSON Schema and raises ``LHP-CFG-062`` if a file is not a valid contract.

Generate the schema files
-------------------------

LHP translates contracts at the start of every ``lhp validate`` and ``lhp generate``
run, before discovery and code generation. No separate command is needed:

.. code-block:: bash

   lhp generate --env dev

Pass ``--no-contracts`` to ``lhp generate`` or ``lhp validate`` to skip translation
for that run.

For each schema object in each contract, LHP writes one LHP schema file to
``contracts/lhp/schemas/``, named ``<contract-file-stem>.<object>_schema.yaml``:

.. code-block:: text

   contracts/
   ├── customer.contract.yaml                              # your ODCS source
   └── lhp/
       └── schemas/
           └── customer.contract.customer_schema.yaml      # generated

The output sits beside the source contracts — mirroring how generated Databricks
Asset Bundle resources live under ``resources/lhp/`` — and is version-controlled, not
gitignored. The translated file is the standard LHP schema format:

.. code-block:: yaml
   :caption: contracts/lhp/schemas/customer.contract.customer_schema.yaml

   name: customer
   version: 1.0.0
   description: One row per customer
   columns:
   - name: customer_id
     type: BIGINT
     nullable: false
     comment: Surrogate key for the customer
   - name: full_name
     type: STRING
     nullable: false
   - name: lifetime_value
     type: DECIMAL(18,2)
     nullable: true
   - name: is_active
     type: BOOLEAN
     nullable: false
   primary_key:
   - customer_id

Translation is deterministic: re-running on an unchanged contract rewrites a
byte-identical file. Contracts coexist with hand-authored ``schemas/`` files — the
``contracts/`` folder is an additional source, not a replacement.

Use the generated schema in a load action
-----------------------------------------

A generated schema file is an ordinary LHP schema file, so a ``cloudfiles`` load
action consumes it in either of the two ways covered in :doc:`ingest_with_autoloader`.

Point ``cloudFiles.schemaHints`` at the file to pin types while Auto Loader infers the
rest. The ``nullable: false`` columns emit ``NOT NULL`` in the generated DDL:

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
         options:
           cloudFiles.format: csv
           cloudFiles.schemaHints: "contracts/lhp/schemas/customer.contract.customer_schema.yaml"
       target: v_customer_raw

Set ``source.schema`` instead to enforce the contract as the full schema — Auto Loader
accepts exactly the declared columns and rejects everything else:

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
         schema: "contracts/lhp/schemas/customer.contract.customer_schema.yaml"
         options:
           cloudFiles.format: csv
       target: v_customer_raw

Translate a multi-table contract
--------------------------------

An ODCS contract's top-level ``schema`` is a list of objects, so one contract file can
describe several tables. LHP writes one schema file per object, each prefixed with the
contract filename so names never collide across contracts:

.. code-block:: text

   contracts/sales.yaml          # schema: [orders, order_line_items]
   →  contracts/lhp/schemas/sales.orders_schema.yaml
   →  contracts/lhp/schemas/sales.order_line_items_schema.yaml

Reference each generated file from the load action for its table.

.. seealso::

   - :doc:`ingest_with_autoloader` — full reference for ``cloudFiles.schemaHints``
     and ``source.schema`` on a ``cloudfiles`` load action.
   - `Open Data Contract Standard <https://bitol.io/open-data-contract-standard/>`_ —
     the ODCS specification.
