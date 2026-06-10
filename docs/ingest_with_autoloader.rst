Ingest with Auto Loader
=======================

.. meta::
   :description: How to stream files from cloud storage with Databricks Auto Loader using an LHP cloudfiles load action — minimum viable config, schema hints, schema enforcement, and recycled-data handling.

This how-to wires Databricks Auto Loader (``cloudfiles``) into a Lakehouse Plumber
(LHP) load action so a :term:`FlowGroup` streams files from S3, ADLS, Google Cloud Storage,
or a Unity Catalog volume into a temporary view. The page focuses on the LHP YAML;
for option semantics, follow the link at the bottom to the Databricks Auto Loader
documentation.

Minimum viable cloudfiles load
------------------------------

The smallest useful ``cloudfiles`` load action streams CSVs from a landing volume
into a temporary view named ``v_customer_cloudfiles``.

.. code-block:: yaml
   :caption: pipelines/bronze/customer.yaml

   actions:
     - name: load_csv_file_from_cloudfiles
       type: load
       readMode: stream
       operational_metadata: ["_source_file_path", "_source_file_size", "_source_file_modification_time"]
       source:
         type: cloudfiles
         path: "${landing_volume}/{{ landing_folder }}/*.csv"
         format: csv
         options:
           cloudFiles.format: csv
           header: True
           delimiter: "|"
           cloudFiles.maxFilesPerTrigger: 11
           cloudFiles.inferColumnTypes: False
           cloudFiles.schemaEvolutionMode: "addNewColumns"
           cloudFiles.rescuedDataColumn: "_rescued_data"
           cloudFiles.schemaHints: "schemas/{{ table_name }}_schema.yaml"
       target: v_customer_cloudfiles
       description: "Load customer CSV files from landing volume"

Required keys:

- ``readMode: stream`` — ``cloudfiles`` only supports streaming. LHP raises a
  validation error if you set ``batch``.
- ``source.type: cloudfiles`` — selects the Auto Loader generator.
- ``source.path`` — a file glob. ``${token}`` substitutions and ``{{ param }}``
  template parameters both work.
- ``source.format`` — ``csv``, ``json``, ``parquet``, ``avro``, ``orc``, ``text``,
  or ``binaryFile``. Default: ``json``.
- ``source.options.cloudFiles.format`` — mandatory Auto Loader option. If you omit
  it, LHP copies ``source.format`` into it during generation.

Optional but commonly set:

- ``operational_metadata`` — a list of audit column names to add to the output.
  ``_source_file_path``, ``_source_file_size``, and ``_source_file_modification_time``
  are the conventional file-lineage triple. See :doc:`operational_metadata` for the
  full column catalog.
- Format reader options (``header``, ``delimiter``, ``multiline``, etc.) live
  alongside ``cloudFiles.*`` options in the same ``options`` block.

Guide schema inference with schemaHints
---------------------------------------

Use ``cloudFiles.schemaHints`` when you want Auto Loader to infer most columns but
need to pin specific types (for example, force a string ID column to ``BIGINT``).
LHP auto-detects three formats:

**Inline DDL string** — best for short schemas:

.. code-block:: yaml

   cloudFiles.schemaHints: "customer_id BIGINT, name STRING, email STRING"

**External YAML file** — recommended when you also want column metadata or
``NOT NULL`` constraints:

.. code-block:: yaml

   cloudFiles.schemaHints: "schemas/customer_schema.yaml"

YAML schemas honour the ``nullable: false`` field per column and emit ``NOT NULL``
in the generated DDL.

**External DDL or SQL file** — for pre-written ``CREATE TABLE``-style DDL:

.. code-block:: yaml

   cloudFiles.schemaHints: "schemas/customer_schema.ddl"
   # or .sql

LHP picks the format by file extension. Anything that is not ``.yaml``, ``.yml``,
``.json``, ``.ddl``, or ``.sql`` is treated as an inline string.

Enforce a full schema instead of inferring
------------------------------------------

Set ``source.schema`` instead of ``cloudFiles.schemaHints`` when you want Auto
Loader to accept exactly the columns you declare and reject everything else.

.. code-block:: yaml

   actions:
     - name: load_customer_with_schema
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: "/data/customers/*.csv"
         format: csv
         schema: schemas/customer_schema.yaml
         options:
           cloudFiles.format: csv
       target: v_customer_raw
       description: "Load customer CSV with explicit schema enforcement"

.. warning::

   ``source.schema`` and ``cloudFiles.schemaHints`` are mutually exclusive. LHP
   raises a configuration-conflict error if you set both, and also blocks the
   legacy ``schema_file`` field when either is present. With ``source.schema``
   set, leave ``cloudFiles.schemaEvolutionMode`` unset — inference is disabled,
   so evolution does not apply.

Filter which files Auto Loader picks up
---------------------------------------

Two LHP-shaped knobs control which files enter the stream:

- **Prefix filter** — narrow the ``source.path`` glob (for example,
  ``s3://bucket/data/{sales,marketing}/*.parquet``). Auto Loader only lists the
  directories that match.
- **Suffix or name filter** — set ``pathGlobFilter`` inside ``source.options`` to
  match basenames only (for example, ``"*.parquet"``).

For multi-source fan-in, brace-expansion globs, and a comparison of approaches,
see :doc:`pipeline_patterns`.

Reprocess files that already streamed
-------------------------------------

Auto Loader tracks ingested files by checkpoint; replays do not re-emit rows by
default. Three options change that behaviour without re-creating the pipeline:

- ``cloudFiles.includeExistingFiles: true`` — on a fresh pipeline, ingest files
  already present at the source path. Has no effect once the stream has
  initialised; flip it at first run.
- ``cloudFiles.allowOverwrites: true`` — re-emit a file when its content changes
  at the same path (overwrite-style sources).
- ``cloudFiles.cleanSource`` plus ``cloudFiles.cleanSource.retentionDuration`` and
  ``cloudFiles.cleanSource.moveDestination`` — let Auto Loader archive or delete
  ingested files so the next run sees only new arrivals.

To force a full re-ingest from scratch, drop the streaming target table and rerun;
LHP regenerates the pipeline Python and Auto Loader rebuilds its checkpoint.

Tune throughput and listing
---------------------------

These pass straight through ``source.options``:

- ``cloudFiles.maxFilesPerTrigger`` — cap files per micro-batch (integer).
- ``cloudFiles.maxBytesPerTrigger`` — cap bytes per micro-batch (string with unit,
  for example ``"10g"``).
- ``cloudFiles.useIncrementalListing: true`` — incremental directory listing on
  S3, ADLS Gen2, and GCS. Defaults to ``"auto"``.
- ``cloudFiles.useNotifications: true`` — switch from directory listing to
  cloud-event notifications. Requires the corresponding cloud-side setup
  (SNS/SQS, Event Grid, or Pub/Sub).

Recover malformed rows
----------------------

Set ``cloudFiles.rescuedDataColumn`` (default behaviour: disabled) to capture
columns that fail type coercion as a JSON struct in a sidecar column. The example
at the top of this page uses the conventional column name ``_rescued_data``.
Combine with a Data Quality Expectations (DQE) transform action and the
quarantine pattern to redirect rescued or failed rows to a dead-letter table;
see :doc:`quarantine_records` for the DLT-friendly recipe.

See also
--------

* :doc:`architecture` — how LHP turns this YAML into Python and where the
  generator and Jinja2 template live in ``src/lhp/``.
* :doc:`actions/load_actions` — full reference for every load sub-type
  (``cloudfiles``, ``delta``, ``sql``, ``jdbc``, ``python``, ``kafka``,
  ``custom_datasource``) and every key on the action model.
* :doc:`pipeline_patterns` — multi-source fan-in, brace-expansion globs, and
  ``pathGlobFilter`` for finer file selection.
* :doc:`operational_metadata` — audit column catalog and FlowGroup-level
  inheritance rules.
* `Databricks Auto Loader documentation
  <https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/>`_
  for the underlying ``cloudFiles.*`` option semantics.
