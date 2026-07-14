===========================================
Incrementally ingest files with Auto Loader
===========================================

.. meta::
   :description: Continuously ingest new files from cloud storage or a Unity Catalog volume with a declarative Lakehouse Plumber cloudfiles load — schema inference and evolution, rescued data, and file selection — without spark.readStream.format("cloudFiles") boilerplate by hand.

Files keep landing. A partner drops a new batch of order JSON into a volume
every few minutes, and your job is to pick up each new file exactly once, cope
with the schema drifting over time, and never let one malformed record stall the
stream. That is what Databricks Auto Loader does — and it is the primary way to
get files into the lakehouse.

You could hand-write the reader — ``spark.readStream.format("cloudFiles")`` with
a stack of ``.option(...)`` calls for the schema location, inference, evolution,
and rescued-data column, then a ``create_streaming_table`` and an ``append_flow``
to land the rows. Or you declare a ``type: cloudfiles`` load and let Lakehouse
Plumber write that Lakeflow for you. That is the idea on every page: **declare
your ETL, don't hand-write it.**

Let's ingest a landing folder of order JSON files into a bronze streaming table —
incrementally, with the schema inferred and allowed to evolve — without touching
PySpark.

Before you start
================

You need Lakehouse Plumber installed and a project to work in. If you followed
the Get Started course you already have one; otherwise scaffold an empty project
first. This guide assumes order files are already arriving under a landing path —
a Unity Catalog volume, or an S3, ADLS, or GCS location. Auto Loader reads files
that land there; it does not create them.

Declare the Auto Loader load
============================

A pipeline in Lakehouse Plumber is a **flowgroup**: a short YAML file describing
a sequence of **actions**. This one has two — a ``load`` that streams the landing
folder into a view, and a ``write`` that appends the view into a streaming table.

Create ``pipelines/orders_ingest.yaml``:

.. literalinclude:: ../../_fixtures/guide_ingest_autoloader/pipelines/orders_ingest.yaml
   :language: yaml
   :caption: pipelines/orders_ingest.yaml

The ``load`` action names the source with ``source.type: cloudfiles``, points
``path`` at a glob of the landing folder, and sets ``format: json``. Auto Loader
also reads ``csv``, ``parquet``, ``avro``, ``orc``, ``text``, and ``binaryFile``;
``json`` is the default. You write ``format`` once — Lakehouse Plumber copies it
into the mandatory ``cloudFiles.format`` reader option at generation time, so you
never repeat yourself.

Auto Loader is a streaming source, so there is no batch alternative: LHP defaults
``readMode`` to ``stream`` and rejects ``batch``. That is why the action has no
``readMode`` line — the default is the only valid value.

The four ``cloudFiles.*`` options under ``options`` are the schema story:

- ``cloudFiles.schemaLocation`` — the durable location where Auto Loader persists
  the schema it infers and tracks how it changes across runs.
- ``cloudFiles.inferColumnTypes: true`` — infer real types (numbers, timestamps)
  from the JSON instead of reading every field as a string.
- ``cloudFiles.schemaEvolutionMode: addNewColumns`` — when a field appears in a
  later file that wasn't in the inferred schema, add it to the table rather than
  failing.
- ``cloudFiles.rescuedDataColumn: _rescued_data`` — capture any value that
  doesn't fit its inferred type in a sidecar JSON column, so a malformed record
  neither fails the stream nor silently disappears.

The ``${...}`` tokens are placeholders you resolve per environment, so the same
flowgroup runs unchanged in dev, staging, and prod.

Point the tokens at your environment
====================================

Those tokens resolve from a per-environment substitutions file. Create
``substitutions/dev.yaml``:

.. literalinclude:: ../../_fixtures/guide_ingest_autoloader/substitutions/dev.yaml
   :language: yaml
   :caption: substitutions/dev.yaml

This is the only place environment-specific values live. To ingest from a
production landing zone later, you write a ``prod.yaml`` with the production
catalog, landing path, and schema location — the flowgroup itself never changes.

Generate the pipeline
=====================

Now compile the YAML into Lakeflow code. Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_ingest  0 files
   ✓ validate (0.32s)
   1 validated · 0.3s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_ingest  1 file
   ✓ generate (0.36s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

``validate`` resolves every token and checks the actions before you commit to
generating; ``generate`` writes the Python.

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/bronze_ingest/orders_ingest.py``. This is the *entire*
output — nothing is hidden behind a runtime:

.. literalinclude:: ../../_fixtures/guide_ingest_autoloader/generated/dev/bronze_ingest/orders_ingest.py
   :language: python
   :caption: generated/dev/bronze_ingest/orders_ingest.py

It's ordinary Lakeflow. The source view is a ``@dp.temporary_view`` wrapping
``spark.readStream.format("cloudFiles")``, with each ``.option(...)`` you
declared emitted in order — the boolean ``inferColumnTypes`` as a bare ``True``,
the rest as strings — and ``cloudFiles.format`` injected from ``source.format``.
The target is a ``dp.create_streaming_table``, and a ``@dp.append_flow`` streams
the view into ``dev_catalog.bronze.orders_raw``. You described a landing folder
and a target table; LHP wrote the reader and the two pieces of plumbing that
connect them.

Because Auto Loader records which files it has already ingested in the checkpoint
beside ``schemaLocation``, each run of the pipeline processes only files that are
new since the last run. That is the incremental behaviour you came for — you get
it from the ``cloudfiles`` type, not from any code you wrote.

Choose a schema strategy
========================

The load above lets Auto Loader **infer and evolve** the schema — the right
default when JSON files drift over time and you'd rather absorb new fields than
break. Two other strategies trade discovery for control, and you pick per load:

- **Guide the inference with** ``cloudFiles.schemaHints``. Pin the types you care
  about — ``"order_id BIGINT, amount DOUBLE"`` — while Auto Loader still discovers
  the rest. The hint accepts an inline DDL string, a YAML schema file (where a
  column marked ``nullable: false`` becomes ``NOT NULL``), or a ``.ddl`` / ``.sql``
  file; LHP picks the form by file extension.
- **Enforce a fixed schema with** ``source.schema``. Point it at a schema file and
  Auto Loader accepts exactly those columns and turns inference off — so schema
  evolution no longer applies.

``source.schema`` and ``cloudFiles.schemaHints`` are two ways to say different
things, and you set at most one. The load action reference documents the schema
file formats and the exact rendering for each.

.. note::

   Declaring more than one schema source on a single load — ``source.schema``,
   ``cloudFiles.schemaHints``, or the legacy ``schema_file`` — is a
   configuration conflict, and ``validate`` rejects it before generation. Pick
   one strategy per load.

Control which files enter the stream
====================================

``source.path`` is passed straight into the generated ``.load(...)`` with no
rewriting, so any glob Spark and Auto Loader support works as-is. Narrow it to
scope the directories Auto Loader lists: brace alternatives like
``{sales,marketing,events}`` scan only those folders, character classes like
``[0-9]`` match ranges, and ``[^x]`` negates a set. Use ``[^x]``, not the shell's
``[!x]``, and avoid the ``**`` globstar — it is undocumented for Auto Loader. On
DBR 12.2+, set ``cloudFiles.useStrictGlobber: "true"`` for predictable,
Spark-standard globbing.

To filter on the file **name** rather than the path, set ``pathGlobFilter:
"*.parquet"`` under ``options`` — it matches each file's basename after listing.
For a rule no glob can express, filter downstream in a SQL transform on the
``_metadata.file_path`` column that Auto Loader exposes at read time. The load
action reference covers the full glob and path-filter set.

Fan several sources into one table
==================================

When the same order schema arrives from several landing zones — one bucket per
region, say — consolidate them into one bronze table without merging the streams.
Declare one ``load`` + ``write`` pair per source, and point every ``write`` at the
same ``streaming_table``. The first write keeps the default ``create_table: true``
and creates the table; each later write sets ``create_table: false`` and appends
to it. LHP emits a single ``dp.create_streaming_table`` and one ``@dp.append_flow``
per source, each with its own checkpoint — so one slow or broken source doesn't
stall the others. The write action reference documents the streaming-table and
append-flow rules in full.

What you just did
=================

Twenty-three lines of YAML. Fifty-six lines of Lakeflow Python. **Zero lines of
PySpark from you** — you never wrote ``readStream.format("cloudFiles")``, the
five-option reader chain, ``create_streaming_table``, or ``append_flow``. And the
output is code you own: version it, diff it, and open it in the Databricks editor
like anything else.

The payoff compounds. Adding schema hints, narrowing the glob, or fanning in a
second landing zone is a few more lines of YAML — not another fifty-six lines of
Python each time.

What's next
===========

- **Handle the rescued rows.** Pair ``cloudFiles.rescuedDataColumn`` with a
  data-quality gate to drop or quarantine records that landed malformed, so only
  clean rows reach downstream tables. See the data-quality guide.
- **Tune throughput and listing.** ``cloudFiles.maxFilesPerTrigger`` and
  ``cloudFiles.maxBytesPerTrigger`` cap each micro-batch;
  ``cloudFiles.useIncrementalListing`` and ``cloudFiles.useNotifications`` change
  how Auto Loader discovers new files on large directories. The load action
  reference lists them with their constraints.
- **Reprocess files.** ``cloudFiles.includeExistingFiles`` (first run only),
  ``cloudFiles.allowOverwrites``, and ``cloudFiles.cleanSource`` control whether
  already-seen files are re-ingested or archived.
- **Add file-lineage columns.** ``operational_metadata`` attaches audit columns
  such as ``_source_file_path``, ``_source_file_size``, and
  ``_source_file_modification_time`` to every row.
- **See every option.** The full ``cloudFiles.*`` set, with defaults and
  constraints, is in the load action reference.
