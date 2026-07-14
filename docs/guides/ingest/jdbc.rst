================================
Ingest from a database over JDBC
================================

.. meta::
   :description: Pull a table from an operational database into the lakehouse with a declarative Lakehouse Plumber JDBC load — connection, secret-supplied credentials, and the batch read generated for you, with no spark.read.format("jdbc") by hand.

Not every source is a file or a Delta table. Often the data you need still
lives in an operational database — a customer master in PostgreSQL, a product
catalog in MySQL — and your job is to pull it into the lakehouse over JDBC.

You could hand-write it: ``spark.read.format("jdbc")``, then a chain of
``.option(...)`` calls for the URL, driver, user, password, and table; a
``dbutils.secrets.get(...)`` around each credential so the password never lands
in source; a ``.load()``; and then a second block to declare the target table.
Or you declare a ``type: jdbc`` load, name the connection and the table, and let
Lakehouse Plumber write that Lakeflow for you. That is the idea on every page:
**declare your ETL, don't hand-write it.**

Let's pull a customer master table out of an operational PostgreSQL database and
land it as a bronze table — over JDBC, with the password supplied by a secret,
without touching PySpark.

Before you start
================

You need Lakehouse Plumber installed and a project to work in. A JDBC read also
has two runtime prerequisites that live outside the flowgroup: the database
driver (here ``org.postgresql.Driver``) must be available on the pipeline's
compute, and the database credentials must already be stored in a Databricks
secret scope. The flowgroup references the secret; it never holds the password.

Declare the JDBC load
=====================

A pipeline in Lakehouse Plumber is a **flowgroup**: a short YAML file describing
a sequence of **actions**. This one has two — a ``load`` that reads the external
table into a view, and a ``write`` that lands the view as a bronze table.

Create ``pipelines/customers_jdbc.yaml``:

.. literalinclude:: ../../_fixtures/guide_ingest_jdbc/pipelines/customers_jdbc.yaml
   :language: yaml
   :caption: pipelines/customers_jdbc.yaml

The ``load`` action's ``source`` names the connection: the JDBC ``url``, the
``driver`` class, and the ``table`` to read. ``readMode: batch`` decides how the
source is read — a JDBC source has no stream to tail, so a JDBC load is a batch
read, and LHP generates ``spark.read`` rather than ``spark.readStream``. Give
either a ``table`` (read the whole table) or a ``query`` (push a ``SELECT`` down
to the database), but not both.

Credentials are the part you never inline. ``user`` and ``password`` are written
as ``${secret:sales_db/username}`` and ``${secret:sales_db/password}`` — secret
references, not literal values. The ``write`` action lands the pulled rows as a
``materialized_view`` in the bronze layer: a JDBC pull has no incremental stream
to append, so a materialized view — recomputed from the source on each refresh —
is the natural landing for a full-table read.

Supply the tokens and the secret scope
=======================================

The ``${...}`` tokens and the secret scope both resolve from a per-environment
substitutions file. Create ``substitutions/dev.yaml``:

.. literalinclude:: ../../_fixtures/guide_ingest_jdbc/substitutions/dev.yaml
   :language: yaml
   :caption: substitutions/dev.yaml

Two things resolve here. The ``catalog`` and ``bronze_schema`` tokens fill in the
target table's name. And the ``secrets`` block maps the *logical* scope name you
wrote in the flowgroup — ``sales_db`` — onto the *actual* workspace secret scope
``dev_sales_db_secrets``. So ``${secret:sales_db/username}`` becomes a lookup
against ``dev_sales_db_secrets``. Point ``prod.yaml`` at a production scope later
and the same flowgroup reads production credentials, unchanged.

.. note::

   The secret reference is resolved to a ``dbutils.secrets.get(...)`` call in the
   generated code — the password is fetched at run time, never written into the
   Python. That is why the credential lives behind ``${secret:scope/key}`` and
   not as a literal string in the flowgroup.

Generate the pipeline
=====================

Now compile the YAML into Lakeflow code. Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_customers  0 files
   ✓ validate (0.44s)
   1 validated · 0.5s

   $ lhp generate --env dev
   ✓ discover (0.00s)
   ✓ preflight (0.00s)
   ✓ bronze_customers  1 file
   ✓ generate (0.38s)
   ✓ format (0.02s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

``validate`` resolves every token and secret reference and checks the actions
before you commit to generating; ``generate`` writes the Python.

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/bronze_customers/customers_jdbc.py``. This is the *entire*
output — nothing is hidden behind a runtime:

.. literalinclude:: ../../_fixtures/guide_ingest_jdbc/generated/dev/bronze_customers/customers_jdbc.py
   :language: python
   :caption: generated/dev/bronze_customers/customers_jdbc.py

It's ordinary Lakeflow. Because you set ``readMode: batch``, the source view is a
``@dp.temporary_view`` wrapping ``spark.read.format("jdbc")`` with the ``url``,
``driver``, and ``dbtable`` options set from your ``source``. Each
``${secret:sales_db/...}`` reference became a
``dbutils.secrets.get(scope="dev_sales_db_secrets", key=...)`` call — the
password is fetched at run time, never stored. The target is a
``@dp.materialized_view`` whose three-part name resolved to
``dev_catalog.bronze.customers``, defined by a batch ``spark.read.table`` of the
source view. You described a connection and a target table; LHP wrote the reader,
the secret lookups, and the view declaration that connect them.

What you just did
=================

A two-action flowgroup — thirty-three lines of YAML, comments and all — compiled
to fifty-four lines of Lakeflow Python. **Zero lines of PySpark came from you**:
not the ``spark.read.format("jdbc")`` reader, not the two ``dbutils.secrets.get``
lookups that keep the password out of source, not the ``@dp.materialized_view``
declaration with its resolved three-part name. And the output is code you own:
version it, diff it, and open it in the Databricks editor like anything else.

The payoff is less about line count than about the parts you did not have to get
right by hand — the secret handling and the batch-read wiring are generated the
same way every time, for every JDBC source you add.

What's next
===========

- **Push a query to the database.** Swap ``table`` for a ``query`` field to read
  the result of a ``SELECT`` instead of a whole table — filter, project, or join
  at the source so the pipeline pulls only the rows it needs.
- **Stamp extraction metadata onto the rows.** ``operational_metadata`` adds
  columns such as an extraction timestamp to the loaded view, so downstream
  tables can tell when each batch arrived.
- **See every JDBC option.** Other drivers (MySQL, SQL Server, Oracle), the
  ``query`` form, and the credential rules are listed in the load action
  reference with their constraints.
