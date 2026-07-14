================================
Ingest from a custom data source
================================

.. meta::
   :description: Wire a user-supplied PySpark DataSource into a Lakehouse Plumber load action — LHP copies the module, registers the class, and reads it, so you never hand-write spark.dataSource.register or the cloudpickle executor fix.

Some sources have no built-in Spark connector: a REST API, an internal protocol,
a queue only your platform speaks. For those you write a PySpark ``DataSource``
class — and then you have to *wire it into the pipeline*: register it on the
session, read it with the right format string, and make it survive the trip to
the executors.

You could hand-write that wiring: a ``spark.dataSource.register(...)`` call, a
``spark.readStream.format(...).load()`` inside a ``@dp.temporary_view``, and — the
line everyone forgets — a cloudpickle registration so the class serializes to the
executors. Or you declare a ``type: custom_datasource`` load, point it at your
module and class, and let Lakehouse Plumber write all of that. That is the idea
on every page: **declare your ETL, don't hand-write it.**

Let's ingest live currency exchange rates from a REST API through a
``DataSource`` you supply, and land them in a bronze streaming table.

Before you start
================

You need Lakehouse Plumber installed and a project to work in. You also need the
``DataSource`` implementation itself — writing one is a PySpark skill, not a
Lakehouse Plumber one, so this guide brings a finished class and focuses on
wiring it in. If the source you need already has a native load type
(``cloudfiles``, ``delta``, ``jdbc``, ``kafka``), reach for that instead; a
custom data source is for the systems Spark cannot read on its own.

Bring the DataSource class
==========================

Put the PySpark ``DataSource`` under ``data_sources/`` at the project root.
This one streams exchange rates from an HTTP endpoint:

.. literalinclude:: ../../_fixtures/guide_ingest_customds/data_sources/exchange_rates_source.py
   :language: python
   :caption: data_sources/exchange_rates_source.py
   :emphasize-lines: 25-27

Two things in this file are load-bearing for the wiring. First, the ``name()``
classmethod returns ``"exchange_rates"`` — that string, **not the class name**,
becomes the ``.format(...)`` argument in the generated read. Second, because
``readMode`` will be ``stream``, the class implements ``streamReader`` and a
``DataSourceStreamReader`` so Spark manages the offsets. A batch source would
implement ``reader`` / ``DataSourceReader`` instead.

Declare the custom data source load
===================================

A flowgroup is a short YAML file describing a sequence of actions. This one has
two — a ``load`` that reads through your ``DataSource``, and a ``write`` that
appends the result into a streaming table. Create
``pipelines/exchange_rates.yaml``:

.. literalinclude:: ../../_fixtures/guide_ingest_customds/pipelines/exchange_rates.yaml
   :language: yaml
   :caption: pipelines/exchange_rates.yaml
   :emphasize-lines: 12-18

The ``source`` block names the class by two required fields: ``module_path``
points at the ``.py`` file relative to the project root, and
``custom_datasource_class`` names the class inside it. Everything under
``options`` is handed to your reader — here an ``api_url`` and a
``base_currencies`` list, which surface as ``self.options`` in the
``DataSourceStreamReader``. ``readMode: stream`` selects the streaming reader;
drop it and ``stream`` is the default anyway.

The ``${...}`` tokens are placeholders you resolve per environment, so the same
flowgroup runs unchanged in dev, staging, and prod.

Point the tokens at your environment
====================================

The ``${exchange_api_url}`` in ``options`` and the ``${catalog}`` /
``${bronze_schema}`` in the write resolve from a per-environment substitutions
file. Create ``substitutions/dev.yaml``:

.. literalinclude:: ../../_fixtures/guide_ingest_customds/substitutions/dev.yaml
   :language: yaml
   :caption: substitutions/dev.yaml

This is the only place environment-specific values live. Substitution reaches
into ``options`` and into the copied DataSource file itself, so a production
endpoint — or a ``${secret:scope/key}`` reference for an API key — is a change
to ``prod.yaml``, never to the flowgroup or the class.

Generate the pipeline
=====================

Now compile the YAML into Lakeflow code. Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_rates  0 files
   ✓ validate (0.33s)
   1 validated · 0.3s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_rates  1 file
   ✓ generate (0.35s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

``validate`` resolves every token and checks the actions before you commit to
generating; ``generate`` writes the Python — and copies your ``DataSource``
module next to it.

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/bronze_rates/exchange_rates.py``. This is the *entire*
output — nothing is hidden behind a runtime:

.. literalinclude:: ../../_fixtures/guide_ingest_customds/generated/dev/bronze_rates/exchange_rates.py
   :language: python
   :caption: generated/dev/bronze_rates/exchange_rates.py
   :emphasize-lines: 10,23,32

Every piece of the wiring is here. LHP registered the class with
``spark.dataSource.register(ExchangeRatesDataSource)``, read it with
``spark.readStream.format("exchange_rates")`` — that format string came from your
``name()`` method, not the class name — and passed the resolved ``api_url`` and
``base_currencies`` through as ``.option(...)`` calls. The
``@dp.temporary_view`` view feeds a ``create_streaming_table`` and an
``@dp.append_flow`` that land the rates in ``dev_catalog.bronze.exchange_rates``.

The one line you would never have remembered is
``_lhp_cloudpickle.register_pickle_by_value(custom_python_functions)``. LHP copied
your module into a ``custom_python_functions/`` package beside the generated file
(with a ``# LHP-SOURCE`` / DO-NOT-EDIT header) and registered that package with
PySpark's *vendored* cloudpickle, so the ``DataSource`` class survives
serialization when the pipeline ships it to the executors. Miss that line by hand
and the pipeline fails at run time with a pickling error.

What you just did
=================

Twenty-two lines of flowgroup YAML compiled to sixty-two lines of Lakeflow
Python. **Zero lines of the wiring came from you** — not the
``spark.dataSource.register`` call, not the ``format(...).load()`` read, not the
``@dp.temporary_view``, not the ``create_streaming_table`` / ``append_flow``, and
not the cloudpickle registration.

The ``DataSource`` class stays yours: LHP does not write your fetch logic, and it
does not touch the class it copies. What it owns is the plumbing that turns a
class on disk into a registered, executor-safe, wired-up streaming source — the
part that is fiddly to get right and identical every time.

What's next
===========

- **Switch to a batch source.** Set ``readMode: batch`` and implement ``reader``
  / ``DataSourceReader`` on the class instead of the streaming pair; LHP emits
  ``spark.read.format(...)`` in place of ``spark.readStream``.
- **Feed a secret in.** Reference ``${secret:scope/key}`` inside ``options`` for
  an API key or token — it resolves to a ``dbutils.secrets.get(...)`` call in the
  generated read, so no credential lands in YAML.
- **See every field.** ``module_path``, ``custom_datasource_class``, ``options``,
  and the ``readMode`` rules are all covered in the load action reference.
