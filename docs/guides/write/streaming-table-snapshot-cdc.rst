==========================================
Build a dimension from periodic snapshots
==========================================

.. meta::
   :description: Materialize SCD Type 2 dimension history from periodic full snapshots in Lakehouse Plumber — declare one snapshot_cdc write action and let LHP write the create_auto_cdc_from_snapshot_flow, instead of hand-writing apply-changes-from-snapshot.

Some sources never hand you a change feed. A partner drops a full CSV export of
every customer each night; an upstream job overwrites a bronze table with the
current state on a schedule. You get *snapshots* — the whole table, over and
over — and it is your job to turn that sequence of full copies into a dimension
that remembers how each customer changed.

You could hand-write it: a ``create_streaming_table``, then a
``create_auto_cdc_from_snapshot_flow`` call, then the loop that hands successive
snapshot versions to the engine, then the ``functools.partial`` that feeds it
your parameters. Or you declare ``mode: snapshot_cdc`` on one ``write`` action
and let Lakehouse Plumber write that Lakeflow for you. That is the idea on every
page: **declare your ETL, don't hand-write it.**

That is also the difference from change-feed CDC. When your source already emits
row-level change events with a sequence column, you use ``cdc`` mode and
``create_auto_cdc_flow`` — the change-feed CDC guide covers that; snapshot CDC is
for when all you get is a periodic full dump and the engine must derive the
inserts, updates, and deletes by comparing each snapshot to the one before it.

Let's build a silver ``dim_customer`` from periodic ``customer_snapshots``,
keeping full SCD Type 2 history, without touching the apply-changes API.

Before you start
================

You need a Lakehouse Plumber project (see the Get Started course for
``lhp init``) and a bronze ``customer_snapshots`` table that carries a
``snapshot_id`` column stamping each full copy with its version. Snapshot CDC
reads that source directly — it does not create it, and this flowgroup needs no
``load`` action to feed it.

Declare the snapshot-CDC write
==============================

A snapshot-CDC dimension is a single ``write`` action: a ``streaming_table``
target with ``mode: snapshot_cdc`` and a ``snapshot_cdc_config`` block. Create
``pipelines/silver_dim_customer.yaml``:

.. literalinclude:: ../../_fixtures/guide_write_st_snapshot_cdc/pipelines/silver_dim_customer.yaml
   :language: yaml
   :caption: pipelines/silver_dim_customer.yaml
   :emphasize-lines: 15

The emphasized line does the work. ``mode: snapshot_cdc`` selects the entire
generated shape — a ``create_auto_cdc_from_snapshot_flow`` instead of the
``append_flow`` a standard streaming table gets. The ``snapshot_cdc_config``
carries the rest: ``keys`` names the primary key the engine matches rows on,
``stored_as_scd_type: 2`` keeps full history (a new row per change, with validity
windows the engine maintains), and ``track_history_column_list`` narrows which
columns open a new history row — a changed ``name``, ``address``, or ``tier``
starts a new version; changes to any other column update the current row in
place.

The source is a ``source_function`` rather than an action-level ``source``,
which is what lets this flowgroup stand alone with no ``load``. The function
returns the snapshots one version at a time; its ``parameters`` are bound to it
at generate time. The write action reference documents the alternative — a plain
``source:`` table reference — along with SCD Type 1 and the other tuning knobs.

.. note::

   The snapshot function runs outside Lakeflow's decorated context — the engine
   calls it as a plain callable. It therefore cannot read a view or table built
   by another action in this pipeline; it must read its source directly, which
   is exactly why no ``load`` action feeds it. Point it at a real table or path.

Write the snapshot function
===========================

``source_function`` points at a Python file and a function name. The function
takes the last version the engine processed and returns the next snapshot as a
``(DataFrame, version)`` pair — or ``None`` when there are no newer snapshots
left. Create ``functions/customer_snapshot.py``:

.. literalinclude:: ../../_fixtures/guide_write_st_snapshot_cdc/functions/customer_snapshot.py
   :language: python
   :caption: functions/customer_snapshot.py

On the first run ``latest_version`` is ``None``, so the function returns the
oldest snapshot; on each later run it returns the next ``snapshot_id`` greater
than the last one processed, and ``None`` once it has caught up. The engine walks
that sequence in order, diffing each full snapshot against the running state to
derive the SCD changes. The ``snapshot_table`` name is a ``parameter``, not
hard-coded — so the same function is reusable across catalogs and testable on
its own.

Point the tokens at your environment
=====================================

The ``${...}`` tokens resolve from a per-environment substitutions file. Create
``substitutions/dev.yaml``:

.. literalinclude:: ../../_fixtures/guide_write_st_snapshot_cdc/substitutions/dev.yaml
   :language: yaml
   :caption: substitutions/dev.yaml

This is the only place environment-specific values live. The
``snapshot_table`` parameter composes them — ``${catalog}.${bronze_schema}.customer_snapshots``
— and LHP resolves the whole string before binding it to the function. To run
against production later, you write a ``prod.yaml``; the flowgroup and the
function never change.

Generate the pipeline
=====================

Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_dim  0 files
   ✓ validate (0.37s)
   1 validated · 0.4s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_dim  1 file
   ✓ generate (0.34s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

``validate`` resolves every token and checks the snapshot-CDC config before you
commit to generating; ``generate`` writes the Python.

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/silver_dim/dim_customer.py``. This is the entire output —
nothing is hidden behind a runtime:

.. literalinclude:: ../../_fixtures/guide_write_st_snapshot_cdc/generated/dev/silver_dim/dim_customer.py
   :language: python
   :caption: generated/dev/silver_dim/dim_customer.py
   :emphasize-lines: 22-24, 27-36

LHP emitted a ``create_streaming_table`` for the dimension, then the
``create_auto_cdc_from_snapshot_flow`` that drives it: your ``keys``,
``stored_as_scd_type``, and ``track_history_column_list`` passed straight
through, and your function wrapped in a ``functools.partial`` with
``snapshot_table`` already resolved to ``dev_catalog.bronze.customer_snapshots``.
It also copied your function into ``custom_python_functions/``, imported it under
an alias, and injected ``spark`` and ``dbutils`` into its namespace so the plain
callable resolves those names against the pipeline's session at run time. You
described *which snapshots, keyed how, tracked to what depth*; LHP wrote the
apply-changes-from-snapshot declaration.

What you just did
=================

Twenty-four lines of YAML — plus a twenty-five-line snapshot function that is
genuinely yours, the versioning logic no framework can guess — compiled to a
thirty-six-line Lakeflow module. **Every line of the apply-changes-from-snapshot
wiring came from LHP, not from you**: the ``create_streaming_table``, the
``create_auto_cdc_from_snapshot_flow`` call, the ``functools.partial`` binding,
the module import, and the ``spark``/``dbutils`` injection. Change ``keys`` or the
tracked columns and regenerate, and LHP rewrites the declaration to match — you
never touch the generated Python.

What's next
===========

- **Supply snapshots from a table instead of a function.** When the current
  state already lives in one Delta table, a plain ``source:`` reference replaces
  ``source_function``. The write action reference documents both forms.
- **Switch to SCD Type 1.** Set ``stored_as_scd_type: 1`` to overwrite each row
  in place instead of keeping history — one field, same flow.
- **Compare with change-feed CDC.** When your source emits row-level change
  events with a sequence column, ``cdc`` mode and ``create_auto_cdc_flow`` fit
  better than snapshots. See the change-feed CDC guide.
