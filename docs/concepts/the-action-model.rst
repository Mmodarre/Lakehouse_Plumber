====================
The action model
====================

.. meta::
   :description: The mental model behind Lakehouse Plumber — every pipeline is an ordered sequence of typed actions (load, transform, write, test) wired together by named views, instead of hand-written readStream and table plumbing.

Every Lakehouse Plumber pipeline is built from one kind of thing: an **action**.
An action is a single, typed step — read a source, reshape a view, persist a
table, or assert a rule on the rows. A **flowgroup** is an ordered list of those
steps that carries data from its source all the way to a table.

That is the whole model. You do not open a ``readStream``, thread the resulting
DataFrame through three transformations, and hand it to a
``create_streaming_table`` call — keeping every intermediate variable and every
flow decorator straight by hand. You declare *what each step is* and *which view
it reads*, and Lakehouse Plumber writes the plumbing that connects them.
**Declare your ETL, don't hand-write it.**

Four verbs
==========

We model every step as one of four action kinds. Each answers a different
question about the data:

- **load** — *get data in.* A load reads one external source and produces the
  first view in the flowgroup. The source is a typed descriptor, not raw code:
  Auto Loader (``cloudfiles``), a Delta table or its Change Data Feed
  (``delta``), a SQL query (``sql``), an external database (``jdbc``), Kafka, a
  Python function, or a custom data source.
- **transform** — *reshape it.* A transform reads a view and produces another
  view. It is optional and repeatable: a flowgroup has zero, one, or many.
  Transforms come in kinds too — SQL, Python, schema enforcement, temporary
  tables, and data-quality rules.
- **write** — *persist it.* A write reads the last view and materializes it as a
  real object: a ``streaming_table`` (incremental, including CDC and snapshot
  modes), a ``materialized_view`` (batch-refreshed analytics), or a ``sink``
  that pushes data out of the lakehouse. A flowgroup ends here.
- **test** — *assert on it.* A test attaches a data-quality expectation to a
  view — uniqueness, referential integrity, completeness, a value range, a
  schema match, or a custom rule — and fails or warns when the rows violate it.

The verbs are fixed; the sub-types are the vocabulary within each. The action
reference lists every sub-type and its fields exhaustively, and the how-to
guides walk through choosing one for a given workload. This page is only about
how the four fit together.

Chained by views
================

The verbs are wired together by **named views**. Every action names its input
with ``source`` and its output with ``target``, and a ``target`` is just a view
name — an in-flow handle, not a table. One action's ``target`` becomes the next
action's ``source``. That single rule is the whole wiring scheme.

.. code-block:: yaml
   :caption: A load → transform → write chain

   actions:
     - name: load_orders_csv
       type: load
       source:
         type: cloudfiles              # the external source, not a view
         path: "${landing_path}/orders/*.csv"
         format: csv
       target: v_orders_raw            # produces the first view

     - name: clean_orders
       type: transform
       transform_type: sql
       source: v_orders_raw            # reads the load's view
       target: v_orders                # produces the next view
       sql: "SELECT order_id, customer_id, amount FROM stream(v_orders_raw)"

     - name: write_orders_bronze
       type: write
       source: v_orders                # reads the transform's view
       write_target:
         type: streaming_table
         catalog: "${catalog}"
         schema: "${bronze_schema}"
         table: orders                 # the terminus — a real table

Read the chain top to bottom and it reads as a sentence: load raw orders into
``v_orders_raw``, clean that into ``v_orders``, write ``v_orders`` to the
``orders`` table. The two ends of the chain are asymmetric on purpose. A load's
``source`` is the external system, so the load originates the first view. A
write's terminus is a table declared under ``write_target``, so the write has no
downstream view of its own. Everything between is a view feeding a view.

A test sits off to the side of this spine. It names a view as its ``source`` and
checks it, but it does not hand a view to the next step — so adding or removing a
test never reshapes the data flow, only what the pipeline asserts about it.

.. note::

   The one rule to carry away: **actions chain by view name.** Each action's
   ``target`` view is the next action's ``source``. A flowgroup is that chain,
   ordered from the source that opens it to the table that closes it.

Views also let the chain branch and merge. A ``source`` can name more than one
view to fan several inputs into one action, and more than one write can target
the same streaming table to fan several flows into one output. The wiring rule
does not change — only how many views meet at a node.

Why four verbs, chained by views
================================

Four verbs are enough because every ETL step is one of them: data comes in, gets
reshaped, goes out, or gets checked. Fixing the shape buys three things.

It is **uniform**. Every action carries the same handful of fields — ``name``,
``type``, ``source``, ``target`` — so a flowgroup reads the same way whether it
has two actions or twenty, and whether a step is a one-line SQL rename or a
custom Python source. There is no per-step boilerplate to learn or maintain.

It is **composable**. Because steps connect only through view names, you insert a
transform or a data-quality check by pointing it at an existing view and letting
the next step read *its* output instead. Nothing downstream needs rewiring. The
chain grows by naming, not by re-plumbing.

It **maps onto Lakeflow directly**. Each verb compiles to a known Lakeflow
construct — a load becomes a streaming read into a temporary view, a standard
streaming write becomes a ``create_streaming_table`` plus an ``append_flow``, a
materialized-view write becomes a materialized view, a test becomes a set of
Lakeflow expectations. The action model is not a runtime that hides Lakeflow
behind an abstraction; it is a way to *write* Lakeflow. The generated Python is
ordinary code you own, version, and open in the Databricks editor.

Where to go from here
=====================

- The **Get Started course** builds a real flowgroup end to end and shows the
  generated Lakeflow Python beside the YAML.
- The **how-to guides** cover choosing a load source, a write target, and a
  write mode for a given workload, and the CDC and data-quality patterns.
- The **action reference** documents every sub-type and every field.
