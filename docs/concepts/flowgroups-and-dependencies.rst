===========================
Flowgroups and dependencies
===========================

.. meta::
   :description: How Lakehouse Plumber infers pipeline dependencies and execution order from what each action produces and consumes — no hand-maintained orchestration.

A medallion project is a chain: raw feeds bronze, bronze feeds silver, silver
feeds gold. Something has to know that bronze runs before silver. Build that
chain by hand — in raw Lakeflow, or a Databricks job — and *you* are that
something: you maintain the task graph, the ``depends_on`` clauses, the standing
rule that the silver task waits for bronze. Rename a table or slot in a new
stage and you go back and re-thread the order yourself.

Lakehouse Plumber takes that job away. You describe each action's data flow —
what it reads, what it writes — and nothing else. LHP reads every action across
every flowgroup, works out which action's output feeds which action's input,
and builds the dependency graph for you. From that graph it computes execution
order and can emit a Databricks orchestration job with the task dependencies
already wired. It is the same principle that runs through the whole tool:
**declare the data flow, don't hand-maintain the run order.**

How an edge forms: produce and consume
=======================================

Every action has two halves LHP cares about — what it **produces** and what it
**consumes**.

- A ``load`` or ``transform`` action produces a **view** (its ``target``,
  conventionally named ``v_...``). A ``write`` action produces a **table**: the
  ``streaming_table`` or ``materialized_view`` it targets, or the destination of
  a Delta sink.
- An action consumes whatever it reads — its declared ``source``, plus every
  table read inside the SQL or Python body it points at.

LHP indexes every produced name, then, for each consumed reference, looks up a
producer. A match becomes one edge, pointing from producer to consumer. A
reference with no producer inside the project is an **external source** — a
table LHP reads but does not manage, and a root of the graph.

Matching is by **canonical value**, not by how you spelled it. Before comparing,
LHP lowercases each reference and strips backticks and surrounding whitespace, so
``` `MyCat`.`Sales`.`Orders` ``` and ``mycat.sales.orders`` resolve to the same
producer. It does **not** resolve substitution tokens: a ``${...}`` reference is
matched by its literal bytes, so a producer and a consumer that use a token must
spell it identically to connect. When a reference is only knowable at runtime —
built dynamically, or read through an idiom the parser cannot follow — you
declare the edge by hand. The dependency-analysis guide under Quality and ops
covers that escape hatch.

Views are pipeline-scoped, tables are global
============================================

The rule at the center of the model is where a produced name is visible.

A **view** is pipeline-scoped. It exists only inside the pipeline that declares
it, mirroring how a Lakeflow temporary view lives only for the run that creates
it. LHP matches a view by the pair *(pipeline, name)*: a view produced in one
flowgroup connects to any sibling flowgroup **in the same pipeline**, and is
invisible to every other pipeline. Two pipelines can each declare a ``v_orders``
without colliding.

A **table** is global. Its ``catalog.schema.table`` name addresses one object
for the whole project, so any action in any pipeline can read it and form an
edge.

This is the rule that decides where you need a persisted table. Inside a single
pipeline, split work across as many flowgroups as you like and pass data between
them through views — unmaterialized, no storage. But a boundary **between**
pipelines can only be crossed through a table. If a flowgroup in your silver
pipeline needs data your bronze pipeline produced, bronze must **write a table**
and silver reads it back; a view will not reach across, and the edge simply
never forms. Deciding what to materialize is, in large part, deciding where your
pipeline boundaries fall.

.. mermaid::

   flowchart LR
       subgraph bronze["Pipeline: bronze"]
           A["load orders_raw"] -->|"v_orders_raw — view, pipeline-scoped"| B["write orders — table"]
       end
       subgraph silver["Pipeline: silver"]
           C["transform clean_orders"] -->|"v_clean — view, pipeline-scoped"| D["write orders_clean — table"]
       end
       B -->|"orders — table, global"| C

Within each pipeline a view carries the edge; across the pipeline boundary, only
the global ``orders`` table connects bronze to silver.

From graph to execution stages
==============================

With every edge known, LHP holds a directed graph — the same relationships
rolled up at three levels: individual actions, the flowgroups that contain them,
and the pipelines that contain those. Execution order falls straight out of it.
LHP sorts the graph into **stages**: a producer lands in an earlier stage than
anything that reads it, and everything with no dependency between it shares a
stage. Same-stage items have nothing to wait on each other for, so they run in
parallel.

You never number those stages. Add a gold table that reads a silver table and it
drops into the stage after silver on its own. Ordering works the same way inside
a flowgroup, which is why the generated Python always defines a view before the
action that reads it. And because the order is derived rather than declared, LHP
catches what a hand-maintained job cannot: if two actions read each other — a
cycle with no valid order — LHP reports it (``LHP-DEP-001``) instead of emitting
code that cannot run.

Where to go next
================

This page is the model. To work with it in practice:

- The **dependency-analysis guide** under Quality and ops shows how to run
  ``lhp dag`` to print the graph, its execution stages, and the orchestration
  job LHP generates from them.
- The **multi-flowgroup guide** under Reuse and scale walks through splitting one
  pipeline across several flowgroups that share views.
