===========================
How Lakehouse Plumber works
===========================

.. meta::
   :description: The mental model behind Lakehouse Plumber — a compiler for data pipelines that turns declarative YAML into plain Lakeflow Python you own, with no LHP dependency at runtime.

Think of Lakehouse Plumber as a compiler for data pipelines. You write
declarative YAML that says *what* a pipeline is — a source to read, a table to
write, a check to enforce — and LHP compiles it to plain, readable Lakeflow
Python that runs on Databricks with no dependency on LHP at all. That is the
whole idea: **declare your ETL, don't hand-write it.**

The alternative is to hand-write and then maintain the Lakeflow boilerplate
yourself: the ``readStream`` calls, the streaming-table declarations, the append
flows, the change-data-capture merges, the data-quality expectations — the same
shapes, over and over, once per table. For a handful of tables that is tedious.
For fifty tables across dev, staging, and prod it is a maintenance burden that
grows with every new source. LHP writes that plumbing for you, from a pattern
you describe once.

A compiler, not a runtime
=========================

The distinction that matters most is this: LHP is a **code generator**, not a
runtime framework. It runs at build time, on your machine or in CI, and emits
Python source files. It is not installed on your cluster, it does not sit
between your pipeline and Spark, and it takes no part in execution. The Python
it writes imports only Lakeflow itself — nothing from LHP — so once the code is
generated, LHP is out of the picture entirely.

A runtime framework would work the opposite way: it would ship a library onto
the cluster and interpret your configuration while the pipeline runs. That
buys convenience at the cost of a permanent dependency — your pipelines can
only run where the framework is installed, at a version compatible with the one
you generated against, and every failure has to be debugged through the
framework's own layer.

Compiling to code you own avoids all of that:

- **Transparency.** The generated file is the whole story. There is no hidden
  behaviour behind a runtime abstraction — what you read is what runs, and you
  can open it in the Databricks editor like any other Lakeflow file.
- **Ownership.** The output is ordinary Python. Version it, diff it in pull
  requests, and edit it by hand if you ever need to.
- **No lock-in.** Remove LHP and your pipelines keep running unchanged, because
  nothing in the generated code refers back to it. LHP is a tool you adopt for
  authoring, not a runtime you commit to forever.
- **No runtime overhead or version coupling.** Because LHP never runs on the
  cluster, upgrading it cannot change what an already-deployed pipeline does.
  The build-time tool and the runtime pipeline evolve independently.
- **Familiar debugging.** When something misbehaves in production, you debug
  plain Lakeflow, not a framework's internals.

What you compose
================

You author pipelines from three nested objects. Understanding how they nest is
most of understanding LHP.

**Pipeline** is the outermost grouping — a deployment unit. Everything that
declares the same pipeline name is generated into one place and, when you
package a Databricks bundle, becomes one Lakeflow pipeline resource.

**FlowGroup** is a slice of a pipeline, usually one source table or one business
entity. A flowgroup is a small YAML file: it names its pipeline, names itself,
and lists an ordered set of actions. One file can hold one flowgroup or many.

**Action** is a single step. Every action has one of four kinds, and each kind
has a sub-type that selects how it is generated:

- **load** — read external data (cloud files, Delta, JDBC, Kafka, and more)
  into a view.
- **transform** — reshape, join, or apply data-quality rules to data already
  loaded.
- **write** — persist the result as a streaming table, materialized view, or
  sink.
- **test** — assert a property of the data, such as uniqueness or a row count.

A flowgroup reads top to bottom as a story — load a source, transform it, write
a table — but that order is for *you*, not the engine. The actions form a
directed acyclic graph, wired together by which views each one reads and
produces. Lakeflow's declarative engine schedules them at runtime from those
references, and LHP checks the graph while compiling, so a cycle fails before
you ever deploy.

A single action is compact. This ``load`` reads CSV files with Auto Loader into
a view named ``v_orders_raw``:

.. code-block:: yaml

   - name: load_orders
     type: load
     source:
       type: cloudfiles
       path: "${landing_path}/orders/*.csv"
       format: csv
     target: v_orders_raw

That fragment compiles to a ``readStream`` on ``cloudFiles`` inside a
``@dp.temporary_view`` — a dozen or so lines of Lakeflow you never write by
hand. The ``${landing_path}`` token is a placeholder resolved per environment,
which is how one flowgroup runs unchanged in dev, staging, and prod.

Three reuse primitives layer on top of this model so you describe a pattern once
rather than repeating it. **Presets** supply default values merged into matching
actions; **templates** supply whole parametrised actions; and **blueprints**
stamp out entire flowgroups from a single parameter set. Choosing between them
is covered in the guides and the reference; the point here is that reuse is
expressed in the YAML you author, never bolted on at runtime.

How the compile works
=====================

``lhp generate`` moves every flowgroup it finds through four stages. Reading
them as a compiler's front end to back end is a good mental picture:

.. mermaid::

   graph LR
       Y["Declarative YAML"] --> P["Parse"]
       P --> R["Resolve"]
       R --> G["Generate"]
       G --> F["Format"]
       F --> O["Plain Lakeflow Python"]

**Parse.** LHP discovers your flowgroup files, reads the YAML, and validates it
against a strict schema. Anything malformed is rejected here, with a clear
error, before any code is written.

**Resolve.** This is where the pipeline becomes concrete for one environment.
Local variables, template parameters, preset defaults, and environment tokens
are all resolved in a fixed order, so ``${landing_path}`` becomes a real path
and a template becomes real actions. An unresolved token stops the build rather
than leaking a placeholder into generated code. The full ordering is documented
in the reference.

**Generate.** Each resolved action is handed to the generator for its sub-type,
which emits the corresponding Lakeflow Python — a view, a streaming table, an
append flow, an expectation. This is the step that replaces the boilerplate you
would otherwise type.

**Format.** The generated tree is run through a single, pinned formatting pass
so the output looks the same every time, regardless of any formatting
configuration in your project.

Generation is **deterministic**: the same YAML and the same environment values
always compile to the same Python, and files are rewritten only when their
content actually changes. That is what makes generated output reviewable — a
diff in a pull request reflects a real change in intent, not incidental churn —
and safe to regenerate in CI. Running ``lhp validate`` first performs the parse
and resolve stages and their checks without writing any files, so you can catch
configuration errors before you commit to generating.

Where to go next
================

This page is the *why*. When you want to do something with it:

- **Build your first pipeline** — the Get Started course walks a source-to-table
  flowgroup end to end, from YAML to generated Lakeflow.
- **Do a specific task** — the how-to guides cover ingestion, data quality,
  change data capture, multi-environment substitutions, and bundle packaging.
- **Look something up** — the reference lists every action sub-type and its
  fields, every CLI flag, the substitution syntaxes, and the error codes.
