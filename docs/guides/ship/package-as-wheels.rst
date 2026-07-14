================================
Package pipeline code as a wheel
================================

.. meta::
   :description: Package a Lakehouse Plumber pipeline's generated code and its reusable transform library into one versioned, content-addressed Python wheel a Databricks pipeline installs — instead of syncing loose modules or copy-pasting them between projects.

Your generated pipeline is a folder of Python modules — the flowgroup module
here, a reusable transform library there. You can sync every one of those files
into the workspace on each deploy, and copy the shared ones by hand into the
next project that needs them. Or you package the whole pipeline as a single
versioned wheel: one artifact you build once, a pipeline installs, and you reuse
the way you reuse any Python distribution.

That is the choice on this page: **version and reuse pipeline code as a proper
artifact, not copy-pasted modules.** Packaging is opt-in per pipeline and changes
nothing about the generated logic — the code inside the wheel is byte-identical
to what source mode writes to disk. It pays off on **large projects** (on the
order of 5,000 generated files or more), where the per-file write in
``lhp generate`` and the file-by-file upload in ``databricks bundle deploy``
both grow long and collapsing each pipeline to one uploaded artifact recovers
that time. Below that scale, source mode is simpler.

Let's package a ``silver_orders`` pipeline — whose Python transform is exactly
the kind of reusable library worth versioning — as a wheel, and read every file
LHP writes to wire it up.

Before you start
================

Wheel packaging builds on Declarative Automation Bundle integration, so you need
a **bundle project**: a ``databricks.yml`` at the project root. You also need a
**Unity Catalog volume** your deploy identity can write to. Serverless compute
installs custom wheels only from a ``/Volumes/...`` path, so the built wheel has
to land on a volume — not the workspace file tree.

One structural rule follows from packaging: in wheel mode each flowgroup is
packed under a sanitized import package, so a top-level Python ``import`` of one
flowgroup module from another will not resolve. Relate flowgroups through the
Lakeflow Spark Declarative Pipelines dataset graph, not Python imports.

Point the artifact volume
=========================

Declare the volume built wheels upload to in a top-level ``wheel`` block in
``lhp.yaml``. The value is substitution-token-aware — LHP resolves ``${...}``
tokens per environment exactly as it does any other value — so one entry serves
every environment:

.. literalinclude:: ../../_fixtures/guide_ship_wheel/lhp.yaml
   :language: yaml
   :caption: lhp.yaml

Define the tokens per environment in ``substitutions/<env>.yaml`` so the resolved
path is environment-specific:

.. literalinclude:: ../../_fixtures/guide_ship_wheel/substitutions/dev.yaml
   :language: yaml
   :caption: substitutions/dev.yaml

The **resolved** value must start with ``/Volumes/``. If a pipeline is set to
wheel mode and ``wheel.artifact_volume`` is missing, empty, or resolves to a
non-``/Volumes/`` path, ``lhp generate`` stops with ``LHP-CFG-061``. A malformed
``wheel`` block — ``artifact_volume`` set to something other than a string —
fails with ``LHP-CFG-060``.

Turn on wheel packaging
=======================

The packaging toggle lives in the per-environment pipeline config file you pass
with ``-pc``. It resolves lowest-to-highest: the built-in default ``source``,
then ``project_defaults.packaging`` (the environment-wide default), then a
per-pipeline ``packaging`` block (which wins for that pipeline). The only
accepted values are ``source`` and ``wheel``; anything else fails with
``LHP-VAL-062``.

Here only ``silver_orders`` is packaged, leaving every other pipeline in source
mode:

.. literalinclude:: ../../_fixtures/guide_ship_wheel/config/pipeline_config.yaml
   :language: yaml
   :caption: config/pipeline_config.yaml

Because the toggle lives in the per-environment config, a pipeline can be
``source`` in dev and ``wheel`` in prod with no change to its logic. To package
every pipeline instead, set ``packaging: wheel`` under ``project_defaults`` and
opt individual pipelines back out.

Generate the wheel
==================

Run ``lhp generate`` for the target environment. Source-mode and wheel-mode
pipelines mix freely in one run; LHP builds each wheel once, in process:

.. code-block:: console

   $ lhp generate --env dev -pc config/pipeline_config.yaml
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_orders  1 file
   ✓ generate (0.36s)
   ✓ format (0.04s)
   ✓ monitoring (0.00s)
   ✓ bundle_sync (0.01s)
   1 pipeline generated · 1 file · 0.4s

Note the ``1 file``: in wheel mode LHP writes no loose flowgroup modules to the
workspace tree. The one file it syncs is a runner; the flowgroup code goes into
the wheel.

Read what LHP wrote
===================

**The runner.** ``generated/dev/silver_orders/silver_orders_runner.py`` is the
only file synced to the workspace. This is the whole thing — nothing is hidden
behind a runtime:

.. literalinclude:: ../../_fixtures/guide_ship_wheel/generated/dev/silver_orders/silver_orders_runner.py
   :language: python
   :caption: generated/dev/silver_orders/silver_orders_runner.py

The runner republishes the ambient ``spark`` and ``dbutils`` globals into
``builtins`` and then imports the wheel's flowgroup package, so each module's
dataset-registering decorators execute the same way they do in source mode. It
references only the import-package name — never a flowgroup name, the content
hash, or the version — so its bytes stay stable across pipeline content changes.

**The pipeline resource.** Alongside the runner, LHP wrote one bundle resource
per pipeline under ``resources/lhp/``. Your ``catalog`` and ``schema`` are
resolved and embedded; the last ``environment.dependencies`` entry is the
prebuilt wheel:

.. literalinclude:: ../../_fixtures/guide_ship_wheel/resources/lhp/silver_orders.pipeline.yml
   :language: yaml
   :caption: resources/lhp/silver_orders.pipeline.yml
   :lines: 1-25

The wheel reference is a **file-relative local path**
(``../../generated/dev/_wheels/...whl``), not a ``/Volumes/...`` path.
Declarative Automation Bundles classifies a local reference as a prebuilt wheel:
on deploy it uploads the file to ``<artifact_path>/.internal/`` and rewrites the
dependency to that uploaded location itself. LHP never composes the remote path,
and this file never carries the ``packaging`` key — the toggle is consumed by
LHP, not by Databricks. Below the lines shown, LHP appends a commented catalogue
of every other pipeline option. This folder is wiped and regenerated on every
run, so put custom resources in the top-level ``resources/`` instead.

**The wheel wiring.** ``resources/lhp/_wheels.bundle.yml`` is LHP-owned and
regenerated on every run:

.. literalinclude:: ../../_fixtures/guide_ship_wheel/resources/lhp/_wheels.bundle.yml
   :language: yaml
   :caption: resources/lhp/_wheels.bundle.yml

It sets ``workspace.artifact_path`` to the resolved artifact volume and excludes
the ``_wheels/`` staging directory from the bundle file sync. It carries **no**
``artifacts`` block on purpose: each wheel is already a prebuilt local dependency
that Declarative Automation Bundles uploads and rewrites, so declaring a wheel
artifact with no source files would make the deploy try to rebuild the
already-built wheel. LHP does not edit your ``databricks.yml``.

Look inside the wheel
=====================

Wheel mode leaves no loose ``.py`` files on disk, so to see what a build packaged
use ``lhp inspect-wheel``. Identify the wheel by **pipeline name** — which
resolves the content-hashed filename for you, and so needs ``--env`` — or by an
explicit path to the ``.whl``:

.. code-block:: console

   $ lhp inspect-wheel silver_orders --env dev
   Modules in lhp_silver_orders_dev_9b1ed495cd12-0.9.1-py3-none-any.whl (pipeline=silver_orders, env=dev)
   Module                                        Size (bytes)
   custom_python_functions/order_transforms.py   2029
   silver_orders/__init__.py                     52
   silver_orders/orders_normalize.py             1960
   Total modules: 3

Three modules ship in one wheel: the flowgroup package (``silver_orders/``) and
``custom_python_functions/order_transforms.py`` — the reusable transform library,
packed as a top-level import root. That library travels with the pipeline as a
versioned artifact instead of being copied into the next project by hand.

To read or diff the packaged code in an editor, extract the ``.py`` members with
``--extract``; LHP preserves their in-wheel directory structure and never
modifies the wheel:

.. code-block:: console

   $ lhp inspect-wheel silver_orders --env dev --extract /tmp/silver_orders

Extraction is read-only. Editing the extracted files does not change the deployed
wheel — that is rebuilt from your YAML on the next ``lhp generate``. The command
exits non-zero when the wheel is missing (``LHP-IO-022``), is not a wheel file
(``LHP-IO-023``), or is corrupt (``LHP-IO-024``); when a name resolves to zero or
several wheels (``LHP-GEN-001``); or when a pipeline name is given without
``--env``.

Ship it
=======

The built wheel is deterministic and content-addressed. Its filename encodes the
pipeline's identity — ``lhp_<pipeline>_<env>_<hash>-<lhp-version>-py3-none-any.whl``,
where ``<hash>`` is a 12-character SHA-256 digest over the packaged ``.py`` bytes.
An unchanged pipeline rebuilds a byte-identical wheel with an identical filename,
so its deploy is a no-op; any change to the generated logic changes the hash, and
with it the filename. The same commit regenerates an identical wheel on any
machine, so rollback is a redeploy of a prior commit.

Generation is where LHP's job ends and the Databricks CLI takes over:

.. code-block:: console

   $ databricks bundle validate --target dev
   $ databricks bundle deploy   --target dev

On deploy, Declarative Automation Bundles uploads each prebuilt wheel to
``<artifact_path>/.internal/`` under the artifact volume and rewrites the
pipeline's dependency to that uploaded location; it does not trigger a second
build. When the pipeline runs, it installs the wheel and loads the runner, which
imports your packaged flowgroup modules.

.. note::

   Keep dev and test in **source mode**, even when production runs on wheels.
   Source mode writes clear-text ``.py`` files you can open and edit in place in
   the workspace for a quick iteration — a wheel must be rebuilt and reinstalled
   to change, and ``lhp inspect-wheel`` gives you read-only inspection, not that
   edit loop. Because the toggle is per environment, one pipeline can be
   ``source`` in dev and ``wheel`` in prod. One more thing to check before you
   switch a pipeline: inside a wheel, ``__file__`` resolves into the installed
   package, and only ``.py`` files are packaged, so a flowgroup or helper that
   reads a sidecar data file by a ``__file__``-relative path will not find it in
   wheel mode. Keep such a pipeline in source mode.

What you just did
=================

You turned a pipeline's Python — a flowgroup module and its reusable transform
library, three modules in all — into a single versioned wheel, and the workspace
now syncs one 32-line runner instead of those loose files. The transform library
is a real artifact you version and reuse, not code copy-pasted between projects.
The wheel is content-addressed: rebuild it from the same commit anywhere and you
get the same bytes and the same filename, which makes rollback a redeploy and
makes ``lhp validate`` and dependency analysis — which read YAML — indifferent to
the packaging mode.

What's next
===========

- **Promote the same commit across environments.** A pipeline packaged as a wheel
  in prod stays source in dev; the same commit regenerates an identical wheel,
  so CI/CD promotion and rollback are redeploys of the same commit.
- **Inspect any built wheel.** ``lhp inspect-wheel`` lists or extracts a wheel by
  pipeline name or by path — reach for it whenever you need to see exactly what
  code a build packaged.
- **Deploy the bundle.** The full deploy path — targets, ``databricks bundle
  deploy``, and running the pipeline — is covered in the bundles guide.
