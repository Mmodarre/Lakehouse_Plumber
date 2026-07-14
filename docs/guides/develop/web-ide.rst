==============================
Develop in the local web IDE
==============================

.. meta::
   :description: Launch the Lakehouse Plumber local web IDE with lhp web to edit flowgroup YAML, see the dependency graph and generated Lakeflow code, and run validate and generate live in your browser — instead of hand-editing YAML and re-running the CLI blind.

The normal build loop is a terminal loop. You edit a flowgroup in your editor,
run ``lhp validate``, run ``lhp generate``, scroll the output, open the
generated Python to see what changed, then switch back to the YAML. You edit one
file at a time and hold the rest of the pipeline — the dependency graph, the
generated Lakeflow, the errors — in your head.

``lhp web`` puts that loop in one place. It serves a browser-based workbench for
a single project on your own machine: edit flowgroup YAML with schema-aware
validation, watch the dependency graph redraw as you save, read the generated
Lakeflow code next to the YAML that produced it, and run ``validate`` and
``generate`` with output streaming live. The declarative YAML and the generated
code are exactly what the CLI produces — a guided visual workspace instead of
hand-editing YAML and re-running the CLI blind.

Let's install the extra, launch it, and walk the edit-generate loop.

Install the web extra
=====================

The web IDE is an optional install. It pulls in a FastAPI server and uvicorn,
so it ships as an extra rather than a core dependency:

.. code-block:: bash

   pip install "lakehouse-plumber[webapp]"

Without the extra, ``lhp web`` stops immediately with error ``LHP-IO-026`` and
tells you to install it. The browser app itself ships pre-built inside the
package — you do not need Node.js or any front-end build step.

You also need an initialized project — a directory with an ``lhp.yaml``, your
``pipelines/``, and your ``substitutions/``. To start the IDE in an empty
directory instead, see `Start from an empty directory`_ below.

Launch the IDE
==============

From anywhere inside your project, run:

.. code-block:: console

   $ lhp web
   Starting Lakehouse Plumber web IDE at http://127.0.0.1:8000/#token=<session-token>

The server binds ``127.0.0.1`` only — it is loopback-only and never listens on a
network interface. Once it answers its health probe, your browser opens on the
printed address. The ``#token=`` fragment carries a per-session access token:
every request but the health check needs it, and a fresh token is minted on each
launch, so enter through the printed URL rather than a bookmark.

Port ``8000`` is the default. If it is already in use, the launch fails with
``LHP-IO-027`` — pick a free one:

.. code-block:: bash

   lhp web --port 8001

Pass ``--no-open`` to suppress the browser and click the printed URL yourself.
``--reload`` restarts the server when LHP's own source changes and matters only
when you are developing LHP itself. The full flag list is generated in the CLI
reference.

Edit, generate, and see the result
==================================

Edit any project YAML file in the editor. It validates against LHP's JSON
schemas as you type, marks syntax and schema errors inline, and re-validates on
every save. A save always persists, even when the YAML is invalid — the inline
markers tell you what to fix. Generated code under ``generated/`` is read-only
in the IDE: you browse it, you never hand-edit it.

Alongside the editor, the workspace shows the two things the terminal loop makes
you reconstruct by hand: the **dependency graph** of your flowgroups, redrawn as
you save, and the **generated Lakeflow code** beside the YAML that produced it.

Run ``validate`` and ``generate`` from the browser:

1. Pick an environment, and optionally a single pipeline.
2. Start a ``validate`` or ``generate`` run. Output streams live, with a
   problems panel listing every error and warning.
3. Generated code lands in ``generated/<env>/`` — the same location
   ``lhp generate`` writes to. The IDE is not a second runtime; it drives the
   same generator.

.. note::

   With no pipeline configuration selected, IDE generation is a code-only
   preview: it runs with Declarative Automation Bundles support off, so it
   writes ``generated/<env>/`` but no bundle resource files. For a deployable
   bundle build, select a pipeline configuration for runs in the IDE, or run
   ``lhp generate`` from the CLI.

Start from an empty directory
=============================

To scaffold a brand-new project without running ``lhp init`` first, launch with
``--allow-empty`` in an empty directory:

.. code-block:: bash

   lhp web --allow-empty

Without the flag, ``lhp web`` refuses to start outside an initialized project.
With it, the IDE detects the missing ``lhp.yaml`` and serves an init wizard that
scaffolds the same project layout as ``lhp init``. It does not create a git
repository.

What the IDE does not do
========================

The IDE is a local, single-user workbench, and its scope is deliberate:

- **No git integration.** Commit, branch, and diff with your normal git tooling.
- **No Databricks deployment or execution.** The IDE never connects to a
  workspace. Deploy generated code with Declarative Automation Bundles from the
  CLI or CI.
- **No integrated terminal, no multi-user or hosted mode.** The server binds
  ``127.0.0.1`` with no option to listen on a network interface.
- **No accounts.** The per-session URL token is the only access control.

What's next
===========

- **See every flag.** The full, generated ``lhp web`` option list lives in the
  CLI reference.
- **Chat with the AI assistant.** The IDE includes an assistant panel that
  answers questions about your project and edits its files in place — covered in
  its own develop guide.
- **Develop against a shared environment.** The sandbox workflow lets you build
  in parallel without colliding with teammates.
- **Deploy what you generate.** Bundle-enabled generation and deployment are
  covered in the bundle configuration guide — the step the IDE's preview
  generation deliberately skips.
