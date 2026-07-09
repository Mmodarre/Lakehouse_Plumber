Develop in the local web IDE
============================

.. meta::
   :description: How to launch and use the Lakehouse Plumber local web IDE (lhp web) to browse, edit, validate, and generate SDP pipeline configurations from your browser, on your own machine.

This how-to launches the Lakehouse Plumber (LHP) local web IDE: ``lhp web``
serves a browser-based, single-user workbench for one LHP project on your own
machine. In it you browse pipelines, FlowGroups, tables, Presets, Templates,
Blueprints, per-environment substitutions, and the dependency graph; edit YAML
with schema-aware validation; stream ``validate`` and ``generate`` runs live —
producing the same Lakeflow Spark Declarative Pipelines (SDP) code as the
CLI — and review a recorded run history. The server never leaves
``127.0.0.1``.

.. versionadded:: 0.9.1

Requirements
------------

* The optional webapp dependencies: ``pip install "lakehouse-plumber[webapp]"``
  (adds FastAPI and uvicorn). Without them, ``lhp web`` fails with
  ``LHP-IO-026``.
* An initialized LHP project — or an empty directory plus the
  ``--allow-empty`` flag, which serves an in-app init wizard instead (see
  below).
* A web browser. The browser app ships pre-built inside the published
  package; Node.js is not required.

Launch the IDE
--------------

1. From anywhere inside your project, run:

   .. code-block:: bash

      lhp web

   The command prints the IDE's address and opens your browser once the
   server answers its health probe:

   .. code-block:: text

      Starting Lakehouse Plumber web IDE at http://127.0.0.1:8000/#token=<session-token>

2. If the browser does not open — or you passed ``--no-open`` — click the
   printed URL. The ``#token=`` fragment carries a per-session access token:
   every API request except the health check requires it, and a fresh token
   is minted on every launch, so enter through the printed URL rather than a
   bookmark.

3. If port ``8000`` is taken, the launch fails with ``LHP-IO-027``; pick
   another port:

   .. code-block:: bash

      lhp web --port 8001

The server binds ``127.0.0.1`` only — there is no flag to expose it on a
network interface. ``--reload`` restarts the server when LHP's own source
changes and matters only when developing LHP itself. The complete flag list
is in the :doc:`cli` reference.

Create a project from the browser
---------------------------------

To start a brand-new project without running ``lhp init`` first:

1. Run ``lhp web --allow-empty`` in an empty directory. Without the flag,
   ``lhp web`` refuses to start outside an initialized project.
2. The IDE detects the missing ``lhp.yaml`` and serves the init wizard.
3. Name the project and choose whether to enable Declarative Automation
   Bundles support (on by default). The wizard scaffolds the same project
   layout as ``lhp init``; it does not initialize a git repository.

See :doc:`quickstart` for what the scaffolded project contains.

Edit, validate, and generate
----------------------------

Edit any project YAML file in the IDE's editor. The editor validates against
LHP's canonical JSON schemas as you type, marks syntax errors inline, and
re-validates on every save. A save always persists, even when the YAML is
invalid — the inline markers tell you what to fix.

The IDE creates, edits, and deletes files anywhere in the project except the
write-protected paths ``.git/``, ``generated/``, ``.lhp/logs/``, and
``.lhp/dependencies/``.

Run validation and generation from the Validation page:

1. Pick the environment, and optionally a single pipeline.
2. Start a ``validate`` or ``generate`` run. Output streams live, with
   progress and a Problems panel listing every error and warning.
3. Generated code lands in ``generated/<env>/`` — the same location
   ``lhp generate`` writes to.

.. important::

   IDE generation is a code-generation preview: it always runs with
   Declarative Automation Bundles support disabled, so it neither runs the
   bundle preflight nor writes bundle resource files. For a deployable build
   of a bundle-enabled project, run ``lhp generate`` from the CLI (see
   :doc:`configure_bundles`).

Combine the IDE with other tools
--------------------------------

The IDE follows the project on disk. A file watcher polls the project tree
every two seconds, so edits made outside the IDE — your desktop editor, a
``git checkout``, another terminal — appear in the IDE without a restart, and
open pages refresh themselves over a server-push event stream.

Saves are conflict-checked. Every save carries the version of the file the
editor last read; if the file changed in the meantime — a second browser tab,
or an on-disk edit between your read and your save — the stale save is
rejected instead of silently overwriting the newer content. Reload the file
and re-apply your change.

Review past runs
----------------

The Runs page lists every ``validate`` and ``generate`` run with its kind,
environment, status, timestamps, and result summary; open a run to see its
recorded issues and full output. History is stored in
``.lhp/webapp.db`` at the project root (the ``.lhp/`` directory is
gitignored) and pruned to the newest 100 runs and 30 days. A run interrupted
by a crash is marked failed at the next launch.

Switch themes
-------------

The IDE offers light, dark, and system themes. Your choice persists across
sessions and launches.

Chat with the AI assistant
--------------------------

The IDE includes an assistant panel: a chat dock backed by a locally
running omnigent daemon that answers questions about your project and edits
its files in place. The daemon is a separate, user-installed tool with a
one-time setup of its own — installation, startup, executor choice, and
troubleshooting are covered in :doc:`use_the_ai_assistant`.

What the web IDE does not do
----------------------------

The IDE is a local, single-user workbench, and its scope is deliberate:

* **No git integration.** Commit, branch, and diff with your normal git
  tooling; the file watcher keeps the IDE current after checkouts.
* **No Databricks deployment or execution.** The IDE never connects to a
  workspace. Deploy generated code with Declarative Automation Bundles from
  the CLI or CI (see :doc:`configure_bundles` and :doc:`cicd`).
* **No integrated terminal.**
* **No multi-user or hosted mode.** The server binds ``127.0.0.1`` with no
  option to listen on a network interface; it is not designed to be shared,
  reverse-proxied, or hosted.
* **No accounts.** The per-session URL token is the only access control.

Troubleshooting
---------------

* ``LHP-IO-026`` — the webapp dependencies are not installed. Run
  ``pip install "lakehouse-plumber[webapp]"`` and retry.
* ``LHP-IO-027`` — the chosen port is already in use. Pass ``--port`` with a
  free port, or stop the process holding it.

See :doc:`errors_reference` for both codes in context.

Related articles
----------------

* :doc:`cli` — the generated ``lhp web`` flag reference.
* :doc:`use_the_ai_assistant` — set up and chat with the AI assistant panel.
* :doc:`quickstart` — initialize and build your first project.
* :doc:`configure_bundles` — bundle-enabled generation and deployment, which
  the IDE's preview generation skips.
* :doc:`develop_in_a_sandbox` — parallel development against a shared
  environment.
* :doc:`errors_reference` — ``LHP-IO-026`` and ``LHP-IO-027`` resolution
  steps.
