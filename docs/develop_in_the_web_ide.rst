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

   Without a pipeline configuration selected for runs, IDE generation is a
   code-generation preview: it runs with Declarative Automation Bundles
   support disabled, so it neither runs the bundle preflight nor writes
   bundle resource files. For a deployable build of a bundle-enabled
   project, select a pipeline configuration (see
   `Use a pipeline configuration for runs`_) or run ``lhp generate`` from
   the CLI (see :doc:`configure_bundles`).

Edit configuration with forms
-----------------------------

The Config section edits LHP's three configuration surfaces as forms:

* **Project** — ``lhp.yaml``.
* **Pipelines** — ``config/pipeline_config*.yaml``: pipeline defaults and
  per-pipeline overrides.
* **Jobs** — ``config/job_config*.yaml`` and
  ``config/monitoring_job_config*.yaml``: orchestration job settings.

The Project tab always edits ``lhp.yaml``; the other two tabs pick a file
from ``config/``. If the file does not exist yet, create it in place from
the same templates ``lhp init`` scaffolds, named for an environment
(``config/pipeline_config_<env>.yaml``).

Form saves preserve the file as you wrote it. A save touches only the
lines you changed: hand-written comments and key order survive, and keys
the form does not own — ``run_as`` or ``trigger`` in a job document, for
example — appear as read-only passthrough chips and are written back
exactly as they appear on disk. Switching an optional section off removes
its key entirely, never leaving ``key: null`` behind. One caveat: some
edits — deleting an entry, for example — rewrite the containing document;
comments stay with their settings, but placement and spacing can shift.

The pipeline editor lists a file's documents in a rail ordered the way LHP
merges them: built-in defaults (read-only) → the ``project_defaults``
document → per-pipeline documents, each naming one pipeline
(``pipeline: bronze``) or a group (``pipeline: [bronze, silver]``) that
shares its settings. A pipeline appearing in more than one document gets a
duplicate badge on each — generation rejects the duplicate.

The job editor renders the keys LHP's job generator understands and passes
everything else through verbatim. A legacy single-document ``job_config``
file — read in full as project defaults — shows a banner offering an
explicit conversion to the multi-document layout; a save never converts
it. Files whose basename starts with ``monitoring_job_config`` are edited
as a single flat form instead. Do not convert a file referenced by
``monitoring.job_config_path`` in ``lhp.yaml``: monitoring job configs
stay single flat documents by design.

Config forms follow the page-wide conflict rules (see `Combine the IDE
with other tools`_): a stale save — form over raw-YAML edit, or the
reverse — is rejected with a reload dialog, and a banner flags on-disk
changes while a form holds unsaved edits. For anything the forms do not
cover, **Open raw YAML** opens the same file in the YAML editor, whose
schema hints cover ``lhp.yaml``, pipeline config, and job config files —
multi-document files validate per document.

Use a pipeline configuration for runs
-------------------------------------

IDE runs pass no pipeline configuration by default. To run with one — the
IDE equivalent of the CLI's ``--pipeline-config`` option — open the file
on the Pipelines tab and switch on **Use for runs**. A chip beside the
environment selector names the selection; clear it from the chip or the
toggle. The selection persists across sessions and stays on the file it
was enabled for — picking another file never silently changes what runs
use.

With a pipeline configuration selected, generation on a project with
``databricks.yml`` runs with Declarative Automation Bundles support
enabled: alongside ``generated/<env>/``, it wipes and regenerates the
bundle resource files under ``resources/lhp/``, the same as CLI
generation. Without one, IDE generation stays a code-only preview.

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

The IDE includes an assistant panel: a chat dock that answers questions
about your project and edits its files in place. The built-in provider
ships with the web IDE — nothing else to install — and signs in with your
Claude subscription or a Databricks workspace; a user-managed omnigent
daemon is selectable as an alternative. Setup, sign-in, and troubleshooting
are covered in :doc:`use_the_ai_assistant`.

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
