CLI Reference
=============

.. meta::
   :description: Command-line reference for lhp: generate, validate, deps, show, stats, and all available options.

The **Lakehouse Plumber** command-line interface provides project creation,
validation, code generation and more.  All commands are implemented with
`click <https://click.palletsprojects.com>`_ so you can use the usual
``--help`` flags.

.. click:: lhp.cli.main:cli
   :prog: lhp
   :nested: full

.. seealso::

   For detailed information about dependency analysis and orchestration job generation, see :doc:`configure_bundles`.

Project Initialization
----------------------

The ``lhp init`` command creates a new LakehousePlumber project with the complete directory structure and configuration templates.

Basic Usage
~~~~~~~~~~~

.. code-block:: bash

   # Create a standard project (Declarative Automation Bundles support is on by default)
   lhp init my_project

   # Create a project without bundle support
   lhp init my_project --no-bundle

   # Scaffold the ready-to-run TPC-H sample project
   lhp init my_demo --sample

**Created Directory Structure:**

.. code-block:: text

   my_project/
   ├── config/                    # Configuration templates
   │   ├── job_config.yaml.tmpl
   │   └── pipeline_config.yaml.tmpl
   ├── pipelines/                 # Pipeline flowgroups
   ├── templates/                 # Reusable action templates
   ├── presets/                   # Configuration presets
   ├── substitutions/            # Environment-specific values
   ├── schemas/                  # Table schema definitions
   ├── expectations/             # Data quality expectations
   ├── generated/                # Generated Python code (gitignored)
   └── resources/                # Bundle resources (omitted with --no-bundle)

Sample Project (``--sample``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``lhp init <name> --sample`` scaffolds a ready-to-run TPC-H sample project
instead of the empty skeleton. The sample builds a bronze → silver → gold
medallion architecture against the read-only ``samples.tpch`` dataset
available on every Unity Catalog workspace, and exercises the full LHP
feature surface: delta and Auto Loader (``cloudfiles``) bronze ingestion
driven by a reusable template, both Change Data Capture (CDC) flavors in
silver (streaming CDC with ``apply_as_deletes`` and snapshot CDC), all four
transform types, a gold materialized view from an external SQL file
(``sql/sales_by_nation.sql``), presets, every substitution syntax, and
operational metadata. A data-prep notebook (``notebooks/data_prep.py``) seeds
the source data, and a hand-authored Declarative Automation Bundles job
(``resources/sample_job.yml``) orchestrates the notebook and the three
pipelines.

The sample requires Declarative Automation Bundles support: combining
``--sample`` with ``--no-bundle`` is a usage error (exit code ``2``).

The sample ships a ready ``config/pipeline_config.yaml`` (no ``.tmpl``
suffix). Because the project is bundle-enabled, pass it on every
``lhp validate`` and ``lhp generate`` run:

.. code-block:: bash

   lhp validate --env dev -pc config/pipeline_config.yaml
   lhp generate --env dev -pc config/pipeline_config.yaml

For the guided walkthrough — deploy, run, and re-run the demo — see
:doc:`tutorials/sample_quickstart`.

Configuration Templates
~~~~~~~~~~~~~~~~~~~~~~~

The ``config/`` directory contains template files for customizing Databricks job and pipeline settings:

**job_config.yaml.tmpl**

Template for customizing orchestration job configuration used with ``lhp deps`` command:

- Job execution settings (max_concurrent_runs, timeout, performance_target)
- Queue configuration
- Email and webhook notifications
- Scheduling (Quartz cron expressions)
- Tags and permissions

**Usage:**

.. code-block:: bash

   # 1. Copy and customize the template
   cd my_project
   cp config/job_config.yaml.tmpl config/job_config.yaml
   # Edit config/job_config.yaml with your settings

   # 2. Use with lhp deps command
   lhp deps --job-config config/job_config.yaml --bundle-output

See :doc:`bundle_config_reference` for detailed job configuration options.

**pipeline_config.yaml.tmpl**

Template for customizing Databricks Lakeflow Declarative Pipelines settings
used with ``lhp generate`` command:

- ``catalog`` and ``schema`` — required for bundle projects, defined either in
  ``project_defaults`` (project-wide) or per pipeline. See
  :doc:`configure_catalog_schema` for resolution rules.
- Compute configuration (serverless vs. classic clusters)
- Pipeline edition and runtime channel
- Processing mode (continuous vs. triggered)
- Notifications and tags
- Event logging (can also be configured project-wide in ``lhp.yaml`` without requiring ``-pc``)

**Usage:**

.. code-block:: bash

   # 1. Copy and customize the template
   cp config/pipeline_config.yaml.tmpl config/pipeline_config.yaml
   # Edit config/pipeline_config.yaml with your settings (catalog, schema, …)

   # 2. Pass the path on every generate invocation
   lhp generate -e dev --pipeline-config config/pipeline_config.yaml

The ``--pipeline-config`` (``-pc``) flag tells LHP where to find the file that
defines ``catalog`` and ``schema``. Bundle projects require it on every
``lhp generate`` run; without it, every pipeline fails fast with
``LHP-CFG-023`` pointing at :doc:`configure_catalog_schema`.

See :doc:`bundle_config_reference` for detailed pipeline configuration options
and :doc:`configure_catalog_schema` for the catalog/schema resolution order.

.. note::
   Template files (\*.tmpl) are provided as starting points. Copy and remove the
   ``.tmpl`` extension to activate them. This allows you to keep the original
   templates as reference while customizing your own versions.

Regeneration behavior
~~~~~~~~~~~~~~~~~~~~~

.. deprecated::
   The ``--force`` flag on ``lhp generate`` is retained
   for backwards compatibility but is a no-op. Every ``lhp generate`` run
   regenerates Python output unconditionally and wipes ``resources/lhp/``
   before rewriting one ``.pipeline.yml`` per pipeline.

**Behavior:**

- Bare ``lhp generate -e <env>``: regenerates Python code under
  ``generated/``. Skip bundle sync with ``--no-bundle``.
- ``lhp generate -e <env> --pipeline-config <file>``: regenerates Python code
  and rewrites every file under ``resources/lhp/`` from the pipeline config.
- Files outside ``resources/lhp/`` are never touched, with one exception: the
  monitoring job YAML at ``resources/<name>.job.yml``, identified by its
  ``# Generated by LakehousePlumber - Monitoring Job`` header.

.. note::
   The ``lhp skill install --force`` flag is unrelated and remains active —
   it overwrites an existing skill install.

Inspect a generated wheel
-------------------------

When a pipeline uses ``packaging: wheel`` (see
:doc:`package_pipelines_as_wheels`), ``lhp generate`` emits a single
content-addressed wheel instead of loose ``.py`` files. ``lhp inspect-wheel``
reads that wheel and either lists its Python modules (the default) or extracts
them with ``--extract DIR``. It is read-only and never modifies the wheel.

.. code-block:: bash

   # List the .py modules in a pipeline's built wheel (a pipeline name needs -e)
   lhp inspect-wheel my_pipeline -e prod

   # List by an explicit path to the .whl
   lhp inspect-wheel generated/prod/_wheels/my_pipeline/dist/*.whl

   # Extract the .py modules to a directory, preserving in-wheel structure
   lhp inspect-wheel my_pipeline -e prod --extract /tmp/my_pipeline

The ``SELECTOR`` argument is read as a **wheel path** when it ends in ``.whl`` or
contains a path separator, and otherwise as a **pipeline name**. A pipeline name
requires ``-e/--env`` so LHP can resolve the single hashed wheel under
``generated/<env>/_wheels/<pipeline>/dist/``; ``--env`` is ignored when a path is
given.

**Exit codes:** ``0`` on success; ``2`` for a usage error (a pipeline name without
``--env``, or an ``--extract`` target that is an existing file); and ``1`` for a
wheel-level failure — the wheel is missing (``LHP-IO-022``), is not a wheel file
(``LHP-IO-023``), or is corrupt (``LHP-IO-024``); a pipeline name matches zero or
several wheels (``LHP-GEN-001``); or the ``--extract`` directory is not writable
(``LHP-IO-005``).

Skill Management
----------------

LHP ships a Claude Code skill (``SKILL.md`` + reference markdown) that teaches Claude
how to author LHP YAML configurations. The ``lhp skill`` command group installs and
maintains this skill in your project (or your user-global Claude Code directory).

Subcommands
~~~~~~~~~~~

.. code-block:: bash

   # Install the skill into <cwd>/.claude/skills/lhp/ and write the
   # routing block into <cwd>/CLAUDE.md
   lhp skill install

   # Install for the current user (~/.claude/skills/lhp/)
   lhp skill install --user

   # Overwrite an existing install
   lhp skill install --force

   # Update an existing install to the current LHP version
   lhp skill update

   # Show installed version, current LHP version, and any drift
   lhp skill status

   # Remove the install (prompts for confirmation)
   lhp skill uninstall

   # Remove without prompting
   lhp skill uninstall --force

Behavior
~~~~~~~~

- ``install`` errors if a skill is already present unless ``--force`` is given.
- ``update`` errors if no install is found (use ``install`` instead). The skill is
  always overwritten — local modifications under ``.claude/skills/lhp/`` will be
  replaced.
- ``status`` reports one of: not installed, up-to-date, update available,
  installed-newer-than-CLI (suggests ``pip install -U lakehouse-plumber``), or
  foreign-install (no marker file — suggests ``install --force``).
- An ``.lhp_skill_version`` marker file inside the install directory tracks which
  LHP version produced the content; this file is the source of truth for
  ``status`` and ``update``.
- A project install also writes an LHP routing block into ``<cwd>/CLAUDE.md``
  (creating the file if absent, refreshing it on ``update``). The block tells
  Claude Code that the directory is an LHP project, so requests phrased without
  LHP-specific vocabulary still route to the skill. ``uninstall`` strips the
  block again, preserving any other content you added to ``CLAUDE.md``. A
  ``--user`` install is global and never touches a project ``CLAUDE.md``.

When to use ``--user``
~~~~~~~~~~~~~~~~~~~~~~

Use ``--user`` when you want the skill available across every project on your
machine. The default project-scoped install is best when each project might be
pinned to a different LHP version.

By default, Lakehouse Plumber skips test actions during code generation for faster builds and cleaner production pipelines. Use the ``--include-tests`` flag to include data quality tests when needed.

**Common Usage Patterns:**

.. code-block:: bash

   # Production builds (default) - fast, clean pipelines
   lhp generate -e prod
   lhp generate -e dev

   # Development with data quality testing
   lhp generate -e dev --include-tests

   # Validate without test actions (default)
   lhp validate -e dev

   # Validate with test actions
   lhp validate -e dev --include-tests

**Flag Behavior:**

Both commands respect ``--include-tests`` for per-flowgroup processing.

- **Without flag**: Test actions are skipped during per-flowgroup processing in both validation and generation
- **With flag**: Test actions are included — validated for configuration errors and generated as temporary DLT tables

.. note::
   ``lhp validate`` shares ``lhp generate``'s bundle-preflight flags. It accepts
   ``--no-bundle`` and ``--pipeline-config`` / ``-pc``, and on a project that
   contains ``databricks.yml`` it likewise **requires** ``-pc`` (or
   ``--no-bundle``) — otherwise it fails with ``LHP-CFG-023``, exactly like
   ``generate``. ``validate`` runs the same structural and bundle catalog/schema
   preflight checks as ``generate``, and runs cross-flowgroup conflict detection
   on the **resolved** flowgroups (after presets, templates, and substitutions),
   so a conflict introduced by resolution — e.g. a template that expands into
   two ``create_table: true`` actions targeting the same table — fails
   ``validate``, not just ``generate``. See :doc:`configure_catalog_schema`.

**Examples:**

.. code-block:: bash

   # Skip tests for faster CI/CD builds
   lhp generate -e prod --dry-run

   # Include tests for comprehensive validation
   lhp generate -e dev --include-tests

   # Preview test generation without writing files
   lhp generate -e dev --include-tests --dry-run

.. note::
   Test actions generate temporary DLT tables that persist test results for inspection and debugging while being automatically cleaned up when the pipeline completes.

.. tip::
   When ``test_reporting`` is configured in ``lhp.yaml``, the ``--include-tests``
   flag also generates a test reporting event hook that publishes DQ results to
   external systems. See :doc:`actions/test_reporting`.
