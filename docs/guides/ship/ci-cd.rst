=================================
Validate and ship pipelines in CI
=================================

.. meta::
   :description: Gate Lakehouse Plumber flowgroups in CI — validate on every pull request, fail the build when generated code drifts from its YAML, and deploy the bundle on merge with the databricks CLI.

Lakehouse Plumber compiles your flowgroups into ordinary Lakeflow Python and
bundle YAML — code you commit right next to the YAML that produced it. That
changes what CI can do for a pipeline. A pull request that edits a flowgroup is
a code change like any other, so CI can gate it like any other: resolve every
token, validate the actions, regenerate, confirm the committed output still
matches, and deploy the bundle when the change merges.

The alternative is to paste notebooks into a workspace and read them by eye
before a run. Because your pipelines are declarative YAML that compiles to code
under version control, you skip that: **gate your ETL in CI, don't hand-check
it.**

Let's build a GitHub Actions workflow that validates flowgroups on every pull
request, fails the build if the committed Python has drifted from its YAML, and
deploys the bundle to ``dev`` on merge to ``main``.

Before you start
================

This guide assumes a **bundle project**: a ``databricks.yml`` at the project
root. Its presence is what turns on bundle support, and it changes two commands
below — ``lhp validate`` and ``lhp generate`` both need a pipeline config on a
bundle project.

You need:

- One substitution file per environment (``substitutions/dev.yaml``) and a
  pipeline config (``config/pipeline_config.yaml``). The pipeline config supplies
  the catalog and schema stamped into each bundle resource.
- The generated code committed to the repository. The drift check compares
  regenerated output against what is committed, so ``generated/`` and
  ``resources/lhp/`` must be tracked — not ignored.
- The Databricks CLI and workspace credentials available in the runner, for the
  deploy step only.

Validate the flowgroups
=======================

The first gate resolves every token and runs the structural and preflight
checks. It is a local operation — no Databricks credentials — so it belongs on
every pull request:

.. code-block:: bash

   $ lhp validate --env dev --pipeline-config config/pipeline_config.yaml

``lhp validate`` runs the same preflight and structural checks as
``lhp generate``. On a bundle project it needs the same pipeline config: pass
``-pc`` / ``--pipeline-config`` (or ``--no-bundle`` if you are not using
bundles), or it stops before validating with ``LHP-CFG-023``. Any failure — an
unresolved token, a bad action, a missing config — exits non-zero, which fails
the CI step and stops the workflow before it can deploy. Add ``--strict`` to
treat warnings as failures too.

.. note::

   ``lhp validate`` and ``lhp generate`` exit ``0`` on success and ``1`` on a
   domain error — a failed validation, a bad config, an unresolved token.
   (``2`` means you passed a bad option; ``3`` is an internal bug.) A non-zero
   exit is all CI needs: the failing step halts the job. The CLI reference lists
   the ``LHP-`` error codes behind each failure.

Catch generated-code drift
==========================

Because the generated Python is committed, CI can prove it is still in sync with
its source. Regenerate, then ask git whether anything changed:

.. code-block:: bash

   $ lhp generate --env dev --pipeline-config config/pipeline_config.yaml
   $ git diff --exit-code -- generated resources/lhp

Generation is deterministic — the same YAML and substitutions produce
byte-identical output — so if the committed files already match, ``git diff
--exit-code`` finds nothing and returns ``0``. If someone edited a flowgroup
without regenerating, or hand-edited the generated Python, the diff is non-empty
and ``git diff --exit-code`` returns non-zero, failing the step. This is the
guardrail that keeps the committed code honest: what is in the repo is always
what the YAML produces.

.. note::

   ``lhp diff --env dev --exit-code`` does the same check in one command — it
   plans every flowgroup in memory, compares the result to the on-disk
   ``generated/dev`` tree, and exits ``1`` on any difference, without
   regenerating or touching git. Run it locally to preview a change before you
   commit.

Deploy on merge
===============

Everything above runs without touching a workspace. Deployment is where the
Databricks CLI takes over, and it should run only on the trunk — after a change
has merged to ``main``:

.. code-block:: bash

   $ lhp generate --env dev --pipeline-config config/pipeline_config.yaml
   $ databricks bundle deploy --target dev

Regenerate so the workspace gets exactly the current YAML, then ``databricks
bundle deploy`` uploads the Python and registers the pipelines. The bundle
target ``dev`` pairs with ``substitutions/dev.yaml`` — the same environment name
lives on both sides.

.. note::

   The deploy step is the only one that authenticates. Use a service principal
   with deploy permission on the target workspace, provided through the
   Databricks CLI's environment variables (``DATABRICKS_HOST`` and OAuth
   client credentials). The ``databricks/setup-cli`` action installs the CLI on
   a GitHub runner; see the Databricks CLI docs for OIDC and service-principal
   auth.

The whole workflow
==================

Put the three gates together. Validate and the drift check run on every pull
request; the deploy runs only on push to ``main``:

.. code-block:: yaml
   :caption: .github/workflows/lhp-ci.yml

   name: LHP CI

   on:
     pull_request:
       branches: [main]
     push:
       branches: [main]

   jobs:
     check:
       if: github.event_name == 'pull_request'
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v4
         - uses: actions/setup-python@v5
           with:
             python-version: '3.12'
         - run: pip install lakehouse-plumber
         - run: lhp validate --env dev --pipeline-config config/pipeline_config.yaml
         - run: lhp generate --env dev --pipeline-config config/pipeline_config.yaml
         - run: git diff --exit-code -- generated resources/lhp

     deploy:
       if: github.event_name == 'push' && github.ref == 'refs/heads/main'
       runs-on: ubuntu-latest
       env:
         DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
         DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID }}
         DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}
       steps:
         - uses: actions/checkout@v4
         - uses: actions/setup-python@v5
           with:
             python-version: '3.12'
         - uses: databricks/setup-cli@main
         - run: pip install lakehouse-plumber
         - run: lhp generate --env dev --pipeline-config config/pipeline_config.yaml
         - run: databricks bundle deploy --target dev

The ``check`` job needs no Databricks credentials — validate, generate, and the
git diff are all local. Only ``deploy`` authenticates. Azure DevOps, GitLab, and
Bitbucket follow the same shape: install, validate, drift-check, deploy. The
``lhp`` commands and exit codes are identical across platforms; only the runner
syntax and the auth wiring differ.

What you just did
=================

A flowgroup change now runs through the same gates as any code change. CI
resolves every token and validates the actions, proves the committed Python
still matches its YAML, and ships the bundle on merge — no one opens a notebook
to check by hand. The declarative source and its generated output travel
together through the pipeline, and the drift check keeps them in step.

What's next
===========

- **Promote across environments.** The same three commands ship to any target:
  point ``--env`` and ``--target`` at ``uat`` or ``prod`` against a matching
  ``substitutions/<env>.yaml``. One commit, deployed many times.
- **Preview before you commit.** ``lhp diff --env dev`` prints the ``~`` / ``+``
  / ``-`` lines a regenerate would change on disk — the local counterpart to the
  CI drift check.
- **Every validate and generate flag.** ``--strict``, ``--include-tests``,
  ``--pipeline``, ``--no-bundle``, and the rest are in the CLI reference.
