Requirements
============

.. meta::
   :description: System, workspace, and editor requirements for Lakehouse Plumber (LHP), plus optional dependencies for Asset Bundle deployment and CI/CD.

Prerequisites to install and use Lakehouse Plumber (LHP). Facts here mirror
``pyproject.toml`` for LHP ``0.8.6``.

System requirements
-------------------

Python
~~~~~~

LHP requires **Python 3.11 or later**. Tested against:

============== ===========
Python version Status
============== ===========
3.11           Supported
3.12           Recommended
3.13           Supported
============== ===========

``pip install lakehouse-plumber`` on Python 3.10 or earlier fails the
``requires-python`` check.

Operating system
~~~~~~~~~~~~~~~~

LHP runs on Linux, macOS, and Windows. CI exercises Linux and macOS on Python
3.12, plus import smoke tests across Python 3.11, 3.12, and 3.13 on all three
platforms.

Runtime dependencies
~~~~~~~~~~~~~~~~~~~~

``pip install lakehouse-plumber`` pulls in the libraries pinned in
``pyproject.toml``:

* ``click >= 8.3.0`` — CLI framework.
* ``pyyaml >= 6.0.3``, ``ruamel.yaml >= 0.19.0`` — YAML parsing.
* ``jinja2 >= 3.0.0`` — template rendering.
* ``pydantic >= 2.12.0`` — configuration validation.
* ``rich >= 14.0.0`` — formatted CLI output.
* ``networkx >= 3.6.0`` — dependency graph for ``lhp deps``.
* ``packaging >= 23.2`` — version parsing for ``required_lhp_version``.
* ``ruff == 0.13.3`` — formats generated Python before write.

Pinning ``required_lhp_version`` in ``lhp.yaml`` (see :doc:`cicd`) locks this
set transitively.

Databricks workspace requirements
---------------------------------

LHP generates code, not infrastructure. The target workspace needs:

* **Unity Catalog enabled.** Generated pipelines write three-part names
  (``catalog.schema.table``). Hive metastore-only workspaces are unsupported.
* **Write access to one catalog and schema.** Their names populate
  ``substitutions/<env>.yaml`` as ``${catalog}`` and ``${bronze_schema}``.
* :term:`Lakeflow Declarative Pipeline` **available.** Serverless is the default for
  bundles produced by ``lhp init``; set ``serverless: false`` in
  ``pipeline_config.yaml`` for classic compute.

LHP does not pin a Databricks Runtime version. The generated
``from pyspark import pipelines as dp`` import requires whatever Lakeflow
Declarative Pipelines version Databricks ships with the ``pipelines`` module
— match your workspace defaults.

Some sovereign-cloud workspaces (GovCloud, China) do not provision the
``samples`` catalog used by :doc:`quickstart`. Use your own landing volume
instead.

Editor requirements
-------------------

LHP ships JSON schemas for every YAML file it consumes. Editor support is
optional but recommended — it surfaces invalid YAML before ``lhp validate``
runs.

================ ==========================================================
Editor           Status
================ ==========================================================
VS Code          Supported; ``lhp init`` writes ``.vscode/settings.json``.
Cursor           Supported (VS Code-compatible).
JetBrains IDEs   Manual setup; map YAML schemas to ``schemas/``.
Other editors    Any editor with JSON Schema support over YAML works.
================ ==========================================================

For VS Code and Cursor, install the **YAML** extension by Red Hat. ``lhp init``
generates ``.vscode/settings.json`` and ``.vscode/schemas/`` — IntelliSense,
autocomplete, and hover docs work without further setup. See
:doc:`editor_setup` for manual configuration.

Optional dependencies
---------------------

Asset Bundle deployment
~~~~~~~~~~~~~~~~~~~~~~~

Deploying generated bundles requires the **Databricks CLI** on the machine
running ``databricks bundle deploy``:

* CLI version 0.205 or later (Asset Bundle support).
* Authentication via OAuth, a personal access token, or a service principal.
  The ``databricks/setup-cli`` GitHub Action installs the CLI in CI runners.

LHP does not invoke the Databricks CLI itself — ``lhp generate`` produces
bundle YAML; you call ``databricks bundle deploy`` separately. See
:doc:`configure_bundles`.

CI/CD
~~~~~

For pipelined deployment from a runner:

* A Databricks service principal with deploy permissions on each target
  workspace.
* OIDC trust (recommended) or a stored client secret.
* Python 3.11+ in the runner image. Examples in :doc:`cicd` use Python 3.12
  on ``ubuntu-latest``.

Development
~~~~~~~~~~~

To develop LHP itself, install the ``dev`` extra
(``pip install -e ".[dev]"``). Pulls in ``pytest``, ``pytest-cov``,
``pytest-mock``, ``ruff``, ``mypy``, ``pre-commit``, plus
security tooling (``pip-audit``, ``bandit``, ``liccheck``).

See also
--------

* :doc:`quickstart` — Build a first pipeline against a workspace that meets
  these requirements.
* :doc:`configure_bundles` — Enable Asset Bundle integration.
* :doc:`editor_setup` — Configure VS Code, Cursor, or JetBrains IDEs for YAML
  IntelliSense.
* :doc:`cicd` — Promote bundles across ``dev``, ``uat``, and ``prod``.
