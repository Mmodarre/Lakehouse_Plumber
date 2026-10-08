lhp.yaml: project configuration
===============================

.. meta::
   :description: All lhp.yaml project fields: discovery, required LHP version, formatting, monitoring, metadata, tags, test reporting, wheels and sandbox settings.

**File:** ``lhp.yaml`` at the project root. This page lists every project-level
field. Larger feature blocks link to their complete reference.
For setup instructions, see :doc:`/build/configure`.

Minimal example
---------------

.. code-block:: yaml

   name: my_project
   version: "1.0"

Fields
------

.. list-table::
   :header-rows: 1
   :widths: 27 16 22 35

   * - Field
     - Type
     - Default when omitted
     - Meaning and complete contract
   * - ``name``
     - string
     - ``unnamed_project`` in the file loader
     - Project name. Set it explicitly; generated monitoring and master-job names use it.
   * - ``project_id``
     - string or null
     - unset
     - Project identity. See :doc:`telemetry </reference/telemetry>` for creation, hashing and opt-out behaviour.
   * - ``version``
     - string
     - ``"1.0"``
     - Project version metadata; use ``required_lhp_version`` to constrain the installed tool.
   * - ``description``
     - string or null
     - unset
     - Project description.
   * - ``author``
     - string or null
     - unset
     - Author metadata.
   * - ``created_date``
     - string or null
     - unset
     - Creation-date metadata.
   * - ``include``
     - list[string]
     - All YAML flowgroup files under ``pipelines/``
     - Flowgroup discovery patterns; see `Discovery`_.
   * - ``blueprint_include``
     - list[string]
     - ``blueprints/**/*.yaml`` and ``blueprints/**/*.yml``
     - Blueprint discovery patterns relative to the project root.
   * - ``instance_include``
     - list[string]
     - ``pipelines/**/*.yaml`` and ``pipelines/**/*.yml``
     - Blueprint-instance discovery patterns relative to the project root.
   * - ``operational_metadata``
     - mapping
     - Built-in column definitions
     - :doc:`Column definitions and selection <operational-metadata>`. A definition does not select a column for every action.
   * - ``event_log``
     - mapping
     - No event-log injection
     - :doc:`Per-pipeline event logs <monitoring>`. ``enabled`` defaults to true inside a supplied block.
   * - ``monitoring``
     - mapping
     - No monitoring generation
     - :doc:`Consolidated event-log monitoring <monitoring>`. Requires enabled event logs.
   * - ``required_lhp_version``
     - string or null
     - No version constraint
     - PEP 440 version specifier; see `Required tool version`_.
   * - ``test_reporting``
     - mapping
     - No reporting provider
     - :doc:`Provider module, function and configuration <test-reporting>`.
   * - ``uc_tagging``
     - mapping
     - Tagging enabled for declared tags
     - :doc:`Unity Catalog tag-hook settings <uc-tagging>`.
   * - ``wheel``
     - mapping
     - No artifact volume
     - ``wheel.artifact_volume``: optional string naming the UC volume for wheel artifacts. See :doc:`bundle` and :doc:`/guides/deploy/package-as-wheels`.
   * - ``sandbox``
     - mapping
     - Default sandbox policy
     - :doc:`Team policy <sandbox>`; personal choices belong in ``.lhp/profile.yaml``. Active when running with ``--sandbox``.
   * - ``apply_formatting``
     - boolean
     - ``true``
     - Format generated Python. See `Formatting`_.

Discovery
---------

``include`` patterns filter flowgroup files relative to ``pipelines/``.
Blueprint and instance patterns are relative to the project root. Pattern lists
must contain strings; empty lists use the default discovery behaviour.

.. code-block:: yaml

   include:
     - bronze/**/*.yaml
     - silver/**/*.yaml
   blueprint_include:
     - blueprints/**/*.yaml
   instance_include:
     - pipelines/tenants/**/*.yaml

Blueprint instance documents are expanded through their blueprint; they are
not parsed as ordinary flowgroups. See :doc:`blueprints`.

.. _required-lhp-version:

Required tool version
---------------------

.. code-block:: yaml

   required_lhp_version: ">=0.9.3,<1.0"

LHP checks the installed version against the specifier during project loading
for orchestration. An unsatisfied constraint raises ``LHP-CFG-007``; an invalid
specifier raises ``LHP-CFG-008``. ``LHP_IGNORE_VERSION=1`` bypasses this check
and emits a warning; use a compatible installed version for normal operation.

.. _apply-formatting:

Formatting
----------

``apply_formatting: false`` skips the terminal formatting pass over generated
Python. ``lhp generate --no-format`` overrides a project value of ``true``.
The generated-Python syntax check still runs in both cases. See
:doc:`/reference/cli` for the full command options.
