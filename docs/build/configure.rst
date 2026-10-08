Configure your project
======================

Choose the file by the scope of the change. Feature guides and filename lookup
link to the same reference definitions.

.. list-table::
   :header-rows: 1
   :widths: 35 30 35

   * - I want to change
     - Where it belongs
     - Complete reference
   * - Project discovery, formatting or feature switches
     - ``lhp.yaml``
     - :doc:`/reference/config/project`
   * - Actions, flowgroup variables or template use
     - ``pipelines/**/*.yaml``
     - :doc:`/reference/config/flowgroups`
   * - Environment values or secret aliases
     - ``substitutions/<env>.yaml``
     - :doc:`/reference/config/substitutions`
   * - Pipeline catalog, schema, compute or packaging
     - ``config/pipeline_config.yaml``
     - :doc:`/reference/config/pipeline-config`
   * - A particular output table's namespace
     - ``write_target.catalog`` and ``write_target.schema``
     - :doc:`/reference/actions/write`
   * - Job schedules and orchestration defaults
     - The file passed to ``lhp dag --job-config``
     - :doc:`/reference/config/jobs`
   * - Monitoring tables and collection
     - ``lhp.yaml`` plus the monitoring job file
     - :doc:`/reference/config/monitoring`

Pipeline catalog/schema settings configure the Databricks pipeline resource.
Write-target catalog/schema settings identify the table emitted by an action.
Use environment tokens to keep these choices consistent across development and
production; verify both in the generated output.

.. toctree::
   :maxdepth: 1

   Environments, substitutions and secrets </guides/reuse-and-scale/substitutions-and-secrets>
   How environment substitution works </concepts/substitution-and-envs>
