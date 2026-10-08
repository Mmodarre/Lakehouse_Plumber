Configuration files
===================

Find the file you are editing. Feature and action references are also accessible
directly from :doc:`/reference/index`; each setting has one maintained definition.

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - File or surface
     - Complete reference
   * - ``lhp.yaml``
     - :doc:`project`
   * - ``pipelines/**/*.yaml``
     - :doc:`flowgroups`
   * - ``config/pipeline_config.yaml``
     - :doc:`pipeline-config`
   * - Orchestration ``job_config.yaml``
     - :doc:`jobs`
   * - ``config/monitoring_job_config.yaml``
     - :doc:`monitoring-job`
   * - ``substitutions/<env>.yaml``
     - :doc:`substitutions`
   * - ``presets/*.yaml``
     - :doc:`presets`
   * - ``templates/**/*.yaml``
     - :doc:`templates`
   * - ``blueprints/**/*.yaml`` and instance files
     - :doc:`blueprints`
   * - Table-schema and tag files; schema transforms
     - :doc:`schema-files`
   * - Expectation files and inline test expectations
     - :doc:`expectations`
   * - ``.lhp/profile.yaml``
     - :doc:`sandbox`
   * - ``databricks.yml``
     - :doc:`bundle`

Paths shown here are conventions; fields such as ``schema_file`` and CLI
options such as ``--job-config`` can select another project-relative location.

.. toctree::
   :maxdepth: 1
   :hidden:

   lhp.yaml <project>
   Flowgroup YAML <flowgroups>
   pipeline_config.yaml <pipeline-config>
   Orchestration job_config.yaml <jobs>
   Substitutions <substitutions>
   Presets <presets>
   Templates <templates>
   Blueprints and instances <blueprints>
   Schema and tag files <schema-files>
   Expectation formats <expectations>
   Databricks bundle integration <bundle>
