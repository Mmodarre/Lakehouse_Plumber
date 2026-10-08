Troubleshoot a pipeline
=======================

Start with the symptom, then use the full error title and context. Several LHP
error codes are used by more than one validation path, so the code alone may
not identify the cause. The :doc:`error catalogue </reference/errors>` has
further explanations.

A pipeline or flowgroup is missing
----------------------------------

Check ``pipeline`` and ``flowgroup`` names and the project's ``include``,
``blueprint_include`` and ``instance_include`` patterns. Check any CLI pipeline
filter. Run ``lhp validate --env dev`` with the same selection as generation;
in a bundle project, also pass ``-pc config/pipeline_config.yaml``.
See :doc:`/reference/config/project` and :doc:`/reference/config/flowgroups`.

A substitution token does not resolve
-------------------------------------

Check the selected ``--env``, the filename ``substitutions/<env>.yaml`` and the
mapping with that environment's name inside the file. Secret aliases live in
the top-level ``secrets`` mapping. Blueprint/local ``%{...}`` variables and
environment ``${...}`` tokens have different scopes. See
:doc:`/reference/config/substitutions` and :doc:`/reference/config/blueprints`.

Bundle generation asks for pipeline configuration
-------------------------------------------------

When ``databricks.yml`` exists, supply ``-pc config/pipeline_config.yaml`` to
both validation and generation. To generate without bundle resources, pass
``--no-bundle``. Check that each selected pipeline resolves a catalog and schema.
See :doc:`/develop/bundles` and :doc:`/reference/config/pipeline-config`.

Test actions did not appear
---------------------------

Use ``--include-tests`` when generating test actions. If results are not being
published, check ``test_reporting`` in ``lhp.yaml`` and the provider's function
and configuration file. Provider fields belong to the project; ``test_id``
belongs to an individual test action. See :doc:`/reference/config/test-reporting`
and :doc:`/guides/test/test-reporting`.

Monitoring tables are empty or the job fails
--------------------------------------------

Check that both ``event_log`` and ``monitoring`` are enabled, the source pipelines
have run and emitted logs, and the monitoring job was deployed and run. Check
the run-as identity's access to source logs, output tables and checkpoint storage.
Inspect the union-notebook task before the summary-pipeline task. Use
:doc:`/reference/config/monitoring` for names and inheritance and
:doc:`/reference/config/monitoring-job` for job settings.

Tags were not applied even though the update succeeded
------------------------------------------------------

Check ``tags`` or ``tags_file`` on the table-creating write action, project
``uc_tagging.enabled`` and the run-as identity's tag permissions. Inspect
``hook_progress`` failures in the pipeline event log. Tag-hook failures do not
fail the pipeline update. See :doc:`/reference/config/uc-tagging`.

A dependency is missing
-----------------------

Inspect the graph with :doc:`/guides/ops/dependency-analysis`. Add ``depends_on``
when a dependency cannot be inferred from source code, and check whether
``--trust-depends-on`` changes the analysis. See
:doc:`/reference/dependency-analysis` for matching and scope rules.
