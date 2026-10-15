Reference
=========

.. meta::
   :description: Exact LHP syntax and options. Browse load, transform, write and test actions; feature configuration; or find configuration by filename.

Find the syntax you need while building. Start with an action, a feature or the
:doc:`configuration-file index <config/index>`. Each reference links to worked
guides. CLI and Python API pages are generated from code; configuration tables
describe the supported public interface.

Action YAML
-----------

.. toctree::
   :maxdepth: 1
   :hidden:

   actions/load
   actions/transform
   actions/write
   actions/test
   config/features
   config/index
   cli
   api
   errors
   glossary
   telemetry
   changelog

- :doc:`Load actions <actions/load>`: read from files, tables, queries and other sources.
- :doc:`Transform actions <actions/transform>`: SQL, Python, schemas and row expectations.
- :doc:`Write actions <actions/write>`: append, CDC, snapshot, replace, views and sinks.
- :doc:`Test actions <actions/test>`: assertions on counts, keys, values and relationships.

Feature configuration
---------------------

- :doc:`Operational metadata <config/operational-metadata>`
- :doc:`Unity Catalog tags <config/uc-tagging>`
- :doc:`Quarantine (DLQ) <config/quarantine-dlq>`
- :doc:`Test reporting <config/test-reporting>`
- :doc:`Monitoring and event logs <config/monitoring>`
- :doc:`Sandbox <config/sandbox>`

Configuration files
-------------------

- :doc:`Find a configuration file <config/index>`
- :doc:`lhp.yaml <config/project>`
- :doc:`Flowgroup YAML <config/flowgroups>`
- :doc:`pipeline_config.yaml <config/pipeline-config>`
- :doc:`Orchestration job_config.yaml <config/jobs>`
- :doc:`Monitoring job settings <config/monitoring-job>`
- :doc:`Substitutions <config/substitutions>`
- :doc:`Presets <config/presets>`
- :doc:`Templates <config/templates>`
- :doc:`Blueprints and instances <config/blueprints>`
- :doc:`Schema and tag files <config/schema-files>`
- :doc:`Expectation formats <config/expectations>`
- :doc:`Databricks bundle integration <config/bundle>`

Commands and lookup
-------------------

- :doc:`CLI commands <cli>`
- :doc:`Python API <api>`
- :doc:`Error codes <errors>`
- :doc:`Glossary <glossary>`
- :doc:`Telemetry <telemetry>`
- :doc:`Release notes <changelog>`
