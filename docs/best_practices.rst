=======================================
Enterprise Best Practices
=======================================

.. meta::
   :description: Comprehensive guide for data engineers using Lakehouse Plumber in enterprise environments — covering project structure, naming conventions, templates, presets, substitutions, actions, data quality, and production readiness patterns.

A comprehensive guide for data engineers using Lakehouse Plumber (LHP) in enterprise
environments. These best practices correlate Databricks Lakeflow Declarative Pipeline
conventions, enterprise configuration-framework patterns, and LHP-specific capabilities.




1. Project Structure & Organisation
====================================

.. _bp-1-1:

BP-1.1: Organize pipeline YAML files by data domain
----------------------------------------------------

Group by business domain (``orders/``, ``customers/``, ``inventory/``) rather than by
action type (``loads/``, ``transforms/``). LHP discovers flowgroups from the ``pipelines/``
directory and supports subdirectories, so ``pipelines/orders/bronze_ingest.yaml`` works
natively.

.. _bp-1-2:

BP-1.2: Keep each YAML file small and single-purpose
-----------------------------------------------------

Target 50--200 lines. Use LHP's multi-document (``---``) or array syntax only for tightly
related flowgroups that share a pipeline. Monolithic files with 15+ flowgroups become
unreadable and unreviewable.

.. seealso::
   :doc:`multi_flowgroup_guide` for details on multi-document and array syntax.

.. _bp-1-3:

BP-1.3: Use ``include`` patterns to filter pipeline discovery
-------------------------------------------------------------

For large repos, use the ``include`` glob patterns in ``lhp.yaml`` to control which pipeline
files are processed per environment or team. This enables a mono-repo structure where each
team's files coexist without interfering.

.. _bp-1-4:

BP-1.4: Separate presets, templates, and substitutions into dedicated directories
---------------------------------------------------------------------------------

Follow the standard LHP project layout. See :ref:`Section 2 <bp-2-1>` for detailed
subdirectory guidance within each top-level directory.

.. code-block:: text
   :caption: Standard LHP project layout

   presets/           # Reusable defaults (flat — no subdirectory discovery)
   templates/         # Reusable action patterns (flat — use prefix-based grouping)
   substitutions/     # Environment-specific tokens (dev.yaml, prod.yaml)
   pipelines/         # Flowgroup definitions (supports deep subdirectories)
   sql/               # External SQL files (supports deep subdirectories)
   schemas/           # External schema files (supports deep subdirectories)
   expectations/      # External DQE files (supports deep subdirectories)
   python_modules/    # External Python modules (supports deep subdirectories)

.. _bp-1-5:

BP-1.5: Use a CODEOWNERS file to gate shared resource changes
--------------------------------------------------------------

``CODEOWNERS`` is a GitHub/GitLab feature (a file at the repo root) that enforces
**who must review pull requests** that touch specific files or directories. When a PR
modifies files matching a pattern in ``CODEOWNERS``, the listed team or person is
automatically added as a required reviewer.

In an enterprise LHP project, shared resources like **presets** and **substitutions** and **templates**
affect every pipeline, so changes to them should require platform team approval.
Meanwhile, domain-specific pipelines should be reviewed by the owning team.

.. code-block:: text
   :caption: Example CODEOWNERS file

   # Platform team must review shared configs
   /presets/                @platform-team
   /substitutions/          @platform-team
   /templates/              @platform-team

   # Domain teams own their pipeline definitions
   /pipelines/system_a/    @team-a
   /pipelines/system_b/    @team-b

.. tip::

   Without ``CODEOWNERS``, a change to a preset (e.g., default table properties) could
   silently affect every pipeline that uses it and merge without review from someone who
   understands the blast radius.


2. File Organisation & Subdirectory Structure
=============================================

LHP file types have different subdirectory support. Understanding this is critical for
organizing an enterprise project with hundreds of files.

Subdirectory Support Matrix
---------------------------

.. list-table::
   :header-rows: 1
   :widths: 15 15 20 15 35

   * - File Type
     - Base Directory
     - Subdirectory Support
     - Extensions
     - Notes
   * - Pipeline YAMLs
     - ``pipelines/``
     - Full recursive
     - ``.yaml`` + ``.yml``
     - Discovered via ``rglob("*.yaml")`` — any depth works
   * - SQL files (``sql_path``)
     - project root
     - Full recursive
     - ``.sql``
     - Referenced by relative path from project root
   * - Schema files (``schema_file``)
     - project root
     - Full recursive
     - ``.yaml``, ``.json``, ``.ddl``
     - Referenced by relative path from project root
   * - Expectations files (``expectations_file``)
     - project root
     - Full recursive
     - ``.yaml``, ``.json``
     - Referenced by relative path from project root
   * - Python modules (``module_path``)
     - project root
     - Full recursive
     - ``.py``
     - Referenced by relative path from project root
   * - Templates
     - ``templates/``
     - Flat only
     - ``.yaml`` only |sup1|
     - Discovery uses ``glob("*.yaml")`` — not recursive
   * - Presets
     - ``presets/``
     - Flat only
     - ``.yaml`` only |sup1|
     - Discovery uses ``glob("*.yaml")`` — not recursive
   * - Substitutions
     - ``substitutions/``
     - Flat only
     - ``.yaml`` only
     - One file per environment

.. |sup1| replace:: :sup:`1`

:sup:`1` ``.yml`` extension is also accepted but ``.yaml`` is recommended for consistency.

.. _bp-2-1:

BP-2.1: Organize pipeline YAMLs by source system, then by medallion layer
--------------------------------------------------------------------------

LHP recursively discovers all ``.yaml``/``.yml`` files under ``pipelines/``. Use a
two-level hierarchy — source system first, layer second — so that each team owns a clear
subtree:

.. code-block:: text
   :caption: Pipeline directory structure

   pipelines/
     system_a/                          # Source system / data domain
       bronze/
         system_a_bronze_ingest.yaml    # CloudFiles ingestion
       silver/
         system_a_silver_cleanse.yaml   # Validation and enrichment
       gold/
         system_a_gold_reporting.yaml   # Aggregations
     system_b/
       bronze/
         system_b_bronze_ingest.yaml
       silver/
         system_b_silver_merge.yaml
     shared/
       gold/
         cross_domain_metrics.yaml      # Cross-system gold tables

This structure maps cleanly to CODEOWNERS (``pipelines/system_a/`` owned by Team A) and
to ``include`` patterns when you need to generate a subset.

.. _bp-2-2:

BP-2.2: Organize SQL files mirroring the pipeline structure
-----------------------------------------------------------

All ``sql_path`` references resolve relative to the project root, so
``sql_path: sql/system_a/bronze/cleanse_raw.sql`` works natively. Mirror the pipeline
directory hierarchy:

.. code-block:: text
   :caption: SQL directory structure

   sql/
     system_a/
       bronze/
         parse_json_payload.sql
       silver/
         enrich_orders.sql
         validate_customers.sql
       gold/
         daily_revenue_summary.sql
     system_b/
       silver/
         merge_inventory.sql
     shared/
       lookups/
         currency_conversion.sql

When referencing from YAML:

.. code-block:: yaml
   :caption: Referencing external SQL files

   actions:
     - name: transform_enrich_orders
       type: transform
       transform_type: sql
       sql_path: sql/system_a/silver/enrich_orders.sql
       source: load_raw_orders
       target: enriched_orders_view

.. _bp-2-3:

BP-2.3: Organize schema files by source system and layer
---------------------------------------------------------

Schema files (DDL, YAML, or JSON) also resolve relative to the project root:

.. code-block:: text
   :caption: Schema directory structure

   schemas/
     system_a/
       bronze/
         raw_orders_schema.yaml        # CloudFiles schema hints
         raw_customers_schema.ddl      # DDL format
       silver/
         orders_strict_schema.yaml     # Schema transform definitions
     system_b/
       bronze/
         raw_inventory_schema.json     # JSON format

When referencing:

.. code-block:: yaml
   :caption: Referencing external schema files

   actions:
     - name: transform_enforce_schema
       type: transform
       transform_type: schema
       schema_file: schemas/system_a/silver/orders_strict_schema.yaml
       enforcement: strict

.. _bp-2-4:

BP-2.4: Organize expectations files by domain and quality tier
--------------------------------------------------------------

Store DQE expectation files in a dedicated ``expectations/`` directory, grouped by domain
and quality tier:

.. code-block:: text
   :caption: Expectations directory structure

   expectations/
     system_a/
       bronze/
         raw_orders_warn.yaml          # Bronze: warn-only rules
       silver/
         orders_drop_rules.yaml        # Silver: drop invalid rows
         orders_quarantine_rules.yaml  # Silver: quarantine criteria
       gold/
         revenue_fail_rules.yaml       # Gold: fail on critical invariants
     shared/
       common_not_null_rules.yaml      # Reusable cross-domain rules

When referencing:

.. code-block:: yaml
   :caption: Referencing external expectations files

   actions:
     - name: transform_dqe_orders
       type: transform
       transform_type: data_quality
       expectations_file: expectations/system_a/silver/orders_drop_rules.yaml
       source: enriched_orders_view

.. _bp-2-5:

BP-2.5: Organize Python modules by function type
-------------------------------------------------

For Python-based loads, transforms, and sinks, group modules by their role:

.. code-block:: text
   :caption: Python modules directory structure

   python_modules/
     transforms/
       system_a/
         ml_scoring.py
         custom_dedup.py
       shared/
         phone_normalizer.py
     datasources/
       erp_connector.py                # Custom DataSource V2
     sinks/
       webhook_sink.py                 # Custom DataSink
       foreachbatch/
         notify_downstream.py          # ForEachBatch handlers

.. _bp-2-6:

BP-2.6: Use prefix-based grouping for templates
------------------------------------------------

Templates are discovered only at the top level of ``templates/`` — subdirectories are
**not** discovered by ``lhp list_templates``. Instead, use a structured prefix convention
to categorize templates:

.. code-block:: text
   :caption: Template naming with prefixes

   templates/
     TMPL001_brz_load_cloudfiles_standard.yaml        # Bronze / Load / CloudFiles
     TMPL002_brz_load_kafka_events.yaml               # Bronze / Load / Kafka
     TMPL003_brz_load_delta_snapshot.yaml             # Bronze / Load / Delta snapshot
     TMPL004_slv_transform_sql_enrichment.yaml        # Silver / Transform / SQL
     TMPL005_slv_transform_cdc_merge.yaml             # Silver / Transform / CDC
     TMPL006_slv_write_streaming_table_std.yaml       # Silver / Write / Streaming Table
     TMPL007_gld_write_materialized_view_agg.yaml     # Gold / Write / Materialized View
     TMPL008_full_bronze_to_silver_pipeline.yaml      # Full pipeline template (multi-action)

The prefix pattern ``<layer>_<action_type>_<detail>`` makes templates scannable in
``lhp list_templates`` output and in file explorers. When you have 30+ templates, this
prefix is the primary way to find the right one.

.. seealso::
   :doc:`templates_reference` for details on creating and using templates.

.. _bp-2-7:

BP-2.7: Use prefix-based grouping for presets
----------------------------------------------

Like templates, presets are discovered only at the top level of ``presets/``. Use prefixes
to encode scope and layer:

.. code-block:: text
   :caption: Preset naming with prefixes

   presets/
     global_defaults.yaml                             # Organization-wide
     brz_standard.yaml                                # Bronze layer defaults
     brz_cloudfiles_json.yaml                         # Bronze / CloudFiles / JSON specific
     brz_cloudfiles_csv.yaml                          # Bronze / CloudFiles / CSV specific
     slv_standard.yaml                                # Silver layer defaults
     slv_cdc_scd2.yaml                                # Silver / CDC / SCD Type 2
     gld_standard.yaml                                # Gold layer defaults
     ord_custom_overrides.yaml                        # Orders domain custom

.. seealso::
   :doc:`presets_reference` for details on preset inheritance and merging.

.. _bp-2-8:

BP-2.8: Use ``include`` patterns for team-scoped generation
-----------------------------------------------------------

When multiple teams share a mono-repo, use ``include`` patterns in ``lhp.yaml`` to generate
only relevant pipelines. Patterns are matched against paths relative to ``pipelines/``:

.. code-block:: yaml
   :caption: Include only system_a pipelines

   # lhp.yaml — generate only system_a pipelines
   include:
     - "system_a/**/*.yaml"

Or selectively include specific layers:

.. code-block:: yaml
   :caption: Include only bronze pipelines

   # Only bronze pipelines across all systems
   include:
     - "**/bronze/*.yaml"

.. _bp-2-9:

BP-2.9: Full enterprise project layout example
-----------------------------------------------

.. code-block:: text
   :caption: Complete enterprise project structure

   my_lhp_project/
     lhp.yaml                                # Project config
     substitutions/
       dev.yaml
       staging.yaml
       prod.yaml
     presets/
       global_defaults.yaml
       brz_standard.yaml
       brz_cloudfiles_json.yaml
       slv_standard.yaml
       slv_cdc_scd2.yaml
       gld_standard.yaml
     templates/
       TMPL001_brz_load_cloudfiles_standard.yaml
       TMPL002_slv_transform_sql_enrichment.yaml
       TMPL003_gld_write_mv_aggregation.yaml
     pipelines/
       system_a/
         bronze/
           system_a_bronze_ingest_TMPL001.yaml
         silver/
           system_a_silver_cleanse_TMPL002.yaml
         gold/
           system_a_gold_reporting_TMPL003.yaml
       system_b/
         bronze/
           system_b_bronze_ingest_TMPL001.yaml
         silver/
           system_b_silver_merge_TMPL002.yaml
     sql/
       system_a/
         silver/
           enrich_orders.sql
         gold/
           daily_revenue.sql
       system_b/
         silver/
           merge_inventory.sql
     schemas/
       system_a/
         bronze/
           raw_orders_schema.yaml
         silver/
           orders_strict_schema.yaml
       system_b/
         bronze/
           raw_inventory_schema.yaml
     expectations/
       system_a/
         bronze/
           raw_orders_warn.yaml
         silver/
           orders_drop_rules.yaml
       shared/
         common_not_null_rules.yaml
     python_modules/
       transforms/
         system_a/
           ml_scoring.py
       datasources/
         erp_connector.py
     generated/                               # Output (per environment)
       dev/
         system_a_bronze_pipeline/
           raw_orders.py
         system_a_silver_pipeline/
           orders_cleanse.py

.. _bp-2-10:

BP-2.10: Place blueprint instance files in the layer folder they target
-----------------------------------------------------------------------

For the dominant case (a blueprint that shapes one layer), instance files
belong alongside hand-written flowgroups under
``pipelines/<system>/<layer>/`` — not in a separate top-level
``instances/`` directory. The blueprint discoverer routes by content shape,
so instance and flowgroup YAMLs co-exist in the same folder safely:

.. code-block:: text

   pipelines/
     halo/
       bronze/
         halo_bronze_acme_BP001.yaml         # use_blueprint: halo_bronze
         halo_bronze_globex_BP001.yaml       # use_blueprint: halo_bronze
         halo_bronze_one_off_special.yaml    # hand-written flowgroup

For a blueprint that genuinely spans multiple layers, use a peer
``instances/`` subfolder under the system directory rather than the layer
folder — see :ref:`BP-5.4 <bp-5-4>`.


3. Naming Conventions
=====================

.. _bp-3-1:

BP-3.1: Use ``snake_case`` consistently across all identifiers
--------------------------------------------------------------

Pipelines, flowgroups, action names, templates, presets, variables, table names — all
``snake_case``. LHP generates Python function names from action names, so this ensures
valid Python identifiers.

.. _bp-3-2:

BP-3.2: Prefix pipeline names with the source system and layer
----------------------------------------------------------------

``erp_bronze_pipeline``, ``crm_silver_pipeline`` — not ``bronze_pipeline`` or
``pipeline_v2``. At 200+ pipelines, generic names become meaningless. LHP uses the
``pipeline`` field in flowgroups to group actions into output files.
See :ref:`BP-3.9 <bp-3-9>` for the full enterprise naming pattern.

.. _bp-3-3:

BP-3.3: Name flowgroups to describe the data flow
--------------------------------------------------

``erp_brz_raw_orders``, ``erp_slv_orders_enriched`` — not ``cloudfiles_load_1`` or
``flowgroup_v2``. The flowgroup name appears in generated file names and log output.
Embed the source system and layer for visibility. See :ref:`BP-3.8 <bp-3-8>` for the
full enterprise naming pattern.

.. _bp-3-4:

BP-3.4: Name actions descriptively with the pattern ``<verb>_<entity>_<modifier>``
----------------------------------------------------------------------------------

``load_raw_orders``, ``transform_validate_orders``, ``write_orders_silver``,
``test_orders_row_count``. Action names become Python function names in generated code,
so clarity matters.

.. _bp-3-5:

BP-3.5: Use SCREAMING_SNAKE_CASE for environment tokens
--------------------------------------------------------

Environment tokens (``${SOURCE_CATALOG}``, ``${LANDING_PATH}``) are resolved from
substitution files. Local variables (``%{table_name}``, ``%{source_schema}``) are
flowgroup-scoped. The case distinction makes it immediately clear which resolution
mechanism applies.

.. seealso::
   :doc:`substitutions` for the full substitution processing order and syntax.

.. _bp-3-6:

BP-3.6: Never abbreviate in identifiers
----------------------------------------

``customer_silver_merge`` not ``cust_slvr_mrg``. Config files live in version control
forever; clarity beats brevity.

Structured Naming for Enterprise Visibility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

At enterprise scale (100+ templates, 500+ flowgroups), flat alphabetical lists become
unmanageable. **Templates** use a ``TMPLxxx_`` ID prefix to embed a unique sequence
number, making them instantly scannable and sortable. Flowgroup config files reference
the template ID as a ``_TMPLxxx`` suffix, creating a visible link between a config and
its template. All other artifacts — pipelines, presets, SQL files, schemas, and
expectations — use descriptive prefixes and directory structure for organisation.

.. _bp-3-7:

BP-3.7: Use ``TMPLxxx`` ID prefixes for templates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since templates live in a flat directory (see :ref:`Section 2 <bp-2-6>`), the filename is
the only organisational mechanism. Use a ``TMPLxxx_`` prefix with a sequential number,
followed by a structured name that encodes layer and action type:

.. code-block:: text
   :caption: Template naming pattern

   Pattern: TMPLxxx_<layer>_<action_type>_<source_or_target_type>_<descriptive_name>

   Examples:
     TMPL001_brz_load_cloudfiles_standard        # Bronze / Load / CloudFiles / standard pattern
     TMPL002_brz_load_cloudfiles_with_schema     # Bronze / Load / CloudFiles / with schema hints
     TMPL003_brz_load_kafka_events               # Bronze / Load / Kafka / event stream
     TMPL004_slv_transform_sql_enrichment        # Silver / Transform / SQL / enrichment pattern
     TMPL005_slv_transform_cdc_merge             # Silver / Transform / CDC / merge pattern
     TMPL006_slv_write_st_with_dqe               # Silver / Write / Streaming Table / with DQE
     TMPL007_gld_write_mv_aggregation            # Gold / Write / Materialized View / aggregation
     TMPL008_e2e_full_bronze_to_silver           # End-to-end / multi-action pipeline template

Layer prefixes: ``brz_`` (bronze), ``slv_`` (silver), ``gld_`` (gold), ``e2e_``
(end-to-end multi-action).

The ``TMPLxxx`` prefix sorts templates by creation order in ``lhp list_templates``
output, while the layer prefix groups them logically. The ID also appears as a suffix
in flowgroup config filenames (see :ref:`BP-3.8 <bp-3-8>`), creating a visible link
between configs and their templates.

.. _bp-3-8:

BP-3.8: Use descriptive flowgroup names with a ``_TMPLxxx`` config file suffix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Flowgroup names become Python file names and function names in generated code. Embed the
source system and layer for visibility across large projects:

.. code-block:: text
   :caption: Flowgroup naming pattern

   Pattern: <system>_<layer>_<descriptive_name>

   Examples:
     erp_brz_raw_orders                  # ERP system / Bronze / raw orders
     erp_brz_raw_customers               # ERP system / Bronze / raw customers
     erp_slv_orders_enriched             # ERP system / Silver / enriched orders
     erp_slv_customers_merged            # ERP system / Silver / merged customers
     erp_gld_daily_revenue               # ERP system / Gold / daily revenue
     crm_brz_raw_contacts                # CRM system / Bronze / raw contacts
     crm_slv_contacts_deduped            # CRM system / Silver / deduped contacts

When naming the **Flowgroup file**, append the template ID as a suffix so the template
relationship is visible at a glance without opening the file:

.. code-block:: text
   :caption: Config file naming pattern

   Pattern: <system>_<layer>_<description>_<TMPLxxx>.yaml

   Examples:
     erp_bronze_ingest_TMPL001.yaml      # Uses TMPL001 (CloudFiles standard)
     erp_silver_cleanse_TMPL004.yaml     # Uses TMPL004 (SQL enrichment)
     erp_gold_reporting_TMPL007.yaml     # Uses TMPL007 (MV aggregation)
     crm_bronze_contacts_TMPL001.yaml    # Uses TMPL001 (CloudFiles standard)

This naming ensures that when you see a generated file ``erp_brz_raw_orders.py`` or a DLT
log entry for ``erp_slv_orders_enriched``, you immediately know the source system and layer
without looking up the config. The ``_TMPLxxx`` suffix in the config filename lets you
identify the template at the file system level — useful when browsing directories, reviewing
PRs, or triaging issues.

.. _bp-3-9:

BP-3.9: Use structured prefixes for pipeline names
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pipeline names determine the output directory structure under ``generated/{env}/`` and
appear in Databricks UI. Use ``<system>_<layer>_pipeline`` for clear identification:

.. code-block:: text
   :caption: Pipeline naming pattern

   Pattern: <system>_<layer>_pipeline

   Examples:
     erp_bronze_pipeline                 # All ERP bronze ingestion
     erp_silver_pipeline                 # All ERP silver transforms
     erp_gold_pipeline                   # All ERP gold aggregations
     crm_bronze_pipeline                 # All CRM bronze ingestion
     shared_gold_pipeline                # Cross-system gold tables

This gives you clean, predictable output directories:

.. code-block:: text
   :caption: Generated output with structured names

   generated/dev/
     erp_bronze_pipeline/
       erp_brz_raw_orders.py
       erp_brz_raw_customers.py
     erp_silver_pipeline/
       erp_slv_orders_enriched.py
     crm_bronze_pipeline/
       crm_brz_raw_contacts.py

.. _bp-3-10:

BP-3.10: Use consistent prefixes for presets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since presets are also flat (no subdirectory discovery), the naming prefix is essential for
organisation:

.. code-block:: text
   :caption: Preset naming pattern

   Pattern: <scope>_<layer>_<purpose>

   Examples:
     global_defaults                     # Organisation-wide standards
     brz_standard                        # Bronze layer standard preset
     brz_cloudfiles_json                 # Bronze / CloudFiles / JSON format
     brz_cloudfiles_csv                  # Bronze / CloudFiles / CSV format
     brz_kafka_events                    # Bronze / Kafka event preset
     slv_standard                        # Silver layer standard preset
     slv_cdc_scd2                        # Silver / CDC / SCD Type 2
     gld_standard                        # Gold layer standard preset
     erp_custom                          # ERP domain custom overrides

.. _bp-3-11:

BP-3.11: Name instance files ``<system>_<layer>_<variant>_<BPxxx>.yaml``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Extends :ref:`BP-3.8 <bp-3-8>` for blueprint instance files. The variant
identifier (the parameter that distinguishes one instance from another —
typically site, region, or tenant) goes between the layer and the ``BPxxx``
suffix. The ``BPxxx`` suffix references the blueprint definition file
(:ref:`BP-3.12 <bp-3-12>`):

.. code-block:: text
   :caption: Instance file naming pattern

   Pattern: <system>_<layer>_<variant>_<BPxxx>.yaml

   Examples:
     halo_bronze_acme_BP001.yaml
     halo_bronze_globex_BP001.yaml
     sap_silver_emea_BP005.yaml
     sap_silver_apac_BP005.yaml

This is convention only; the discoverer does not enforce the pattern.

.. _bp-3-12:

BP-3.12: Name blueprint definition files ``BPxxx_<system>_<layer>.yaml``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Mirrors the templates pattern (:ref:`BP-3.7 <bp-3-7>`). Each blueprint
has a stable ID that variant instance files reference in their suffix:

.. code-block:: text
   :caption: Blueprint file naming pattern

   Pattern: BPxxx_<system>_<layer>.yaml

   Examples:
     blueprints/BP001_halo_bronze.yaml
     blueprints/BP002_halo_silver.yaml
     blueprints/BP005_sap_silver.yaml

.. _bp-3-13:

BP-3.13: Pipelines and flowgroups produced by blueprints carry a variant prefix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Inside a blueprint definition, use ``%{variant}`` (or whatever the variant
parameter is named) in pipeline and flowgroup identity fields so each
expanded instance produces uniquely-named outputs:

.. code-block:: yaml
   :caption: Blueprint identity fields

   flowgroups:
     - pipeline: "%{variant}_halo_bronze_pipeline"
       flowgroup: "%{variant}_halo_bronze"
       actions:
         ...

This produces ``acme_halo_bronze_pipeline``, ``globex_halo_bronze_pipeline``,
etc. Without this prefix, all instances would collide on the same identity
keys and fail expansion.

Quick Reference Table
~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 20 30 25

   * - Artifact
     - Convention
     - Example
   * - Pipeline names
     - ``<system>_<layer>_pipeline``
     - ``erp_bronze_pipeline``
   * - Flowgroup names
     - ``<system>_<layer>_<description>``
     - ``erp_brz_raw_orders``
   * - Action names
     - ``<verb>_<entity>_<modifier>``
     - ``load_raw_orders``
   * - Config files
     - ``<system>_<layer>_<description>_<TMPLxxx>.yaml``
     - ``erp_bronze_ingest_TMPL001.yaml``
   * - Template files
     - ``TMPLxxx_<layer>_<action>_<type>_<name>.yaml``
     - ``TMPL001_brz_load_cloudfiles_standard.yaml``
   * - Preset files
     - ``<scope>_<layer>_<purpose>.yaml``
     - ``brz_standard.yaml``
   * - SQL files
     - ``<domain>/<layer>/<description>.sql``
     - ``erp/silver/enrich_orders.sql``
   * - Schema files
     - ``<domain>/<layer>/<description>.yaml``
     - ``erp/bronze/raw_orders_schema.yaml``
   * - Expectations files
     - ``<domain>/<layer>/<description>.yaml``
     - ``erp/silver/orders_drop_rules.yaml``
   * - Generated files
     - ``<flowgroup_name>.py``
     - ``erp_brz_raw_orders.py``
   * - Env tokens
     - ``${SCREAMING_SNAKE_CASE}``
     - ``${SOURCE_CATALOG}``
   * - Local variables
     - ``%{lower_snake_case}``
     - ``%{table_suffix}``
   * - Template params
     - ``{{ lower_snake_case }}``
     - ``{{ partition_column }}``
   * - Blueprint definition files
     - ``BPxxx_<system>_<layer>.yaml``
     - ``BP001_halo_bronze.yaml``
   * - Blueprint instance files
     - ``<system>_<layer>_<variant>_<BPxxx>.yaml``
     - ``halo_bronze_acme_BP001.yaml``
   * - Variant identifier (in blueprint)
     - ``%{variant}`` (local var used in identity fields)
     - ``%{variant}_halo_bronze_pipeline``


4. Template Design
==================

.. _bp-4-1:

BP-4.1: Extract a template only after 3+ flowgroups share the same pattern
--------------------------------------------------------------------------

Building templates for one-off use cases leads to over-generalisation. Write three explicit
flowgroups first, identify the common pattern, then extract the template. LHP templates
support ``parameters`` with ``required``, ``default``, and ``description`` fields.

.. _bp-4-2:

BP-4.2: Keep template parameters minimal and well-documented
-------------------------------------------------------------

Every parameter should have a ``description`` and either be ``required: true`` or have a
sensible ``default``. LHP validates required parameters at generation time and reports clear
errors for missing ones. Avoid templates with 15+ parameters — they add complexity without
reducing it.

.. _bp-4-3:

BP-4.3: Establish "golden templates" for each common pipeline pattern
---------------------------------------------------------------------

Maintain platform-team-owned templates for standard patterns, using the ID-based naming
from :ref:`Section 3 <bp-3-7>`:

- ``TMPL001_brz_load_cloudfiles_standard`` — standard CloudFiles ingestion with operational metadata
- ``TMPL002_brz_load_delta_snapshot`` — Delta table reads with standard options
- ``TMPL003_slv_write_st_with_dqe`` — streaming table with DQE expectations
- ``TMPL004_slv_transform_sql_enrichment`` — SQL-based silver enrichment
- ``TMPL005_gld_write_mv_aggregation`` — materialized view for gold aggregations

These golden templates embed organisational standards (default expectations, metadata
columns, table properties) so domain teams can't accidentally skip them.

.. _bp-4-4:

BP-4.4: Templates live in a flat directory — organise by naming convention
--------------------------------------------------------------------------

LHP discovers templates only from the top level of ``templates/`` (using
``glob("*.yaml")``, not recursive). Subdirectories under ``templates/`` are **not**
discovered by ``lhp list_templates``. Instead, use the structured prefix convention from
:ref:`BP-3.7 <bp-3-7>` to group templates logically.

.. note::
   Subdirectories under ``templates/`` are not discovered. Referencing templates via
   subfolder paths (e.g., ``use_template: "subfolder/name"``) is not supported. Stick to
   the flat directory with prefix-based naming.

.. _bp-4-5:

BP-4.5: Templates can reference presets — use this to layer defaults
--------------------------------------------------------------------

A template can declare ``presets: [brz_standard]`` to inherit default options. Flowgroups
using the template can add additional presets that override. This creates a clean defaults
hierarchy: template presets -> flowgroup presets -> explicit action config.

.. _bp-4-6:

BP-4.6: Use template parameters for what varies; presets for what is standard
-----------------------------------------------------------------------------

Template parameters should capture the unique aspects of each use case (source path, target
table, specific columns). Standard aspects (table properties, operational metadata, reader
options) belong in presets. This keeps template usage concise.

.. _bp-4-7:

BP-4.7: Reference external files from templates using parameterised paths
-------------------------------------------------------------------------

Templates can reference external files via ``sql_path``, ``schema_file``, or
``expectations_file``. Use template parameters for the variable part of the path, combined
with a fixed subdirectory convention:

.. code-block:: yaml
   :caption: Template with parameterised SQL path

   # Template: slv_transform_sql_enrichment.yaml
   name: slv_transform_sql_enrichment
   parameters:
     - name: system
       required: true
       description: "Source system name (used in file paths)"
     - name: entity
       required: true
       description: "Entity name"
   actions:
     - name: transform_enrich_{{ entity }}
       type: transform
       transform_type: sql
       sql_path: "sql/{{ system }}/silver/enrich_{{ entity }}.sql"
       source: "load_raw_{{ entity }}"
       target: "enriched_{{ entity }}_view"

This way, the directory structure convention (``sql/<system>/silver/``) is baked into
the template, ensuring all teams follow the same file organisation.

.. seealso::
   :doc:`templates_reference` for the full template specification and
   :doc:`dynamic_templates_guide` for conditionals, loops, and advanced Jinja2 features.


5. Blueprint Strategy
=====================

Blueprints are the right tool when the *same* parameterised flowgroup shape
must be instantiated across many variants — typically sites, regions,
tenants, or other identifier dimensions. They are conceptually adjacent to
templates but operate one layer higher: a template parameterises an action;
a blueprint parameterises a whole flowgroup (or a small set of flowgroups).

An instance file is just a small YAML document that says "give me this
blueprint with these parameter values". The shape mirrors templates:

.. code-block:: yaml
   :caption: New (preferred) instance syntax

   use_blueprint: example_system_bronze
   parameters:
     variant: acme
     source_path: s3://example-bucket/acme/raw/

This parallels ``use_template:`` / ``template_parameters:`` so operators do
not have to learn a different idiom.

.. note::
   The legacy ``blueprint:`` + flat parameters form is deprecated and
   removed in V0.9. A migration deprecation warning is emitted once per file.

.. _bp-5-1:

BP-5.1: Reach for blueprints when there are 10+ near-identical variants
-----------------------------------------------------------------------

Below 10 variants, a hand-written flowgroup per variant is more readable.
Blueprints become valuable when the variant count is high enough that
copy-paste drift is a real risk and the shape is genuinely identical.

If only the *write target* differs (e.g. table name) but the actions are the
same, a template usually fits better. If the *whole flowgroup shape* is
identical and only identifiers vary across variants, a blueprint fits.

.. _bp-5-2:

BP-5.2: Prefer one blueprint per ``(system, layer)`` pair
---------------------------------------------------------

A blueprint that spans multiple layers (e.g. raw → bronze → silver in one
definition) is convenient at first, but it couples the layers' release
cycles together: any change to silver-layer logic forces re-expansion of
all variants for all layers, and the blast radius grows with the number of
variants.

The default is one blueprint per ``(system, layer)``. Each variant gets
one instance file *per layer*, located in the layer folder.

.. _bp-5-3:

BP-5.3: Place instance files in the layer folder they target (Case 1)
---------------------------------------------------------------------

The dominant case is single-layer: a blueprint shapes one layer's
flowgroups, and an instance file fits semantically alongside hand-written
flowgroups in that layer's directory.

.. code-block:: text
   :caption: Case 1 — single-layer blueprint (recommended)

   pipelines/
     halo/
       bronze/
         halo_bronze_acme_BP001.yaml          # instance (use_blueprint: ...)
         halo_bronze_globex_BP001.yaml        # instance
         halo_bronze_one_off_special.yaml     # hand-written flowgroup, fine to coexist

This keeps CODEOWNERS scoping intact and makes the layer folder the single
source of truth for "what flows through this layer".

.. _bp-5-4:

BP-5.4: Use ``pipelines/<system>/instances/`` for multi-layer blueprints (Case 2)
---------------------------------------------------------------------------------

When a blueprint genuinely spans multiple layers (the escape hatch), there
is no single layer folder for the instance to live in. Use a peer
``instances/`` subfolder under the system directory:

.. code-block:: text
   :caption: Case 2 — multi-layer blueprint (escape hatch)

   pipelines/
     halo/
       bronze/                              # layer folders, hand-written or per-layer instances
       silver/
       gold/
       instances/                           # multi-layer instance files only
         halo_full_stack_acme_BP010.yaml

This signals to readers that the instance is intentionally cross-layer and
that the blueprint's blast radius is wide.

.. _bp-5-5:

BP-5.5: Naming — blueprints ``BPxxx_<system>_<layer>.yaml``, instances ``<system>_<layer>_<variant>_<BPxxx>.yaml``
-----------------------------------------------------------------------------------------------------------------

Blueprint definition files follow the same prefix idea as templates
(:ref:`BP-3.7 <bp-3-7>`):

.. code-block:: text

   blueprints/BP001_halo_bronze.yaml
   blueprints/BP002_sap_bronze.yaml

Instance files extend the flowgroup-config naming (:ref:`BP-3.8 <bp-3-8>`)
with a variant identifier:

.. code-block:: text

   pipelines/halo/bronze/halo_bronze_acme_BP001.yaml
   pipelines/halo/bronze/halo_bronze_globex_BP001.yaml

Generated pipelines and flowgroups carry the variant prefix
(:ref:`BP-3.13 <bp-3-13>`) — ``acme_halo_bronze_pipeline`` etc.

.. seealso::
   :ref:`AP-14 <ap-14>` for the cross-system god-blueprint anti-pattern.


6. Preset Strategy
==================

.. _bp-6-1:

BP-6.1: Design a preset hierarchy — global, domain, pipeline-specific
----------------------------------------------------------------------

LHP supports preset inheritance via ``extends`` and preset chaining (multiple presets in a
list, merged left-to-right). Use this to build layers:

- ``global_defaults`` — organisation-wide standards (table properties, metadata)
- ``bronze_standard`` extends ``global_defaults`` — bronze-layer conventions
- ``orders_bronze`` extends ``bronze_standard`` — domain-specific overrides

.. _bp-6-2:

BP-6.2: Encode organisational standards in presets, not just values
-------------------------------------------------------------------

A high-value preset sets multiple related properties together:

.. code-block:: yaml
   :caption: Bronze standard preset example

   name: bronze_standard
   extends: global_defaults
   defaults:
     load_actions:
       cloudfiles:
         options:
           cloudFiles.schemaEvolutionMode: rescue
           cloudFiles.rescuedDataColumn: _rescued_data
           cloudFiles.maxFilesPerTrigger: 1000
     write_actions:
       streaming_table:
         table_properties:
           pipelines.reset.allowed: "false"
     operational_metadata:
       - ingest_timestamp
       - source_file

.. _bp-6-3:

BP-6.3: Limit the total number of presets
-----------------------------------------

More than 15--20 distinct presets leads to confusion and misuse. Consolidate overlapping
presets. LHP's ``lhp list_presets`` command helps audit the current set.

.. _bp-6-4:

BP-6.4: Use ``lhp show`` to verify effective configuration
-----------------------------------------------------------

After preset merging, template expansion, and substitution, the effective config can differ
from what the YAML file suggests. Always verify with ``lhp show <flowgroup> --env <env>``
before deploying changes to shared presets. This is LHP's equivalent of "fully resolved
config."

.. _bp-6-5:

BP-6.5: Treat preset changes as high-blast-radius events
---------------------------------------------------------

A change to a global preset affects every pipeline using it. Version presets (add a version
field), document changes, and run ``lhp validate --env <env>`` across the entire project
before merging preset changes.

.. seealso::
   :doc:`presets_reference` for complete details on preset inheritance and merging.


7. Substitution & Environment Management
=========================================

.. _bp-7-1:

BP-7.1: Use directory-based environment separation
---------------------------------------------------

Maintain ``substitutions/dev.yaml``, ``substitutions/staging.yaml``,
``substitutions/prod.yaml``. All environments are visible on the same branch. LHP resolves
``${token}`` patterns from these files.

.. _bp-7-2:

BP-7.2: Put all environment-varying values in substitution tokens
-----------------------------------------------------------------

Catalog names, schema names, storage paths, cluster policies, alert emails — all should be
tokens. LHP supports recursive token expansion (tokens referencing other tokens, up to 10
iterations), so you can compose:

.. code-block:: yaml
   :caption: Recursive token expansion

   global:
     catalog_prefix: main

   dev:
     catalog: "${catalog_prefix}_dev"

   prod:
     catalog: "${catalog_prefix}_prod"

.. _bp-7-3:

BP-7.3: Use the ``global`` section for shared values
-----------------------------------------------------

LHP's substitution files support a ``global`` section whose values are inherited by all
environments. Environment-specific sections override global values. This eliminates
duplication.

.. _bp-7-4:

BP-7.4: Never put secret values in substitution files
------------------------------------------------------

Use LHP's ``${secret:scope/key}`` syntax. LHP converts these to
``dbutils.secrets.get(scope="scope", key="key")`` calls in generated code. Configure
``secrets.default_scope`` and ``scopes`` aliases in the substitution file for clean
references.

.. important::
   Secrets in substitution files will be committed to version control and leaked. Always
   use the ``${secret:scope/key}`` syntax exclusively.

.. _bp-7-5:

BP-7.5: Use ``lhp substitutions`` to audit available tokens
------------------------------------------------------------

Before writing flowgroups, run ``lhp substitutions --env <env>`` to check what tokens are
available. This prevents unresolved token errors at generation time.

.. _bp-7-6:

BP-7.6: Design substitution tokens for the medallion pattern
-------------------------------------------------------------

Standard token set for a medallion project:

.. code-block:: yaml
   :caption: Medallion substitution tokens

   global:
     bronze_catalog: "${catalog_prefix}_bronze"
     silver_catalog: "${catalog_prefix}_silver"
     gold_catalog: "${catalog_prefix}_gold"
     landing_path_base: "abfss://landing@${storage_account}.dfs.core.windows.net"

.. seealso::
   :doc:`substitutions` for the full substitution processing order and syntax.


8. Local Variables
==================

.. _bp-8-1:

BP-8.1: Use local variables for flowgroup-scoped repetition
------------------------------------------------------------

When the same value (table name, schema, path segment) appears multiple times within a
single flowgroup, define it as a local variable rather than repeating it. LHP resolves
``%{var}`` first, before template expansion.

.. _bp-8-2:

BP-8.2: Prefer local variables over hardcoded values
-----------------------------------------------------

.. code-block:: yaml
   :caption: Using local variables

   variables:
     entity: orders
     source_schema: raw
   actions:
     - name: load_%{entity}
       source:
         table: "${BRONZE_CATALOG}.%{source_schema}.%{entity}"

.. _bp-8-3:

BP-8.3: Do not use local variables for environment-specific values
------------------------------------------------------------------

``%{var}`` is scoped to a single flowgroup and resolved at parse time. Environment-specific
values belong in substitution tokens (``${TOKEN}``) which are resolved per environment.

.. seealso::
   :doc:`substitutions` for details on local variables and environment tokens.


9. FlowGroup Design
====================

.. _bp-9-1:

BP-9.1: Use array syntax with field inheritance for multi-flowgroup pipelines
-----------------------------------------------------------------------------

When multiple flowgroups share the same pipeline, presets, or template, use LHP's array
syntax to inherit:

.. code-block:: yaml
   :caption: Array syntax with inheritance

   pipeline: orders_bronze
   presets: [bronze_standard]
   operational_metadata: true
   flowgroups:
     - flowgroup: raw_orders
       actions: [...]
     - flowgroup: raw_returns
       actions: [...]

Inherited fields: ``pipeline``, ``use_template``, ``presets``, ``operational_metadata``,
``job_name``.

.. seealso::
   :doc:`multi_flowgroup_guide` for the full multi-flowgroup reference.

.. _bp-9-2:

BP-9.2: Scope one pipeline per data domain
-------------------------------------------

Pipeline ``orders_bronze`` contains flowgroups ``raw_orders``, ``raw_returns``,
``raw_refunds``. Each flowgroup generates its own Python function set but runs in the same
DLT pipeline, enabling dependency resolution across them.

.. _bp-9-3:

BP-9.3: Use ``job_name`` to group flowgroups into Databricks jobs
-----------------------------------------------------------------

LHP's ``lhp deps --format job`` generates job resource definitions. Use ``job_name`` to
control which flowgroups are orchestrated together in a Databricks Workflow.

.. seealso::
   :doc:`concepts` for details on ``job_name`` and multi-job orchestration.

.. _bp-9-4:

BP-9.4: Order actions as Load, Transform, Write, Test
------------------------------------------------------

This matches the data flow direction and makes YAML files scannable. LHP resolves
dependencies automatically, but consistent ordering improves readability.


10. Load Actions
===============

.. _bp-10-1:

BP-10.1: Always set ``schemaEvolutionMode`` and ``rescuedDataColumn`` for CloudFiles
------------------------------------------------------------------------------------

LHP's CloudFiles generator supports all Auto Loader options. In production, always use:

.. code-block:: yaml
   :caption: CloudFiles with schema rescue

   source:
     type: cloudfiles
     path: "${LANDING_PATH}/orders/"
     format: json
     options:
       cloudFiles.schemaEvolutionMode: rescue
       cloudFiles.rescuedDataColumn: _rescued_data

.. tip::
   Put these options in a ``bronze_standard`` preset so they apply everywhere without
   repetition.

.. _bp-10-2:

BP-10.2: Use ``readMode: stream`` for bronze, ``readMode: batch`` for lookups
-----------------------------------------------------------------------------

LHP's ``readMode`` field controls whether ``spark.readStream`` or ``spark.read`` is
generated. Bronze sources should stream; dimension/lookup tables should batch-read.

.. _bp-10-3:

BP-10.3: Use full three-part names via substitution tokens for Delta loads
-------------------------------------------------------------------------

.. code-block:: yaml
   :caption: Delta source with substitution tokens

   source:
     type: delta
     catalog: "${SILVER_CATALOG}"
     database: "orders"
     table: "validated_orders"

LHP constructs ``catalog.database.table`` references. Never hardcode catalog or database
names.

.. _bp-10-4:

BP-10.4: Rate-limit Auto Loader in production
---------------------------------------------

Use ``cloudFiles.maxFilesPerTrigger`` and ``cloudFiles.maxBytesPerTrigger`` options (via
presets) to prevent bronze ingestion from overwhelming downstream tables. Set this in your
``bronze_standard`` preset.

.. _bp-10-5:

BP-10.5: Use ``schema_hints`` for critical columns
--------------------------------------------------

LHP supports ``cloudFiles.schemaHints`` option strings. For columns where wrong type
inference would cause downstream failures (amounts, IDs, timestamps), provide explicit
hints.

.. seealso::
   :doc:`actions/load_actions` for the full load action specification.


11. Transform Actions
=====================

.. _bp-11-1:

BP-11.1: Default to SQL transforms for silver/gold layer logic
--------------------------------------------------------------

LHP's SQL transform generator supports inline SQL or external SQL files via ``sql_path``.
SQL is more readable, more widely understood, and easier to review than Python transforms
for standard operations. Use external SQL files for anything over ~5 lines.

.. _bp-11-2:

BP-11.2: Use external SQL files for complex transformations
-----------------------------------------------------------

LHP resolves ``sql_path`` relative to the project root. Store SQL in
``sql/<system>/<layer>/<transform_name>.sql`` (see :ref:`Section 2 <bp-2-2>`). This keeps
YAML files concise and enables SQL-specific linting.

.. _bp-11-3:

BP-11.3: Use Python transforms only when SQL cannot express the logic
---------------------------------------------------------------------

LHP's Python transform generator copies external modules and calls your function. The
signature depends on the number of sources:

- **Single source:** ``function(df, spark, parameters)`` — receives the source DataFrame directly
- **Multiple sources:** ``function(dataframes, spark, parameters)`` — receives a list of DataFrames
- **No sources:** ``function(spark, parameters)`` — function generates data from scratch

Reserve Python transforms for UDFs, ML scoring, or complex procedural logic.

.. _bp-11-4:

BP-11.4: Use schema transforms for explicit column control
-----------------------------------------------------------

LHP's ``schema`` transform type supports column renaming (arrow syntax:
``old_name -> new_name``), type casting, and strict/permissive enforcement. Use
``enforcement: strict`` at silver to reject unexpected columns from bronze.

.. _bp-11-5:

BP-11.5: Use data_quality transforms for DQE expectations
----------------------------------------------------------

LHP's ``data_quality`` transform type reads expectations from YAML/JSON files or inline
definitions, generating the appropriate ``@dp.expect_all()``,
``@dp.expect_all_or_drop()``, or ``@dp.expect_all_or_fail()`` decorators.

.. _bp-11-6:

BP-11.6: Use temp_table transforms for intermediate calculations
----------------------------------------------------------------

LHP generates ``@dp.table(temporary=True)`` for temp tables. Use these for intermediate
steps that should not be published to Unity Catalog.

.. seealso::
   :doc:`actions/transform_actions` for the full transform action specification.


12. Write Actions
=================

.. _bp-12-1:

BP-12.1: Default to materialized views for silver/gold layers
-------------------------------------------------------------

LHP's materialized_view write target generates ``@dp.materialized_view()``. Materialized
views always produce correct results — they reprocess when source data changes. Use them for
all joins, aggregations, and enrichment.

.. _bp-12-2:

BP-12.2: Use streaming tables for bronze ingestion and CDC targets
------------------------------------------------------------------

LHP's streaming_table write target generates ``dp.create_streaming_table()`` +
``@dp.append_flow()``. Streaming tables are optimal for append-only ingestion.

.. important::
   Joins in streaming tables do not recompute when dimensions change — use materialized
   views for enrichment.

.. _bp-12-3:

BP-12.3: Set ``pipelines.reset.allowed: "false"`` on history tables
--------------------------------------------------------------------

LHP supports ``table_properties`` in write targets. This prevents accidental full refresh
from destroying historical data:

.. code-block:: yaml
   :caption: Protecting history tables from reset

   write_target:
     type: streaming_table
     table_properties:
       pipelines.reset.allowed: "false"

.. tip::
   Put this in your ``silver_standard`` and ``gold_standard`` presets.

.. _bp-12-4:

BP-12.4: Use ``cluster_columns`` (liquid clustering) instead of ``partition_columns``
-------------------------------------------------------------------------------------

LHP supports both, but liquid clustering is the modern recommendation. It's incremental,
allows redefining keys without rewriting data, and works well with high-cardinality columns:

.. code-block:: yaml
   :caption: Liquid clustering

   write_target:
     type: streaming_table
     cluster_columns: [customer_id, order_date]

.. _bp-12-5:

BP-12.5: Use ``comment`` on every write target
-----------------------------------------------

LHP passes the ``comment`` field to the generated table/view definition. This appears in
Unity Catalog UI and is queryable.

.. _bp-12-6:

BP-12.6: Use ``spark_conf`` for per-table performance tuning
-------------------------------------------------------------

LHP supports ``spark_conf`` on write targets. Use it for adaptive shuffle or per-table
optimisations rather than global pipeline settings.

.. _bp-12-7:

BP-12.7: For CDC, use the ``cdc`` mode with explicit ``cdc_config``
--------------------------------------------------------------------

LHP generates ``dp.create_auto_cdc_flow()`` with full support for ``keys``,
``sequence_by`` (including STRUCT for tie-breaking), ``scd_type`` (1 or 2),
``apply_as_deletes``, ``ignore_null_updates``, ``track_history_column_list``, and
``track_history_except_column_list`` options. Always specify ``sequence_by`` explicitly.

.. _bp-12-8:

BP-12.8: Use ``once: true`` for backfill flows
-----------------------------------------------

LHP supports the ``once`` flag on individual actions, generating one-time flows for
historical data backfill without affecting the ongoing streaming ingestion.

.. _bp-12-9:

BP-12.9: Multiple write actions targeting the same table are automatically grouped
----------------------------------------------------------------------------------

LHP consolidates multiple sources writing to the same streaming table into one
``create_streaming_table`` with multiple ``append_flow`` functions. Use this for
multi-source ingestion patterns.

.. _bp-12-10:

BP-12.10: Use ``snapshot_cdc`` mode for full-snapshot change data capture
--------------------------------------------------------------------------

LHP also supports ``mode: "snapshot_cdc"`` on streaming tables, generating
``dp.create_auto_cdc_from_snapshot_flow()``. Use this when your source provides full
snapshots (not a change feed) and you want LHP to detect changes automatically.

Configuration uses ``snapshot_cdc_config`` (not ``cdc_config``):

.. code-block:: yaml
   :caption: Snapshot CDC configuration

   write_target:
     type: streaming_table
     streaming_table_config:
       mode: "snapshot_cdc"
       snapshot_cdc_config:
         source_function:
           file: "functions/my_snapshots.py"
           function: "my_snapshot_function"
         keys: [id]
         stored_as_scd_type: 2

Key differences from ``cdc`` mode:

- Config key is ``snapshot_cdc_config`` (not ``cdc_config``)
- SCD type field is ``stored_as_scd_type`` (not ``scd_type``)
- Requires a ``source_function`` with ``file`` and ``function`` fields
- Does not use ``sequence_by`` — ordering is implicit from snapshot timing

.. _bp-12-11:

BP-12.11: Use ``sink`` write targets for streaming to external destinations
---------------------------------------------------------------------------

LHP supports a ``sink`` write target type for writing to external systems. Four sink
subtypes are available:

- **delta** — write to external Delta tables outside Unity Catalog (e.g., cross-workspace or external storage)
- **kafka** — write to Kafka or Azure Event Hubs for event-driven architectures
- **custom** — use a custom DataSink V2 class via the ``custom_sink_class`` config field
- **foreachbatch** — ForEachBatch handlers for custom per-batch processing (API calls, notifications, etc.)

.. code-block:: yaml
   :caption: Kafka sink example

   write_target:
     type: sink
     sink_type: kafka
     sink_config:
       kafka.bootstrap.servers: "${KAFKA_BROKERS}"
       topic: "enriched_orders"

Use sinks when data must leave the lakehouse — for downstream consumers, event buses, or
external APIs. Pair with streaming tables for the primary lakehouse copy.

.. seealso::
   :doc:`actions/write_actions` for the full write action specification.


13. Data Quality (Expectations)
===============================

.. _bp-13-1:

BP-13.1: Tier expectations by medallion layer
----------------------------------------------

- **Bronze**: ``warn`` only — never drop or fail at bronze. Every raw record is precious.
- **Silver**: ``drop`` for structural quality rules. Route violations to a quarantine table.
- **Gold/Critical**: ``fail`` for reference table integrity and business-critical invariants.

LHP's DQE parser supports ``failureAction: fail|drop|warn`` in expectation files and
generates the appropriate decorators.

.. seealso::
   For configuring quarantine mode in LHP, see :doc:`quarantine`.

.. _bp-13-2:

BP-13.2: Centralise expectation definitions in external DQE files
-----------------------------------------------------------------

LHP supports ``expectations_file`` pointing to YAML/JSON files. Store these in
``expectations/<domain>/`` and reference them from multiple actions. This enables reuse and
independent review of quality rules.

.. _bp-13-3:

BP-13.3: Name expectations descriptively
-----------------------------------------

Convention: ``valid_<column>_<constraint_type>`` (e.g., ``valid_order_id_not_null``,
``valid_amount_positive``). These names appear in the DLT Data Quality tab and event log.


.. _bp-13-5:

BP-13.5: Use test actions for cross-table validation
----------------------------------------------------

LHP's 9 test action types (``row_count``, ``uniqueness``, ``referential_integrity``,
``completeness``, ``range``, ``schema_match``, ``all_lookups_found``, ``custom_sql``,
``custom_expectations``) generate SQL-based validation views. Use ``--include-tests`` flag
to generate them. Always run these in staging before production deployment.

To publish test results to external systems like Azure DevOps or a Delta audit table,
see :doc:`actions/test_reporting`.

.. seealso::
   :doc:`actions/test_actions` for the full test action specification.


14. Operational Metadata
========================

.. _bp-14-1:

BP-14.1: Define operational metadata columns in ``lhp.yaml``
-------------------------------------------------------------

LHP supports project-level ``operational_metadata`` with column definitions, presets, and
defaults. Define standard columns once:

.. code-block:: yaml
   :caption: Operational metadata configuration in lhp.yaml

   operational_metadata:
     columns:
       ingest_timestamp:
         expression: "F.current_timestamp()"
         description: "When the record was ingested"
         applies_to: [streaming_table, materialized_view]
       source_file:
         expression: "F.input_file_name()"
         description: "Source file path"
         applies_to: [streaming_table]
         enabled: true
       pipeline_id:
         expression: "F.lit(spark.conf.get('pipelines.id'))"
         description: "Pipeline identifier"
         additional_imports:
           - "from pyspark.sql import functions as F"

Each column config supports these fields:

- ``expression`` (required) — PySpark expression string
- ``description`` — Human-readable description
- ``applies_to`` — List of target types (default: ``[streaming_table, materialized_view]``)
- ``enabled`` — Boolean to enable/disable the column (default: ``true``)
- ``additional_imports`` — List of extra Python import statements needed by the expression

.. _bp-14-2:

BP-14.2: Create metadata presets for different layers
-----------------------------------------------------

LHP supports ``operational_metadata.presets`` for named groups in ``lhp.yaml``:

.. code-block:: yaml
   :caption: Metadata presets

   operational_metadata:
     presets:
       bronze_standard: [ingest_timestamp, source_file, pipeline_id]
       silver_standard: [updated_at, pipeline_run_id]

.. note::
   Metadata presets are defined at the project level for documentation and organisational
   purposes. At the flowgroup or action level, ``operational_metadata`` accepts either
   ``true`` (to enable all columns) or an explicit list of column name strings — not preset
   names. Reference the preset definitions as a guide when writing the column name lists in
   your flowgroups.

.. _bp-14-3:

BP-14.3: Metadata is additive across preset, flowgroup, and action levels
-------------------------------------------------------------------------

LHP deep-merges operational metadata with deduplication. This means you can set a baseline
in a preset and add columns at the flowgroup or action level without losing the preset
columns.

.. _bp-14-4:

BP-14.4: Use ``applies_to`` to control which target types get each column
-------------------------------------------------------------------------

``input_file_name()`` is only valid in streaming/batch reads — set
``applies_to: [streaming_table]``. ``current_timestamp()`` works everywhere — set
``applies_to: [streaming_table, materialized_view]``.

.. seealso::
   :doc:`operational_metadata` for the full operational metadata reference.


15. Schema Management
=====================

.. _bp-15-1:

BP-15.1: Use schema files for bronze layer schema definition
------------------------------------------------------------

LHP's ``schema_file`` field in load actions points to external DDL, YAML, or JSON schema
files. This makes schema definitions reviewable independently of pipeline config.

.. _bp-15-2:

BP-15.2: Use schema transforms at the bronze-to-silver boundary
----------------------------------------------------------------

LHP's ``schema`` transform type provides explicit column control:

- Arrow syntax for renaming: ``old_col -> new_col``
- Type casting: ``amount: decimal(18,2)``
- Strict enforcement to reject unexpected columns

.. _bp-15-3:

BP-15.3: Use ``enforcement: strict`` at silver to prevent schema drift
----------------------------------------------------------------------

LHP's schema transform with ``enforcement: strict`` generates code that only keeps declared
columns. Combined with silver-layer DQE expectations, this creates a clean schema contract
between bronze and silver.


16. Validation & CI Integration
===============================

.. _bp-16-1:

BP-16.1: Run ``lhp validate`` as a blocking CI check on every PR
-----------------------------------------------------------------

LHP's validation stack catches: missing required fields, unknown fields (with fuzzy-match
suggestions), circular dependencies, invalid references, template parameter mismatches, and
type-specific validation for all 7 load types, 5 transform types, and all write target
types.

.. _bp-16-2:

BP-16.2: Run ``lhp generate --dry-run`` to verify code generation
------------------------------------------------------------------

Dry-run generates code without writing files. Use this in CI to catch generation errors
early.

.. _bp-16-3:

BP-16.3: Maintain dry-run baselines for regression detection
------------------------------------------------------------

Commit expected generated output to the repo. In CI, run ``lhp generate --dry-run`` and
diff against baselines. Unexpected changes (especially from preset modifications) are
flagged for review. This is the config-equivalent of snapshot testing.

.. _bp-16-4:

BP-16.4: Layer your CI validation pipeline
------------------------------------------

.. list-table::
   :header-rows: 1
   :widths: 15 35 25

   * - Layer
     - What it checks
     - Tool
   * - Syntax
     - Valid YAML, correct indentation
     - ``yamllint``
   * - Schema
     - Required fields, correct types
     - JSON Schema (LHP provides schemas in ``src/lhp/schemas/``)
   * - Semantic
     - References resolve, no circular deps
     - ``lhp validate --env <env>``
   * - Generation
     - Config generates valid Python
     - ``lhp generate --dry-run --env <env>``
   * - Regression
     - No unintended diff in output
     - Baseline comparison
   * - Functional
     - Test actions pass
     - ``pytest`` with ``--include-tests``

.. seealso::
   :doc:`cicd_reference` for comprehensive CI/CD patterns and deployment strategies.


17. State Management & Incremental Generation
==============================================

.. _bp-17-1:

BP-17.1: DO NOT Commit ``.lhp_state.json`` to version control
-------------------------------------------------------

LHP's state tracking enables smart regeneration — only files whose source YAML,
dependencies, or generation context changed are regenerated. This significantly speeds up
``lhp generate`` for large projects but must not be committed to source control

.. _bp-17-2:

BP-17.2: Use ``lhp state`` to audit orphaned and stale files
-------------------------------------------------------------

After refactoring (renaming flowgroups, deleting pipelines), use the available flags to
audit and manage state:

.. list-table::
   :header-rows: 1
   :widths: 20 50

   * - Flag
     - Purpose
   * - ``--orphaned``
     - Show generated files with no corresponding source YAML
   * - ``--stale``
     - Show files where the source YAML has changed since last generation
   * - ``--new``
     - Show new/untracked YAML files that haven't been generated yet
   * - ``--cleanup``
     - Remove orphaned files
   * - ``--regen``
     - Regenerate stale files
   * - ``--dry-run``
     - Preview cleanup or regen without actually modifying files

Combine filters: ``lhp state --env dev --orphaned --cleanup --dry-run`` previews which
orphaned files would be deleted.

.. _bp-17-3:

BP-17.3: Use ``--force`` only when necessary
---------------------------------------------

LHP's ``ForceGenerationStrategy`` regenerates everything. Use it only after framework
upgrades or preset changes where you want to verify all output. Normal development should
rely on smart generation.

.. seealso::
   :doc:`cli` for the full ``lhp state`` command reference.


18. Bundle Integration (Databricks Asset Bundles)
=================================================

.. _bp-18-1:

BP-18.1: Use ``lhp deps --format job`` to generate DAB job resource definitions
--------------------------------------------------------------------------------

LHP analyses dependencies and generates pipeline and job resource YAML for Databricks
Asset Bundles. Use ``--bundle-output`` to specify where bundle files are written.

.. _bp-18-2:

BP-18.2: Bundle scaffolding is included by default
---------------------------------------------------

LHP scaffolds the full DAB structure by default with ``lhp init``, including
``databricks.yml``, resource definitions, and standard folder layout. Use
``lhp init <name> --no-bundle`` to skip DAB setup if you manage bundle configuration
separately.

.. _bp-18-3:

BP-18.3: Keep generated bundle resources separate from hand-written ones
------------------------------------------------------------------------

LHP generates bundle resources from dependency analysis. Store them in a dedicated
directory (e.g., ``bundle/generated/``) so they can be regenerated without conflicting
with manually defined resources.

.. seealso::
   :doc:`databricks_bundles` for the full bundle integration guide.


19. Architectural Pattern Support
=================================

.. _bp-19-1:

BP-19.1: Medallion architecture — use LHP's layered approach
-------------------------------------------------------------

.. list-table::
   :header-rows: 1
   :widths: 10 20 15 20 35

   * - Layer
     - Write Target
     - DQE Tier
     - Metadata
     - Key Characteristics
   * - Bronze
     - Streaming table
     - ``warn`` only
     - ingest_timestamp, source_file
     - Raw ingestion, CloudFiles/Kafka, schema rescue
   * - Silver
     - Materialized view
     - ``drop`` bad rows
     - updated_at, pipeline_run_id
     - Validated, deduplicated, schema-enforced
   * - Gold
     - Materialized view
     - ``fail`` on critical
     - (inherited)
     - Aggregations, denormalised reporting

LHP supports all these natively through its action types, write targets, and DQE
integration.

.. _bp-19-2:

BP-19.2: Environment promotion — use substitution files per environment
-----------------------------------------------------------------------

Same YAML configs, different ``--env`` flags. LHP resolves all tokens per environment.
Generated code is environment-specific but source configs are environment-agnostic.

.. _bp-19-3:

BP-19.3: Multi-pipeline orchestration — use ``job_name`` and ``lhp deps``
--------------------------------------------------------------------------

LHP's dependency analysis produces pipeline-level and job-level dependency graphs. Use
these to build Databricks Workflow orchestration that respects data dependencies across
pipelines.

.. seealso::
   :doc:`dependency_analysis` for pipeline dependency analysis and orchestration job
   generation.

.. _bp-19-4:

BP-19.4: Multi-source ingestion — use multiple load/write actions targeting the same table
------------------------------------------------------------------------------------------

LHP consolidates multiple write actions to the same streaming table into multiple
``append_flow`` functions. This supports fan-in patterns (multiple sources -> one table)
natively.


20. Documentation & Discoverability
====================================

.. _bp-20-1:

BP-20.1: Use ``description`` fields on every action and write target
--------------------------------------------------------------------

LHP passes descriptions through to generated code comments and table metadata. Fill these
in consistently.

.. _bp-20-2:

BP-20.2: Use ``comment`` on write targets for Unity Catalog table descriptions
------------------------------------------------------------------------------

These appear in the Data Explorer and are queryable. Make them meaningful:
"Silver layer orders — deduped, validated, enriched with customer data."

.. _bp-20-3:

BP-20.3: Use YAML comments for "why" decisions
-----------------------------------------------

.. code-block:: yaml
   :caption: Comments explaining decisions

   # Using batch mode because source schema changes frequently and CDC is not supported
   readMode: batch

The YAML declares *what*; comments explain *why*.

.. _bp-20-4:

BP-20.4: Use ``lhp info`` and ``lhp stats`` for project documentation
----------------------------------------------------------------------

These commands produce summaries of project structure, pipeline counts, and action
distributions. Use them in onboarding documentation.

.. seealso::
   :doc:`cli` for the full CLI command reference.


21. Anti-Patterns to Avoid
==========================

.. warning::
   The following are common mistakes that undermine the value of using LHP. Each
   anti-pattern lists the impact and the recommended fix.

.. list-table::
   :header-rows: 1
   :widths: 5 20 35 30

   * - ID
     - Anti-Pattern
     - Why It's Harmful
     - Fix
   * - AP-1
     - Hardcoding catalog/schema names in YAML
     - Makes environment promotion impossible
     - Always use substitution tokens
   * - AP-2
     - Using ``expect_or_fail`` at bronze
     - One bad record stops the entire pipeline
     - Use ``warn`` at bronze; reserve ``fail`` for critical tables
   * - AP-3
     - Skipping ``lhp validate`` before ``lhp generate``
     - Generation errors from invalid config are harder to diagnose
     - Always validate first
   * - AP-4
     - Using streaming tables for join-based enrichment
     - Streaming tables don't recompute when dimensions change
     - Use materialized views for any join with updating dimensions
   * - AP-5
     - Building templates before understanding the pattern
     - Leads to over-generalised, hard-to-use templates
     - Write 3+ concrete flowgroups first, then extract
   * - AP-6
     - Treating preset changes as low-risk
     - A global preset change affects every pipeline using it
     - Validate the full project after any preset change
   * - AP-7
     - Not using operational metadata
     - Debugging production issues without audit columns is very hard
     - Use LHP's operational metadata system consistently
   * - AP-8
     - Monolithic YAML files
     - Unreadable, unreviewable, untestable
     - One pipeline per file
   * - AP-9
     - Secrets in substitution files
     - Secrets in version control will be leaked
     - Use ``${secret:scope/key}`` syntax exclusively
   * - AP-10
     - Ignoring ``_rescued_data`` column
     - Schema mismatches without rescue silently drop data
     - Always enable ``cloudFiles.rescuedDataColumn`` at bronze
   * - AP-11
     - Dumping all SQL files in a flat ``sql/`` directory
     - At 100+ SQL files, finding the right one is painful
     - Use ``sql/<system>/<layer>/`` subdirectories
   * - AP-12
     - Using subdirectories for templates or presets
     - LHP only discovers flat ``*.yaml`` in these directories
     - Use prefix-based naming instead (see :ref:`Section 2 <bp-2-6>`)
   * - AP-13
     - Generic names without system/layer context
     - ``pipeline_1``, ``ingest.yaml``, ``transform.sql`` are meaningless at scale
     - Use ID-based naming: ``erp_brz_raw_orders`` (see :ref:`Section 3 <bp-3-7>`)
   * - .. _ap-14:

       AP-14
     - Building cross-system, multi-layer god-blueprints
     - One blueprint covering many systems × many layers couples unrelated release
       cycles, hides scope, and produces an enormous expansion blast radius
     - Prefer one blueprint per ``(system, layer)`` and use Case 2's
       ``pipelines/<system>/instances/`` only for genuine multi-layer cases
       (see :ref:`Section 5 <bp-5-2>`)
