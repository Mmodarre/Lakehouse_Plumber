Governance
==========

.. meta::
   :description: Reasoning behind LHP governance choices — Unity Catalog mapping, operational metadata, table comments, lineage, and Databricks Asset Bundle integration.

For the step-by-step procedure for enabling :term:`Databricks Asset Bundle <DAB>` (DAB)
integration, see :doc:`../configure_bundles`. This page explains the
governance choices the framework supports and how they fit together.

Where governance lives in an LHP project
----------------------------------------

LHP touches three governance surfaces. Unity Catalog (UC) gets table
names, comments, and column metadata from the FlowGroup definitions.
The generated Python carries audit columns through operational metadata.
The Databricks Asset Bundle wraps generated pipelines into deployable
resource definitions with environment-specific permissions.

Each surface answers a different question. UC governance answers "what
does this table represent and who can read it?". Operational metadata
answers "where did this row come from and when did it land?". Bundle
configuration answers "how does this pipeline get from a Git commit to
a running pipeline in a workspace?".

The pages on this site treat them as separate concerns because they
have different release cycles and different review audiences. A change
to a table comment is low-risk and platform-team-reviewed. A change to
bundle permissions affects which humans can run which pipelines and
needs security review.

Unity Catalog naming as a flow of substitutions
-----------------------------------------------

Every write target in LHP produces a Unity Catalog object —
``catalog.schema.table``. The three-part name is the most common piece
of configuration that has to differ across environments, and the
recommended pattern is to source every part from a substitution token:

.. code-block:: yaml

   write_target:
     type: streaming_table
     database: "${SILVER_CATALOG}.${SILVER_SCHEMA}"
     table: "%{entity}_silver"
     comment: "Silver layer for %{entity}, validated and enriched."

The catalog and schema come from substitution tokens, so the dev
pipeline writes to ``main_silver_dev.orders.customers_silver`` and the
prod pipeline writes to ``main_silver_prod.orders.customers_silver``
from the same YAML. The local variable ``%{entity}`` keeps the
FlowGroup parameterised by entity name; the comment string carries the
same.

The medallion-token convention pairs with this naturally. A standard
substitution-token set — ``bronze_catalog``, ``silver_catalog``,
``gold_catalog`` — gives every write target a predictable home and
keeps the substitution files small.

Comments are queryable governance
---------------------------------

The ``comment`` field on write targets propagates to the
generated ``@dp.table`` decorator as a ``comment=...`` argument.
Databricks stores this comment as table metadata; it shows up in the
Data Explorer UI and is queryable through ``DESCRIBE TABLE EXTENDED``
or the UC information schema.

This is the single highest-leverage governance field LHP exposes. A
team that fills in meaningful comments on every silver and gold table
("Silver layer orders — deduped, validated, enriched with customer
data; refresh every 15 minutes; owner: erp-team") makes the data
discoverable to downstream consumers without any separate catalog
tool. A team that leaves comments blank — or fills them with the
table name repeated — gets a UC catalog full of opaque table names.

The same applies at the column level through schema transforms,
where each renamed column can carry a description. The descriptions
flow into the table's column metadata.

Operational metadata as an audit trail
--------------------------------------

Every row in a generated table can carry standard audit columns
through LHP's :term:`operational metadata <Operational metadata>` system. The columns are defined
once at the project level in ``lhp.yaml``:

.. code-block:: yaml
   :caption: lhp.yaml operational metadata block

   operational_metadata:
     columns:
       ingest_timestamp:
         expression: "F.current_timestamp()"
         description: "When the record was ingested"
         applies_to: [streaming_table, materialized_view]
       source_file:
         expression: "F.input_file_name()"
         description: "Source file path (CloudFiles only)"
         applies_to: [streaming_table]
       pipeline_run_id:
         expression: "F.lit(spark.conf.get('pipelines.id'))"
         description: "Pipeline run identifier"

The fields are straightforward. ``expression`` is the PySpark
expression that produces the column value; LHP inlines it into the
generated code. ``description`` becomes column metadata.
``applies_to`` limits which write-target types receive the column —
``input_file_name()`` only makes sense in CloudFiles-backed streaming
tables and produces an error elsewhere. ``additional_imports`` adds
import statements the expression needs.

Layer presets bundle the metadata columns appropriate to each
layer. A ``bronze_standard`` preset declares ``ingest_timestamp``,
``source_file``, and ``pipeline_run_id``. A ``silver_standard`` preset
declares ``updated_at`` (the silver-layer equivalent). Operational
metadata is *additive* across preset, FlowGroup, and action levels —
LHP deep-merges with deduplication, so a FlowGroup can add a column
without losing the preset's defaults.

The payoff is post-hoc debugging. When a downstream report shows
suspicious numbers, the operational metadata columns let you trace
exactly which pipeline run wrote which rows, when, and from which
source file. Without them, you reverse-engineer that information from
DLT logs and timestamps, which is much slower.

Lineage from generated code
---------------------------

Unity Catalog's automatic lineage tracker watches Spark execution and
records read/write relationships between tables. LHP's generated
Python uses standard Spark and DLT decorators, so the lineage tracker
sees every read and write without LHP doing anything special.

The implication is that LHP's job is to generate code that names
tables consistently — the same ``${SILVER_CATALOG}.orders.customers_silver``
across pipelines — so the lineage graph in UC connects the right
nodes. Inconsistent table naming across pipelines (one FlowGroup
writes to ``main.orders.customers``, another reads from
``catalog.orders.customers``) produces a lineage graph with
disconnected components.

The substitution layer is the enforcement mechanism. A
project-wide ``${SILVER_CATALOG}`` token means every FlowGroup that
reads or writes silver tables uses the same catalog name. Hardcoded
catalog names are the predictable source of broken lineage.

Bundle integration as deployment governance
-------------------------------------------

The Databricks Asset Bundle layer turns FlowGroups into deployable
units. LHP scaffolds the bundle structure by default with ``lhp init``
(opt out with ``--no-bundle`` if you manage bundles separately). The
generated structure includes ``databricks.yml``, per-environment
target configuration, and resource files (``*.pipeline.yml``,
``*.job.yml``) that point at the generated Python.

``lhp deps --format job`` produces job resource definitions from the
dependency analysis. The graph of FlowGroups and their cross-pipeline
dependencies determines the task graph in the generated job. This
gives you an orchestrated multi-pipeline deployment from the same
declarative source, with the dependency edges deduced from the data
rather than maintained by hand.

The recommendation to keep generated bundle resources in a dedicated
directory (``bundle/generated/`` or similar) is operational hygiene.
Generated and hand-written resources should not share a directory
because regeneration overwrites everything in its output path. A
clean separation means you can regenerate bundle resources freely
without worrying about losing manual additions.

Per-environment bundle targets pair with the substitution-file
pattern. The bundle's ``targets.dev`` and ``targets.prod`` entries
control workspace, permissions, and tags. The substitution file
controls the catalog and schema names the generated code writes to.
Together they specify both *where* the pipeline deploys and *what
data* it touches. Keep the two layers separate; do not try to encode
catalog overrides in the bundle target file.

DQE tiering as a governance lever
---------------------------------

The data-quality tier per layer is itself a governance decision.
Bronze ``warn``-only is permissive on purpose — raw data is precious,
even bad raw data. Silver ``drop`` is where you commit to a contract:
bad rows do not propagate, but you save them through a quarantine
table for review. Gold ``fail`` is the strict tier reserved for
business-critical invariants (a primary-key uniqueness violation, a
referential-integrity failure on a reference table).

Each tier corresponds to a different governance posture. Bronze says
"we will keep everything and triage later". Silver says "downstream
consumers can trust this layer's schema". Gold says "if this is
wrong, stop the pipeline". The decision belongs to the data steward
of each domain, not the pipeline author — which is why DQE files
live in ``expectations/`` separately from FlowGroup YAML and benefit
from CODEOWNERS scoping.

Test reporting for audit
------------------------

LHP's test actions generate validation views. Their results can be
published to Azure DevOps test reports or a Delta audit table —
turning pipeline tests into a governance signal that surfaces in the
same dashboards as application tests. See
:doc:`../actions/test_reporting` for the publishing mechanics.

Anti-patterns
-------------

**Not using operational metadata.** Debugging production data
incidents without ingest timestamps and source file paths is much
harder than with them. The cost of adding the metadata is one preset
declaration; the cost of debugging without it is hours per incident.

**Blank or trivial table comments.** A UC catalog where every comment
is the table name repeated is no better than no catalog. Meaningful
descriptions pay for themselves the first time a downstream consumer
finds the right table without asking.

**Hardcoded catalog names.** Beyond breaking environment promotion,
they fragment UC lineage. Every catalog name should come from a
substitution token.

**Mixing generated and hand-written bundle resources in one
directory.** Regeneration overwrites everything in the output path.
Keep them in separate directories.

See also
--------

- :doc:`../configure_bundles` for the step-by-step Databricks Asset
  Bundle integration procedure.
- :doc:`../operational_metadata` for the full operational metadata
  reference, including all configuration fields.
- :doc:`../actions/test_reporting` for publishing test results to
  external audit systems.
- :doc:`environments` for the substitution patterns that pair with
  UC naming consistency.
