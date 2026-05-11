Project Structure
=================

.. meta::
   :description: Reasoning behind LHP project layout, directory organisation, naming conventions, and template and preset prefix patterns.

For the step-by-step version of laying out a project, see :doc:`../quickstart`.
This page explains why the layout looks the way it does and where the rough
edges are.

Why directory layout outlives the project
-----------------------------------------

LHP discovers files by walking specific directories. The discovery rules are
not symmetric — some directories support recursion and others do not — and
the difference shapes every other organisational choice.

``pipelines/``, ``sql/``, ``schemas/``, ``expectations/``, and
``python_modules/`` are discovered recursively. You can nest as deep as you
want; LHP finds files by glob pattern at any depth. ``presets/``,
``templates/``, and ``substitutions/`` are flat: discovery uses ``glob("*.yaml")``,
not ``rglob``, so subdirectories under these are silently ignored.

The asymmetry exists because pipelines, SQL, schemas, expectations, and
Python modules belong to a specific data domain or layer — putting them
under a subdirectory communicates that scope. Presets, templates, and
substitutions are project-wide reusable assets; nesting them would suggest
scope they do not actually have.

This single fact drives the two organisational rules that matter most:

- For directories that support subdirectories, use them. Mirror the
  domain/layer hierarchy across ``pipelines/``, ``sql/``, ``schemas/``, and
  ``expectations/``.
- For directories that do not, use a prefix in the filename to encode the
  scope a folder would have given you.

Domain-first, not action-first
------------------------------

Group ``pipelines/`` by data domain, not by action type. ``pipelines/orders/``
beats ``pipelines/loads/`` for the same reason most code is grouped by
feature rather than by language construct: people work on a domain at a time.
A CODEOWNERS rule that says "Team A owns ``pipelines/orders/``" maps to
reality. A CODEOWNERS rule that says "Team A owns all ``load`` actions
across the project" does not.

Within each domain, group by medallion layer:

.. code-block:: text
   :caption: Pipeline directory shape

   pipelines/
     erp/
       bronze/
       silver/
       gold/
     crm/
       bronze/
       silver/
     shared/
       gold/                              # cross-system aggregates

The same layout in ``sql/``, ``schemas/``, and ``expectations/`` lets a
reviewer who is reading ``pipelines/erp/silver/orders_enriched.yaml`` find
the referenced SQL at ``sql/erp/silver/enrich_orders.sql`` without thinking.
Mirrored structures eliminate one cognitive switch.

Single-purpose YAML files
-------------------------

Aim for 50-200 lines per YAML file. The number is not magic — it is the
size at which one PR can sensibly review one file. Monolithic files with
fifteen or more :term:`FlowGroups <FlowGroup>` become unreadable, and ``lhp validate`` errors
become harder to triage because the line number alone does not tell you
which FlowGroup is failing.

LHP supports multi-document (``---``) and array (``flowgroups:``) syntax
for cases where several FlowGroups share a pipeline. Use that syntax when
the FlowGroups genuinely belong together — they share ``pipeline``,
``presets``, ``operational_metadata``, or ``job_name``. Do not use it as
a workaround to keep the file count low; the FlowGroup boundary exists
for a reason.

See :doc:`../multi_flowgroup_guide` for the array-syntax mechanics.

The flat-directory problem
--------------------------

Templates and presets cannot live in subdirectories. At three templates,
this is not a problem. At thirty, a flat alphabetical list becomes the
discoverability bottleneck. The fix is a prefix convention that encodes
layer and action type in the filename:

.. code-block:: text
   :caption: Template prefix pattern

   templates/
     TMPL001_brz_load_cloudfiles_standard.yaml
     TMPL002_brz_load_kafka_events.yaml
     TMPL004_slv_transform_sql_enrichment.yaml
     TMPL006_slv_write_st_with_dqe.yaml
     TMPL007_gld_write_mv_aggregation.yaml

The ``TMPLxxx_`` numeric prefix sorts templates by creation order in
``lhp list_templates`` output. The layer prefix (``brz_``, ``slv_``,
``gld_``) groups templates that belong together in the same alphabetical
neighbourhood. The descriptive suffix tells you what the template does
without opening it.

The same idea applies to presets, where the prefix encodes scope:

.. code-block:: text

   presets/
     global_defaults.yaml
     brz_standard.yaml
     brz_cloudfiles_json.yaml
     slv_cdc_scd2.yaml
     gld_standard.yaml

This is convention, not enforcement. LHP does not parse the prefix. But
the convention pays for itself when someone joins the project and has to
find the right template in under a minute.

Why the variant suffix matters in FlowGroup files
-------------------------------------------------

A FlowGroup file named ``erp_bronze_ingest_TMPL001.yaml`` carries two
useful pieces of information in its name: the FlowGroup's data domain and
layer, and the template it uses. When you see a generated file
``erp_brz_raw_orders.py`` in a PR diff, you can find the source YAML
without grepping. When you see ``TMPL001`` in a filename, you know which
template changes would cascade through.

The mirror rule applies to identifiers everywhere. ``snake_case`` for
pipelines, FlowGroups, actions, templates, presets, and variables —
because action names become Python function names, and they have to be
valid identifiers. ``${SCREAMING_SNAKE_CASE}`` for environment tokens
because they are constants resolved at generation time.
``%{lower_snake_case}`` for local variables because they are
FlowGroup-scoped. ``{{ snake_case }}`` for template parameters because
Jinja2 renders them. The case alone tells you which substitution layer
applies.

Anti-patterns
-------------

The following organisational choices look reasonable on day one and break
on day ninety. Each is followed by the recommended alternative.

**Generic names without system/layer context.** ``pipeline_1``,
``ingest.yaml``, ``transform.sql`` mean nothing at five hundred FlowGroups.
A name has to survive being read out of context — in a log line, a Git
blame, a Databricks UI list. ``erp_brz_raw_orders`` does; ``pipeline_v2``
does not.

**Subdirectories under ``templates/`` or ``presets/``.** They are not
discovered. The user files a bug about a template not being found, you
investigate, and the answer is that the discovery glob is ``glob("*.yaml")``
not ``rglob``. Use prefix-based naming.

**Dumping all SQL files in a flat ``sql/`` directory.** At one hundred SQL
files, finding ``enrich_orders.sql`` becomes painful. The recursive
discovery means there is no reason not to mirror
``sql/<system>/<layer>/<description>.sql``.

**Monolithic YAML files.** Fifteen FlowGroups in one file is unreadable
and unreviewable. One reviewable unit per file; multi-document syntax
only when FlowGroups genuinely share inheritable fields.

**Cross-system, multi-layer god-blueprints.** A blueprint that spans
several systems and several layers couples release cycles that should be
independent. The default is one blueprint per ``(system, layer)`` pair.
The escape hatch — a multi-layer blueprint with its instance files in
``pipelines/<system>/instances/`` — exists for the rare case where the
shape really is cross-layer, and signals that the blast radius is wide.

CODEOWNERS as the structural enforcement layer
----------------------------------------------

The directory layout pays off most when paired with a ``CODEOWNERS`` file
at the repo root. ``CODEOWNERS`` is a Git platform feature (GitHub,
GitLab, Azure DevOps) that names required reviewers for PRs touching
specific paths. The directory shape determines what CODEOWNERS rules you
can write.

The natural mapping puts the platform team on shared assets and the
domain teams on their pipelines:

.. code-block:: text
   :caption: Example CODEOWNERS

   /presets/                @platform-team
   /substitutions/          @platform-team
   /templates/              @platform-team
   /pipelines/erp/          @erp-team
   /pipelines/crm/          @crm-team

A change to a preset — say, defaulting all bronze tables to a different
schema evolution mode — affects every pipeline that uses it. Without
CODEOWNERS, that PR can merge with no input from someone who understands
the blast radius. With CODEOWNERS, the platform team is on the review
automatically.

The reason this matters more than the typical CODEOWNERS use case is
that LHP intentionally moves shared behaviour into shared assets. The
whole point of presets and templates is leverage. The same leverage
makes mistakes scale.

See also
--------

- :doc:`../substitutions` for the substitution syntax that pairs with the
  naming conventions on this page.
- :doc:`../templates_reference` and :doc:`../presets_reference` for the
  reusable-asset specifications.
- :doc:`../blueprints` for the blueprint instance-file layout decisions.
