# Documentation diagram review

Scope: 133 RST documents in the current documentation tree, excluding shared includes and build output. Inventory covers every page and existing visual reference; the conceptual guides received closer prose review. The six priority additions (A–F) were approved, checked against code, generated and integrated. Secondary candidates (G–P) remain proposals. No application code was changed.

## Completed replacements

| Page | Message | Evidence checked |
| --- | --- | --- |
| `index.rst` | One or more loads → optional transforms → one or more writes; dotted, faded stacks represent additional actions. Direct load-to-write is valid. Presented as a common shape, not a requirement on standalone writes. | `src/lhp/models/_flowgroup.py`, `_action.py`; `src/lhp/core/codegen/grouping.py`; dependency resolver including self-contained writes; `docs/_fixtures/first_pipeline/` |
| `concepts/how-lhp-works.rst` | Build-time compilation produces owned Python; deployment separates this from Lakeflow execution. | `src/lhp/core/coordination/orchestrator.py`; `core/processing/flowgroup_resolver.py`; `core/codegen/coordinator.py`; `core/codegen/formatter.py` |
| `concepts/flowgroups-and-dependencies.rst` | Local views connect within a pipeline; a persisted table connects pipelines. | `src/lhp/core/dependencies/builder.py` pipeline/name index; `_producers.py` table index and matching |
| `guides/transform/quarantine.rst` | Clean path plus inbox repair, CDF selection, deduplicated outbox, reconstruction and revalidation before union. | `src/lhp/generators/transform/data_quality.py`; `src/lhp/templates/transform/data_quality_quarantine.py.j2`; `tests/e2e/test_quarantine_e2e.py` assertions; generated guide fixture |

Three Mermaid directives replaced; Mermaid extension and docs dependency removed. Existing quarantine artwork was found at `docs/_static/quarantine_data_flow.svg`; the guide now uses the newly generated PNG. The original unused artwork is retained. Quarantine reference points readers to the illustrated guide.

Ten images are now integrated: the four original replacements plus six approved additions. All use the built-in image generation tool, with visual review and import into the existing Superdesign project. Prompts and selected project-relative paths are in `diagram-prompts.json`; the approved batch and targeted edits are also preserved in `approved-prompts.json` and `approved-refinements.json`. Canvas records are in `diagram-canvas-assets.json`. They are local assets at `docs/_static/diagrams/`, with descriptive alternative text and keyboard-accessible full-size links. Warm illustration panels are deliberately retained in both page themes; no colour inversion is applied.

### Quarantine facts that constrain the visual

- LHP generates the subsystem; it does not automatically repair rows or create the inbox/outbox DDL.
- The quarantine branch applies the generated SQL inverse filter, not a separately invented classifier.
- The CDF stream selects fixed inserts/update postimages for the configured source table.
- The recycle sink keeps the latest commit per key in its batch and uses insert-only MERGE into the outbox. Editing a key already there does not replace that ledger row.
- The recycled view reconstructs columns from VARIANT and uses the recycled expectation set. The generator omits rules mentioning `_rescued_data`.
- Failed recycled rows are dropped. The code contains no automatic requeue from that view to the inbox.
- The picture does not promise immediate repair or a precisely timed run boundary. Existing guide timing prose was not independently runtime-tested.

## Approved additions — completed

The user approved the six priority concepts presented in the review (A–F), and requested the index action-count refinement. Each visual was defined from source before generation. Related guides link to the canonical illustrated explanation.

| ID | Asset | Evidence and limits |
| --- | --- | --- |
| A | `concept-map.png` | `models/_flowgroup.py`, `models/_action.py`, `core/processing/blueprint_expander.py`, `template_engine.py`, `flowgroup_resolver.py`. Blueprint instances expand flowgroups, templates append actions, presets apply settings. Pipeline grouping is determined by names. Tests are opt-in; shown action counts are examples. |
| B | `reuse-example.png` | `docs/_fixtures/guide_reuse_templates/` and `guide_reuse_blueprints/`, preset manager and flowgroup resolver. Template preset gives both readers maxFilesPerTrigger 200; orders' later flowgroup preset overrides to 1000. The blueprint row uses two of the fixture's three instances: two patterns times two instances gives four flowgroups. |
| C | `environment-resolution.png` | `core/processing/local_variables.py`, `flowgroup_resolver.py`, `substitution.py`, `core/codegen/secrets.py`, substitution guide fixture. Local-variable resolution precedes template expansion and presets; environment substitution follows. Secret references and scope aliases become lookup code; values are fetched at runtime. |
| D | `cdc-timelines.png` | `generators/write/streaming_table.py`, `templates/write/streaming_table.py.j2`, CDC and snapshot fixtures. The comparison uses three updates to one key and tracks tier for SCD2. [Databricks AUTO CDC](https://docs.databricks.com/aws/en/ldp/cdc) checked for runtime semantics: SCD1 current state, SCD2 versions, ordered snapshots as an alternate input. Generation does not itself apply changes. |
| E | `capability-overview.png` | `generators/registration.py`, `core/codegen/test_reporting.py`, `uc_tagging.py`, `core/sandbox/scope_resolver.py`, `bundle/manager.py`, `api/_skill_facade.py`, `api/_wheel_facade.py`, `webapp/app.py`, plus reuse, substitution, quarantine, dependency and monitoring implementations. Groups public capabilities without claims of market exclusivity or universal enablement. |
| F | `monitoring-architecture.png` | `core/coordination/monitoring_pipeline_builder.py`, `templates/monitoring/union_event_logs.py.j2`, `templates/bundle/monitoring_job_resource.yml.j2`. Sources selected at generation; independent available-now streams and checkpoints; shared Delta table; dependent view refresh. Default events_summary can be replaced or omitted. SDK job correlation is opt-in. The same table is drawn in each task to explain the handoff. |

Concept-map helper arrows and the shared-preset branch received targeted corrections after visual review. Nearby prose was corrected where it conflicted with the diagram contract: a flowgroup is a definition rather than a file or pipeline, blueprint expansion is not fixed to one pipeline, and local variables resolve before environment substitution rather than at parse time.

## Candidate inventory

A–F below are implemented as recorded above. G–P remain secondary proposals and need approval plus code verification before generation. Reuse shared visuals from related pages instead of making near-duplicates.

| ID | Priority | Proposed visual and question it answers | Main placement | Verification before generation |
| --- | --- | --- | --- | --- |
| A | First | **LHP concept map.** How YAML describes pipelines, flowgroups and actions; where presets, templates, blueprints and environment settings contribute. Distinguish containment, expansion and default application. | `concepts/how-lhp-works.rst`, linked from `build/compose.rst` | Models, blueprint expansion, flowgroup resolution, template engine, preset manager, code generation grouping. Do not imply one YAML file equals one pipeline or blueprint always equals one pipeline. |
| B | First | **A worked reuse example.** One pattern instantiated twice, with preset settings, template actions and blueprint flowgroups shown separately. What changes and what stays shared? | `concepts/presets-templates-blueprints.rst`, reused by the three reuse guides | Exact expansion and precedence against a real fixture, including explicit actions and settings. |
| C | First | **Environment and secret resolution.** One authoring source becomes dev/prod output; which values resolve when, and which remain runtime lookups? | `concepts/substitution-and-envs.rst`, linked from substitution guide | Local variables, template rendering, environment substitution and secret rewriting. Verify the order; do not turn the prose's four-tier model into an unsupported single pass. |
| D | First | **CDC timelines.** Follow a small record history through append, SCD1, SCD2 and snapshot CDC. Why do the resulting rows differ? | CDC and snapshot CDC guides | Write generators, templates, fixtures and tests; authoritative platform documentation for runtime semantics beyond what generated code proves. |
| E | First, user added | **Homepage capability overview: “What LHP handles for you.”** A grouped overview of authoring/reuse, processing, quality/recovery, delivery and operations. Make monitoring and quarantine prominent and link each group to its docs. | `index.rst`, after the introductory example | Inventory public features, flags and generated artifacts. Distinguish LHP automation from Lakeflow runtime features. Do not claim market exclusivity. Candidate inventory below. |
| F | First | **Monitoring architecture.** Pipeline event logs → collection/checkpoints → shared event table → summary views, with the monitoring job's task order. What gets generated and what runs? | `guides/ops/monitoring.rst`, linked from `operate/monitoring.rst` | Monitoring generation/templates, task dependencies, optional views and optional job monitoring. |
| G | Next | **Quality choices.** Warn, drop, fail and quarantine shown against the same bad row, beside a separate test-action branch. What happens to the row and to the update? | Data-quality and data-tests guides | DQE/test generation, expectation semantics, include-tests flag, reporting behavior. |
| H | Next | **Sandbox boundaries.** Two developers' produced tables and rewritten in-scope reads, alongside an unchanged shared external dimension. What is isolated? | `guides/develop/sandbox.rst` | Scope resolver, rewrite plan, table matching, profile rules, monitoring behavior. Do not portray this as workspace or security isolation. |
| I | Next | **Data dependencies become orchestration.** Producer/consumer edges roll into pipeline stages and job tasks, including independent branches that can run together. | `guides/ops/dependency-analysis.rst`, linked from `develop/jobs.rst` | Dependency builder, graph operations, job grouping, master job generation and explicit depends_on handling. |
| J | Next | **Generate, package, deploy, run.** Files and ownership at each boundary; optional wheel path shown separately. Which command creates which artifact? | `develop/bundles.rst`, linked from CI/CD and wheel packaging guides | Bundle manager, wheel packaging, job generation and command flags. Separate generation from Databricks deployment/execution. |
| K | Next | **Many flows, one table.** Independent source views and append flows converge on one declared target. How does multi-source ingestion differ from a join? | Auto Loader fan-in section and multi-flowgroup guide | Write grouping, create_table handling, append-flow generation. No unsupported failure-isolation guarantees. |
| L | Targeted | **Stream-static join.** Incremental orders meet a static customer lookup; distinguish stream inputs from table snapshots. | `guides/transform/sql.rst` | SQL pass-through, read modes and fixture; platform semantics if explaining refresh behavior. |
| M | Targeted | **Schema transformation before/after.** Columns renamed, cast, retained or omitted, with strict/permissive comparison. | `guides/transform/schema.rst` | Schema parser/generator and enforcement behavior; show actual small input/output column sets. |
| N | Targeted | **Test reporting route.** Test metrics → event hook → provider → external results, showing run correlation and failure constraints. | `guides/test/test-reporting.rst` | Reporting hook generation, provider contract, test IDs and warning/failure behavior. |
| O | Targeted | **Operational columns versus catalog tags.** Row-level metadata inside the table and descriptive tags attached to table/column objects. | `build/metadata.rst` | Metadata selection and UC tagging hook, supported targets and permissions/error handling. |
| P | Targeted | **Schema inference, evolution and rescued data.** A small incoming record shows where unexpected fields/types go and how quarantine can help. | Auto Loader schema-strategy section | Cloudfiles generator, declared options, fixtures and platform behavior. Keep runtime guarantees sourced. |

### Homepage overview candidate inventory (E)

The requested “all unique features” is best treated as a complete capability inventory feeding a concise visual. The homepage should show groups, with detail in linked pages; dozens of equally sized boxes would obscure the main message.

- **Author and reuse:** YAML-to-readable-Python generation; presets; templates; blueprints; local/environment substitutions and secret references; coding-agent skill; YAML schema/editor support; local web IDE.
- **Read and transform:** available ingestion sources; SQL/Python transformations; schema enforcement; intermediate tables; operational metadata.
- **Write and publish:** streaming append; CDC/SCD history; snapshot CDC; conditional replacement; materialized views; external sinks; Unity Catalog tagging.
- **Check and recover:** expectations; data tests; test reporting; quarantine with inbox/outbox recycling.
- **Develop and deliver:** sandbox namespacing; inferred and explicit dependencies; generated orchestration jobs; bundle integration; optional wheel packaging; validation and CI workflows.
- **Observe and operate:** centralized event-log monitoring; summary views; optional job correlation; diagnostic dependency graphs.

This inventory was checked against the implementation before generating the approved overview. It is not a statement that every item is unique to LHP or always enabled. The footer explicitly asks readers to enable the capabilities their project needs.

## Review coverage and deliberate exclusions

A page-level mapping follows in `diagram-coverage.tsv`. Conceptual pages and long guides are candidates above; short field-reference pages should link to the corresponding diagram rather than repeat it. Existing Web IDE, assistant and install screenshots should remain actual product screenshots. Navigation indexes, CLI/API listings, changelog, error catalog, telemetry and compact syntax tables do not need decorative illustrations.

Two wording issues surfaced while checking visual messages: the homepage's universal load/transform/write claim was narrowed because standalone write actions exist; the quarantine explanation now states the recycled-rule exception and drop behavior. Broader prose cleanup and runtime verification are outside this diagram pass.

## Verification of the approved batch

- Strict clean Sphinx HTML build: all 133 documents, warnings treated as errors, zero warnings.
- Browser checks: eight illustrated pages at 1440, 810 and 390 pixels, in light and dark themes (48 combinations); images load, alternative text and full-size links are present, no page overflow.
- All ten full-size diagram assets open from local file URLs. Desktop and mobile screenshots reviewed.
- Existing theme smoke suite passes 28 responsive page checks, navigation, native search, code copy, theme persistence, all ten agent prompts, and keyboard/dialog/mobile controls.
- Superdesign imports were fetched again and verified to retain the selected asset URLs.
