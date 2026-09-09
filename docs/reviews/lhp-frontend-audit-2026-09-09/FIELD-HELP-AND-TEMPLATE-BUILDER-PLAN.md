# Field guidance and template authoring: implementation plan

Status: approved and implemented on `feat/frontend-guidance-templates-092`. See [delivery and validation](FIELD-HELP-AND-TEMPLATE-BUILDER-IMPLEMENTATION.md); browser acceptance remains outstanding.

Reviewed on 9 September 2026 against `feat/frontend-workspace-ux-092`, commit `8517a62d3533134be78ffc507042cba4a992e7c8`, in `/private/tmp/lhp-frontend-workspace-ux-092`. This includes the earlier workspace/configuration improvements and descends from `origin/release/V0.9.2`.

Three subagents independently reviewed field guidance, the template frontend, and template engine/API behavior. The recommendations below combine those findings with direct source inspection and accessibility guidance.

## 1. Outcomes and priorities

1. **P0 — Help people make the right field-level decision.** Explain what a setting does, when to use it, accepted input, and what omission means. Fix incorrect contextual guidance and missing help before extending it.
2. **P0 — Correct template selection and parameter semantics.** The authoring UI must agree with the runtime about file references, required values, defaults, and supported syntax.
3. **P1 — Provide one discoverable template workspace.** A user can create or edit a reusable LHP template, declare parameters, build actions, try sample values, save, and create a flowgroup that invokes it.

“Template” here means a YAML definition under `templates/`, invoked through `use_template` and `template_parameters`.

## 2. Findings that shape the implementation

Paths below are relative to the reviewed worktree; line references describe the current implementation.

| Finding | Evidence | Required response |
| --- | --- | --- |
| Help is one string in a narrow tooltip, often repeating the field label. | `web_app/src/components/common/FieldHelp.tsx:5`; `src/lhp/schemas/flowgroup.schema.json:238,290,294` | Add visible essential hints and structured, readable field help. |
| Help lookup lacks action subtype, so the same YAML path can receive the wrong explanation. Auto Loader schema help mixes file paths, inline DDL and unrelated action meanings. | `web_app/src/hooks/useSchemaHelp.ts:6`; schema `flowgroup.schema.json:279`; `src/lhp/generators/load/cloudfiles.py:87` | Resolve help by document kind, action subtype and field path; reconcile docs with implementation. |
| Important pipeline/job controls have no help binding. | `PipelineCoreFields.tsx:63,111,121`; `JobCoreFields.tsx:85,165,219` under `web_app/src/components/config/` | Audit every surfaced field and record its binding or justified exclusion. |
| Resources → Templates opens a read-only summary; editing is available through a different Files entry point. | `web_app/src/components/shell/explorer/StructureLens.tsx:438`; `components/sidebar/FileBrowser.tsx:83` | Route every template entry point to the same path-owned editor. |
| Template details reuse flowgroup metadata, hiding parameters and omitting template name/version while exposing irrelevant fields. | `web_app/src/components/entity/EntityDetails.tsx:20,41`; `src/lhp/models/_template.py:10` | Provide template-specific Basics and Parameters sections. |
| Required-with-default can pass the creation UI and produce an invocation the runtime rejects; creation can enable before parameter metadata loads. | `web_app/src/components/editor/createFlowgroupSupport.ts:28`; `CreateFlowgroupDialog.tsx:143,211`; `template_engine.py:129` | Track supplied keys separately from default values; block creation until metadata is ready. |
| API selection confuses declared name and filename reference, omits nested templates, and loses absent/default-null distinction. | `src/lhp/webapp/routers/templates.py:48,74,90`; `src/lhp/api/_inspection_facade.py:340`; `_inspection_converters.py:276` | Add a path-based authoring catalog and lossless parameter metadata. |
| Parameter `type` is advisory metadata; raw-template schema rejects some valid action shapes, including test actions. | `src/lhp/models/_template.py:15`; `src/lhp/schemas/template.schema.json`; `docs/reference/config/templates.rst:81` | Describe actual semantics; separate template structure checks from rendered-action validation. |
| No preview resolves an unsaved template draft. | `src/lhp/webapp/routers/templates.py:1`; `src/lhp/api/_inspection_facade.py:123` | Add an in-memory preview through the production engine and public API. |

The prior UX branch already provides safe create-only writes through `IF_MATCH_CREATE_ONLY` in `web_app/src/api/files.ts` and the backend mutation lock. Reuse that behavior; a new persistence mechanism is unnecessary.

## 3. Field guidance design

### Interaction

Use three levels of guidance:

1. **Visible hint:** one short sentence beneath the label when users need a rule, format or consequence to fill in the field. Associate it with the input using `aria-describedby`, alongside any validation message.
2. **Expanded help:** clicking/tapping the information button, or pressing Enter/Space, opens a persistent popover with a title, explanation, relevant choices, examples, omission/inheritance behavior, and links to related guidance.
3. **Full documentation:** a descriptive link opens the matching documentation section. Keep the in-app explanation usable offline.

The popover supports text selection and example copying, Escape/close, focus restoration, keyboard traversal to links, viewport collision handling, and a comfortable 28–32px button target. A small icon can remain inside that target. Help remains available in viewer mode. Opening or copying help must never change source content or field values.

Do not place interactive links in a tooltip. The W3C tooltip pattern distinguishes tooltips from focusable help content; its APG page is explicitly a work in progress. Use an appropriately labelled nonmodal popover for interactive help. [W3C tooltip pattern](https://www.w3.org/WAI/ARIA/apg/patterns/tooltip/).

Essential instructions remain visible; optional detail can be disclosed. This follows the distinction in [GOV.UK text input guidance](https://design-system.service.gov.uk/components/text-input/) and [GOV.UK Details guidance](https://design-system.service.gov.uk/components/details/). If a short hover preview is retained, it must be dismissible, hoverable and persistent as described in [WCAG 1.4.13](https://www.w3.org/WAI/WCAG22/Understanding/content-on-hover-or-focus.html).

### Content contract and maintenance

Create a reviewed help catalog with stable IDs such as:

```text
pipeline.packaging
project.wheel.artifact_volume
action.load.cloudfiles.source.schema
action.load.jdbc.source.password
template.parameter.default
```

Each entry contains `summary`, optional explanatory paragraphs, choice explanations, labelled YAML examples, omission behavior, relevant constraints, related-help IDs, and source documentation paths/anchors. Keep applicability/bindings separate: schema kind, field path, action subtype and any conditional mode. Array indices use wildcard bindings; arbitrary map keys use a placeholder.

Proposed source location: `src/lhp/schemas/help/`, split by configuration family and action subtype. Package these resources with LHP and expose them through a small cached local help endpoint such as `GET /api/help/{kind}`. The frontend loads only the needed category. Use the same catalog for form help and contextual Monaco hover; do not bundle the full documentation into the entry chunk. The existing schema description remains a fallback during migration.

Author concise explanations from local `docs/reference/**`, relevant guides and `src/lhp/resources/skills/lhp/references/**`. When prose conflicts with a generator, parser or resolver, fix the documentation/schema and cite the verified behavior. Do not automatically scrape paragraphs or fetch documentation during normal editing. Exact source anchors and release-aware external links are established during content integration; local sources remain the verification basis.

Generate a coverage manifest that checks unique IDs, valid bindings, source files/anchors, related links and contextual variants. Documentation changes should flag affected entries for review. Human review assesses usefulness and correctness; string length and nonempty-description tests are insufficient.

### Example copy for review

| Field | Visible hint | Expanded guidance |
| --- | --- | --- |
| Auto Loader · Read mode | Auto Loader reads incrementally using streaming mode. | LHP supports `stream` for this reader and uses it when omitted. `batch` is rejected. YAML example: `source: { readMode: stream }`. Explain separately from pipeline continuous mode. |
| Auto Loader · Schema file | Choose a schema file to define the input columns. | Example: `schemas/orders.yaml`. Leave explicit schema sources unset to use inference. Explain the documented conflicts with legacy `schema_file` and `cloudFiles.schemaHints`; inline DDL hints belong in `cloudFiles.schemaHints`. Verify the path base before publishing. |
| Pipeline · Packaging | Choose how the generated pipeline code is shipped. | `source` ships source files; `wheel` builds a Python wheel and requires `wheel.artifact_volume` in `lhp.yaml`. Unset inherits project defaults; only when neither level specifies packaging does LHP use `source`. |
| Template parameter · Required | Require every flowgroup to supply this parameter explicitly. | LHP checks required parameters before applying defaults. A required parameter still needs an explicit entry even when a default is declared. Show an invocation example with that key present. |

Sources: `src/lhp/resources/skills/lhp/references/actions-load-cloudfiles.md`, `src/lhp/generators/load/cloudfiles.py`, `docs/reference/config/bundle.rst`, `docs/reference/config/templates.rst`, and `src/lhp/core/processing/template_engine.py`.

### Coverage

Cover project, pipeline, job and monitoring configuration; all 24 registered action subtype specifications and their nested modes; flowgroup creation/metadata; template definitions and invocation values. Review existing preset, blueprint and instance controls without creating unrelated forms. Fix schema examples and YAML hover alongside their corresponding form content. Every existing help icon receives reviewed content; every nontrivial field missing help gets an explicit coverage decision.

## 4. Template authoring experience

Use a normal workspace tab with **Builder | Code | Preview**. Builder contains independently accessible **Basics, Parameters, Actions** sections. Existing templates open directly into editing; new templates get a small completion checklist. The checklist should guide first use without forcing repeated wizard navigation.

```text
Templates / ingestion/orders          Unsaved    Save    Use template
Builder | Code | Preview

Basics      Template name, description, version, presets
Parameters  Input declarations, descriptions, defaults, uses
Actions     Action list / graph, add action, edit, insert parameter
```

At narrow widths the section index becomes a selector and controls wrap. Preview inputs and output stack vertically. Reuse the existing app shell, typography, save controls, document status, keyboard navigation and panel behavior.

### Find and create

- Resources → Templates shows search, description, path, parameter/action counts, and **New template**. Templates with invalid YAML stay visible with **Open code** and a diagnostic.
- New template supports **Blank** and **Duplicate existing**. Choose a reference/path, use `.yaml`, and create an unsaved buffer. Validate the relative path immediately; the existing atomic create guard handles collisions on Save.
- Show declared name, physical source path and invocation reference separately. Nested `templates/ingestion/orders.yaml` is referenced as `ingestion/orders`. Duplicate display names are disambiguated by path.
- A `.yml` template remains editable and is labelled as unavailable to `use_template` in the current runtime. New files use `.yaml`; extension compatibility changes are outside this implementation.
- Resources, Files, quick open, source links and template links must converge on one tab and one buffer. Upgrading an existing raw-file tab must preserve its dirty content, cursor and identity.

### Basics and parameters

- Basics edits the template's actual `name`, `version`, `description` and `presets`. The reference identifier is derived from its file path.
- Parameters show name, description, required state, optional default, and uses within this template. Provide explicit **No default / Set default** controls so `null`, `false`, `0`, `""`, `[]` and `{}` stay distinct.
- Offer useful string/number/boolean/list/object value editors with a YAML-value fallback. Label existing declared `type` as advisory metadata. The UI must not imply runtime type enforcement or promise that rendering preserves every numeric-looking string.
- Report duplicate/empty names and malformed value input beside the relevant field. Rename/delete shows the affected references and unresolved expressions before an explicit change. Preserve existing consumers; do not silently rewrite flowgroups or arbitrary Jinja with regular expressions.
- Provide **Show uses** for references in this draft. Automatic reference rewriting and cross-project rename are later enhancements; the first version can guide a deliberate edit and then report remaining old references. Static analysis must label unresolved/dynamic references honestly.

### Actions and parameter binding

- Reuse the action palette, forms and graph. An action list provides a keyboard-friendly alternative to the graph.
- In compatible fields, **Insert parameter** offers declared inputs with their descriptions and inserts the appropriate `{{ parameter }}` expression. Expose an expression mode for numeric, boolean, list/object and enum-valued fields rather than forcing expressions through literal-only controls.
- Completion distinguishes template parameters, environment substitutions and secret references. Explain the syntax at the point of insertion.
- Applying an action edit updates the shared draft. The current action dialog saves the whole file; introduce an explicit host option for **Apply action changes** in the template builder. Global Save remains the persistence action.
- Preserve unknown fields, multiline SQL/Jinja and supported YAML structures. Keep Code available. Pause only the unsafe structured edits when a source shape cannot be represented safely, with a specific explanation. Never rebuild the whole template from a reduced form object.

### Preview and use

Preview offers sample input controls, an explicit refresh action, and separate **Expanded actions** and **Resolved flowgroup** results. Expanded preview needs parameter values; resolved preview also needs an explicit sample flowgroup, pipeline and environment. Preview environment is local to this editor and must not unexpectedly change global run controls.

Use the current unsaved template snapshot. Sample values live in transient editor state scoped to project and source path; they are not saved as parameter defaults. Show omission versus explicit overrides. Mark results stale whenever source, sample values, context or saved dependencies change. Discard late responses for earlier revisions. Loading, missing-input, invalid-source and request-failure states have specific recovery actions.

The output shows rendered YAML/actions and, where valid, a graph. Diagnostics link to a parameter/action/source location when the backend can identify it. An error summary should link to the corresponding controls and use the same wording as inline errors. [GOV.UK error summary guidance](https://design-system.service.gov.uk/components/error-summary/).

State exactly what was checked: expansion or local resolution, with saved presets/configuration/substitutions. It is not a Databricks execution test. Generated Python preview can follow later.

**Use template** opens the existing Create flowgroup workflow with this template selected and shows the emitted `use_template` and `template_parameters`. A dirty/new template offers **Save and use**; failed saves block use and retain the draft. Gate creation on successful metadata loading and current required-key checks. After creation, open the new flowgroup with an **Edit template** route back. Saving an incomplete source draft remains possible independently of preview validity.

## 5. Contracts and implementation constraints

### Template catalog

Add `GET /api/templates/catalog` and path-based detail lookup without relying on the declared-name route. Register fixed routes before the existing name catch-all. Return:

```text
source_path, reference, declared_name, version, description
state: ready | invalid | unsupported_extension
parameters: name, required, has_default, default, declared_type, description
presets, action_count, diagnostics
```

`source_path` is the identity; `reference` is nullable for non-invocable files. Discovery is recursive and retains invalid files. `has_default` preserves absent versus explicit null. Keep legacy unique-name reads compatible and report ambiguity rather than selecting an arbitrary duplicate.

### Unsaved preview

Add `POST /api/templates/preview`, exposed through `lhp.api` so web routes retain the public-API boundary.

```text
Request:
  source_path, source_yaml, request_revision
  stage: inspect | expanded | resolved
  sample_parameters: map of JSON-compatible values
  context?: pipeline, flowgroup, environment, presets, variables

Response:
  request_revision, source_hash, stage
  status: ready | needs_parameters | needs_context | invalid | stale
  diagnostics, missing_parameters, effective_parameters?
  expanded_actions?, resolved_flowgroup?
  saved_dependencies: paths and revisions/fingerprint
```

Inspect checks template structure and declarations and compiles engine-eligible Jinja scalar expressions without sample values. Malformed expressions are reported at this stage; block-only Jinja receives a compatibility warning because the engine leaves it literal. Expanded rendering waits for required keys. Resolved preview uses the real variable/template/preset/substitution/validation order. Diagnostics include severity, code, stage, message, suggested correction, source path, and a field/line location only when known. Warnings about advisory metadata or undeclared variables must not be presented as runtime errors.

Collect saved dependencies as they are actually read, including transitive preset inheritance and substitutions. Use a request-local snapshot or verify the consumed revisions before returning a ready result. A dependency change during resolution returns `stale` with a refresh action. Fingerprinting only after rendering must not associate old output with newer source files.

Extract shared parse-from-text and render-from-model entry points while keeping existing file-based behavior unchanged. For resolved preview, use a fresh request-local template provider and `FlowgroupResolutionService`, whose constructor already accepts the engine and other services. Do not swap source files, patch the application's cached engine, or resolve a fabricated saved flowgroup. Bound request size/work duration and retain existing project-path and request authentication guards. Preview must not write generated files or contact Databricks.

Preserve the current runtime's semantics and record them in parity tests: required checks precede defaults; `type` is advisory; rendering is per string value containing `{{ ... }}`; mapping keys and block-only Jinja are not rendered; result coercion is heuristic. Do not introduce strict undefined handling, native Jinja conversion or whole-document control-flow rendering in this UI project. Correct docs that imply otherwise. Raw-template validation must allow templated action fields and test actions; rendered models provide the authoritative action checks.

### Workspace and source ownership

Keep `workspaceStore.buffers` as the source of text, dirty state, ETags and saving; reuse `documentStore` and the YAML CST mutation helpers. Extend template-specific view types rather than allowing Builder/Preview on ordinary flowgroups. Map existing template Graph links/state to Builder → Actions, preserving Code links. Root owns shared store/navigation and generated API changes to avoid concurrent agent edits.

Preserve comments and unknown values. Scalar edits should retain the current surgical behavior; structural edits may normalize local formatting where the existing YAML library does so. Test anchors/aliases, multiline scalars, CRLF and unsupported structures; never claim byte identity for an operation that rewrites a sequence.

## 6. Subagent execution plan

Implementation should use a new branch `feat/frontend-guidance-templates-092` and worktree `/private/tmp/lhp-frontend-guidance-templates-092`, based on the reviewed UX commit `8517a62d...`. This retains the release 0.9.2 lineage and the fixes already being tested. Keep the original checkout untouched; use the established writable Git administration copy if its `.git` remains restricted.

Root first freezes the help bindings, catalog/preview DTOs, template view model and file ownership. Agents may work with contract fixtures while dependencies are in progress. Root plus at most three active agents respects the available concurrency limit.

| Packet | Deliverable and ownership | Dependency |
| --- | --- | --- |
| **H1 — Documentation and help catalog** | Inventory all surfaced controls; write contextual help entries and examples; correct non-template schema descriptions; add source mappings and coverage validation. Own `src/lhp/schemas/help/**`, help build/coverage tooling and assigned docs. | Root's help contract. |
| **T1 — Template API and runtime parity** | Recursive path catalog, default presence, invalid-file visibility, inspect/expanded/resolved preview, focused shared-engine refactors and backend tests. Own template API/core/parser modules, template schema and engine-semantics docs. | Root's catalog/preview contract. |
| **T2 — Template document operations** | Safe metadata/parameter/default mutations, declaration diagnostics, reference inspection and source-preservation fixtures. Own template model helpers and CST mutation code/tests. Supply a stable adapter to the builder. | Root's draft model and runtime rules. |
| **H2 — Shared help UI and bindings** | Accessible rich help, visible hints, contextual resolution, missing config/action bindings, Monaco integration, offline/category caching. Own common help components, field chrome, help hook/resolver, config consumers and action help context. | H1 catalog contract/content; root mounts local help route. |
| **T3 — Template builder** | New/Duplicate dialogs, Basics/Parameters/Actions UI, value/expression modes, parameter insertion, host-controlled action Apply. Own new `components/template/**` except preview/use files, declaration UI and designer editing changes. | T2 mutations, T1 catalog and H1 help IDs. |
| **T4 — Preview and use workflow** | Preview components/client/hook, revision handling, sample-value editors, required/default fix, loading/error gates, preselected Create flowgroup and Save/use coordination. Own preview files and existing `editor/ParamsForm`, `CreateFlowgroupDialog`, `createFlowgroupSupport` and their tests. | T1 endpoint, T2 value contract and T3 mounting interface. |
| **Q1 — Independent review** | Review semantics and source preservation; exercise acceptance journeys, keyboard/zoom and regressions. Report issues separately; root assigns fixes to file owners. | Integrated implementation. |

**Wave 1:** H1 + T1 + T2. Root handles contract review, package-data wiring and schema/API integration.

**Wave 2:** H2 + T3 + T4. Root integrates Structure/Files/quick-open navigation, shared store/view types, source links and save coordination. Within shared action-renderer files, H2 owns help bindings and T3 owns value/expression behavior: root serializes those patches rather than allowing concurrent edits.

**Wave 3:** Q1 reviews independently while root runs integration checks and resolves findings. Regenerate API types once the backend contract stabilizes. Commit coherent work packets on the isolated branch; merge or publication remains a separate action.

## 7. Acceptance and verification

The primary acceptance journey is: create a template → declare an object/list parameter → bind it to an action → preview with sample values → save → create a valid invoking flowgroup, without manually writing YAML.

Required behavioral checks:

- Auto Loader and Delta receive different schema help; subtype changes refresh open help. An inherited wheel setting never receives a false effective-default label.
- Essential hints and errors are both associated with the input. Keyboard/touch opening, copying, documentation links, Escape and focus return work without dirtying the file.
- Copyable catalog examples parse as valid YAML; representative action/configuration/template examples validate or render against real fixtures. A help-category loading failure keeps fields usable with available inline/schema fallback guidance and a retry path.
- Every template entry point reaches one draft. Switching views, applying actions, opening help and closing tabs preserves the earlier dirty-file protections.
- No default, explicit null, false, zero, empty string/list/object, required-with-default and missing metadata all behave according to the real engine.
- Nested templates, duplicate declared names, mismatched file/name, both extensions and malformed files remain discoverable and correctly selectable/editable.
- Preview parity covers template/flowgroup presets, substitutions, native collections, coercion quirks, missing values and eligible Jinja syntax. Concurrent requests cannot leak drafts or replace newer results.
- Preview uses unsaved source and saved dependency revisions accurately, leaves project/generated files unchanged, and survives failure/retry.
- Source preservation covers comments, unknown keys, multiline SQL/Jinja, CRLF, aliases/anchors and safe fallbacks. Creating over an existing file and external ETag conflicts preserve the original content.
- Viewer mode, keyboard-only navigation, screen-reader labels, narrow windows and 200% zoom work in the running frontend.

Use `docs/_fixtures/guide_reuse_templates` for real preview-versus-engine comparisons; use `tests/e2e/fixtures/testing_project`, including its nested ingestion template, for the primary manual walkthrough. Use `Example_Projects/performance_testing` for large-project navigation and help-loading checks.

Run relevant frontend component/document tests and backend template/parser/resolver/API tests during each packet. At integration run the frontend suite, TypeScript/Vite build, ESLint, generated-API drift check, relevant backend suites, mypy, Ruff and repository boundary/constitution checks. Retain the current **150 KB entry / 1.95 MB total JavaScript** size budgets: lazily load the builder/preview and help categories. Do not solve a size regression by silently raising limits.

Browser testing is a release gate and must be reported separately from jsdom/ASGI tests. The prior session could not bind a local server port; arrange the existing launcher in an environment that permits it, and report browser checks as outstanding until actually performed.

## 8. Follow-on enhancements

After the complete create/edit/preview/use journey is stable: extract a template from an existing flowgroup; make an existing literal into a parameter atomically; safe automated Jinja/reference renames; saved reusable preview scenarios; generated Python preview. These are useful extensions with additional source/transformation semantics, so they have separate acceptance criteria.
