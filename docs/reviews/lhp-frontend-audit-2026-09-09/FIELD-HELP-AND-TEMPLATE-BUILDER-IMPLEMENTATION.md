# Field guidance and template authoring delivery

Implemented on `feat/frontend-guidance-templates-092` in `/private/tmp/lhp-frontend-guidance-templates-092`, preserving the earlier UX changes at `8517a62d3533134be78ffc507042cba4a992e7c8`. That parent descends from `origin/release/V0.9.2`. The original checkout and its existing project edits remain separate. Nothing has been pushed or merged.

The approved [plan](FIELD-HELP-AND-TEMPLATE-BUILDER-PLAN.md) was implemented with three subagents covering guidance, template authoring, and the engine/API, with root integration and independent cross-review.

## What changed

- **Field guidance:** 363 curated entries across project, pipeline, job, action and template settings. Essential hints stay visible; information buttons open persistent help with examples, choices, omission/inheritance behavior and documentation links. The same category cache supplies contextual YAML hovers. All 24 registered action subtypes have field coverage, including nested branches. Catalogs ship locally; external documentation is supplementary. CI flags source-document changes for review.
- **Template workspace:** Resources and Files open the same path-owned Builder / Code / Preview tab. Builder offers Basics, Parameters and Actions, with list/graph presentation, new drafts, duplication, source references and unsaved-draft visibility. Existing buffers, comments and unknown YAML fields are retained.
- **Typed parameter editing:** explicit omitted/default/null/false/zero/empty values, nested list/object forms, YAML fallback, declared-type guidance and required/default diagnostics. Action fields support literal/expression modes and parameter insertion. Apply updates the template draft; saving remains explicit.
- **Preview:** check structure, expand actions, or resolve a sample flowgroup from the current unsaved draft through the actual template engine/resolver. Resolved preview accepts pipeline, flowgroup, environment, extra presets and string runtime variables. It shows rendered YAML, an optional graph, diagnostics and saved dependencies. Changed drafts, samples or saved dependencies make prior results stale. Requests can be cancelled; obsolete results are discarded.
- **Use template:** saved metadata is checked before opening a preselected Create flowgroup dialog. Dirty templates use Save and use. Nested references come from the source path, and required parameters need explicit keys even when they declare defaults. Metadata loading/errors, viewer mode, write conflicts and edits arriving during asynchronous work block inappropriate writes or creation.

## Validation

| Check | Result |
| --- | --- |
| Full frontend suite | **1,746 tests passed in 190 files** |
| Backend API/webapp/help suite | **1,955 tests passed** |
| Final template authoring and help recheck | **315 tests passed**; overlaps the broad suite |
| Frontend build and ESLint | Passed |
| JavaScript size budgets | **148.65 kB / 150 kB entry**, **1.89 MB / 1.95 MB total**, Brotli |
| Generated API types | Regeneration produced no drift |
| Python typing | Strict public API and webapp mypy passed |
| Python/repository quality | Ruff, formatting, all four constitution checks and all seven import contracts passed |
| Help content | **363 entries / five catalogs**, source/anchor/binding drift check passed; 24 action subtype coverage tests and real-validator examples passed |
| Wheel packaging | Built offline with `--no-isolation`; all four packaging assertions passed; extracted wheel serves all 363 entries and production assets without repository resources |
| Testing-project smoke check | Production index, five help categories, four-template catalog, nested path and unsaved expanded preview passed through ASGI; project source files unchanged |
| Live browser review | **Outstanding** because this environment cannot bind a local listening socket |

Logs accompany this document in `guidance-template-logs/`. The broader backend run initially failed because the temporary environment resolved a broken Ruff shim and lacked cached SDK dependencies; correcting that environment yielded the passing rerun above. Frontend checks used the existing dependency installation (Node 25.9.0, installed Vite 7.3.1 and Vitest 4.1.9); a clean `npm ci` was not run and dependency manifests were not changed. The complete Python repository suite was not run.

## Try it locally

Run the prepared launcher from your terminal:

```bash
bash /Users/mehdi.modarressi/Coding/Lakehouse_Plumber/docs/reviews/lhp-frontend-audit-2026-09-09/run-frontend-preview.sh
```

It uses port 8137 and a separate copy of `tests/e2e/fixtures/testing_project` at `/private/tmp/lhp-guidance-testing-project-092`. Optional arguments select a project directory and port. Existing server processes must be stopped or a different port supplied. The launcher uses the new branch's Python source and staged production frontend; it does not install packages or change the original testing fixture.

This environment cannot bind a listening socket, so the server was **not** started here and live browser acceptance remains outstanding. The built frontend and launcher are prepared. Temporary worktree/runtime/sample paths are local conveniences and may be removed by operating-system cleanup.

## Browser acceptance checklist

- Open project, pipeline and job settings. Read an essential hint; open help using mouse, Enter and Space. Copy an example, close with Escape, and verify focus returns to its trigger. Repeat in Viewer mode; editing must remain unavailable.
- Compare Auto Loader and Delta schema help; verify the meanings differ. Read matching YAML hover guidance in Code.
- In Resources → Templates, create a nested `.yaml` draft. Set its name/description, declare list/object inputs, add values with fields, add an action and insert a parameter expression. Apply, switch to Code and back, and verify the draft survives without saving.
- Preview with explicit false, zero, null and empty values. Check a missing required parameter's jump link. Try expanded and resolved output. Change an input or saved preset and confirm the old result is marked stale; refresh and cancel requests.
- Save and use the template. Verify its nested path reference and required/default handling in the invocation YAML, then create the flowgroup.
- Duplicate a commented template; inspect its source. Open invalid YAML and a `.yml` source and verify recovery guidance. Leave a malformed typed value and verify navigation/save do not silently discard it.
- Check narrow layouts, 200% zoom, keyboard navigation, visible focus, contrast and the large performance project.

## Deliberate boundaries

Structured editing falls back to Code for YAML anchors/aliases and multiple documents. `.yml` sources remain editable but are not invocable by the current runtime; duplication can create a `.yaml` copy. Parameter types remain advisory metadata. Complex Jinja references are reported for review; renaming a parameter does not rewrite consumers.

Preview uses a restricted Jinja environment to prevent preview expressions from reaching Python internals. Restricted constructs produce a specific diagnostic; ordinary generation retains its existing behavior. Preview has a 512 KiB source limit, 10-second deadline, two-worker concurrency limit and 2 MiB result cap. It validates/resolves data without executing a pipeline. Saved dependencies are fingerprinted; unsaved edits to other files are not included.

Browser validation is still required before release. Template extraction from flowgroups, automated reference renames, saved scenarios and generated-Python preview remain the explicitly deferred follow-ons from the approved plan.
