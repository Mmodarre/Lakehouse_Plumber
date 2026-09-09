# Frontend UX implementation review

This delivery implements the reviewed frontend changes on `feat/frontend-workspace-ux-092`, based on the locally available `origin/release/V0.9.2` at `2dad5b4948e20a309fa7672db878bc853dd1f066`.

Worktree: `/private/tmp/lhp-frontend-workspace-ux-092`.
Git administration: `/private/tmp/lhp-frontend-ux-092.git`.

The original repository's `.git` is read-only in this session, so this is an independent local Git copy with a real linked worktree and branch. The user's original working files remain unchanged. Nothing is merged or pushed. A portable Git bundle is also saved in the original checkout’s audit directory so the branch can be imported if the temporary worktree is removed.

The [implementation plan](IMPLEMENTATION-PLAN.md) assigns three subagents to workspace, configuration, and run/data changes, with root integration for the shell, graph, diagnostics and supporting UX. The [original audit](AUDIT.md) is retained as historical evidence; source line references there describe the pre-change checkout.

## Bugs addressed

| Audit IDs | Result |
|---|---|
| B01 | New file and Duplicate use atomic create-only writes, retaining the existing file on collision. |
| B02 | Save acknowledgements advance the saved baseline without overwriting edits made during the request. |
| B03 | Viewer controls and central save/document mutation guards prevent edits through raw, graph and config entry points. |
| B04, B10 | A shared file opener resolves document-owning tabs, upgrades configuration tabs, preserves buffers and reveals source locations. |
| B05 | A stable document toolbar and keyboard save work across Graph, Form and Code. |
| B06 | Multi-flowgroup files explicitly use Code; unsafe structured edits cannot silently change another flowgroup. |
| B07 | History hydration works through StrictMode remounts and cannot replace live-session results. |
| B08, B09 | One persistent run coordinator owns streams, queues save validation, guards stale callbacks, and distinguishes stopped/incomplete/failed/successful outcomes. |
| B11 | External dependency links open the actual pipeline workspace tab. |
| B12, B13 | Visible pipeline scope and project-specific environment selection reconcile against available choices. |
| B14 | File and run pushes refresh affected resources, configuration preview and generated-artifact queries. |
| B15, B17 | Source/explorer failures show explicit error and recovery states. |
| B16 | Direct initialization success provides Open workspace. |
| B18 | Lineage arrows correspond to recorded edges; siblings and consumers are not rendered as an invented chain. |
| B19 | Unbound, bound-here and bound-elsewhere configuration captions describe the actual run selection. |

## UX delivered and scope

| Audit ID | Implementation |
|---|---|
| U01 | Save/Save all, file status, explicit save-before-run, visible execution inputs, and immutable run inputs during asynchronous saving. Failed or incomplete saves block execution. |
| U02 | Visible panel toggles, focus mode, viewport-aware panel drawers and bounded dimensions. Browser layout/zoom review is still required. |
| U03 | Workspace Back/Forward, Project map access, pipeline breadcrumbs, quick open/commands, and shareable active-view links. History is within the workspace; native browser Back/Forward is not a replacement for these controls. |
| U04 | File/path filtering, active-file reveal, contextual creation, and retained explorer lenses/search/expansion while navigating. |
| U05 | Roving keyboard tabs, Arrow/Home/End navigation, keyboard splitters, graph Enter-to-edit, discoverable shortcuts and non-conflicting entity/config view shortcuts. |
| U06 | Source/line navigation, severity/file filters, full explanations/suggestions, honest empty scope, and result provenance/staleness hints. Lines are revealed when the backend supplies them; no action or line is guessed. |
| U07 | Working Help guide, blueprint metadata and source navigation replace development placeholders. Catalog column discovery remains explicitly unavailable. |
| U08 | Graph Details exposes metadata, variables, presets, template values and template parameters, with source-template navigation. |
| U09 | New project/pipeline/job configuration is available from Structure using existing templates and safe writes. |
| U10 | Graph undo/redo restores original YAML and comments; history refuses to overwrite newer Code edits and is cleared across projects/closed documents. Action dialogs confirm discarding drafts. Field inputs guard reload and commit before Save. |
| U11 | Conversation drafts and reading positions survive dock/tab changes, session rekeying preserves them, IME Enter does not send prematurely, and assistant context is stated explicitly. These are session conveniences, not persisted chat backups. |
| U12 | Compact/comfortable density, clearer document/tab status, full-path disambiguation and larger controls in comfortable mode. Contrast and zoom measurements remain part of browser review. |
| U13 | Loaded-history filters, incremental loading through the actual 200-run backend cap, expanded detail, source links, timestamps, export and retained investigation state. |
| U14 | Local error boundaries around center/inspector/assistant and visible retry paths preserve the surrounding workspace. |
| U15 | Production bundle measurements, deferred editor/graph code and on-demand surfaces keep the existing size gates. Browser interaction profiling remains outstanding. |
| U16 | Right-click Close, Close others, Close right, Close saved and Close all, with one unsaved-file review and Save/Discard/Cancel. Failed saves stay open; bulk actions respect pins. |
| U17 | Search open tabs, reveal the active tab, pin/unpin, reorder and reopen closed tabs. Explicitly discarded content is not resurrected. |
| U18 | Permanent Project configuration access, clear source paths and run bindings, and shared file ownership. |
| U19 | Setting/key search, section index, configured-only filtering, collapse/expand all and responsive document selection. |
| U20 | Section collapse is presentation-only; adding/removing configuration is explicit, and removal is confirmed. |
| U21 | Saved effective settings use production LHP resolution. Preview explains merge layers and backend limitations; override/reset wording distinguishes absent versus explicit values. A read-only saved/working diff reviews unsaved changes. Per-field provenance across all resolver transformations is not claimed. |
| U22 | Contextual New file here, Duplicate, Copy relative path, Open code/configuration and Reveal. Reference-safe rename/move requires backend support and remains a follow-up. |
| U23 | Editor cursor/scroll, Code artifacts, graph viewport, configuration selection/scroll, explorer lenses and history selection are retained for returning views. Session state is not promised as durable across browser restarts. |

## Review checklist

- [ ] Right-click a dirty tab; try Close others and Close all. Cancel retains all files. Failed Save retains affected tabs. Pins remain unless explicitly closed.
- [ ] Type in a configuration field and press Save without clicking elsewhere. Switch Form/Code and confirm the same buffer, comments and save state.
- [ ] Search configuration keys, collapse sections, and inspect an effective saved preview for a pipeline and a job. Save to include edits in the preview.
- [ ] Create a file at an existing path and verify the original remains unchanged.
- [ ] Start a run, switch views and close its launching tab; the run remains visible. Stop and disconnect do not report success.
- [ ] Open a diagnostic while the same file is already a structured tab. Confirm preserved edits, YAML selection and supplied line.
- [ ] Delete a graph action, undo, and compare YAML. Edit an action and dismiss the dialog; Keep editing retains the draft.
- [ ] Review light/dark at 1280×800 and 1440×900, narrow windows and 125–200% zoom. Check drawers, focus return and comfortable density.
- [ ] Reopen assistant conversations while composing or reading old messages. Validate IME composition and session transitions.

## Explicit follow-ups

- Live browser visual, keyboard-focus, contrast and performance profiling could not be completed in this sandbox. Component tests do not substitute for this review.
- Multi-flowgroup structured editing uses a safe Code fallback; it needs stable selected-document identity before expansion.
- History is limited to 200 loaded runs by the existing API. Durable stopped/disconnected reasons, full pagination and comparisons need backend changes.
- Effective preview describes saved configuration and resolver layers. It omits unsaved drafts, generated tasks/paths, template-only defaults and generation-added wheel dependencies, and preserves standard-job tokens exactly as the job generator does.
- Reference-aware file rename/move and catalog-backed table columns remain separate backend capabilities.
