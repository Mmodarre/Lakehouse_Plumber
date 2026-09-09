# LHP frontend audit — review backlog

Reviewed 9 September 2026. **19 bugs / functional defects and 23 UX recommendations.** This is a review backlog for selection; no application fixes or redesign have been implemented.

The most valuable next step is to make editing, saving, navigation, and execution behave consistently across the workspace. The existing semantic theme, graph views, structured configuration forms, and conflict editor provide a useful foundation. Reliability and clear state should lead the work; visual refinement should follow.

Follow-up emphasis: everyday workspace ergonomics, especially project configuration and tab management. Original findings remain; B19 and U16–U23 extend the review following user feedback.

## Evidence and limits

- Inspected the active router and workspace shell; Files/Structure/Tables navigation; tab and document stores; graph, code, config and table views; action editing and shared fields; file creation, saves and conflicts; validation, history and streaming; assistant composition; onboarding; styling and loading/error paths. Reviewed backend file-write behavior where needed to establish frontend impact.
- **Build and TypeScript check: passed. ESLint: passed. Bundle-size gates: passed.** App entry is 148.45 kB Brotli against a 150 kB budget; total JavaScript including chunks/workers is 1.85 MB against 1.95 MB. These are build measurements, not browser timing measurements.
- **Existing suite: 164 test files / 1,561 tests passed** with `NODE_OPTIONS=--no-experimental-webstorage`. The default Node v24.14.1 invocation first produced 356 failures, largely around unusable `localStorage`; the compatibility invocation resolved all of them. Do not treat that initial failure count as application defects. Installed Vite reported 7.3.1 although the manifest requests ^8.1.5, so these checks describe the existing dependency installation, not a clean lockfile install.
- **Nine temporary, targeted audit probes passed**, reproducing existing failures in file creation, delayed save acknowledgement, problem navigation, StrictMode hydration, run cancellation/concurrency/lifetime, multi-flowgroup selection, and duplicate config tabs. Their assertions describe current faulty behavior; they are not acceptance tests for fixes. See [probe source](audit-probes.tsx.txt) and [results](audit-probes-results.txt).
- A live browser review was unavailable: localhost binding was denied (`EPERM`), the Python environment lacks `uvicorn`, and the installed Chromium could not remain running in this session. Layout findings below come from current CSS/component structure; no screenshot-based contrast, responsive rendering, focus-order, or live Databricks/assistant behavior is claimed. Those checks remain necessary before implementation is signed off.
- Shared field behavior was reviewed; this is not an exhaustive parity check of every action specification against every backend schema.

**Priority:** P1 = risk to work, correctness, or a core workflow; P2 = significant usability/recovery problem; P3 = refinement. **Evidence:** Probe = reproduced in an isolated test against current code; Source = directly traced implementation; Design = recommendation requiring usability/visual validation. Effort S/M/L is relative and provisional.

## Bug checklist

| Select | ID | Priority | Finding | Evidence |
|---|---|---|---|---|
| [ ] | B01 | P1 | New file can blank an existing file | Probe + backend trace |
| [ ] | B02 | P1 | Save acknowledgement can overwrite newer edits | Probe |
| [ ] | B03 | P1 | Viewer mode allows raw editing and file mutations | Source |
| [ ] | B04 | P1 | File/problem links fail for already-open structured tabs | Probe + source |
| [ ] | B05 | P1 | Graph structure and config forms have no save action in their own view | Source |
| [ ] | B06 | P1 | Multi-flowgroup files display/edit the wrong flowgroup | Probe |
| [ ] | B07 | P2 | Previous validation does not hydrate under development StrictMode | Probe |
| [ ] | B08 | P1 | Stopped or incomplete runs can be reported as successful | Probe |
| [ ] | B09 | P1 | Run ownership is tied to individual views and allows competing streams | Probes + source |
| [ ] | B10 | P2 | Config can open twice; closing one tab drops their shared buffer | Probe |
| [ ] | B11 | P2 | External-connection navigation still targets a retired modal | Source |
| [ ] | B12 | P1 | Pipeline picker can say “All pipelines” while a hidden filter remains active | Source |
| [ ] | B13 | P2 | Environment defaults to `dev` even when it is unavailable | Source |
| [ ] | B14 | P2 | File-change notifications miss resource and artifact query keys | Source |
| [ ] | B15 | P2 | Failed graph source loading is shown as an indefinite spinner | Source |
| [ ] | B16 | P2 | Direct `/init` success has no path into the workspace | Source |
| [ ] | B17 | P2 | Explorer request errors appear as empty data | Source |
| [ ] | B18 | P2 | Table lineage draws relationships that may not exist | Source |
| [ ] | B19 | P2 | Unbound pipeline config claims Validate/Generate use this file | Source |

### B01 — “New file” can destroy an existing file

**Trigger:** Files → New file → enter a path that already exists → Enter. `submitCreate` sends `writeFile(path, '')` without a conditional header. The backend explicitly permits an unconditional overwrite when the header is absent. There is already an `IF_MATCH_CREATE_ONLY` helper used for safer creation elsewhere.

**Change:** use an atomic create-only request and show “File already exists” with an Open action. Do not rely solely on the currently cached file tree.

**Acceptance:** an existing file retains its bytes, whether it existed before the interaction or was created concurrently; a new path still creates successfully. **Effort: S.**

Evidence: [FileBrowser.tsx:124](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/sidebar/FileBrowser.tsx:124), [files.ts:29](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/api/files.ts:29), [files.py:164](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/src/lhp/webapp/routers/files.py:164). Probe B01 verifies the unsafe request; backend source establishes overwrite behavior.

### B02 — Save responses can erase edits made during the request

**Trigger:** save text A; continue editing to text B while the request is pending; receive the successful response for A. `setEtagAndBaseline` writes A back into `content` and marks the buffer clean. CodeView also reconciles clean buffer text into Monaco. In a raw-file view, the live editor and persisted buffer can instead diverge until another capture.

**Change:** update the saved baseline/ETag to A while preserving current content B; calculate dirty state from the current content. Keep document and editor state aligned without replacing newer text.

**Acceptance:** delayed and out-of-order responses never drop later edits; only the submitted snapshot becomes the disk baseline. **Effort: M.**

Evidence: [workspaceStore.ts:795](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/store/workspaceStore.ts:795), [useWorkspaceSave.ts:94](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/workspace/useWorkspaceSave.ts:94), [CodeView.tsx:274](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/CodeView.tsx:274). Probe B02.

### B03 — Viewer mode is only partially enforced

**Trigger:** enable “Viewer lens (read-only preview)”, then use raw Files, config YAML, or an entity's Code source tab. Their read-only checks use only the path. New file, delete, and new flowgroup controls also do not consult viewer mode. Structured forms do become inert, so the behavior changes between views.

**Change:** apply one editing policy to editor options, save entry points, creation and deletion. Decide explicitly whether Generate is available in viewer mode because it writes generated files. This concerns the promised UI behavior; the lens is not an access-control boundary.

**Acceptance:** switching views cannot bypass the chosen viewer policy, including shortcuts and Save All. **Effort: M.**

Evidence: [CenterArea.tsx:391](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/CenterArea.tsx:391), [CodeView.tsx:374](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/CodeView.tsx:374), [FileBrowser.tsx:119](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/sidebar/FileBrowser.tsx:119), [documentStore.ts:263](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/store/documentStore.ts:263).

### B04 — Existing structured tabs cannot reliably be opened from file links

**Trigger:** open a config from Structure, or a flowgroup graph, then switch to another tab and click its file in Files or Problems. These handlers call `setActive(rawFilePath)` when the backing buffer exists. `setActive` accepts tab IDs, while structured tab IDs are `config:…` or `entity:…`, so the action does nothing. Inspector problem links share the same pattern.

**Change:** route file locations through one path-aware opener, resolve the owning tab, switch to YAML/Code as appropriate, and reveal the requested line.

**Acceptance:** clicking a file/problem always focuses the correct existing tab without duplication or loss of edits. **Effort: S–M.**

Evidence: [FileBrowser.tsx:76](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/sidebar/FileBrowser.tsx:76), [ProblemsPanel.tsx:59](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/validation/ProblemsPanel.tsx:59), [Inspector.tsx:68](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/inspector/Inspector.tsx:68), [workspaceStore.ts:549](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/store/workspaceStore.ts:549). Probe B04.

### B05 — Saving disappears in Graph and Form

**Trigger:** edit a project/pipeline/job form, or add/duplicate/delete a graph action. Those operations dirty the shared buffer, but these views have no Save button. The save shortcut is installed by Monaco, and `saveActive` targets the mounted YAML buffer; it does not provide a workspace-wide save command while Form/Graph is active. Users have to discover switching to YAML/Code. The action modal's Save behaves differently and persists the whole file.

**Change:** add a shared document command area with Save, saved/unsaved/saving state, and an active-document shortcut independent of Monaco. Define whether modal Save means “Apply to document” or “Save file”.

**Acceptance:** every editable view can save its document directly and shows an accurate outcome; validation does not silently use an older on-disk version when users expect their edits. **Effort: M.**

Evidence: [ConfigFormView.tsx:47](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/entity/ConfigFormView.tsx:47), [GraphView.tsx:115](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/entity/GraphView.tsx:115), [useWorkspaceSave.ts:176](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/workspace/useWorkspaceSave.ts:176), [MonacoEditorWrapper.tsx:140](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/editor/MonacoEditorWrapper.tsx:140).

### B06 — Multi-flowgroup YAML selects the first entry regardless of the requested entity

**Trigger:** one YAML file contains two flowgroups; select the second from Structure. The tab records the second identity, but `useFlowgroupDoc` selects index 0, as does the staged action editor. This can present and mutate the first flowgroup under the second tab's identity. The source documents this limitation, but the UI does not make it safe or clear.

**Change:** retain a stable selected document/flowgroup identity through the graph, modal and save paths. An acceptable interim option is to detect multi-flowgroup files and offer clearly labelled YAML-only editing.

**Acceptance:** selecting the second entity can never edit the first one inadvertently. Cover renames and two tabs referencing the same source file. **Effort: M–L.**

Evidence: [useFlowgroupDoc.ts:90](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/entity/useFlowgroupDoc.ts:90), [ActionModalEditor.tsx:106](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/designer/ActionModalEditor.tsx:106). Probe B06.

### B07 — History hydration is broken in development StrictMode

**Trigger:** boot the app in development with prior validation history. The effect sets `done.current = true`, StrictMode cleanup cancels its asynchronous response, and the second setup returns immediately because `done` is already true. Problems stay empty.

**Change:** make the lifecycle restartable or use a query-backed fetch with effect-safe application to the store. Preserve the guard against overwriting a live run.

**Acceptance:** both StrictMode and production hydrate once successfully; unmounted or stale fetches cannot overwrite newer results. **Effort: S.** This specific reproduction is a development-mode bug, not a claim about production effect replay.

Evidence: [useHydrateProblems.ts:43](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/hooks/useHydrateProblems.ts:43), [main.tsx:37](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/main.tsx:37). Probe B07.

### B08 — Stop and incomplete streams can report success

**Trigger:** Stop before a terminal completion frame, or a stream closes without one. The transport reports `{ aborted }`, but the controller ignores it and invokes `finish()`, whose default terminal state is `success`. The status bar can consequently say “Generated” or “Validated”.

**Change:** carry cancellation and missing-completion states through the controller/store. Only a successful completion frame should produce success.

**Acceptance:** completed, failed, stopped, disconnected and incomplete runs remain distinguishable across status bar, Run and History. **Effort: M.**

Evidence: [runStore.ts:86](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/store/runStore.ts:86), [runStore.ts:383](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/store/runStore.ts:383), [useEventStream.ts:246](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/hooks/useEventStream.ts:246). Probe B08.

### B09 — Run lifetime and concurrency depend on the launching component

**Trigger A:** action-modal Save starts auto-validation, then `onSaved` closes the modal and unmounts its controller. Transport cleanup aborts the just-started validation. CodeView-owned auto-validation can similarly end when changing view/tab.

**Trigger B:** Generate is running from the command bar, then another controller starts validation after Save. Each controller checks its own `stream.isRunning`, even though results and Stop use shared state. A competing request or a late completion can overwrite the displayed run state. The backend lock does not resolve frontend ownership.

**Change:** one persistent run coordinator, one global concurrency policy and run-ID guards. Views request runs; closing a view does not implicitly stop them. Coalesce auto-validation behind active work.

**Acceptance:** saves and navigation cannot truncate or relabel another run; Stop targets the displayed run. **Effort: L.**

Evidence: [runStore.ts:350](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/store/runStore.ts:350), [ActionModalEditor.tsx:178](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/designer/ActionModalEditor.tsx:178), [useEventStream.ts:133](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/hooks/useEventStream.ts:133), [persistBuffer.ts:95](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/workspace/persistBuffer.ts:95). Two B09 probes reproduce competing instances and unmount cancellation; the modal sequence is source-traced.

### B10 — Opening config after raw YAML duplicates ownership

**Trigger:** open `lhp.yaml` from Files, then Project from Structure. `openConfigTab` appends a second tab rather than upgrading the file tab. Closing either removes their shared buffer, leaving the other tab without it until some later reload path repairs the state. Entity opening already has an upgrade path that config opening lacks.

**Change:** enforce one document owner per path or manage shared buffer references deliberately. Never delete a buffer while another tab needs it.

**Acceptance:** either opening order yields a coherent workspace; closing one representation cannot strand the other. **Effort: M.**

Evidence: [workspaceStore.ts:621](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/store/workspaceStore.ts:621), [workspaceStore.ts:529](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/store/workspaceStore.ts:529). Probe B10.

### B11 — External dependency links silently do nothing

**Trigger:** select a target pipeline in a graph node's external-connections menu. It calls `openPipelineModal`, but AppShell no longer mounts a consumer for the retired drill-modal state.

**Change:** route to `openPipelineDag` or the target entity tab, preserving navigation context. **Acceptance:** every selectable cross-pipeline connection opens its destination. **Effort: S.**

Evidence: [ExternalBadge.tsx:49](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/graph/badges/ExternalBadge.tsx:49), [AppShell.tsx:97](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/AppShell.tsx:97).

### B12 — Displayed pipeline scope can differ from applied scope

**Trigger:** select a pipeline, then remove it or change sandbox scope so it falls out of the picker. `shownFilter` becomes null and the picker says “All pipelines”, but `uiStore.pipelineFilter` retains the old value. Outside the sandbox membership guard, map filtering still consumes that raw value; non-sandbox runs do too. The sandbox guard can temporarily mask the old selection, which remains stored when sandbox mode is disabled again. A filter also keeps affecting runs after navigating away from the map, where its control lives.

**Change:** reconcile invalid selections, or show an explicit unavailable-filter state. Display run scope beside Validate/Generate and distinguish a map filter from execution scope if they are meant to differ.

**Acceptance:** the visible scope matches graph/run behavior after deletion, scope changes and tab changes. **Effort: M.**

Evidence: [PipelineFilter.tsx:40](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/PipelineFilter.tsx:40), [runStore.ts:400](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/store/runStore.ts:400), [CommandBar.tsx:171](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/CommandBar.tsx:171).

### B13 — A project without `dev` starts with an invalid environment

**Trigger:** load a project whose environments are, for example, `test` and `prod`. The store initializes to `dev`; the selector only lists actual environments, but nothing reconciles the selected value when that list arrives. Runs and table resolution can still request `dev`. Reload also resets an intentionally selected environment to `dev`.

**Change:** choose a valid configured default, retain the choice per project, and handle environment removal/empty lists explicitly. **Acceptance:** selection, table data and run requests always agree. **Effort: S–M.**

Evidence: [uiStore.ts:77](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/store/uiStore.ts:77), [CommandBar.tsx:209](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/CommandBar.tsx:209), [useEnvironments.ts:4](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/hooks/useEnvironments.ts:4).

### B14 — Some resource/artifact data misses live invalidation

**Trigger:** edit a preset, template, substitution or project configuration while its dependent UI remains mounted. Push invalidates a fixed key list missing `project`, `environments`, `environment-resolved`, `presets`, `preset`, `templates`, `template`, `blueprints` and operational metadata. CodeView uses `flowgroup-related`, while push invalidates `flowgroup-related-files`. Invalidating the tree does not refresh those independent cached results.

**Change:** centralize query keys and a path-to-dependent-query mapping. Cover clean open-buffer external refresh separately, preserving dirty-buffer conflicts.

**Acceptance:** affected mounted views update after relevant edits without reload; unrelated views avoid excessive refetches. **Effort: M.**

Evidence: [usePushChannel.ts:41](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/hooks/usePushChannel.ts:41), [CodeView.tsx:135](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/CodeView.tsx:135), [useEnvironments.ts:13](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/hooks/useEnvironments.ts:13).

### B15 — Failed graph source fetch becomes endless loading

**Trigger:** open a flowgroup graph whose source request fails. The buffer gets `loadFailed`, `useEntityDocument` returns read-only reason `loading`, and GraphView renders its loading spinner. The error toast is temporary; the graph has no persistent failure/retry state. Users can only discover switching to Code for recovery.

**Change:** represent pending and failed loads separately; expose Retry and Open Code in the graph's persistent error state. **Acceptance:** a failed request ends loading and offers a usable recovery action. **Effort: S.**

Evidence: [documentStore.ts:258](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/store/documentStore.ts:258), [GraphView.tsx:321](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/entity/GraphView.tsx:321), [flowgroupBuffers.ts:28](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/workspace/flowgroupBuffers.ts:28).

### B16 — Direct initialization is a dead end

**Trigger:** enter `/init` directly and successfully create a project. That route sits outside AppShell; invalidating health cannot swap it into the workspace. Its success text says to use navigation above, but that route has none.

**Change:** navigate to `/` after success or show a clear Open workspace button; redirect away from `/init` when a project already exists. **Acceptance:** embedded and direct-entry onboarding both end in the working project. **Effort: S.**

Evidence: [router.tsx:16](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/router.tsx:16), [InitProjectPage.tsx:183](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/pages/InitProjectPage.tsx:183).

### B17 — Request failures look like empty projects

**Trigger:** pipeline, table or file listing fails. Structure and Tables inspect loading/data but not the query error, and Files can render an empty tree. Messages such as “No pipelines” or “No tables resolved” can conceal a request failure.

**Change:** use separate loading, empty, filtered-empty and failed states, with local Retry and useful error context. **Acceptance:** a server error never claims the project simply has no data. **Effort: S–M.**

Evidence: [StructureLens.tsx:163](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/explorer/StructureLens.tsx:163), [TablesLens.tsx:68](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/explorer/TablesLens.tsx:68), [FileBrowser.tsx:33](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/sidebar/FileBrowser.tsx:33).

### B18 — The lineage rail implies false dependencies

**Trigger:** lineage contains parallel inputs A → C and B → C, or multiple independent consumers. Nodes are topologically sorted and rendered with an arrow between every adjacent card. This can draw A → B → C; consumers are also chained in a single row. A topological ordering is not an edge list.

**Change:** render the actual edges with branches, or use labelled groups for Inputs / Transformation / Output / Consumers without implying connections between peers.

**Acceptance:** every displayed relationship corresponds to an actual lineage edge; branching fixtures remain unambiguous. **Effort: M.**

Evidence: [TableDetailView.tsx:80](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/TableDetailView.tsx:80), [TableDetailView.tsx:406](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/TableDetailView.tsx:406).

### B19 — Run-config caption is misleading when no config is selected

**Trigger:** open a pipeline config with no file bound to runs. The “Use for runs” switch is off, but `otherName` is null and the caption says “Validate/Generate use this file.” The same caption is used when this file is selected and when no file is selected.

**Change:** distinguish “Not used for runs”, “This file is used for runs”, and “Runs use another file”. Keep the selected file visible and navigable in the command bar.

**Acceptance:** the caption and toggle accurately explain all three states. **Effort: S.** Source-confirmed; not included in the nine earlier probes.

Evidence: [UseForRunsToggle.tsx:20](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/config/pipeline/UseForRunsToggle.tsx:20), [UseForRunsToggle.tsx:48](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/config/pipeline/UseForRunsToggle.tsx:48).

## UX and design recommendations

These are proposed product choices, not additional claims of reproduced bugs. Dependencies on the fixes above are intentional.

| Select | ID | Priority | Recommendation | Effort |
|---|---|---|---|---|
| [ ] | U01 | P1 | Make save state and run input explicit | M |
| [ ] | U02 | P1 | Protect the central workspace at laptop widths and zoom | M–L |
| [ ] | U03 | P2 | Add coherent navigation history, deep links and quick open | L |
| [ ] | U04 | P2 | Improve explorer search, reveal and state retention | M |
| [ ] | U05 | P2 | Make keyboard and assistive navigation consistent | M |
| [ ] | U06 | P2 | Turn diagnostics into a complete fix-and-verify workflow | M |
| [ ] | U07 | P2 | Replace dead-end Help/resources with useful content or actions | S–M |
| [ ] | U08 | P2 | Restore structured flowgroup/template metadata editing | M–L |
| [ ] | U09 | P2 | Surface creation paths for configs and reusable resources | M |
| [ ] | U10 | P1 | Preserve drafts and add recovery for structural edits | M–L |
| [ ] | U11 | P2 | Preserve assistant composition and reading position | M |
| [ ] | U12 | P2 | Improve typography, density and visual hierarchy | M |
| [ ] | U13 | P2 | Give run history filtering and an expandable detail workspace | M |
| [ ] | U14 | P2 | Add local failure boundaries and consistent recovery | M |
| [ ] | U15 | P3 | Measure and reduce first-load/editor costs | M |
| [ ] | U16 | P2 | Tab context menu: Close all / others / right / saved | M |
| [ ] | U17 | P2 | Tab overflow picker, reorder, pin and reopen | M |
| [ ] | U18 | P2 | A consistent, readily accessible configuration entry | M |
| [ ] | U19 | P2 | Search and section navigation inside config forms | M |
| [ ] | U20 | P2 | Separate collapsing a config section from removing it | S–M |
| [ ] | U21 | P2 | Explain effective config values, inheritance and run binding | M–L |
| [ ] | U22 | P2 | Contextual file/folder actions with meaningful destinations | M–L |
| [ ] | U23 | P2 | Preserve working position when moving between views | M |


### U01 — One clear editing and execution contract

Provide a stable document toolbar: path, Unsaved/Saving/Saved state, Save, and actions. Before Validate/Generate, offer “Save changes and run” or an explicit “Run saved files” choice if buffers are dirty. Show the environment, exact pipeline/sandbox scope, and config used by the run beside the commands. Record that context with results, so changing the environment selector does not make old results appear current. Resolve B02, B05, B08, B09 and B12 first.

### U02 — Let the active work occupy the screen

The default explorer (260px), inspector (300px) and closed assistant rail (44px) consume **604px** before center content. With assistant open they consume **880px**: at 1280px width that leaves roughly **400px** for the editor; at 1024px, about **144px**, before internal padding. There is no responsive shell collapse rule. Use visible panel toggles, a focus mode, viewport-aware width limits, and automatically trade inspector space for assistant space on smaller windows. Clamp the bottom panel to leave a usable center height. Validate 1280/1440px laptops, narrow windows, and browser zoom before choosing exact breakpoints.

Evidence: [layoutStore.ts:68](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/store/layoutStore.ts:68), [AppShell.tsx:93](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/AppShell.tsx:93), [BottomPanel.tsx:24](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/bottom/BottomPanel.tsx:24).

### U03 — Make location and navigation predictable

Add clickable Project → Pipeline → Flowgroup breadcrumbs, Back/Forward within the workspace, an always-available Project map entry, and a searchable quick-open/command palette. Persist/share an entity/view URL independently of the open-tab collection; account for unavailable entities when loading an old link. Current router state is only `/init` versus `*`, while breadcrumbs are text. This is useful for repeated navigation and support, not just appearance.

Evidence: [router.tsx:21](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/router.tsx:21), [EntityHeader.tsx:73](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/EntityHeader.tsx:73).

### U04 — Make large projects easy to scan

Add filename/path search in Files, “Reveal active file”, clear-filter actions, collapse/expand controls, and session retention of each lens's search and expansion state. Keep the primary creation action visible in Structure. Files is the default lens, but its folders initially start collapsed; switching away unmounts its local expansion state. Resolve tab IDs back to file paths for active highlights. Preserve the distinct purpose of Structure, Tables and Files while making the transitions understandable.

Evidence: [FileBrowser.tsx:42](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/sidebar/FileBrowser.tsx:42), [Explorer.tsx:112](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/explorer/Explorer.tsx:112), [StructureLens.tsx:182](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/explorer/StructureLens.tsx:182).

### U05 — Complete the keyboard interaction model

Use consistent tab semantics, selected-state announcements, roving focus and arrow-key behavior for workspace/explorer/artifact/bottom tabs. Make splitters keyboard-focusable and adjustable; provide visible panel commands as well as shortcuts. Wire graph Enter to open the selected action: it currently tries to focus an inspector ref that GraphView intentionally leaves null. Review shortcuts that conflict with browser navigation. Test focus return from every modal and keyboard access to close buttons. Include a shortcut reference.

Evidence: [TabStrip.tsx:141](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/TabStrip.tsx:141), [Explorer.tsx:121](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/explorer/Explorer.tsx:121), [useDesignerCanvasWiring.ts:169](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/designer/useDesignerCanvasWiring.ts:169), [GraphView.tsx:137](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/entity/GraphView.tsx:137).

### U06 — Diagnostics should lead directly to repair

Clicking a diagnostic should reveal its exact file, line and action, show the full explanation/suggestions, and allow revalidation in the same scope. Group/filter by severity and file; clearly distinguish “Not validated”, “Validated with no issues” and “Results out of date”. Avoid silently replacing a file-specific panel with unrelated project issues when that file has none. Give long messages an expandable reading area rather than relying on tooltips within the fixed inspector width. Coordinate Inspector and Problems so they have complementary roles.

Evidence: [IssueList.tsx:73](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/validation/IssueList.tsx:73), [scopeIssues.ts:49](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/inspector/scopeIssues.ts:49), [Inspector.tsx:102](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/inspector/Inspector.tsx:102).

### U07 — Remove visible development placeholders

Help currently says “Field help arrives with the inspector routing task.” Blueprint detail says it “arrives in a later wave.” Replace these with context-sensitive schema help, usage/examples, or a working Open source action. Until those are available, hide a control that only leads to a placeholder. Table columns explicitly say they are unavailable; retain a truthful explanation, and treat catalog-backed columns as a separate backend capability rather than inventing data.

Evidence: [Inspector.tsx:148](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/inspector/Inspector.tsx:148), [ResourceStub.tsx:27](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/ResourceStub.tsx:27), [TableDetailView.tsx:465](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/TableDetailView.tsx:465).

### U08 — Give metadata and templates a structured editing home

The graph focuses on actions; the retired form leaves flowgroup metadata and template parameters largely dependent on Code. `TemplateParamsCard` exists but has no live consumer. Add a compact Details surface for names, descriptions, presets/template choice and parameters, plus an explicit Source versus Resolved/Inherited explanation. A template-based flowgroup should link directly to its template and expose instance parameters, rather than only saying to open Code. Preview inherited behavior without implying it is directly editable.

Evidence: [TemplateParamsCard.tsx:40](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/entity/TemplateParamsCard.tsx:40), [GraphView.tsx:345](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/entity/GraphView.tsx:345), [EntityHeader.tsx:28](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/EntityHeader.tsx:28).

### U09 — Make creation discoverable at the point of use

Expose New pipeline config/New job config in the relevant Structure groups, and reuse the existing template-based config dialog, which currently has no live import. Offer the corresponding creation action in empty states and keep new-flowgroup creation reachable without changing lens. Use previews and sensible paths so users do not have to create a blank YAML file manually. Do B01 and B03 first.

Evidence: [CreateFromTemplateDialog.tsx:59](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/config/CreateFromTemplateDialog.tsx:59), [StructureLens.tsx:325](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/explorer/StructureLens.tsx:325).

### U10 — Preserve intent and make structural changes recoverable

Choose a consistent staged-edit policy for action modals. Closing via Escape/backdrop/X currently discards staged action work without a dirty prompt; config text also lives in local drafts until blur/Enter, which needs protection on reload or programmatic view changes. Add document-level undo/redo for structural graph operations, or at minimum an Undo action for deletion/duplication and a visible pending-change review. Reconcile deleted on-disk files with open buffers so users understand whether a later save would recreate them. Confirm explicit discards without prompting for ordinary tab switches that retain drafts.

Evidence: [GraphView.tsx:418](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/entity/GraphView.tsx:418), [DraftInput.tsx:89](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/config/fields/DraftInput.tsx:89), [FileBrowser.tsx:138](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/sidebar/FileBrowser.tsx:138).

### U11 — Assistant should preserve both writing and reading context

Store a separate draft per conversation and restore it when the dock reopens; ChatComposer currently holds a single local string, which disappears on unmount and can carry across session switches. Avoid automatically scrolling to the bottom when users deliberately read earlier messages; show a “New messages” button instead. Account for IME composition before treating Enter as Send. Make the active project/entity context visible if it is being included, and state when it is not. Keep provider/setup details in the assistant's settings flow.

Evidence: [ChatComposer.tsx:87](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/assistant/ChatComposer.tsx:87), [ChatThread.tsx:133](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/assistant/ChatThread.tsx:133), [AssistantDock.tsx:38](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/AssistantDock.tsx:38).

### U12 — Refine density without losing the technical character

Retain Inter for UI, JetBrains Mono for code/identifiers, and semantic action-kind colors. Increase the relative prominence of the active document and primary task. Reserve very small text for secondary metadata; some counters are 9–10px despite the stated 11px floor. Offer compact/comfortable density, larger interaction areas for tiny close/delete controls, clearer active-tab treatment, and full-path disambiguation for repeated basenames. Measure contrast and clipping in light and dark modes; this review does not claim a measured contrast failure.

Evidence: [index.css:14](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/index.css:14), [TabStrip.tsx:198](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/TabStrip.tsx:198), [Inspector.tsx:172](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/inspector/Inspector.tsx:172).

### U13 — Make execution history useful for investigation

Add environment/pipeline/status filters, explicit time context, pagination or load-more beyond the current 50-run request, and a detail pane that can expand into the center workspace. Let issue rows in history open their source, export useful logs, and retain expanded-run selection when switching panels. Comparing two runs is a later option after run identity/outcome correctness is fixed. Do not imply old results describe the current unsaved document.

Evidence: [useRuns.ts:7](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/hooks/useRuns.ts:7), [RunHistoryView.tsx:99](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/bottom/RunHistoryView.tsx:99).

### U14 — Keep failures local and recovery visible

Add error boundaries around the center and assistant feature regions, with retry/reset for the failed feature. AppShell currently protects onboarding and the create dialog, but ordinary feature render failures fall through to router-level recovery. Standardize API failure cards with Retry, preserve drafts, and distinguish loading, disconnected, unsupported and unavailable states. Ensure connection health does not leave Validate/Generate looking ready when no project is initialized.

Evidence: [AppShell.tsx:96](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/AppShell.tsx:96), [AppShell.tsx:120](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/AppShell.tsx:120), [CommandBar.tsx:240](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/CommandBar.tsx:240).

### U15 — Measure performance before broad optimization

The app entry is close to its existing size budget. `main.tsx` eagerly imports Monaco setup; inspect the actual entry/chunk network graph to determine how much editor code loads before an editor is used. Profile first map opening, large trees/graphs, tab switching, and long run/chat logs; virtualize only where measurements justify it. Preserve editor view state/undo across tab switches if users rely on that workflow. Record performance baselines on representative project sizes before choosing targets.

Evidence: [main.tsx:1](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/main.tsx:1), [MonacoEditorWrapper.tsx:149](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/editor/MonacoEditorWrapper.tsx:149), [useEventStream.ts:183](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/hooks/useEventStream.ts:183).

### U16 — Right-click tab management (explicit user request)

**Current friction:** the tab strip only provides individual close and middle-click close. There is no context menu or bulk operation, so clearing a busy workspace requires closing tabs one at a time. The store's existing `closeAllBuffers` clears everything directly; it is not a safe UI bulk-close workflow for dirty documents.

**Proposed menu:** Close; Close other tabs; Close tabs to the right; Close saved tabs; Close all tabs. “Other” and “to the right” refer to the tab that was right-clicked, even when it is inactive. Expose the same commands through a keyboard-accessible menu button/command palette so they are not mouse-only.

**Acceptance:** bulk-close handles dirty documents once in a consolidated Save / Discard / Cancel review, retains any file whose save fails or conflicts, and leaves a sensible active tab. Do not discard unrelated buffers or staged work. Ordinary closure of clean tabs should be immediate. Implement this before optional pin/reorder features.

Evidence: [TabStrip.tsx:141](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/TabStrip.tsx:141), [CenterArea.tsx:361](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/CenterArea.tsx:361), [workspaceStore.ts:547](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/store/workspaceStore.ts:547).

### U17 — Keep many open tabs manageable

**Current friction:** tabs only overflow horizontally. There is no open-tabs picker, reorder/pin support or reopen-closed history; there is also no explicit active-tab reveal when selection changes elsewhere.

**Proposed behavior:** first add a searchable Open tabs dropdown, reveal the active tab automatically and Reopen closed tab. Then consider drag/keyboard reordering and pinning frequently used config/flowgroup tabs. If pins are accepted, define how bulk-close commands treat them. Preview tabs are a later option, and must become permanent when edited.

**Acceptance:** with 20+ tabs, users can find the active document, select a hidden one and restore an accidentally closed clean tab. Restoring a tab after an explicit discard must not resurrect edits the user chose to discard. Distinguish identical basenames with their parent path.

Evidence: [TabStrip.tsx:147](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/TabStrip.tsx:147), [workspaceStore.ts:342](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/store/workspaceStore.ts:342).

### U18 — Make configuration feel like one coherent area

**Current friction:** the default explorer is Files, while the structured Project/Pipeline/Job config entries live in Structure's bottom region alongside Resources. A file opened from Files may be raw YAML; selecting it through Structure opens Form/YAML. File classification also relies on names such as `pipeline_config*`, so a differently named config can land in “Other”. “Project” (`lhp.yaml`) and “Project defaults” inside pipeline/job files describe different things but are easy to conflate.

**Proposed behavior:** provide a persistent Configuration entry or clearly discoverable project menu action. Inside, distinguish Project settings (`lhp.yaml`), Pipeline configurations and Job configurations, retaining full path context. Opening a recognized config from any lens should focus the same Form/YAML document. Add Open as configuration where automatic classification is uncertain. Expose New configuration next to the corresponding group.

**Acceptance:** the same file opens consistently regardless of entry point; users can reach Project settings without knowing which explorer lens exposes it. Choose the final placement after reviewing the config navigation preference; a separate full settings page is not assumed.

Evidence: [layoutStore.ts:70](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/store/layoutStore.ts:70), [StructureLens.tsx:325](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/explorer/StructureLens.tsx:325), [explorerData.ts:75](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/explorer/explorerData.ts:75). Builds on B04/B10 and U09.

### U19 — Find settings without scrolling through the whole form

**Current friction:** Project configuration is a vertical sequence of section cards with no field search or section index. Pipeline/job editors add a document-selection rail inside the already constrained center pane; their 240px rail further squeezes fields.

**Proposed behavior:** add Search settings, a compact section index with issue counts, links from validation summaries to fields, and “Show configured only” / “Show all” modes. On narrow center panes, replace the internal document rail with a selector. Restore the selected config document and section when returning to the tab. Search should match human labels and YAML keys, and reveal/expand its target without silently adding a missing optional section.

**Acceptance:** users can find a setting by its YAML key, reach an invalid field directly, and edit pipeline/job settings without four simultaneous navigation columns. Current layout findings are source-based and need browser verification.

Evidence: [ProjectConfigForm.tsx:82](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/config/project/ProjectConfigForm.tsx:82), [PipelineDocList.tsx:46](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/config/pipeline/PipelineDocList.tsx:46), [JobDocList.tsx:44](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/config/job/JobDocList.tsx:44).

### U20 — Collapsing is different from removing configuration

**Current friction:** an optional section's switch controls the key's presence in YAML. Switching off opens a removal confirmation and deletes the whole section if confirmed. This is intentional behavior, but it supplies no separate way to collapse a completed section while retaining its values.

**Proposed behavior:** add a disclosure chevron for Expand/Collapse. Label presence actions explicitly as Add section / Remove section (or an equally clear “Include in configuration” control). Show “Configured” / “Not configured” status and a small summary when collapsed. “Collapse all” should change only presentation. Preserve the existing confirmation for removal and consider Undo.

**Acceptance:** collapsing sections never changes YAML or dirty state; actual removal is unmistakable and retains its explicit confirmation. A closed section can show its error count without forcing the entire form open.

Evidence: [SectionCard.tsx:19](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/config/SectionCard.tsx:19), [SectionCard.tsx:95](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/config/SectionCard.tsx:95).

### U21 — Explain what configuration will actually take effect

**Current friction:** pipeline/job rails explain precedence at the document level, which is useful, but users must still mentally combine built-in defaults, file-level defaults and their pipeline/job entries. Browsing a config and choosing one for runs are separate operations; the current off-state caption can even suggest the opposite (B19).

**Proposed behavior:** provide an effective-value preview for a selected target/environment, with value origin such as Built-in / File defaults / Explicit override. Clearly distinguish “Set override”, “Reset to inherited”, “Empty” and “Remove key” where semantics differ. Label run binding as a persistent action and let the command-bar chip open the bound config. Offer a compact changed-settings summary before saving.

**Acceptance:** users can explain why a value will apply and identify which file Validate/Generate will use. Effective values must come from the actual resolution rules; do not implement a separate guessed frontend precedence model. This preview may require backend support and is larger than fixing the caption.

Evidence: [PipelineDocList.tsx:48](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/config/pipeline/PipelineDocList.tsx:48), [UseForRunsToggle.tsx:17](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/config/pipeline/UseForRunsToggle.tsx:17), [CommandBar.tsx:112](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/CommandBar.tsx:112).

### U22 — Put file and folder actions where users look for them

**Current friction:** Files offers opening, folder expansion, a delete affordance and top-level creation. It lacks context menus for Copy relative path, Reveal in explorer, New file here, Duplicate, or Rename/Move. Users must manually type paths or leave the app for common operations.

**Proposed behavior:** start with Copy relative path, Open in Code/Form when supported, Reveal active file, and New file in this folder. Add Duplicate using create-only semantics. Treat Rename/Move as a separate item requiring backend support and reference handling; changing an LHP path can affect YAML references, so do not present it as a trivial label edit. Mirror appropriate actions in tab menus.

**Acceptance:** context actions refer to the clicked item, respect viewer/read-only state, preserve dirty buffers, and never silently overwrite a destination. Disable unavailable operations with a useful explanation rather than allowing predictable errors.

Evidence: [FileTreeItem.tsx:68](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/sidebar/FileTreeItem.tsx:68), [FileBrowser.tsx:109](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/sidebar/FileBrowser.tsx:109), [files.ts:34](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/api/files.ts:34).

### U23 — Returning to a view should return to the same place

**Current friction:** editor models are recreated on view switches; config document selection and several explorer/artifact choices are component-local. Navigating away can lose the selected config section, artifact, scroll position or working context. This is distinct from losing file content: repeating navigation is itself unnecessary work.

**Proposed behavior:** retain per-document cursor/scroll state, config document/section selection, selected artifact, and graph viewport/selection where appropriate. Reveal an explicit navigation target (e.g. a clicked problem) only when requested; otherwise restore the previous position. Make file rename/deletion and project switching invalidate the relevant saved UI state deliberately.

**Acceptance:** edit near the bottom of a config, visit a graph, and return to the same section; read generated SQL/Python, visit another tab, and return to that artifact; preserve editor undo where the chosen model lifecycle supports it. Prioritize config position and active-tab reveal before broader session persistence.

Evidence: [PipelineConfigEditor.tsx:130](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/config/pipeline/PipelineConfigEditor.tsx:130), [CodeView.tsx:187](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/CodeView.tsx:187), [YamlView.tsx:107](/Users/mehdi.modarressi/Coding/Lakehouse_Plumber/web_app/src/components/shell/center/YamlView.tsx:107).

## Suggested review order

1. **Protect files and edit identity:** B01, B02, B03, B06, B10.
2. **Make save/run behavior trustworthy:** B05, B08, B09, B12, U01.
3. **Repair navigation, data freshness and recovery:** B04, B07, B11, B13–B18.
4. **Improve the workspace experience:** prioritize U16 (tab context menu), U18–U20 (config access, search and collapsing), then select from the remaining UX items. Include B19 with config clarity.
5. **Measure and tune:** U15 plus browser accessibility/responsive verification.

This is sequencing guidance, not an approved implementation plan. For each ID, mark **Implement**, **Defer**, or **Reject**; comments are especially useful for the choices around explicit save versus autosave, viewer behavior, and desktop-only versus narrower-window support. Once selected, turn the accepted items into a dependency-ordered implementation plan with scoped changes, regression scenarios and browser acceptance checks.

## Artifacts and workspace impact

Only review artifacts and local Superdesign context were added. The temporary audit test was removed from the application test directory after execution and retained here as text. No application source or dependency manifest was changed. The repository's pre-existing modifications in the performance-testing example skill files were left intact.
