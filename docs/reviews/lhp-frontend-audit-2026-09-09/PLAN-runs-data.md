# Run coordination, scope and history implementation

Owner: runs_data subagent. Branch: feat/frontend-workspace-ux-092; base release/V0.9.2 (2dad5b49).

1. B08/B09: move stream execution outside launching component lifetime, enforce one global active run, guard callbacks by run identity, and queue/coalesce save-triggered validation. Success requires an explicit terminal completion; stopping, failed transport and incomplete streams remain distinct. Stop cancels queued validation.
2. B07: make previous-validation hydration restartable under StrictMode and prevent history from replacing any live-session run, including a completed run with zero issues.
3. B12/B13/U01: reconcile pipeline scope against loaded choices and environments against project configuration, retain environment per project, show run inputs persistently, and save dirty documents before explicit execution. Save failures block execution. Save-before-run suppresses automatic validation (dependency: workspace agent shared save API).
4. B14: invalidate all file-derived resource, environment and artifact query prefixes on file pushes; refresh run details on run updates and generated artifacts after generation. Preserve manual stale-graph refresh policy for source edits.
5. U13: add history filters, explicit timestamp context, incremental loading up to the backend's documented 200-run cap, persistent selection, source issue navigation and expandable/exportable run details. Do not claim server pagination or stored stopped-state distinctions unsupported by the existing API.
6. Integrate persistent Project settings / explorer / focus controls into CommandBar (dependency: root layoutStore toggleFocusMode).
7. Verify with meaningful regression scenarios: StrictMode hydration; unmounted launcher; two controllers; queued validation; stopped/incomplete/success states; stale scope/environment reconciliation; cache invalidation; history filtering and retained selection. Run affected tests, TypeScript/build/lint and report material limitations.

## Implementation and verification

Implemented B07/B08/B09/B12/B13/B14, run-input visibility and save-before-run from U01, and history investigation controls from U13. The command bar captures environment/pipeline/config/sandbox inputs before asynchronous saving, aborts execution when saving fails or newer dirty content remains, and refuses Generate in viewer mode. Save-triggered validation waits behind active work and Stop clears that queue. Project settings, explorer and focus controls are integrated.

History provides environment/pipeline/status filters over the explicitly labelled loaded results, local timestamps, incremental loading through the backend's 200-run maximum, retained selection/events, source-line navigation, expanded details, and full JSON export. Historical backend statuses remain `completed`/`failed`/`running`; the existing history API does not distinguish stopped versus disconnected runs. The live session does distinguish stopped, incomplete and transport errors. Full server pagination, historical stop reasons and run comparison require later backend work.

Additional root-delegated U05 work: optional shared severity/file filtering in IssueList, enabled in Problems and Inspector, with clear/no-match states and stable source indexes. Diagnostic and lineage regressions verify saved-file provenance, existing structured-tab/line navigation, retained edits, details/suggestions, empty file scope, actual branching edges, independent disconnected nodes, and useful Help content.

Checks: the original focused run/data suite passed 119 tests in 11 files; CommandBar tests then passed 4 scenarios including immutable run-input capture. The final overlapping integration set passed 64 tests in 9 files. Owned source and added diagnostics/tests pass ESLint. Root integration owns the final repository-wide build, bundle gates and complete suite. Browser/layout validation remains outstanding; no live browser verification is claimed here.

## Additional integration review

Root requested review of responsive shell/navigation and assistant context preservation. Fixed and covered these concrete integration cases:

- Project-scope bootstrap completes before a deep link initializes; old-project navigation history clears, and history records the linked destination rather than the previously restored tab.
- Back/forward preserves forward history. Alt-arrow inside text inputs preserves normal word navigation. Entering focus mode by shortcut focuses the center workspace; clipboard failures are caught locally.
- Responsive drawers auto-collapse only on crossing a breakpoint, so a manually reopened drawer stays open during small width adjustments.
- Focus mode hides bottom-pane controls semantically, retains the saved expanded preference, and lets panel commands exit focus mode. Programmatically activated panes enter the retained visited set.
- Composer drafts and thread reading positions survive conversation/dock changes; drafts/positions move when the backend assigns a real session ID. IME Enter/keyCode 229 and Shift+Enter do not send. An unbound composer is disabled. The visible context note accurately describes project files on disk and invites explicit paths/pasted edits.
- Problems distinguish not validated, a successful zero-issue validation, unsaved edits, in-progress/partial outcomes, and restored historical records.

The shell/chat integration set passed 54 tests in 8 files before the final extra programmatic-pane-retention regression; owned shell/chat ESLint passed. Existing assistant provider/stream tests were included. Size-related lazy-loading edits were coordinated with the workspace agent and preserved.
