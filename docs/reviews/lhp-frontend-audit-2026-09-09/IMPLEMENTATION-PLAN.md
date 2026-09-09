# Frontend workspace implementation plan

Base: locally available `origin/release/V0.9.2`, commit `2dad5b4948e20a309fa7672db878bc853dd1f066`.
Branch: `feat/frontend-workspace-ux-092`.
Worktree: `/private/tmp/lhp-frontend-workspace-ux-092`.
Git administrative repository: `/private/tmp/lhp-frontend-ux-092.git` (isolated local copy because original `.git` is read-only in this session).

User authorized subagent implementation of the reviewed backlog, with special emphasis on configuration UX and tab context menus. Original audit IDs remain the review/acceptance contract. Optional alternatives are resolved with conservative UI choices; functionality must remain faithful to LHP's backend semantics.

## Parallel workstreams and ownership

| Owner | Workstream | Audit scope |
|---|---|---|
| workspace subagent | Safe file creation/saves, document ownership and open/reveal API, common document Save, tab context menus and consolidated dirty close, open-tab picker/reopen/pin/reorder, file context actions, safe multi-flowgroup handling | B01–B06, B10; U01, U16, U17, U22; document portion U23 |
| runs_data subagent | Persistent run coordinator, cancellation/concurrency, StrictMode hydration, valid env/scope, cache invalidation, run history filtering/export/details, visible execution context | B07–B09, B12–B14; run portion U01, U13 |
| configuration subagent | Config access/creation, section search/navigation/collapse, narrow document selector, effective-value/default clarity, correct run-binding caption and state retention | B19; U09, U18–U21; config portion U23 |
| root integration | Responsive shell/focus commands, navigation/history/quick-open, keyboard and diagnostics, error recovery, graph keyboard/metadata/undo, real lineage edges, onboarding, resource help, assistant drafts/read position, density and performance review | B11, B15–B18; U02–U08, U10–U12, U14–U15; shared U23 |

Each subagent maintains a bounded plan alongside this document and writes regression tests for behavior that can lose work or mislead execution. Agents edit only their assigned worktree files and coordinate shared APIs before touching another owner's files.

## Integration order

1. Establish baseline and preserve audit artifacts in the separate worktree.
2. Agree on shared APIs: path-aware document opener/reveal; async Save All with optional suppression of auto-validation; global queued run ownership; shell focus mode.
3. Implement independent workstreams concurrently. Preserve existing ETag, YAML/comment and viewer contracts.
4. Integrate navigation and shared controls after store APIs land. Cover bulk-close failures, cross-view save, viewer mutation guards and environment changes with focused tests.
5. Run the complete frontend suite, TypeScript/build, ESLint and size gates. Investigate failures against the release baseline and fix regressions.
6. Perform additional runtime/browser checks if the session allows them. Record blocked checks without claiming visual verification.
7. Review the final diff, update the ID coverage/status record, and commit a reviewable result on the separate branch. Do not merge or push.

## Product decisions

- Explicit save with a common document toolbar; dirty-buffer handling before run is visible and deliberate.
- Closing clean tabs is immediate. Closing multiple dirty tabs uses one consolidated review; failed/conflicting saves remain open.
- Multi-flowgroup files use an explicit source-only fallback unless selected-entry identity is fully safe.
- Config collapse changes only presentation. Removing YAML configuration remains explicit and confirmed.
- Backend-derived effective settings only; never invent a second approximate precedence model.
- Laptop/narrow-window usability is supported; a phone-specific pipeline designer is not implied.
- File rename/move must account for references and backend support. Expose working operations rather than a misleading destructive shortcut.

## Validation baseline

The earlier audit on the original checkout passed 1,561 tests with Node experimental web storage disabled. This release worktree may have a different test total. Use `NODE_OPTIONS=--no-experimental-webstorage npm run test -- --run` from `web_app` for the installed environment. Existing node_modules is shared read-only by convention; do not install dependencies into that symlink. Builds write into the isolated worktree.
