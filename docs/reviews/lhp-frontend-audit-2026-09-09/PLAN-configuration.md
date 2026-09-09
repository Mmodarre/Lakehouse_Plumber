# Configuration workstream

Scope: B19, U09, U18–U21 and configuration position from U23.

1. Preserve YAML editing semantics while separating section expansion from Add/Remove. Add search by label/YAML key, a section index, configured-only filtering and collapse/expand all. Keep hidden fields mounted to preserve drafts.
2. Keep document selection and section presentation per open file. Use a compact document selector in narrow editor containers; preserve the existing precedence rail for wide containers.
3. Expose template creation in Structure, clarify Project settings versus file defaults, show full source paths, and correct unbound run captions. Root integrates a permanent Project settings shortcut and the workspace agent unifies file opening.
4. Add a read-only saved-config preview endpoint using actual loader/resolver code. Display returned values with explicit saved-file scope and resolution limitations; do not duplicate precedence in JavaScript.
5. Add regressions for searching/collapse without dirtying, confirmed removal, retained selection, run binding captions, config creation and backend resolution/path safety. Run existing byte-preservation/config suites, TypeScript and lint.

No application commits from this workstream; root reviews and integrates the shared worktree.

Implemented: Section navigation/search indexes human labels and YAML paths, configured-only filtering reflects actual key presence, collapse retains field DOM/drafts and never mutates YAML, optional Add/Remove retains confirmation. Pipeline/job selection and form scroll are retained for open files; narrow containers use a selector. Structure has viewer-aware template creation and local load-error retry; config headers show full paths and browser-safe Ctrl/Cmd+Alt+1/2 shortcuts. Pending DraftInput text warns on reload and Enter respects IME composition. Unset boolean fallbacks are labelled as such, with Set override / Reset to inherited actions.

Saved effective preview: `GET /api/configuration/preview` delegates to the public `lhp.api.preview_configuration`. Pipeline generation and preview share `BundleManager.resolve_pipeline_settings` for merge, project event log and environment substitution. Ordinary job values use `JobGenerator.get_job_config_for_job`; monitoring uses its actual flat-job resolver. UI explicitly excludes unsaved edits, sandbox transforms and generated resource-only fields, and reports standard job token behavior. No guessed frontend merge or per-field provenance model was added.

Validation: initial five stdlib resolver tests pass using the original `.venv` Python with worktree `PYTHONPATH=src`, including actual resolver values, byte-identical disk state, job semantics, monitoring shape, traversal/symlink/env guards and missing target inheritance. Root created an isolated temporary backend environment using cached offline dependencies, preserving the original `.venv`. All 117 resolver/HTTP/affected bundle and pipeline tests passed. A real e2e fixture preview resolved successfully. TypeScript passed; dedicated navigation/draft/caption/Structure tests passed (21). Root runs final full-suite/build checks after integration.


Final integration: viewer mode permits search, section expansion, document selection and saved previews while source fields, Add/Remove, document deletion and conversion remain disabled. A file-load retry sits outside disabled fields. Review unsaved changes lazily opens the existing Monaco diff with both sides read-only. Last configuration suite: 155 passing tests before two final viewer/retry regressions; those final regressions are checked separately. No per-field provenance attribution is claimed; the preview shows authoritative values plus source tiers.
