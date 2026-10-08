# Editor integration API contract

Status: proposed for review on `feature/vscode-integration`.

The VS Code adapter needs a stable public way to inspect an LHP project, locate
the authored YAML behind resolved actions, and validate or preview unsaved
documents. The existing facade has seven eagerly constructed subfacades and
inspection is at its method cap. Adding editor operations to that class would
violate the method-count rule; an eighth subfacade would change the approved
composition model for this integration. This branch instead exports focused
module functions from `lhp.api` and reuses the same internal service-graph
constructor as `LakehousePlumberApplicationFacade.for_project`.

One-shot functions return frozen DTOs. Validation and text preview stream
`Iterator[LHPEvent]`, with the canonical terminal response event and error
rendezvous. The editor's unsaved inputs are project-relative overlays applied
to a bounded temporary project mirror. The mirror is also used for saved
validation and preview, so an editor read does not create generated files or
cache state in the selected project. Generation remains the canonical saved,
full-project facade operation; this API deliberately does not change its commit
semantics or expose a filtered editor generation path.

Source positions are produced by `lhp.parsers.editor_source_map` and raw-node
ownership by `lhp.core.discovery.editor_sources`. The API projection composes
those indexes with the canonical discovery, resolver, template and blueprint
provenance, and dependency analysis. YAML ranges are zero-based UTF-16 and
carry a YAML document index and path. Template and blueprint definitions remain
separate from their editable invocation nodes. The catalogue includes installed
schemas, canonical Action model schema, field help, templates, presets and
blueprints. The editor adapter consumes `lhp.api` only; it does not import the
internal source indexes.

The DTO placement is an explicit architecture exception proposed for review:
`.claude/CODING_CONSTITUTION.md` §11 defines a DTO as a frozen dataclass in
`lhp/api/responses.py` or `lhp/api/views.py`. `LOCAL/TARGET_ARCHITECTURE.md`
§8 likewise names those files and grants `views.py` a 700-line limit. At this
branch's baseline, `views.py` already has 611 lines and `responses.py` has 693.
Putting the editor projections in either file would exceed the named size grant
or make an unrelated registry too large. The new focused
`lhp/api/editor_views.py` therefore contains only editor-facing frozen DTOs,
exports them through `lhp.api`, and follows the same field-type, JSON
serialization and pickling contracts. It is not a general relaxation of §1's
frozen public contract, §3's file-size limits, the seven-subfacade composition,
or the boundaries for other DTOs. This proposed placement is part of the draft
PR review; the constitution and local target documents in the original dirty
checkout are not edited by this branch.

The resulting API is provisional. Snapshot dependency graphs are authoritative
for discovered project sources but do not claim environment-specific SQL text
after substitutions. Editor preview is the canonical source-mode generation
plan and returns generated text, but it does not claim parity with bundle sync,
monitoring finalisation, sandbox rewrites or wheel artifacts. Wheel-mode preview
returns a clear unsupported error before attempting to decode binary files.
Invalid unsaved YAML retains the last saved graph with a `stale` flag and an
exact source diagnostic; an incomplete domain draft without reliable source
context produces a project-level diagnostic instead of a fabricated location.
