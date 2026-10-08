# Editor integration API contract

Status: editor integration accepted on release/V0.9.3; additive sandbox extension
proposed on `feature/vscode-sandbox-0.9.3`.

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
facade operation with its existing optional sandbox scope; this API does not change its commit
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
monitoring finalisation or wheel artifacts. Wheel-mode preview
returns a clear unsupported error before attempting to decode binary files.
Invalid unsaved YAML retains the last saved graph with a `stale` flag and an
exact source diagnostic; an incomplete domain draft without reliable source
context produces a project-level diagnostic instead of a fabricated location.

Sandbox is an additive `sandbox: bool = False` keyword on editor inspection,
validation, preview and `GenerationFacade.plan_generation`. Source preview
resolves the same personal profile and team policy as generation, constructs
the canonical sandbox rewrite plan and passes it to the same generate-to-temp
primitive. SQL, bound Python parameters, copied module transformations, runtime
shims, formatting and warnings therefore share the generation implementation.
Preview also accepts `include_tests: bool = False`. Bundle and wheel output
remain outside this source-only contract.

The mirror copies only the exact `.lhp/profile.yaml` private input, including a
new unsaved profile overlay. It never traverses or copies other `.lhp` state.
Profile parent/file symlinks, aliases, traversal and oversized files are rejected
before copying; the existing aggregate mirror budget includes profile and draft
bytes. All profile, policy, environment and pipeline drafts resolve together.
Malformed sandbox profiles do not affect normal validation or preview when
sandbox mode is off. Unsafe profile paths are rejected in either mode.

`EditorProjectView.sandbox_enabled` records the explicit mode; `sandbox` reports
the existing provisional `SandboxScopeResult` with additive effective `strategy`
and `table_pattern` fields even while mode is off. The snapshot retains the full
project graph for display-only scope switching. Invalid draft fallbacks clear
resolved sandbox pipelines and report an error instead of presenting saved scope
as current. Validation and preview emit `ErrorEmitted` before raising structured
sandbox failures. No missing or invalid profile falls back to ordinary generation.

`EditorCatalogView.template_related_files` maps each project-relative template
path to declared resource references, including unused definitions. References
from resolved action instances retain their actual consuming flowgroup; declared
parameter paths remain unresolved rather than being counted as concrete files.
Frozen DTO, JSON and pickle contracts remain additive and provisional.
