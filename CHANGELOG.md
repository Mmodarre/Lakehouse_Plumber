# Changelog

All notable changes to Lakehouse Plumber are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.1] — 2026-06-10

Developer sandbox mode plus a dependency-extraction overhaul.
`lhp generate --sandbox` gives each developer a personal, namespaced copy of
their slice of the project without touching shared tables. SQL table
extraction is now parser-based and byte-faithful to substitution tokens,
Python reads declared through YAML parameters form real dependency edges, and
unresolvable reads surface as actionable advisory warnings instead of
silently missing edges.

### Added

- **`lhp web` — a single-user, localhost-only in-project Web IDE.** A new
  optional extra `lakehouse-plumber[webapp]` (FastAPI + plain `uvicorn` —
  deliberately not `uvicorn[standard]`, since uvloop/httptools/websockets/
  watchfiles are unnecessary for a localhost single-user server) serves a
  browser IDE bound to `127.0.0.1`. The React SPA is pre-built at CI/release
  time and vendored into the wheel, so users never need Node. The IDE browses
  the project, pipelines, flowgroups, tables, and the dependency graph; edits
  YAML in Monaco with canonical JSON-schema validation (served from the
  packaged schemas via `GET /api/schemas/{kind}`); creates, edits, and deletes
  files behind a path-traversal guard with write-protected paths (`.git/`,
  `generated/`, `.lhp/logs/`, `.lhp/dependencies/`); streams `validate`/
  `generate` runs live over NDJSON with progress and a Problems panel; and
  auto-validates YAML on save with inline syntax markers (saves persist; `PUT`
  returns the YAML syntax position). Flags: `--port`, `--no-open`, `--reload`.
  A missing extra surfaces as a friendly `LHP-IO-026` error.
  - Dropped relative to the experimental `feature/lhp-web-app` branch (cut, not
    deferred): authentication & `/me`, workspace mode, git integration,
    state/staleness tracking, structured YAML CRUD endpoints, `PUT`
    project/pipeline config, dependencies export, the generate
    plan/preview/single-flowgroup/browse endpoints, `/validate/yaml` and
    flowgroup preview-yaml, and the visual flowgroup builder. The chat
    assistant, cut from that branch alongside these, returns in 0.9.1 as the
    assistant panel — built-in Claude provider by default, Omnigent daemon
    selectable (see "Built-in Claude assistant provider" below).
  - Deferred (returns in a later version):
    - Full multi-level dependency-graph DTO with per-level drill — v1 is
      pipeline-level only; needs a public graph DTO → v2.
    - Preset detail resolved-chain view — no public preset-resolution surface
      yet → v2.
    - Semantic per-field YAML diagnostics in the editor — v1 has syntax-level
      markers plus JSON-schema validation → v2.
    - Related-files for template-driven flowgroups — raw-YAML walking cannot
      see template-side file references.
    - Structured YAML CRUD — returns behind the planned `AuthoringFacade` → v2.
    - Table metadata: the materialized-view vs streaming-table distinction is
      not surfaced — the public `ActionView` carries no target-type
      discriminator, so the tables page reports `streaming_table`/`sink`.
    - Generation/validation streams run with bundle support disabled — bundle
      preflight needs a pipeline config, which is not applicable to the IDE
      preview flow.
- **Web IDE hardening.** A usability and security pass over the `lhp web`
  server, CLI launcher, and SPA:
  - **Live edits without restart** — file writes and deletes through the IDE
    invalidate the server's cached facade, so `/api/flowgroups`, validation,
    and generation always reflect the on-disk YAML.
  - **Optimistic concurrency** — file `GET`s return a strong ETag over the raw
    bytes; a `PUT` with a stale `If-Match` fails with `412 Precondition
    Failed` (the SPA offers a reload) instead of silently overwriting a
    concurrent edit.
  - **Session-token auth** — `lhp web` mints a per-session token, passes it to
    the server via `LHP_WEBAPP_TOKEN`, and echoes it in the printed URL
    fragment (`#token=`); every `/api/*` route except `/api/health` requires
    it (header `X-LHP-Token` or `?token=`). TrustedHost and Origin guards
    reject DNS-rebinding hosts (400) and cross-origin state-changing requests
    (403); reads with a foreign `Origin` are unaffected. The `dev_mode`
    setting is removed.
  - **UTF-8 file I/O everywhere** — reads and writes are UTF-8 end-to-end
    (platform default encoding no longer leaks in); `GET` of a non-UTF-8 file
    returns `415` instead of mojibake.
  - **JSON API 404s** — unknown `/api/*` paths return JSON `{"detail": ...}`
    (never the SPA's HTML shell), while non-API paths fall back to the SPA
    shell for client-side routes; dependency drill views degrade gracefully
    on 404 instead of crashing.
  - **Health reports project state** — `/api/health` gains `project_state`
    (`"ok"` / `"no_project"`) and `root`, so the SPA can render init guidance
    instead of a broken IDE when the served root has no `lhp.yaml`.
  - **Launch hardening** — `lhp web` preflights the port and fails with a
    framed `LHP-IO-027` error (instead of a raw `Errno 48` traceback) when it
    is already taken, and opens the browser only after a readiness poll on
    `/api/health` succeeds.
  - **Frontend resilience** — per-modal error boundaries, an unsaved-changes
    `beforeunload` guard, and a degraded-health banner.
  - Webapp tests now run in CI as a dedicated `webapp-tests` job (previously
    cluster-gated).
- **Web IDE persistence & live updates.** A follow-up pass adding run history
  and a server-push channel to `lhp web`:
  - **SQLite run history** — every `validate`/`generate` stream is recorded to
    `<project_root>/.lhp/webapp.db` (plain `sqlite3`, WAL journal mode,
    `PRAGMA user_version` migrations): run metadata, every NDJSON frame, and
    extracted issues. Runs left in `running` state by a crash are marked
    `failed` at the next server startup, and history is pruned to the newest
    100 runs / 30 days.
  - **Run history API** — `GET /api/runs` lists recorded runs (kind, env,
    status, timestamps, result summary); `GET /api/runs/{run_id}` returns one
    run's summary and issues, plus the full recorded frame stream with
    `?include_events=true`.
  - **`GET /api/events` — SSE push channel** — a single Server-Sent-Events
    stream (session token via `?token=`, comment heartbeat every 15 s)
    carrying `run-updated` events at run start/finish and `file-changed`
    events with changed rel paths.
  - **File watcher** — a stdlib mtime-poll watcher (2 s tick, no extra
    dependency) detects project-file changes made *outside* the IDE (editor,
    git, another terminal), invalidates the server's cached facade, and
    publishes `file-changed` — so API reads and the SPA reflect on-disk edits
    without a restart or a `PUT`.
  - **Push-driven SPA refresh** — the SPA subscribes to `/api/events` and
    maps pushed events to TanStack Query invalidations (files, flowgroups,
    dependency graph, run history), with exponential-backoff reconnect and a
    full refetch after a reconnect gap.
  - **Generated API types** — `npm run gen:api` regenerates
    `src/types/api.generated.ts` from the FastAPI OpenAPI schema via
    openapi-typescript (key-sorted for determinism), so Python↔TypeScript
    drift surfaces as a tsc break instead of a runtime surprise.
  - **Dependency pins & log hygiene** — `fastapi>=0.115,<1.0` and
    `uvicorn>=0.34,<1.0` upper bounds; uvicorn's access log is disabled (it
    would print full request targets, including `?token=`) in favor of the
    existing query-free request-logging middleware.
- **Web IDE visual overhaul.** A design-system pass over the `lhp web`
  frontend:
  - **oklch design-token system** — all colors flow from CSS custom
    properties defined in oklch, with light, dark, and system themes (the
    persisted choice is applied by an inline script before the bundle loads,
    so a dark-mode reload never flashes light). Monaco editors, the
    dependency-graph canvas, and toasts all follow the resolved theme.
  - **shadcn/Radix UI primitives** — dialogs, alert-dialogs, dropdowns,
    selects, tabs, tooltips, and popovers replace the hand-rolled modals,
    bringing focus traps, Escape handling, and accessible titles/labels;
    `window.confirm` prompts are gone.
  - **lucide icons** replace emoji glyphs throughout the UI.
  - **Bundled fonts** — Inter and JetBrains Mono ship inside the SPA bundle
    (`@fontsource`); no font CDN requests leave the machine.
  - **Unified semantic colors** — one set of action-kind (`load` /
    `transform` / `write` / `test`) and severity (error/warning) tokens is
    shared by the file tree, flowgroup tables, detail views, and the
    dependency graph, plus a new status bar in the shell.
  - **Accessibility pass** — `aria-live` announcements for run outcomes,
    `aria-sort` on sortable table headers, and titled dialogs.
  - **Frontend engineering foundation** — a vitest + Testing Library harness
    for store and component tests, and a dedicated `webapp-frontend` CI job
    running eslint, `tsc -b`, vitest, the generated-API-types drift check,
    and the production build.
- **Web IDE workspace & lifecycle.** A follow-up pass replacing modal editing
  with a persistent workspace and adding project-lifecycle pages:
  - **Persistent editor workspace** — files and flowgroups open as tabbed
    buffers in a workspace editor (Monaco) instead of one-shot modals; buffers
    survive navigation, dirty tabs are marked and guarded on close, route
    change, and `beforeunload`, and saves keep the ETag optimistic-concurrency
    loop (a stale save surfaces a conflict dialog with a Monaco side-by-side
    diff of the on-disk vs in-buffer content — reload theirs, keep yours, or
    overwrite).
  - **In-app project init wizard** — when the served root has no `lhp.yaml`
    (`project_state: "no_project"`), the SPA renders an init wizard instead of
    a broken IDE; `POST /api/project/init` scaffolds the project (optionally
    with bundle support) into the served root, runs the run-history DB
    migrations, and flips the server to `"ok"` without a restart (repeat init
    responds `409`).
  - **Lifecycle pages** — new lazy-loaded Blueprints (definitions with
    parameter/flowgroup counts and expanded instances), Presets (raw YAML plus
    the resolved deep-merged config and base→leaf `extends` inheritance
    chain), Templates, Environments (declared substitution files with fully
    resolved tokens per environment, opening straight into a workspace
    buffer), and Run History (recorded `validate`/`generate` runs with their
    extracted issues) pages, grouped under a "Resources" dropdown in the
    header.
  - **Public API: `InspectionFacade.resolve_preset`** — resolves a preset's
    `extends` chain and returns a `PresetResolutionResult` DTO (base→leaf
    chain plus the deep-merged configuration); a broken or unknown `extends`
    surfaces as `LHP-ACT-001`.
  - **New endpoints** — `GET /api/blueprints` (`?include_instances=true` for
    per-instance expansion), `POST /api/project/init`,
    `GET /api/environments/{env}/resolved`, and `GET /api/presets/{name}` now
    returns the resolved config and inheritance chain.
  - The SPA also migrates to React Router's data router (`createBrowserRouter`
    with lazy route modules), and the Problems panel and Run History share one
    issue-list component.
- **Built-in Claude assistant provider — the default.** The web IDE's
  assistant panel now runs on an in-process provider powered by the
  [Claude Agent SDK](https://code.claude.com/docs/en/agent-sdk):
  `pip install 'lakehouse-plumber[webapp]'` is the only install step (the
  SDK's platform wheels bundle a self-contained `claude` runtime — no
  Node.js, no daemon, nothing else to start). The setup card preselects it;
  the Omnigent daemon (below) remains selectable as an alternative provider.
  - **Two new `webapp`-extra dependencies:** `claude-agent-sdk` (the
    in-process provider) and `databricks-sdk` (pure-Python bearer minting
    for the Databricks auth mode — no `databricks` CLI binary required).
    Both are sanctioned by constitution §5.8; the core install never pulls
    them.
  - **Token-free sign-in, two ways.** *Claude subscription*: the machine's
    Claude Code login (or the NAME of an env var holding a
    `claude setup-token` token — validated as an identifier, never a token
    value). *Databricks workspace*: a `~/.databrickscfg` profile; a bearer
    is minted fresh per turn via `databricks-sdk` against the workspace's
    Anthropic-compatible `/ai-gateway/anthropic` endpoint, never cached,
    stored, or logged.
  - **Same chat experience, same wire protocol.** The provider reproduces
    the panel's pinned NDJSON frame vocabulary byte-for-byte — streaming
    markdown, reasoning disclosure, tool cards, approval cards
    (read-only tools run without asking; everything else asks), interrupt,
    and reload rehydration (transcripts persist locally in the webapp
    store; SQLite schema v3, append-only).
  - **Permission modes.** A selector under the chat composer picks the
    approval policy per message, mirroring Claude Code's modes: *Ask every
    time* (the default), *Accept edits* (file edits run without asking;
    commands and web access still ask), and *Allow all* (nothing asks).
    The choice is remembered in the browser and rides along with each
    turn (`permission_mode` on `POST /api/assistant/chat`).
  - Executor configs gain a `provider` field (`claude_sdk` / `omnigent`);
    configs stored by earlier 0.9.1 dev builds read as `omnigent`
    unchanged.
  - **Friendlier tool and approval cards.** Tool calls render as a one-line
    summary — the verb plus the argument that matters (file path relative
    to the project, shell command, search pattern, URL) with a pass/fail
    icon; arguments and output stay behind a "Details" disclosure instead
    of raw JSON. Approval cards show the same summary with the full request
    one disclosure away.
  - **Resizable assistant panel.** The dock's left edge is a drag handle
    (300–760 px, keyboard-adjustable); the width is remembered in the
    browser.
- **Web IDE AI assistant panel (Omnigent provider).** `lhp web`'s chat
  panel (a right-hand dock opened from the header) can alternatively be
  backed by a locally running
  [omnigent](https://github.com/omnigent-ai/omnigent) daemon — an agent
  runtime that answers questions about the project and edits its files in
  place:
  - **The daemon is user-installed — never an LHP dependency.** omnigent is
    Python 3.12-only; install it yourself (e.g. `uv tool install omnigent`) —
    LHP does not declare, vendor, or install it. The panel walks a detection
    ladder (binary → server → host) and offers "Start it for me", which spawns
    `omnigent server start` and `omnigent host` as detached, user-owned
    processes logging to `.lhp/logs/omnigent-server.log` /
    `.lhp/logs/omnigent-host.log`. LHP retains no handles and never waits for,
    stops, or kills them — they deliberately outlive the IDE so live agent
    sessions survive a server restart.
  - **One-time executor setup.** A first-use card picks how the assistant
    runs: omnigent defaults (credentials from `omnigent setup`), a Databricks
    CLI profile (model calls through the Databricks gateway; LHP reads profile
    *names* from `~/.databrickscfg`, never credential values), or the NAME of
    an API-key environment variable on the omnigent server (validated as an
    env-var identifier — pasted key material is rejected, and no secret value
    ever transits or is stored by LHP). Changing the executor takes effect on
    the next chat turn.
  - **Chat turns stream over NDJSON** through the LHP server (the browser
    never talks to the daemon directly): rendered markdown, tool-call cards,
    approval cards (accept / decline / cancel), a collapsed reasoning
    disclosure, turn interrupt, a files-changed count chip, and
    session-snapshot rehydration when the panel reloads. Assistant file edits
    land on disk like any external edit — the file watcher and push channel
    refresh open IDE views.
  - **Backstop gates** return structured 409s instead of half-starting a
    turn: `LHP-WEB-001` (executor not configured), `LHP-WEB-002` (LHP skill
    not installed in the project), `LHP-WEB-003` (no omnigent host online).
  - **`LHP_WEBAPP_ASSISTANT_URL`** overrides the daemon address (default
    `http://127.0.0.1:6767`) and is loopback-only (`127.0.0.1`, `localhost`,
    `::1`): the omnigent REST API is unauthenticated, so a non-loopback URL
    is rejected at startup. Run the daemon only on a trusted, single-user
    machine.
  - **Public API: `SkillFacade.install_project_skill`** — installs or
    force-updates the packaged LHP skill into a project's
    `.claude/skills/lhp/` (the panel uses it to provision the skill the
    assistant relies on), returning the new `SkillInstallResult` DTO
    (`api/responses.py`): install dir, skill version, previous version,
    `installed`/`updated` action, installed files, and `CLAUDE.md`
    routing-block status. Both are `:stability:` provisional.
- **Web IDE Configuration UI.** The `lhp web` IDE gains a Config section
  that edits all three configuration surfaces as forms — `lhp.yaml`
  (project), `config/pipeline_config*.yaml` (pipeline defaults,
  per-pipeline overrides, and anonymous groups `pipeline: [a, b]`), and
  `config/job_config*.yaml` / `config/monitoring_job_config*.yaml`
  (orchestration jobs) — with a per-tab file picker and
  create-from-template for missing files, backed by a new
  `GET /api/config-templates/{kind}` endpoint serving the packaged
  `lhp init` templates:
  - **Comment- and passthrough-preserving saves.** Form saves round-trip
    the file through a YAML Document-API layer: only the changed lines are
    touched, hand-written comments and key order survive, unknown
    ("passthrough") keys are shown as read-only chips and written back
    verbatim, and disabling an optional section removes its key entirely
    (never `key: null`). Deleting an entry rewrites only the containing
    document — comments move with their settings, but placement can shift.
  - **Precedence rail.** The pipeline and job editors list a file's
    documents ordered the way the loaders merge them — built-in defaults
    (read-only) → `project_defaults` → per-pipeline/per-job documents —
    with cross-document duplicate-pipeline badges. Legacy single-document
    `job_config` files get an explicit "Convert to multi-document format"
    action (a save never converts silently); monitoring job configs are
    edited as the flat single document their loader mandates.
  - **Run wiring.** A "Use for runs" toggle binds a pipeline config file
    to IDE Validate/Generate — the CLI `-pc`/`--pipeline-config`
    equivalent, carried as the new optional `pipeline_config` field on the
    run-stream request — with the active selection shown in a header chip.
    With a config bound, generation on a project with `databricks.yml`
    runs with bundle support enabled and regenerates `resources/lhp/`, so
    bundle resource generation is now reachable from the IDE; without one,
    IDE generation keeps its bundle-disabled preview behavior. Backend
    facades are cached per config path.
  - **JSON schemas for config files.** New `pipeline_config.schema.json`
    and `job_config.schema.json` describe the multi-document layouts and
    drive Monaco YAML hints for `config/pipeline_config*` /
    `config/job_config*` / `config/monitoring_job_config*` files
    (validated per document); `project.schema.json` is refreshed against
    the Pydantic model (adds `test_reporting`, `wheel`, `sandbox`,
    `apply_formatting`) with a schema-model parity test.
  - **Conflict handling matches the editor.** Stale saves are rejected in
    both directions (form save over a raw-YAML edit and vice versa) with a
    reload dialog, and a banner flags on-disk changes while a form holds
    unsaved edits. "Open raw YAML" remains the escape hatch for anything
    the forms do not cover.
- **Web IDE Designer — a visual flowgroup builder.** The `lhp web` IDE gains a
  Designer that drills from a pipeline down through its flowgroups to a
  per-flowgroup action graph, then into a canvas that edits one flowgroup as a
  node-and-edge diagram instead of raw YAML:
  - **Drill-down graph levels.** The dependency views navigate
    pipeline → flowgroup → action graph. Action edges are derived client-side
    from each action's `source` / `sources` / `view` / `table` and labelled
    with the view name they carry (a "named-pipe" edge visual); manual
    `depends_on` links render as a distinct edge kind. Derivation needs no
    server round-trip, so the graph renders for unsaved files too.
  - **Flowgroup Designer canvas tab.** A flowgroup opens as a canvas of
    load / transform / write / test action nodes; selecting a node opens a
    per-action form generated from that sub-type's spec (one for every
    load/transform/write/test sub-type). Forms edit explicit YAML only —
    clearing a control deletes the key (never writes `''` / `null`) and keys
    the spec does not name are left untouched; soft-validation hints show on
    the field but never block a write. SQL / Python / expectations fields open
    in a Monaco modal or as a companion file, and a fan-in affordance appends a
    producer view to an action's `source`.
  - **Immediate, comment-preserving write-through.** Each committed field edit
    PUTs the flowgroup file instantly through the shared YAML Document layer:
    only the changed node is touched, hand-written comments and key order
    survive, and edits serialize on a promise chain so a second edit applies on
    top of the first (a stale save raises the same `412` conflict dialog as the
    editor). Structural list/map edits are index-safe under rapid clicks: adds
    resolve their position from the freshly-parsed document and destructive
    controls are disabled while a write is in flight.
  - **Creation flows.** New actions come from an action palette; a flowgroup is
    created blank, from a template (`use_template` with its parameters), or from
    a blueprint — and templates themselves are authored on the same canvas with
    a parameters panel.
  - **Dev-sandbox scope toggle.** A toggle surfaces the developer-sandbox scope
    (`SandboxFacade.describe_scope`) — whether `.lhp/profile.yaml` opts in and
    which pipelines a `--sandbox` run would cover.
- **`lhp init <name> --sample` — scaffold a complete, runnable sample
  project.** The new flag generates a TPC-H medallion-architecture project:
  bronze ingestion via `delta` and `cloudfiles` loads, silver CDC in both
  flavors (streaming AUTO CDC with deletes, and snapshot CDC), a gold
  materialized view, plus templates, presets, every substitution syntax,
  operational metadata columns, a data-prep notebook, and a hand-authored
  Declarative Automation Bundles job. The project runs against the
  `samples.tpch` dataset available on any Unity Catalog workspace. `--sample`
  requires bundle support and cannot be combined with `--no-bundle`.
- **Developer sandbox mode** — `lhp generate --sandbox` and
  `lhp validate --sandbox` (mutually exclusive with `-p`/`--pipeline`). A
  gitignored personal profile (`.lhp/profile.yaml`, `sandbox:` key) declares a
  `namespace` and the `pipelines` in scope (exact names or globs); an optional
  `sandbox:` block in `lhp.yaml` sets team policy — `strategy` (v1: `table`
  only), `table_pattern` (default `{namespace}_{table}`), and `allowed_envs`.
  Generation and validation are scoped to the profile's pipelines, and every
  table produced in scope is renamed through the pattern — write targets,
  delta-sink `tableName`, and in-scope reads, across structured fields, SQL
  bodies in generated `spark.sql` literals (including single-quoted table
  arguments to `table_changes(...)` and `IDENTIFIER(...)`), table literals and
  f-string SQL literal segments in copied Python modules, and table refs passed
  as YAML parameter values into user Python code (python-transform
  `parameters`, python-load `source.parameters`, and snapshot-CDC
  `source_function.parameters`, whole-value match only) — while reads of tables
  produced outside the scope stay pointed at the shared tables (read-shared /
  write-own). A recognized Python table read whose name argument is only known
  at runtime is wrapped in a generated `__lhp_sandbox_table(...)` helper that
  applies the same rename at execution time. Bundle resource files regenerate
  for the scoped pipelines only, the project event-log table name is namespaced
  through the same pattern, and the monitoring phase is skipped so shared
  monitoring artifacts are never clobbered. Deferred to a
  follow-up (v1 limitations):
  - `catalog` / `schema` rename strategies — v1 ships the `table` strategy
    only (the strategy seam is in place).
  - No sandbox teardown command — `bundle destroy` removes the sandbox
    streaming tables and materialized views, but Delta-sink tables created by
    sandbox runs are not auto-removed (manual cleanup; see the
    "Develop in a sandbox" how-to).
  - `lhp validate --sandbox` applies the structured renames but does not
    surface the Python unrewritable-read warnings or wrap opaque reads —
    `LHP-VAL-066`, `LHP-VAL-067`, and the runtime shim are generate-only in
    v1.
  - Per-pipeline explicit `event_log:` dicts are not rewritten — only the
    project-level composed event-log table name is namespaced.
- **Sandbox warnings and errors.** `LHP-VAL-065` warns when a sandbox-renamed
  sink table is also produced by out-of-scope pipelines (mixed producers; the
  rewrite proceeds); `LHP-VAL-066` warns when an in-scope Python read that
  statically resolves to an in-scope table cannot be rewritten because its
  argument is not a plain string literal (a bound variable, static
  concatenation, `.format(...)`, resolved f-string, or constant-key container
  subscript); and the new `LHP-VAL-067` warns when a `spark.sql(...)` body
  names tables only known at runtime (an opaque argument, or an f-string with
  two or more interpolated name parts) so it can be neither verified nor
  rewritten. An opaque table *read* whose name argument is runtime-only is
  wrapped in the `__lhp_sandbox_table(...)` runtime shim instead, with no
  warning. All three ride the warning stream with category `sandbox` and never
  fail a run; `LHP-VAL-066` and `LHP-VAL-067` are emitted by `lhp generate`
  only. New errors `LHP-IO-025` (missing `.lhp/profile.yaml`), `LHP-CFG-062`
  (invalid `sandbox:` block in `lhp.yaml`), `LHP-CFG-063` (invalid
  `table_pattern`), `LHP-CFG-064` (invalid personal profile), `LHP-CFG-065`
  (environment not sandbox-enabled), and `LHP-VAL-064` (a profile `pipelines`
  entry matches no pipeline) cover the failure modes.
- **Sandbox documentation** — a "Develop in a sandbox" how-to, a "Sandbox Mode
  Reference" page, error-reference entries for all nine sandbox codes, and
  AI-skill parity updates (new `sandbox.md` reference plus `project-config.md`
  and `errors.md` additions).
- **Parameter-bound Python dependency resolution.** Table reads built from
  YAML-declared parameters now form dependency edges, mirroring the generated
  code exactly: snapshot_cdc `source_function.parameters` (keyword-only
  binding), Python transform `parameters` (positional dict), and Python load
  `source.parameters` (positional dict, applied to `get_df` by default). The
  analyzer also unrolls for-loops over statically-known string lists and
  resolves string methods (`.replace`, `.upper`, `.lower`, `.strip`,
  `.lstrip`, `.rstrip`, `.join`), subscripts, `.get()`, and binding-aware
  f-strings. Calls routed through helper functions are deliberately not
  followed.
- **Dependency-extraction warnings, on by default.** `LHP-DEP-002` flags a
  recognized Python table-read whose argument cannot be statically resolved;
  `LHP-DEP-003` flags an unparseable SQL body. Both are advisory-only — they
  never fail a run — and each carries flowgroup/action/file/line context and
  recommends an explicit `depends_on`. Warnings surface on `lhp dag` stderr
  and in the JSON (`warnings` + `metadata.total_warnings`) and text outputs,
  but never in DOT or job/orchestration YAML. New provisional public DTO
  `DependencyWarningView` (`api/responses.py`), exposed as
  `DependencyAnalysisResult.warnings`.
- **E2E coverage for dependency extraction** — a new fixture pipeline
  `19_dependency_bindings` exercises parameter-bound snapshot/transform reads,
  loop unrolling, and `LHP-DEP-002`; orchestration, master-job, and monitoring
  baselines were regenerated to include it, and mid-segment-token SQL
  extraction gained dedicated e2e coverage.
- **Persistent on-disk parse cache under `.lhp/cache/`.** Default-on: each
  pipeline YAML file gets one cache shard under `<project>/.lhp/cache/parse/`,
  keyed by resolved path, mtime, size, installed LHP version, cache schema
  version, and the flowgroup model shape — so upgrading LHP or touching a file
  invalidates its shard automatically. The cache is delete-safe at any time:
  corrupt or stale shards silently degrade to a fresh parse, and results are
  identical with or without it (the `.lhp/` directory is already covered by
  the init templates' gitignore).
- **`--no-cache` flag on `lhp generate`, `lhp validate`, `lhp dag`, and
  `lhp deps`** — disables the persistent parse cache for a single run.
- **`LHP_NO_CACHE` environment variable** — a truthy value (`1`, `true`,
  `yes`, case-insensitive) disables the persistent parse cache for every run
  in that environment (e.g. CI).
- **`--max-workers` on `lhp dag`/`lhp deps`** — same precedence as
  `generate`/`validate`: explicit flag → `LHP_MAX_WORKERS` env var →
  automatic default.
- **`for_project(no_cache=...)`** — new keyword-only parameter on the public
  facade constructor (`:stability:` provisional).
- **Unity Catalog table & column tagging.** Streaming-table and
  materialized-view write actions can declare UC **tags** at the table level
  (a `tags:` mapping on `write_target`, or the `tags:` block of a `tags_file`)
  and at the column level (the per-column `tags:` inside a `tags_file`). Because
  Spark Declarative
  Pipelines cannot set UC tags as part of table creation, LHP collects all
  declared tags and emits a single per-pipeline `_uc_tagging_hook.py` that runs
  as a `@dp.on_event_hook` and applies them via the Unity Catalog *Entity Tag
  Assignments* REST API — during and at the end of a pipeline update. Existing
  tag state is read once at pipeline initialization with a single
  `system.information_schema` query (`table_tags` `UNION ALL` `column_tags`).
  - **On by default; opt in by declaring tags.** The hook is generated only
    when some table declares `tags` or a `tags_file`. The `uc_tagging` block in
    `lhp.yaml` is optional: `enabled` (default `true` — set `false` to disable),
    `remove_undeclared_tags` (default `false`), `tag_update_concurrency`
    (default `16`, range 1–20).
  - **Additive by default.** `remove_undeclared_tags: false` only creates/updates
    declared tags; `true` reconciles to the declared state, deleting existing
    tags whose key is not declared for a managed entity (can remove tags applied
    by other tools). An explicit `tags: {}` means "managed with an empty set".
  - **Best-effort and non-blocking.** Only the table-creating action is tagged
    (`create_table: true`); temporary tables and sinks are excluded. Tag-write
    failures surface as event-log warnings and never fail the pipeline (event
    hooks cannot). Key-only tags use `""`, `~`, or an omitted value.
  - **Permissions.** A pipeline that declares `tags` needs `APPLY TAG` on the
    table and `ASSIGN` on required governed tags, plus `USE CATALOG`,
    `USE SCHEMA`, and `SELECT` on `system.information_schema`.
  - **Unified schema/tags file (one file for both fields).** A write target's
    `tags_file:` and `table_schema:` share a single unified file format
    (convention `schemas/<table>.yaml`) — one file can serve BOTH (`table_schema`
    reads the column types, `tags_file` reads the UC tags) or they can point at
    different files. `tags_file` is mutually exclusive with an inline `tags:`
    mapping; `table_schema` combines with either. The file's recognised keys are
    an **optional** identifier `table` (canonical) or its alias `name`, a
    table-level `tags:` mapping, and a `columns:` list; the legacy schema keys
    `version`/`description`/`primary_key` are tolerated and ignored (`version` is
    no longer validated). Each `columns:` entry has a required `name` plus
    optional `type`/`nullable`/`comment` (read by `table_schema`) and `tags:`
    (this column's UC tags, read by `tags_file`). The former top-level
    `column_tags:` key is now an unknown-key error. When used as a `tags_file`
    the identifier should match the write target's table name; a mismatch (or a
    `table`/`name` disagreement, with `table` winning) logs an `LHP-CFG-068`
    warning and generation proceeds using the write target's table (skipped under
    `--sandbox`, which renames the table).
  - **`uc_tags/` retired; UC tags live under `schemas/`.** `lhp init` no longer
    scaffolds a `uc_tags/` directory — the unified schema/tags files live under
    `schemas/`, and one file can back both a write target's `table_schema` and
    its `tags_file`.
  - New error codes **`LHP-CFG-066`** (a declared UC tag key or value is illegal
    under Unity Catalog charset/length rules, raised at generation time),
    **`LHP-CFG-067`** (an invalid unified schema/tags file), the warning-only
    **`LHP-CFG-068`** (a `tags_file` identifier does not match the write target's
    table, or `table` and `name` disagree — logged, never raised; generation
    proceeds using the write target's table), and the warning-only
    **`LHP-CFG-069`** (a `table_schema` file carries UC tags but is not also
    wired as that action's `tags_file`, so the tags are dropped — logged at
    generate time by the streaming-table and materialized-view writes only).
    Plus a `uc_tagging` project-config block with JSON-schema validation, the
    public `PlannedFileView.kind` literal gains `uc_tagging_hook`, docs in
    `docs/reference/actions/write.rst` with skill-MD parity, and golden + e2e
    tests.
  - **Deferred (returns in a later version):** the `LHP-CFG-068`/`LHP-CFG-069`
    tags warnings are emitted on CLI stderr / server logs only, not as
    structured `WarningEmitted` events — the web IDE's event stream does not
    surface them yet. Routing these commit-phase warnings into the structured
    stream needs a `warnings` field on the `PipelineDelta` public DTO and is
    intentionally deferred.

### Changed

- **Dependency-graph node ids are now pipeline-qualified.** The provisional
  `DependencyGraphNodeView.id` value changed shape: flowgroup nodes are keyed
  `{pipeline}.{flowgroup}` and action nodes `{pipeline}.{flowgroup}.{action}`
  (pipeline nodes are unchanged). This closes a collision where two
  same-named flowgroups in different pipelines merged into one graph node.
  Node `label`/`flowgroup`/`pipeline` fields stay bare; only the `id` value
  is qualified. Consequently `lhp dag` / `lhp deps` **DOT and text** output now
  render pipeline-qualified node names (the same collision previously merged
  same-named flowgroups in those outputs too); the `job` / JSON pipeline outputs
  are unaffected.
- **SQL table extraction is now parser-based.** Dependency analysis parses SQL
  with `sqlglot` (Spark/Databricks dialect) instead of regexes, replacing the
  legacy regex SQL parser.
- **Unresolvable f-string reads no longer emit a `"{var}"` marker.** F-strings
  containing names that cannot be statically resolved previously produced a
  junk `"{var}"` placeholder table in external sources; such reads are now
  reported as unresolved via `LHP-DEP-002`, and the junk `{var}` entries
  disappear from `lhp dag` outputs.
- **`LHP-DEP-002` messages now name the unresolved expression.** The advisory
  reads ``Cannot statically resolve the table argument of `<call>(...)` — the
  value of `<expr>` is only known at runtime``, quoting the exact argument
  expression (e.g. `os.environ['TBL']`, `helper(x)`) instead of a generic
  "not statically resolvable" note.
- **Extraction warnings are now aggregated per unresolved read site.** The
  graph builder groups leaf advisories by `(code, file_path, line, message)`,
  emitting one record per read *site* rather than one per affected action. A
  helper referenced by thousands of near-identical actions (one field case
  produced 11,451 warnings from four helper sites) now yields four records.
  Each record carries a representative `flowgroup`/`action`, the
  `edit_yaml_path` where a `depends_on` fix belongs, and the full
  `affected_actions` list (with `affected_count`).
- **`depends_on` now suppresses the action's extraction advisories.** A
  non-empty `depends_on` on an action silences its `LHP-DEP-002`/`LHP-DEP-003`
  warnings — the author has taken manual control of that action's edges.
  Suppression is per *action*, not per read (matching a declared entry to an
  opaque read is uncomputable), so an action with two opaque reads and one
  declared upstream stops warning about the second read as well. Declared
  entries still contribute edges additively, unchanged.
- **`--expand-blueprints` is now a deprecated, ignored no-op.** Blueprint
  synthetic flowgroups are always fully expanded (see Fixed, below), so the
  flag no longer changes behavior; passing it prints a deprecation notice and
  emits a `DeprecationWarning`. `--blueprint <name>` (restrict analysis to one
  blueprint) is unchanged.
- **Removed the `expand_blueprints` keyword argument** from the provisional
  `DependencyFacade.analyze_dependencies` and
  `DependencyFacade.save_dependency_outputs` (constitution §1.13
  provisional-API removal notice). Dependency analysis is always fully
  expanded; the parameter no longer had any effect.
- **`analyze_dependencies` / `save_dependency_outputs` moved to a new
  `DependencyFacade`.** The two provisional dependency methods relocate from
  `facade.inspection.*` to `facade.dependency.*` (home
  `lhp/api/_dependency_facade.py`, re-exported from `lhp.api`); this is a hard
  move with no compatibility alias left on `InspectionFacade` (constitution
  §1.13 provisional-API relocation notice). Signatures and behavior are
  unchanged. The split keeps `InspectionFacade` under the §3.2
  15-public-method cap and gives the dependency surface room to grow its own
  refresh/persistence paths.
- **`lhp web` serves the dependency graph stale and rebuilds only on an
  explicit Refresh.** While the project is edited the IDE now serves the
  last-good dependency graph instead of rebuilding on every file change: the
  file watcher marks the graph stale (a new `graph-stale` SSE event) rather
  than dropping the cached facade, `GET /api/dependencies/staleness` reports
  freshness in O(1) from a per-app flag, and `POST /api/dependencies/refresh`
  forces a fresh rebuild. Backed by a new keyword-only `force_rebuild`
  parameter on `DependencyFacade.analyze_dependencies` (bypasses the in-process
  memo and the on-disk graph cache, re-reads disk, and re-persists — a §1.13
  provisional-API minor addition) plus a new provisional
  `DependencyFacade.describe_graph_staleness` method returning the new
  provisional `DependencyStalenessResult` DTO.
- **Public API `DependencyWarningView` gained `edit_yaml_path`,
  `affected_actions`, and `affected_count`; new `AffectedActionView` DTO**
  exported from `lhp.api` (both `:stability:` provisional). In the JSON
  output each `warnings[]` entry gains `edit_yaml_path`, `affected_actions`
  (array of objects), and `affected_count`; `metadata.total_warnings` now
  counts distinct read sites, and a new `metadata.total_warning_occurrences`
  counts site × affected-action pairs.
- **Project-scope `lhp skill install` now requires an initialized LHP
  project.** Run outside a directory containing `lhp.yaml`, it fails with
  `LHP-CFG-011` ("Not a LakehousePlumber project directory") and points at
  `lhp init <project_name>`. `lhp skill install --user` (targeting
  `~/.claude/`) is unchanged and does not need a project.
- **`lhp dag`/`lhp deps` no longer run per-flowgroup configuration
  validation.** The dependency-analysis path still fully resolves flowgroups
  (templates, presets, substitutions) and secret-reference validation still
  runs, but per-flowgroup configuration errors (`LHP-VAL-007`) are no longer
  raised there — run `lhp validate` for configuration diagnosis.
  `lhp validate` and `lhp generate` are unchanged.

### Performance

- **Inter-procedural resolution is budget-bounded.** The call-resolution
  engine does not memoize guard-degraded results; a Python helper whose
  resolution repeatedly trips cycle/depth guards therefore re-replayed
  environments combinatorially — observed at ~1.6 s per action on a real
  17,695-flowgroup project (an ~8-hour `lhp dag`). Past a guard-hit budget
  the engine now degrades every further answer straight to "not statically
  known" (the same conservative direction the guards already move in),
  bounding the pathological case to ~1 ms per action. Files that never
  approach the budget resolve exactly as before.
- **Cross-action Python extraction cache.** Extraction results are memoized
  per (body content, YAML parameter bindings): thousands of actions binding
  the same values to one shared helper now pay a single visitor walk instead
  of one per action.
- **`lhp dag --trust-depends-on` (opt-in).** An action with a non-empty
  `depends_on` skips SQL/Python body extraction entirely; its declared
  entries (plus any explicit `source:` config) become its authoritative
  source set. A fast path for large projects whose actions fully declare
  their upstreams.
- **The graph-build phase is perf-instrumented.** `lhp --perf dag` now logs
  `dependency_graph_build`, `action_graph_extraction`, and
  `dependency_analysis` phases, so the formerly silent expensive phase is
  visible in `.lhp/logs/perf.log`.
- **One analysis per `lhp dag` invocation.** `analyze_project` is memoized so a
  single command does one discovery + graph build + analysis, shared across
  the analysis, output serialization, and job-orchestration phases (previously
  each phase re-analyzed).
- **Per-run parse caches.** A helper `.py` module referenced by many actions is
  read and parsed once per run, and SQL bodies are deduplicated by content,
  so large projects no longer re-parse the same sources repeatedly.
- **libyaml C loader for YAML parsing.** YAML files are loaded with
  `yaml.CSafeLoader` when the installed PyYAML has libyaml bindings, with a
  pure-Python `SafeLoader` fallback otherwise. `lhp dag` on a 2,802-flowgroup
  project drops from 8.6s to ~4s.
- **Dependency analysis skips per-flowgroup config validation.** `lhp dag`/
  `lhp deps` no longer pay the configuration-validation cost on every
  flowgroup (see Changed, above).
- **Persistent on-disk parse cache makes warm discovery near-instant.** Repeat
  runs re-parse only changed files: warm discovery on 2,802 files drops from
  ~5.9s to well under a second, and a whole warm `lhp dag` runs in ~3s
  end-to-end (vs 8.6s originally). See Added for the cache location and the
  `--no-cache`/`LHP_NO_CACHE` switches.
- **Parallel cold discovery for large projects.** When more than one worker is
  configured and at least 500 files miss the parse cache, cold parsing fans
  out to a process pool (one worker per 250 miss files, capped by the resolved
  worker count). Small projects always stay serial, and a file that fails in a
  worker is re-parsed serially, so error behavior is identical to a serial
  run.

### Fixed

- **YAML schema validation in the web IDE was silently dead.**
  monaco-editor 0.53 changed its web-worker bootstrap, and the frozen
  `monaco-worker-manager` package that `monaco-yaml` still depends on never
  spawned the YAML language worker under it — so schema diagnostics
  (including for `lhp.yaml` and flowgroup files) were never produced for
  any YAML file. An in-repo worker-manager shim replicating the maintained
  monaco bootstrap, aliased over the frozen package at build time, restores
  schema validation in the editor.
- **SQL extraction no longer drops the explicit `source:` declaration.** When
  a SQL body parsed to real tables, the declared `source:` refs were
  discarded — breaking the documented premise that `$source`-placeholder
  edges are carried by the declaration. Both parse branches now union with
  the explicit refs.
- **No more phantom cross-catalog edges.** A 3-part `catalog.schema.table`
  read whose exact producer lookup missed could suffix-match a producer in a
  DIFFERENT explicit catalog; the fallback now reconciles only with
  empty-catalog (2-part) producers.
- **Parameter shadowing in Python extraction.** A function parameter now
  shadows same-named module/outer bindings even when its value is unknown;
  previously the outer value leaked through, fabricating table names and
  silently suppressing LHP-DEP-002 advisories.
- **Pipeline-scoped view matching is case/backtick-insensitive** (matching
  Spark's view-name resolution), so canonicalized `depends_on` entries and
  case-variant reads match view targets.
- **Kwonly parameter seeding models `functools.partial` correctly.** YAML
  parameters applied as keyword arguments now also seed positional-or-keyword
  parameters, not just keyword-only ones.
- **Advisory line numbers anchor to the real file.** Python bodies are no
  longer stripped of leading blank lines before parsing, so LHP-DEP-002/003
  `file:line` references match the on-disk source.
- **`lhp dag --pipeline` is exposed** (the analysis service already supported
  it), and the `job` output format is skipped with a warning when a
  pipeline/blueprint filter is active — orchestration job files are
  whole-project deployment artifacts and were previously written from the
  unfiltered flowgroup set, inconsistent with the other outputs.
- **Windows worker pools clamp to 61 processes.** `ProcessPoolExecutor`
  raises `ValueError` above 61 workers on Windows; auto-detected worker
  counts on high-core machines could crash generation and cold discovery.
- **Cyclic graphs no longer crash `get_execution_order`.**
  `nx.topological_generations` raises `NetworkXUnfeasible`, which is not a
  `NetworkXError` subclass and slipped past the guard.
- **Per-job analysis results carry pipeline stages**, and the cross-job
  dependency log is derived from the edges the job partition actually drops
  (it could never fire before).
- **DOT export escapes quotes/backslashes** in node names and labels.
- **`TemplateEngine` instances pickle with an empty compiled-template memo.**
  The compiled-template cache is per-process state; previously a warm cache
  made sandbox runs on template-using projects fail at the worker spawn
  boundary (sandbox-specific: only the sandbox pre-pass warms the cache on
  the main thread before the worker pool starts).
- **Substitution tokens survive SQL extraction byte-for-byte.** The regex
  parser silently truncated mid-segment tokens (`FROM cat.sch.tbl${suffix}`
  was extracted as `cat.sch.tbl`), so suffix-carrying producers could never
  match suffixed readers in `lhp dag`. The parser-based extraction also
  correctly excludes MERGE/INSERT/CTAS write targets and handles CTEs, quoted
  identifiers, multi-statement SQL, and string literals containing `FROM`;
  the `$source` placeholder no longer leaks into external sources.
- **Blueprint instances that parameterize `pipeline:` no longer lose pipelines
  from the dependency graph.** Dependency analysis previously deduplicated
  synthetic flowgroups by `(blueprint_name, spec_index)`, keeping one
  representative node per spec. When a blueprint set `pipeline:` per instance,
  every non-representative instance's pipeline silently vanished from the DAG,
  the JSON output, and the `--format job` orchestration YAML. Blueprint
  synthetic flowgroups are now always fully expanded (one node per instance),
  so per-instance pipelines are preserved everywhere.
- **Static Python resolution now spans function boundaries (RC1–RC4).** Reads
  the extractor previously reported as unresolvable (`LHP-DEP-002`) now
  resolve when statically knowable:
  - **RC1 — function parameters:** a parameter's value set is the union of the
    argument values across the function's call sites in the same file, plus
    any YAML-bound parameters and signature defaults.
  - **RC2 — return values:** a bare-name call to a user function in the same
    file resolves to the union of its `return` expressions (`return None` and
    bare `return` are skipped; any dynamic return poisons the result to
    "unknown").
  - **RC3 — boolean and builtin folds:** `a or b` / `a and b` fold to the union
    of the operand value sets (so `parameters or {}` resolves to the
    parameters dict), and the allowlisted collection builtins `list(x)`,
    `tuple(x)`, `sorted(x)`, `set(x)`, and `dict.fromkeys(x)` fold as identity
    on the element value set.
  - **RC4 — loop unrolling:** `for t in <statically-foldable iterable>` unrolls
    (including iteration over dict keys), emitting one read per element.

  Resolution is memoized, cycle-guarded (recursion resolves to "unknown",
  keeping the advisory), depth-capped, and value-set-capped. It is never
  speculative — anything genuinely dynamic (e.g. `os.environ`) still yields
  `LHP-DEP-002`.
- **Dependency-analysis outputs are now deterministic across runs and
  machines.** Dependency-graph edge insertion, per-node `external_sources`,
  `depends_on` lists, execution stages, and discovery file ordering are
  sorted, so the JSON (`pipeline_dependencies.json`), DOT, and text outputs
  are byte-identical from run to run (previously the ordering varied with
  Python's hash seed). Existing users will see a one-time ordering diff in
  regenerated dependency outputs; the `.job.yml` orchestration output was
  already sorted and is unchanged.

### Removed

- **Legacy regex SQL parser** (`lhp/utils/sql_parser.py`) — superseded by the
  sqlglot-based extraction in `core/dependencies/`.

### Dependencies

- Add `sqlglot>=26.0,<28` as a runtime dep (parser-based SQL table extraction
  for dependency analysis).

### Notes

- **Sample-project live-workspace validation is deferred.** Offline validation
  of the `--sample` project — init → validate → generate, plus an end-to-end
  test — is covered in CI. Deploying the bundle and running the sample job
  (rounds 1–2) on a real workspace is deliberately deferred to post-merge
  manual verification.

## [0.9.0] — 2026-06-10

Architecture refactor release. The internals were reorganized into a layered,
boundary-enforced architecture — a versioned public `lhp.api` facade, focused
`core/` sub-packages, and an event-stream CLI — without changing the YAML
configuration contract. Existing projects generate the same pipeline code.

### Added

- **Dependency analysis overhaul.** More accurate internal-vs-external edge
  classification, a new optional `depends_on` action field as an explicit
  dependency escape hatch (validated as `LHP-VAL-063`), and a shared dependency
  graph contract reused by `lhp deps` and orchestration-job generation.
- **`dag --format job`** now validates `job_name` consistency on the live path.
- **Write-target fields `cluster_by_auto` and `refresh_policy`** for streaming
  tables and materialized views.
- **Deterministic per-pipeline wheel packaging** — content-addressed wheels
  emitted alongside source-mode pipelines.
- **`lhp inspect-wheel <selector>` command** to list the `.py` modules inside a
  built pipeline wheel, or extract them with `--extract DIR` (preserving the
  in-wheel structure). The selector is either a path to a `.whl` or a pipeline
  name; the pipeline form requires `--env`/`-e`. New error codes `LHP-IO-022`
  (wheel file not found), `LHP-IO-023` (not a wheel file), and `LHP-IO-024`
  (corrupt wheel archive) cover the failure modes.
- **Provisional `WheelFacade` public API** — reachable as `facade.wheel` with
  `list_modules(...)` / `extract_modules(...)`, returning the new provisional
  DTOs `WheelContentsView` and `WheelModuleView` (`api/views.py`) and
  `WheelExtractionResult` (`api/responses.py`). All are `:stability: provisional`.
- **Public typed API.** A versioned `lhp.api` surface with stability annotations
  on every public symbol, shipped with `py.typed`.
- **Opt-in `--log-file` flag** to persist verbose logs without cluttering the
  console.
- **`$` is now allowed** in schema-transform source column names.

### Changed

- **Tooling consolidated onto `ruff`** as the single linter, formatter, and
  import-sorter (replaces black + isort + flake8).
- **CLI rebuilt as a pure presentation layer** over a single generate/validate
  event-stream facade, with animated progress, lazy startup, and a readable
  run summary.
- **`core/` reorganized into focused sub-packages** (`codegen`, `coordination`,
  `loaders`, `processing`, `discovery`, `jobs`, `registry`, `dependencies`,
  `packaging`, `validators`); the orchestrator, loaders, models, and validators
  were decomposed and domain errors moved into a dedicated error-code registry.
- Documentation rewritten (Sphinx RST) with condensed per-action-sub-type AI
  skill references.
- **Bundle wheel-locator deduplicated.** `bundle/manager._find_wheel_filename`
  now delegates to the shared `core/packaging/wheel_reader.locate_pipeline_wheel`,
  removing the duplicate locate logic. The `TARGET_ARCHITECTURE.md` §5bis budget
  is amended accordingly (`wheel_reader.py ≤130`; `core/packaging/` total raised
  ≤650 → ≤700).

### Performance

- **`lhp generate` is substantially faster on large projects** — generation
  hot-path optimization, read-once / parse-once discovery, a single shared
  preflight for `validate` and `generate`, and a consolidated parallel-execution
  engine.
- **Cross-platform determinism** — transitive Python-file copying emits
  byte-identical output on every OS.

### Fixed

- Malformed-secret references are now rejected; referential-integrity tests
  enforce a column-count check; operational-metadata ordering is deterministic.
- Blueprint instance-reference rejection, monitoring-cleanup marker hardening,
  and a bundle read-only error invariant.
- `job_name` dependency propagation for orchestration jobs.

## [0.8.8] — 2026-05-22

This release is a CLI rendering overhaul: `lhp generate`, `lhp validate`,
and `lhp show` now drive a Rich `Live` panel during execution and emit
a per-pipeline summary table on exit, errors render as category-coloured
Rich panels, and warnings are surfaced through a per-run collector
instead of a process-wide flag. There are no breaking changes to YAML
schemas, generated code, or exit codes.

### Added

- **Rich-based CLI rendering.** `lhp generate`, `lhp validate`, and
  `lhp show` now run inside a Rich `Live` panel that opens with a
  `Discovering flowgroups…` spinner, switches to per-phase duration
  markers (✓ / ✗) and an in-flight progress spinner as work proceeds,
  and prints a per-pipeline summary table on exit. Coloring honors
  `NO_COLOR` and falls back to plain text when stdout is not a TTY.
  Shared rendering primitives live in `src/lhp/cli/live_panel.py`,
  with command-specific summary tables in `generate_summary.py` and
  `validate_summary.py`.
- **`--show-all` / `-a` flag on `lhp generate` and `lhp validate`.**
  The post-run summary table defaults to failed pipelines only; pass
  `--show-all` to render the full table including passing rows. On a
  full-success run with no failures, the table is suppressed entirely
  and a single-line footer is printed (with a `(use -a to list)` hint
  when more than one pipeline ran). This keeps quiet runs short
  without losing the table for users who want it.
- **`rich-click` for CLI help rendering.** `click` is imported as
  `rich_click as click`; existing decorators are unchanged but
  `--help` output now renders as a styled Rich panel.
- **`LHPError.__rich__` + `LHPError.from_unexpected_exception`.** Errors
  are rendered to stderr as a category-coloured Rich `Panel` with
  Context / Suggestions / Example / docs-link sections; the plain-text
  `__str__` rendering is preserved verbatim for log files and non-TTY
  output. Unexpected exceptions are now converted into a generic
  `LHPError` via `from_unexpected_exception` instead of being matched
  by sniffing for `"Error [LHP-"` substrings in the stringified message.
- **`WarningCollector` + pre-pool YAML scanner.** A per-run
  `WarningCollector` (`src/lhp/cli/warning_collector.py`) deduplicates
  non-fatal warnings by `(category, message)` and renders them as a
  yellow Rich panel at end of run. The deprecated bare-`{token}`
  substitution warning is now detected by a main-thread scan of raw
  YAML (`src/lhp/cli/yaml_scanner.py`) before workers are spawned —
  this fixes a silent regression where worker-side `logger.warning`
  calls were swallowed by the workers' `NullHandler`-only logger.
- **`EnhancedSubstitutionManager.has_deprecated_bare_tokens`.**
  Per-instance flag flipped on the first deprecated `{token}`
  substitution so the orchestrator can forward a single entry to the
  `WarningCollector`. Replaces the previous module-level
  `_DEPRECATED_BARE_TOKEN_WARNED` flag.
- **Snapshot tests via `syrupy`.** Rich rendering for `LHPError`
  panels, the validate summary table, and `show` flowgroup output is
  pinned by `.ambr` snapshots under `tests/__snapshots__/`. An autouse
  `_isolate_lhp_console` fixture in `tests/conftest.py` swaps the
  `lhp.cli.console` singletons for a deterministic no-color Console at
  fixed width so snapshots are stable across terminals and CI.

### Changed

- **`cli_error_boundary` renders via Rich Console.** Errors print
  through `err_console.print(lhp_error)` so the new `__rich__` panel
  takes effect on TTYs; the stringified-LHPError sniffing safety net
  is replaced by `LHPError.from_unexpected_exception`.
- **`error_formatter.py` reshaped around a single template dict.**
  Plain-text (`_format_message`) and Rich (`__rich__`) renderings now
  consume the same `_template_data()` dict so the two presentations
  cannot drift. Category labels and Rich border styles are centralized
  in `_category_label` / `_border_style`.
- **`src/lhp/cli/main.py` slimmed.** Unused helpers
  (`_ensure_project_root`, `_load_project_config`,
  `_get_include_patterns`, `_discover_yaml_files_with_include`) and a
  stray `warnings.filterwarnings` call were removed — the Click
  subcommands own these concerns directly.

### Removed

- **Module-level `_DEPRECATED_BARE_TOKEN_WARNED` global** in
  `lhp/utils/substitution.py` (replaced by per-instance
  `has_deprecated_bare_tokens` + `WarningCollector`).
- **`DOLLAR_TOKEN_SIMPLE_PATTERN`** in `EnhancedSubstitutionManager` —
  was unused.

### Dependencies

- Add `rich>=13.0.0` and `rich-click>=1.9.0,<2.0` as runtime deps.
- Add `syrupy>=4.6.0` as a dev dep for snapshot testing.

## [0.8.7] — 2026-05-21

### Breaking changes

- **`lhp generate` now requires `--pipeline-config` / `-pc`** when bundle
  support is enabled (`databricks.yml` present, `--no-bundle` not set).
  Previously, generation would proceed and fail late during bundle sync.
  New error code: `LHP-CFG-023`. Users who relied on a default-empty
  pipeline_config must now supply a path explicitly. See
  `docs/configure_catalog_schema.rst`.
- **`resources/lhp/` is now wiped and regenerated on every
  `lhp generate`.** The directory is exclusively LHP-managed. Users
  who placed custom resource YAMLs (hand-written jobs, dashboards,
  secret scopes) under `resources/lhp/` **must move them to
  `resources/` (top level) or a non-`lhp` subdirectory before
  upgrading**, or those files will be deleted on the next generate.
  Files outside `resources/lhp/` are never touched, with one
  exception: the monitoring job YAML at `resources/<name>.job.yml`
  is identified by its sentinel header
  (`# Generated by LakehousePlumber - Monitoring Job`) and replaced
  on each run.
- **`catalog` and `schema` are now REQUIRED in
  `pipeline_config.yaml`** — set them per-pipeline or via the
  top-level `project_defaults` block. Missing or incomplete
  catalog/schema causes `lhp generate` to fail fast with
  `LHPConfigError` (`LHP-CFG-026`), aggregated across all pipelines
  plus the synthetic monitoring pipeline (when monitoring is
  enabled). Programmatic consumers read
  `LHPConfigError.context["failures"]` (grouped by `both_missing`,
  `incomplete`, `empty_after_substitution`) instead of parsing
  message text. Previously LHP auto-populated
  `default_pipeline_catalog` / `default_pipeline_schema` in
  `databricks.yml` and the pipeline-resource template fell back to
  `${var.default_pipeline_*}`. Both halves of that pathway are
  removed. See `docs/configure_catalog_schema.rst` for the migration
  guide.
- **Smart generation removed.** Every `lhp generate` now
  full-regenerates all flowgroups (equivalent to the old
  `--force --no-state` behaviour); there is no incremental mode. The
  `lhp state` subcommand, the `--no-cleanup` flag, and the
  `.lhp_state.json` / `.lhp_state/` files are all removed. Any
  leftover state files are auto-cleaned on the first run of this
  version.
- **`PythonFileCopier.apply_copy_record` no longer mutates state.** It
  returns a list of `CopiedFileEntry` records and the caller invokes
  `track_generated_file` separately. In-tree callers route through
  `PipelineProcessor._apply_copy_records` automatically.

### Added

- **Pre-flight catalog/schema validation** (`lhp.bundle.preflight`):
  every pipeline (plus the synthetic monitoring pipeline if
  monitoring is enabled) is validated for `catalog` and `schema`
  resolution BEFORE any side effects. Failures are aggregated across
  all pipelines and grouped by failure type
  (`both_missing` / `incomplete` / `empty_after_substitution`),
  surfaced via `LHP-CFG-026` with structured `context["failures"]`.
  Preflight runs identically under `--dry-run`. Typical fail-fast
  time is under 1 second with zero filesystem changes — the most
  common cause of an empty `resources/lhp/` (catalog/schema
  misconfig) can no longer happen.
- **Per-pipeline parallel generation.** One worker process per
  pipeline via `ProcessPoolExecutor` (spawn start method). Phase A
  (parse, codegen, format) and the intra-pipeline portion of Phase B
  (cross-flowgroup validation, `.py` writes, copied-module
  application, test-reporting hook) all run inside the worker —
  eliminating the main-thread GIL-starvation hot path that
  bottlenecked large projects.
- **Worker count auto-detection** at ~80% of OS-visible CPU, capped
  to the workload size. Override with the `LHP_MAX_WORKERS` env var
  or the `--max-workers` CLI flag on `lhp generate` / `lhp validate`.
  Use `--max-workers 1` for sequential execution.
- **`LHPError.from_worker_exception` + `lhp_error_from_worker_failure`
  factories** — reconstruct worker-side exceptions on the main
  thread while preserving the original exception type via
  dual-inheritance subclasses (`LHPValidationError(LHPError,
  ValueError)`, `LHPFileError(LHPError, FileNotFoundError)`).
  Existing `except ValueError` / `except FileNotFoundError` handlers
  continue to catch worker failures.
- **`docs/configure_catalog_schema.rst`** — migration guide for the
  new mandatory catalog/schema rule, with worked examples for
  per-pipeline config, `project_defaults`, resolution order, the
  migration from the removed `databricks.yml` variables, and an
  error reference covering `LHP-CFG-023` and `LHP-CFG-026`.
- **Release-time performance gate** (`pytest tests/performance/ -m
  performance`) — opt-in via `LHP_RUN_PERFORMANCE_GATE=1`. Captures
  baselines per release; excluded from default CI to keep PR
  pipelines fast. Comprehensive unit tests for `PerformanceTimer`
  and its snapshot accessor.
- **Picklable test fakes** (`tests/fakes/`,
  `tests/test_fakes_picklable.py`) for worker-boundary tests that
  need to cross the spawn boundary.
- **E2E coverage expansion**: state CLI semantics, test actions,
  temp-table transformations, JDBC load, Python load, built-in
  sinks, Delta CDC reader, generate flags, negative paths.

### Changed

- Catalog/schema validation moved from the bundle-write phase to a
  dedicated preflight stage. Late-bound validation inside
  `BundleManager.generate_resource_file_content` is replaced by a
  single internal-error guard (`LHP-GEN-001`) that fires only when
  preflight is bypassed by a non-CLI caller (programming bug). The
  three separate raises that this method used to emit are gone.
- `LHP-CFG-026` is now the **aggregated** form: a single error with
  all failures grouped by category. Programmatic consumers should
  read `LHPConfigError.context["failures"]` instead of parsing
  message text.
- `PipelineDelta` moved from `lhp.core.state_models` to
  `lhp.models.processing` (lost its `files_skipped` field).
- `build_lhp_source_header` moved from `lhp.utils.smart_file_writer`
  to `lhp.utils.file_header`; adds a `normalize_content` helper used
  by every bundle/pipeline write site.
- `ChecksumCache` no longer holds a `threading.Lock` (main-thread-only
  usage now that workers are process-isolated).
- The blueprint provenance map
  (`Dict[Tuple[str, str], BlueprintProvenance]`) now crosses the
  spawn boundary into workers; synthetic-flowgroup detection and
  blueprint-aware dependency resolution work the same in the worker
  as on the main thread.

### Deprecated

- `--force` and `--no-state` flags on `lhp generate`: accepted with
  a deprecation warning; will be removed in a future release. Both
  are no-ops since full regeneration is the default. Remove them
  from CI invocations.

### Fixed

- Generating without `--pipeline-config` no longer wipes
  `resources/lhp/` and `generated/<env>/` before failing. A
  subsequent `databricks bundle deploy` will no longer find an empty
  `resources/lhp/` from a misconfigured run.
- Init template (`pipeline_config_env.yaml.tmpl`) no longer claims
  LHP auto-loads `templates/bundle/pipeline_config.yaml` — that
  pathway was removed in this release. The comment now correctly
  documents the `--pipeline-config` requirement.

### Removed

- **Smart-generation subsystem entirely**: `lhp state` CLI
  subcommand, `--no-cleanup` flag, `.lhp_state.json` / `.lhp_state/`
  files (auto-deleted on first run of new version), and the modules
  `lhp.core.state_manager`,
  `lhp.core.state_dependency_resolver`, `lhp.core.state.*`,
  `lhp.utils.smart_file_writer`, `lhp.services.state_display_*`,
  `lhp.core.strategies`,
  `lhp.core.services.generation_planning_service`,
  `lhp.cli.commands.state_command`.
- `DatabricksYAMLManager` module (and the runtime dependency on
  ruamel.yaml that it was the sole consumer of, where applicable).
- `BundleManager._update_databricks_variables` method, plus the
  auto-population of `default_pipeline_catalog` /
  `default_pipeline_schema` in `databricks.yml`
  `targets.<env>.variables`.
- Bundle-resource preservation decision tree (Scenarios 1a / 1b /
  2 / 4 in `BundleManager._sync_pipeline_resource` — the
  Conservative Approach for LHP-vs-user file preservation); orphan
  cleanup of `resources/lhp/`; LHP-vs-user header sentinel inside
  `resources/lhp/`.
- Deprecated `${var.default_pipeline_catalog}` /
  `${var.default_pipeline_schema}` fallback in
  `pipeline_resource.yml.j2`.
- `variables:` block in the init template's `databricks.yml.j2`
  (`default_pipeline_catalog` / `default_pipeline_schema` were the
  only entries).
- `ActionOrchestrator._sync_bundle_resources` (orphan method with no
  production caller) and its 8 test references.
- Orchestrator main-thread machinery: `_PipelineProgress`,
  `_assemble_pipeline`, `_assemble_pipeline_outputs` (worker-internal
  now).
- `DependencyTracker.set_checksum_cache` (the cache is now provided
  at factory-construction time via
  `DependencyTracker.for_project(..., checksum_cache=...)`).
- Outdated benchmark scripts and results (replaced by the
  structured release-time performance gate above).

### Known limitation

- `lhp generate` is **non-atomic** between the `resources/lhp/` wipe
  and the end of bundle sync: if the process is interrupted (OS
  kill, power loss) **after** preflight passes but **before** bundle
  sync completes, `resources/lhp/` may be left partially populated
  or empty. A subsequent `databricks bundle deploy` against that
  state could silently drop pipelines from the workspace.
  Catalog/schema misconfigs — the most common trigger in
  pre-preflight versions — can no longer cause this; preflight
  rejects them before any wipe. Re-running `lhp generate` to
  completion restores the directory.

## [0.8.6] — 2026-05-11

### Changed

- **`custom_datasource` and `custom_sink` generated output format** changed from
  *inline-embed* to *copy-and-import*. The user's PySpark `DataSource` /
  `DataSink` source file is now copied verbatim into a
  `custom_python_functions/` subdirectory beside the generated pipeline file,
  and the pipeline imports the class by name. Previously the user's class body
  (50–250 lines) was inlined into every generated file that registered a
  custom source/sink. **YAML user-facing surface is unchanged.** The new
  generated file is shorter, more diff-able, and consistent with the existing
  python-transform action pattern.

### Added

- **Cloudpickle registration for custom sources/sinks**: generated files now
  emit `_lhp_cloudpickle.register_pickle_by_value(custom_python_functions)`
  between the imports block and `PIPELINE_ID`. This is the one-line fix that
  makes the import-based pattern work across the local-Spark / executor
  boundary — PySpark's *vendored* cloudpickle is what serializes registered
  DataSource classes to executors, and only `register_pickle_by_value`
  against the vendored copy actually takes effect.

- **Import name-collision detection in `ImportManager.add_import`**: two
  `from … import …` lines that bind the same local name to *different*
  modules now raise `LHPValidationError` (LHP-VAL-021). This catches a class
  of silent shadowing bugs that affected `python` load/transform actions and
  (post-refactor) the new copy-and-import pattern. Existing projects with
  legitimately conflicting symbol names will see this as an error on the
  next regenerate; rename one of the conflicting symbols, or alias one of
  the imports, to resolve.

## [0.8.5] — 2026-04-24

### Added

- Job-config **pass-through**: any top-level key in `job_config.yaml` that is
  not one of LHP's explicitly handled keys (`max_concurrent_runs`, `queue`,
  `performance_target`, `timeout_seconds`, `tags`, `email_notifications`,
  `webhook_notifications`, `permissions`, `schedule`, `notebook_cluster`) is
  now rendered verbatim into the generated job YAML. Users can use
  newly-released Databricks Jobs API fields (trigger types, `continuous`,
  `run_as`, `git_source`, `health`, `parameters`, `environments`,
  `edit_mode`, `budget_policy_id`, …) without waiting for an LHP release
  that adds explicit support.

### Changed

- **Python dependency extraction tracks local-variable bindings**: patterns
  like `tbl = "cat.sch.t"; spark.read.table(tbl)` are now resolved.
  Reassignments and conditional branches emit the union of possible values.
  Variables whose value comes from function parameters, function return
  values, or string concatenation remain unresolvable — for those cases,
  declare an explicit `source:` on the action; for Python actions, parser
  output and explicit `source:` are unioned. Implemented as a new
  `_TableExtractor(ast.NodeVisitor)` with `_Scope`/`_Binding` classes in
  `utils/python_parser.py`, replacing the prior `ast.walk` over `ast.Call`
  that logged `ast.Name` arguments as unresolvable.
- **Jinja2 templates loaded via `PackageLoader`** instead of file-system
  loaders. Templates ship as package resources and are discovered through
  `importlib.resources`, removing the editable-install dependency on a
  source tree shadow. Applied in `utils/template_renderer.py`,
  `core/init_template_loader.py`, and `generators/base_generator.py`;
  `job_generator.py` retains `FileSystemLoader` for user-provided bundle
  template directories, which are not package resources.

### Fixed

- `lhp deps` now honors `trigger.file_arrival` (and any other Databricks
  Jobs API field) in `job_config.yaml`. Previously, keys the Jinja template
  didn't explicitly handle were silently dropped from the generated
  orchestration job YAML.
- **`lhp deps` extracts source tables from externalized write-target code**:
  materialized views and custom sinks using `write_target.sql_path`,
  `write_target.sql`, `write_target.module_path`, or
  `write_target.batch_handler` previously appeared to have no upstream
  dependencies, causing gold pipelines and silver-MV flowgroups to be
  reported as root nodes in the dependency graph. New
  `_iter_sql_bodies` / `_iter_python_bodies` generators in
  `core/services/dependency_analyzer.py` walk these externalized bodies
  the same way they walk inline SQL/Python.

## [0.8.4] — 2026-04-22

### Fixed

- **Missing `.j2` files in published wheels**: the `lhp.templates.monitoring`
  package was declared in `pyproject.toml` but its `*.j2` template files were
  not included by the wheel's `package-data` glob. As a result,
  `lhp generate` against a project that exercised monitoring would fail with
  `TemplateNotFound: monitoring/union_event_logs.py.j2` on installs from PyPI
  (editable installs from a checkout were unaffected because the source tree
  shadowed the missing package data). The package-data inclusion list now
  covers monitoring templates so wheel and editable installs render the same
  output.

## [0.8.3] — 2026-04-21

### Fixed

- **Cold-run race in the monitoring event-log union notebook**: the generated
  notebook ran *N* parallel streaming queries that each called
  `.toTable(TARGET_TABLE)`, so on a cold target table all *N* threads raced to
  `CREATE` the table — one thread won and the remaining *N−1* failed with
  `TABLE_OR_VIEW_ALREADY_EXISTS`. The notebook now pre-creates the target
  through an idempotent `_ensure_target_exists()` prologue that samples the
  schema from the first readable source event log before the
  `ThreadPoolExecutor` block starts. Existing projects must regenerate their
  monitoring notebook to pick up the prologue.

### Changed

- **Dedicated `monitoring.job_config_path` replaces the `__eventlog_monitoring`
  alias**: the monitoring workflow job is now configured from a separate,
  single-document `job_config.yaml` that describes only the monitoring
  workflow, rather than from a special-cased alias inside the shared
  `job_config.yaml`. `ProjectConfigLoader` validates that
  `monitoring.job_config_path` is set and that the file exists (tokenized
  paths are deferred to the orchestrator); `JobGenerator.generate_monitoring_job`
  now receives a resolved `job_config` dict from its caller. Users on 0.8.2
  with monitoring enabled should add `monitoring.job_config_path` to
  `lhp.yaml` and move the `__eventlog_monitoring` block from the shared
  `job_config.yaml` into the file it points at.

## [0.8.2] — 2026-04-17

### Added

- **Multi-CDC fan-in to one streaming table** (closes #113): multiple write
  actions in `mode: cdc` that share a `catalog.schema.table` target now
  combine into one `dp.create_streaming_table()` plus *N*
  `dp.create_auto_cdc_flow()` calls, each with its own `name=` and its own
  per-flow CDC parameters (`ignore_null_updates`, `apply_as_deletes`,
  `apply_as_truncates`, `column_list`, `except_column_list`). This is the
  CDC counterpart to the standard-mode append-flow fan-in LHP already
  supported. A new `CdcFanInCompatibilityValidator` raises `LHPConfigError`
  when shared fields (keys, `sequence_by`, SCD type, `track_history_*`,
  `partition_columns`, `table_properties`) disagree between fan-in
  participants, rejects mode-mixing on the same target, and rejects
  `source: [v1, v2] + mode: cdc` with guidance to split into one write
  action per source (this combination silently caused truncations on prior
  versions).
- **Per-pipeline event-log monitoring checkpoints** (fixes #96): replaces the
  single-query `UNION ALL` streaming flowgroup with a pair of artifacts whose
  checkpoints survive adding or removing monitored pipelines. A notebook
  (`monitoring/{env}/union_event_logs.py`) runs *N* independent streaming
  queries under a `ThreadPoolExecutor` with `trigger(availableNow=True)`, each
  with its own checkpoint at `{checkpoint_path}/{pipeline_name}`; a separate
  MV-only DLT flowgroup reads the populated Delta table; a Databricks
  workflow chains the two via `notebook_task` → `pipeline_task`. New required
  setting: `monitoring.checkpoint_path`. New tunables: `max_concurrent_streams`
  (default 10) and `enable_job_monitoring`.
- **`test` action type in the JSON schema**: `flowgroup.schema.json` now lists
  `"test"` in the action `type` enum (previously accepted only via runtime
  parsing).

### Changed

- **Env-scoped generation context replaces per-file composite checksums**: the
  per-file `file_composite_checksum` and `generation_context` on `FileState`
  are replaced by a single `last_generation_context` on `ProjectState`. The
  `--include-tests` flip detection is now a single env-wide comparison instead
  of *O(N)* per-file checksum recomputations. A new `StalenessCache` lets the
  display phase and per-pipeline filter share a single env-wide staleness
  scan per run, and `include_tests` is forwarded to orphan cleanup so
  `test_reporting_*` artifacts are reaped when the flag flips to `False`.
- **`generate` and `validate` now share one flowgroup discovery pass** rather
  than re-scanning at each phase; `FlowgroupDiscoverer` caches include
  patterns and the source-path index across calls within a run.
- **`lhp validate --include-tests` has parity with `lhp generate`**, filtering
  test actions out of validation when the flag is absent.
- **Hardened state persistence**: malformed or legacy state files now raise
  `LHPFileError` with actionable guidance instead of silently resetting.

### Deprecated

- **`{token}` substitution syntax**: docs, init templates, generator
  templates, and source comments are migrated to `${token}`. The deprecation
  is surfaced through `logger.warning` (rather than `DeprecationWarning`) so
  it reaches end users via normal CLI output. Migrate any `{token}` usages
  in YAML to `${token}`; the only legitimate non-`$` braces syntax remaining
  is `%{local_var}` for local variables.

## [0.8.1] — 2026-04-14

### Added

- **External test result reporting**: new `test_reporting` block in `lhp.yaml`
  generates a per-pipeline `_test_reporting_hook.py` event hook. The hook
  uses `@dp.on_event_hook` to accumulate DQ expectation results from
  `flow_progress` events and publishes them at pipeline terminal state via a
  user-supplied provider module declared by `module_path` and
  `function_name`. Actions gain an optional `test_id` field for linkage to
  external test management systems. Generated hooks, provider module copies,
  and `__init__.py` are tracked as pipeline artifacts and cleaned up when
  `test_reporting` is removed from `lhp.yaml`. `${token}` substitution is
  applied to provider module copies so secret/config tokens resolve at
  generate time.
- **Three built-in test-reporting providers**: `delta_test_reporter.py`
  (appends results to a pre-existing Delta table), `ado_test_reporter.py`
  (publishes to ADO Test Plans via a `test_case_mapping` config), and
  `ado_test_reporter_inline.py` (publishes to ADO where `test_id` is itself
  the ADO Test Case ID). Each implements the
  `publish_results(results, config, context, spark)` contract with
  `dry_run` support and structured logging.
- **`--include-tests` flag on `lhp validate`** for test-reporting validation
  parity with `lhp generate`.

## [0.8.0] — 2026-04-12

### Added

- **`source_function` parameters for snapshot CDC**: declare keyword arguments
  in `source_function` and have them bound via `functools.partial` at
  generation time. Makes snapshot functions reusable and testable outside LHP
  without baking substitution tokens into the function body. AST validation
  enforces keyword-only args (`*` separator) and rejects unknown parameter
  names.
- **`PerformanceTimer` utility** for structured timing instrumentation of
  generation phases.
- **Performance testing project** (`Example_Projects/performance_testing/`):
  synthetic 4000-flowgroup project across 100 per-domain pipelines (20
  domains × 5 layers) for realistic stress testing of discovery, staleness
  analysis, and generation.

### Changed

- **Large-project generation is ~8× faster**: the `find_source_yaml` *O(N×F)*
  bottleneck is replaced by a lazy source-path index on
  `FlowgroupDiscoverer`, reducing roughly 4M filesystem operations to a
  single-pass *O(1)* lookup — 500 s → 64 s for a 2000-flowgroup project. A
  per-run `ChecksumCache` with thread-safe locking ensures each file is read
  and hashed at most once during parallel generation; redundant discovery,
  hashing, and staleness analysis across phases of `generate` are
  eliminated; a single `CodeFormatter` instance is reused across worker
  threads (no more repeated `pyproject.toml` reads); and `--force` now wipes
  the output directory directly instead of running orphan detection over a
  pre-built `active_flowgroups` set.

### Fixed

- **CloudFiles `source.schema` now applies via `readStream.schema()` before
  `.load()`** instead of `df.schema()` after the load (#98). The previous
  ordering left Auto Loader unable to honor user-supplied schemas in some
  configurations.
- **CloudFiles path/file exclusion via `pathGlobFilter` and metadata-based
  filtering** (#87): Auto Loader pipelines can now exclude directories or
  individual files.

## [0.7.8] — 2026-04-08

### Added

- **`catalog`/`schema` namespace format** replaces the legacy
  `database: "catalog.schema"` field across load sources and write targets.
  YAML can now declare `catalog: my_cat` and `schema: my_schema` as separate
  keys. A new `namespace_normalizer` service transparently converts the old
  `database` shorthand and emits a deprecation warning, so existing projects
  keep generating identical output. Closes #100.
- **`source.schema` enforcement for CloudFiles loads**: the load action's
  `source.schema` is now applied on the `DataStreamReader` chain before
  `.load()` for Auto Loader sources, replacing the previously invalid
  post-load property access that silently dropped the user-supplied schema.
  New `source_schema_load` E2E fixture exercises the path end-to-end.
- **Mandatory pipeline configuration**: every pipeline must now have an
  explicit `pipeline_config.yaml`. The implicit fallback that derived
  catalog/schema from project-level defaults is deprecated and will be
  removed. Migration: add a `pipeline_config.yaml` (or per-environment
  override) for any pipeline that previously relied on the default lookup.
- **Supply chain security hardening**: all GitHub Actions are pinned to
  immutable SHA hashes; new workflows add `pip-audit` (SARIF), `bandit`
  (SARIF), `gitleaks`, `liccheck` license compliance, OpenSSF Scorecard,
  SLSA provenance generation, and reproducible builds via
  `SOURCE_DATE_EPOCH`. Adds `.pre-commit-config.yaml`, `CODEOWNERS`,
  `SECURITY.md`, Dependabot config, and pinned dev dependencies.

### Changed

- **Deterministic `depends_on` ordering** in generated orchestration job
  YAML. `job_generator` now sorts the `depends_on` list so generated job
  files are byte-identical across runs, eliminating spurious diffs when
  regenerating bundles.
- **Data quality templates use `withColumns`** (plural) in a single dict
  call instead of looped `withColumn` calls. Generated code is shorter
  and runs one projection per stage. Fixes #92.

### Fixed

- **CloudFiles description fallback** now formats the file format string
  correctly. Previously the fallback emitted the literal string
  `<built-in function format>` because the generator referenced Python's
  built-in `format()` instead of the `file_format` variable.
- **CloudFiles template dead batch branch** removed — the template had an
  unreachable batch read path that has been cleaned up alongside the
  schema-placement fix. Closes #98.

## [0.7.7] — 2026-03-17

### Added

- **Quarantine mode for data quality transforms**: new `mode: quarantine`
  on data-quality transform actions routes failed rows to a Dead Letter
  Queue table via `foreach_batch_sink` + `append_flow`, with automatic
  recycling of fixed records through Change Data Feed. Supports both
  CloudFiles and non-CloudFiles sources. Backed by a new `QuarantineConfig`
  model, schema and validator updates, a new `data_quality_quarantine.py.j2`
  template, and `get_all_expectations_as_drop()` in the DQE parser to
  coerce expectations into the quarantine pattern.
- **Delta load options validation**: the Delta load generator now
  validates the `options` block against the supported flag set and rejects
  incompatible combinations (for example, mixing `readChangeFeed` with
  options that don't apply to CDF reads). Documentation in
  `docs/actions/load_actions.rst` lists the supported options and their
  constraints.

### Fixed

- **Quote escaping in quarantine template**: applied the `tojson` Jinja2
  filter to the four remaining interpolation sites in
  `data_quality_quarantine.py.j2` (`inverse_filter`, `failed_rule_data`,
  and two related sites) to prevent `SyntaxError` when an expectation
  rule contains double quotes (e.g. `status = "active"`). The
  `_EXPECTATIONS` dict already used `tojson`; this brings the rest of the
  template in line.

## [0.7.6] — 2026-03-06

### Added

- **Declarative event log configuration**: new `event_log:` section in
  `lhp.yaml` with `catalog`, `schema`, and `name_suffix` (LHP token
  substitution supported). LHP injects an `event_log` block into every
  generated pipeline bundle resource automatically. Pipelines can override
  the project-wide setting (`event_log: {custom}`) or opt out
  (`event_log: false`) in `pipeline_config.yaml`. Closes #82.
- **Synthetic monitoring pipeline generation**: new `monitoring:` section
  in `lhp.yaml` generates a self-contained DLT pipeline that UNIONs every
  event log table in the project into a single streaming table and emits a
  default `pipeline_run_summary` materialized view (status, duration, and
  row metrics per update). Knobs: `pipeline_name`, `catalog`, `schema`,
  `streaming_table`, and `materialized_views` for custom MV definitions.
  The `__eventlog_monitoring` alias is recognised as a reserved pipeline
  target in `pipeline_config.yaml`.
- **`enable_job_monitoring: true`**: when set under `monitoring:`, LHP
  generates an additional `jobs_stats` materialized view via a Python load
  action that uses the Databricks SDK to correlate pipeline updates with
  the triggering job runs and enriches the output with both pipeline and
  job tags. The `jobs_stats_loader.py` is shipped as a package resource
  under `src/lhp/templates/monitoring/` and loaded via
  `importlib.resources`.
- **Instance pool support in pipeline clusters**: `instance_pool_id` and
  `driver_instance_pool_id` are now accepted in `pipeline_config.yaml`
  cluster blocks as alternatives to `node_type_id`. The Jinja2 template
  conditionally renders the pool fields and omits `node_type_id` when a
  pool is configured. Closes #83.
- **`sql_path` on materialized view write targets** and `batch_handler` /
  `foreachbatch` sink_type are now recognised by the Pydantic
  `WriteTarget` model, both JSON schemas, the MV field allowlist, and the
  MV validator. `custom_datasource` is also accepted as a load source
  type. Closes #85.
- **Self-contained materialized views** no longer require a load action.
  The validator and dependency resolver now exempt MV-only flowgroups
  that use `sql`, `sql_path`, or a CTE-only definition, so a gold MV can
  read directly from upstream tables without an explicit load.

### Changed

- **Documentation overhaul**: switched the Sphinx theme to Furo with dark
  mode and a custom OG image, added SEO `meta` descriptions to every
  page, reorganized the landing page (problem statement → value
  proposition → trimmed example → grouped features), and split the
  monolithic `actions_reference.rst` into per-type pages under
  `docs/actions/`. Extracted new standalone guides for substitutions,
  operational metadata, and dynamic templates. Toctrees are grouped into
  Getting Started, Configuration Guides, Deployment & Operations, and
  Reference. Added "Best Practices" sections. Closes #27.
- **Monitoring docs** now document `jobs_stats` as a materialized view
  (it was previously mis-typed as a streaming table) and include full
  schema tables for `events_summary` (16 columns) and `jobs_stats` (11
  columns).

### Fixed

- **Materialized view & write target validation stack (13 bugs)**: synced
  the validators, Pydantic models, and JSON schemas with what the
  generators and docs already supported. Removes the undocumented `name`
  alias for `table` across validator and generators, removes the invalid
  `path` field from the delta source allowlist, removes a dead
  `transform_fields` dict and unreachable dict-source handling from write
  generators, and fixes an orphaned transform to append (not raise) so
  accumulated errors like "must have at least one Load action" are
  preserved. Fixes the malformed
  `Reference_Templates/standard_ingestion.yaml`.

## [0.7.5] — 2026-02-17

### Added

- **Pipeline-level configuration entries** in `pipeline_config.yaml`:
  arbitrary Spark/DLT key-value pairs declared under a `configuration:`
  block are now rendered alongside the mandatory `bundle.sourcePath` in
  generated bundle resource YAML. Validation enforces a dict of
  string-only values, and `bundle.sourcePath` is filtered out to prevent
  duplication.
- **Pipeline environment dependencies propagation**: the `environment`
  section in `pipeline_config.yaml` is now rendered into generated bundle
  resource YAML, enabling pip package dependencies for DLT pipelines.
  Closes #74.
- **`lhp deps` refactor**: consolidated three duplicate source extraction
  implementations into a shared `source_extractor` module
  (`extract_action_sources`, `is_cdc_write_action`,
  `extract_cdc_sources`); replaced per-job re-analysis with NetworkX
  graph partitioning that filters the global graph by job membership;
  switched from `nx.find_cycle` (one cycle) to `nx.simple_cycles` (all
  cycles, capped at 20); and added a circular-dependency guard that
  skips job-format generation with a warning when cycles are detected
  while still emitting the other output formats.

### Changed

- **`lhp init` initializes in the current working directory** instead of
  creating a subdirectory; `project_name` is now used only for template
  rendering. The `--bundle` flag was flipped to `--no-bundle` so bundle
  (Databricks Asset Bundles) is the default. The generated bundle now
  includes a `bundle_uuid` field rendered into `databricks.yml`, and the
  `.gitignore` template adds `*.tmpl`, `.lhp/`, and `.bundle/` while
  dropping `.vscode/` (kept for IntelliSense schemas). Conflict detection
  switched from a directory-exists check to an `lhp.yaml` conflict
  check, with selective cleanup on failure instead of `shutil.rmtree`.
  Migration: existing users invoking `lhp init <name>` should now run it
  from inside the intended project directory and pass `--no-bundle` to
  opt out of bundle generation.
- **`DependenciesCommand` error propagation**: removed the
  error-swallowing `try/except` from `DependenciesCommand.execute()` and
  the `IOError` wrapping from `DependencyOutputManager.save_outputs()`.
  Errors now propagate to the CLI error boundary so failures are
  surfaced instead of silently downgraded.

### Fixed

- **SQL parser CTE name leak** that created false cross-CTE dependencies
  when one CTE referenced another; the parser now scopes CTE names per
  query and correctly handles subqueries and `UNION` / `INTERSECT` /
  `EXCEPT` set operations.

## [0.7.4] — 2026-01-19

### Added

- **Local variables in flowgroups**: new top-level `variables:` section in
  flowgroup YAML lets users define reusable values scoped to a single
  flowgroup, resolved before template parameters and environment
  substitution. Referenced via `%{var_name}` to reduce repetition and
  keep related values close to where they are used. Resolves #58.
- **Multi-target config for jobs and pipelines**: `job_name` and
  `pipeline` keys now accept a list of names in `job_config.yaml` and
  `pipeline_config.yaml`, applying the same configuration block to every
  entry. Duplicate names and empty lists are rejected with clear errors.
  Resolves #66.
- **Python 3.13 support**: CI now tests against Python 3.11, 3.12, and
  3.13. `pyproject.toml` dependencies updated to their latest compatible
  versions.

### Changed

- **Delta load action: unified `options` field.** YAML now uses a single
  `options:` map for Delta sources; the previous `reader_options`,
  `cdf_enabled`, and `cdc_options` fields are no longer supported and
  raise an error pointing users to the new structure. Migration: move
  keys from `reader_options:` and `cdc_options:` into `options:` (for
  example, `cdf_enabled: true` becomes `options: { readChangeFeed: "true" }`).
- **Minimum Python version raised to 3.11.** Python 3.8, 3.9, and 3.10
  are no longer supported. Resolves #72.

### Fixed

- Load generators (`CloudFiles`, `Delta`, `Kafka`) now validate that the
  `options` field is a dictionary and raise a user-friendly error rather
  than failing later during template rendering.
- `CodeFormatter` now logs the error type, traceback, and the first 500
  characters of the offending code when Black formatting fails, replacing
  silent or opaque failures.

## [0.7.3] — 2026-01-07

### Added

- **ForEachBatch sink**: new `foreachbatch` sink type lets users invoke a
  user-supplied Python `batch_handler` callable for each micro-batch
  produced by a streaming flow. Required keys (`module_path`,
  `batch_handler`) are validated up-front by `WriteActionValidator`, and
  the referenced module is tracked as a dependency so edits trigger
  regeneration. Documentation covers configuration, common use cases
  (REST APIs, external systems, custom merge logic), and best practices.
  Resolves #18.
- **Substitution in `python_transform` action fields**: `module_path`
  and `function_name` are now passed through the substitution engine,
  so values like `${python_modules_root}/cleanup.py` and `${env}_clean`
  resolve correctly per environment.

### Changed

- **Sink dependency tracking**: `StateDependencyResolver` now records
  `module_path` for both `foreachbatch` and `custom` sinks, so edits to
  those handler files participate in incremental regeneration alongside
  the YAML.
- Validators (`ConfigValidator`, `FlowgroupProcessor`,
  `PipelineValidator`) now re-raise `LHPError` as-is rather than wrapping
  it, producing consistent error formatting and preserving the original
  error context. Secret and flowgroup validation messages now include
  the detailed validation context.

## [0.7.2] — 2025-12-19

### Added

- **Per-source `readMode` for streaming tables**: each source in a
  streaming-table write action can now declare `readMode: stream` or
  `readMode: batch` independently. The generated code emits
  `spark.readStream` or `spark.read` per source, instead of forcing a
  single mode on the whole table. Resolves #22.
- **Parallel flowgroup processing**: `ActionOrchestrator` now processes
  flowgroups concurrently via a new `parallel_processor` module, with
  `PythonFileCopier` providing thread-safe file copy and conflict
  detection for the python-action copy step. Single-process behavior is
  preserved for diagnostics; speed-up is largest on projects with many
  flowgroups.
- **External-file dependency extraction from template parameters**:
  `StateDependencyResolver` heuristically detects file paths (schema
  files, SQL files, custom python modules) passed as template parameters
  and tracks them as dependencies, so edits trigger correct regeneration
  even when the path lives inside a template-expanded value.
- **Pipeline-generation summary**: generation now logs the number of
  files written vs. skipped by the smart writer, surfacing what the
  incremental path actually did.

### Changed

- **Cross-platform path normalization**: `StateDependencyResolver`,
  `DependencyTracker`, `StateCleanupService`, `PythonFileCopier`,
  `Action`, and `CodeGenerator` now normalize file paths to forward
  slashes via a new `utils/path_utils.py`. State files written on
  Windows and Linux are now interoperable, and generated python files
  use relative, environment-independent paths. Resolves #52.
- **Orchestrator refactor**: `Orchestrator` was restructured into helper
  functions for source extraction and batch processing. Action
  validators moved into a dedicated `validators/` package, and a new
  `OperationalMetadataService` centralizes operational-metadata handling
  (previously duplicated across generators). `generate_pipeline()` was
  removed in favor of the unified `generate_pipeline_by_field()` path.
- **Operational metadata import management**: `BaseActionGenerator` now
  consolidates metadata retrieval and import detection into a single
  `get_metadata_and_imports()` call, and registers expressions with
  `ImportManager` for semantic tracking. Imports declared by metadata
  expressions stay consistent with the file's actual imports.
- **Package description updated** in `pyproject.toml`, `README.md`, and
  `LLM.txt` from "Lakeflow Declarative Pipelines" to "Lakeflow Spark
  Declarative Pipelines", aligning with the upstream Databricks
  terminology adopted in 0.7.0.

### Fixed

- `create_table: false` is now correctly honored end-to-end (minor
  flow-only bug).
- `TableCreationValidator` now embeds a complete example configuration
  in its `LHPError`, so users can see the exact YAML shape needed to
  resolve a conflict rather than only the error class.

## [0.7.1] — 2025-11-26

### Added

- **Catalog and schema in `pipeline_config.yaml`**: new `catalog` and
  `schema` keys at the pipeline-config level let users set the Unity
  Catalog target per environment without repeating the value in every
  flowgroup. Values are validated and embedded in the generated
  `*.pipeline.yml` resource.
- **Multi-job orchestration via `job_name`**: flowgroups can now declare
  a `job_name`, grouping them into separate Databricks jobs. `lhp deps`
  generates per-job orchestration files plus a master orchestration job;
  an "all-or-nothing" validation rule prevents partially-tagged
  pipelines from producing an ambiguous job graph. Resolves #45.
- **Schema file support (YAML/JSON) in streaming-table and
  materialized-view writes**: `table_schema:` can now reference an
  external `.yaml`, `.yml`, or `.json` file in addition to inline
  DDL/SQL. A new `SchemaParser` converts the structured definition to
  DDL at generation time.
- **External-file schema for `cloudFiles.schemaHints`**: schema hints
  can now point at an external DDL or SQL file rather than being
  inlined. The referenced files are tracked by `StateDependencyResolver`
  so edits trigger regeneration.
- **`schema_transform` action: external files and strict/permissive
  modes.** The schema transform was reworked to support external schema
  files and fixed strict/permissive mode handling. Resolves #23.

### Changed

- **`schema` field renamed to `table_schema`** in write-action
  configuration. `schema` remains accepted for backward compatibility
  but is now documented as legacy; new code and examples should use
  `table_schema`. The rename disambiguates the field from PySpark's
  `schema` and from cloud-files schema hints.
- **Templates renamed** to drop characters that broke checkouts on
  Windows file systems.

### Fixed

- `SQLLoadGenerator` and `SQLTransformGenerator` now fall back to
  `Path.cwd()` when no `project_root` is available in the context,
  fixing failures in projects that invoke the API directly. Resolves
  #16.
- Error handling in `LakehousePlumberApplicationFacade` and
  `ActionOrchestrator` no longer duplicates error details across log
  lines and re-raises.

## [0.7.0] — 2025-11-10

### Changed

- **Generated code migrated from `dlt` to `pyspark.pipelines as dp`
  (Spark Declarative Pipelines API).** This is the headline change in
  0.7.0: Lakehouse Plumber now emits Lakeflow Spark Declarative
  Pipelines (SDP) code aligned with the current Databricks API,
  replacing the legacy DLT decorators across every code path.
  Specifically:
  - `import dlt` → `from pyspark import pipelines as dp`
  - `@dlt.table` → `@dp.materialized_view` (for materialized views)
  - `@dlt.view` → `@dp.temporary_view`
  - All `dlt.*` calls (e.g. `dlt.read_stream`,
    `dlt.create_streaming_table`) → `dp.*`
  - The deprecated `refresh_schedule` parameter on materialized views
    is no longer emitted.

  Import categorization also recognizes `pyspark.pipelines` as the
  DLT-equivalent module. Migration: YAML inputs are unchanged —
  regenerate the project (`lhp generate --env <env> --force`) to pick
  up the new decorators. Hand-edited `dlt.*` code in custom python
  actions must be updated to `dp.*` manually. Cluster/runtime must
  support the `pyspark.pipelines` API.

### Added

- **Sink framework for write actions.** New modular sink architecture
  (`BaseSink` + concrete implementations) lets write actions target
  destinations beyond Delta tables. Three sink types ship in 0.7.0:
  - **Delta sink**: writes to Delta tables with full streaming-table /
    materialized-view option support.
  - **Kafka sink**: streams output to Kafka topics with configurable
    serialization and partitioning. A dedicated `kafka_validator`
    checks broker URLs, topic names, and options up-front.
  - **Custom sink**: extensibility hook for user-supplied destinations.

  Sinks are configured under a `sink:` block in the write action.
  Existing Delta-only `write` actions continue to work unchanged.
  Resolves #17.
- **Unresolved-token validation** (`LHP-CFG-010`): a new
  `validate_no_unresolved_tokens()` step runs after substitution and
  recursively scans rendered config for stray `{token}` patterns
  (excluding `dbutils.secrets.get` references), with detailed context
  and fix suggestions. Circular substitution references are detected
  with a 10-iteration cap and surfaced as warnings.
- **`lhp show substitutions` command**: displays available substitution
  tokens for an environment, useful for diagnosing rendering issues
  caught by the new validator. Resolves #42.
- **Quarantine plumbing in expectations template**: the data-quality
  expectations Jinja template now uses named variables (fail / drop /
  warn) for expectation lists, in preparation for upcoming quarantine
  support. Resolves #39.

### Fixed

- Action-validator integration with the new write/sink schema; bundle
  manager updated to emit sink-aware pipeline resources.

## [0.6.5] — 2025-10-29

### Added

- **Kafka load source action**: new `type: kafka` load action generates
  code for `spark.readStream.format("kafka")` (or batch `spark.read`)
  with full option pass-through. Built on the same generator pattern as
  CloudFiles, with a dedicated `KafkaLoadGenerator`, Jinja2 template,
  action-registry entry, and config validator. Includes optional
  operational-metadata columns and works with both streaming and batch
  read modes. Closes #38.
- **Kafka auth reference templates**: shipped reference templates for
  the two most common managed-Kafka auth patterns — Azure Event Hubs
  with OAuth and AWS MSK with IAM authentication — plus documentation
  describing the required options and connection strings.

### Fixed

- **Quote/backslash escaping in load templates**: `cloudfiles`,
  `custom_datasource`, and `jdbc` load templates now correctly escape
  quotes and backslashes in option values. Previously, an option value
  containing a quote or a backslash (e.g., a regex pattern, a Windows
  path, or a JSON-encoded secret) could produce syntactically invalid
  generated Python or change the runtime value at generation time.

## [0.6.4] — 2025-10-29

### Added

- **Multi-flowgroup YAML files**: a single pipeline YAML file can now
  declare multiple flowgroups, in either multi-document syntax
  (multiple `---`-separated documents per file) or array syntax (a
  top-level list of flowgroup mappings). Previously, every flowgroup
  required its own file, which forced large pipelines into deep
  directory trees. Existing single-flowgroup files continue to parse
  unchanged. Closes #12, #28.
- **`lhp generate -pc <config-file>` regenerates DAB pipeline YAML on
  `--force`**: when the pipeline-config flag is supplied together with
  `--force`, the corresponding Databricks Asset Bundle pipeline YAML
  files under `resources/` are rewritten. Previously, `--force` only
  regenerated the Python pipeline code, leaving stale resource YAML on
  disk.
- **Anonymous usage telemetry with explicit opt-out**: `lhp generate`
  now emits aggregated, anonymous usage metrics (flowgroup and template
  counts, project/machine identifiers hashed) to help prioritize
  feature work. Two opt-out paths are honored: setting
  `LHP_DISABLE_ANALYTICS=1` in the environment, and running inside
  `pytest`. Documentation describes what is collected and how to turn
  it off.

### Fixed

- **Malformed YAML for cluster config in pipeline-config flow**: custom
  cluster blocks supplied via `lhp generate -pc` no longer produce
  syntactically invalid DAB pipeline YAML. Closes #37.

## [0.6.3] — 2025-10-28

### Fixed

- **Template-level presets are now applied** (#34): templates could
  declare a `presets:` list, but the field was missing from the
  `Template` model and the value was silently dropped — none of the
  listed presets were actually applied to generated actions. The
  `Template` model now carries `presets`, and `FlowgroupProcessor`
  applies them after template expansion with the documented precedence:
  flowgroup-level presets override template-level presets. Referencing
  a preset that does not exist now raises `ValueError` instead of
  failing silently. Projects that previously relied on the (broken)
  silent-skip behavior will see a hard error on regenerate — remove the
  bogus reference or create the missing preset file (e.g.,
  `bronze_layer.yaml`) to resolve.

## [0.6.2] — 2025-10-28

### Added

- **Customizable DLT pipeline configuration via YAML** (`-pc` /
  `--pipeline-config`): a new pipeline-config YAML file lets projects
  define DLT pipeline-level defaults (serverless, clusters,
  notifications, channel, edition, photon, configuration map, …) and
  per-pipeline overrides. The `BundleManager` loads this file and
  merges it into the generated DAB pipeline resource YAML, removing
  the need to hand-edit generated `resources/` files after every
  regenerate. Closes #13, #14, #31; resolves #29.
- **Customizable orchestration job configuration**: a complementary
  job-config file lets users set `max_concurrent_runs`, notifications,
  schedule/trigger, tags, and related Databricks Jobs API fields
  applied by `lhp deps` when generating the orchestration job YAML.
  Fixes #15, #21.
- **Bundle-mode job output to `resources/`**: when bundle output is
  enabled, generated job YAML files are written to the `resources/`
  directory so they are picked up by `databricks bundle deploy` without
  additional wiring. Fixes #26.

### Fixed

- **Python transform action: import resolution bug** affecting
  generated pipeline files has been corrected.

## [0.6.1] — 2025-10-01

### Fixed

- **Dependency detection bug**: a minor incorrect-edge issue in the
  `lhp deps` graph builder (introduced alongside the v0.6.0 dependency
  feature) has been corrected. Graphs now match the intended
  source-to-target relationships.

## [0.6.0] — 2025-10-01

### Added

- **Pipeline dependency analysis (`lhp deps`)**: new subcommand that
  walks every flowgroup, extracts source-table references from both
  SQL and Python action bodies, and produces a project-wide dependency
  graph. Output is available in multiple formats — `dot` (Graphviz),
  `json`, `text`, and `job` (a generated Databricks Jobs YAML that
  runs upstream pipelines before downstream ones), plus `all` which
  emits every format. Backed by a new `DependencyOutputManager`, a
  `PythonParser` that recognizes `spark.sql(...)`,
  `spark.read.table(...)`, and related patterns, and a `SQLParser`
  that handles joins, CTEs, and quoted / multi-part identifiers. This
  is the foundation `lhp deps` continues to build on in subsequent
  releases.

## [0.5.9] — 2025-09-15

### Changed

- **Architectural refactor: monolithic classes broken down into
  single-responsibility services.** `ActionOrchestrator` (~1300 lines),
  `StateManager` (~1300 lines), and the `lhp` CLI entrypoint (~1500
  lines) were decomposed into focused service modules. New modules
  include `core/services/` (`code_generator`, `flowgroup_discoverer`,
  `flowgroup_processor`, `pipeline_validator`,
  `generation_planning_service`), `core/state/` (`dependency_tracker`,
  `state_analyzer`, `state_persistence`, `state_cleanup_service`),
  `core/commands.py`, `core/factories.py`, `core/layers.py`,
  `core/strategies.py`, and `utils/template_renderer.py` /
  `utils/yaml_loader.py`. Public APIs were promoted from previously
  private methods so the orchestration pipeline is composable and
  testable. User-facing CLI behavior is unchanged.
- **CLI restructured into per-command modules under
  `src/lhp/cli/commands/`.** Each command (`generate`, `validate`,
  `init`, `show`, `state`, `stats`, plus `list_*`) now lives in its
  own file behind a shared `base_command.py`. The single 1500-line
  `main.py` is now a thin dispatcher.

### Removed

- **`src/lhp/bundle/yaml_processor.py` removed.** The legacy YAML
  processor module and its tests were deleted; bundle YAML
  modifications now go through `bundle/databricks_yaml_manager.py`
  (ruamel.yaml-backed) and the new template-renderer service. No
  user-facing YAML syntax change.
- **`LegacyGenerateCommand` and its 1300-line test suite removed.**
  Generation now flows exclusively through the new `GenerateCommand`
  implementation introduced earlier in the 0.5.x series.

### Fixed

- **Deterministic ordering in `BundleManager`.** Iterating over
  pipelines for bundle resource sync is now sorted, eliminating
  spurious diffs between consecutive `lhp generate` runs on different
  platforms.

### Added

- **End-to-end integration test fixture project under `tests/e2e/`**
  (`testing_project/`) covering bronze/silver/gold pipelines, test
  actions, custom Python functions, and a multi-environment
  substitution setup. Used by the new E2E suite to catch generation
  regressions across the whole orchestrator-to-bundle path.
- **CI: JUnit XML reports and Codecov integration.** Test runs in
  GitHub Actions now publish JUnit XML and upload coverage to Codecov.

## [0.5.2] — 2025-09-02

### Added

- **Automatic `databricks.yml` variable management.** `lhp generate`
  now scans generated Python files for the first `catalog.schema`
  pattern, locates the matching variable names in the current
  environment's `substitutions/<env>.yaml`, and writes the resolved
  per-environment values into the `variables:` block of every matching
  target in `databricks.yml`. Two variables are populated:
  `default_pipeline_catalog` and `default_pipeline_schema`. This
  removes the need to hand-edit `databricks.yml` after changing
  substitution values.
- **ruamel.yaml-backed `DatabricksYAMLManager`.** The new
  `src/lhp/bundle/databricks_yaml_manager.py` is the only place in LHP
  that uses ruamel.yaml; it preserves comments, quoting, and key order
  when updating `databricks.yml` variables. Other YAML operations
  continue to use PyYAML.
- New runtime dependency: `ruamel.yaml>=0.17.0`.

### Changed

- **Bundle pipeline-resource template now emits static variable
  references** (`${var.default_pipeline_catalog}` /
  `${var.default_pipeline_schema}`) instead of inlining catalog/schema
  values extracted from generated Python. Catalog/schema selection is
  now driven entirely by `databricks.yml` variables, which LHP
  populates from substitutions.
- **`resources/lhp/` is now flat** — per-environment subdirectories
  (`resources/lhp/dev/`, `resources/lhp/prod/`, …) were removed;
  resource files live directly under `resources/lhp/`. The bundle
  template references
  `${workspace.file_path}/generated/${bundle.target}/<pipeline>/**` so
  each environment still gets its own deployed artifact set. Existing
  projects regenerating with `lhp generate` will see resource YAML
  files relocate; commit the move.
- **`lhp init --bundle` scaffolding refreshed.** Generated
  `databricks.yml` now defines top-level `variables:` for
  `default_pipeline_catalog` / `default_pipeline_schema`, adds a `tst`
  target between `dev` and `prod`, and switches `prod`/`tst` to
  service-principal `run_as` and permission stubs.
- **Pre-flight validation of `databricks.yml` targets.** Generation
  now fails fast with `MissingDatabricksTargetError` if any
  substitution environment lacks a matching `targets.<env>` block in
  `databricks.yml`.

## [0.5.1] — 2025-09-01

### Added

- README link to the ReadTheDocs documentation site.
- `black>=23.0.0` is now a runtime dependency (previously only a
  dev-time requirement); generated code formatting works in
  environments that install LHP without dev extras.

## [0.5.0] — 2025-08-29

### Added

- **Test actions.** A new top-level `ActionType.TEST` plus nine
  `test_type` values — `row_count`, `uniqueness`,
  `referential_integrity`, `completeness`, `range`, `schema_match`,
  `all_lookups_found`, `custom_sql`, `custom_expectations`. Each test
  is emitted as a DLT expectation with a configurable `on_violation`
  of `fail`, `warn`, or `drop`. Tests are validated by a new
  `TestActionValidator` and generated by `TestActionGenerator`
  (`src/lhp/generators/test/`). See `docs/test_actions.rst`.
- **`lhp generate --include-tests` flag.** Test actions are skipped
  by default for faster CI builds; pass `--include-tests` (or run a
  test-only environment) to emit them. Flowgroups that contain only
  tests produce no Python file when `--include-tests` is not set.
- **Per-environment generated output.** `lhp generate --env <env>`
  now writes to `generated/<env>/<pipeline>/` by default instead of a
  single `generated/` tree, so dev/tst/prod artifacts coexist without
  overwriting each other. Override with `--output`.
- **`required_lhp_version` in `lhp.yaml`.** Projects can pin the
  framework to a PEP 440 specifier (`==`, `~=`, `>=,<`). `lhp generate`
  and `lhp validate` fail with a clear `LHP-CFG-007/008` error when
  the installed version is out of range. Bypass with
  `LHP_IGNORE_VERSION=1` (intended for emergencies, not production).
- **Per-environment bundle resources.** Bundle resource files are
  written under `resources/lhp/<env>/` so each target deploys its own
  resource set; the pipeline template emits `libraries.glob` and
  `root_path` paths scoped to the environment.
- **CI/CD reference documentation** (`docs/cicd_reference.rst`, ~2000
  lines) covering GitHub Actions, Azure DevOps, and Bitbucket
  workflows for Asset-Bundle deployments. A sample
  `lakehouse-cicd.yml` workflow ships in the ACME example project.
- New runtime dependency: `packaging>=23.2` (used for version-specifier
  checking).

### Changed

- **`lhp generate --format` flag removed.** Black formatting is now
  always applied to generated code; the redundant opt-in flag was
  dropped. Behavior is equivalent to the previous `--format` on, so
  no migration is needed beyond removing the flag from scripts.
- **`lhp generate` warns when the requested `--env` has no matching
  target in `databricks.yml`.** Generation continues, but the warning
  surfaces the missing target before deploy time.
- **Empty flowgroups are now skipped silently.** A flowgroup whose
  actions all evaluate to no-ops (typically a tests-only flowgroup
  without `--include-tests`) no longer produces an empty `.py` file
  or a state entry.

## Earlier Releases (v0.2.6-alpha – v0.4.1) — 2025-07-10 through 2025-08-18

Fourteen tagged releases (`V0.2.6-alpha` through `v0.4.1`) spanning
roughly five weeks of early development. These predate the v0.5.0
test-actions / per-environment-output milestone and were primarily
rapid internal iteration on the core surface area. Pull requests were
not yet routine; nearly all changes landed as direct pushes. What
follows is a condensation, not a per-version diary.

### Added

- **Initial PyPI release.** `V0.2.6-alpha` (2025-07-10) was the first
  tagged release on PyPI; the CI publish workflow and PyPI
  version-check guard landed shortly after, followed by ReadTheDocs
  and GitHub Pages publishing.
- **Databricks Asset Bundles integration** (`v0.3.1`, PR #6,
  2025-07-21). Introduced `src/lhp/bundle/` (manager, template
  fetcher, YAML processor, exceptions, bundle-detection), the
  `lhp init --bundle` flag, automatic resource-YAML synchronization
  under `resources/lhp/`, and the `databricks.yml.tmpl` scaffold. The
  flag became the default in v0.7.5.
- **State tracking and staleness detection** (`v0.2.12`–`v0.2.13`).
  Introduced `.lhp_state.json`, the `lhp state` command, the
  state-display service, composite-checksum staleness logic, and
  dependency discovery — the basis of incremental regeneration still
  used today.
- **`create_table` field and append-flow API** for streaming-table
  writes, with orchestrator-level cross-flowgroup validation
  (`V0.2.6-alpha`).
- **VS Code IntelliSense** for LHP YAML via `lhp setup-intellisense`,
  with JSON schemas and editor configuration (`v0.2.7`+).
- **Include-pattern filtering** in `lhp.yaml` to scope which YAML
  files are processed (`v0.2.7`).
- **Pipeline-field-based flowgroup discovery** — the orchestrator now
  groups flowgroups by their `pipeline:` field rather than directory
  layout, allowing one directory tree to contribute to many pipelines
  (`v0.2.7`).
- **Table tags** on streaming tables and materialized views
  (`v0.3.3`).
- **Temp-table transform** action (`v0.2.15`).
- **Operational metadata** integration across load and transform
  generators, with template-level column-sort logic.
- **Custom PySpark DataSource as a load action** (`v0.4.0`,
  2025-08-04). New `custom_datasource` generator and `import_manager`
  utility for detecting and rewriting user imports — the foundation
  of the `custom_datasource` / `custom_sink` surface area today.
- **Multi-platform, multi-version CI** — Linux, macOS, Windows across
  Python 3.8–3.12, with forward-compatible type-annotation refactors
  and Windows-specific logging fixes (`v0.3.6`).
- **`lhp init` Jinja2 template system** (`v0.3.4`). The scaffolded
  project layout — `pipelines/`, `presets/`,
  `substitutions/dev|tst|prod.yaml`, `expectations/`, `schemas/`,
  `templates/`, `bundle/` — was introduced here as `*.j2`/`*.tmpl`
  assets in `src/lhp/templates/init/`.

### Removed

- The legacy `src/lhp/notebook/` module (`deployment.py`,
  `interface.py`, `widgets.py`, ~1,700 LOC) was deleted in `v0.3.4`
  and replaced with the Jinja2 init-template loader. Projects
  scaffolded prior to v0.3.4 used a notebook-based deployment flow
  that no longer exists.

For per-commit detail across this era, see
`git log V0.2.6-alpha..v0.4.1`.

[Unreleased]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.8.6...HEAD
[0.8.6]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.8.5...v0.8.6
[0.8.5]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.8.4...v0.8.5
[0.8.4]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.8.3...v0.8.4
[0.8.3]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.8.2...v0.8.3
[0.8.2]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.8...v0.8.0
[0.7.8]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.7...v0.7.8
[0.7.7]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.6...v0.7.7
[0.7.6]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.5...v0.7.6
[0.7.5]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.4...v0.7.5
[0.7.4]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.3...v0.7.4
[0.7.3]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.2...v0.7.3
[0.7.2]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.6.5...v0.7.0
[0.6.5]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.6.4...v0.6.5
[0.6.4]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.6.3...v0.6.4
[0.6.3]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.6.2...v0.6.3
[0.6.2]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.5.9...v0.6.0
[0.5.9]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.5.2...v0.5.9
[0.5.2]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.4.1...v0.5.0
