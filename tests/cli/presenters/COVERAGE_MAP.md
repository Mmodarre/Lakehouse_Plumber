# Live-panel / warning-panel / render-primitive coverage map

When the CLI was rebuilt (v0.9.0 architecture refactor), the legacy
single-`Live`-panel rendering stack was deleted and replaced by the
**event-stream presenter** package (`src/lhp/cli/presenters/event_stream/`)
plus the per-command **presenters** (`summary`, `list`, `dag`,
`substitutions`, `diff`, …). The legacy modules

- `lhp.cli.render`            (listing tables, empty-state, command header/wordmark)
- `lhp.cli.live_panel`        (PhaseTracker, OverallProgress, ActivityTail, render_live_frame, coalescer, wordmark)
- `lhp.cli.warning_panel`     (render_warning_panel)
- `lhp.cli.generate_summary`  (print_summary_table)
- `lhp.cli.validate_summary`  (print_validate_summary_table)
- `lhp.api.WarningCollector`  (data-side warning collector)

are all gone. With them went their test files. This document maps every
behavior those deleted tests pinned to its **successor** test, and explicitly
names the behaviors that have **no successor by design** (and why).

## Deleted test files (12) and raw behavior counts

| Deleted test file | tests | what it pinned |
|---|---:|---|
| `tests/cli/test_render.py` | 26 | `render_listing_table` / `render_empty_state` / `render_command_header` (TTY + plain), wordmark constant reuse, default-sink-resolves-at-call-time |
| `tests/cli/test_live_panel_primitives.py` | 43 | `PhaseTracker`, `OverallProgress`, `ActivityTail`, `render_live_frame`, `_LiveUpdateCoalescer`, `_get_lhp_version` cache |
| `tests/cli/test_warning_panel.py` | 5 | `render_warning_panel` title/dedup/order rendering |
| `tests/cli/test_base_command.py` | 4 | `BaseCommand.announce_log_file()` log-path echo |
| `tests/test_generate_status_panel.py` | 17 | `print_summary_table` rows/footer/verbs + `PhaseTracker`/`render_live_frame`/`rich_handler_attached` |
| `tests/test_discovery_spinner.py` | 3 | discovery-phase spinner inside `render_live_frame` |
| `tests/test_validate_rendering.py` | 2 | `print_validate_summary_table` columns + status icons (syrupy) |
| `tests/test_validate_live_rendering.py` | 4 | post-Live validate summary + `_issue_view_to_lhp_error` → `render_error_panel` bridge |
| `tests/test_show_all_flag.py` | 16 | `--show-all` filtering / footer hint in `print_summary_table` + `print_validate_summary_table` |
| `tests/test_show_rendering.py` | 1 | `ShowCommand._display_flowgroup_configuration` YAML panel (syrupy) |
| `tests/test_show_substitutions_rendering.py` | 2 | `ShowCommand.show_substitutions` Tree (TTY) vs dotted-flatten (plain) |
| `tests/test_blueprint_cli.py` | 5 | `lhp list_blueprints --verbose`, `lhp show --instance`, mutual-exclusion CFG-057/058 |

Also removed: two orphaned syrupy snapshots whose tests are gone —
`tests/__snapshots__/test_show_rendering.ambr`,
`tests/__snapshots__/test_validate_rendering.ambr`.
(`test_lhperror_rendering.ambr` is kept: its test still lives.)

## Successor surface

Live/non-TTY rendering of a run now flows through two renderers selected by a
factory and driven by one dispatch loop:

- `event_stream/renderer_factory.py` — picks TTY vs CI/non-TTY renderer.
- `event_stream/live_renderer.py` — `LiveRenderer` (the successor to `render_live_frame` + `PhaseTracker` + `OverallProgress` + `ActivityTail` + the coalescer).
- `event_stream/log_renderer.py` — `LogRenderer` (non-TTY/CI, one line per distinct event; warning dedup).
- `event_stream/_status_line.py` — `build_status_line`, `_bar`, `_counter`, `duration_suffix`, and the `GLYPH_CHECK/CROSS/WARN/SPINNER` + `_BAR_FULL/_BAR_EMPTY` constants (the successor to the old wordmark/glyph/bar primitives).
- `event_stream/_event_dispatch.py` — `EventSink` ABC + `drive()` (successor to the manual frame-pump loop).
- `event_stream/_model.py` — `RunHeader`, `RenderOptions`, `WarningLine`, `FailureLine`, `RunOutcome` (frozen value objects; successor to `HeaderContext` + `PipelineRecord`).
- `presenters/summary_presenter.py::print_run_summary` — the **single** post-run summary (successor to both `print_summary_table` and `print_validate_summary_table`).
- `presenters/list_presenter.py`, `substitutions_presenter.py`, `dag_presenter.py`, `diff_presenter.py` — successors to the `render_listing_table` / Tree / empty-state callers.

Successor tests:

- `tests/cli/presenters/event_stream/test_live_renderer.py`
- `tests/cli/presenters/event_stream/test_log_renderer.py`
- `tests/cli/presenters/event_stream/test_factory.py`
- `tests/cli/presenters/event_stream/test_dispatch.py`
- `tests/cli/presenters/event_stream/test_model.py`
- `tests/cli/presenters/test_summary_presenter.py`
- `tests/cli/presenters/test_no_errors_import.py`
- `tests/cli/test_list_command.py`, `test_substitutions_command.py`, `test_dag_command.py`, `test_diff_presenter.py` (per-command presenter behavior)

---

## Old behavior → successor mapping

### A. `render_live_frame` / `PhaseTracker` / `OverallProgress` / `ActivityTail`
(`test_live_panel_primitives.py`, `test_generate_status_panel.py`, `test_discovery_spinner.py`)

| Old behavior | Successor |
|---|---|
| Active phase renders a `Spinner` so the outer Live animates (`..._render_active_phase_contains_spinner`, `..._active_discovery_contains_spinner`, `..._active_phase_contains_spinner`) | `test_live_renderer.py::test_bar_and_counter_present_at_narrow_width` (every frame keeps live motion: bar + counter); spinner glyph is now a live `rich.spinner.Spinner` frame passed as the `spinner` arg to `_status_line.build_status_line` |
| Completed `Discovering` leaves no spinner (`..._after_discovery_completes_has_no_spinner`) | `test_live_renderer.py::test_success_path_prints_no_permanent_success_line` + `test_failed_phase_renders_cross_on_stage_line` (permanent stage lines, not a lingering spinner) |
| Failed phase renders red `✗` marker (`phase_tracker_failure_records_red_x_marker`, `phase_marker_failed_uses_red_x_marker`) | `test_live_renderer.py::test_failed_phase_renders_cross_on_stage_line` (asserts `GLYPH_CROSS` on the failed stage line) |
| Frame body includes the `OverallProgress` (Progress) bar (`..._body_includes_progress_renderable`, `..._with_records_shows_overall_progress`) | `test_live_renderer.py::test_bar_and_counter_present_at_narrow_width` (bar `█`/`░` + `n/m` counter present in every frame; reaches `2/2`) |
| Frame fits width / wide vs narrow layout (`..._wide_terminal_body_is_grid…`, `..._wordmark_omitted_on_narrow_terminal`) | `test_live_renderer.py::test_every_status_frame_fits_width` (parametrized widths 30/40/200 — the resize-safe successor) |
| Title carries command/env/elapsed (`..._title_contains_header_fields`, `..._with_records_shows_overall_progress` `pipelines=1`) | `test_model.py::test_run_header_construct_and_frozen` (RunHeader holds command/env) + `test_factory.py` render-path tests build a `RunHeader` end to end |
| Title appends/omits "N failed" suffix (`..._title_appends_failed_suffix_when_positive`, `..._omits_failed_suffix_when_zero`) | `test_summary_presenter.py::test_generate_counts_line_uses_generated_and_failed` + `test_validate_counts_line_uses_validated_and_warning_and_failed` (the failed-count now lives on the post-run counts line) |
| `ActivityTail` caps history / "Recent:" label / per-record `✓`/`✗`/`—`+code/duration glyphs | `test_log_renderer.py` (per-event line, dedup) + `test_summary_presenter.py::test_…_failures_include_flowgroup_and_file_segments` / `test_generate_failures_are_pipeline_level_no_flowgroup_segment` (per-failure attribution with code) |
| `ActivityTail.render()` snapshots `_entries` before iterating to avoid `deque mutated during iteration` under the refresh thread (`..._render_snapshots_before_iterating`) | **No successor — by design.** See "No-successor-by-design (1)". |
| `PhaseTracker.complete(unknown)` is a no-op + DEBUG breadcrumb (`..._complete_unknown_name_is_noop`, `..._emits_debug_log`) | **No successor — by design.** `PhaseTracker` is gone; the dispatch loop only forwards events that the stream actually emits, so there is no "unknown phase name" path. Closest live analog: `test_dispatch.py::test_drive_dispatches_in_order…` (only emitted events are dispatched). |
| `PhaseTracker.complete(..., suppress_if_fast=True)` drops sub-250ms phases (`..._sub_threshold_complete_suppresses_when_opted_in`, `phase_marker_suppress_if_fast_drops…`) | **No successor — by design.** Sub-threshold suppression was a single-Live cosmetic; the log renderer prints one line per `PhaseCompleted` unconditionally and the live renderer's stage lines are event-driven. |
| `OverallProgress` task-clock lifecycle: not-started at construction, `start()` starts clock, `stop()` freezes, `stop()` before `start()` is a no-op (`..._task_clock_not_started…`, `..._start_starts_the_task_clock`, `..._stop_freezes…`, `..._stop_before_start_leaves_clock_unstarted`) | **No successor — by design.** `OverallProgress` (a bespoke `rich.progress.Progress` wrapper) is gone; elapsed time is now `duration_suffix()` over event durations. Covered indirectly: `test_summary_presenter.py` footer renders elapsed from `elapsed_s`. |
| `OverallProgress.start()` must NOT activate the inner `Progress` Live / push onto `console._live_stack` (`..._start_does_not_activate_internal_live`, `..._stop_does_not_leave_live_on_stack`, `..._auto_refresh_is_disabled`) | **No successor — by design.** See "No-successor-by-design (2): inner-Live stacking". The whole class of double-bar bugs is structurally impossible now — `LiveRenderer` owns exactly one `rich.live.Live` and there is no nested `Progress`-Live to stack. |

### B. `_LiveUpdateCoalescer` (manual frame coalescer)
(`test_live_panel_primitives.py`)

| Old behavior | Successor |
|---|---|
| Throttles updates within `min_interval_s` (`..._throttles_within_interval`) | **No successor — by design.** See "No-successor-by-design (4): manual coalescer". `LiveRenderer` delegates refresh cadence to Rich's own `Live(refresh_per_second=…)`; there is no hand-rolled coalescer to test. |
| `force=True` always flushes / flushes after a throttled update (`..._force_always_passes`, `..._force_flushes_after_throttled_update`) | **No successor — by design** (same reason). The "force a final repaint when the submit loop drains" need is gone: `drive()` runs to the terminal event and `LiveRenderer` paints the last frame on teardown. Closest live analog: `test_live_renderer.py` asserts the counter reaches its terminal `2/2`. |

### C. wordmark / version
(`test_render.py`, `test_live_panel_primitives.py`)

| Old behavior | Successor |
|---|---|
| `render_command_header` prints the ASCII wordmark on a wide TTY, is a silent no-op on a narrow TTY / non-TTY, reuses the `_LHP_WORDMARK` constant, renders subtitle | **No successor — by design.** See "No-successor-by-design (5): wordmark header". The decorative wordmark banner was dropped from the rebuilt CLI; commands no longer print a header panel. |
| Wordmark version label uses `importlib.metadata.version('lakehouse-plumber')` and agrees with `lhp --version` (`..._wordmark_version_uses_package_metadata`) | `lhp --version` itself: patch-target moved to `lhp.cli._version.get_version`; exercised by `tests/cli/test_main_group.py`. The wordmark surface that had to *agree* with it is gone. |
| `_get_lhp_version` is `functools.cache`-d so `importlib.metadata.version` is called ≤1×/process (`test_get_lhp_version_is_cached_per_process`) | **No successor — by design.** The per-Live-refresh version label that motivated the cache is gone (no wordmark in the live frame). `get_version()` is called once per `--version` invocation. |

### D. listing tables / empty state
(`test_render.py`)

| Old behavior | Successor |
|---|---|
| `render_listing_table` TTY: title/columns/rows present, total label present/absent, column styles applied, long strings don't crash | `test_list_command.py::test_presets_with_file_renders_a_row` (a real listing row), `test_blueprints_instances_shows_per_instance_pipelines` (per-instance rows + totals) |
| `render_listing_table` plain (non-TTY): tab-separated, no box-drawing, full long value preserved | `test_dag_command.py::test_dag_json_writes_file_path_to_stdout_and_analysis_to_stderr` (plain stdout/stderr split) + `test_list_command.py` plain-notice tests |
| `render_empty_state` TTY panel (both lines + box-drawing) and plain (two bare lines) | `test_list_command.py::test_presets_missing_dir_exits_zero_with_empty_notice` + `test_templates_missing_dir_exits_zero_with_empty_notice` (empty-state notice, exit 0) |
| Default `sink=None` resolves at call time (singleton swap honored) | Covered structurally: presenters now take an explicit `console`/`err_console` parameter (see `print_run_summary(err_console=…)`), so there is no module-level singleton to re-resolve. `test_no_errors_import.py` pins the import boundary. |

### E. substitutions Tree vs flatten
(`test_show_substitutions_rendering.py`)

| Old behavior | Successor |
|---|---|
| TTY: nested map renders as a Rich `Tree` (parent + `├──`/`└──` leaves), simple/reserved as tables | `test_substitutions_command.py::test_presenter_renders_nested_map_as_tree` |
| Non-TTY: nested map renders dotted-flattened, no tree connectors | `test_substitutions_command.py::test_substitutions_lists_tokens_against_fixture` (plain token listing to stdout) |
| Secret-reference form surfaced | `test_substitutions_command.py::test_presenter_renders_secret_reference_form` + `test_substitutions_lists_tokens_against_fixture` (default secret scope) |
| Reserved tokens (`workspace_env`, `logical_env`) present | covered by the fixture-driven `test_substitutions_lists_tokens_against_fixture` |

### F. generate / validate summary tables
(`test_generate_status_panel.py`, `test_show_all_flag.py`, `test_validate_rendering.py`, `test_validate_live_rendering.py`)

The two old table functions (`print_summary_table`, `print_validate_summary_table`)
are now the **one** `print_run_summary`.

| Old behavior | Successor |
|---|---|
| Counts/footer: "Generated N" / "Validated N", all-passed, warning count (singular vs plural) | `test_summary_presenter.py::test_generate_counts_line_uses_generated_and_failed`, `test_validate_counts_line_uses_validated_and_warning_and_failed`, `test_warning_plural_two_warnings` |
| Failure rows carry the real error code, omit file count, no `=====` border leak, code ≠ `—` | `test_summary_presenter.py::test_validate_failures_include_flowgroup_and_file_segments`, `test_generate_failures_are_pipeline_level_no_flowgroup_segment` |
| Per-failure rich Panel via `_issue_view_to_lhp_error` + `render_error_panel` (`test_validate_live_rendering.py`) | `test_summary_presenter.py::test_show_details_prints_panel_per_failure`, `test_show_details_generate_pipeline_level_panel`. NB: `_issue_view_to_lhp_error` was **not deleted** — it moved to `lhp.api._converters_common` and is tested via `tests/api/` (`test_to_dict_round_trip.py`, `test_validation_view_contract.py`). Only the old import path `lhp.cli.commands.validate_command._issue_view_to_lhp_error` is gone. |
| `--show-all` filtering (failures-only default, full table opt-in) + "(use -a to list)" footer hint | `--show-all`/`-a` renamed to `--show-details`/`-s`. Successor: `test_summary_presenter.py::test_show_details_prints_panel_per_failure` + `test_success_prints_no_next_step_hint` / `test_failures_print_next_step_hint` (the hint behavior). |
| Empty `records={}` → no output / early return | `test_factory.py::test_render_clean_generate_yields_no_failures` (clean run yields an outcome with no failure lines) |
| Elapsed footer uses real wall-clock, not `sum(duration_s)` | `test_summary_presenter.py` footer renders `_format_elapsed(elapsed_s)`; the double-count bug is structurally gone (presenter takes `elapsed_s`, never sums rows) |
| `PipelineRecord` defaults (`success is None` = in-flight) | `test_model.py` value-object tests (`RunOutcome`/`FailureLine`/`WarningLine` frozen construction) — `PipelineRecord` (a mutable in-flight accumulator) is replaced by immutable outcome lines |
| dry-run verb ("Would generate") differs from non-dry-run | **No successor — by design.** `generate --dry-run` was removed; previewing is now the separate `lhp diff` command — `test_diff_presenter.py` covers would-create / modified / orphan rendering. |
| `rich_handler_attached` restores root-logger handlers on exit | **No successor — by design.** That contextmanager belonged to the single-Live stack; logging is now configured once in `logging_config.py` and the renderers do not swap root handlers. `test_log_renderer.py::test_stdout_stays_empty` + `test_live_is_never_constructed` pin the new logging/stdout discipline. |

### G. warning panel
(`test_warning_panel.py`)

| Old behavior | Successor |
|---|---|
| Empty collector → no panel | `test_log_renderer.py` emits a line only per real `WarningEmitted`; `test_summary_presenter.py` counts banner shows no warning segment when none |
| Single "deprecation" → "Deprecation Warning" title; multiple/other → generic "Warnings" title | **No successor — by design.** The single-vs-generic *panel title* was a warning_panel cosmetic. Warnings now render as one line each (`test_log_renderer.py::test_repeated_warnings_collapse_to_one_line_each`) and are counted on the summary line (`test_warning_plural_two_warnings`). |
| Dedup preserves first-seen order across re-adds (`..._dedup_preserves_first_seen_order`) | `test_log_renderer.py::test_repeated_warnings_collapse_to_one_line_each` (4 events, 2 distinct `(code, file)` → 2 lines; `outcome.warnings` length 2). The dedup key moved from `WarningCollector` (deleted) to `LogRenderer`'s `(code, file)` set. |

### H. `BaseCommand.announce_log_file()`
(`test_base_command.py`)

| Old behavior | Successor |
|---|---|
| Log-file path echoed exactly once when a file was written, independent of `--verbose`; nothing when no file | **No successor — by design.** `BaseCommand` is deleted (constitution §9.11: no command base class / business logic in `cli/`). The `--log-file` path announcement is now handled in `logging_config.py` / the group callback (`main.py` wires `configure_logging`). Closest live coverage: `tests/cli/test_logging_config.py` (log-file plumbing) + `tests/cli/test_main_group.py`. |

### I. blueprint CLI
(`test_blueprint_cli.py`)

| Old behavior | Successor |
|---|---|
| `lhp list_blueprints` / `--verbose` shows blueprint summary + instances | `list_blueprints` → `list blueprints`; `--verbose` → `--instances`. Successor: `test_list_command.py::test_blueprints_instances_shows_per_instance_pipelines` + `test_blueprints_without_instances_omits_per_instance_block` |
| `lhp show --instance <yaml>` resolves a blueprint instance's flowgroups | **No successor — by design.** The `show` command is dropped entirely. |
| `show` mutual-exclusion errors CFG-057 (both flowgroup+instance) / CFG-058 (neither) | **No successor — by design.** These guarded `show`'s argument parsing, which is gone with the command. (Unrelated to the DO-NOT-TOUCH `--pipeline`-filter CFG-015→VAL-901 assertion in `tests/test_blueprint_validate.py`, which is a different command and is left untouched.) |

---

## No-successor-by-design (explicit)

These behaviors have **no** successor test, and that is correct — the code they
asserted no longer exists, and the bug class each prevented is now structurally
impossible rather than merely re-tested.

1. **`ActivityTail.render()` deque-snapshot race**
   (`test_live_panel_primitives.py::test_activity_tail_render_snapshots_before_iterating`).
   The race only existed because a background Rich refresh thread iterated
   `ActivityTail._entries` while the main thread appended. `LiveRenderer`
   builds each frame synchronously inside the `drive()` loop on a single
   thread — there is no concurrent producer/consumer over a shared `deque`, so
   `RuntimeError: deque mutated during iteration` cannot occur. Re-testing it
   would require re-introducing the deleted threading model.

2. **Inner-`Live` stacking / duplicate-bar invariants**
   (`..._start_does_not_activate_internal_live`,
   `..._stop_does_not_leave_live_on_stack`, `..._auto_refresh_is_disabled`,
   `..._start_does_not_activate_internal_live`).
   These pinned that the old `OverallProgress` (a `rich.progress.Progress`,
   which owns its *own* `rich.live.Live`) was never pushed onto
   `console._live_stack` beneath the outer panel `Live` — otherwise Rich
   composed both into a `Group` and painted the progress bar twice.
   `LiveRenderer` owns exactly one `rich.live.Live` and renders the bar as a
   plain string (`build_status_line` in `_status_line.py`), so there is no
   second `Live` to stack and no double-bar to guard.

3. **Multi-line / wide-vs-narrow frame layout structure**
   (`..._wide_terminal_body_is_grid_with_wordmark`,
   `..._body_is_group`, `..._wordmark_omitted_on_narrow_terminal`).
   The two-column `Table.grid` + wordmark layout is gone. The *load-bearing*
   property — frames stay within the terminal width at any size — survives as
   `test_live_renderer.py::test_every_status_frame_fits_width` (widths
   30/40/200). The specific internal renderable tree (`Group` vs `Table.grid`)
   was implementation detail of the deleted `render_live_frame`.

4. **Manual `_LiveUpdateCoalescer` throttle/force**
   (`..._throttles_within_interval`, `..._force_always_passes`,
   `..._force_flushes_after_throttled_update`).
   Refresh cadence is now Rich's responsibility via
   `Live(refresh_per_second=…)`; there is no hand-rolled debounce object and no
   "force a final flush when the submit loop drains" call site. The terminal
   state still lands (`test_live_renderer.py` asserts the counter reaches
   `2/2`), but the coalescer that produced it is deleted.

5. **Wordmark command header**
   (`test_render.py::TestRenderCommandHeader::*`, the wordmark assertions in
   `test_live_panel_primitives.py`, `test_get_lhp_version_is_cached_per_process`).
   The decorative ASCII wordmark / command-header panel was removed from the
   rebuilt CLI; commands no longer print a banner, so its TTY/non-TTY/threshold
   behavior and the per-refresh version-cache that fed it have nothing to
   succeed them. The version string itself is still surfaced by `lhp --version`
   (`test_main_group.py`).

## Acceptance

```
ls tests/cli/{test_render,test_live_panel_primitives,test_warning_panel,test_base_command}.py   # → no such file
ls tests/test_{show_rendering,show_substitutions_rendering,show_all_flag,blueprint_cli,generate_status_panel,validate_rendering,validate_live_rendering,discovery_spinner}.py  # → no such file
python -m pytest tests/cli/presenters/ -q   # → 45 passed
```
