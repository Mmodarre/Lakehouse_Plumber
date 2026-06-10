# LHP CLI Rendering Style Guide

## 1. Purpose

This file is the contract all CLI commands render against. Every static
command in `src/lhp/cli/commands/` must conform to the rules below. When a
command's output diverges from this guide, this guide wins — fix the command,
not the guide. The generate/validate Live frames in `live_panel.py` are out of
scope; this guide governs only the static commands (`init`, `show`, `info`,
`substitutions`, `stats`, `deps`, `list_presets`, `list_templates`,
`skill *`).

## 2. Output Sinks

Three sinks, never crossed:

- **`console` (stdout)** — primary data. Anything a caller might pipe, grep,
  redirect, or feed to another tool.
- **`err_console` (stderr)** — warnings, in-flight hints, and errors. Never
  primary data.
- **logging `StreamHandler` (stderr)** — diagnostic logs. Never primary data,
  never user-facing prose. Configured once in `main.py`.

`console` and `err_console` are module-level singletons in
`src/lhp/cli/console.py`. Import them; do not construct ad-hoc `Console()`
instances inside commands.

**Import the module, not the names.** Command modules use module-attribute
access so test-time monkey-patching of the singletons is honored:

```python
from .. import console as console_module
console_module.console.print(...)
console_module.err_console.print(...)
```

`from ..console import console, err_console` captures the originals at import
time; subsequent rebinding of `lhp.cli.console.console` by `tests/conftest.py`
(width=999 invariant) does not reach the captured name, and long output lines
silently wrap to width 80, fragmenting asserted substrings. Module-attribute
access resolves the singleton on every call, so the test fixture takes effect.

## 3. TTY-Aware Rendering Rule

Helpers in `cli/render.py` branch on `console.is_terminal`. **Callers never
branch.** A command function calls the helper with the same arguments
regardless of TTY state; the helper decides whether to render Rich
Tables/Panels with box-drawing or to emit plain output.

- **Terminal mode:** Rich `Table` / `Panel` / `Tree` with box-drawing.
- **Non-TTY mode** (pipe, redirect, CI log): plain `key: value` lines for
  scalar fields, tab-separated rows for tabular data. No ANSI, no box glyphs.

If you find yourself writing `if console.is_terminal:` inside a command,
move that branch into a helper.

## 4. Glyph Vocabulary

Three glyphs, total:

- `✓` — success / present / pass
- `✗` — failure / missing / fail
- `⚠` — warning / partial / attention

No sparkle, rocket, party-popper, or other decorative emoji. No box-drawing
characters typed by hand — Rich draws those.

## 5. Color Discipline

Five styles, total. Anything else is a bug.

| Style          | Use                                    |
| -------------- | -------------------------------------- |
| `bold green`   | success counts, ✓ totals               |
| `bold red`     | failure counts, ✗ totals, error labels |
| `bold yellow`  | warning counts, ⚠ totals               |
| `dim`          | hints, subtitles, secondary metadata   |
| (default)      | body text                              |

No background colors. No 256-color palette. No rainbow gradients on numbers.

## 6. Primitives

The only Rich primitives in static commands:

- **`Panel`** — `border_style="dim"`. Used for command-completion summaries
  and grouped notices.
- **`Table`** — `border_style="dim"`, `header_style="bold dim"`. Built only
  via the helpers in §7. Tables built outside the helpers must not set their
  own box-drawing style — use the helpers.
- **`Tree`** — nested-map rendering (substitutions only, currently).
- **`Text.assemble`** — multi-style inline strings.

No `Layout`, no `Columns`, no `Align`, no `Rule` in static commands. The Live
frames in `live_panel.py` use richer primitives — that is their domain, not
this one.

## 7. Exported Helpers (`cli/render.py`)

Phase 1 implements these. They are the **only** shared rendering helpers;
everything else inlines.

```python
render_listing_table(title, columns, rows, *, total_label=None, sink=None)
render_empty_state(title, hint, *, sink=None)
render_command_header(command_name, *, sink=None)
```

`sink` defaults to `console`. Pass `err_console` for warning-context
renderings.

`render_command_header` **must** be called as the first output line of every
static command. Gating logic (TTY check + terminal width ≥ 90 columns) lives
**inside the helper** — callers do not check width, do not check TTY, do not
conditionally call it. The helper reuses the existing `_LHP_WORDMARK`
renderable from `live_panel.py` (lines ~59–65) and the
`_WORDMARK_MIN_PANEL_WIDTH = 90` threshold (line ~77). It does not reinvent
the wordmark.

## 8. Inline-Only Sites

These sites render inline in their command modules. Do not promote them to
shared helpers — they are one-offs by intent:

- `init` success Panel
- `skill install` / `skill uninstall` one-liners
- `skill status` body
- `deps` Analysis Summary header
- `deps` execution-order Table
- `stats` complexity Table
- `substitutions` nested-map Tree

If a second site needs the same shape, that is the moment to discuss
promotion — not before.

## 9. References

- `src/lhp/cli/console.py` — `console` and `err_console` singletons.
- `src/lhp/cli/live_panel.py` — existing Rich `Live` infrastructure for
  `generate` and `validate`; source of the `_LHP_WORDMARK` constant and the
  `_WORDMARK_MIN_PANEL_WIDTH` threshold that `render_command_header` reuses.

The Live-frame commands (`generate`, `validate`) are governed by their own
rendering code in `live_panel.py`, `generate_summary.py`, and
`validate_summary.py`. This style guide does not constrain them.
