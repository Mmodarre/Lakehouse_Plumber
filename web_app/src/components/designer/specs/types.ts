// ── Action spec vocabulary — the contract for every action sub-type form ─
//
// A `ActionSubTypeSpec` describes, declaratively, how to render an editing
// form for ONE action sub-type (e.g. load/cloudfiles). The shared engine
// (`ActionForm`) is the only renderer; a spec never touches React, yaml-doc,
// or the network. Adding a new sub-type form is: write one spec module +
// one `registry.ts` line — no engine change.
//
// Design rules baked into this vocabulary:
//  • Forms edit EXPLICIT YAML only. Every widget maps a key's presence to a
//    control; clearing a control DELETES the key (never writes '' or null).
//    Keys not named by the spec are never read, written, or reordered.
//  • Every text-ish control tolerates substitution tokens (`${x}`, `{{ x }}`,
//    `%{x}`); the engine handles that uniformly, so specs never special-case
//    tokens.
//  • Client validation is SOFT (hints only) and never blocks a write —
//    `lhp validate` is the authority. `required` + `CrossFieldRule` feed the
//    hint line under a field; that is all they do.
//
// Field addressing: `FieldSpec.path` is relative to the action mapping, e.g.
// `['source', 'format']` or `['write_target', 'cdc_config', 'scd_type']`.
// The engine maps it through flowgroup-doc's `setActionField` /
// `deleteActionField`, which create/prune intermediate containers.

import type { ActionKind, YamlPath } from '@/lib/flowgroup-doc'

export type { ActionKind, YamlPath }

/**
 * The control a field renders as. Each maps to a Config-UI field primitive
 * (`components/config/fields`). A widget the engine does not yet render
 * degrades to a read-only value plus an "edit in the YAML" note, so a later
 * spec can name a not-yet-built widget without breaking the panel.
 */
export type FieldWidget =
  /** OptionalTextField, single line. */
  | 'text'
  /** OptionalTextField, multiline — SQL bodies, inline schema DDL. */
  | 'textarea'
  /** OptionalNumberField — integer, optional bounds. */
  | 'number'
  /** BoolSwitch — tri-state: an absent key shows `defaultValue`. */
  | 'bool'
  /** EnumSelect over `options`. */
  | 'enum'
  /** StringListEditor — ordered list of strings. */
  | 'stringList'
  /**
   * A ref that YAML allows as a single string OR a list (e.g. `source`):
   * renders a text field when the value is a string/absent, and a list
   * editor when the value is already a sequence. It does not silently
   * convert a string into a one-item list — the file's shape is preserved.
   */
  | 'stringOrList'
  /** KeyValueMapEditor — str→str map (options, table_properties, tags, spark_conf…). */
  | 'keyValue'
  /**
   * List-of-objects editor (test `expectations`…). Renders one card per row;
   * each row is a sub-object edited by `itemFields` (reusing the per-item
   * widgets). Add/delete a row, delete-on-clear of the last row, and every
   * item edit is an immediate write at `[...path, rowIndex, ...itemFieldPath]`.
   * `itemFields` is REQUIRED and declares the row shape.
   */
  | 'objectList'
  /**
   * Fixed-arity-2 list — two independently-addressable slots backing a
   * two-table compare (test `row_count`: `source: [tableA, tableB]`). Unlike
   * `stringList`, the arity is pinned at two: there is no add/remove, editing
   * either slot rewrites the WHOLE two-item array, and clearing BOTH slots
   * deletes the key (never leaves `['', '']`). A legacy non-2 list reads its
   * first two elements into the slots without reshaping the file; the
   * "exactly two" soft rule (a cross-field rule) flags the mismatch.
   */
  | 'dualSource'
  /**
   * Segmented "one-of" toggle over mutually-exclusive branches that have NO
   * backing discriminator key (inline `sql` ⊕ `sql_path`; a materialized
   * view's `source` ⊕ `sql` ⊕ `sql_path`; …). Each branch OWNS one YAML key
   * and renders its own control (inline code / file ref / plain text) per its
   * `backing`. Switching branches PRUNES every inactive branch's key (via
   * `applyDiscriminatorChange` with no discriminator write). Declared through
   * the `oneOf` descriptor; the field's own `path` is unused for the value.
   */
  | 'oneOfToggle'

/**
 * A discriminator's branch-ownership map: each branch VALUE maps to the
 * key-paths that branch OWNS. It is the input to `applyDiscriminatorChange`
 * (formModel), which, on a switch to `newValue`, deletes every path owned by a
 * now-inactive branch that the `newValue` branch does not also own — repairing
 * the stale-branch bug the Python validator rejects (e.g. a `quarantine:`
 * block left behind after `mode` moves off `quarantine`).
 *
 * The shape is uniform across both discriminator styles:
 *  • N-way enum (transform `mode` dqe/quarantine, `write_target.mode`
 *    standard/cdc/snapshot_cdc, `source.type`, `write_target.type`,
 *    `test_type`): one entry per option, each owning that branch's blocks.
 *  • 2-way inline⊕file toggle (`sql` ⊕ `sql_path`): two entries, each owning
 *    its single key; such a toggle may have no backing discriminator key of
 *    its own (see `applyDiscriminatorChange`'s optional `fieldPath`).
 *
 * Paths are relative to the action mapping — the same base `setActionField` /
 * `deleteActionField` expect. A path listed under more than one branch is
 * SHARED and survives a switch between those branches.
 */
export type BranchPathMap = Record<string, YamlPath[]>

export interface FieldSpec {
  /** Path relative to the action mapping, e.g. `['source', 'format']`. */
  path: YamlPath
  label: string
  widget: FieldWidget
  /** Placeholder shown while empty — good place to surface a default. */
  placeholder?: string
  /**
   * Soft-required: shows a non-blocking "required" hint when the key is
   * absent AND the field is visible. Never blocks the write.
   */
  required?: boolean

  // enum ------------------------------------------------------------------
  /** enum: allowed values, in display order (always strings; see `valueType`). */
  options?: readonly string[]
  /**
   * enum: how the control renders. `'segmented'` shows a segmented control
   * (one button per option) instead of the default dropdown — for short
   * discriminator sets (source.type, transform mode, write mode, scd_type,
   * sink_type). Omitting it changes nothing. Default `'select'`.
   */
  display?: 'segmented' | 'select'
  /**
   * enum: label for an explicit "not set" entry that DELETES the key. Omit
   * for a required enum. Mutually exclusive with `enumDefault`.
   */
  unsetLabel?: string
  /**
   * enum: the value that is the effective default when the key is ABSENT
   * (e.g. `write_target.mode` → `standard`). The control shows this value
   * while the key is absent; selecting it DELETES the key (back to default);
   * selecting anything else SETS the key. Mutually exclusive with
   * `unsetLabel`.
   */
  enumDefault?: string
  /**
   * enum/discriminator: which key-paths each branch value OWNS. When present,
   * a value change is committed through `applyDiscriminatorChange` (formModel)
   * rather than a bare `setActionField`, so the now-inactive branch's
   * exclusive keys are PRUNED on switch. Optional and additive — a discriminator
   * without it just sets the value, keeping today's hide-only behavior.
   *
   * The map's keys should cover the field's `options`; paths are relative to
   * the action mapping. See `BranchPathMap`.
   */
  branchPaths?: BranchPathMap

  // typing ----------------------------------------------------------------
  /**
   * The scalar type the committed value is written as. Applies to `enum`
   * (and any string-entered scalar): `'number'`/`'integer'` coerce the value
   * so the YAML holds `scd_type: 2`, not `scd_type: "2"`. Default `'string'`.
   */
  valueType?: 'string' | 'number' | 'integer' | 'boolean'

  // number ----------------------------------------------------------------
  /** number: inclusive bounds enforced at commit. */
  min?: number
  max?: number

  // bool ------------------------------------------------------------------
  /** bool: value shown while the key is absent. */
  defaultValue?: boolean

  // file reference --------------------------------------------------------
  /**
   * Mark the field as a project-relative FILE reference (a path whose content
   * is edited through the files API), opting it into a file control. When set,
   * `accept` is the extension allow-list (e.g. `['.py']`, `['.sql']`) and the
   * optional `baseDir` roots browse/create. When ABSENT, `fileRefForField`
   * falls back to the last-segment heuristic in `codeFields.ts`, so a field
   * whose key is a well-known path segment (`sql_path`, `module_path`, …) still
   * gets a file control without declaring `fileRef` explicitly.
   */
  fileRef?: {
    accept: string[]
    baseDir?: string
    /** Content the "New"/"Create file" affordance seeds (wins over the extension stub). */
    stub?: (raw: Record<string, unknown>) => string
    /** Path proposed when the field is empty and the user clicks New; null = no proposal. */
    suggestPath?: (raw: Record<string, unknown>) => string | null
  }

  // one-of toggle ---------------------------------------------------------
  /**
   * `oneOfToggle`: the mutually-exclusive branches this control switches
   * between. Each option OWNS one YAML `path` (relative to the action mapping)
   * and renders its active control per `backing`. On a switch, the engine
   * prunes every now-inactive branch's `path` (through `applyDiscriminatorChange`
   * with `fieldPath` undefined — no discriminator write, just the prune). The
   * field's own `FieldSpec.path` is unused for the value; the branches own the
   * keys. The `language` (backing `'inline'` → Monaco language) union mirrors
   * `CodeLanguage`, inlined here to keep this vocabulary free of a
   * `codeFields` import (that dependency is one-directional).
   */
  oneOf?: {
    options: {
      /** Branch id — the segment value (e.g. `'inline'` | `'file'` | `'sql'`). */
      value: string
      /** Segment label (e.g. `'Inline SQL'` | `'From file'`). */
      label: string
      /** The YAML key this branch owns, relative to the action mapping. */
      path: YamlPath
      /**
       * How the active branch's control renders. `'fields'` renders a nested
       * set of `fields` through the shared FieldRenderer — for a branch whose
       * key is an OBJECT with sub-fields (snapshot `source_function`:
       * file/function/parameters); the other backings render one control.
       */
      backing: 'inline' | 'file' | 'text' | 'fields'
      /** backing `'inline'`: Monaco language for the code body. */
      language?: 'sql' | 'python' | 'yaml'
      /** backing `'file'`: FileRefField accept allow-list (each with its dot). */
      accept?: string[]
      /**
       * backing `'fields'`: the sub-fields of this branch's object key, each a
       * full FieldSpec with its own (absolute, action-relative) `path`. Rendered
       * nested via the shared FieldRenderer; the branch's own `path` (the object)
       * is what the switch prunes. Ignored for the other backings.
       */
      fields?: FieldSpec[]
      placeholder?: string
    }[]
  }

  // presentation / collections -------------------------------------------
  /** Render values monospace (identifiers, SQL, table names). */
  monospace?: boolean
  /** keyValue/stringList: keep an empty `{}` / `[]` rather than delete the key. */
  allowEmpty?: boolean
  /**
   * objectList: the shape of each row. Each item field's `path` is relative to
   * the row object (e.g. `['name']`); the engine addresses it absolutely at
   * `[...path, rowIndex, ...itemField.path]`.
   */
  itemFields?: FieldSpec[]

  // visibility ------------------------------------------------------------
  /**
   * Render the field only when this predicate over the action's raw mapping
   * holds — used for discriminator-driven (mode-dependent) field sets. Omit
   * = always visible.
   */
  visibleWhen?: (raw: Record<string, unknown>) => boolean
  /**
   * Render the field but DISABLED (read-only control) when this predicate over
   * the action's raw mapping holds — for a key that is visible-but-not-editable
   * in a given state (`apply_as_truncates` under SCD Type 2; `create_table`
   * forced on in snapshot_cdc). The engine disables when `disabled ||
   * disabledWhen?.(raw)`. Additive: a field without it is never force-disabled.
   * The explanatory "why" is carried by a soft `CrossFieldRule` hint.
   */
  disabledWhen?: (raw: Record<string, unknown>) => boolean
}

export interface FieldGroup {
  /** Group heading — a real structural fact ("Source", "CDC configuration"). */
  title?: string
  /** Optional one-liner under the heading. */
  description?: string
  fields: FieldSpec[]
  /**
   * Render the whole group only when this holds (the discriminator switch,
   * e.g. `write_target.mode === 'cdc'`). Omit = always visible.
   */
  visibleWhen?: (raw: Record<string, unknown>) => boolean
  /**
   * Render the group collapsed initially. The shell auto-expands it anyway
   * when `groupHasValueOrIssue` holds (a visible field carries a present value
   * or a computed issue), so a collapsed section never hides set/invalid data.
   */
  collapsed?: boolean
  /**
   * Mark the group as "advanced" — a semantic hint that it holds
   * rarely-touched knobs. Implies collapsed-by-default in the shell.
   */
  advanced?: boolean
}

/**
 * A cross-field soft-validation rule. It produces a NON-BLOCKING hint on the
 * involved visible fields; the write always proceeds.
 *
 * Presence semantics (shared by every rule): a value counts as "present"
 * when it is not `undefined`/`null`, not `''`, not `false`, and not an empty
 * list — so an omitted or falsy key never trips a rule.
 */
export type CrossFieldRule =
  /** Exactly one of `paths` present (e.g. `sql` ⊕ `sql_path`). */
  | { kind: 'xor'; paths: YamlPath[]; message: string }
  /** At most one of `paths` present (e.g. `cluster_columns` ⊕ `cluster_by_auto`). */
  | { kind: 'mutuallyExclusive'; paths: YamlPath[]; message: string }
  /** At least one of `paths` present. */
  | { kind: 'requiredOneOf'; paths: YamlPath[]; message: string }
  /** Arbitrary check; `check` returns the hint message when VIOLATED, else `null`. */
  | {
      kind: 'custom'
      /** Fields the hint attaches to (only the visible ones show it). */
      paths: YamlPath[]
      check: (raw: Record<string, unknown>) => string | null
    }

export interface ActionSubTypeSpec {
  kind: ActionKind
  /**
   * The sub-type discriminator value this spec renders (`source.type` for
   * loads, `transform_type`, `write_target.type`, `test_type`). The registry
   * keys on `(kind, subType)`.
   */
  subType: string
  /** Panel title, e.g. "Auto Loader (cloudFiles)". */
  title: string
  /** One-line description of the sub-type. */
  summary?: string
  groups: FieldGroup[]
  /** Cross-field soft-validation rules (hints only). */
  rules?: CrossFieldRule[]
}
