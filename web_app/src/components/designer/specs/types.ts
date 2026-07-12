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

export interface FieldSpec {
  /** Path relative to the action mapping, e.g. `['source', 'format']`. */
  path: YamlPath
  label: string
  widget: FieldWidget
  /** One-line help under the control. */
  help?: string
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
