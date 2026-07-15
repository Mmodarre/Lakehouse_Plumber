// ── Pure form model — spec → render inputs + soft hints ──────
//
// The testable core of the ActionForm engine: which groups/fields are
// visible for a given raw action mapping, the soft-validation hints each
// visible field carries (required-missing + cross-field rules), enum option
// augmentation for token values, and value coercion for typed enums. No
// React here.

import { isPlainObject } from '@/lib/config-model'
import type { FlowgroupDocHandle } from '@/lib/flowgroup-doc'
import { deleteActionField, setActionField } from '@/lib/flowgroup-doc'
import { isPresent, isSubstitutionToken, readPath } from './specs/helpers'
import type {
  ActionSubTypeSpec,
  BranchPathMap,
  CrossFieldRule,
  FieldGroup,
  FieldSpec,
  YamlPath,
} from './specs/types'

/**
 * A single synchronous flowgroup-doc mutation applied to a kept parse handle.
 * The write contract shared by the designer form widgets (ActionForm,
 * CodeModal) and the graph compose hook. Structurally identical to
 * `useFlowgroupDoc.FlowgroupMutator` — both are `(doc) => void` over the same
 * comment-preserving handle — so an entity view's `commit` plugs straight in.
 */
export type DesignerMutator = (doc: FlowgroupDocHandle) => void

/** Stable string key for a field path (issue map keys). */
export function pathKey(path: YamlPath): string {
  return JSON.stringify(path)
}

/**
 * The row objects of an `objectList` field at `path`. A non-array (absent key,
 * or a malformed non-list value) yields `[]`; a non-object row normalizes to
 * `{}` so the row still renders (its item fields read as absent). The engine
 * renders one card per element and addresses each item field at
 * `[...path, rowIndex, ...itemFieldPath]`.
 */
export function objectListRows(
  raw: Record<string, unknown>,
  path: YamlPath,
): Record<string, unknown>[] {
  const value = readPath(raw, path)
  if (!Array.isArray(value)) return []
  return value.map((row) => (isPlainObject(row) ? row : {}))
}

/** The two slots exposed by a `dualSource` field (see `FieldWidget`). */
export type DualSourceSlots = [string | undefined, string | undefined]

/**
 * A `dualSource` mutation, expressed in the shell's own set-vs-unset terms so
 * it plugs straight into `setActionField` / `deleteActionField`. `'set'`
 * writes the WHOLE two-item array; `'delete'` prunes the key. This mirrors how
 * stringList/objectList commit (a value write vs a delete-on-clear), just for
 * a fixed pair.
 */
export type DualSourceMutation =
  | { kind: 'set'; value: [string, string] }
  | { kind: 'delete' }

/**
 * The two slots of a `dualSource` field's list at `path`. A non-array (absent
 * key, or a malformed non-list value) yields `[undefined, undefined]`; a
 * length-1 array fills slot 0 only. Only the first two elements are surfaced —
 * a legacy non-2 list (extra elements) reads back its first two without
 * reshaping the file (the "exactly two" soft rule flags the mismatch). A
 * non-string element degrades to `undefined` so the slot stays type-honest.
 */
export function dualSourceSlots(raw: Record<string, unknown>, path: YamlPath): DualSourceSlots {
  const value = readPath(raw, path)
  if (!Array.isArray(value)) return [undefined, undefined]
  const slot = (element: unknown): string | undefined =>
    typeof element === 'string' ? element : undefined
  return [slot(value[0]), slot(value[1])]
}

/**
 * Compose the mutation for setting slot `index` of a `dualSource` pair to
 * `next`. Given the current pair, it writes the WHOLE two-item array (empty
 * slots serialized as `''` to keep the arity pinned at two); when BOTH
 * resulting slots are empty (empty = the model's set-vs-unset notion,
 * `isPresent`) it signals a key delete instead of writing `['', '']`.
 */
export function dualSourceWrite(
  current: DualSourceSlots,
  index: 0 | 1,
  next: string,
): DualSourceMutation {
  const pair: DualSourceSlots = [current[0], current[1]]
  pair[index] = next
  if (!isPresent(pair[0]) && !isPresent(pair[1])) return { kind: 'delete' }
  return { kind: 'set', value: [pair[0] ?? '', pair[1] ?? ''] }
}

/**
 * Commit a discriminator switch, pruning the branch that is no longer active.
 *
 * The designer's `visibleWhen` only HIDES a discriminator's inactive branch;
 * its keys persist in the YAML, and the Python validator then rejects the file
 * (`'quarantine' configuration block is only valid when mode='quarantine'`,
 * the inline⊕file XORs, …). Because the same engine authors NEW actions, the
 * create flow can already emit invalid configs. This mutation fixes both.
 *
 * Given a per-branch-value → owned-paths map (`spec.branchPaths` on the
 * discriminator field), a switch to `newValue`:
 *   1. writes `newValue` at `fieldPath` (skipped when `fieldPath` is undefined
 *      — an inline⊕file toggle with no backing discriminator key of its own);
 *   2. deletes every path owned by a NOW-INACTIVE branch that the `newValue`
 *      branch does not also own (a path shared with `newValue`'s branch is
 *      kept). Deletes are addressed off the action root, matching
 *      `deleteActionField` (a missing path is a safe no-op, so an unset branch
 *      key needs no guard).
 *
 * Takes `doc` + `actionName` directly (both required by the flowgroup-doc
 * mutators) and reuses `setActionField` / `deleteActionField` — no new
 * mutation surface. Composes inside a `DesignerMutator` closure the Task-3
 * modal shell drives (`commit((doc) => applyDiscriminatorChange(doc, name, …))`).
 */
export function applyDiscriminatorChange(
  doc: FlowgroupDocHandle,
  actionName: string,
  fieldPath: YamlPath | undefined,
  newValue: string,
  branchPaths: BranchPathMap,
): void {
  if (fieldPath !== undefined) {
    setActionField(doc, actionName, fieldPath, newValue)
  }
  const kept = new Set((branchPaths[newValue] ?? []).map(pathKey))
  for (const [branchValue, paths] of Object.entries(branchPaths)) {
    if (branchValue === newValue) continue
    for (const path of paths) {
      if (kept.has(pathKey(path))) continue
      deleteActionField(doc, actionName, path)
    }
  }
}

/** Groups whose `visibleWhen` holds for `raw`. */
export function visibleGroups(spec: ActionSubTypeSpec, raw: Record<string, unknown>): FieldGroup[] {
  return spec.groups.filter((group) => group.visibleWhen?.(raw) ?? true)
}

/** Is a field visible (its group must already be visible when calling from a group)? */
export function fieldVisible(field: FieldSpec, raw: Record<string, unknown>): boolean {
  return field.visibleWhen?.(raw) ?? true
}

function visibleFields(spec: ActionSubTypeSpec, raw: Record<string, unknown>): FieldSpec[] {
  return visibleGroups(spec, raw).flatMap((group) =>
    group.fields.filter((field) => fieldVisible(field, raw)),
  )
}

function presentCount(raw: Record<string, unknown>, paths: YamlPath[]): number {
  return paths.filter((path) => isPresent(readPath(raw, path))).length
}

/** The hint a rule produces for `raw`, or null when satisfied. */
function ruleViolation(rule: CrossFieldRule, raw: Record<string, unknown>): string | null {
  switch (rule.kind) {
    case 'xor':
      return presentCount(raw, rule.paths) === 1 ? null : rule.message
    case 'mutuallyExclusive':
      return presentCount(raw, rule.paths) > 1 ? rule.message : null
    case 'requiredOneOf':
      return presentCount(raw, rule.paths) >= 1 ? null : rule.message
    case 'custom':
      return rule.check(raw)
  }
}

/**
 * Soft-validation hints keyed by `pathKey`, for the VISIBLE fields only.
 * Required-missing wins over a cross-field hint on the same field; the write
 * is never blocked regardless.
 */
export function computeIssues(
  spec: ActionSubTypeSpec,
  raw: Record<string, unknown>,
): Map<string, string> {
  const issues = new Map<string, string>()
  const visible = visibleFields(spec, raw)
  const visibleKeys = new Set(visible.map((field) => pathKey(field.path)))

  // A visible `oneOfToggle` OWNS its `oneOf.options[].path`: those branch keys
  // are not themselves visible FieldSpecs, so a cross-field rule keyed by one
  // would otherwise be silently dropped. Map each owned branch path → the
  // toggle's (synthetic) visible path so the hint re-surfaces ON the toggle.
  // Additive: the map is empty when no toggle is visible, so non-toggle specs
  // keep the exact visible-key behavior below.
  const toggleOwner = new Map<string, string>()
  for (const field of visible) {
    if (field.widget === 'oneOfToggle' && field.oneOf) {
      for (const option of field.oneOf.options) {
        toggleOwner.set(pathKey(option.path), pathKey(field.path))
      }
    }
  }

  for (const field of visible) {
    if (field.required && !isPresent(readPath(raw, field.path))) {
      issues.set(pathKey(field.path), `${field.label} is required.`)
    }
  }
  for (const rule of spec.rules ?? []) {
    const message = ruleViolation(rule, raw)
    if (message === null) continue
    for (const path of rule.paths) {
      const key = pathKey(path)
      if (visibleKeys.has(key)) {
        if (!issues.has(key)) issues.set(key, message)
      } else {
        // Re-key to the owning visible oneOfToggle when one owns this branch
        // path; otherwise drop the hint, exactly as before.
        const owner = toggleOwner.get(key)
        if (owner !== undefined && !issues.has(owner)) issues.set(owner, message)
      }
    }
  }
  return issues
}

/**
 * Should a collapsed/advanced group be auto-expanded? True when any of the
 * group's VISIBLE fields carries a present value in `raw` or a computed issue,
 * so a collapsed section never hides data the user set or a hint they need to
 * act on. A group gated off by its own `visibleWhen` is never expanded.
 *
 * Presence reuses the model's own set-vs-unset notion (`isPresent` — the same
 * test `computeIssues` uses for required-missing and cross-field rules), and
 * issues reuse `computeIssues` itself, scoped to just this group. Cross-field
 * rules are spec-level, so a single-group scope surfaces only field-local
 * (required-missing) hints — the write-blocking authority remains `lhp
 * validate`.
 */
export function groupHasValueOrIssue(group: FieldGroup, raw: unknown): boolean {
  if (!isPlainObject(raw)) return false
  if (!(group.visibleWhen?.(raw) ?? true)) return false

  const visible = group.fields.filter((field) => fieldVisible(field, raw))
  if (visible.some((field) => isPresent(readPath(raw, field.path)))) return true

  const issues = computeIssues({ groups: [group] } as ActionSubTypeSpec, raw)
  return visible.some((field) => issues.has(pathKey(field.path)))
}

/**
 * Options for an enum field's current value: when the value is a
 * substitution token outside `options`, it is appended so EnumSelect shows
 * it as a selected custom entry rather than blanking it.
 */
export function enumOptions(field: FieldSpec, current: string | undefined): readonly string[] {
  const options = field.options ?? []
  if (current !== undefined && isSubstitutionToken(current) && !options.includes(current)) {
    return [...options, current]
  }
  return options
}

/** Coerce a committed enum/text value to the field's declared scalar type. */
export function coerceValue(field: FieldSpec, value: string): unknown {
  if (isSubstitutionToken(value)) return value // never coerce a token
  if (field.valueType === 'number' || field.valueType === 'integer') {
    const parsed = Number(value)
    return Number.isFinite(parsed) ? parsed : value
  }
  if (field.valueType === 'boolean') return value === 'true'
  return value
}
