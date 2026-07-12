// ── Pure form model — spec → render inputs + soft hints ──────
//
// The testable core of the ActionForm engine: which groups/fields are
// visible for a given raw action mapping, the soft-validation hints each
// visible field carries (required-missing + cross-field rules), enum option
// augmentation for token values, and value coercion for typed enums. No
// React here.

import { isPlainObject } from '@/lib/config-model'
import { isPresent, isSubstitutionToken, readPath } from './specs/helpers'
import type { ActionSubTypeSpec, CrossFieldRule, FieldGroup, FieldSpec, YamlPath } from './specs/types'

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
      if (visibleKeys.has(key) && !issues.has(key)) issues.set(key, message)
    }
  }
  return issues
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
