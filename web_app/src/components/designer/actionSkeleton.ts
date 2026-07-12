// ── actionSkeleton — minimal valid action objects from a spec ─
//
// Pure builders that turn a (kind, subType) choice from the palette into a
// new action mapping ready for flowgroup-doc's `addAction`. No React, no
// yaml-doc, no network — fully unit-testable.
//
// A skeleton is "minimal valid" for its sub-type: it carries the sub-type
// DISCRIMINATOR the CLI dispatches on (action_dispatch.py:364-379 —
// load→`source.type`, transform→`transform_type`, write→`write_target.type`,
// test→`test_type`) plus every ALWAYS-VISIBLE required field the spec
// declares, with empty/placeholder values the user then fills. Mode-gated
// groups (a write's cdc / snapshot_cdc fields) stay out: their `visibleWhen`
// is evaluated against the skeleton, whose mode is absent (→ 'standard').
//
// Producers (load / transform) additionally get a derived `target` of
// `v_<name>` rather than an empty string: unique per action name, it avoids
// phantom empty-view collisions in the graph and lets the per-node "+" wire a
// downstream `source` immediately.

import type { ActionKind, FieldSpec, YamlPath } from './specs/types'
import { getActionSpec } from './specs/registry'
import { fieldVisible, visibleGroups } from './formModel'

/** A deterministic unique name `<base>_<n>` not colliding with `existing`. */
export function uniqueActionName(base: string, existing: Iterable<string>): string {
  const taken = new Set(existing)
  for (let n = 1; ; n++) {
    const candidate = `${base}_${n}`
    if (!taken.has(candidate)) return candidate
  }
}

/** The placeholder value a required field is seeded with, by widget. `undefined`
 * skips the field (e.g. numbers, which are rarely required and read cleaner
 * absent). */
function placeholderForField(field: FieldSpec): unknown {
  switch (field.widget) {
    case 'stringList':
      return []
    case 'keyValue':
      return {}
    case 'objectList':
      return []
    case 'bool':
      return field.defaultValue ?? false
    case 'enum':
      return field.enumDefault ?? field.options?.[0] ?? ''
    case 'number':
      return undefined
    default:
      // text | textarea | stringOrList → an empty string is "present but
      // empty": the form shows it with a soft required hint.
      return ''
  }
}

/** Set `value` at a string `path` inside `obj`, creating intermediate maps
 * and merging into ones the discriminator already placed (e.g. `write_target`). */
function setAtPath(obj: Record<string, unknown>, path: YamlPath, value: unknown): void {
  let cursor = obj
  for (let i = 0; i < path.length - 1; i++) {
    const segment = String(path[i])
    const next = cursor[segment]
    if (typeof next !== 'object' || next === null || Array.isArray(next)) {
      cursor[segment] = {}
    }
    cursor = cursor[segment] as Record<string, unknown>
  }
  cursor[String(path[path.length - 1])] = value
}

/** True when the spec declares a field at exactly `['target']`. */
function producesTarget(kind: ActionKind, spec: { groups: { fields: FieldSpec[] }[] }): boolean {
  if (kind !== 'load' && kind !== 'transform') return false
  return spec.groups.some((g) =>
    g.fields.some((f) => f.path.length === 1 && f.path[0] === 'target'),
  )
}

/**
 * The shape of a spec's primary `source` field (path exactly `['source']`):
 * `'string'` for a text / stringOrList ref, `'list'` for a stringList ref,
 * `null` when there is none (loads read a structured `source` mapping, so
 * they take no upstream view). Drives pre-wiring and the fan-in affordance.
 */
export function sourceFieldShape(kind: ActionKind, subType: string): 'string' | 'list' | null {
  const spec = getActionSpec(kind, subType)
  if (spec === undefined) return null
  for (const group of spec.groups) {
    for (const field of group.fields) {
      if (field.path.length === 1 && field.path[0] === 'source') {
        if (field.widget === 'stringList') return 'list'
        if (field.widget === 'text' || field.widget === 'stringOrList') return 'string'
        return null
      }
    }
  }
  return null
}

/**
 * A minimal valid action mapping for `(kind, subType)`, named uniquely against
 * `existing`. Missing spec (a not-yet-built sub-type) still yields a
 * discriminator-only skeleton so the action round-trips and renders.
 */
export function buildActionSkeleton(
  kind: ActionKind,
  subType: string,
  existing: Iterable<string>,
): Record<string, unknown> {
  const name = uniqueActionName(subType, existing)
  const action: Record<string, unknown> = { name, type: kind }

  switch (kind) {
    case 'load':
      action.source = { type: subType }
      break
    case 'transform':
      action.transform_type = subType
      break
    case 'write':
      action.write_target = { type: subType }
      break
    case 'test':
      action.test_type = subType
      break
  }

  const spec = getActionSpec(kind, subType)
  if (spec !== undefined) {
    for (const group of visibleGroups(spec, action)) {
      for (const field of group.fields) {
        if (!field.required || !fieldVisible(field, action)) continue
        const value = placeholderForField(field)
        if (value === undefined) continue
        setAtPath(action, field.path, value)
      }
    }
    if (producesTarget(kind, spec)) action.target = `v_${name}`
  }

  return action
}

/**
 * Pre-wire a freshly-built downstream action's `source` to `upstreamView`
 * (the view the clicked "+" node publishes), so the graph edge appears on
 * insert. String-source sub-types get the bare view; list-source ones get a
 * single-element list; structured-source loads are left unwired.
 */
export function prewireSource(
  action: Record<string, unknown>,
  kind: ActionKind,
  subType: string,
  upstreamView: string,
): void {
  const shape = sourceFieldShape(kind, subType)
  if (shape === 'string') action.source = upstreamView
  else if (shape === 'list') action.source = [upstreamView]
}

/**
 * The action object to insert from a palette choice: a skeleton, pre-wired to
 * `upstreamView` when inserting downstream of a producing node.
 */
export function buildInsertion(
  kind: ActionKind,
  subType: string,
  opts: { existing: Iterable<string>; upstreamView?: string },
): Record<string, unknown> {
  const action = buildActionSkeleton(kind, subType, opts.existing)
  if (opts.upstreamView !== undefined && opts.upstreamView !== '') {
    prewireSource(action, kind, subType, opts.upstreamView)
  }
  return action
}
