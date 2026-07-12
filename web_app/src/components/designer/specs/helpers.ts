// ── Pure helpers shared by specs + the ActionForm engine ─────
//
// No React / yaml-doc / network here: specs use these to write `visibleWhen`
// predicates and `custom` rule checks; the engine uses them to read values
// and evaluate rules. Kept out of any component/.tsx file for fast-refresh
// purity.

import { isPlainObject } from '@/lib/config-model'
import type { YamlPath } from '@/lib/flowgroup-doc'

// LHP substitution syntaxes: ${token}, ${secret:scope/key}, {{ param }}, %{var}.
const TOKEN_RE = /\$\{[^}]*\}|\{\{[^}]*\}\}|%\{[^}]*\}/

/**
 * True when a value is a string carrying an LHP substitution token. Such a
 * value must round-trip through any widget without coercion or a validation
 * error (a "number" field may legitimately hold `"${max_files}"`).
 */
export function isSubstitutionToken(value: unknown): boolean {
  return typeof value === 'string' && TOKEN_RE.test(value)
}

// Jinja template parameter, e.g. `{{ table_name }}`. Real Jinja2 evaluated
// BEFORE `${env}` substitution when authoring a template under templates/;
// the designer never evaluates it — it just marks the field as bound. This
// pattern is NON-global (safe for repeated `.test()` — no leaking lastIndex);
// name extraction constructs its own global instance per call.
const TEMPLATE_PARAM_RE = /\{\{\s*[^}]*?\s*\}\}/

/** True when a string references at least one `{{ param }}` template token. */
export function hasTemplateParam(value: unknown): boolean {
  return typeof value === 'string' && TEMPLATE_PARAM_RE.test(value)
}

/** True when the whole (trimmed) value is a single `{{ param }}` reference. */
export function isPureTemplateParam(value: unknown): boolean {
  return typeof value === 'string' && /^\{\{\s*[^}]*?\s*\}\}$/.test(value.trim())
}

/** The inner names of every `{{ ... }}` token, in order (blanks dropped). */
export function templateParamNames(value: string): string[] {
  const names: string[] = []
  for (const match of value.matchAll(/\{\{\s*([^}]*?)\s*\}\}/g)) {
    const inner = match[1].trim()
    if (inner !== '') names.push(inner)
  }
  return names
}

/** Read the raw value at `path` within an action mapping (undefined if absent). */
export function readPath(raw: Record<string, unknown>, path: YamlPath): unknown {
  let current: unknown = raw
  for (const segment of path) {
    if (typeof segment === 'number') {
      current = Array.isArray(current) ? current[segment] : undefined
    } else {
      current = isPlainObject(current) ? current[segment] : undefined
    }
    if (current === undefined) return undefined
  }
  return current
}

/**
 * Presence for cross-field rules: `undefined` / `null` / `''` / `false` /
 * `[]` all count as ABSENT, so an omitted or falsy key never trips a rule
 * (e.g. `cluster_by_auto: false` does not conflict with `cluster_columns`).
 */
export function isPresent(value: unknown): boolean {
  if (value === undefined || value === null || value === false || value === '') return false
  if (Array.isArray(value) && value.length === 0) return false
  return true
}

/** Convenience: read a discriminator with its effective default (absent → fallback). */
export function effectiveValue(
  raw: Record<string, unknown>,
  path: YamlPath,
  fallback: string,
): string {
  const value = readPath(raw, path)
  return typeof value === 'string' ? value : fallback
}
