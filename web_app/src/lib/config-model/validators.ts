/**
 * Shared validation vocabulary for the config-model layer.
 *
 * The per-surface modules (pipelineConfig / jobConfig / projectConfig) build
 * `ValidationIssue`s that mirror the Python loaders' behavior exactly: an
 * `error` is emitted only where the loader raises (Save must be blocked,
 * because the CLI would reject the file), a `warning` only where the loader
 * logs-and-continues or where the UI needs a non-blocking signal.
 *
 * Helpers here are generic; every surface-specific rule lives in the surface
 * module next to a citation of the loader line it mirrors.
 */

/** One validation finding, addressed to a document + path for form display. */
export interface ValidationIssue {
  /** Index of the YAML document the issue belongs to (0 for single-doc files). */
  docIndex: number
  /** Path to the offending node within the document ([] = whole document). */
  path: (string | number)[]
  severity: 'error' | 'warning'
  message: string
  /** LHP error code when the rule mirrors a named loader error (e.g. VAL_006). */
  code?: string
}

/** Construct an error issue. */
export function errorIssue(
  docIndex: number,
  path: (string | number)[],
  message: string,
  code?: string,
): ValidationIssue {
  return code === undefined
    ? { docIndex, path, severity: 'error', message }
    : { docIndex, path, severity: 'error', message, code }
}

/** Construct a warning issue. */
export function warningIssue(
  docIndex: number,
  path: (string | number)[],
  message: string,
  code?: string,
): ValidationIssue {
  return code === undefined
    ? { docIndex, path, severity: 'warning', message }
    : { docIndex, path, severity: 'warning', message, code }
}

/**
 * Is `value` a plain mapping (what a YAML block/flow map parses to via
 * `toJS`)? Mirrors Python-side `isinstance(x, dict)` checks.
 */
export function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

/** Is `value` an array whose every element is a string? */
export function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === 'string')
}

/**
 * Is `value` an integer in the Python sense — `isinstance(x, int)` with the
 * explicit bool rejection the loaders use (bool is an int subclass there)?
 * YAML integers arrive as JS numbers; a float like 10.5 is rejected.
 */
export function isStrictInt(value: unknown): value is number {
  return typeof value === 'number' && Number.isInteger(value)
}

/** Result of an integer range check (bounds inclusive). */
export type IntRangeResult = 'ok' | 'not-int' | 'out-of-range'

/** Check `value` is an integer (never a boolean) within `[min, max]`. */
export function intRangeCheck(value: unknown, min: number, max: number): IntRangeResult {
  if (typeof value === 'boolean' || !isStrictInt(value)) return 'not-int'
  return value >= min && value <= max ? 'ok' : 'out-of-range'
}

/** Is `value` one of `allowed`? (Exact, case-sensitive — like the loaders.) */
export function isEnumMember(value: unknown, allowed: readonly string[]): boolean {
  return typeof value === 'string' && allowed.includes(value)
}

/** Keys of `map` whose values are not strings (str→str map validation). */
export function nonStringValueKeys(map: Record<string, unknown>): string[] {
  return Object.keys(map).filter((key) => typeof map[key] !== 'string')
}

/**
 * Which of `keys` are PRESENT on `obj` (presence, not truthiness — mirrors
 * Python `k in entry`). Used for exactly-one-of checks.
 */
export function presentKeys(obj: Record<string, unknown>, keys: readonly string[]): string[] {
  return keys.filter((key) => key in obj)
}

/**
 * Interpret a value the way Pydantic v2 lax mode coerces `bool` fields
 * (models validated from parsed YAML: real booleans, 0/1 numbers, and the
 * usual true/false word forms). Returns `undefined` for values Pydantic
 * would reject — callers decide whether that is an error on their surface.
 */
export function parseLaxBool(value: unknown): boolean | undefined {
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') {
    if (value === 0) return false
    if (value === 1) return true
    return undefined
  }
  if (typeof value === 'string') {
    const lower = value.trim().toLowerCase()
    if (['true', 'yes', 'on', 'y', 't', '1'].includes(lower)) return true
    if (['false', 'no', 'off', 'n', 'f', '0'].includes(lower)) return false
  }
  return undefined
}
