import type { ParamDecl } from './ParamsForm'

// ── createFlowgroupSupport — pure helpers for the create dialog ─

/** A value counts as present unless it is undefined/null/'' or an empty list. */
export function isPresent(value: unknown): boolean {
  if (value === undefined || value === null || value === '') return false
  if (Array.isArray(value) && value.length === 0) return false
  return true
}

/** Map the loosely-typed template parameter payload to ParamDecl. */
export function mapTemplateParams(
  raw: readonly Record<string, unknown>[] | undefined,
): ParamDecl[] {
  if (!raw) return []
  return raw
    .filter((p) => typeof p.name === 'string')
    .map((p) => ({
      name: p.name as string,
      type: typeof p.type === 'string' ? p.type : undefined,
      required: p.required === true,
      default: p.default,
      description: typeof p.description === 'string' ? p.description : undefined,
    }))
}

/**
 * A required parameter is unsatisfied only when it has neither an entered
 * value nor a usable declared default — a required param that carries its own
 * default needs no entry (the template/blueprint applies the default).
 */
export function requiredParamsMissing(
  params: ParamDecl[],
  values: Record<string, unknown>,
): boolean {
  return params.some(
    (p) => p.required && !isPresent(values[p.name]) && !isPresent(p.default),
  )
}
