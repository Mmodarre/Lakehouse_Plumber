import { parse } from 'yaml'
import { fetchFileContentWithMeta } from '../../api/files'
import type { ParamDecl } from './ParamsForm'

// ── blueprintParams — declared params for a blueprint definition ─
//
// GET /api/blueprints exposes only a parameter COUNT (webapp/schemas/blueprint
// + api/views.py:BlueprintView), never the declarations. Rather than change
// the backend, the dialog reads the blueprint definition file directly through
// the existing files API and parses its `parameters:` block. Definitions live
// at the conventional `blueprints/<name>.yaml` (verified against the sample
// project); a non-conventional path 404s and the dialog falls back to a manual
// key/value editor.

/** Extract parameter declarations from a blueprint definition's YAML text. */
export function parseBlueprintParams(yamlText: string): ParamDecl[] {
  let doc: unknown
  try {
    doc = parse(yamlText)
  } catch {
    return []
  }
  if (typeof doc !== 'object' || doc === null) return []
  const params = (doc as Record<string, unknown>).parameters
  if (!Array.isArray(params)) return []
  return params
    .filter((p): p is Record<string, unknown> => typeof p === 'object' && p !== null)
    .filter((p) => typeof p.name === 'string')
    .map((p) => ({
      name: p.name as string,
      required: p.required === true,
      default: p.default,
      description: typeof p.description === 'string' ? p.description : undefined,
    }))
}

/** Fetch + parse a blueprint definition's declared parameters (conventional path). */
export async function fetchBlueprintParams(name: string): Promise<ParamDecl[]> {
  const { content } = await fetchFileContentWithMeta(`blueprints/${name}.yaml`)
  return parseBlueprintParams(content)
}
