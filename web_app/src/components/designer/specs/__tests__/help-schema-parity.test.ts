// Drift guard (permanent): every designer FieldSpec.path must resolve to a
// non-empty `description` in the packaged flowgroup schema, so the schema
// (Monaco hover + the designer "(i)" tooltips) and the designer forms cannot
// drift. The schema is now the SOLE source of field help — the designer specs
// no longer carry `help` strings; this test guarantees every path is covered.

import { describe, expect, it } from 'vitest'
import { readFileSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { buildSchemaHelpResolver, type SchemaPath } from '@/lib/schema-help'
import { listActionSpecs } from '../registry'
import type { FieldSpec } from '../types'

/**
 * Load the REAL packaged flowgroup schema from disk — it lives in the Python
 * package (`src/lhp/schemas/`), not this JS package, so it is not importable.
 * Vitest's cwd is `web_app/`, so `../src/lhp/schemas/...` is the repo's schema;
 * an `import.meta.url`-relative walk is the fallback if the cwd is unexpected.
 */
function loadFlowgroupSchema(): Record<string, unknown> {
  const here = dirname(fileURLToPath(import.meta.url))
  const candidates = [
    resolve(process.cwd(), '../src/lhp/schemas/flowgroup.schema.json'),
    // __tests__ → specs → designer → components → src → web_app → repo root
    resolve(here, '../../../../../../src/lhp/schemas/flowgroup.schema.json'),
  ]
  const errors: string[] = []
  for (const path of candidates) {
    try {
      return JSON.parse(readFileSync(path, 'utf8')) as Record<string, unknown>
    } catch (err) {
      errors.push(`${path}: ${(err as Error).message}`)
    }
  }
  throw new Error(`flowgroup.schema.json not found. Tried:\n${errors.join('\n')}`)
}

interface Leaf {
  /** `${kind}/${subType}` — the owning spec, for the gap report. */
  spec: string
  label: string
  /** Resolver path from the Action root. */
  path: SchemaPath
}

/**
 * Enumerate EVERY FieldSpec across all registered specs, including nested
 * objectList `itemFields`. An itemField's resolver path is the parent
 * objectList path + a numeric index + the item field's own path: the resolver
 * only steps into an array's `items` for a NUMERIC segment (a bare string
 * segment does not descend), so the index level is REQUIRED — this mirrors
 * ActionForm's runtime `[...field.path, rowIndex, ...itemField.path]`.
 */
function collectLeaves(): Leaf[] {
  const leaves: Leaf[] = []
  const walk = (specKey: string, fields: readonly FieldSpec[], prefix: readonly (string | number)[]) => {
    for (const field of fields) {
      const path = [...prefix, ...field.path]
      leaves.push({ spec: specKey, label: field.label, path })
      if (field.itemFields && field.itemFields.length > 0) {
        walk(specKey, field.itemFields, [...path, 0])
      }
    }
  }
  for (const spec of listActionSpecs()) {
    const specKey = `${spec.kind}/${spec.subType}`
    for (const group of spec.groups) walk(specKey, group.fields, [])
  }
  return leaves
}

describe('help ↔ schema parity (flowgroup Action)', () => {
  it('every designer FieldSpec.path resolves to a non-empty schema description', () => {
    const schema = loadFlowgroupSchema()
    const resolver = buildSchemaHelpResolver(schema, '#/definitions/Action')

    const unresolved = collectLeaves().filter((leaf) => {
      const description = resolver(leaf.path)
      return typeof description !== 'string' || description.trim() === ''
    })

    const report = unresolved
      .map((leaf) => `${leaf.spec}  ${JSON.stringify(leaf.path)}`)
      .join('\n')

    expect(
      unresolved,
      `FieldSpec paths with no schema description:\n${report}`,
    ).toEqual([])
  })
})
