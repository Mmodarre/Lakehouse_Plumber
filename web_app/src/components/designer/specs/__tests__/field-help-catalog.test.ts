import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'
import { listActionSpecs } from '../registry'
import type { FieldSpec } from '../types'

type Binding = { path: (string | number)[]; subtype?: string }
type Entry = { id: string; summary: string; bindings: Binding[] }
const catalog = JSON.parse(
  readFileSync(resolve(process.cwd(), '../src/lhp/schemas/help/flowgroup.json'), 'utf8'),
) as { entries: Entry[] }

function paths(fields: readonly FieldSpec[], prefix: (string | number)[] = []): string[][] {
  return fields.flatMap((field) => {
    if (field.widget === 'oneOfToggle' && field.oneOf) {
      return field.oneOf.options.flatMap((option) => [
        [...prefix, ...option.path].map(String),
        ...paths(option.fields ?? [], prefix),
      ])
    }
    return [
      [...prefix, ...field.path].map(String),
      ...paths(field.itemFields ?? [], [...prefix, ...field.path, '*']),
    ]
  })
}

describe('reviewed help covers the actual action forms', () => {
  for (const spec of listActionSpecs()) {
    const subtype = `${spec.kind}:${spec.subType}`
    it(`${subtype} includes every field and nested branch`, () => {
      const bindings = catalog.entries.flatMap((entry) => entry.bindings)
      const missing = spec.groups.flatMap((group) => paths(group.fields)).filter((path) =>
        !bindings.some((binding) => binding.subtype === subtype &&
          JSON.stringify(binding.path) === JSON.stringify(path)),
      )
      expect(missing).toEqual([])
    })
  }
})
