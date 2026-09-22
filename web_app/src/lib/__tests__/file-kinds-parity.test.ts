import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'

// The backend's `FRONTEND_MIRROR_GLOBS` (services/file_kinds.py) is the
// canonical table of the eight file kinds the editor also schema-validates;
// `SCHEMA_FILE_MATCH` in lib/monaco-setup.ts mirrors it. Both tables are read
// from source text so the comparison is between the literal glob strings each
// side ships (monaco-setup.ts cannot be imported under jsdom: it boots the
// Monaco workers at module load).

const PYTHON_TABLE = resolve(__dirname, '../../../../src/lhp/webapp/services/file_kinds.py')
const TS_TABLE = resolve(__dirname, '../monaco-setup.ts')

// TS key → Python `FileKind` member (lower-cased). Only `project` is spelled
// differently.
const KIND_BY_TS_KEY: Record<string, string> = {
  flowgroup: 'flowgroup',
  preset: 'preset',
  template: 'template',
  substitution: 'substitution',
  project: 'project_config',
  pipeline_config: 'pipeline_config',
  job_config: 'job_config',
  schema: 'schema',
}

function block(source: string, marker: string): string {
  const start = source.indexOf(marker)
  if (start === -1) throw new Error(`marker not found: ${marker}`)
  const end = source.indexOf('\n}', start)
  if (end === -1) throw new Error(`unterminated block after: ${marker}`)
  return source.slice(start, end)
}

/** `FileKind.X: ("a", "b")` entries → { x: ['a', 'b'] }. */
function pythonGlobs(): Record<string, string[]> {
  const table = block(readFileSync(PYTHON_TABLE, 'utf-8'), 'FRONTEND_MIRROR_GLOBS:')
  const out: Record<string, string[]> = {}
  for (const entry of table.matchAll(/FileKind\.([A-Z_]+):\s*\(([^)]*)\)/g)) {
    out[entry[1].toLowerCase()] = [...entry[2].matchAll(/"([^"]+)"/g)].map((m) => m[1])
  }
  return out
}

/** `key: ['a', 'b']` entries → { key: ['a', 'b'] }. */
function tsGlobs(): Record<string, string[]> {
  const table = block(readFileSync(TS_TABLE, 'utf-8'), 'const SCHEMA_FILE_MATCH')
  const out: Record<string, string[]> = {}
  for (const entry of table.matchAll(/^\s*(\w+):\s*\[([^\]]*)\]/gm)) {
    out[entry[1]] = [...entry[2].matchAll(/'([^']+)'/g)].map((m) => m[1])
  }
  return out
}

describe('file-kind glob parity with services/file_kinds.py', () => {
  const py = pythonGlobs()
  const ts = tsGlobs()

  it('both tables list exactly the eight shared kinds', () => {
    expect(Object.keys(ts).sort()).toEqual(Object.keys(KIND_BY_TS_KEY).sort())
    expect(Object.keys(py).sort()).toEqual(Object.values(KIND_BY_TS_KEY).sort())
  })

  it.each(Object.entries(KIND_BY_TS_KEY))(
    'the %s globs are identical on both sides (python kind %s)',
    (tsKey, pyKind) => {
      expect(ts[tsKey].length).toBeGreaterThan(0)
      expect([...ts[tsKey]].sort()).toEqual([...py[pyKind]].sort())
    },
  )

  it('no glob is claimed by two kinds on either side', () => {
    for (const table of [py, ts]) {
      const all = Object.values(table).flat()
      expect(new Set(all).size).toBe(all.length)
    }
  })
})
