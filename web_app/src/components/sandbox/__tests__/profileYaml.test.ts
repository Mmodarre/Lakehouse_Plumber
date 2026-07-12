import { describe, expect, it } from 'vitest'
import { parse } from 'yaml'
import { assembleProfileYaml, normalizePipelines } from '../profileYaml'

// The assembled text must parse to the shape the loader expects: a top-level
// `sandbox:` mapping with a scalar `namespace` and a `pipelines` list
// (src/lhp/models/_sandbox.py SandboxProfile; the loader's own example).

describe('assembleProfileYaml — fresh file', () => {
  it('produces a top-level sandbox mapping with namespace + pipelines', () => {
    const yaml = assembleProfileYaml({
      namespace: 'alice',
      pipelines: ['acmi_edw_bronze', 'acmi_edw_silver'],
    })
    expect(parse(yaml)).toEqual({
      sandbox: { namespace: 'alice', pipelines: ['acmi_edw_bronze', 'acmi_edw_silver'] },
    })
  })

  it('quotes a leading-glob pattern so it round-trips as a string, not an alias', () => {
    const yaml = assembleProfileYaml({ namespace: 'bob', pipelines: ['*_bronze', 'acmi_edw_silv*'] })
    expect(parse(yaml)).toEqual({
      sandbox: { namespace: 'bob', pipelines: ['*_bronze', 'acmi_edw_silv*'] },
    })
  })
})

describe('assembleProfileYaml — editing an existing file', () => {
  it('preserves a file header comment while updating the scope', () => {
    const existing = `# my personal sandbox — do not commit
sandbox:
  namespace: old_ns
  pipelines:
    - old_pipeline
`
    const yaml = assembleProfileYaml(
      { namespace: 'alice', pipelines: ['acmi_edw_bronze'] },
      existing,
    )
    expect(yaml).toContain('# my personal sandbox — do not commit')
    expect(parse(yaml)).toEqual({
      sandbox: { namespace: 'alice', pipelines: ['acmi_edw_bronze'] },
    })
  })

  it('falls back to a fresh document when the existing source cannot be parsed', () => {
    const broken = 'sandbox: : : not valid yaml\n  - ['
    const yaml = assembleProfileYaml({ namespace: 'alice', pipelines: ['p1'] }, broken)
    expect(parse(yaml)).toEqual({ sandbox: { namespace: 'alice', pipelines: ['p1'] } })
  })

  it('treats an empty existing source as fresh', () => {
    const yaml = assembleProfileYaml({ namespace: 'alice', pipelines: ['p1'] }, '   ')
    expect(parse(yaml)).toEqual({ sandbox: { namespace: 'alice', pipelines: ['p1'] } })
  })
})

describe('normalizePipelines', () => {
  it('trims, drops blanks, and dedupes preserving first-seen order', () => {
    expect(normalizePipelines(['  b ', 'a', '', 'a', 'c', '  '])).toEqual(['b', 'a', 'c'])
  })
})
