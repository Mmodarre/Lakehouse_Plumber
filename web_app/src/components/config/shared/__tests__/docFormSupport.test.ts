import { describe, expect, it } from 'vitest'
import type { ValidationIssue } from '../../../../lib/config-model'
import type { ConfigFileHandle } from '../../../../lib/yaml-doc'
import { parseConfigFile, serializeConfigFile } from '../../../../lib/yaml-doc'
import { bindDocApi, countErrors, snapshotDocs } from '../docFormSupport'
import type { ConfigDocSource } from '../docFormSupport'

// ── bindDocApi over a ConfigDocSource ────────────────────────
//
// After the config re-host, bindDocApi routes writes through a
// ConfigDocSource.mutate funnel (ConfigFormView backs it with
// documentStore.mutate) instead of useConfigFile. Here mutate runs the op
// straight on a real handle — the same contract documentStore honours — so
// these tests pin the surgical write behaviour independent of React.

function sourceFor(content: string): {
  source: ConfigDocSource
  handle: ConfigFileHandle
  text: () => string
} {
  const handle = parseConfigFile(content)
  const source: ConfigDocSource = {
    path: 'x.yaml',
    isLoading: false,
    loadError: null,
    handle,
    errors: handle.errors,
    version: 0,
    mutate: (fn) => {
      // Mirror documentStore.mutate's gate: refuse on parse errors.
      if (handle.errors.length > 0) return
      fn(handle)
    },
  }
  return { source, handle, text: () => serializeConfigFile(handle) }
}

describe('bindDocApi over a ConfigDocSource', () => {
  it('set routes through mutate and preserves untouched bytes/comments', () => {
    const { source, text } = sourceFor('# top\nproject_defaults:\n  catalog: main # keep\n')
    const api = bindDocApi(source, 0, ['project_defaults'], { catalog: 'main' }, [])
    api.set(['schema'], 'raw')
    expect(text()).toBe('# top\nproject_defaults:\n  catalog: main # keep\n  schema: raw\n')
  })

  it('set replaces a bare-null section with a map spine', () => {
    const { source, text } = sourceFor('project_defaults:\n')
    const api = bindDocApi(source, 0, ['project_defaults'], {}, [])
    api.set(['catalog'], 'main')
    expect(text()).toBe('project_defaults:\n  catalog: main\n')
  })

  it('del removes exactly the key (pristine absence, no residue)', () => {
    const { source, text } = sourceFor('project_defaults:\n  catalog: main\n  schema: raw\n')
    const api = bindDocApi(source, 0, ['project_defaults'], { catalog: 'main', schema: 'raw' }, [])
    api.del(['schema'])
    expect(text()).toBe('project_defaults:\n  catalog: main\n')
  })

  it('issueAt surfaces the worst issue at exactly base+rel (error outranks warning)', () => {
    const { source } = sourceFor('project_defaults:\n  catalog: main\n')
    const issues: ValidationIssue[] = [
      { docIndex: 0, path: ['project_defaults', 'catalog'], message: 'a warning', severity: 'warning' },
      { docIndex: 0, path: ['project_defaults', 'catalog'], message: 'an error', severity: 'error' },
    ]
    const api = bindDocApi(source, 0, ['project_defaults'], { catalog: 'main' }, issues)
    expect(api.issueAt(['catalog'])).toEqual({ message: 'an error', severity: 'error' })
    expect(api.issueAt(['schema'])).toBeUndefined()
  })

  it('snapshotDocs returns [] on a degraded (parse-error) source', () => {
    const { source } = sourceFor('foo: [1, 2\n')
    expect(source.errors.length).toBeGreaterThan(0)
    expect(snapshotDocs(source)).toEqual([])
  })

  it('snapshotDocs returns one snapshot per document, in file order', () => {
    const { source } = sourceFor('project_defaults:\n  catalog: main\n---\npipeline: bronze\n')
    const docs = snapshotDocs(source)
    expect(docs).toHaveLength(2)
    expect(docs[0]).toEqual({ project_defaults: { catalog: 'main' } })
    expect(docs[1]).toEqual({ pipeline: 'bronze' })
  })

  it('countErrors counts only error-severity issues', () => {
    const issues: ValidationIssue[] = [
      { docIndex: 0, path: [], message: 'e', severity: 'error' },
      { docIndex: 0, path: [], message: 'w', severity: 'warning' },
      { docIndex: 0, path: [], message: 'e2', severity: 'error' },
    ]
    expect(countErrors(issues)).toBe(2)
  })
})
