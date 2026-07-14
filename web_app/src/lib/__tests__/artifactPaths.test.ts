import { describe, expect, it } from 'vitest'
import type { FileNode } from '../../types/api'
import {
  collectFilePaths,
  generatedPythonCandidates,
  resolveArtifactPaths,
  type RelatedArtifact,
} from '../artifactPaths'

const SOURCE = 'pipelines/01_bronze/bronze_customers.yaml'
const PIPELINE = 'bronze_ingest'
const FLOWGROUP = 'bronze_customers'

function tree(paths: string[]): FileNode {
  // Minimal tree: a root directory whose children are flat file nodes. The
  // resolver only reads `type`/`path`, so nesting depth is irrelevant.
  return {
    name: '',
    path: '',
    type: 'directory',
    children: paths.map((p) => ({ name: p.split('/').pop()!, path: p, type: 'file' as const })),
  }
}

describe('collectFilePaths', () => {
  it('flattens the tree to file paths only, skipping directories', () => {
    const node: FileNode = {
      name: '',
      path: '',
      type: 'directory',
      children: [
        {
          name: 'pipelines',
          path: 'pipelines',
          type: 'directory',
          children: [{ name: 'a.yaml', path: 'pipelines/a.yaml', type: 'file' }],
        },
        { name: 'lhp.yaml', path: 'lhp.yaml', type: 'file' },
      ],
    }
    expect(collectFilePaths(node)).toEqual(['pipelines/a.yaml', 'lhp.yaml'])
  })

  it('returns [] for an absent tree', () => {
    expect(collectFilePaths(undefined)).toEqual([])
    expect(collectFilePaths(null)).toEqual([])
  })
})

describe('generatedPythonCandidates', () => {
  it('prefers the per-flowgroup file, then the pipeline runner', () => {
    expect(generatedPythonCandidates('dev', PIPELINE, FLOWGROUP)).toEqual([
      'generated/dev/bronze_ingest/bronze_customers.py',
      'generated/dev/bronze_ingest/bronze_ingest_runner.py',
    ])
  })

  it('scopes to the active env', () => {
    expect(generatedPythonCandidates('prod', PIPELINE, FLOWGROUP)[0]).toBe(
      'generated/prod/bronze_ingest/bronze_customers.py',
    )
  })

  it('returns [] when there is no pipeline or env (e.g. a template)', () => {
    expect(generatedPythonCandidates('dev', '', FLOWGROUP)).toEqual([])
    expect(generatedPythonCandidates('', PIPELINE, FLOWGROUP)).toEqual([])
  })

  it('drops the per-flowgroup candidate when there is no flowgroup name', () => {
    expect(generatedPythonCandidates('dev', PIPELINE, '')).toEqual([
      'generated/dev/bronze_ingest/bronze_ingest_runner.py',
    ])
  })
})

describe('resolveArtifactPaths', () => {
  const base = { pipeline: PIPELINE, flowgroup: FLOWGROUP, sourceFilePath: SOURCE, env: 'dev' }

  it('always includes the source yaml first, editable, no chip', () => {
    const [first] = resolveArtifactPaths(base, [])
    expect(first).toMatchObject({
      path: SOURCE,
      label: 'bronze_customers.yaml',
      kind: 'source-yaml',
      editable: true,
      chip: null,
      language: 'yaml',
    })
  })

  it('resolves the per-flowgroup generated python when it exists', () => {
    const py = 'generated/dev/bronze_ingest/bronze_customers.py'
    const refs = resolveArtifactPaths(base, tree([SOURCE, py]).children!.map((c) => c.path))
    const python = refs.find((r) => r.kind === 'generated-python')
    expect(python).toMatchObject({
      path: py,
      label: 'bronze_customers.py',
      editable: false,
      chip: 'generated · read-only',
      language: 'python',
    })
  })

  it('falls back to the pipeline runner when the per-flowgroup file is absent', () => {
    const runner = 'generated/dev/bronze_ingest/bronze_ingest_runner.py'
    const refs = resolveArtifactPaths(base, [SOURCE, runner])
    const python = refs.find((r) => r.kind === 'generated-python')
    expect(python?.path).toBe(runner)
    expect(python?.label).toBe('bronze_ingest_runner.py')
  })

  it('prefers the per-flowgroup file over the runner when both exist', () => {
    const py = 'generated/dev/bronze_ingest/bronze_customers.py'
    const runner = 'generated/dev/bronze_ingest/bronze_ingest_runner.py'
    const refs = resolveArtifactPaths(base, [SOURCE, runner, py])
    expect(refs.find((r) => r.kind === 'generated-python')?.path).toBe(py)
  })

  it('omits generated python entirely when neither candidate exists', () => {
    const refs = resolveArtifactPaths(base, [SOURCE])
    expect(refs.some((r) => r.kind === 'generated-python')).toBe(false)
  })

  it('scopes generated python to the active env', () => {
    const devPy = 'generated/dev/bronze_ingest/bronze_customers.py'
    const refs = resolveArtifactPaths({ ...base, env: 'prod' }, [SOURCE, devPy])
    // The dev python must not surface when env is prod.
    expect(refs.some((r) => r.kind === 'generated-python')).toBe(false)
  })

  it('includes referenced sql/schema that exist, marked read-only', () => {
    const sql = 'sql/brz/bronze_customers.sql'
    const schema = 'schemas/bronze_customers.yaml'
    const related: RelatedArtifact[] = [
      { path: sql, category: 'sql' },
      { path: schema, category: 'schema' },
    ]
    const refs = resolveArtifactPaths({ ...base, related }, [SOURCE, sql, schema])
    const sqlRef = refs.find((r) => r.kind === 'source-sql')
    const schemaRef = refs.find((r) => r.kind === 'source-schema')
    expect(sqlRef).toMatchObject({ path: sql, editable: false, chip: 'read-only', language: 'sql' })
    expect(schemaRef).toMatchObject({ path: schema, editable: false, chip: 'read-only' })
  })

  it('excludes referenced sql/schema that do not exist on disk', () => {
    const related: RelatedArtifact[] = [{ path: 'sql/missing.sql', category: 'sql' }]
    const refs = resolveArtifactPaths({ ...base, related }, [SOURCE])
    expect(refs.some((r) => r.kind === 'source-sql')).toBe(false)
  })

  it('includes referenced python/expectations that exist, marked read-only', () => {
    const py = 'py_functions/helper.py'
    const rules = 'expectations/generic_quality.json'
    const related: RelatedArtifact[] = [
      { path: py, category: 'python' },
      { path: rules, category: 'expectations' },
    ]
    const refs = resolveArtifactPaths({ ...base, related }, [SOURCE, py, rules])
    const pyRef = refs.find((r) => r.kind === 'source-python')
    const expRef = refs.find((r) => r.kind === 'source-expectations')
    expect(pyRef).toMatchObject({
      path: py,
      label: 'helper.py',
      editable: false,
      chip: 'read-only',
      language: 'python',
    })
    expect(expRef).toMatchObject({
      path: rules,
      label: 'generic_quality.json',
      editable: false,
      chip: 'read-only',
      language: 'json',
    })
  })

  it('excludes referenced python/expectations that do not exist on disk', () => {
    const related: RelatedArtifact[] = [
      { path: 'py_functions/missing.py', category: 'python' },
      { path: 'expectations/missing.json', category: 'expectations' },
    ]
    const refs = resolveArtifactPaths({ ...base, related }, [SOURCE])
    expect(refs.some((r) => r.kind === 'source-python')).toBe(false)
    expect(refs.some((r) => r.kind === 'source-expectations')).toBe(false)
  })

  it('ignores unknown related categories', () => {
    const related: RelatedArtifact[] = [{ path: 'notes/readme.md', category: 'docs' }]
    const refs = resolveArtifactPaths({ ...base, related }, [SOURCE, 'notes/readme.md'])
    expect(refs).toHaveLength(1)
    expect(refs[0].kind).toBe('source-yaml')
  })

  it('does not duplicate an artifact whose path repeats across inputs', () => {
    // A related entry that points at the source yaml must not create a 2nd tab.
    const related: RelatedArtifact[] = [{ path: SOURCE, category: 'schema' }]
    const refs = resolveArtifactPaths({ ...base, related }, [SOURCE])
    expect(refs.filter((r) => r.path === SOURCE)).toHaveLength(1)
  })

  it('drops generated python for a template (no pipeline)', () => {
    const refs = resolveArtifactPaths(
      { pipeline: '', flowgroup: 'my_template', sourceFilePath: 'templates/t.yaml', env: 'dev' },
      ['templates/t.yaml', 'generated/dev//my_template.py'],
    )
    expect(refs).toHaveLength(1)
    expect(refs[0].kind).toBe('source-yaml')
  })
})
