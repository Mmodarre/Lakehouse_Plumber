import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import type { ActionSubTypeSpec, CrossFieldRule, FieldGroup, FieldSpec } from '../types'
import type { YamlPath } from '@/lib/flowgroup-doc'
import { loadCloudfilesSpec } from '../load-cloudfiles'
import { loadDeltaSpec } from '../load-delta'
import { loadSqlSpec } from '../load-sql'
import { loadPythonSpec } from '../load-python'
import { loadJdbcSpec } from '../load-jdbc'
import { loadKafkaSpec } from '../load-kafka'
import { loadCustomDatasourceSpec } from '../load-custom-datasource'

// ── spec-level helpers ───────────────────────────────────────

const key = (path: YamlPath) => JSON.stringify(path)

function findField(spec: ActionSubTypeSpec, path: YamlPath): FieldSpec | undefined {
  return spec.groups.flatMap((g) => g.fields).find((f) => key(f.path) === key(path))
}

function groupOf(spec: ActionSubTypeSpec, path: YamlPath): FieldGroup | undefined {
  return spec.groups.find((g) => g.fields.some((f) => key(f.path) === key(path)))
}

function customRules(spec: ActionSubTypeSpec): Extract<CrossFieldRule, { kind: 'custom' }>[] {
  return (spec.rules ?? []).filter((r): r is Extract<CrossFieldRule, { kind: 'custom' }> => r.kind === 'custom')
}

const CLOUDFILES_FORMATS = ['json', 'csv', 'parquet', 'avro', 'orc', 'text', 'binaryfile', 'xml']

// ── load/cloudfiles — the biggest delta (gap closing) ────────

describe('load/cloudfiles spec — gap closing', () => {
  it('format is a required enum over the 8 supported formats, seeded json, with no enumDefault/unsetLabel', () => {
    const format = findField(loadCloudfilesSpec, ['source', 'format'])!
    expect(format.widget).toBe('enum')
    expect(format.options).toEqual(CLOUDFILES_FORMATS)
    expect(format.options?.[0]).toBe('json') // skeleton seeds options[0]
    expect(format.required).toBe(true)
    expect(format.enumDefault).toBeUndefined()
    expect(format.unsetLabel).toBeUndefined()
  })

  it('keeps source.path with its "Path" label (GraphView asserts it) and stream-only source.readMode', () => {
    const path = findField(loadCloudfilesSpec, ['source', 'path'])!
    expect(path.label).toBe('Path')
    const readMode = findField(loadCloudfilesSpec, ['source', 'readMode'])!
    expect(readMode.widget).toBe('enum')
    expect(readMode.options).toEqual(['stream'])
  })

  it('source.schema and source.schema_file are fileRefs accepting schema extensions', () => {
    const accept = ['.json', '.yaml', '.yml', '.ddl', '.sql']
    expect(findField(loadCloudfilesSpec, ['source', 'schema'])!.fileRef?.accept).toEqual(accept)
    expect(findField(loadCloudfilesSpec, ['source', 'schema_file'])!.fileRef?.accept).toEqual(accept)
  })

  it('adds the gap fields with the right widgets/defaults', () => {
    const loc = findField(loadCloudfilesSpec, ['source', 'schema_location'])!
    expect(loc.widget).toBe('text')

    const infer = findField(loadCloudfilesSpec, ['source', 'schema_infer_column_types'])!
    expect(infer.widget).toBe('bool')
    expect(infer.defaultValue).toBe(false)

    const maxFiles = findField(loadCloudfilesSpec, ['source', 'max_files_per_trigger'])!
    expect(maxFiles.widget).toBe('number')
    expect(maxFiles.min).toBe(1)

    const evolution = findField(loadCloudfilesSpec, ['source', 'schema_evolution_mode'])!
    expect(evolution.widget).toBe('enum')
    expect(evolution.options).toEqual(['addNewColumns', 'rescue', 'failOnNewColumns'])
    expect(evolution.unsetLabel).toBe('Not set (default)')

    const rescue = findField(loadCloudfilesSpec, ['source', 'rescue_data_column'])!
    expect(rescue.widget).toBe('text')
    expect(rescue.placeholder).toBe('_rescued_data')
  })

  it('puts max_files_per_trigger in an advanced (collapsed) group', () => {
    const group = groupOf(loadCloudfilesSpec, ['source', 'max_files_per_trigger'])!
    expect(group.advanced).toBe(true)
  })

  it('extends the schema mutual-exclusion rule to include options.cloudFiles.schemaHints', () => {
    const rule = (loadCloudfilesSpec.rules ?? []).find((r) => r.kind === 'mutuallyExclusive')
    expect(rule).toBeDefined()
    const paths = (rule as Extract<CrossFieldRule, { kind: 'mutuallyExclusive' }>).paths.map(key)
    expect(paths).toContain(key(['source', 'schema']))
    expect(paths).toContain(key(['source', 'schema_file']))
    expect(paths).toContain(key(['source', 'options', 'cloudFiles.schemaHints']))
  })

  it('flags the legacy scalar ⊕ cloudFiles.* twin conflict (generator hard-errors on both)', () => {
    const rule = customRules(loadCloudfilesSpec).find((r) =>
      r.paths.some((p) => key(p) === key(['source', 'schema_location'])),
    )
    expect(rule).toBeDefined()
    // Both the scalar and the twin option set → conflict hint.
    const both = {
      source: {
        schema_location: '/mnt/s',
        options: { 'cloudFiles.schemaLocation': '/mnt/s' },
      },
    }
    expect(rule!.check(both)).toBeTypeOf('string')
    // Only the scalar → no conflict.
    expect(rule!.check({ source: { schema_location: '/mnt/s' } })).toBeNull()
    // Only the option → no conflict.
    expect(
      rule!.check({ source: { options: { 'cloudFiles.schemaLocation': '/mnt/s' } } }),
    ).toBeNull()
  })

  it('keeps the readMode-must-be-stream soft rule (source.readMode)', () => {
    const rule = customRules(loadCloudfilesSpec).find((r) =>
      r.paths.some((p) => key(p) === key(['source', 'readMode'])),
    )
    expect(rule).toBeDefined()
    expect(rule!.check({ source: { readMode: 'batch' } })).toBeTypeOf('string')
    expect(rule!.check({ source: { readMode: 'stream' } })).toBeNull()
  })
})

// ── the other six — structural + fileRef only ────────────────

describe('load spec structural redesign — the other six', () => {
  it('delta keeps top-level readMode and groups reader knobs under an advanced group', () => {
    expect(findField(loadDeltaSpec, ['readMode'])).toBeDefined()
    const optionsGroup = groupOf(loadDeltaSpec, ['source', 'options'])!
    expect(optionsGroup.advanced).toBe(true)
  })

  it('sql swaps sql_path to a .sql fileRef while sql stays inline code', () => {
    expect(findField(loadSqlSpec, ['source', 'sql_path'])!.fileRef?.accept).toEqual(['.sql'])
    expect(findField(loadSqlSpec, ['source', 'sql'])!.fileRef).toBeUndefined()
    expect((loadSqlSpec.rules ?? []).some((r) => r.kind === 'xor')).toBe(true)
  })

  it('python swaps module_path to a .py fileRef', () => {
    expect(findField(loadPythonSpec, ['source', 'module_path'])!.fileRef?.accept).toEqual(['.py'])
  })

  it('jdbc keeps user/password as plain text (never masked) and the table⊕query xor', () => {
    expect(findField(loadJdbcSpec, ['source', 'user'])!.widget).toBe('text')
    expect(findField(loadJdbcSpec, ['source', 'password'])!.widget).toBe('text')
    expect((loadJdbcSpec.rules ?? []).some((r) => r.kind === 'xor')).toBe(true)
  })

  it('kafka keeps top-level readMode + the 3-way subscription xor + stream rule', () => {
    expect(findField(loadKafkaSpec, ['readMode'])).toBeDefined()
    const xor = (loadKafkaSpec.rules ?? []).find((r) => r.kind === 'xor')
    expect((xor as Extract<CrossFieldRule, { kind: 'xor' }>).paths).toHaveLength(3)
    expect(customRules(loadKafkaSpec).some((r) => r.check({ readMode: 'batch' }))).toBe(true)
  })

  it('custom_datasource swaps module_path to a .py fileRef and keeps top-level readMode', () => {
    expect(findField(loadCustomDatasourceSpec, ['source', 'module_path'])!.fileRef?.accept).toEqual([
      '.py',
    ])
    expect(findField(loadCustomDatasourceSpec, ['readMode'])).toBeDefined()
  })
})

// ── render integration — cloudfiles through the modal shell ──

const { commitSpy } = vi.hoisted(() => ({
  commitSpy: vi.fn<(fn: (doc: unknown) => void) => boolean>(),
}))

vi.mock('@/components/entity/useFlowgroupDoc', () => ({
  useFlowgroupDoc: () => ({ commit: commitSpy, readOnly: false }),
}))
vi.mock('@/workspace/persistBuffer', () => ({ persistBufferToDisk: vi.fn(() => Promise.resolve(true)) }))
vi.mock('@/store/runStore', async (importActual) => {
  const actual = await importActual<typeof import('@/store/runStore')>()
  return {
    ...actual,
    useRunController: () => ({ isRunning: false, startValidate: vi.fn(), startGenerate: vi.fn(), abort: vi.fn() }),
  }
})
vi.mock('@/hooks/useEnvironments', () => ({
  useEnvironmentResolved: vi.fn(() => ({ data: { tokens: {} } })),
}))
vi.mock('@/store/uiStore', () => ({
  useUIStore: (sel: (s: { selectedEnv: string }) => unknown) => sel({ selectedEnv: 'dev' }),
}))
vi.mock('@/hooks/useOperationalMetadata', () => ({
  useOperationalMetadata: () => ({ data: { columns: [] }, isLoading: false, error: null }),
}))
vi.mock('../../useCompanionFile', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../useCompanionFile')>()
  return { ...actual, useCompanionFile: () => ({ status: 'unavailable' as const, create: vi.fn() }) }
})

import type { ActionRead } from '@/lib/flowgroup-doc'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { ActionModalEditor } from '../../ActionModalEditor'

const CF_PATH = 'pipelines/bronze/orders.yaml'
const CF_YAML = `pipeline: bronze
flowgroup: orders
actions:
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: "\${landing}/orders/*.json"
      format: json
    target: v_orders_raw
`
const CF_ACTION: ActionRead = {
  name: 'load_orders',
  kind: 'load',
  subType: 'cloudfiles',
  target: 'v_orders_raw',
  sources: [],
  dependsOn: [],
  raw: {
    name: 'load_orders',
    type: 'load',
    source: { type: 'cloudfiles', path: '${landing}/orders/*.json', format: 'json' },
    target: 'v_orders_raw',
  },
  index: 0,
}

function wrapper({ children }: { children: ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return <QueryClientProvider client={qc}>{children}</QueryClientProvider>
}

function renderCloudfiles() {
  useWorkspaceStore.getState().openBuffer(CF_PATH, { content: CF_YAML, exists: true })
  return render(
    <ActionModalEditor filePath={CF_PATH} docKind="flowgroup" action={CF_ACTION} actionId="load_orders" />,
    { wrapper },
  )
}

beforeEach(() => {
  vi.clearAllMocks()
  commitSpy.mockReturnValue(true)
  useWorkspaceStore.setState({ buffers: [], tabs: [], activePath: null })
  Element.prototype.scrollIntoView = vi.fn()
  Element.prototype.hasPointerCapture = vi.fn(() => false) as never
  Element.prototype.setPointerCapture = vi.fn()
  Element.prototype.releasePointerCapture = vi.fn()
})

describe('load/cloudfiles — rendered through ActionModalEditor', () => {
  it('renders format as a select (combobox), plus the evolution-mode select and gap fields', () => {
    renderCloudfiles()
    expect(screen.getByRole('combobox', { name: 'Format' })).toBeInTheDocument()
    expect(screen.getByRole('combobox', { name: 'Schema evolution mode' })).toBeInTheDocument()
    expect(screen.getByText('Schema location')).toBeInTheDocument()
    expect(screen.getByText('Rescued data column')).toBeInTheDocument()
  })

  it('renders both schema fields as FileRefField (Browse / New)', () => {
    renderCloudfiles()
    expect(screen.getAllByRole('button', { name: /^browse$/i })).toHaveLength(2)
    expect(screen.getAllByRole('button', { name: /^new$/i })).toHaveLength(2)
  })

  it('collapses the Advanced group, hiding max_files_per_trigger until expanded', async () => {
    renderCloudfiles()
    const advanced = screen.getByRole('button', { name: /advanced/i })
    expect(advanced).toHaveAttribute('aria-expanded', 'false')
    expect(screen.queryByText('Max files per trigger')).toBeNull()

    await userEvent.setup().click(advanced)
    expect(screen.getByText('Max files per trigger')).toBeInTheDocument()
  })
})
