import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import type { ActionSubTypeSpec, CrossFieldRule, FieldGroup, FieldSpec } from '../types'
import type { YamlPath } from '@/lib/flowgroup-doc'
import { transformSqlSpec } from '../transform-sql'
import { transformPythonSpec } from '../transform-python'
import { transformDataQualitySpec } from '../transform-data-quality'
import { transformTempTableSpec } from '../transform-temp-table'
import { transformSchemaSpec } from '../transform-schema'

// ── spec-level helpers (mirror load-specs.redesign.test) ─────

const key = (path: YamlPath) => JSON.stringify(path)

function findField(spec: ActionSubTypeSpec, path: YamlPath): FieldSpec | undefined {
  return spec.groups.flatMap((g) => g.fields).find((f) => key(f.path) === key(path))
}

function groupOf(spec: ActionSubTypeSpec, path: YamlPath): FieldGroup | undefined {
  return spec.groups.find((g) => g.fields.some((f) => key(f.path) === key(path)))
}

function oneOfField(spec: ActionSubTypeSpec): FieldSpec | undefined {
  return spec.groups.flatMap((g) => g.fields).find((f) => f.widget === 'oneOfToggle')
}

function customRules(spec: ActionSubTypeSpec): Extract<CrossFieldRule, { kind: 'custom' }>[] {
  return (spec.rules ?? []).filter(
    (r): r is Extract<CrossFieldRule, { kind: 'custom' }> => r.kind === 'custom',
  )
}

// ── sql — sql ⊕ sql_path collapse into one oneOfToggle ───────

describe('transform/sql spec', () => {
  it('replaces the two sql/sql_path fields with a single inline⊕file oneOfToggle', () => {
    const toggle = oneOfField(transformSqlSpec)!
    expect(toggle.widget).toBe('oneOfToggle')
    const opts = toggle.oneOf!.options
    expect(opts.map((o) => o.value)).toEqual(['inline', 'file'])
    expect(opts.map((o) => key(o.path))).toEqual([key(['sql']), key(['sql_path'])])
    expect(opts[0].backing).toBe('inline')
    expect(opts[0].language).toBe('sql')
    expect(opts[1].backing).toBe('file')
    expect(opts[1].accept).toEqual(['.sql'])
    // sql/sql_path are no longer standalone fields, but the xor rule is KEPT as
    // a raw-YAML soft backstop (Phase-4 preamble) alongside the toggle's prune.
    expect(findField(transformSqlSpec, ['sql'])).toBeUndefined()
    expect(findField(transformSqlSpec, ['sql_path'])).toBeUndefined()
    expect((transformSqlSpec.rules ?? []).some((r) => r.kind === 'xor')).toBe(true)
    expect(findField(transformSqlSpec, ['source'])!.widget).toBe('stringOrList')
  })
})

// ── python — module_path fileRef, readMode to Advanced ───────

describe('transform/python spec', () => {
  it('swaps module_path to a .py fileRef, keeps keyValue parameters, moves readMode to Advanced', () => {
    expect(findField(transformPythonSpec, ['module_path'])!.fileRef?.accept).toEqual(['.py'])
    expect(findField(transformPythonSpec, ['parameters'])!.widget).toBe('keyValue')
    const readMode = findField(transformPythonSpec, ['readMode'])!
    expect(readMode.enumDefault).toBe('batch')
    expect(readMode.options).toEqual(['batch', 'stream'])
    expect(groupOf(transformPythonSpec, ['readMode'])!.advanced).toBe(true)
  })
})

// ── temp_table — plain inline sql (no sql_path, no toggle) ───

describe('transform/temp_table spec', () => {
  it('keeps sql as a plain inline textarea with no sql_path and no oneOfToggle', () => {
    const sql = findField(transformTempTableSpec, ['sql'])!
    expect(sql.widget).toBe('textarea')
    expect(oneOfField(transformTempTableSpec)).toBeUndefined()
    expect(findField(transformTempTableSpec, ['sql_path'])).toBeUndefined()
  })

  it('documents the literal {source} placeholder (not a ${} token) and moves readMode to Advanced', () => {
    expect(groupOf(transformTempTableSpec, ['sql'])!.description).toMatch(/\{source\}/)
    expect(groupOf(transformTempTableSpec, ['readMode'])!.advanced).toBe(true)
    expect(findField(transformTempTableSpec, ['readMode'])!.enumDefault).toBe('batch')
  })
})

// ── schema — schema_inline ⊕ schema_file toggle + enforcement ─

describe('transform/schema spec', () => {
  it('collapses schema_inline/schema_file into a yaml⊕file oneOfToggle', () => {
    const toggle = oneOfField(transformSchemaSpec)!
    const opts = toggle.oneOf!.options
    expect(opts.map((o) => key(o.path))).toEqual([key(['schema_inline']), key(['schema_file'])])
    expect(opts[0].backing).toBe('inline')
    expect(opts[0].language).toBe('yaml')
    expect(opts[1].accept).toEqual(['.yaml', '.yml'])
    // xor rule KEPT as a raw-YAML soft backstop (Phase-4 preamble).
    expect((transformSchemaSpec.rules ?? []).some((r) => r.kind === 'xor')).toBe(true)
  })

  it('keeps source string-only and enforcement in a non-advanced (Logic) group', () => {
    expect(findField(transformSchemaSpec, ['source'])!.widget).toBe('text')
    const enforcement = findField(transformSchemaSpec, ['enforcement'])!
    expect(enforcement.widget).toBe('enum')
    expect(enforcement.options).toEqual(['strict', 'permissive'])
    expect(enforcement.enumDefault).toBe('permissive')
    expect(groupOf(transformSchemaSpec, ['enforcement'])!.advanced).toBeUndefined()
  })
})

// ── data_quality — segmented mode + prune-on-switch (bug fix) ─

describe('transform/data_quality spec', () => {
  it('mode is a segmented discriminator that owns (and prunes) the quarantine subtree', () => {
    const modeField = findField(transformDataQualitySpec, ['mode'])!
    expect(modeField.display).toBe('segmented')
    expect(modeField.options).toEqual(['dqe', 'quarantine'])
    expect(modeField.enumDefault).toBe('dqe')
    expect(modeField.branchPaths).toEqual({ dqe: [], quarantine: [['quarantine']] })
  })

  it('expectations_file is a .yaml/.yml fileRef and the quarantine group is mode-gated', () => {
    expect(findField(transformDataQualitySpec, ['expectations_file'])!.fileRef?.accept).toEqual([
      '.yaml',
      '.yml',
    ])
    const group = groupOf(transformDataQualitySpec, ['quarantine', 'dlq_table'])!
    expect(group.visibleWhen!({ mode: 'quarantine' })).toBe(true)
    expect(group.visibleWhen!({ mode: 'dqe' })).toBe(false)
    expect(group.visibleWhen!({})).toBe(false) // default dqe
    expect(findField(transformDataQualitySpec, ['quarantine', 'source_table'])!.required).toBe(true)
  })

  it('renders readMode as an informational note (no editable readMode field)', () => {
    expect(findField(transformDataQualitySpec, ['readMode'])).toBeUndefined()
    const note = groupOf(transformDataQualitySpec, ['mode'])!.description ?? ''
    expect(note).toMatch(/stream/i)
    expect(note).toMatch(/required/i)
  })

  it('carries the quarantine-coercion advisory on the quarantine group', () => {
    expect(groupOf(transformDataQualitySpec, ['quarantine', 'dlq_table'])!.description).toMatch(
      /coerce/i,
    )
  })

  it('soft-hints a non-3-part quarantine table but tolerates 3-part names and tokens', () => {
    const dlq = customRules(transformDataQualitySpec).find((r) =>
      r.paths.some((p) => key(p) === key(['quarantine', 'dlq_table'])),
    )!
    expect(dlq.check({ quarantine: { dlq_table: 'orders_dlq' } })).toBeTypeOf('string')
    expect(dlq.check({ quarantine: { dlq_table: 'c.s.orders_dlq' } })).toBeNull()
    expect(dlq.check({ quarantine: { dlq_table: '${catalog}.${schema}.dlq' } })).toBeNull()
  })
})

// ── render integration through the modal shell ───────────────

const { commitSpy } = vi.hoisted(() => ({
  commitSpy: vi.fn<(fn: (doc: unknown) => void) => boolean>(),
}))

vi.mock('@/components/entity/useFlowgroupDoc', () => ({
  useFlowgroupDoc: () => ({ commit: commitSpy, readOnly: false }),
}))
vi.mock('@/workspace/persistBuffer', () => ({
  persistBufferToDisk: vi.fn(() => Promise.resolve(true)),
}))
vi.mock('@/store/runStore', async (importActual) => {
  const actual = await importActual<typeof import('@/store/runStore')>()
  return {
    ...actual,
    useRunController: () => ({
      isRunning: false,
      startValidate: vi.fn(),
      startGenerate: vi.fn(),
      abort: vi.fn(),
    }),
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

function wrapper({ children }: { children: ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return <QueryClientProvider client={qc}>{children}</QueryClientProvider>
}

function open(path: string, yaml: string, action: ActionRead) {
  useWorkspaceStore.getState().openBuffer(path, { content: yaml, exists: true })
  return render(
    <ActionModalEditor filePath={path} docKind="flowgroup" action={action} actionId={action.name} />,
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

const SQL_PATH = 'pipelines/silver/orders.yaml'
const SQL_YAML = `pipeline: silver
flowgroup: orders
actions:
  - name: t_orders
    type: transform
    transform_type: sql
    source: v_orders_raw
    sql: SELECT * FROM v_orders_raw
    target: v_orders
`
const SQL_ACTION: ActionRead = {
  name: 't_orders',
  kind: 'transform',
  subType: 'sql',
  target: 'v_orders',
  sources: ['v_orders_raw'],
  dependsOn: [],
  raw: {
    name: 't_orders',
    type: 'transform',
    transform_type: 'sql',
    source: 'v_orders_raw',
    sql: 'SELECT * FROM v_orders_raw',
    target: 'v_orders',
  },
  index: 0,
}

const SQL_EMPTY_YAML = `pipeline: silver
flowgroup: orders
actions:
  - name: t_orders
    type: transform
    transform_type: sql
    source: v_orders_raw
    target: v_orders
`
const SQL_EMPTY_ACTION: ActionRead = {
  ...SQL_ACTION,
  raw: {
    name: 't_orders',
    type: 'transform',
    transform_type: 'sql',
    source: 'v_orders_raw',
    target: 'v_orders',
  },
}

describe('transform/sql — rendered through ActionModalEditor', () => {
  it('renders the sql oneOfToggle as segmented Inline SQL / From file (inline active)', () => {
    open(SQL_PATH, SQL_YAML, SQL_ACTION)
    expect(screen.getByRole('radio', { name: 'Inline SQL' })).toHaveAttribute('data-state', 'on')
    expect(screen.getByRole('radio', { name: 'From file' })).toBeInTheDocument()
  })

  it('surfaces the xor soft hint ON the toggle when neither branch is set', () => {
    open(SQL_PATH, SQL_EMPTY_YAML, SQL_EMPTY_ACTION)
    expect(screen.getByText(/provide exactly one of sql/i)).toBeInTheDocument()
  })

  it('switching to From file swaps in a FileRefField (Browse)', async () => {
    open(SQL_PATH, SQL_YAML, SQL_ACTION)
    await userEvent.setup().click(screen.getByRole('radio', { name: 'From file' }))
    expect(screen.getByRole('button', { name: /^browse$/i })).toBeInTheDocument()
  })
})

const SCHEMA_PATH = 'pipelines/silver/typed.yaml'
const SCHEMA_YAML = `pipeline: silver
flowgroup: typed
actions:
  - name: t_typed
    type: transform
    transform_type: schema
    source: v_orders
    schema_inline: |
      columns:
        - "id -> order_id: BIGINT"
    enforcement: permissive
    target: v_orders_typed
`
const SCHEMA_ACTION: ActionRead = {
  name: 't_typed',
  kind: 'transform',
  subType: 'schema',
  target: 'v_orders_typed',
  sources: ['v_orders'],
  dependsOn: [],
  raw: {
    name: 't_typed',
    type: 'transform',
    transform_type: 'schema',
    source: 'v_orders',
    schema_inline: 'columns:\n  - "id -> order_id: BIGINT"',
    enforcement: 'permissive',
    target: 'v_orders_typed',
  },
  index: 0,
}

describe('transform/schema — rendered through ActionModalEditor', () => {
  it('renders the schema oneOfToggle (Inline schema / From file) and enforcement as a select', () => {
    open(SCHEMA_PATH, SCHEMA_YAML, SCHEMA_ACTION)
    expect(screen.getByRole('radio', { name: 'Inline schema' })).toHaveAttribute('data-state', 'on')
    expect(screen.getByRole('radio', { name: 'From file' })).toBeInTheDocument()
    expect(screen.getByRole('combobox', { name: 'Enforcement' })).toBeInTheDocument()
  })
})

const DQ_PATH = 'pipelines/silver/dq.yaml'
const DQ_YAML = `pipeline: silver
flowgroup: dq
actions:
  - name: dq_orders
    type: transform
    transform_type: data_quality
    source: v_orders
    expectations_file: expectations/orders.yaml
    mode: quarantine
    quarantine:
      dlq_table: cat.sch.orders_dlq
      source_table: cat.sch.orders
    target: v_orders_validated
`
const DQ_ACTION: ActionRead = {
  name: 'dq_orders',
  kind: 'transform',
  subType: 'data_quality',
  target: 'v_orders_validated',
  sources: ['v_orders'],
  dependsOn: [],
  raw: {
    name: 'dq_orders',
    type: 'transform',
    transform_type: 'data_quality',
    source: 'v_orders',
    expectations_file: 'expectations/orders.yaml',
    mode: 'quarantine',
    quarantine: { dlq_table: 'cat.sch.orders_dlq', source_table: 'cat.sch.orders' },
    target: 'v_orders_validated',
  },
  index: 0,
}

describe('transform/data_quality — rendered through ActionModalEditor', () => {
  it('renders mode as a segmented control and expectations_file as a FileRefField', () => {
    open(DQ_PATH, DQ_YAML, DQ_ACTION)
    expect(screen.getByRole('radio', { name: 'quarantine' })).toHaveAttribute('data-state', 'on')
    expect(screen.getByRole('radio', { name: 'dqe' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /^browse$/i })).toBeInTheDocument()
  })

  it('reveals the quarantine fields when mode=quarantine and hides them (prune) on switch to dqe', async () => {
    open(DQ_PATH, DQ_YAML, DQ_ACTION)
    expect(screen.getByText('DLQ table')).toBeInTheDocument()
    expect(screen.getByText('Source table')).toBeInTheDocument()

    await userEvent.setup().click(screen.getByRole('radio', { name: 'dqe' }))
    expect(screen.queryByText('DLQ table')).toBeNull()
    expect(screen.queryByText('Source table')).toBeNull()
  })
})

// ── mode prune-on-switch — replay the spec's branchPaths on a fresh doc ──

import {
  listActions,
  parseFlowgroupFile,
  selectFlowgroupAt,
  type FlowgroupDocHandle,
} from '@/lib/flowgroup-doc'
import { applyDiscriminatorChange } from '../../formModel'
import { readPath } from '../helpers'

const freshDoc = (yaml: string): FlowgroupDocHandle =>
  selectFlowgroupAt(parseFlowgroupFile(yaml), 0) as FlowgroupDocHandle

describe('data_quality mode branchPaths — replay on a fresh doc', () => {
  it('quarantine → dqe prunes the whole quarantine block and writes mode: dqe', () => {
    const doc = freshDoc(DQ_YAML)
    const branchPaths = findField(transformDataQualitySpec, ['mode'])!.branchPaths!
    applyDiscriminatorChange(doc, 'dq_orders', ['mode'], 'dqe', branchPaths)
    const raw = listActions(doc)[0].raw
    expect(raw.mode).toBe('dqe')
    expect(readPath(raw, ['quarantine'])).toBeUndefined() // pruned subtree
    expect(readPath(raw, ['expectations_file'])).toBe('expectations/orders.yaml') // untouched
  })
})
