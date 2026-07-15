import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import type { ActionSubTypeSpec, BranchPathMap, CrossFieldRule, FieldSpec } from '../types'
import type { YamlPath } from '@/lib/flowgroup-doc'
import { writeStreamingTableSpec as spec } from '../write-streaming-table'

// ── spec-level helpers (mirror transform/load redesign tests) ────

const key = (path: YamlPath) => JSON.stringify(path)

function findField(s: ActionSubTypeSpec, path: YamlPath): FieldSpec | undefined {
  return s.groups.flatMap((g) => g.fields).find((f) => key(f.path) === key(path))
}

function groupTitleOf(s: ActionSubTypeSpec, path: YamlPath): string | undefined {
  return s.groups.find((g) => g.fields.some((f) => key(f.path) === key(path)))?.title
}

function groupOf(s: ActionSubTypeSpec, title: string) {
  return s.groups.find((g) => g.title === title)!
}

/** The oneOfToggle in a group by its synthetic path. */
function toggle(path: YamlPath): FieldSpec {
  const f = findField(spec, path)!
  expect(f.widget).toBe('oneOfToggle')
  return f
}

/** Replicate OneOfToggle's per-branch prune map from a toggle's options. */
function toggleBranchPaths(f: FieldSpec): BranchPathMap {
  return Object.fromEntries((f.oneOf?.options ?? []).map((o) => [o.value, [o.path]]))
}

const CDC = ['write_target', 'cdc_config']
const SNAP = ['write_target', 'snapshot_cdc_config']

// ── mode discriminator ───────────────────────────────────────

describe('write/streaming_table — mode discriminator', () => {
  it('renders mode as a segmented 3-option discriminator with branchPaths', () => {
    const m = findField(spec, ['write_target', 'mode'])!
    expect(m.display).toBe('segmented')
    expect(m.options).toEqual(['standard', 'cdc', 'snapshot_cdc'])
    expect(m.enumDefault).toBe('standard')
    expect(m.branchPaths).toEqual({
      standard: [['source']],
      cdc: [['source'], CDC],
      snapshot_cdc: [SNAP],
    })
  })

  it('hides the action-level Source group only in snapshot_cdc', () => {
    const src = groupOf(spec, 'Source').visibleWhen!
    expect(src({})).toBe(true) // default standard
    expect(src({ write_target: { mode: 'cdc' } })).toBe(true)
    expect(src({ write_target: { mode: 'snapshot_cdc' } })).toBe(false)
  })
})

// ── CDC config — scd_type gating + disabledWhen ──────────────

describe('write/streaming_table — CDC config', () => {
  it('renders scd_type as a segmented integer enum defaulting to 1', () => {
    const scd = findField(spec, [...CDC, 'scd_type'])!
    expect(scd.display).toBe('segmented')
    expect(scd.options).toEqual(['1', '2'])
    expect(scd.valueType).toBe('integer')
    expect(scd.enumDefault).toBe('1')
  })

  it('gates track_history_* on SCD Type 2 (handles number 2 AND string "2")', () => {
    const th = findField(spec, [...CDC, 'track_history_column_list'])!
    const the = findField(spec, [...CDC, 'track_history_except_column_list'])!
    for (const f of [th, the]) {
      expect(f.visibleWhen!({ write_target: { cdc_config: { scd_type: 2 } } })).toBe(true)
      expect(f.visibleWhen!({ write_target: { cdc_config: { scd_type: '2' } } })).toBe(true)
      expect(f.visibleWhen!({ write_target: { cdc_config: { scd_type: 1 } } })).toBe(false)
      expect(f.visibleWhen!({ write_target: { cdc_config: {} } })).toBe(false)
    }
  })

  it('disables apply_as_truncates under SCD Type 2 (number AND string), else enabled', () => {
    const t = findField(spec, [...CDC, 'apply_as_truncates'])!
    expect(t.disabledWhen!({ write_target: { cdc_config: { scd_type: 2 } } })).toBe(true)
    expect(t.disabledWhen!({ write_target: { cdc_config: { scd_type: '2' } } })).toBe(true)
    expect(t.disabledWhen!({ write_target: { cdc_config: { scd_type: 1 } } })).toBe(false)
    expect(t.disabledWhen!({ write_target: { cdc_config: {} } })).toBe(false)
  })

  it('keeps apply_as_deletes/apply_as_truncates as SQL-expression text fields', () => {
    expect(findField(spec, [...CDC, 'apply_as_deletes'])!.widget).toBe('text')
    expect(findField(spec, [...CDC, 'apply_as_truncates'])!.widget).toBe('text')
  })
})

// ── snapshot CDC config — source ⊕ source_function toggle ────

describe('write/streaming_table — snapshot CDC config', () => {
  it('collapses source ⊕ source_function into a oneOfToggle (text vs fields backing)', () => {
    const t = toggle([...SNAP, '__source'])
    const opts = t.oneOf!.options
    expect(opts.map((o) => o.value)).toEqual(['source', 'source_function'])
    expect(opts.map((o) => key(o.path))).toEqual([key([...SNAP, 'source']), key([...SNAP, 'source_function'])])
    expect(opts[0].backing).toBe('text')
    expect(opts[1].backing).toBe('fields')
    // the standalone source / source_function fields are gone
    expect(findField(spec, [...SNAP, 'source'])).toBeUndefined()
    expect(findField(spec, [...SNAP, 'source_function'])).toBeUndefined()
  })

  it('source_function branch declares file(.py fileRef)/function/parameters sub-fields', () => {
    const fn = toggle([...SNAP, '__source']).oneOf!.options.find((o) => o.value === 'source_function')!
    const sub = fn.fields!
    expect(sub.map((f) => key(f.path))).toEqual([
      key([...SNAP, 'source_function', 'file']),
      key([...SNAP, 'source_function', 'function']),
      key([...SNAP, 'source_function', 'parameters']),
    ])
    expect(sub[0].fileRef?.accept).toEqual(['.py'])
    expect(sub[0].required).toBe(true)
    expect(sub[1].required).toBe(true)
    expect(sub[2].widget).toBe('keyValue')
  })

  it('uses stored_as_scd_type (NOT scd_type) as a segmented int; track_history is NOT SCD-gated', () => {
    const s = findField(spec, [...SNAP, 'stored_as_scd_type'])!
    expect(s.display).toBe('segmented')
    expect(s.valueType).toBe('integer')
    expect(s.enumDefault).toBe('1')
    expect(findField(spec, [...SNAP, 'scd_type'])).toBeUndefined()
    // snapshot track_history has no visibleWhen (shown regardless of SCD type)
    expect(findField(spec, [...SNAP, 'track_history_column_list'])!.visibleWhen).toBeUndefined()
    expect(findField(spec, [...SNAP, 'track_history_except_column_list'])!.visibleWhen).toBeUndefined()
  })
})

// ── table options — advanced group, forced create_table, once, schema toggle ─

describe('write/streaming_table — table options', () => {
  it('puts table options in an advanced group', () => {
    expect(groupOf(spec, 'Table options').advanced).toBe(true)
  })

  it('forces create_table disabled in snapshot_cdc and surfaces the hint rule', () => {
    const ct = findField(spec, ['write_target', 'create_table'])!
    expect(ct.defaultValue).toBe(true)
    expect(ct.disabledWhen!({ write_target: { mode: 'snapshot_cdc' } })).toBe(true)
    expect(ct.disabledWhen!({ write_target: { mode: 'cdc' } })).toBe(false)
    expect(ct.disabledWhen!({})).toBe(false)
    const rule = (spec.rules ?? []).find(
      (r): r is Extract<CrossFieldRule, { kind: 'custom' }> =>
        r.kind === 'custom' && r.paths.some((p) => key(p) === key(['write_target', 'create_table'])),
    )!
    expect(rule.check({ write_target: { mode: 'snapshot_cdc' } })).toMatch(/forced on/i)
    expect(rule.check({ write_target: { mode: 'standard' } })).toBeNull()
  })

  it('adds an action-level once bool and a table_schema inline⊕file toggle (shared key)', () => {
    expect(findField(spec, ['once'])!.widget).toBe('bool')
    expect(groupTitleOf(spec, ['once'])).toBe('Table options')
    const t = toggle(['write_target', '__table_schema'])
    const opts = t.oneOf!.options
    expect(opts.map((o) => o.value)).toEqual(['inline', 'file'])
    // both branches own the single real table_schema key (auto-detected inline/file)
    expect(opts.every((o) => key(o.path) === key(['write_target', 'table_schema']))).toBe(true)
    expect(opts[0].backing).toBe('inline')
    expect(opts[1].backing).toBe('file')
    expect(opts[1].accept).toEqual(['.yaml', '.yml', '.json', '.sql'])
    expect(findField(spec, ['write_target', 'table_schema'])).toBeUndefined() // no longer standalone
  })
})

// ── kept cross-field rules ───────────────────────────────────

describe('write/streaming_table — kept cross-field rules', () => {
  it('keeps the cluster / cdc-column / track-history / snapshot-xor rules', () => {
    const kinds = (spec.rules ?? []).map((r) => r.kind)
    expect(kinds.filter((k) => k === 'mutuallyExclusive').length).toBeGreaterThanOrEqual(3)
    expect(kinds).toContain('xor') // snapshot source ⊕ source_function
    const xor = (spec.rules ?? []).find((r) => r.kind === 'xor')!
    expect(xor.paths.map((p) => key(p))).toEqual([key([...SNAP, 'source']), key([...SNAP, 'source_function'])])
  })
})

// ── mode branchPaths — replay prune on a fresh doc ───────────

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

const CDC_YAML = `pipeline: silver
flowgroup: customers
actions:
  - name: w_customer_dim
    type: write
    source: v_customer_bronze
    write_target:
      type: streaming_table
      mode: cdc
      catalog: main
      schema: silver
      table: customer_dim
      cdc_config:
        keys:
          - customer_id
        scd_type: 2
`

const SNAP_FN_YAML = `pipeline: silver
flowgroup: customers
actions:
  - name: w_customer_dim
    type: write
    write_target:
      type: streaming_table
      mode: snapshot_cdc
      catalog: main
      schema: silver
      table: customer_dim
      snapshot_cdc_config:
        source_function:
          file: snapshots/customer.py
          function: next_snapshot
        keys:
          - customer_id
`

const SNAP_SRC_YAML = `pipeline: silver
flowgroup: customers
actions:
  - name: w_customer_dim
    type: write
    write_target:
      type: streaming_table
      mode: snapshot_cdc
      catalog: main
      schema: silver
      table: customer_dim
      snapshot_cdc_config:
        source: main.bronze.customer_snapshot
        keys:
          - customer_id
`

describe('write/streaming_table mode branchPaths — replay on a fresh doc', () => {
  const modeBranchPaths = findField(spec, ['write_target', 'mode'])!.branchPaths!
  const modePath = ['write_target', 'mode']

  it('cdc → standard prunes cdc_config, keeps shared source, writes mode standard', () => {
    const doc = freshDoc(CDC_YAML)
    applyDiscriminatorChange(doc, 'w_customer_dim', modePath, 'standard', modeBranchPaths)
    const raw = listActions(doc)[0].raw
    expect(readPath(raw, modePath)).toBe('standard')
    expect(readPath(raw, CDC)).toBeUndefined() // cdc_config pruned
    expect(readPath(raw, ['source'])).toBe('v_customer_bronze') // shared source survives
  })

  it('cdc → snapshot_cdc prunes BOTH action source and cdc_config', () => {
    const doc = freshDoc(CDC_YAML)
    applyDiscriminatorChange(doc, 'w_customer_dim', modePath, 'snapshot_cdc', modeBranchPaths)
    const raw = listActions(doc)[0].raw
    expect(readPath(raw, ['source'])).toBeUndefined() // pruned
    expect(readPath(raw, CDC)).toBeUndefined() // pruned
    expect(readPath(raw, modePath)).toBe('snapshot_cdc')
  })

  it('snapshot_cdc → standard prunes snapshot_cdc_config', () => {
    const doc = freshDoc(SNAP_FN_YAML)
    applyDiscriminatorChange(doc, 'w_customer_dim', modePath, 'standard', modeBranchPaths)
    const raw = listActions(doc)[0].raw
    expect(readPath(raw, SNAP)).toBeUndefined()
  })
})

// ── snapshot source toggle — replay prune (built like OneOfToggle) ──

describe('write/streaming_table snapshot source toggle — replay prune', () => {
  const bp = toggleBranchPaths(toggle([...SNAP, '__source']))

  it('switching source_function → source prunes the source_function object', () => {
    const doc = freshDoc(SNAP_FN_YAML)
    applyDiscriminatorChange(doc, 'w_customer_dim', undefined, 'source', bp)
    const raw = listActions(doc)[0].raw
    expect(readPath(raw, [...SNAP, 'source_function'])).toBeUndefined() // pruned
    expect(readPath(raw, [...SNAP, 'keys'])).toEqual(['customer_id']) // untouched
  })

  it('switching source → source_function prunes the source key', () => {
    const doc = freshDoc(SNAP_SRC_YAML)
    applyDiscriminatorChange(doc, 'w_customer_dim', undefined, 'source_function', bp)
    const raw = listActions(doc)[0].raw
    expect(readPath(raw, [...SNAP, 'source'])).toBeUndefined() // pruned
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

const WRITE_ACTION = (raw: Record<string, unknown>): ActionRead => ({
  name: 'w_customer_dim',
  kind: 'write',
  subType: 'streaming_table',
  target: 'main.silver.customer_dim',
  sources: [],
  dependsOn: [],
  raw: { name: 'w_customer_dim', type: 'write', ...raw },
  index: 0,
})

const STANDARD_YAML = `pipeline: silver
flowgroup: customers
actions:
  - name: w_customer_dim
    type: write
    source: v_customer_bronze
    write_target:
      type: streaming_table
      catalog: main
      schema: silver
      table: customer_dim
`

const CDC_SCD1_YAML = `pipeline: silver
flowgroup: customers
actions:
  - name: w_customer_dim
    type: write
    source: v_customer_bronze
    write_target:
      type: streaming_table
      mode: cdc
      catalog: main
      schema: silver
      table: customer_dim
      cdc_config:
        keys:
          - customer_id
`

describe('write/streaming_table — rendered through ActionModalEditor', () => {
  it('renders mode as a segmented control (standard active by default)', () => {
    open('pipelines/silver/customers.yaml', STANDARD_YAML, WRITE_ACTION({ source: 'v_customer_bronze' }))
    expect(screen.getByRole('radio', { name: 'standard' })).toHaveAttribute('data-state', 'on')
    expect(screen.getByRole('radio', { name: 'cdc' })).toBeInTheDocument()
    expect(screen.getByRole('radio', { name: 'snapshot_cdc' })).toBeInTheDocument()
  })

  it('reveals track_history_* and disables apply_as_truncates when scd_type flips to 2', async () => {
    open('pipelines/silver/customers.yaml', CDC_SCD1_YAML, WRITE_ACTION({}))
    // SCD segmented control present; track_history hidden and truncates enabled at SCD 1
    expect(screen.getByRole('radio', { name: '2' })).toBeInTheDocument()
    expect(screen.queryByText('Track history — columns')).toBeNull()
    expect(screen.getByLabelText('Apply as truncates')).not.toBeDisabled()

    await userEvent.setup().click(screen.getByRole('radio', { name: '2' }))

    expect(screen.getByText('Track history — columns')).toBeInTheDocument()
    expect(screen.getByLabelText('Apply as truncates')).toBeDisabled()
  })

  it('renders the snapshot source toggle, source_function FileRefField, and forced create_table', async () => {
    open(
      'pipelines/silver/customers.yaml',
      SNAP_FN_YAML,
      WRITE_ACTION({
        write_target: {
          type: 'streaming_table',
          mode: 'snapshot_cdc',
          snapshot_cdc_config: {
            source_function: { file: 'snapshots/customer.py', function: 'next_snapshot' },
          },
        },
      }),
    )
    // source ⊕ source_function toggle, source_function active (present)
    expect(screen.getByRole('radio', { name: 'Source function' })).toHaveAttribute('data-state', 'on')
    expect(screen.getByRole('radio', { name: 'Source table' })).toBeInTheDocument()
    // source_function.file renders a FileRefField (Browse) via the fields backing
    expect(screen.getByRole('button', { name: /^browse$/i })).toBeInTheDocument()

    // Table options is an advanced (collapsed) group — expand it, then assert the
    // table_schema toggle + the forced-on/disabled create_table switch.
    await userEvent.setup().click(screen.getByRole('button', { name: /table options/i }))
    expect(screen.getByRole('radio', { name: 'Inline DDL' })).toBeInTheDocument()
    expect(screen.getByRole('radio', { name: 'From file' })).toBeInTheDocument()
    expect(screen.getByLabelText('Create table')).toBeDisabled()
  })
})
