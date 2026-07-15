import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import type { ActionSubTypeSpec, BranchPathMap, FieldSpec } from '../types'
import type { YamlPath } from '@/lib/flowgroup-doc'
import { writeMaterializedViewSpec as mv } from '../write-materialized-view'
import { writeSinkSpec as sink } from '../write-sink'

// ── spec-level helpers (mirror the streaming_table redesign test) ─

const key = (path: YamlPath) => JSON.stringify(path)

function findField(s: ActionSubTypeSpec, path: YamlPath): FieldSpec | undefined {
  return s.groups.flatMap((g) => g.fields).find((f) => key(f.path) === key(path))
}

function groupOf(s: ActionSubTypeSpec, title: string) {
  return s.groups.find((g) => g.title === title)!
}

function toggle(s: ActionSubTypeSpec, path: YamlPath): FieldSpec {
  const f = findField(s, path)!
  expect(f.widget).toBe('oneOfToggle')
  return f
}

/** Replicate OneOfToggle's per-branch prune map from a toggle's options. */
function toggleBranchPaths(f: FieldSpec): BranchPathMap {
  return Object.fromEntries((f.oneOf?.options ?? []).map((o) => [o.value, [o.path]]))
}

const WT = ['write_target']

// ── materialized_view — query-source 3-way oneOfToggle ───────

describe('write/materialized_view — query source', () => {
  it('collapses source ⊕ sql ⊕ sql_path into ONE 3-way oneOfToggle', () => {
    const t = toggle(mv, ['__query_source'])
    const opts = t.oneOf!.options
    expect(opts.map((o) => o.value)).toEqual(['source', 'sql', 'sql_path'])
    expect(opts.map((o) => key(o.path))).toEqual([
      key(['source']),
      key([...WT, 'sql']),
      key([...WT, 'sql_path']),
    ])
    expect(opts.map((o) => o.backing)).toEqual(['text', 'inline', 'file'])
    expect(opts[1].language).toBe('sql')
    expect(opts[2].accept).toEqual(['.sql'])
    // the standalone source / sql / sql_path fields are gone
    expect(findField(mv, ['source'])).toBeUndefined()
    expect(findField(mv, [...WT, 'sql'])).toBeUndefined()
    expect(findField(mv, [...WT, 'sql_path'])).toBeUndefined()
  })

  it('keeps requiredOneOf + mutuallyExclusive on the three query-source keys', () => {
    const qsPaths = [key(['source']), key([...WT, 'sql']), key([...WT, 'sql_path'])]
    const req = (mv.rules ?? []).find((r) => r.kind === 'requiredOneOf')!
    const excl = (mv.rules ?? []).find(
      (r) => r.kind === 'mutuallyExclusive' && r.paths.some((p) => key(p) === key(['source'])),
    )!
    expect(req.paths.map((p) => key(p))).toEqual(qsPaths)
    expect(excl.paths.map((p) => key(p))).toEqual(qsPaths)
  })

  it('renders refresh_policy as a select (not segmented) with an unset default entry, and omits create_table', () => {
    const rp = findField(mv, [...WT, 'refresh_policy'])!
    expect(rp.widget).toBe('enum')
    expect(rp.display).not.toBe('segmented')
    expect(rp.options).toEqual(['auto', 'incremental', 'incremental_strict', 'full'])
    expect(rp.unsetLabel).toBe('Not set (default)')
    expect(findField(mv, [...WT, 'create_table'])).toBeUndefined()
  })

  it('puts table options (comment/refresh/etc.) in an advanced group', () => {
    expect(groupOf(mv, 'Table options').advanced).toBe(true)
  })
})

// ── sink — sink_type segmented discriminator + branchPaths ───

describe('write/sink — sink_type discriminator', () => {
  it('renders sink_type segmented, required, with NO default (explicit empty state)', () => {
    const st = findField(sink, [...WT, 'sink_type'])!
    expect(st.display).toBe('segmented')
    expect(st.options).toEqual(['delta', 'kafka', 'custom', 'foreachbatch'])
    expect(st.required).toBe(true)
    expect(st.enumDefault).toBeUndefined()
    expect(st.unsetLabel).toBeUndefined()
  })

  it('declares branchPaths with options shared delta+kafka and module_path shared custom+foreachbatch', () => {
    const st = findField(sink, [...WT, 'sink_type'])!
    expect(st.branchPaths).toEqual({
      delta: [[...WT, 'options']],
      kafka: [[...WT, 'bootstrap_servers'], [...WT, 'topic'], [...WT, 'options']],
      custom: [[...WT, 'module_path'], [...WT, 'custom_sink_class']],
      foreachbatch: [[...WT, 'module_path'], [...WT, 'batch_handler']],
    })
  })

  it('custom = module_path (fileRef .py) + custom_sink_class, and drops the options field', () => {
    const mp = findField(sink, [...WT, 'module_path'])!
    expect(mp.fileRef?.accept).toEqual(['.py'])
    expect(mp.required).toBe(true)
    expect(findField(sink, [...WT, 'custom_sink_class'])!.required).toBe(true)
    const customFields = groupOf(sink, 'Custom sink').fields.map((f) => key(f.path))
    expect(customFields).not.toContain(key([...WT, 'options']))
  })

  it('foreachbatch collapses module_path ⊕ batch_handler into a oneOfToggle (file .py vs inline python)', () => {
    const t = toggle(sink, [...WT, '__handler'])
    const opts = t.oneOf!.options
    expect(opts.map((o) => o.value)).toEqual(['module_path', 'batch_handler'])
    expect(opts.map((o) => key(o.path))).toEqual([
      key([...WT, 'module_path']),
      key([...WT, 'batch_handler']),
    ])
    expect(opts[0].backing).toBe('file')
    expect(opts[0].accept).toEqual(['.py'])
    expect(opts[1].backing).toBe('inline')
    expect(opts[1].language).toBe('python')
  })

  it('keeps the delta options XOR, the 3-part tableName hint, the foreachbatch XOR, and the single-source rule', () => {
    const rules = sink.rules ?? []
    const optionRules = rules.filter(
      (r) => r.kind === 'custom' && r.paths.some((p) => key(p) === key([...WT, 'options'])),
    )
    expect(optionRules).toHaveLength(2) // tableName⊕path + 3-part tableName hint
    // XOR (tableName / path)
    expect(
      optionRules.some(
        (r) =>
          r.kind === 'custom' &&
          /both tableName and path|either tableName or path/i.test(
            r.check({ write_target: { sink_type: 'delta' } }) ?? '',
          ),
      ),
    ).toBe(true)
    // 3-part tableName hint fires on a non-3-part name, not on a valid one / a token
    const threePart = optionRules.find(
      (r) =>
        r.kind === 'custom' &&
        /3-part name/i.test(
          r.check({ write_target: { sink_type: 'delta', options: { tableName: 'orders' } } }) ?? '',
        ),
    )
    expect(threePart).toBeDefined()
    if (threePart && threePart.kind === 'custom') {
      expect(
        threePart.check({ write_target: { sink_type: 'delta', options: { tableName: 'c.s.orders' } } }),
      ).toBeNull()
      expect(
        threePart.check({ write_target: { sink_type: 'delta', options: { tableName: '${full}' } } }),
      ).toBeNull()
    }
    // foreachbatch XOR + single-source rule still present
    const fbXor = rules.find(
      (r) =>
        r.kind === 'custom' &&
        r.paths.some((p) => key(p) === key([...WT, 'module_path'])) &&
        r.paths.some((p) => key(p) === key([...WT, 'batch_handler'])),
    )
    expect(fbXor).toBeDefined()
    const single = rules.find(
      (r) => r.kind === 'custom' && r.paths.length === 1 && key(r.paths[0]) === key(['source']),
    )
    expect(single).toBeDefined()
  })
})

// ── replay prune on a freshly-parsed doc ─────────────────────

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

const MV_MULTI_YAML = `pipeline: gold
flowgroup: summary
actions:
  - name: w_summary
    type: write
    source: v_customer_orders
    write_target:
      type: materialized_view
      catalog: main
      schema: gold
      table: customer_summary
      sql: SELECT 1
      sql_path: sql/customer_summary.sql
`

describe('write/materialized_view query-source toggle — replay on a fresh doc', () => {
  const bp = toggleBranchPaths(toggle(mv, ['__query_source']))

  it('switching to sql prunes source AND sql_path (keeps exactly one)', () => {
    const doc = freshDoc(MV_MULTI_YAML)
    applyDiscriminatorChange(doc, 'w_summary', undefined, 'sql', bp)
    const raw = listActions(doc)[0].raw
    expect(readPath(raw, ['source'])).toBeUndefined()
    expect(readPath(raw, [...WT, 'sql_path'])).toBeUndefined()
    expect(readPath(raw, [...WT, 'sql'])).toBe('SELECT 1')
  })

  it('switching to source prunes sql AND sql_path (keeps exactly one)', () => {
    const doc = freshDoc(MV_MULTI_YAML)
    applyDiscriminatorChange(doc, 'w_summary', undefined, 'source', bp)
    const raw = listActions(doc)[0].raw
    expect(readPath(raw, [...WT, 'sql'])).toBeUndefined()
    expect(readPath(raw, [...WT, 'sql_path'])).toBeUndefined()
    expect(readPath(raw, ['source'])).toBe('v_customer_orders')
  })
})

const DELTA_YAML = `pipeline: gold
flowgroup: exports
actions:
  - name: w_sink
    type: write
    source: v_metrics
    write_target:
      type: sink
      sink_type: delta
      sink_name: analytics_export
      options:
        tableName: main.gold.orders
`

const KAFKA_YAML = `pipeline: gold
flowgroup: exports
actions:
  - name: w_sink
    type: write
    source: v_metrics
    write_target:
      type: sink
      sink_type: kafka
      sink_name: analytics_export
      bootstrap_servers: \${kafka_bootstrap_servers}
      topic: acme.orders
      options:
        kafka.security.protocol: SASL_SSL
`

const CUSTOM_YAML = `pipeline: gold
flowgroup: exports
actions:
  - name: w_sink
    type: write
    source: v_metrics
    write_target:
      type: sink
      sink_type: custom
      sink_name: analytics_export
      module_path: sinks/my_sink.py
      custom_sink_class: MyCustomDataSink
`

const FB_MODULE_YAML = `pipeline: gold
flowgroup: exports
actions:
  - name: w_sink
    type: write
    source: v_metrics
    write_target:
      type: sink
      sink_type: foreachbatch
      sink_name: analytics_export
      module_path: batch_handlers/my_handler.py
`

describe('write/sink sink_type branchPaths — replay on a fresh doc', () => {
  const bp = findField(sink, [...WT, 'sink_type'])!.branchPaths!
  const sinkTypePath = [...WT, 'sink_type']

  it('delta → kafka keeps the shared options key', () => {
    const doc = freshDoc(DELTA_YAML)
    applyDiscriminatorChange(doc, 'w_sink', sinkTypePath, 'kafka', bp)
    const raw = listActions(doc)[0].raw
    expect(readPath(raw, sinkTypePath)).toBe('kafka')
    expect(readPath(raw, [...WT, 'options', 'tableName'])).toBe('main.gold.orders')
  })

  it('kafka → delta prunes bootstrap_servers/topic, keeps shared options', () => {
    const doc = freshDoc(KAFKA_YAML)
    applyDiscriminatorChange(doc, 'w_sink', sinkTypePath, 'delta', bp)
    const raw = listActions(doc)[0].raw
    expect(readPath(raw, [...WT, 'bootstrap_servers'])).toBeUndefined()
    expect(readPath(raw, [...WT, 'topic'])).toBeUndefined()
    expect(readPath(raw, [...WT, 'options'])).toBeDefined()
  })

  it('custom → foreachbatch prunes custom_sink_class, keeps shared module_path', () => {
    const doc = freshDoc(CUSTOM_YAML)
    applyDiscriminatorChange(doc, 'w_sink', sinkTypePath, 'foreachbatch', bp)
    const raw = listActions(doc)[0].raw
    expect(readPath(raw, [...WT, 'custom_sink_class'])).toBeUndefined()
    expect(readPath(raw, [...WT, 'module_path'])).toBe('sinks/my_sink.py')
  })

  it('delta → custom prunes the delta options (custom does not use them)', () => {
    const doc = freshDoc(DELTA_YAML)
    applyDiscriminatorChange(doc, 'w_sink', sinkTypePath, 'custom', bp)
    const raw = listActions(doc)[0].raw
    expect(readPath(raw, [...WT, 'options'])).toBeUndefined()
  })
})

describe('write/sink foreachbatch handler toggle — replay prune', () => {
  const bp = toggleBranchPaths(toggle(sink, [...WT, '__handler']))

  it('switching module_path → batch_handler prunes module_path', () => {
    const doc = freshDoc(FB_MODULE_YAML)
    applyDiscriminatorChange(doc, 'w_sink', undefined, 'batch_handler', bp)
    const raw = listActions(doc)[0].raw
    expect(readPath(raw, [...WT, 'module_path'])).toBeUndefined()
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

const WRITE_ACTION = (subType: string, raw: Record<string, unknown>): ActionRead => ({
  name: 'w_sink',
  kind: 'write',
  subType,
  target: '',
  sources: [],
  dependsOn: [],
  raw: { name: 'w_sink', type: 'write', ...raw },
  index: 0,
})

const MV_SQL_YAML = `pipeline: gold
flowgroup: summary
actions:
  - name: w_sink
    type: write
    write_target:
      type: materialized_view
      catalog: main
      schema: gold
      table: customer_summary
      sql: SELECT 1
`

const SINK_EMPTY_YAML = `pipeline: gold
flowgroup: exports
actions:
  - name: w_sink
    type: write
    source: v_metrics
    write_target:
      type: sink
      sink_name: analytics_export
`

describe('write/materialized_view — rendered through ActionModalEditor', () => {
  it('renders the query-source 3-way toggle (Inline SQL active when sql is present)', () => {
    open('pipelines/gold/summary.yaml', MV_SQL_YAML, WRITE_ACTION('materialized_view', {}))
    expect(screen.getByRole('radio', { name: 'Source view(s)' })).toBeInTheDocument()
    expect(screen.getByRole('radio', { name: 'Inline SQL' })).toBeInTheDocument()
    expect(screen.getByRole('radio', { name: 'SQL file' })).toBeInTheDocument()
    expect(screen.getByRole('radio', { name: 'Inline SQL' })).toHaveAttribute('data-state', 'on')
  })

  it('switching the toggle to SQL file reveals a FileRefField (Browse)', async () => {
    open('pipelines/gold/summary.yaml', MV_SQL_YAML, WRITE_ACTION('materialized_view', {}))
    await userEvent.setup().click(screen.getByRole('radio', { name: 'SQL file' }))
    expect(screen.getByRole('radio', { name: 'SQL file' })).toHaveAttribute('data-state', 'on')
    expect(screen.getByRole('button', { name: /^browse$/i })).toBeInTheDocument()
  })
})

describe('write/sink — rendered through ActionModalEditor', () => {
  it('renders sink_type segmented with NOTHING selected (explicit empty state)', () => {
    open('pipelines/gold/exports.yaml', SINK_EMPTY_YAML, WRITE_ACTION('sink', {}))
    for (const name of ['delta', 'kafka', 'custom', 'foreachbatch']) {
      const radio = screen.getByRole('radio', { name })
      expect(radio).toBeInTheDocument()
      expect(radio).not.toHaveAttribute('data-state', 'on')
    }
  })

  it('custom sink renders module_path as a FileRefField plus custom_sink_class', () => {
    open(
      'pipelines/gold/exports.yaml',
      CUSTOM_YAML,
      WRITE_ACTION('sink', {
        write_target: {
          type: 'sink',
          sink_type: 'custom',
          module_path: 'sinks/my_sink.py',
          custom_sink_class: 'MyCustomDataSink',
        },
      }),
    )
    expect(screen.getByRole('radio', { name: 'custom' })).toHaveAttribute('data-state', 'on')
    expect(screen.getByRole('button', { name: /^browse$/i })).toBeInTheDocument()
    expect(screen.getByLabelText('Custom sink class')).toBeInTheDocument()
  })

  it('foreachbatch renders the module_path ⊕ batch_handler toggle (module_path is a FileRefField)', () => {
    open(
      'pipelines/gold/exports.yaml',
      FB_MODULE_YAML,
      WRITE_ACTION('sink', {
        write_target: {
          type: 'sink',
          sink_type: 'foreachbatch',
          module_path: 'batch_handlers/my_handler.py',
        },
      }),
    )
    expect(screen.getByRole('radio', { name: 'Module path' })).toBeInTheDocument()
    expect(screen.getByRole('radio', { name: 'Inline handler' })).toBeInTheDocument()
    expect(screen.getByRole('radio', { name: 'Module path' })).toHaveAttribute('data-state', 'on')
    expect(screen.getByRole('button', { name: /^browse$/i })).toBeInTheDocument()
  })
})
