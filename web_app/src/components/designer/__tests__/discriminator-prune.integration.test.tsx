import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import type { ActionRead, FlowgroupDocHandle } from '@/lib/flowgroup-doc'
import { listActions, parseFlowgroupFile, selectFlowgroupAt } from '@/lib/flowgroup-doc'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { readPath } from '../specs/helpers'

// ── discriminator prune-on-switch — END-TO-END regression guard ─
//
// Drives the REAL ActionModalEditor shell (Task 3.1c staged record-and-replay)
// through every discriminator the plan added a prune to (Task 0.5). For each
// case it mounts an action with the "from" branch populated, switches the
// discriminator through the ACTUAL UI (segmented enum / oneOfToggle), clicks
// Save, then REPLAYS the recorded staged mutators onto a fresh real doc and
// reads the mapping back. Asserting on that replayed doc — not a live spy —
// makes this a true guard: a future edit that regresses a switch into a stray
// inactive-branch block (which `lhp validate` rejects) fails here.
//
// The seams that reach the store/disk are mocked exactly as ActionModalEditor's
// own suite mocks them: `useFlowgroupDoc.commit` (the real-doc replay target)
// and `persistBufferToDisk` (the PUT). The detached parse + staged reads run
// for real off a seeded workspace buffer.
const { commitSpy } = vi.hoisted(() => ({
  commitSpy: vi.fn<(fn: (doc: FlowgroupDocHandle) => void) => boolean>(),
}))

vi.mock('@/components/entity/useFlowgroupDoc', () => ({
  useFlowgroupDoc: () => ({ commit: commitSpy, readOnly: false }),
}))

vi.mock('@/workspace/persistBuffer', () => ({ persistBufferToDisk: vi.fn() }))

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

// Every string field renders TokenAutocomplete (shell threads tokenComplete);
// stub its env/ui seams so a bare mount needs no provider tree or network.
vi.mock('@/hooks/useEnvironments', () => ({
  useEnvironmentResolved: vi.fn(() => ({ data: { tokens: {} } })),
}))
vi.mock('@/store/uiStore', () => ({
  useUIStore: (sel: (s: { selectedEnv: string }) => unknown) => sel({ selectedEnv: 'dev' }),
}))

// transform → MetadataMultiSelect (network-bound). Resolve to an empty catalogue.
vi.mock('@/hooks/useOperationalMetadata', () => ({
  useOperationalMetadata: () => ({ data: { columns: [] }, isLoading: false, error: null }),
}))

// A file-ref branch renders FileRefField, which probes companion-file existence.
vi.mock('../useCompanionFile', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../useCompanionFile')>()
  return { ...actual, useCompanionFile: () => ({ status: 'unavailable' as const, create: vi.fn() }) }
})

import { persistBufferToDisk } from '@/workspace/persistBuffer'
import { ActionModalEditor } from '../ActionModalEditor'

const mockPersist = vi.mocked(persistBufferToDisk)

function wrapper({ children }: { children: ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return <QueryClientProvider client={qc}>{children}</QueryClientProvider>
}

/** Compute the ActionRead exactly as the app does (kind/subType/raw/index). */
function actionFrom(yaml: string, index = 0): ActionRead {
  const doc = selectFlowgroupAt(parseFlowgroupFile(yaml), 0) as FlowgroupDocHandle
  return listActions(doc)[index]
}

/** Seed a buffer and mount the real shell for the file's single action. */
function mount(path: string, yaml: string): ActionRead {
  const action = actionFrom(yaml)
  useWorkspaceStore.getState().openBuffer(path, { content: yaml, exists: true })
  render(
    <ActionModalEditor
      filePath={path}
      docKind="flowgroup"
      action={action}
      actionId={action.name}
    />,
    { wrapper },
  )
  return action
}

/**
 * Replay the ONE staged batch recorded at Save onto a fresh real doc parsed
 * from the same YAML, and read the action mapping back. The recorded mutators
 * close over actionId/path/value (not the working handle), so replaying them
 * against a fresh doc reproduces the staged result end-to-end.
 */
function replayStaged(yaml: string, index = 0): Record<string, unknown> {
  const replay = commitSpy.mock.calls[0][0]
  const doc = selectFlowgroupAt(parseFlowgroupFile(yaml), 0) as FlowgroupDocHandle
  replay(doc)
  return listActions(doc)[index].raw
}

/** Click a segment inside the named radiogroup, then Save; wait for the commit. */
async function switchAndSave(user: ReturnType<typeof userEvent.setup>, group: string, segment: string) {
  await user.click(within(screen.getByRole('radiogroup', { name: group })).getByRole('radio', { name: segment }))
  await user.click(screen.getByRole('button', { name: 'Save' }))
  await waitFor(() => expect(commitSpy).toHaveBeenCalledTimes(1))
}

beforeEach(() => {
  vi.clearAllMocks()
  commitSpy.mockReturnValue(true)
  mockPersist.mockResolvedValue(true)
  useWorkspaceStore.setState({ buffers: [], tabs: [], activePath: null })
  // Radix ToggleGroup / AlertDialog lean on pointer-capture / scroll APIs jsdom
  // lacks; stub them so the segmented mounts + switches don't crash.
  Element.prototype.scrollIntoView = vi.fn()
  Element.prototype.hasPointerCapture = vi.fn(() => false) as never
  Element.prototype.setPointerCapture = vi.fn()
  Element.prototype.releasePointerCapture = vi.fn()
})

// ── (a) transform/data_quality — mode quarantine → dqe prunes quarantine ──
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

// ── (b) write/streaming_table — mode cdc → standard prunes cdc_config ──
const ST_CDC_YAML = `pipeline: silver
flowgroup: st
actions:
  - name: w_customer
    type: write
    source: v_customer_bronze
    write_target:
      type: streaming_table
      mode: cdc
      catalog: main
      schema: silver
      table: customer_dim
      cdc_config:
        keys: [customer_id]
        sequence_by: ts
        scd_type: 1
`

// ── (c) write/streaming_table — mode snapshot_cdc → standard prunes snapshot_cdc_config ──
// ── (d) write/streaming_table — snapshot source ⊕ source_function toggle prune ──
const ST_SNAP_YAML = `pipeline: silver
flowgroup: st
actions:
  - name: w_snap
    type: write
    write_target:
      type: streaming_table
      mode: snapshot_cdc
      catalog: main
      schema: silver
      table: customer_dim
      snapshot_cdc_config:
        source: main.bronze.customer_snapshot
        keys: [customer_id]
        stored_as_scd_type: 1
`

// ── (e) write/sink — sink_type kafka → delta prunes kafka-exclusive keys, keeps shared options ──
const SINK_KAFKA_YAML = `pipeline: silver
flowgroup: snk
actions:
  - name: w_sink
    type: write
    source: v_metrics
    write_target:
      type: sink
      sink_type: kafka
      sink_name: metrics_export
      bootstrap_servers: \${kafka_bootstrap_servers}
      topic: acme.orders
      options:
        kafka.security.protocol: SASL_SSL
`

// ── (f) write/materialized_view — query-source switch keeps EXACTLY ONE of source/sql/sql_path ──
// Seeded with all three present (a legacy invalid file); switching to inline SQL
// must leave sql alone and prune the other two.
const MV_YAML = `pipeline: gold
flowgroup: mv
actions:
  - name: w_mv
    type: write
    source: v_customer_orders
    write_target:
      type: materialized_view
      catalog: main
      schema: gold
      table: customer_summary
      sql: SELECT * FROM v_customer_orders
      sql_path: sql/customer_summary.sql
`

// ── (g) transform/sql — inline ⊕ file toggle prunes the inactive key ──
const TSQL_YAML = `pipeline: silver
flowgroup: tsql
actions:
  - name: t_sql
    type: transform
    transform_type: sql
    source: v_orders_raw
    sql: SELECT * FROM v_orders_raw
    target: v_orders
`

describe('discriminator prune-on-switch — ActionModalEditor end-to-end', () => {
  it('(a) data_quality mode quarantine→dqe writes mode and prunes the quarantine block', async () => {
    const user = userEvent.setup()
    mount('pipelines/silver/dq_orders.yaml', DQ_YAML)
    // Quarantine block visible while mode=quarantine.
    expect(screen.getByText('DLQ table')).toBeInTheDocument()

    await switchAndSave(user, 'Mode', 'dqe')

    const raw = replayStaged(DQ_YAML)
    expect(raw.mode).toBe('dqe')
    expect(readPath(raw, ['quarantine'])).toBeUndefined()
    // Untouched siblings survive.
    expect(raw.source).toBe('v_orders')
    expect(raw.expectations_file).toBe('expectations/orders.yaml')
    expect(raw.target).toBe('v_orders_validated')
  })

  it('(b) streaming_table mode cdc→standard writes mode, prunes cdc_config, keeps shared source', async () => {
    const user = userEvent.setup()
    mount('pipelines/silver/w_customer.yaml', ST_CDC_YAML)
    expect(screen.getByText('CDC configuration')).toBeInTheDocument()

    await switchAndSave(user, 'Write mode', 'standard')

    const raw = replayStaged(ST_CDC_YAML)
    expect(readPath(raw, ['write_target', 'mode'])).toBe('standard')
    expect(readPath(raw, ['write_target', 'cdc_config'])).toBeUndefined()
    // `source` is shared by standard+cdc → survives the switch.
    expect(raw.source).toBe('v_customer_bronze')
    // Target table keys are untouched.
    expect(readPath(raw, ['write_target', 'table'])).toBe('customer_dim')
  })

  it('(c) streaming_table mode snapshot_cdc→standard writes mode and prunes snapshot_cdc_config', async () => {
    const user = userEvent.setup()
    mount('pipelines/silver/w_snap.yaml', ST_SNAP_YAML)

    await switchAndSave(user, 'Write mode', 'standard')

    const raw = replayStaged(ST_SNAP_YAML)
    expect(readPath(raw, ['write_target', 'mode'])).toBe('standard')
    expect(readPath(raw, ['write_target', 'snapshot_cdc_config'])).toBeUndefined()
    expect(readPath(raw, ['write_target', 'catalog'])).toBe('main')
  })

  it('(d) streaming_table snapshot source→source_function prunes the inactive source key', async () => {
    const user = userEvent.setup()
    mount('pipelines/silver/w_snap.yaml', ST_SNAP_YAML)
    // The snapshot source toggle seeds to "Source table" (source key present).

    // The oneOfToggle renders as a SegmentedControl labelled by the field.
    await user.click(
      within(screen.getByRole('radiogroup', { name: 'Snapshot source' })).getByRole('radio', {
        name: 'Source function',
      }),
    )
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(commitSpy).toHaveBeenCalledTimes(1))

    const raw = replayStaged(ST_SNAP_YAML)
    // Inactive branch key pruned; the new branch writes nothing until filled.
    expect(readPath(raw, ['write_target', 'snapshot_cdc_config', 'source'])).toBeUndefined()
    expect(readPath(raw, ['write_target', 'snapshot_cdc_config', 'source_function'])).toBeUndefined()
    // The rest of the snapshot config block survives.
    expect(readPath(raw, ['write_target', 'snapshot_cdc_config', 'keys'])).toEqual(['customer_id'])
    expect(readPath(raw, ['write_target', 'mode'])).toBe('snapshot_cdc')
  })

  it('(e) sink sink_type kafka→delta prunes kafka-exclusive keys and keeps the shared options', async () => {
    const user = userEvent.setup()
    mount('pipelines/silver/w_sink.yaml', SINK_KAFKA_YAML)
    expect(screen.getByText('Kafka sink')).toBeInTheDocument()

    await switchAndSave(user, 'Sink type', 'delta')

    const raw = replayStaged(SINK_KAFKA_YAML)
    expect(readPath(raw, ['write_target', 'sink_type'])).toBe('delta')
    // kafka-exclusive keys pruned.
    expect(readPath(raw, ['write_target', 'bootstrap_servers'])).toBeUndefined()
    expect(readPath(raw, ['write_target', 'topic'])).toBeUndefined()
    // `options` is shared by delta+kafka → survives; `sink_name` is common.
    expect(readPath(raw, ['write_target', 'options', 'kafka.security.protocol'])).toBe('SASL_SSL')
    expect(readPath(raw, ['write_target', 'sink_name'])).toBe('metrics_export')
  })

  it('(f) materialized_view query-source switch keeps EXACTLY ONE of source/sql/sql_path', async () => {
    const user = userEvent.setup()
    mount('pipelines/gold/w_mv.yaml', MV_YAML)
    // Seeds to "Source view(s)" (first present branch); switch to inline SQL.

    await user.click(
      within(screen.getByRole('radiogroup', { name: 'Query source' })).getByRole('radio', {
        name: 'Inline SQL',
      }),
    )
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(commitSpy).toHaveBeenCalledTimes(1))

    const raw = replayStaged(MV_YAML)
    // Exactly one query source remains: the chosen `sql`.
    expect(readPath(raw, ['write_target', 'sql'])).toBe('SELECT * FROM v_customer_orders')
    expect(raw.source).toBeUndefined()
    expect(readPath(raw, ['write_target', 'sql_path'])).toBeUndefined()
  })

  it('(g) transform sql inline→file prunes the inactive sql key', async () => {
    const user = userEvent.setup()
    mount('pipelines/silver/t_sql.yaml', TSQL_YAML)
    // Seeds to "Inline SQL" (sql key present); switch to the file branch.

    await user.click(
      within(screen.getByRole('radiogroup', { name: 'SQL' })).getByRole('radio', { name: 'From file' }),
    )
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(commitSpy).toHaveBeenCalledTimes(1))

    const raw = replayStaged(TSQL_YAML)
    expect(raw.sql).toBeUndefined()
    // The new branch owns sql_path but nothing is written until the user fills it.
    expect(raw.sql_path).toBeUndefined()
    // Untouched siblings survive.
    expect(raw.source).toBe('v_orders_raw')
    expect(raw.target).toBe('v_orders')
  })
})
