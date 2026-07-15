import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import type { ActionRead, FlowgroupDocHandle } from '@/lib/flowgroup-doc'
import { listActions, parseFlowgroupFile, selectFlowgroupAt } from '@/lib/flowgroup-doc'
import { useWorkspaceStore } from '@/store/workspaceStore'

// ActionModalEditor stages edits on a DETACHED handle (record-and-replay):
// nothing touches the shared documentStore/workspace buffer until Save replays
// the recorded mutators through the REAL `commit`, then persists. So the two
// seams that reach the store/disk are mocked as spies at the module boundary —
// `useFlowgroupDoc.commit` (the real-doc replay) and `persistBufferToDisk` (the
// PUT) — while the detached parse + staged reads run for real off a seeded
// workspace buffer.
const { commitSpy } = vi.hoisted(() => ({ commitSpy: vi.fn<(fn: (doc: FlowgroupDocHandle) => void) => boolean>() }))

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

// The shell threads tokenComplete=true into every string field, so text fields
// render TokenAutocomplete — stub its env/ui seams (as the FieldRenderer suite
// does) so a bare mount needs no provider tree or network.
vi.mock('@/hooks/useEnvironments', () => ({
  useEnvironmentResolved: vi.fn(() => ({ data: { tokens: {} } })),
}))
vi.mock('@/store/uiStore', () => ({
  useUIStore: (sel: (s: { selectedEnv: string }) => unknown) => sel({ selectedEnv: 'dev' }),
}))

// transform → the shell renders MetadataMultiSelect (network-bound). Stub the
// hook so it resolves to an empty catalogue synchronously.
vi.mock('@/hooks/useOperationalMetadata', () => ({
  useOperationalMetadata: () => ({ data: { columns: [] }, isLoading: false, error: null }),
}))

// The sql_path field renders FileRefField, which probes companion-file existence.
vi.mock('../useCompanionFile', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../useCompanionFile')>()
  return { ...actual, useCompanionFile: () => ({ status: 'unavailable' as const, create: vi.fn() }) }
})

import { persistBufferToDisk } from '@/workspace/persistBuffer'
import { ActionModalEditor } from '../ActionModalEditor'

const mockPersist = vi.mocked(persistBufferToDisk)

const PATH = 'pipelines/silver/orders.yaml'

const YAML = `# Silver orders
pipeline: silver
flowgroup: orders

actions:
  - name: t_orders
    type: transform
    transform_type: sql
    source: v_orders_raw
    sql: SELECT * FROM v_orders_raw
    target: v_orders
`

const ACTION: ActionRead = {
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

function wrapper({ children }: { children: ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return <QueryClientProvider client={qc}>{children}</QueryClientProvider>
}

function seedAndRender(props?: {
  onSaved?: () => void
  onCancel?: () => void
  onOpenCodeView?: () => void
}) {
  useWorkspaceStore.getState().openBuffer(PATH, { content: YAML, exists: true })
  return render(
    <ActionModalEditor
      filePath={PATH}
      docKind="flowgroup"
      action={ACTION}
      actionId="t_orders"
      onSaved={props?.onSaved}
      onCancel={props?.onCancel}
      onOpenCodeView={props?.onOpenCodeView}
    />,
    { wrapper },
  )
}

/** Type + commit (blur) a value into the DraftInput/TokenAutocomplete field. */
function editField(label: string, value: string) {
  const input = screen.getByLabelText(label)
  fireEvent.change(input, { target: { value } })
  fireEvent.blur(input)
}

beforeEach(() => {
  vi.clearAllMocks()
  commitSpy.mockReturnValue(true)
  mockPersist.mockResolvedValue(true)
  useWorkspaceStore.setState({ buffers: [], tabs: [], activePath: null })
  // The data_quality sub-type renders Radix Select enums (mode/readMode) and the
  // sub-type switch opens a Radix AlertDialog; both lean on pointer-capture /
  // scroll APIs jsdom lacks. Stubbing them keeps those mounts from crashing.
  Element.prototype.scrollIntoView = vi.fn()
  Element.prototype.hasPointerCapture = vi.fn(() => false) as never
  Element.prototype.setPointerCapture = vi.fn()
  Element.prototype.releasePointerCapture = vi.fn()
})

const DQ_PATH = 'pipelines/silver/dq_orders.yaml'

const DQ_YAML = `# Silver DQ
pipeline: silver
flowgroup: orders

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
    description: Validate orders
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
    description: 'Validate orders',
  },
  index: 0,
}

describe('ActionModalEditor — staged-save shell', () => {
  it('renders the read-only name, interactive sub-type discriminator, spec sections, and metadata', () => {
    seedAndRender()
    // Read-only name (no rename control).
    expect(screen.getByText('t_orders')).toBeInTheDocument()
    // Interactive sub-type discriminator (segmented) of the kind's sub-types,
    // with the current sub-type (SQL transform) selected.
    expect(screen.getByRole('radiogroup', { name: 'Action sub-type' })).toBeInTheDocument()
    const sqlSegment = screen.getByRole('radio', { name: 'SQL transform' })
    expect(sqlSegment).toBeInTheDocument()
    expect(sqlSegment).toHaveAttribute('aria-checked', 'true')
    // The transform/sql spec's single group + its fields render.
    expect(screen.getByText('Query')).toBeInTheDocument()
    expect(screen.getByText('Source view(s)')).toBeInTheDocument()
    expect(screen.getByText('Target view')).toBeInTheDocument()
    // Shell-level metadata section (transform → MetadataMultiSelect).
    expect(screen.getByText('Operational metadata')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'All' })).toBeInTheDocument()
  })

  it('editing a field then Cancel discards — no commit, no persist, no onSaved', () => {
    const onCancel = vi.fn()
    const onSaved = vi.fn()
    seedAndRender({ onCancel, onSaved })

    editField('Target view', 'v_orders_v2')
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))

    expect(onCancel).toHaveBeenCalledTimes(1)
    expect(commitSpy).not.toHaveBeenCalled()
    expect(mockPersist).not.toHaveBeenCalled()
    expect(onSaved).not.toHaveBeenCalled()
  })

  it('editing a field then Save replays through commit once, persists once, then onSaved', async () => {
    const onSaved = vi.fn()
    seedAndRender({ onSaved })

    editField('Target view', 'v_orders_v2')
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    await waitFor(() => expect(commitSpy).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(mockPersist).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(onSaved).toHaveBeenCalledTimes(1))

    // The recorded mutator closes over actionId/path/value (NOT the working
    // doc), so replaying it against a fresh real doc applies the staged edit.
    const replay = commitSpy.mock.calls[0][0]
    const doc = selectFlowgroupAt(parseFlowgroupFile(YAML), 0) as FlowgroupDocHandle
    replay(doc)
    expect(listActions(doc)[0].raw.target).toBe('v_orders_v2')
    // Persisted the same file.
    expect(mockPersist.mock.calls[0][0]).toBe(PATH)
  })

  it('a failed persist (simulated 412) surfaces the soft banner and never fires onSaved', async () => {
    const onSaved = vi.fn()
    const onOpenCodeView = vi.fn()
    mockPersist.mockResolvedValue(false)
    seedAndRender({ onSaved, onOpenCodeView })

    editField('Target view', 'v_orders_v2')
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    // The anti-dead-end banner appears with an Open-in-Code-view escape.
    const escape = await screen.findByRole('button', { name: /open in code view/i })
    expect(screen.getByRole('alert')).toBeInTheDocument()
    expect(onSaved).not.toHaveBeenCalled()

    fireEvent.click(escape)
    expect(onOpenCodeView).toHaveBeenCalledTimes(1)
  })

  it('switching the header sub-type resets destructively (staged), preserving name/target/description', async () => {
    const onSaved = vi.fn()
    useWorkspaceStore.getState().openBuffer(DQ_PATH, { content: DQ_YAML, exists: true })
    render(
      <ActionModalEditor
        filePath={DQ_PATH}
        docKind="flowgroup"
        action={DQ_ACTION}
        actionId="dq_orders"
        onSaved={onSaved}
      />,
      { wrapper },
    )
    // The data_quality quarantine block is initially visible (mode=quarantine).
    expect(screen.getByText('DLQ table')).toBeInTheDocument()

    const user = userEvent.setup()
    // Switch the discriminator to SQL transform → a confirm dialog appears.
    await user.click(screen.getByRole('radio', { name: 'SQL transform' }))
    await user.click(await screen.findByRole('button', { name: 'Switch' }))

    // The new sql sections render; the old data_quality quarantine block is gone.
    expect(screen.getByText('Query')).toBeInTheDocument()
    expect(screen.queryByText('DLQ table')).toBeNull()

    // Save replays the whole staged batch (deletes + reseed) in ONE commit.
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await waitFor(() => expect(commitSpy).toHaveBeenCalledTimes(1))

    // Replay the recorded staged mutators onto a fresh real doc and read back.
    const replay = commitSpy.mock.calls[0][0]
    const doc = selectFlowgroupAt(parseFlowgroupFile(DQ_YAML), 0) as FlowgroupDocHandle
    replay(doc)
    const raw = listActions(doc)[0].raw
    // Discriminator moved to sql; the old sub-type's fields (incl. quarantine) gone.
    expect(raw.transform_type).toBe('sql')
    expect(raw.quarantine).toBeUndefined()
    expect(raw.mode).toBeUndefined()
    expect(raw.expectations_file).toBeUndefined()
    // Preserved keys survive untouched.
    expect(raw.name).toBe('dq_orders')
    expect(raw.target).toBe('v_orders_validated')
    expect(raw.description).toBe('Validate orders')
  })
})
