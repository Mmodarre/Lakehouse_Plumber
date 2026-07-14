import { Fragment, type ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import { TooltipProvider } from '@/components/ui/tooltip'
import { serializeFlowgroupFile } from '@/lib/flowgroup-doc'
import type { FlowgroupFileHandle } from '@/lib/flowgroup-doc'
import { entityTabId, useWorkspaceStore, type DocKind } from '@/store/workspaceStore'
import { useDocumentStore } from '@/store/documentStore'
import { useLayoutStore } from '@/store/layoutStore'
import { useSelectionStore } from '@/store/selectionStore'
import { writeFile } from '@/api/files'
import { GraphView } from '../GraphView'

// GraphView drives the real documentStore + workspaceStore + layoutStore (same
// data core as FlowgroupFormView), so structural edits are exercised through
// the actual comment-preserving buffer. Only @xyflow/react (heavy in jsdom)
// and elk-api (spawns a Web Worker) are mocked — the REAL useElkLayout mapping,
// useDesignerCompose, and toDesignerGraph wiring all run.

const rf = vi.hoisted(() => ({ fitView: vi.fn() }))

// The React Flow stub renders one button per node (forwarding the component's
// real onNodeClick / onNodeDoubleClick with the full node object, exactly as
// React Flow does at runtime) and ALSO renders its children so the in-canvas
// Panels (Add action, NodeActionsToolbar) are exercised. Panel echoes children.
// It additionally surfaces each node's `data.onEditAction` as an `rf-edit-*`
// button so the pencil affordance's GraphView wiring is testable (the real
// pencil lives in DesignerActionNode, which this stub replaces).
vi.mock('@xyflow/react', () => {
  type StubNode = { id: string; type?: string; data: Record<string, unknown> }
  return {
    ReactFlow: ({
      nodes,
      onNodeClick,
      onNodeDoubleClick,
      children,
    }: {
      nodes: StubNode[]
      onNodeClick?: (e: unknown, node: StubNode) => void
      onNodeDoubleClick?: (e: unknown, node: StubNode) => void
      children?: ReactNode
    }) => (
      <div data-testid="react-flow">
        {nodes.map((n) => (
          <Fragment key={n.id}>
            <button
              data-testid={`rf-node-${n.id}`}
              onClick={(e) => onNodeClick?.(e, n)}
              onDoubleClick={(e) => onNodeDoubleClick?.(e, n)}
            >
              {String(n.data.label ?? n.id)}
            </button>
            {typeof n.data.onEditAction === 'function' && (
              <button
                data-testid={`rf-edit-${n.id}`}
                onClick={() => (n.data.onEditAction as (id: string) => void)(n.id)}
              >
                edit {n.id}
              </button>
            )}
          </Fragment>
        ))}
        {children}
      </div>
    ),
    ReactFlowProvider: ({ children }: { children?: ReactNode }) => <>{children}</>,
    Panel: ({ children }: { children?: ReactNode }) => <div>{children}</div>,
    Background: () => null,
    BackgroundVariant: { Dots: 'dots' },
    Controls: () => null,
    Handle: () => null,
    EdgeLabelRenderer: ({ children }: { children?: ReactNode }) => <>{children}</>,
    Position: { Left: 'left', Right: 'right', Top: 'top', Bottom: 'bottom' },
    useReactFlow: () => ({ fitView: rf.fitView }),
    getBezierPath: () => ['', 0, 0, 0, 0] as const,
  }
})

// elk-api spawns a Web Worker at module load (unavailable in jsdom). Echo the
// graph back with positions so the real useElkLayout node mapping still runs.
vi.mock('elkjs/lib/elk-api.js', () => ({
  default: class {
    async layout(graph: { children?: Array<{ x?: number; y?: number }> }) {
      return {
        ...graph,
        children: (graph.children ?? []).map((c, i) => ({ ...c, x: i * 100, y: 0 })),
      }
    }
  },
}))

// The action-editor modal persists to disk on Save (persistBufferToDisk →
// writeFile). Stub the PUT so the failed-persist dead-end / escape path is
// testable; everything else in @/api/files stays real.
vi.mock('@/api/files', async (importActual) => {
  const actual = await importActual<typeof import('@/api/files')>()
  return { ...actual, writeFile: vi.fn() }
})

const mockWriteFile = vi.mocked(writeFile)

const PATH = 'pipelines/bronze/orders.yaml'
const TAB_ID = entityTabId('bronze', 'orders')

// 5 actions with distinct names (→ 5 canvas nodes) + inline comments that must
// survive structural edits.
const YAML = `# Bronze orders flowgroup
pipeline: bronze
flowgroup: orders

actions:
  # Auto Loader ingest
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /mnt/raw/orders
      format: json
    target: v_orders_raw

  # Clean + normalise
  - name: clean_orders
    type: transform
    transform_type: sql
    source: v_orders_raw
    sql: SELECT * FROM v_orders_raw
    target: v_orders_clean

  # Persist to bronze
  - name: write_orders
    type: write
    source: v_orders_clean
    write_target:
      type: streaming_table
      database: bronze
      table: orders

  # Uniqueness guard
  - name: test_orders_unique
    type: test
    test_type: uniqueness
    source: v_orders_clean
    columns: [order_id]
`

const EMPTY = `# Empty bronze flowgroup
pipeline: bronze
flowgroup: orders
actions: []
`

const BROKEN = `# Bronze orders flowgroup
pipeline: bronze
flowgroup: "orders
`

function providers({ children }: { children: ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return (
    <QueryClientProvider client={qc}>
      <TooltipProvider delayDuration={0}>{children}</TooltipProvider>
    </QueryClientProvider>
  )
}

function seedAndRender(content = YAML, docKind: DocKind = 'flowgroup') {
  useWorkspaceStore.getState().openBuffer(PATH, { content, exists: true })
  useWorkspaceStore
    .getState()
    .openEntityTab('bronze', 'orders', PATH, { docKind, view: 'graph', activate: false })
  useDocumentStore.getState().open(PATH, docKind)
  return render(<GraphView tabId={TAB_ID} filePath={PATH} docKind={docKind} />, {
    wrapper: providers,
  })
}

function bufferContent(): string {
  return useWorkspaceStore.getState().buffers.find((b) => b.path === PATH)!.content
}

function entityView(): string | undefined {
  const tab = useWorkspaceStore.getState().tabs.find((t) => t.kind === 'entity')
  return tab?.kind === 'entity' ? tab.view : undefined
}

beforeEach(() => {
  localStorage.clear()
  rf.fitView.mockClear()
  mockWriteFile.mockReset()
  useWorkspaceStore.setState({
    buffers: [],
    tabs: [],
    activePath: null,
    projectRoot: null,
    restoredDirtyCount: 0,
  })
  useDocumentStore.setState({ docs: {} })
  useLayoutStore.setState({ viewerMode: false })
  useSelectionStore.setState({ byTab: {} })
})

describe('GraphView — rendering', () => {
  it('renders one canvas node per action of a multi-action flowgroup', async () => {
    seedAndRender()
    await screen.findByTestId('rf-node-load_orders')
    expect(screen.getByTestId('rf-node-clean_orders')).toBeInTheDocument()
    expect(screen.getByTestId('rf-node-write_orders')).toBeInTheDocument()
    expect(screen.getByTestId('rf-node-test_orders_unique')).toBeInTheDocument()
    expect(screen.getAllByTestId(/^rf-node-/)).toHaveLength(4)
  })
})

describe('GraphView — first-mount / loading-vs-empty (regression: empty-graph bug)', () => {
  // Buffer already loaded, entity document NOT pre-opened — the view's own
  // useFlowgroupDoc effect must open() it post-mount (the first-flowgroup-in-a-
  // fresh-session path where the graph used to render "No flowgroup here"
  // forever until a re-mount).
  function seedNoPreopen(content = YAML) {
    useWorkspaceStore.getState().openBuffer(PATH, { content, exists: true })
    useWorkspaceStore
      .getState()
      .openEntityTab('bronze', 'orders', PATH, { docKind: 'flowgroup', view: 'graph', activate: false })
    return render(<GraphView tabId={TAB_ID} filePath={PATH} docKind="flowgroup" />, {
      wrapper: providers,
    })
  }

  it('renders the nodes on FIRST mount when open() runs post-mount at version 0', async () => {
    seedNoPreopen()
    // Must resolve to the real graph without a re-mount, and never wedge on the
    // empty state (the bug: derived frozen on the null projection at version 0).
    await screen.findByTestId('rf-node-load_orders')
    expect(screen.getAllByTestId(/^rf-node-/)).toHaveLength(4)
    expect(screen.queryByText('No flowgroup here')).not.toBeInTheDocument()
  })

  it('shows the loading state (not the empty state) while the buffer is still loading', () => {
    useWorkspaceStore.getState().openBuffer(PATH, { exists: true, loading: true })
    useWorkspaceStore
      .getState()
      .openEntityTab('bronze', 'orders', PATH, { docKind: 'flowgroup', view: 'graph', activate: false })
    render(<GraphView tabId={TAB_ID} filePath={PATH} docKind="flowgroup" />, { wrapper: providers })

    // A still-loading buffer must render the spinner, never "No flowgroup here"
    // (which would be indistinguishable from a genuinely empty file).
    expect(screen.getByRole('status', { name: 'Loading' })).toBeInTheDocument()
    expect(screen.queryByText('No flowgroup here')).not.toBeInTheDocument()
  })

  it('shows "No flowgroup here" only for a genuinely no-flowgroup file (fully synced)', async () => {
    // A file with no flowgroup at index 0 — the ONE case the empty state is for.
    seedAndRender('# just a comment, no flowgroup here\n')
    await screen.findByText('No flowgroup here')
    expect(screen.queryByRole('status', { name: 'Loading' })).not.toBeInTheDocument()
  })
})

describe('GraphView — structural edits round-trip the buffer', () => {
  it('add via the palette appends an action and preserves comments', async () => {
    seedAndRender()
    await screen.findByTestId('rf-node-load_orders')

    fireEvent.click(screen.getByRole('button', { name: 'Add action' }))
    const filter = await screen.findByLabelText(/filter action types/i)
    fireEvent.change(filter, { target: { value: 'row_count' } })
    fireEvent.click(screen.getByRole('button', { name: /row_count/i }))

    const content = bufferContent()
    expect(content).toContain('test_type: row_count')
    // Existing comments survive the byte-surgical append.
    expect(content).toContain('# Bronze orders flowgroup')
    expect(content).toContain('# Uniqueness guard')
    // Sync invariant: the kept handle serializes to exactly the buffer text.
    const handle = useDocumentStore.getState().docs[PATH].handle as FlowgroupFileHandle
    expect(serializeFlowgroupFile(handle)).toBe(content)
  })

  it('remove via the node toolbar drops the action and preserves comments', async () => {
    seedAndRender()
    const node = await screen.findByTestId('rf-node-clean_orders')

    fireEvent.click(node) // select → structural toolbar appears
    fireEvent.click(screen.getByRole('button', { name: /delete action/i }))

    const content = bufferContent()
    expect(content).not.toContain('name: clean_orders')
    expect(content).toContain('# Bronze orders flowgroup')
    expect(content).toContain('# Persist to bronze')
  })
})

describe('GraphView — publishes the selection to the inspector store', () => {
  it('a node click publishes the resolved source selection keyed by tab id', async () => {
    seedAndRender()
    const node = await screen.findByTestId('rf-node-load_orders')
    expect(useSelectionStore.getState().getSelection(TAB_ID)).toBeNull()

    fireEvent.click(node)

    const sel = useSelectionStore.getState().getSelection(TAB_ID)
    expect(sel).toMatchObject({ mode: 'source', actionId: 'load_orders' })
    expect(sel?.mode === 'source' && sel.action.name).toBe('load_orders')
  })

  it('clears the published selection on unmount', async () => {
    const { unmount } = seedAndRender()
    fireEvent.click(await screen.findByTestId('rf-node-load_orders'))
    expect(useSelectionStore.getState().getSelection(TAB_ID)).not.toBeNull()

    unmount()
    expect(useSelectionStore.getState().getSelection(TAB_ID)).toBeNull()
  })
})

describe('GraphView — double-click opens the action editor modal (Fix #3)', () => {
  it('single click selects but does not open the modal', async () => {
    seedAndRender()
    fireEvent.click(await screen.findByTestId('rf-node-load_orders'))
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  })

  it('double-click a source node opens a modal with its action editor', async () => {
    seedAndRender()
    fireEvent.doubleClick(await screen.findByTestId('rf-node-load_orders'))

    const dialog = await screen.findByRole('dialog')
    expect(within(dialog).getByText('Edit action')).toBeInTheDocument()
    // The editor is bound to the double-clicked action's fields.
    expect(within(dialog).getByLabelText('source.path')).toHaveValue('/mnt/raw/orders')
  })

  it('the node pencil affordance opens the same modal (discoverability fix)', async () => {
    seedAndRender()
    // GraphView threads onEditAction onto each editable node's data; the stub
    // surfaces it as an rf-edit-* button (the real pencil lives in
    // DesignerActionNode — see its own test).
    const editBtn = await screen.findByTestId('rf-edit-load_orders')
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument()

    fireEvent.click(editBtn)

    const dialog = await screen.findByRole('dialog')
    expect(within(dialog).getByText('Edit action')).toBeInTheDocument()
    expect(within(dialog).getByLabelText('source.path')).toHaveValue('/mnt/raw/orders')
  })

  it('every editable action node exposes an edit affordance', async () => {
    seedAndRender()
    await screen.findByTestId('rf-node-load_orders')
    // All 4 action nodes get an rf-edit-* button (GraphView threads
    // onEditAction onto non-external node data); no stray affordances.
    expect(screen.getAllByTestId(/^rf-edit-/)).toHaveLength(4)
  })
})

describe('GraphView — a failed modal save is not a locked dead-end (Fix #2)', () => {
  it('keeps the modal open, locks Save, and Open in Code view escapes to the Code view', async () => {
    // yaml_error → persistBufferToDisk returns false AFTER commit cleared the
    // buffer's dirty flag, so the re-derived action makes Save disabled.
    mockWriteFile.mockResolvedValue({
      written: true,
      path: PATH,
      etag: 'etag-2',
      yaml_error: { line: 1, column: 1, message: 'bad' },
    })
    seedAndRender()
    fireEvent.doubleClick(await screen.findByTestId('rf-node-load_orders'))
    const dialog = await screen.findByRole('dialog')

    fireEvent.change(within(dialog).getByLabelText('source.path'), {
      target: { value: '/mnt/raw/orders_v2' },
    })
    fireEvent.click(within(dialog).getByRole('button', { name: 'Save' }))

    // The escape banner appears and Save is now locked (dead-end proven)…
    const escape = await screen.findByRole('button', { name: /open in code view/i })
    await waitFor(() =>
      expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled(),
    )

    // …but Open in Code view closes the modal and switches to the Code view.
    fireEvent.click(escape)
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
    expect(entityView()).toBe('code')
  })
})

describe('GraphView — readOnly chain', () => {
  it('degraded: shows the banner and jumps to the Code view', async () => {
    seedAndRender()
    await screen.findByTestId('rf-node-load_orders')

    act(() => {
      useWorkspaceStore.getState().updateContent(PATH, BROKEN)
      useDocumentStore.getState().reparse(PATH, BROKEN)
    })

    await screen.findByText('YAML has syntax errors — fix in the Code view.')
    fireEvent.click(screen.getByRole('button', { name: /open code view/i }))
    expect(entityView()).toBe('code')
  })

  it('viewer lens disables authoring without a degraded banner', async () => {
    seedAndRender(EMPTY)
    const addFirst = await screen.findByRole('button', { name: /add first action/i })
    expect(addFirst).toBeEnabled()

    act(() => useLayoutStore.getState().setViewerMode(true))

    expect(screen.getByRole('button', { name: /add first action/i })).toBeDisabled()
    expect(
      screen.queryByText('YAML has syntax errors — fix in the Code view.'),
    ).not.toBeInTheDocument()
  })
})
