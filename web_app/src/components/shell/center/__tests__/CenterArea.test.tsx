import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { CenterArea } from '../CenterArea'
import {
  useWorkspaceStore,
  workspaceTabId,
  type EditorBuffer,
  type WorkspaceTabRef,
} from '../../../../store/workspaceStore'
import { useDocumentStore } from '../../../../store/documentStore'
import { loadBufferContent } from '../../../workspace/flowgroupBuffers'

// Heavy view bodies are mocked to sentinels: the subject under test is
// CenterArea's tab-kind / EntityTab-view routing + the view-switch flush and
// document-handle lifecycle, not the form/graph/editor internals.
vi.mock('../CodeView', () => ({
  CodeView: () => <div data-testid="code-view" />,
}))
vi.mock('../../../entity/GraphView', () => ({
  GraphView: () => <div data-testid="graph-view" />,
}))
vi.mock('../../../entity/ConfigFormView', () => ({
  ConfigFormView: () => <div data-testid="config-form-view" />,
}))
vi.mock('../ProjectMapView', () => ({
  default: () => <div data-testid="project-map" />,
  ProjectMapView: () => <div data-testid="project-map" />,
}))

// The YAML view mock installs a fake Monaco handle on editorRef so the
// view-switch flush (which reads editorRef.current.getValue()) can be exercised
// without a real editor. `fakeEditorValue` is the "typed but not yet captured"
// text; default matches buffer('…').content so unrelated switches are no-ops.
let fakeEditorValue = 'x'
vi.mock('../YamlView', () => ({
  YamlView: ({ editorRef }: { editorRef: { current: unknown } }) => {
    if (editorRef) {
      editorRef.current = {
        getValue: () => fakeEditorValue,
        setValue: () => {},
        setYamlMarkers: () => {},
      }
    }
    return <div data-testid="yaml-view" />
  },
}))
vi.mock('../../../workspace/flowgroupBuffers', () => ({ loadBufferContent: vi.fn() }))

function buffer(path: string, over: Partial<EditorBuffer> = {}): EditorBuffer {
  return {
    path,
    language: 'yaml',
    category: 'yaml',
    content: 'x',
    originalContent: 'x',
    isDirty: false,
    isSaving: false,
    etag: null,
    exists: true,
    isNew: false,
    loading: false,
    loadFailed: false,
    ...over,
  }
}

function setWorkspace(over: {
  tabs?: WorkspaceTabRef[]
  activePath?: string | null
  buffers?: EditorBuffer[]
}) {
  useWorkspaceStore.setState({
    tabs: over.tabs ?? [],
    activePath: over.activePath ?? null,
    buffers: over.buffers ?? [],
    projectRoot: '/proj',
    restoredDirtyCount: 0,
  })
}

function renderCenter() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity } },
  })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  return render(<CenterArea />, { wrapper })
}

const ENTITY = {
  pipeline: 'p',
  flowgroup: 'f',
  filePath: 'pipelines/p/f.yaml',
  docKind: 'flowgroup' as const,
}

beforeEach(() => {
  fakeEditorValue = 'x'
  setWorkspace({})
  useDocumentStore.setState({ docs: {} })
  vi.mocked(loadBufferContent).mockClear()
})

describe('CenterArea', () => {
  it('shows the empty state when no tab is open', () => {
    renderCenter()
    expect(screen.getByText('No entity open')).toBeInTheDocument()
  })

  it('routes an EntityTab graph view to the graph view', async () => {
    setWorkspace({
      tabs: [{ kind: 'entity', ...ENTITY, view: 'graph' }],
      activePath: 'entity:p/f',
    })
    renderCenter()
    expect(await screen.findByTestId('graph-view')).toBeInTheDocument()
  })

  it('routes an EntityTab code view to the Code view', async () => {
    setWorkspace({
      tabs: [{ kind: 'entity', ...ENTITY, view: 'code' }],
      activePath: 'entity:p/f',
    })
    renderCenter()
    expect(await screen.findByTestId('code-view')).toBeInTheDocument()
    // The Code view — not the graph — is mounted for this tab.
    expect(screen.queryByTestId('graph-view')).not.toBeInTheDocument()
  })

  it('renders the Graph|Code switcher for an entity tab (no Form/YAML)', async () => {
    setWorkspace({
      tabs: [{ kind: 'entity', ...ENTITY, view: 'graph' }],
      activePath: 'entity:p/f',
    })
    renderCenter()
    const tablist = screen.getByRole('tablist', { name: 'Entity view' })
    expect(tablist).toBeInTheDocument()
    // Flowgroups get Graph | Code only — Form and YAML are retired.
    expect(screen.queryByRole('tab', { name: 'Form' })).not.toBeInTheDocument()
    expect(screen.queryByRole('tab', { name: 'YAML' })).not.toBeInTheDocument()
    // The Graph tab is selected; clicking Code switches the view live.
    await userEvent.setup().click(screen.getByRole('tab', { name: 'Code' }))
    expect(useWorkspaceStore.getState().tabs[0]).toMatchObject({ kind: 'entity', view: 'code' })
  })

  it('⌘2 / ⌘1 switch the entity view between Code and Graph', () => {
    setWorkspace({
      tabs: [{ kind: 'entity', ...ENTITY, view: 'graph' }],
      activePath: 'entity:p/f',
    })
    renderCenter()
    // ⌘2 → Code, ⌘1 → Graph (⌘3 is gone — Form/YAML are retired).
    fireEvent.keyDown(window, { key: '2', metaKey: true })
    expect(useWorkspaceStore.getState().tabs[0]).toMatchObject({ kind: 'entity', view: 'code' })
    fireEvent.keyDown(window, { key: '1', metaKey: true })
    expect(useWorkspaceStore.getState().tabs[0]).toMatchObject({ kind: 'entity', view: 'graph' })
    // ⌘3 no longer maps to a view — no churn.
    const before = useWorkspaceStore.getState().tabs
    fireEvent.keyDown(window, { key: '3', metaKey: true })
    expect(useWorkspaceStore.getState().tabs).toBe(before)
  })

  it('routes a config Form tab to the config Form view with a Form|YAML switcher (no Graph)', async () => {
    setWorkspace({
      tabs: [{ kind: 'config', path: 'lhp.yaml', configKind: 'project', view: 'form' }],
      activePath: 'config:lhp.yaml',
      buffers: [buffer('lhp.yaml')],
    })
    renderCenter()
    expect(await screen.findByTestId('config-form-view')).toBeInTheDocument()
    expect(screen.getByRole('tablist', { name: 'Config view' })).toBeInTheDocument()
    // Config surfaces get Form | YAML only — never a Graph view.
    expect(screen.queryByRole('tab', { name: 'Graph' })).not.toBeInTheDocument()
    // Toggling to YAML switches the view live.
    await userEvent.setup().click(screen.getByRole('tab', { name: 'YAML' }))
    expect(useWorkspaceStore.getState().tabs[0]).toMatchObject({ kind: 'config', view: 'yaml' })
  })

  it('loads the buffer for a freshly-opened config tab that has none (fetch-on-open)', async () => {
    // openConfigTab opens NO buffer; a Form body reads it via documentStore, so
    // without fetch-on-open it would skeleton forever (T3.1 review gotcha).
    setWorkspace({
      tabs: [{ kind: 'config', path: 'lhp.yaml', configKind: 'project', view: 'form' }],
      activePath: 'config:lhp.yaml',
      buffers: [],
    })
    renderCenter()
    await waitFor(() => {
      expect(useWorkspaceStore.getState().buffers.some((b) => b.path === 'lhp.yaml')).toBe(true)
    })
    expect(vi.mocked(loadBufferContent)).toHaveBeenCalledWith('lhp.yaml')
  })

  it('routes the project-map tab to the project map view', async () => {
    setWorkspace({ tabs: [{ kind: 'project-map' }], activePath: 'project-map' })
    renderCenter()
    expect(await screen.findByTestId('project-map')).toBeInTheDocument()
  })

  it('routes a legacy designer tab to the graph view', async () => {
    setWorkspace({
      tabs: [
        {
          kind: 'designer',
          id: 'designer:p/f',
          pipeline: 'p',
          flowgroup: 'f',
          filePath: 'pipelines/p/f.yaml',
        },
      ],
      activePath: 'designer:p/f',
    })
    renderCenter()
    expect(await screen.findByTestId('graph-view')).toBeInTheDocument()
  })

  it('flushes the pending Monaco text on a same-tab YAML→Form switch (lost-update fix)', () => {
    // Exercised on a config tab: it still has a Monaco YAML view whose buffer
    // path drops to null on the switch to Form (the same yamlBufferPathFor
    // transition that triggers the flush). Entity tabs route Monaco through the
    // Code view now (filled by T-code), so the mechanism is verified here.
    setWorkspace({
      tabs: [{ kind: 'config', path: 'lhp.yaml', configKind: 'project', view: 'yaml' }],
      activePath: 'config:lhp.yaml',
      buffers: [buffer('lhp.yaml', { content: 'name: old', originalContent: 'name: old' })],
    })
    // A document is open so the flush's reparse fires (echo token updates).
    useDocumentStore.getState().open('lhp.yaml', 'project')
    renderCenter()

    // Simulate text typed <500ms before the switch (capture never fired).
    fakeEditorValue = 'name: typed'
    act(() => {
      useWorkspaceStore.getState().setTabView('config:lhp.yaml', 'form')
    })

    const buf = useWorkspaceStore.getState().buffers.find((b) => b.path === 'lhp.yaml')
    expect(buf?.content).toBe('name: typed')
    // The config document's handle re-anchored to the just-typed text.
    expect(useDocumentStore.getState().docs['lhp.yaml']?.lastSerializedText).toBe('name: typed')
  })

  it('drops the entity document handle when the tab closes', async () => {
    const user = userEvent.setup()
    setWorkspace({
      tabs: [{ kind: 'entity', ...ENTITY, view: 'graph' }],
      activePath: 'entity:p/f',
      buffers: [buffer('pipelines/p/f.yaml')],
    })
    useDocumentStore.getState().open('pipelines/p/f.yaml', 'flowgroup')
    expect(useDocumentStore.getState().docs['pipelines/p/f.yaml']).toBeDefined()
    renderCenter()

    await user.click(screen.getByRole('button', { name: 'Close p·f' }))
    expect(useDocumentStore.getState().docs['pipelines/p/f.yaml']).toBeUndefined()
  })

  it('closing the active middle tab focuses the neighbour and removes its backing buffer', async () => {
    const user = userEvent.setup()
    setWorkspace({
      tabs: [
        { kind: 'file', path: 'a.yaml' },
        { kind: 'entity', ...ENTITY, view: 'graph' },
        { kind: 'file', path: 'c.yaml' },
      ],
      activePath: 'entity:p/f',
      buffers: [buffer('a.yaml'), buffer('pipelines/p/f.yaml'), buffer('c.yaml')],
    })
    renderCenter()

    // Entity tab label is `${pipeline}·${flowgroup}` → close button "Close p·f".
    await user.click(screen.getByRole('button', { name: 'Close p·f' }))

    const s = useWorkspaceStore.getState()
    // The neighbour that slid into the closed slot takes focus.
    expect(s.activePath).toBe('c.yaml')
    expect(s.tabs.map((t) => workspaceTabId(t))).toEqual(['a.yaml', 'c.yaml'])
    // A clean close of a buffer-backed tab removes its buffer (no persist leak).
    expect(s.buffers.some((b) => b.path === 'pipelines/p/f.yaml')).toBe(false)
  })

  it('closing a dirty buffer-backed tab prompts, then discards and closes on confirm', async () => {
    const user = userEvent.setup()
    setWorkspace({
      tabs: [{ kind: 'entity', ...ENTITY, view: 'graph' }],
      activePath: 'entity:p/f',
      buffers: [buffer('pipelines/p/f.yaml', { isDirty: true })],
    })
    renderCenter()

    await user.click(screen.getByRole('button', { name: 'Close p·f' }))
    // Dirty → the discard prompt appears; the tab/buffer are untouched so far.
    expect(await screen.findByRole('alertdialog')).toBeInTheDocument()
    expect(screen.getByText('Discard unsaved changes?')).toBeInTheDocument()
    expect(useWorkspaceStore.getState().tabs).toHaveLength(1)

    await user.click(screen.getByRole('button', { name: 'Discard changes' }))
    const s = useWorkspaceStore.getState()
    expect(s.tabs).toHaveLength(0)
    expect(s.buffers).toHaveLength(0)
    expect(s.activePath).toBeNull()
  })

  it('closing a clean non-active tab does not steal focus from the active tab', async () => {
    const user = userEvent.setup()
    setWorkspace({
      tabs: [
        { kind: 'file', path: 'a.yaml' },
        { kind: 'file', path: 'b.yaml' },
      ],
      activePath: 'a.yaml',
      buffers: [buffer('a.yaml'), buffer('b.yaml')],
    })
    renderCenter()

    await user.click(screen.getByRole('button', { name: 'Close b.yaml' }))
    const s = useWorkspaceStore.getState()
    // Focus stays on the untouched active tab; only b.yaml's buffer is gone.
    expect(s.activePath).toBe('a.yaml')
    expect(s.buffers.map((b) => b.path)).toEqual(['a.yaml'])
    expect(s.tabs.map((t) => workspaceTabId(t))).toEqual(['a.yaml'])
  })
})
