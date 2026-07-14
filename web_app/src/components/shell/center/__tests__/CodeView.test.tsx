import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { act, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { CodeView } from '../CodeView'
import {
  entityTabId,
  useWorkspaceStore,
  type EditorBuffer,
  type WorkspaceTabRef,
} from '../../../../store/workspaceStore'
import { useDocumentStore } from '../../../../store/documentStore'
import { useUIStore } from '../../../../store/uiStore'
import { fetchFiles, fetchFileContentWithMeta } from '../../../../api/files'
import { fetchFlowgroupRelatedFiles } from '../../../../api/flowgroups'
import type { FileNode } from '../../../../types/api'

// A stateful fake Monaco handle: getValue()/setValue() read+write shared state,
// so the reused save + flush-on-view-switch wiring and the out-of-band reconcile
// effect run against realistic editor text. The old marker's getValue() returned
// '', which left that wiring unguarded.
const fakeEditor = vi.hoisted(() => {
  const state = { value: '' }
  return {
    state,
    getValue: vi.fn(() => state.value),
    setValue: vi.fn((v: string) => {
      state.value = v
    }),
  }
})

// The editable yaml sub-tab renders through YamlView (mocked here as a marker
// that installs the fake Monaco handle, mirroring the CenterArea test).
vi.mock('../YamlView', () => ({
  YamlView: ({ editorRef }: { editorRef: { current: unknown } }) => {
    if (editorRef) {
      editorRef.current = {
        getValue: fakeEditor.getValue,
        setValue: fakeEditor.setValue,
        setYamlMarkers: () => {},
        clearYamlMarkers: () => {},
      }
    }
    return <div data-testid="yaml-view" />
  },
}))

// Read-only artifact panes mount MonacoEditorWrapper (default export, imported
// lazily) — stub it so we can assert path + readOnly without a real editor.
vi.mock('../../../editor/MonacoEditorWrapper', async () => {
  const react = await import('react')
  return {
    default: react.forwardRef(function MockMonaco(props: { path: string; readOnly?: boolean }) {
      return react.createElement('div', {
        'data-testid': 'readonly-monaco',
        'data-path': props.path,
        'data-readonly': String(props.readOnly ?? false),
      })
    }),
  }
})

vi.mock('../../../../api/files', () => ({
  fetchFiles: vi.fn(),
  fetchFileContentWithMeta: vi.fn(),
  writeFile: vi.fn(),
}))
vi.mock('../../../../api/flowgroups', () => ({
  fetchFlowgroupRelatedFiles: vi.fn(),
}))
vi.mock('../../../workspace/flowgroupBuffers', () => ({
  loadBufferContent: vi.fn(() => Promise.resolve()),
}))

const fetchFilesMock = vi.mocked(fetchFiles)
const fetchContentMock = vi.mocked(fetchFileContentWithMeta)
const relatedMock = vi.mocked(fetchFlowgroupRelatedFiles)

const PIPELINE = 'bronze_ingest'
const FLOWGROUP = 'bronze_customers'
const FILE = 'pipelines/01_bronze/bronze_customers.yaml'
const TAB_ID = entityTabId(PIPELINE, FLOWGROUP)
const PY = 'generated/dev/bronze_ingest/bronze_customers.py'
const RUNNER = 'generated/dev/bronze_ingest/bronze_ingest_runner.py'
const SQL = 'sql/brz/bronze_customers.sql'
const SCHEMA = 'schemas/customer_schema.yaml'
const YAML_CONTENT = 'pipeline: bronze_ingest\nflowgroup: bronze_customers\n'

function fileTree(paths: string[]): FileNode {
  return {
    name: '',
    path: '',
    type: 'directory',
    children: paths.map((p) => ({ name: p.split('/').pop()!, path: p, type: 'file' as const })),
  }
}

function relatedResponse(entries: Array<{ path: string; category: string }>) {
  const mk = (path: string, category: string) => ({
    path,
    category,
    action_name: 'a',
    field: 'f',
    exists: true,
  })
  return {
    flowgroup: FLOWGROUP,
    source_file: mk(FILE, 'yaml'),
    related_files: entries.map((e) => mk(e.path, e.category)),
    environment: 'dev',
  } as unknown as Awaited<ReturnType<typeof fetchFlowgroupRelatedFiles>>
}

function buffer(path: string, over: Partial<EditorBuffer> = {}): EditorBuffer {
  return {
    path,
    language: 'yaml',
    category: 'yaml',
    content: YAML_CONTENT,
    originalContent: YAML_CONTENT,
    isDirty: false,
    isSaving: false,
    etag: 'e1',
    exists: true,
    isNew: false,
    loading: false,
    loadFailed: false,
    ...over,
  }
}

function seedWorkspace() {
  const tab: WorkspaceTabRef = {
    kind: 'entity',
    pipeline: PIPELINE,
    flowgroup: FLOWGROUP,
    filePath: FILE,
    docKind: 'flowgroup',
    view: 'code',
  }
  useWorkspaceStore.setState({
    tabs: [tab],
    activePath: TAB_ID,
    buffers: [buffer(FILE)],
    projectRoot: '/proj',
    restoredDirtyCount: 0,
  })
}

function renderCodeView() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity } },
  })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  return render(
    <CodeView tabId={TAB_ID} pipeline={PIPELINE} flowgroup={FLOWGROUP} filePath={FILE} />,
    { wrapper },
  )
}

beforeEach(() => {
  vi.clearAllMocks()
  seedWorkspace()
  // The live editor starts in sync with the seeded buffer (getValue === content),
  // so the reconcile effect is a no-op until a test drives it.
  fakeEditor.state.value = YAML_CONTENT
  useDocumentStore.setState({ docs: {} })
  useUIStore.setState({ selectedEnv: 'dev' })
  fetchFilesMock.mockResolvedValue(fileTree([FILE, PY, SQL, SCHEMA]))
  relatedMock.mockResolvedValue(
    relatedResponse([
      { path: SQL, category: 'sql' },
      { path: SCHEMA, category: 'schema' },
    ]),
  )
  fetchContentMock.mockResolvedValue({ content: '# generated\nimport dlt\n', etag: 'py1' })
})

describe('CodeView', () => {
  it('renders a sub-tab strip: editable yaml + generated python + referenced sql/schema', async () => {
    renderCodeView()
    // The yaml source tab is present immediately; the rest arrive after the
    // file-tree + related-files queries resolve.
    expect(await screen.findByRole('tab', { name: /bronze_customers\.yaml/ })).toBeInTheDocument()
    expect(await screen.findByRole('tab', { name: /bronze_customers\.py/ })).toBeInTheDocument()
    expect(await screen.findByRole('tab', { name: /bronze_customers\.sql/ })).toBeInTheDocument()
    expect(await screen.findByRole('tab', { name: /customer_schema\.yaml/ })).toBeInTheDocument()
  })

  it('marks the generated python tab "generated · read-only" and sources "read-only"', async () => {
    renderCodeView()
    expect(
      await screen.findByRole('tab', { name: /bronze_customers\.py.*generated · read-only/ }),
    ).toBeInTheDocument()
    // The two source artifacts (sql + schema) carry the plain read-only chip.
    const readOnlyChips = await screen.findAllByText('read-only')
    expect(readOnlyChips.length).toBe(2)
  })

  it('opens on the editable yaml tab (YamlView, not a read-only model)', async () => {
    renderCodeView()
    expect(await screen.findByTestId('yaml-view')).toBeInTheDocument()
    expect(screen.queryByTestId('readonly-monaco')).not.toBeInTheDocument()
    const yamlTab = screen.getByRole('tab', { name: /bronze_customers\.yaml/ })
    expect(yamlTab).toHaveAttribute('aria-selected', 'true')
  })

  it('switches to a generated tab and mounts a read-only Monaco model for it', async () => {
    renderCodeView()
    const pyTab = await screen.findByRole('tab', { name: /bronze_customers\.py/ })
    await userEvent.click(pyTab)

    const monaco = await screen.findByTestId('readonly-monaco')
    expect(monaco).toHaveAttribute('data-path', PY)
    expect(monaco).toHaveAttribute('data-readonly', 'true')
    expect(screen.queryByTestId('yaml-view')).not.toBeInTheDocument()
    expect(fetchContentMock).toHaveBeenCalledWith(PY)
  })

  it('falls back to the pipeline runner when the per-flowgroup python is absent', async () => {
    fetchFilesMock.mockResolvedValue(fileTree([FILE, RUNNER]))
    relatedMock.mockResolvedValue(relatedResponse([]))
    renderCodeView()
    expect(await screen.findByRole('tab', { name: /bronze_ingest_runner\.py/ })).toBeInTheDocument()
  })

  it('shows a "not generated yet" hint when no generated python exists', async () => {
    fetchFilesMock.mockResolvedValue(fileTree([FILE]))
    relatedMock.mockResolvedValue(relatedResponse([]))
    renderCodeView()
    // The python tab is still surfaced (as a placeholder) so the artifact is
    // discoverable, and activating it shows the hint instead of a model.
    const pyTab = await screen.findByRole('tab', { name: /bronze_customers\.py/ })
    await userEvent.click(pyTab)
    expect(await screen.findByText('Not generated yet')).toBeInTheDocument()
    expect(screen.queryByTestId('readonly-monaco')).not.toBeInTheDocument()
  })

  it('handles a read-only fetch error without crashing the view', async () => {
    fetchContentMock.mockRejectedValue(new Error('boom'))
    renderCodeView()
    const pyTab = await screen.findByRole('tab', { name: /bronze_customers\.py/ })
    await userEvent.click(pyTab)
    expect(await screen.findByText("Couldn't load file")).toBeInTheDocument()
    // The strip is still intact.
    expect(screen.getByRole('tab', { name: /bronze_customers\.yaml/ })).toBeInTheDocument()
  })

  it('flushes live yaml into the buffer before a Code→Graph view switch', async () => {
    renderCodeView()
    await screen.findByTestId('yaml-view')
    // The user typed into the live editor; no debounced capture has fired yet.
    const typed = `${YAML_CONTENT}note: typed\n`
    fakeEditor.state.value = typed
    // Switching the tab's view removes the yaml body — the flush subscription
    // must capture the live text into the buffer before it unmounts.
    act(() => {
      useWorkspaceStore.getState().setTabView(TAB_ID, 'graph')
    })
    expect(useWorkspaceStore.getState().buffers.find((b) => b.path === FILE)?.content).toBe(typed)
  })

  it('reconciles the live editor when the buffer is discarded out of band', async () => {
    // A dirty source buffer whose live editor holds the dirty text.
    const dirty = `${YAML_CONTENT}note: dirty\n`
    useWorkspaceStore.setState({ buffers: [buffer(FILE, { content: dirty, isDirty: true })] })
    fakeEditor.state.value = dirty
    renderCodeView()
    await screen.findByTestId('yaml-view')
    // A dirty buffer is the user's in-flight edit — never clobbered on mount.
    expect(fakeEditor.setValue).not.toHaveBeenCalled()
    // discardDirty resets content→originalContent + clears dirty; the reconcile
    // effect pushes the reverted text into the mounted editor (CenterArea's
    // editorRef is null while CodeView owns the editor).
    act(() => {
      useWorkspaceStore.getState().discardDirty()
    })
    expect(fakeEditor.setValue).toHaveBeenCalledWith(YAML_CONTENT)
    expect(fakeEditor.state.value).toBe(YAML_CONTENT)
  })
})
