import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { TabStrip } from '../TabStrip'
import {
  useWorkspaceStore,
  type EditorBuffer,
  type WorkspaceTabRef,
} from '../../../../store/workspaceStore'

function buffer(path: string, over: Partial<EditorBuffer> = {}): EditorBuffer {
  return {
    path,
    language: 'yaml',
    category: 'yaml',
    content: '',
    originalContent: '',
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

const TABS: WorkspaceTabRef[] = [
  { kind: 'file', path: 'pipelines/x/foo.yaml' },
  {
    kind: 'entity',
    pipeline: 'ingestion',
    flowgroup: 'customers',
    filePath: 'pipelines/ingestion/customers.yaml',
    docKind: 'flowgroup',
    view: 'graph',
  },
  { kind: 'config', path: 'lhp.yaml', configKind: 'project', view: 'form' },
  { kind: 'project-map' },
  { kind: 'pipeline-dag', pipeline: 'bronze' },
  { kind: 'table-detail', fqn: 'cat.bronze.customers' },
  { kind: 'resource', resourceKind: 'preset', name: 'bronze_layer', filePath: 'presets/bronze.yaml' },
]

function seed(over: { buffers?: EditorBuffer[]; activePath?: string | null } = {}) {
  useWorkspaceStore.setState({
    tabs: TABS,
    buffers: over.buffers ?? [buffer('pipelines/x/foo.yaml', { isDirty: true })],
    activePath: over.activePath ?? 'pipelines/x/foo.yaml',
    projectRoot: '/proj',
    restoredDirtyCount: 0,
  })
}

beforeEach(() => {
  seed()
})

describe('TabStrip', () => {
  it('renders a label for every tab kind', () => {
    render(<TabStrip onSelect={vi.fn()} onClose={vi.fn()} />)
    expect(screen.getByText('foo.yaml')).toBeInTheDocument()
    expect(screen.getByText('ingestion·customers')).toBeInTheDocument()
    expect(screen.getByText('lhp.yaml')).toBeInTheDocument()
    expect(screen.getByText('Project map')).toBeInTheDocument()
    // pipeline-dag label is the pipeline name
    expect(screen.getByText('bronze')).toBeInTheDocument()
    // table-detail label is the short (last) segment of the FQN
    expect(screen.getByText('customers')).toBeInTheDocument()
    expect(screen.getByText('bronze_layer')).toBeInTheDocument()
  })

  it('shows the dirty dot only on the dirty buffer-backed tab', () => {
    render(<TabStrip onSelect={vi.fn()} onClose={vi.fn()} />)
    const dots = screen.getAllByText('(unsaved changes)')
    expect(dots).toHaveLength(1)
  })

  it('calls onSelect with the namespaced tab id when a tab is clicked', async () => {
    const onSelect = vi.fn()
    render(<TabStrip onSelect={onSelect} onClose={vi.fn()} />)
    await userEvent.click(screen.getByText('ingestion·customers'))
    expect(onSelect).toHaveBeenCalledWith('entity:ingestion/customers')
  })

  it('calls onClose with the tab id from the close button', async () => {
    const onClose = vi.fn()
    render(<TabStrip onSelect={vi.fn()} onClose={onClose} />)
    await userEvent.click(screen.getByRole('button', { name: 'Close Project map' }))
    expect(onClose).toHaveBeenCalledWith('project-map')
  })

  it('closes a tab on middle-click', () => {
    const onClose = vi.fn()
    render(<TabStrip onSelect={vi.fn()} onClose={onClose} />)
    const wrapper = screen.getByText('foo.yaml').closest('div') as HTMLElement
    fireEvent(wrapper, new MouseEvent('auxclick', { bubbles: true, button: 1 }))
    expect(onClose).toHaveBeenCalledWith('pipelines/x/foo.yaml')
  })

  it('marks a not-yet-created file tab as missing', () => {
    seed({ buffers: [buffer('pipelines/x/foo.yaml', { exists: false })] })
    render(<TabStrip onSelect={vi.fn()} onClose={vi.fn()} />)
    const fileTab = screen.getByText('foo.yaml').closest('div') as HTMLElement
    expect(within(fileTab).getByTitle(/edit and save to create/i)).toBeInTheDocument()
  })
})
