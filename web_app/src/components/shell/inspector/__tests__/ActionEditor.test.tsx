import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import type { ActionRead } from '@/lib/flowgroup-doc'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { useDocumentStore } from '@/store/documentStore'
import { useLayoutStore } from '@/store/layoutStore'
import { writeFile } from '@/api/files'
import { ActionEditor } from '../ActionEditor'

// ActionEditor drives the REAL documentStore/useFlowgroupDoc write path (same
// comment-preserving buffer GraphView uses), so Save is exercised end-to-end.
// The disk PUT is mocked (`writeFile`) so we can assert Save now persists (#9).

vi.mock('@/api/files', () => ({
  writeFile: vi.fn(),
}))

// ActionEditor calls useRunController() to refresh validation after a clean
// save (Fix #1). Stub it so Save doesn't fire a real validate stream (there is
// no global fetch mock); the persistBuffer unit test covers the refresh wiring.
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

const mockWriteFile = vi.mocked(writeFile)

const PATH = 'pipelines/bronze/customers.yaml'

const YAML = `# Bronze customers
pipeline: bronze
flowgroup: customers

actions:
  - name: load_customers_raw
    type: load
    source:
      type: cloudfiles
      path: /Volumes/raw/customers
      format: csv
    options:
      header: true
      inferSchema: true
    target: v_customers_raw
`

const ACTION: ActionRead = {
  name: 'load_customers_raw',
  kind: 'load',
  subType: 'cloudfiles',
  target: 'v_customers_raw',
  sources: [],
  dependsOn: [],
  raw: {
    name: 'load_customers_raw',
    type: 'load',
    source: { type: 'cloudfiles', path: '/Volumes/raw/customers', format: 'csv' },
    options: { header: true, inferSchema: true },
    target: 'v_customers_raw',
  },
  index: 0,
}

function bufferContent(): string {
  return useWorkspaceStore.getState().buffers.find((b) => b.path === PATH)!.content
}

function wrapper({ children }: { children: ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return <QueryClientProvider client={qc}>{children}</QueryClientProvider>
}

function seedAndRender(props?: { onSaved?: () => void; onOpenCodeView?: () => void }) {
  useWorkspaceStore.getState().openBuffer(PATH, { content: YAML, exists: true })
  useDocumentStore.getState().open(PATH, 'flowgroup')
  return render(
    <ActionEditor
      action={ACTION}
      actionId="load_customers_raw"
      filePath={PATH}
      docKind="flowgroup"
      onSaved={props?.onSaved}
      onOpenCodeView={props?.onOpenCodeView}
    />,
    { wrapper },
  )
}

beforeEach(() => {
  vi.clearAllMocks()
  mockWriteFile.mockResolvedValue({ written: true, path: PATH, etag: 'etag-2' })
  useWorkspaceStore.setState({ buffers: [], tabs: [], activePath: null })
  useDocumentStore.setState({ docs: {} })
  useLayoutStore.setState({ viewerMode: false })
})

describe('ActionEditor', () => {
  it('renders the kind chip, mono name, and a field grid from the raw action', () => {
    seedAndRender()
    expect(screen.getByText('load')).toBeInTheDocument()
    expect(screen.getByText('load_customers_raw')).toBeInTheDocument()
    // String leaves are editable inputs, flattened to dotted labels.
    expect(screen.getByLabelText('source.type')).toHaveValue('cloudfiles')
    expect(screen.getByLabelText('source.path')).toHaveValue('/Volumes/raw/customers')
    expect(screen.getByLabelText('target')).toHaveValue('v_customers_raw')
    // Boolean map entries render read-only (no input to label).
    expect(screen.getByText('options.header')).toBeInTheDocument()
    expect(screen.queryByLabelText('options.header')).not.toBeInTheDocument()
  })

  it('starts clean: Save + Revert disabled, no unsaved caption', () => {
    seedAndRender()
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Revert' })).toBeDisabled()
    expect(screen.queryByText(/Editing action/)).not.toBeInTheDocument()
  })

  it('editing a field marks it dirty and Save writes back to the buffer', () => {
    seedAndRender()
    const path = screen.getByLabelText('source.path')
    fireEvent.change(path, { target: { value: '/Volumes/raw/customers_v2' } })

    expect(screen.getByText(/Editing action · unsaved/)).toBeInTheDocument()
    const save = screen.getByRole('button', { name: 'Save' })
    expect(save).toBeEnabled()

    fireEvent.click(save)

    const content = bufferContent()
    expect(content).toContain('path: /Volumes/raw/customers_v2')
    // Byte-surgical: unrelated lines + comments survive.
    expect(content).toContain('# Bronze customers')
    expect(content).toContain('format: csv')
  })

  it('Save persists the mutated buffer to disk via PUT (#9)', async () => {
    seedAndRender()
    fireEvent.change(screen.getByLabelText('source.path'), {
      target: { value: '/Volumes/raw/customers_v2' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    await waitFor(() => expect(mockWriteFile).toHaveBeenCalledTimes(1))
    const [calledPath, calledContent] = mockWriteFile.mock.calls[0]
    expect(calledPath).toBe(PATH)
    expect(calledContent).toContain('path: /Volumes/raw/customers_v2')
    // On a clean disk write the buffer is marked saved (baseline re-anchored).
    await waitFor(() =>
      expect(useWorkspaceStore.getState().buffers.find((b) => b.path === PATH)?.isDirty).toBe(
        false,
      ),
    )
  })

  it('calls onSaved after a successful disk write (modal-close hook)', async () => {
    const onSaved = vi.fn()
    seedAndRender({ onSaved })
    fireEvent.change(screen.getByLabelText('source.path'), { target: { value: 'changed' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    await waitFor(() => expect(onSaved).toHaveBeenCalledTimes(1))
  })

  it('does not persist when nothing changed (Save disabled, no PUT)', () => {
    seedAndRender()
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled()
    expect(mockWriteFile).not.toHaveBeenCalled()
  })

  it('Revert restores the original field value', () => {
    seedAndRender()
    const path = screen.getByLabelText('source.path')
    fireEvent.change(path, { target: { value: 'changed' } })
    expect(path).toHaveValue('changed')

    fireEvent.click(screen.getByRole('button', { name: 'Revert' }))
    expect(screen.getByLabelText('source.path')).toHaveValue('/Volumes/raw/customers')
    expect(screen.queryByText(/Editing action/)).not.toBeInTheDocument()
  })

  it('viewer lens disables editing + Save', () => {
    useLayoutStore.setState({ viewerMode: true })
    seedAndRender()
    expect(screen.getByLabelText('source.path')).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled()
  })

  it('a failed persist keeps the modal open and offers an Open in Code view escape (Fix #2)', async () => {
    const onSaved = vi.fn()
    const onOpenCodeView = vi.fn()
    // A yaml_error (like a 412) makes persistBufferToDisk return false. In the
    // live GraphView modal `commit` re-derives `action`, flipping dirty back to
    // false and LOCKING Save (see GraphView.test.tsx); here `action` is a fixed
    // prop, so this asserts the escape mechanism itself.
    mockWriteFile.mockResolvedValue({
      written: true,
      path: PATH,
      etag: 'etag-2',
      yaml_error: { line: 1, column: 1, message: 'bad' },
    })
    seedAndRender({ onSaved, onOpenCodeView })

    fireEvent.change(screen.getByLabelText('source.path'), {
      target: { value: '/Volumes/raw/customers_v2' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    // The recovery banner appears; onSaved never fired (modal host keeps it open).
    const escape = await screen.findByRole('button', { name: /open in code view/i })
    expect(onSaved).not.toHaveBeenCalled()
    // The escape hatch fires the host callback (which closes + jumps to Code).
    fireEvent.click(escape)
    expect(onOpenCodeView).toHaveBeenCalledTimes(1)
  })

  it('editing a field again after a failed persist clears the recovery banner', async () => {
    mockWriteFile.mockResolvedValue({
      written: true,
      path: PATH,
      etag: 'etag-2',
      yaml_error: { line: 1, column: 1, message: 'bad' },
    })
    seedAndRender()

    fireEvent.change(screen.getByLabelText('source.path'), { target: { value: 'v2' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    await screen.findByRole('button', { name: /open in code view/i })

    // Editing again dismisses the banner and re-enables Save for a retry.
    fireEvent.change(screen.getByLabelText('source.path'), { target: { value: 'v3' } })
    expect(screen.queryByRole('button', { name: /open in code view/i })).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Save' })).toBeEnabled()
  })
})
