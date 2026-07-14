import { beforeEach, describe, expect, it, vi } from 'vitest'
import { QueryClient } from '@tanstack/react-query'

vi.mock('@/api/files', () => ({ writeFile: vi.fn() }))
vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

import { toast } from 'sonner'
import { writeFile } from '@/api/files'
import { ApiError } from '@/api/client'
import type { ErrorDetail } from '@/types/api'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { persistBufferToDisk } from '../persistBuffer'

const mockWriteFile = vi.mocked(writeFile)
const mockToastError = vi.mocked(toast.error)
const mockToastSuccess = vi.mocked(toast.success)
const mockToastDismiss = vi.mocked(toast.dismiss)

// A stub run controller: persistBufferToDisk mirrors saveBuffer's clean-YAML
// validation refresh (Fix #1). `isRunning: false` makes startScopedValidate
// call startValidate synchronously (no abort/defer path).
const startValidate = vi.fn()
const runController = {
  isRunning: false,
  startValidate,
  startGenerate: vi.fn(),
  abort: vi.fn(),
}

const PATH = 'pipelines/bronze/orders.yaml'

function makeApiError(status: number): ApiError {
  const detail: ErrorDetail = {
    code: 'LHP-IO-001',
    category: 'io',
    message: 'boom',
    details: '',
    suggestions: [],
    context: {},
    http_status: status,
  }
  return new ApiError(status, detail)
}

/** A buffer whose in-memory content has drifted ahead of disk (dirty). */
function seedDirtyBuffer() {
  const ws = useWorkspaceStore.getState()
  ws.openBuffer(PATH, { content: 'a: 1\n', etag: 'e1', exists: true })
  ws.updateContent(PATH, 'a: 2\n')
}

function buffer() {
  return useWorkspaceStore.getState().buffers.find((b) => b.path === PATH)
}

let qc: QueryClient

beforeEach(() => {
  vi.clearAllMocks()
  qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  useWorkspaceStore.setState({ buffers: [], tabs: [], activePath: null })
})

describe('persistBufferToDisk', () => {
  it('writes the buffer with its etag and marks it saved on a clean write', async () => {
    seedDirtyBuffer()
    mockWriteFile.mockResolvedValue({ written: true, path: PATH, etag: 'e2' })

    const ok = await persistBufferToDisk(PATH, qc, runController)

    expect(ok).toBe(true)
    // PUT carries the drifted content + the pre-write etag (optimistic lock).
    expect(mockWriteFile).toHaveBeenCalledWith(PATH, 'a: 2\n', 'e1')
    const b = buffer()!
    expect(b.isDirty).toBe(false)
    expect(b.etag).toBe('e2')
    expect(b.originalContent).toBe('a: 2\n')
    expect(mockToastSuccess).toHaveBeenCalled()
    expect(mockToastDismiss).toHaveBeenCalledWith(`stale:${PATH}`)
  })

  it('returns false and points to the Code view on a 412 conflict (no clobber)', async () => {
    seedDirtyBuffer()
    mockWriteFile.mockRejectedValue(makeApiError(412))

    const ok = await persistBufferToDisk(PATH, qc, runController)

    expect(ok).toBe(false)
    // The write never landed → the buffer stays dirty for the user to resolve.
    expect(buffer()!.isDirty).toBe(true)
    expect(mockToastError).toHaveBeenCalledWith(
      expect.stringContaining('Code view'),
      expect.objectContaining({ id: `stale:${PATH}` }),
    )
  })

  it('returns false but keeps the persisted write on a yaml_error', async () => {
    seedDirtyBuffer()
    mockWriteFile.mockResolvedValue({
      written: true,
      path: PATH,
      etag: 'e2',
      yaml_error: { line: 1, column: 1, message: 'bad' },
    })

    const ok = await persistBufferToDisk(PATH, qc, runController)

    expect(ok).toBe(false)
    // The write DID persist, so the buffer is marked saved even though invalid.
    expect(buffer()!.isDirty).toBe(false)
    expect(mockToastError).toHaveBeenCalledWith(expect.stringContaining('Code view'))
  })

  it('refuses to write a still-loading buffer', async () => {
    useWorkspaceStore.getState().openBuffer(PATH, { loading: true })
    const ok = await persistBufferToDisk(PATH, qc, runController)
    expect(ok).toBe(false)
    expect(mockWriteFile).not.toHaveBeenCalled()
  })

  it('refuses to write a read-only path', async () => {
    const roPath = 'generated/pipelines/orders.py'
    useWorkspaceStore.getState().openBuffer(roPath, { content: 'x', exists: true })
    const ok = await persistBufferToDisk(roPath, qc, runController)
    expect(ok).toBe(false)
    expect(mockWriteFile).not.toHaveBeenCalled()
  })
})

describe('persistBufferToDisk — validation refresh (Fix #1)', () => {
  it('kicks a scoped validate on a clean YAML write, scoped to the derived pipeline', async () => {
    const ws = useWorkspaceStore.getState()
    ws.openBuffer(PATH, { content: 'pipeline: bronze\n', etag: 'e1', exists: true })
    ws.updateContent(PATH, 'pipeline: bronze\nflowgroup: orders\n')
    mockWriteFile.mockResolvedValue({ written: true, path: PATH, etag: 'e2' })

    const ok = await persistBufferToDisk(PATH, qc, runController)

    expect(ok).toBe(true)
    // Mirrors useWorkspaceSave: derivePipelineFromYaml(content) → 'bronze',
    // startScopedValidate → startValidate(undefined, 'bronze').
    expect(startValidate).toHaveBeenCalledTimes(1)
    expect(startValidate).toHaveBeenCalledWith(undefined, 'bronze')
  })

  it('does NOT refresh validation on a 412 conflict (write never landed)', async () => {
    seedDirtyBuffer()
    mockWriteFile.mockRejectedValue(makeApiError(412))

    const ok = await persistBufferToDisk(PATH, qc, runController)

    expect(ok).toBe(false)
    expect(startValidate).not.toHaveBeenCalled()
  })

  it('does NOT refresh validation on a yaml_error (persisted but unparseable)', async () => {
    seedDirtyBuffer()
    mockWriteFile.mockResolvedValue({
      written: true,
      path: PATH,
      etag: 'e2',
      yaml_error: { line: 1, column: 1, message: 'bad' },
    })

    const ok = await persistBufferToDisk(PATH, qc, runController)

    expect(ok).toBe(false)
    expect(startValidate).not.toHaveBeenCalled()
  })
})
