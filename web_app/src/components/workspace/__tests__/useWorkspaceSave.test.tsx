import { describe, expect, it, vi, beforeEach } from 'vitest'
import type { ReactNode, RefObject } from 'react'
import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

vi.mock('../../../api/files', () => ({
  writeFile: vi.fn(),
}))
vi.mock('sonner', () => ({
  toast: {
    error: vi.fn(),
    success: vi.fn(),
    dismiss: vi.fn(),
    info: vi.fn(),
  },
}))
// The run controller / synthetic-issue plumbing is exercised elsewhere; here
// it only needs to exist so the hook can be rendered in isolation.
vi.mock('../../../store/runStore', () => ({
  useRunController: () => ({
    isRunning: false,
    startValidate: vi.fn(),
    startGenerate: vi.fn(),
    abort: vi.fn(),
  }),
  useRunStore: (selector: (s: { setSyntheticSyntaxIssue: () => void }) => unknown) =>
    selector({ setSyntheticSyntaxIssue: vi.fn() }),
}))

import { toast } from 'sonner'
import { writeFile } from '../../../api/files'
import { ApiError } from '../../../api/client'
import { useWorkspaceStore } from '../../../store/workspaceStore'
import { useWorkspaceSave } from '../useWorkspaceSave'
import type { MonacoEditorHandle } from '../../editor/MonacoEditorWrapper'

const mockWriteFile = vi.mocked(writeFile)
const mockToastError = vi.mocked(toast.error)
const mockToastDismiss = vi.mocked(toast.dismiss)

let queryClient: QueryClient

function wrapper({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
}

/** Render the hook with a fake live editor whose text can drift ahead of the
 * store (exactly what happens between capture points while the user types). */
function setup(path: string) {
  let liveText = useWorkspaceStore.getState().buffers.find((b) => b.path === path)?.content ?? ''
  const handle: MonacoEditorHandle = {
    getValue: () => liveText,
    setValue: vi.fn((v: string) => {
      liveText = v
    }),
    setYamlMarkers: vi.fn(),
    clearYamlMarkers: vi.fn(),
  }
  const editorRef: RefObject<MonacoEditorHandle | null> = { current: handle }
  const activePathRef: RefObject<string | null> = { current: path }
  // Mirrors WorkspaceEditor.captureActive: flush the live editor into the store.
  const captureActive = () => {
    if (activePathRef.current) {
      useWorkspaceStore.getState().updateContent(activePathRef.current, liveText)
    }
  }
  const { result } = renderHook(() => useWorkspaceSave({ editorRef, activePathRef, captureActive }), {
    wrapper,
  })
  return {
    result,
    handle,
    /** Simulate typing that has NOT been captured into the store yet. */
    type: (t: string) => {
      liveText = t
    },
  }
}

function bufferFor(path: string) {
  return useWorkspaceStore.getState().buffers.find((b) => b.path === path)
}

describe('useWorkspaceSave', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorage.clear()
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    useWorkspaceStore.setState({
      buffers: [],
      activePath: null,
      projectRoot: null,
      restoredDirtyCount: 0,
    })
  })

  it('first save of a new flowgroup YAML invalidates inspection caches but NOT the graph keys (serve-stale)', async () => {
    // Under serve-stale a brand-new flowgroup misses the graph cache, so a
    // graph-key invalidation here would force an ungated cold rebuild and a
    // contradictory freshly-rebuilt-graph + stale-badge state. The watcher's
    // graph-stale mark drives the badge; Refresh owns the single rebuild.
    const path = 'pipelines/bronze/new_fg.yaml'
    useWorkspaceStore
      .getState()
      .openBuffer(path, { content: 'pipeline: bronze\nflowgroup: new_fg\n', isNew: true, exists: false })
    const { result } = setup(path)
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')
    mockWriteFile.mockResolvedValueOnce({ etag: 'e1' } as Awaited<ReturnType<typeof writeFile>>)

    let ok = false
    await act(async () => {
      ok = await result.current.saveBuffer(path)
    })
    expect(ok).toBe(true)

    const keys = invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)
    // Inspection caches must still refetch so the new file shows in tree/lists.
    expect(keys).toContainEqual(['files'])
    expect(keys).toContainEqual(['flowgroups'])
    expect(keys).toContainEqual(['pipelines'])
    // Graph keys must NOT be invalidated on the new-flowgroup save path.
    expect(keys).not.toContainEqual(['dep-graph'])
    expect(keys).not.toContainEqual(['execution-order'])
    expect(keys).not.toContainEqual(['circular-deps'])
  })

  it('412 lifecycle: stale toast, Resolve reads CURRENT content, keep-mine re-PUTs it with the fresh etag and ends clean', async () => {
    const path = 'sql/query.sql'
    useWorkspaceStore.getState().openBuffer(path, { content: 'v1', etag: 'e1', exists: true })
    const { result, handle, type } = setup(path)

    // Captured edit, then the save is rejected as stale.
    type('v2 mine')
    act(() => useWorkspaceStore.getState().updateContent(path, 'v2 mine'))
    mockWriteFile.mockRejectedValueOnce(
      new ApiError(412, {
        code: 'STALE_WRITE',
        category: 'io',
        message: 'stale',
        details: '',
        suggestions: [],
        context: {},
        http_status: 412,
      }),
    )
    let ok = true
    await act(async () => {
      ok = await result.current.saveBuffer(path)
    })
    expect(ok).toBe(false)
    expect(mockToastError).toHaveBeenCalledWith(
      'query.sql changed on disk since you opened it',
      expect.objectContaining({ id: `stale:${path}`, duration: Infinity }),
    )

    // The user keeps editing while the Infinity toast sits there — only the
    // live editor has the new text (the capture debounce has not fired).
    type('v3 post-412 edit')

    // Fix 5: Resolve must read the text at CLICK time, not the frozen
    // failed-save snapshot ('v2 mine').
    const toastOptions = mockToastError.mock.calls.at(-1)![1] as unknown as {
      action: { onClick: (e: unknown) => void }
    }
    act(() => toastOptions.action.onClick({}))
    expect(result.current.conflict).toEqual({ path, mine: 'v3 post-412 edit', language: 'sql' })

    // Keep-mine re-PUTs the CURRENT content with the freshly fetched etag.
    mockWriteFile.mockResolvedValueOnce({ etag: 'e2' } as Awaited<ReturnType<typeof writeFile>>)
    await act(async () => {
      result.current.resolveKeepMine(path, result.current.conflict!.mine, 'fresh-etag')
    })
    await waitFor(() => expect(bufferFor(path)?.isDirty).toBe(false))
    expect(mockWriteFile).toHaveBeenLastCalledWith(path, 'v3 post-412 edit', 'fresh-etag')
    const buf = bufferFor(path)!
    expect(buf.content).toBe('v3 post-412 edit')
    expect(buf.etag).toBe('e2')
    // The editor was synced to the saved text and the stale toast dismissed.
    expect(handle.setValue).toHaveBeenCalledWith('v3 post-412 edit')
    expect(mockToastDismiss).toHaveBeenCalledWith(`stale:${path}`)
  })

  it('take-theirs replaces the buffer, syncs the editor, dismisses the toast, and ends clean', () => {
    const path = 'sql/query.sql'
    useWorkspaceStore.getState().openBuffer(path, { content: 'v1', etag: 'e1', exists: true })
    useWorkspaceStore.getState().updateContent(path, 'mine')
    const { result, handle } = setup(path)

    act(() => result.current.resolveTakeTheirs(path, 'disk version', 'e-disk'))

    const buf = bufferFor(path)!
    expect(buf.content).toBe('disk version')
    expect(buf.originalContent).toBe('disk version')
    expect(buf.etag).toBe('e-disk')
    expect(buf.isDirty).toBe(false)
    expect(handle.setValue).toHaveBeenCalledWith('disk version')
    expect(mockToastDismiss).toHaveBeenCalledWith(`stale:${path}`)
    expect(mockWriteFile).not.toHaveBeenCalled()
  })

  it('a load-failed buffer never reaches writeFile (Fix 3)', async () => {
    const path = 'schemas/orders_schema.json'
    useWorkspaceStore.getState().openBuffer(path, { exists: true, loading: true })
    useWorkspaceStore.getState().markLoadFailed(path)
    const { result } = setup(path)

    let ok = true
    await act(async () => {
      ok = await result.current.saveBuffer(path)
    })
    expect(ok).toBe(false)
    expect(mockWriteFile).not.toHaveBeenCalled()
    expect(mockToastError).toHaveBeenCalledWith(expect.stringContaining('has not loaded'))

    // saveAllDirty must skip it too, even if a dirty flag sneaks on.
    act(() => useWorkspaceStore.getState().setDirty(path, true))
    await act(async () => {
      result.current.saveAllDirty()
      await Promise.resolve()
    })
    expect(mockWriteFile).not.toHaveBeenCalled()
  })
})
