import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { startStream } from '../../api/stream'
import { abortCurrentRun, useRunController, useRunStore } from '../runStore'
import { useUIStore } from '../uiStore'
import { useLayoutStore } from '../layoutStore'

vi.mock('../../api/stream', () => ({ startStream: vi.fn() }))
const start = vi.mocked(startStream)
const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
function wrapper({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}
function pendingResponse() {
  let writer!: ReadableStreamDefaultController<Uint8Array>
  const body = new ReadableStream<Uint8Array>({ start(controller) { writer = controller } })
  return { response: new Response(body), writer }
}
function complete(writer: ReadableStreamDefaultController<Uint8Array>) {
  writer.enqueue(new TextEncoder().encode(JSON.stringify({
    type: 'ValidationCompleted', response: { success: true, pipeline_responses: {} },
  }) + '\n'))
  writer.close()
}
beforeEach(() => {
  vi.clearAllMocks()
  useRunStore.getState().reset()
  useUIStore.setState({ selectedEnv: 'test', pipelineFilter: null, selectedPipelineConfig: null, sandboxEnabled: false })
  useLayoutStore.setState({ viewerMode: false })
})
afterEach(async () => {
  act(() => abortCurrentRun())
  await waitFor(() => expect(useRunStore.getState().isRunning).toBe(false))
  client.clear()
})

describe('persistent run lifetime', () => {
  it('finishes after the launching view unmounts and blocks competing controllers', async () => {
    const stream = pendingResponse()
    start.mockResolvedValue(stream.response)
    const first = renderHook(() => useRunController(), { wrapper })
    const second = renderHook(() => useRunController(), { wrapper })
    act(() => first.result.current.startValidate())
    await waitFor(() => expect(start).toHaveBeenCalledTimes(1))
    first.unmount()
    expect(start.mock.calls[0][2]?.aborted).toBe(false)
    act(() => second.result.current.startGenerate())
    expect(start).toHaveBeenCalledTimes(1)
    act(() => complete(stream.writer))
    await waitFor(() => expect(useRunStore.getState().terminal).toBe('success'))
    expect(useRunStore.getState().isRunning).toBe(false)
  })

  it('reports Stop separately and does not launch queued validation afterward', async () => {
    const stream = pendingResponse()
    start.mockResolvedValue(stream.response)
    const { result } = renderHook(() => useRunController(), { wrapper })
    act(() => result.current.startGenerate())
    await waitFor(() => expect(start).toHaveBeenCalledTimes(1))
    act(() => result.current.queueValidate(undefined, 'bronze'))
    expect(useRunStore.getState().validationQueued).toBe(true)
    act(() => result.current.abort())
    await waitFor(() => expect(useRunStore.getState().terminal).toBe('stopped'))
    expect(useRunStore.getState().validationQueued).toBe(false)
    expect(start).toHaveBeenCalledTimes(1)
  })

  it('coalesces validation of different saved pipelines behind generation', async () => {
    const initial = pendingResponse()
    const followup = pendingResponse()
    start.mockResolvedValueOnce(initial.response).mockResolvedValueOnce(followup.response)
    const { result } = renderHook(() => useRunController(), { wrapper })
    act(() => result.current.startGenerate())
    await waitFor(() => expect(start).toHaveBeenCalledTimes(1))
    act(() => {
      result.current.queueValidate(undefined, 'bronze')
      result.current.queueValidate(undefined, 'silver')
    })
    expect(start).toHaveBeenCalledTimes(1)
    expect(start.mock.calls[0][2]?.aborted).toBe(false)
    act(() => initial.writer.close())
    await waitFor(() => expect(start).toHaveBeenCalledTimes(2))
    expect(start.mock.calls[1][0]).toBe('/api/validate/stream')
    expect(start.mock.calls[1][1]).toMatchObject({ env: 'test', pipeline: undefined })
    act(() => complete(followup.writer))
    await waitFor(() => expect(useRunStore.getState().terminal).toBe('success'))
  })

  it('marks a stream with no terminal completion incomplete', async () => {
    const stream = pendingResponse()
    start.mockResolvedValue(stream.response)
    const { result } = renderHook(() => useRunController(), { wrapper })
    act(() => result.current.startValidate())
    act(() => stream.writer.close())
    await waitFor(() => expect(useRunStore.getState().terminal).toBe('incomplete'))
  })

  it('does not execute a write-producing Generate in viewer mode', () => {
    useLayoutStore.setState({ viewerMode: true })
    const { result } = renderHook(() => useRunController(), { wrapper })
    act(() => result.current.startGenerate())
    expect(start).not.toHaveBeenCalled()
  })
})
