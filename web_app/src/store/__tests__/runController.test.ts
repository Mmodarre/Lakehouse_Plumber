import { beforeEach, describe, expect, it, vi } from 'vitest'
import { renderHook } from '@testing-library/react'
import { useRunController, useRunStore } from '@/store/runStore'
import { useUIStore } from '@/store/uiStore'
import type { StreamCallbacks } from '@/hooks/useEventStream'

// useRunController wires the transport hook into the store; the transport
// itself is exercised in hooks/__tests__/useEventStream.test.tsx, so here it
// is replaced with a controllable stub and the wiring is what's under test.
const transport = vi.hoisted(() => ({
  start: vi.fn(),
  abort: vi.fn(),
  isRunning: false,
}))

vi.mock('@/hooks/useEventStream', () => ({
  useEventStream: () => ({
    start: transport.start,
    abort: transport.abort,
    isRunning: transport.isRunning,
    frames: [],
    error: null,
  }),
}))

function capturedCallbacks(): StreamCallbacks {
  const callbacks = transport.start.mock.calls[0]?.[1] as StreamCallbacks | undefined
  if (!callbacks) throw new Error('transport.start was not called with callbacks')
  return callbacks
}

beforeEach(() => {
  vi.clearAllMocks()
  transport.isRunning = false
  useRunStore.getState().reset()
  useUIStore.setState({
    selectedEnv: 'dev',
    pipelineFilter: null,
    selectedPipelineConfig: null,
    sandboxEnabled: false,
  })
})

describe('useRunController', () => {
  it('startValidate() defaults env/pipeline from uiStore and begins the run', () => {
    const { result } = renderHook(() => useRunController())
    result.current.startValidate()

    expect(useRunStore.getState().runKind).toBe('validate')
    expect(useRunStore.getState().isRunning).toBe(true)
    expect(transport.start).toHaveBeenCalledTimes(1)
    expect(transport.start.mock.calls[0][0]).toEqual({
      path: '/api/validate/stream',
      env: 'dev',
      pipeline: undefined,
    })
  })

  it('uses the uiStore pipeline filter when no override is passed', () => {
    useUIStore.setState({ selectedEnv: 'tst', pipelineFilter: 'bronze' })
    const { result } = renderHook(() => useRunController())
    result.current.startValidate()

    expect(transport.start.mock.calls[0][0]).toEqual({
      path: '/api/validate/stream',
      env: 'tst',
      pipeline: 'bronze',
    })
  })

  it('explicit env/pipeline overrides win over uiStore state', () => {
    useUIStore.setState({ selectedEnv: 'dev', pipelineFilter: 'bronze' })
    const { result } = renderHook(() => useRunController())
    result.current.startGenerate('prod', 'sales')

    expect(useRunStore.getState().runKind).toBe('generate')
    expect(transport.start.mock.calls[0][0]).toEqual({
      path: '/api/generate/stream',
      env: 'prod',
      pipeline: 'sales',
    })
  })

  it('passes the selected pipeline config to a validate run', () => {
    useUIStore.setState({ selectedPipelineConfig: 'config/pipeline_config_dev.yaml' })
    const { result } = renderHook(() => useRunController())
    result.current.startValidate()

    expect(transport.start.mock.calls[0][0]).toEqual({
      path: '/api/validate/stream',
      env: 'dev',
      pipeline: undefined,
      pipeline_config: 'config/pipeline_config_dev.yaml',
    })
  })

  it('passes the selected pipeline config to a generate run', () => {
    useUIStore.setState({ selectedPipelineConfig: 'config/pipeline_config_prod.yaml' })
    const { result } = renderHook(() => useRunController())
    result.current.startGenerate()

    expect(transport.start.mock.calls[0][0]).toEqual({
      path: '/api/generate/stream',
      env: 'dev',
      pipeline: undefined,
      pipeline_config: 'config/pipeline_config_prod.yaml',
    })
  })

  it('sends no pipeline_config while none is selected (both kinds)', () => {
    const { result } = renderHook(() => useRunController())
    result.current.startValidate()
    expect(transport.start.mock.calls[0][0].pipeline_config).toBeUndefined()

    // The wire-level key omission for undefined is pinned in
    // src/api/__tests__/stream.test.ts (JSON.stringify drops it).
    result.current.startGenerate()
    expect(transport.start.mock.calls[1]?.[0].pipeline_config).toBeUndefined()
  })

  it('ignores a start while the transport is already running', () => {
    transport.isRunning = true
    const { result } = renderHook(() => useRunController())
    result.current.startValidate()

    expect(transport.start).not.toHaveBeenCalled()
    expect(useRunStore.getState().runKind).toBeNull()
  })

  it('routes transport callbacks into the store (frame / error / done)', () => {
    const { result } = renderHook(() => useRunController())
    result.current.startGenerate()
    const callbacks = capturedCallbacks()

    callbacks.onFrame?.({ type: 'info', code: 'I', message: 'working' })
    expect(useRunStore.getState().infoLog).toEqual(['working'])

    callbacks.onError?.(new Error('socket closed'))
    expect(useRunStore.getState().errorFrame).toMatchObject({
      code: 'STREAM_ERROR',
      title: 'socket closed',
    })

    callbacks.onDone?.({ aborted: false })
    expect(useRunStore.getState().isRunning).toBe(false)
    expect(useRunStore.getState().terminal).toBe('error')
  })

  it('exposes the transport abort and running flag directly', () => {
    const { result } = renderHook(() => useRunController())
    expect(result.current.isRunning).toBe(false)
    result.current.abort()
    expect(transport.abort).toHaveBeenCalledTimes(1)
  })

  it('sends sandbox:true and no pipeline when the sandbox toggle is on', () => {
    useUIStore.setState({ sandboxEnabled: true, pipelineFilter: 'bronze' })
    const { result } = renderHook(() => useRunController())
    result.current.startGenerate()

    // The stale pipeline filter is dropped — sandbox scope owns the run.
    expect(transport.start.mock.calls[0][0]).toEqual({
      path: '/api/generate/stream',
      env: 'dev',
      pipeline: undefined,
      sandbox: true,
    })
    expect(useRunStore.getState().sandbox).toBe(true)
  })

  it('leaves the designer\'s pipeline-scoped validate untouched by the toggle', () => {
    // An explicit pipeline (the designer inspecting one flowgroup) stays
    // pipeline-scoped even while the global sandbox toggle is on.
    useUIStore.setState({ sandboxEnabled: true })
    const { result } = renderHook(() => useRunController())
    result.current.startValidate('dev', 'sales')

    const body = transport.start.mock.calls[0][0]
    expect(body.pipeline).toBe('sales')
    expect(body.sandbox).toBeUndefined()
    expect(useRunStore.getState().sandbox).toBe(false)
  })

  it('records a non-sandbox run when the toggle is off', () => {
    const { result } = renderHook(() => useRunController())
    result.current.startValidate()
    expect(transport.start.mock.calls[0][0].sandbox).toBeUndefined()
    expect(useRunStore.getState().sandbox).toBe(false)
  })
})
