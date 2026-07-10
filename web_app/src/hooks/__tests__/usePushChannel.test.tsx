import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { renderHook } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { usePushChannel } from '../usePushChannel'

// jsdom has no EventSource; install a controllable stub. The hook reads the
// static CLOSED constant off the global, so the stub must carry the
// EventSource readyState constants too.
class MockEventSource {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSED = 2
  static instances: MockEventSource[] = []

  url: string
  readyState = MockEventSource.CONNECTING
  onopen: ((ev: Event) => void) | null = null
  onerror: ((ev: Event) => void) | null = null
  close = vi.fn(() => {
    this.readyState = MockEventSource.CLOSED
  })

  private listeners = new Map<string, Set<(ev: Event) => void>>()

  constructor(url: string) {
    this.url = url
    MockEventSource.instances.push(this)
  }

  addEventListener(type: string, listener: (ev: Event) => void): void {
    const set = this.listeners.get(type) ?? new Set()
    set.add(listener)
    this.listeners.set(type, set)
  }

  removeEventListener(type: string, listener: (ev: Event) => void): void {
    this.listeners.get(type)?.delete(listener)
  }

  // ── test drivers ──
  open(): void {
    this.readyState = MockEventSource.OPEN
    this.onopen?.(new Event('open'))
  }

  emitMessage(type: string, data: unknown): void {
    const ev = new MessageEvent(type, { data })
    for (const listener of this.listeners.get(type) ?? []) listener(ev)
  }

  emitPlainEvent(type: string): void {
    const ev = new Event(type)
    for (const listener of this.listeners.get(type) ?? []) listener(ev)
  }

  failWith(readyState: number): void {
    this.readyState = readyState
    this.onerror?.(new Event('error'))
  }
}

function latestSource(): MockEventSource {
  const source = MockEventSource.instances.at(-1)
  if (!source) throw new Error('no EventSource was created')
  return source
}

function mount() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  const rendered = renderHook(() => usePushChannel(), { wrapper })
  return { ...rendered, invalidateSpy }
}

const fileChangedPayload = JSON.stringify({ paths: ['pipelines/a.yaml'] })
const runUpdated = (kind: string, status: string) =>
  JSON.stringify({ run_id: 'r1', kind, status })

beforeEach(() => {
  vi.useFakeTimers()
  MockEventSource.instances = []
  vi.stubGlobal('EventSource', MockEventSource)
  vi.spyOn(console, 'debug').mockImplementation(() => {})
})

afterEach(() => {
  vi.unstubAllGlobals()
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('usePushChannel', () => {
  it('opens a single push source against /api/events', () => {
    mount()
    expect(MockEventSource.instances).toHaveLength(1)
    expect(latestSource().url).toBe('/api/events')
  })

  it('file-changed invalidates every file-derived query key', () => {
    const { invalidateSpy } = mount()
    latestSource().emitMessage('file-changed', fileChangedPayload)

    const keys = invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)
    expect(keys).toContainEqual(['files'])
    expect(keys).toContainEqual(['flowgroups'])
    expect(keys).toContainEqual(['flowgroup'])
    expect(keys).toContainEqual(['flowgroup-related-files'])
    expect(keys).toContainEqual(['flowgroup-resolved'])
    expect(keys).toContainEqual(['pipelines'])
    expect(keys).toContainEqual(['pipeline'])
    expect(keys).toContainEqual(['pipeline-flowgroups'])
    expect(keys).toContainEqual(['tables'])
    expect(keys).toContainEqual(['dep-graph'])
    expect(keys).toContainEqual(['execution-order'])
    expect(keys).toContainEqual(['circular-deps'])
    expect(keys).toContainEqual(['stats'])
    expect(keys).toContainEqual(['file-content', 'pipelines/a.yaml'])
    expect(invalidateSpy).toHaveBeenCalledTimes(14)
  })

  it('file-changed invalidates a per-path file-content key for EACH changed path', () => {
    const { invalidateSpy } = mount()
    latestSource().emitMessage(
      'file-changed',
      JSON.stringify({ paths: ['lhp.yaml', 'config/pipeline_config_dev.yaml', 42] }),
    )

    const keys = invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)
    expect(keys).toContainEqual(['file-content', 'lhp.yaml'])
    expect(keys).toContainEqual(['file-content', 'config/pipeline_config_dev.yaml'])
    // Non-string entries are skipped, never turned into keys.
    expect(keys).not.toContainEqual(['file-content', 42])
    expect(invalidateSpy).toHaveBeenCalledTimes(15) // 13 broad + 2 per-path
  })

  it('ignores file-changed events it cannot parse', () => {
    const { invalidateSpy } = mount()
    const source = latestSource()

    source.emitMessage('file-changed', 'not json {')
    source.emitMessage('file-changed', JSON.stringify({ nope: true }))
    source.emitMessage('file-changed', 12345) // non-string data
    source.emitPlainEvent('file-changed') // not a MessageEvent

    expect(invalidateSpy).not.toHaveBeenCalled()
  })

  it('run-updated (validate) invalidates only run-history', () => {
    const { invalidateSpy } = mount()
    latestSource().emitMessage('run-updated', runUpdated('validate', 'completed'))

    expect(invalidateSpy).toHaveBeenCalledExactlyOnceWith({
      queryKey: ['run-history'],
    })
  })

  it('run-updated (generate completed) adds the post-generate invalidations', () => {
    const { invalidateSpy } = mount()
    latestSource().emitMessage('run-updated', runUpdated('generate', 'completed'))

    const keys = invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)
    expect(keys).toEqual([['run-history'], ['files'], ['dep-graph']])
  })

  it('run-updated (generate running/failed) does not add the generate extras', () => {
    const { invalidateSpy } = mount()
    latestSource().emitMessage('run-updated', runUpdated('generate', 'running'))
    latestSource().emitMessage('run-updated', runUpdated('generate', 'failed'))

    const keys = invalidateSpy.mock.calls.map(([arg]) => arg?.queryKey)
    expect(keys).toEqual([['run-history'], ['run-history']])
  })

  it('ignores run-updated payloads that fail the shape guard', () => {
    const { invalidateSpy } = mount()
    latestSource().emitMessage('run-updated', runUpdated('deploy', 'completed'))
    latestSource().emitMessage('run-updated', runUpdated('generate', 'paused'))
    latestSource().emitMessage('run-updated', JSON.stringify({ run_id: 42 }))

    expect(invalidateSpy).not.toHaveBeenCalled()
  })

  it('leaves a CONNECTING error to EventSource auto-retry (no manual reconnect)', () => {
    mount()
    const source = latestSource()
    source.failWith(MockEventSource.CONNECTING)

    vi.advanceTimersByTime(60_000)
    expect(MockEventSource.instances).toHaveLength(1)
    expect(source.close).not.toHaveBeenCalled()
  })

  it('reconnects a CLOSED source with doubling backoff, reset on open', () => {
    mount()

    // 1st failure → retry after the initial 1s backoff.
    latestSource().failWith(MockEventSource.CLOSED)
    expect(latestSource().close).toHaveBeenCalledTimes(1)
    vi.advanceTimersByTime(999)
    expect(MockEventSource.instances).toHaveLength(1)
    vi.advanceTimersByTime(1)
    expect(MockEventSource.instances).toHaveLength(2)

    // 2nd consecutive failure → backoff doubled to 2s.
    latestSource().failWith(MockEventSource.CLOSED)
    vi.advanceTimersByTime(1_999)
    expect(MockEventSource.instances).toHaveLength(2)
    vi.advanceTimersByTime(1)
    expect(MockEventSource.instances).toHaveLength(3)

    // A successful open resets the backoff to 1s.
    latestSource().open()
    latestSource().failWith(MockEventSource.CLOSED)
    vi.advanceTimersByTime(1_000)
    expect(MockEventSource.instances).toHaveLength(4)
  })

  it('invalidates everything (unfiltered) on the first open after a disconnect', () => {
    const { invalidateSpy } = mount()

    // A clean first open must NOT trigger the catch-up invalidation.
    latestSource().open()
    expect(invalidateSpy).not.toHaveBeenCalled()

    latestSource().failWith(MockEventSource.CLOSED)
    vi.advanceTimersByTime(1_000)
    latestSource().open()

    expect(invalidateSpy).toHaveBeenCalledExactlyOnceWith()

    // The disconnect flag is consumed: a further clean re-open is quiet.
    latestSource().open()
    expect(invalidateSpy).toHaveBeenCalledTimes(1)
  })

  it('unmount closes the source and cancels any pending reconnect', () => {
    const { unmount } = mount()
    const source = latestSource()
    source.failWith(MockEventSource.CLOSED)

    unmount()
    expect(source.close).toHaveBeenCalled()

    vi.advanceTimersByTime(120_000)
    expect(MockEventSource.instances).toHaveLength(1)
  })

  it('is a no-op when the environment has no EventSource', () => {
    vi.unstubAllGlobals()
    vi.stubGlobal('EventSource', undefined)
    const { invalidateSpy } = mount()
    expect(MockEventSource.instances).toHaveLength(0)
    expect(invalidateSpy).not.toHaveBeenCalled()
  })
})
