import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { parseLine, splitLines, useEventStream } from '../useEventStream'
import { startStream } from '../../api/stream'
import { ApiError } from '../../api/client'
import type { StreamFrame } from '../../types/api'

vi.mock('../../api/stream', () => ({
  startStream: vi.fn(),
}))

const startStreamMock = vi.mocked(startStream)

// ── Line-buffering / frame-parse units ───────────────────────────

describe('splitLines', () => {
  it('splits complete lines and carries the trailing partial fragment', () => {
    expect(splitLines('{"a":1}\n{"b":2}\n{"c"')).toEqual({
      lines: ['{"a":1}', '{"b":2}'],
      rest: '{"c"',
    })
  })

  it('returns an empty rest when the buffer ends on a newline', () => {
    expect(splitLines('{"a":1}\n')).toEqual({ lines: ['{"a":1}'], rest: '' })
  })

  it('holds a buffer with no newline entirely as rest', () => {
    expect(splitLines('{"partial')).toEqual({ lines: [], rest: '{"partial' })
  })

  it('handles an empty buffer', () => {
    expect(splitLines('')).toEqual({ lines: [], rest: '' })
  })
})

describe('parseLine', () => {
  it('parses a bare NDJSON line', () => {
    expect(parseLine('{"type":"PhaseStarted","phase":"Validating"}')).toEqual({
      type: 'PhaseStarted',
      phase: 'Validating',
    })
  })

  it('parses a data:-prefixed SSE line (with and without a space)', () => {
    expect(parseLine('data: {"type":"info","code":"X","message":"m"}')).toEqual({
      type: 'info',
      code: 'X',
      message: 'm',
    })
    expect(parseLine('data:{"type":"info","code":"X","message":"m"}')).toEqual({
      type: 'info',
      code: 'X',
      message: 'm',
    })
  })

  it('returns null for empty and whitespace-only lines', () => {
    expect(parseLine('')).toBeNull()
    expect(parseLine('   ')).toBeNull()
  })

  it('returns null for SSE comments', () => {
    expect(parseLine(': keep-alive')).toBeNull()
  })

  it('returns null for non-data SSE fields', () => {
    expect(parseLine('event: run-updated')).toBeNull()
    expect(parseLine('id: 42')).toBeNull()
    expect(parseLine('retry: 3000')).toBeNull()
  })

  it('returns null for a data: line with an empty payload', () => {
    expect(parseLine('data:')).toBeNull()
    expect(parseLine('data:   ')).toBeNull()
  })

  it('throws on malformed JSON (callers tolerate by skipping the line)', () => {
    expect(() => parseLine('{"broken')).toThrow()
  })
})

// ── Hook integration (mocked transport) ──────────────────────────

const encoder = new TextEncoder()

/** A closed stream that delivered `chunks` in order. */
function streamOf(chunks: string[]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk))
      controller.close()
    },
  })
}

function responseOf(chunks: string[]): Response {
  return { body: streamOf(chunks) } as Response
}

/** A stream held open until the caller closes/errors it (abort tests). */
function openStream(): {
  stream: ReadableStream<Uint8Array>
  push: (chunk: string) => void
  close: () => void
  errorWith: (err: unknown) => void
} {
  let controller!: ReadableStreamDefaultController<Uint8Array>
  const stream = new ReadableStream<Uint8Array>({
    start(c) {
      controller = c
    },
  })
  return {
    stream,
    push: (chunk) => controller.enqueue(encoder.encode(chunk)),
    close: () => controller.close(),
    errorWith: (err) => controller.error(err),
  }
}

function frameLine(frame: StreamFrame): string {
  return `${JSON.stringify(frame)}\n`
}

const infoFrame = (message: string): StreamFrame => ({
  type: 'info',
  code: 'I',
  message,
})

function setup() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  const rendered = renderHook(() => useEventStream(), { wrapper })
  return { ...rendered, invalidateSpy }
}

const startOptions = { path: '/api/validate/stream', env: 'dev' } as const

beforeEach(() => {
  vi.clearAllMocks()
})

describe('useEventStream', () => {
  it('decodes multiple frames from a single chunk, in order', async () => {
    startStreamMock.mockResolvedValue(
      responseOf([frameLine(infoFrame('one')) + frameLine(infoFrame('two'))]),
    )
    const { result } = setup()
    const onFrame = vi.fn()
    const onDone = vi.fn()

    act(() => result.current.start(startOptions, { onFrame, onDone }))
    await waitFor(() => expect(result.current.isRunning).toBe(false))

    expect(onFrame.mock.calls.map(([f]) => f.message)).toEqual(['one', 'two'])
    expect(result.current.frames).toHaveLength(2)
    expect(result.current.error).toBeNull()
    expect(onDone).toHaveBeenCalledExactlyOnceWith({ aborted: false })
  })

  it('buffers a frame split across chunk boundaries', async () => {
    const line = frameLine(infoFrame('split-across-chunks'))
    startStreamMock.mockResolvedValue(
      responseOf([line.slice(0, 12), line.slice(12)]),
    )
    const { result } = setup()

    act(() => result.current.start(startOptions))
    await waitFor(() => expect(result.current.isRunning).toBe(false))

    expect(result.current.frames).toEqual([infoFrame('split-across-chunks')])
  })

  it('flushes a trailing frame when the stream ends without a newline', async () => {
    startStreamMock.mockResolvedValue(
      responseOf([frameLine(infoFrame('first')), JSON.stringify(infoFrame('last'))]),
    )
    const { result } = setup()

    act(() => result.current.start(startOptions))
    await waitFor(() => expect(result.current.isRunning).toBe(false))

    expect(result.current.frames.map((f) => (f as { message: string }).message)).toEqual([
      'first',
      'last',
    ])
  })

  it('skips malformed JSON lines without aborting the run', async () => {
    startStreamMock.mockResolvedValue(
      responseOf(['{"broken\n', frameLine(infoFrame('good'))]),
    )
    const { result } = setup()
    const onError = vi.fn()

    act(() => result.current.start(startOptions, { onError }))
    await waitFor(() => expect(result.current.isRunning).toBe(false))

    expect(result.current.frames).toEqual([infoFrame('good')])
    expect(result.current.error).toBeNull()
    expect(onError).not.toHaveBeenCalled()
  })

  it('tolerates SSE framing (data: prefix, comments, non-data fields)', async () => {
    startStreamMock.mockResolvedValue(
      responseOf([
        ': keep-alive\nevent: run\nid: 1\nretry: 3000\n',
        `data: ${JSON.stringify(infoFrame('via-sse'))}\n`,
      ]),
    )
    const { result } = setup()

    act(() => result.current.start(startOptions))
    await waitFor(() => expect(result.current.isRunning).toBe(false))

    expect(result.current.frames).toEqual([infoFrame('via-sse')])
  })

  it('surfaces a terminal error frame via state and onError', async () => {
    const errorFrame: StreamFrame = {
      type: 'error',
      code: 'LHP-IO-001',
      title: 'Disk full',
      details: null,
      suggestions: [],
      context: {},
      doc_link: null,
    }
    startStreamMock.mockResolvedValue(responseOf([frameLine(errorFrame)]))
    const { result, invalidateSpy } = setup()
    const onError = vi.fn()
    const onDone = vi.fn()

    act(() => result.current.start(startOptions, { onError, onDone }))
    await waitFor(() => expect(result.current.isRunning).toBe(false))

    expect(result.current.error).toEqual(errorFrame)
    expect(onError).toHaveBeenCalledExactlyOnceWith(errorFrame)
    expect(onDone).toHaveBeenCalledExactlyOnceWith({ aborted: false })
    expect(invalidateSpy).not.toHaveBeenCalled()
  })

  it('surfaces an HTTP open failure (ApiError) via onError', async () => {
    const apiError = new ApiError(422, {
      code: 'LHP-CFG-001',
      category: 'config',
      message: 'bad env',
      details: '',
      suggestions: [],
      context: {},
      http_status: 422,
    })
    startStreamMock.mockRejectedValue(apiError)
    const { result } = setup()
    const onError = vi.fn()

    act(() => result.current.start(startOptions, { onError }))
    await waitFor(() => expect(result.current.isRunning).toBe(false))

    expect(result.current.error).toBe(apiError)
    expect(onError).toHaveBeenCalledExactlyOnceWith(apiError)
  })

  it('errors when the response has no body', async () => {
    startStreamMock.mockResolvedValue({ body: null } as Response)
    const { result } = setup()

    act(() => result.current.start(startOptions))
    await waitFor(() => expect(result.current.isRunning).toBe(false))

    expect(result.current.error).toBeInstanceOf(Error)
    expect((result.current.error as Error).message).toBe(
      'Stream response had no body',
    )
  })

  it('invalidates files + dep-graph only after a clean successful generate', async () => {
    const completed: StreamFrame = {
      type: 'GenerationCompleted',
      response: {
        success: true,
        pipeline_responses: {},
        total_files_written: 0,
        aggregate_generated_filenames: [],
        output_location: null,
        error_message: null,
        error_code: null,
      },
    }
    startStreamMock.mockResolvedValue(responseOf([frameLine(completed)]))
    const { result, invalidateSpy } = setup()

    act(() => result.current.start({ path: '/api/generate/stream', env: 'dev' }))
    await waitFor(() => expect(result.current.isRunning).toBe(false))

    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['files'] })
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['dep-graph'] })
    expect(invalidateSpy).toHaveBeenCalledTimes(2)
  })

  it('ignores a re-entrant start() while a stream is running', async () => {
    const held = openStream()
    startStreamMock.mockResolvedValue({ body: held.stream } as Response)
    const { result } = setup()

    act(() => result.current.start(startOptions))
    await waitFor(() => expect(result.current.isRunning).toBe(true))
    act(() => result.current.start(startOptions))
    expect(startStreamMock).toHaveBeenCalledTimes(1)

    held.close()
    await waitFor(() => expect(result.current.isRunning).toBe(false))
  })

  it('abort() ends the run as aborted: no error, no invalidation', async () => {
    const held = openStream()
    startStreamMock.mockImplementation(async (_path, _body, signal) => {
      // Mirror real fetch: an abort rejects the in-flight read.
      signal?.addEventListener('abort', () =>
        held.errorWith(new DOMException('The operation was aborted.', 'AbortError')),
      )
      return { body: held.stream } as Response
    })
    const { result, invalidateSpy } = setup()
    const onError = vi.fn()
    const onDone = vi.fn()

    act(() => result.current.start(startOptions, { onError, onDone }))
    await waitFor(() => expect(result.current.isRunning).toBe(true))
    act(() => result.current.abort())
    await waitFor(() => expect(result.current.isRunning).toBe(false))

    expect(onDone).toHaveBeenCalledExactlyOnceWith({ aborted: true })
    expect(onError).not.toHaveBeenCalled()
    expect(result.current.error).toBeNull()
    expect(invalidateSpy).not.toHaveBeenCalled()
  })

  it('a new start() clears the previous run’s frames and error', async () => {
    startStreamMock.mockResolvedValueOnce(
      responseOf([
        frameLine({
          type: 'error',
          code: 'X',
          title: 'boom',
          details: null,
          suggestions: [],
          context: {},
          doc_link: null,
        }),
      ]),
    )
    const { result } = setup()
    act(() => result.current.start(startOptions))
    await waitFor(() => expect(result.current.isRunning).toBe(false))
    expect(result.current.error).not.toBeNull()

    startStreamMock.mockResolvedValueOnce(responseOf([frameLine(infoFrame('fresh'))]))
    act(() => result.current.start(startOptions))
    await waitFor(() => expect(result.current.isRunning).toBe(false))

    expect(result.current.error).toBeNull()
    expect(result.current.frames).toEqual([infoFrame('fresh')])
  })
})
