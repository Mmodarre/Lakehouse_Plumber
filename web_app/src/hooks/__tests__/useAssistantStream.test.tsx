import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { useAssistantStream } from '../useAssistantStream'
import { startAssistantChat } from '../../api/assistant'
import { ApiError } from '../../api/client'
import { useAssistantStore } from '../../store/assistantStore'
import type { AssistantFrame } from '../../types/assistant'

vi.mock('../../api/assistant', () => ({
  startAssistantChat: vi.fn(),
}))

const startChatMock = vi.mocked(startAssistantChat)

// ── Scripted-stream helpers (mirrors useEventStream.test) ────────

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

function frameLine(frame: AssistantFrame): string {
  return `${JSON.stringify(frame)}\n`
}

function setup() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  return renderHook(() => useAssistantStream(), { wrapper })
}

function resetStore() {
  useAssistantStore.setState({
    parts: [],
    streaming: false,
    statusState: null,
    pendingApproval: null,
    sessionMeta: null,
    failure: null,
    interrupted: false,
    panelOpen: false,
  })
}

beforeEach(() => {
  vi.clearAllMocks()
  resetStore()
})

describe('useAssistantStream', () => {
  it('feeds decoded frames into the store in arrival order', async () => {
    startChatMock.mockResolvedValue(
      responseOf([
        frameLine({ type: 'status', state: 'preparing' }) +
          frameLine({ type: 'session', session_id: 'conv_1', created: true }) +
          frameLine({ type: 'text.delta', delta: 'Hel' }),
        frameLine({ type: 'text.delta', delta: 'lo' }) +
          frameLine({ type: 'item.done', item: { id: 'fc1', type: 'function_call' } }) +
          frameLine({ type: 'turn.completed' }),
      ]),
    )
    const { result } = setup()

    act(() => result.current.send('hi'))
    await waitFor(() => expect(useAssistantStore.getState().streaming).toBe(false))

    const s = useAssistantStore.getState()
    expect(s.sessionMeta).toEqual({ sessionId: 'conv_1' })
    expect(s.parts).toMatchObject([
      { kind: 'text', role: 'user', text: 'hi' },
      { kind: 'text', role: 'assistant', text: 'Hello' },
      { kind: 'item', item: { id: 'fc1' } },
    ])
    expect(s.failure).toBeNull()
    expect(startChatMock).toHaveBeenCalledExactlyOnceWith(
      { message: 'hi' },
      expect.any(AbortSignal),
    )
  })

  it('a frame split across chunk boundaries is buffered, not dropped', async () => {
    const line = frameLine({ type: 'text.delta', delta: 'split-frame' })
    startChatMock.mockResolvedValue(
      responseOf([line.slice(0, 10), line.slice(10), frameLine({ type: 'turn.completed' })]),
    )
    const { result } = setup()

    act(() => result.current.send('hi'))
    await waitFor(() => expect(useAssistantStore.getState().streaming).toBe(false))

    expect(useAssistantStore.getState().parts[1]).toMatchObject({
      kind: 'text',
      text: 'split-frame',
    })
  })

  it('a terminal error frame lands as a stream_error failure', async () => {
    startChatMock.mockResolvedValue(
      responseOf([
        frameLine({ type: 'text.delta', delta: 'partial' }),
        frameLine({
          type: 'error',
          code: 'LHP-GEN-902',
          title: 'Assistant daemon connection lost',
          details: null,
          suggestions: [],
          context: {},
          doc_link: null,
        }),
      ]),
    )
    const { result } = setup()

    act(() => result.current.send('hi'))
    await waitFor(() => expect(useAssistantStore.getState().streaming).toBe(false))

    const s = useAssistantStore.getState()
    expect(s.failure).toMatchObject({
      kind: 'stream_error',
      frame: { code: 'LHP-GEN-902' },
    })
    // The partial text stays visible above the failure card.
    expect(s.parts[1]).toMatchObject({ kind: 'text', text: 'partial' })
  })

  it('a gate 409 on open (LHP-WEB-002) becomes a gate failure', async () => {
    startChatMock.mockRejectedValue(
      new ApiError(409, {
        code: 'LHP-WEB-002',
        category: 'CFG',
        message: 'LHP skill is not installed in this project',
        details: '',
        suggestions: [],
        context: {},
        http_status: 409,
      }),
    )
    const { result } = setup()

    act(() => result.current.send('hi'))
    await waitFor(() => expect(useAssistantStore.getState().streaming).toBe(false))

    expect(useAssistantStore.getState().failure).toMatchObject({
      kind: 'gate',
      code: 'LHP-WEB-002',
    })
  })

  it('a transport drop mid-stream becomes a transport failure', async () => {
    const held = openStream()
    startChatMock.mockResolvedValue({ body: held.stream } as Response)
    const { result } = setup()

    act(() => result.current.send('hi'))
    act(() => held.push(frameLine({ type: 'text.delta', delta: 'x' })))
    await waitFor(() =>
      expect(useAssistantStore.getState().parts).toHaveLength(2),
    )
    act(() => held.errorWith(new Error('connection reset')))
    await waitFor(() => expect(useAssistantStore.getState().streaming).toBe(false))

    expect(useAssistantStore.getState().failure).toMatchObject({
      kind: 'transport',
    })
  })

  it('abort mid-stream closes the turn without recording a failure', async () => {
    const held = openStream()
    startChatMock.mockImplementation(async (_body, signal) => {
      // Mirror real fetch: an abort rejects the in-flight read.
      signal?.addEventListener('abort', () =>
        held.errorWith(new DOMException('The operation was aborted.', 'AbortError')),
      )
      return { body: held.stream } as Response
    })
    const { result } = setup()

    act(() => result.current.send('hi'))
    await waitFor(() => expect(result.current.isStreaming).toBe(true))
    act(() => result.current.abort())
    await waitFor(() => expect(useAssistantStore.getState().streaming).toBe(false))

    expect(useAssistantStore.getState().failure).toBeNull()
    expect(useAssistantStore.getState().interrupted).toBe(false)
  })

  it('ignores a re-entrant send() while a turn is streaming', async () => {
    const held = openStream()
    startChatMock.mockResolvedValue({ body: held.stream } as Response)
    const { result } = setup()

    act(() => result.current.send('one'))
    await waitFor(() => expect(result.current.isStreaming).toBe(true))
    act(() => result.current.send('two'))
    expect(startChatMock).toHaveBeenCalledTimes(1)
    // Only the first user message was appended.
    expect(
      useAssistantStore.getState().parts.filter((p) => p.kind === 'text'),
    ).toHaveLength(1)

    held.close()
    await waitFor(() => expect(useAssistantStore.getState().streaming).toBe(false))
  })
})
