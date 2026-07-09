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
    conversations: {},
    tabOrder: [],
    activeTabKey: null,
    tabTitles: {},
    nextDraftId: 1,
    panelOpen: false,
    permissionMode: 'default',
  })
}

const store = () => useAssistantStore.getState()

function conversationOf(tabKey: string) {
  return store().conversations[tabKey]
}

beforeEach(() => {
  vi.clearAllMocks()
  resetStore()
})

describe('useAssistantStream', () => {
  it('feeds decoded frames into the tab conversation, rekeying on the session frame', async () => {
    startChatMock.mockResolvedValue(
      responseOf([
        frameLine({ type: 'status', state: 'preparing' }) +
          frameLine({ type: 'session', session_id: 'claude_1', created: true }) +
          frameLine({ type: 'text.delta', delta: 'Hel' }),
        frameLine({ type: 'text.delta', delta: 'lo' }) +
          frameLine({ type: 'item.done', item: { id: 'fc1', type: 'function_call' } }) +
          frameLine({ type: 'turn.completed' }),
      ]),
    )
    const draftKey = store().openTab()
    const { result } = setup()

    act(() => result.current.send(draftKey, 'hi'))
    await waitFor(() =>
      expect(conversationOf('claude_1')?.streaming).toBe(false),
    )

    const s = store()
    // The draft tab was re-keyed to the real session id, position preserved.
    expect(s.tabOrder).toEqual(['claude_1'])
    expect(s.activeTabKey).toBe('claude_1')
    expect(s.conversations[draftKey]).toBeUndefined()
    const conversation = conversationOf('claude_1')
    expect(conversation.sessionMeta).toEqual({ sessionId: 'claude_1' })
    expect(conversation.parts).toMatchObject([
      { kind: 'text', role: 'user', text: 'hi' },
      { kind: 'text', role: 'assistant', text: 'Hello' },
      { kind: 'item', item: { id: 'fc1' } },
    ])
    expect(conversation.failure).toBeNull()
    expect(startChatMock).toHaveBeenCalledExactlyOnceWith(
      // The tab key doubles as the session target; the store's permission
      // mode rides along on every turn.
      { message: 'hi', permission_mode: 'default', session_id: draftKey },
      expect.any(AbortSignal),
    )
  })

  it('sends the currently selected permission mode and the real tab key', async () => {
    startChatMock.mockResolvedValue(
      responseOf([frameLine({ type: 'turn.completed' })]),
    )
    store().setPermissionMode('acceptEdits')
    store().openSessionTab('claude_9')
    const { result } = setup()

    act(() => result.current.send('claude_9', 'go'))
    await waitFor(() => expect(conversationOf('claude_9').streaming).toBe(false))

    expect(startChatMock).toHaveBeenCalledExactlyOnceWith(
      { message: 'go', permission_mode: 'acceptEdits', session_id: 'claude_9' },
      expect.any(AbortSignal),
    )
  })

  it('two tabs stream simultaneously with independent parts', async () => {
    const heldA = openStream()
    const heldB = openStream()
    startChatMock
      .mockResolvedValueOnce({ body: heldA.stream } as Response)
      .mockResolvedValueOnce({ body: heldB.stream } as Response)
    store().openSessionTab('claude_a')
    store().openSessionTab('claude_b')
    const { result } = setup()

    act(() => {
      result.current.send('claude_a', 'one')
      result.current.send('claude_b', 'two')
    })
    act(() => {
      heldA.push(frameLine({ type: 'text.delta', delta: 'Alpha' }))
      heldB.push(frameLine({ type: 'text.delta', delta: 'Beta' }))
    })
    await waitFor(() => {
      expect(conversationOf('claude_a').parts).toHaveLength(2)
      expect(conversationOf('claude_b').parts).toHaveLength(2)
    })

    expect(conversationOf('claude_a').parts[1]).toMatchObject({ text: 'Alpha' })
    expect(conversationOf('claude_b').parts[1]).toMatchObject({ text: 'Beta' })
    expect(conversationOf('claude_a').streaming).toBe(true)
    expect(conversationOf('claude_b').streaming).toBe(true)

    // One tab finishing leaves the other streaming.
    act(() => heldA.close())
    await waitFor(() => expect(conversationOf('claude_a').streaming).toBe(false))
    expect(conversationOf('claude_b').streaming).toBe(true)

    heldB.close()
    await waitFor(() => expect(conversationOf('claude_b').streaming).toBe(false))
  })

  it('frames from a background tab keep applying to THAT tab after a rekey', async () => {
    const held = openStream()
    startChatMock.mockResolvedValue({ body: held.stream } as Response)
    const draftKey = store().openTab()
    const { result } = setup()

    act(() => result.current.send(draftKey, 'hi'))
    act(() =>
      held.push(
        frameLine({ type: 'session', session_id: 'claude_bg', created: true }),
      ),
    )
    await waitFor(() =>
      expect(store().tabOrder).toEqual(['claude_bg']),
    )

    // User switches away: the stream's tab is now in the background.
    store().openTab()
    expect(store().activeTabKey).toBe('draft:2')

    act(() => held.push(frameLine({ type: 'text.delta', delta: 'later' })))
    await waitFor(() =>
      expect(conversationOf('claude_bg').parts).toHaveLength(2),
    )
    expect(conversationOf('claude_bg').parts[1]).toMatchObject({ text: 'later' })
    expect(conversationOf('draft:2').parts).toEqual([])

    held.close()
    await waitFor(() => expect(conversationOf('claude_bg').streaming).toBe(false))
  })

  it('a frame split across chunk boundaries is buffered, not dropped', async () => {
    const line = frameLine({ type: 'text.delta', delta: 'split-frame' })
    startChatMock.mockResolvedValue(
      responseOf([line.slice(0, 10), line.slice(10), frameLine({ type: 'turn.completed' })]),
    )
    store().openSessionTab('claude_1')
    const { result } = setup()

    act(() => result.current.send('claude_1', 'hi'))
    await waitFor(() => expect(conversationOf('claude_1').streaming).toBe(false))

    expect(conversationOf('claude_1').parts[1]).toMatchObject({
      kind: 'text',
      text: 'split-frame',
    })
  })

  it('a terminal error frame lands as a stream_error failure on its tab', async () => {
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
    store().openSessionTab('claude_1')
    const { result } = setup()

    act(() => result.current.send('claude_1', 'hi'))
    await waitFor(() => expect(conversationOf('claude_1').streaming).toBe(false))

    const conversation = conversationOf('claude_1')
    expect(conversation.failure).toMatchObject({
      kind: 'stream_error',
      frame: { code: 'LHP-GEN-902' },
    })
    // The partial text stays visible above the failure card.
    expect(conversation.parts[1]).toMatchObject({ kind: 'text', text: 'partial' })
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
    store().openSessionTab('claude_1')
    const { result } = setup()

    act(() => result.current.send('claude_1', 'hi'))
    await waitFor(() => expect(conversationOf('claude_1').streaming).toBe(false))

    expect(conversationOf('claude_1').failure).toMatchObject({
      kind: 'gate',
      code: 'LHP-WEB-002',
    })
  })

  it('a transport drop mid-stream becomes a transport failure', async () => {
    const held = openStream()
    startChatMock.mockResolvedValue({ body: held.stream } as Response)
    store().openSessionTab('claude_1')
    const { result } = setup()

    act(() => result.current.send('claude_1', 'hi'))
    act(() => held.push(frameLine({ type: 'text.delta', delta: 'x' })))
    await waitFor(() =>
      expect(conversationOf('claude_1').parts).toHaveLength(2),
    )
    act(() => held.errorWith(new Error('connection reset')))
    await waitFor(() => expect(conversationOf('claude_1').streaming).toBe(false))

    expect(conversationOf('claude_1').failure).toMatchObject({
      kind: 'transport',
    })
  })

  it('per-tab abort closes ONLY that turn, leaving the other stream running', async () => {
    const heldA = openStream()
    const heldB = openStream()
    startChatMock.mockImplementation(async (body, signal) => {
      const held = body.session_id === 'claude_a' ? heldA : heldB
      signal?.addEventListener('abort', () =>
        held.errorWith(new DOMException('The operation was aborted.', 'AbortError')),
      )
      return { body: held.stream } as Response
    })
    store().openSessionTab('claude_a')
    store().openSessionTab('claude_b')
    const { result } = setup()

    act(() => {
      result.current.send('claude_a', 'one')
      result.current.send('claude_b', 'two')
    })
    await waitFor(() => expect(conversationOf('claude_a').streaming).toBe(true))

    act(() => result.current.abort('claude_a'))
    await waitFor(() => expect(conversationOf('claude_a').streaming).toBe(false))

    // Abort records no failure; the OTHER tab's stream is untouched.
    expect(conversationOf('claude_a').failure).toBeNull()
    expect(conversationOf('claude_b').streaming).toBe(true)

    act(() => heldB.push(frameLine({ type: 'turn.completed' })))
    act(() => heldB.close())
    await waitFor(() => expect(conversationOf('claude_b').streaming).toBe(false))
  })

  it('ignores a re-entrant send() on the SAME tab while it streams', async () => {
    const held = openStream()
    startChatMock.mockResolvedValue({ body: held.stream } as Response)
    store().openSessionTab('claude_1')
    const { result } = setup()

    act(() => result.current.send('claude_1', 'one'))
    await waitFor(() => expect(conversationOf('claude_1').streaming).toBe(true))
    act(() => result.current.send('claude_1', 'two'))
    expect(startChatMock).toHaveBeenCalledTimes(1)
    // Only the first user message was appended.
    expect(
      conversationOf('claude_1').parts.filter((p) => p.kind === 'text'),
    ).toHaveLength(1)

    held.close()
    await waitFor(() => expect(conversationOf('claude_1').streaming).toBe(false))
  })

  it('the double-send guard follows a rekeyed tab', async () => {
    const held = openStream()
    startChatMock.mockResolvedValue({ body: held.stream } as Response)
    const draftKey = store().openTab()
    const { result } = setup()

    act(() => result.current.send(draftKey, 'one'))
    act(() =>
      held.push(
        frameLine({ type: 'session', session_id: 'claude_1', created: true }),
      ),
    )
    await waitFor(() => expect(store().tabOrder).toEqual(['claude_1']))

    // A send on the tab's NEW key is still guarded by the live stream.
    act(() => result.current.send('claude_1', 'two'))
    expect(startChatMock).toHaveBeenCalledTimes(1)

    held.close()
    await waitFor(() => expect(conversationOf('claude_1').streaming).toBe(false))
  })
})
