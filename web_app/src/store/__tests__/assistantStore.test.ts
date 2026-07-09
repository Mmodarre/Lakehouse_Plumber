import { beforeEach, describe, expect, it } from 'vitest'
import {
  normalizeSnapshotItem,
  useAssistantStore,
} from '@/store/assistantStore'
import { ApiError } from '@/api/client'
import type { AssistantFrame, SessionFailedHint } from '@/types/assistant'
import type { ErrorFrame } from '@/types/api'

// ── Frame builders ───────────────────────────────────────────────

const textDelta = (delta: string): AssistantFrame => ({ type: 'text.delta', delta })
const reasoningDelta = (delta: string): AssistantFrame => ({
  type: 'reasoning.delta',
  delta,
})

const errorFrame: ErrorFrame = {
  type: 'error',
  code: 'LHP-GEN-902',
  title: 'Assistant daemon connection lost',
  details: null,
  suggestions: ['Restart the daemon'],
  context: {},
  doc_link: null,
}

function apply(...frames: AssistantFrame[]) {
  for (const frame of frames) {
    useAssistantStore.getState().applyFrame(frame)
  }
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
  resetStore()
})

describe('assistantStore.beginTurn', () => {
  it('appends the user message and opens the streaming turn', () => {
    useAssistantStore.getState().beginTurn('hello')
    const s = useAssistantStore.getState()
    expect(s.parts).toMatchObject([{ kind: 'text', role: 'user', text: 'hello' }])
    expect(s.streaming).toBe(true)
    expect(s.failure).toBeNull()
    expect(s.interrupted).toBe(false)
  })

  it('clears the previous turn failure and stale pending approval', () => {
    useAssistantStore.setState({
      failure: { kind: 'transport', message: 'boom' },
      pendingApproval: { elicitationId: 'e1', params: {} },
    })
    useAssistantStore.getState().beginTurn('again')
    const s = useAssistantStore.getState()
    expect(s.failure).toBeNull()
    expect(s.pendingApproval).toBeNull()
  })
})

describe('assistantStore.applyFrame — delta coalescing', () => {
  it('coalesces consecutive text deltas into one assistant part', () => {
    useAssistantStore.getState().beginTurn('hi')
    apply(textDelta('Hel'), textDelta('lo'))
    const parts = useAssistantStore.getState().parts
    expect(parts).toHaveLength(2)
    expect(parts[1]).toMatchObject({ kind: 'text', role: 'assistant', text: 'Hello' })
  })

  it('never coalesces an assistant delta into the user message', () => {
    useAssistantStore.getState().beginTurn('hi')
    apply(textDelta('yo'))
    const parts = useAssistantStore.getState().parts
    expect(parts[0]).toMatchObject({ kind: 'text', role: 'user', text: 'hi' })
    expect(parts[1]).toMatchObject({ kind: 'text', role: 'assistant', text: 'yo' })
  })

  it('starts new parts across text/reasoning boundaries, preserving order', () => {
    useAssistantStore.getState().beginTurn('hi')
    apply(
      textDelta('a'),
      textDelta('b'),
      reasoningDelta('think'),
      reasoningDelta('ing'),
      textDelta('c'),
    )
    const parts = useAssistantStore.getState().parts
    expect(parts.map((p) => p.kind)).toEqual(['text', 'text', 'reasoning', 'text'])
    expect(parts[1]).toMatchObject({ kind: 'text', text: 'ab' })
    expect(parts[2]).toMatchObject({ kind: 'reasoning', text: 'thinking' })
    expect(parts[3]).toMatchObject({ kind: 'text', text: 'c' })
  })

  it('an item.done between text deltas splits the text into two parts', () => {
    useAssistantStore.getState().beginTurn('hi')
    apply(
      textDelta('before'),
      { type: 'item.done', item: { id: 'fc1', type: 'function_call' } },
      textDelta('after'),
    )
    const parts = useAssistantStore.getState().parts
    expect(parts.map((p) => p.kind)).toEqual(['text', 'text', 'item', 'text'])
    expect(parts[2]).toMatchObject({ kind: 'item', item: { id: 'fc1' } })
  })
})

describe('assistantStore.applyFrame — approvals', () => {
  it('approval.request sets pendingApproval and appends an approval part', () => {
    apply({
      type: 'approval.request',
      elicitation_id: 'e1',
      params: { message: 'Run this?' },
    })
    const s = useAssistantStore.getState()
    expect(s.pendingApproval).toEqual({
      elicitationId: 'e1',
      params: { message: 'Run this?' },
    })
    expect(s.parts).toMatchObject([
      { kind: 'approval', elicitationId: 'e1', resolved: null },
    ])
  })

  it('resolveApprovalLocal marks the part resolved and clears the pending state', () => {
    apply({ type: 'approval.request', elicitation_id: 'e1', params: {} })
    useAssistantStore.getState().resolveApprovalLocal('e1', 'accept')
    const s = useAssistantStore.getState()
    expect(s.pendingApproval).toBeNull()
    expect(s.parts[0]).toMatchObject({ kind: 'approval', resolved: 'accept' })
  })

  it('resolving an unknown elicitation leaves a different pending one intact', () => {
    apply({ type: 'approval.request', elicitation_id: 'e1', params: {} })
    useAssistantStore.getState().resolveApprovalLocal('other', 'decline')
    expect(useAssistantStore.getState().pendingApproval).not.toBeNull()
  })
})

describe('assistantStore.applyFrame — session identity', () => {
  it('records the session id', () => {
    apply({ type: 'session', session_id: 'conv_1', created: false })
    expect(useAssistantStore.getState().sessionMeta).toEqual({
      sessionId: 'conv_1',
    })
  })

  it('created=true on the FIRST turn adds no divider', () => {
    useAssistantStore.getState().beginTurn('hi')
    apply({ type: 'session', session_id: 'conv_1', created: true })
    expect(
      useAssistantStore.getState().parts.every((p) => p.kind !== 'divider'),
    ).toBe(true)
  })

  it('created=true mid-conversation inserts a divider before the new user message', () => {
    useAssistantStore.getState().beginTurn('first')
    apply(textDelta('answer'), { type: 'turn.completed' })
    useAssistantStore.getState().beginTurn('second')
    apply({ type: 'session', session_id: 'conv_2', created: true })
    const parts = useAssistantStore.getState().parts
    expect(parts.map((p) => p.kind)).toEqual(['text', 'text', 'divider', 'text'])
    expect(parts[2]).toMatchObject({ kind: 'divider', label: 'new session started' })
    expect(parts[3]).toMatchObject({ kind: 'text', role: 'user', text: 'second' })
  })
})

describe('assistantStore.applyFrame — terminal frames', () => {
  it('turn.completed closes the streaming turn cleanly', () => {
    useAssistantStore.getState().beginTurn('hi')
    apply({ type: 'status', state: 'running' }, { type: 'turn.completed' })
    const s = useAssistantStore.getState()
    expect(s.streaming).toBe(false)
    expect(s.statusState).toBeNull()
    expect(s.failure).toBeNull()
  })

  it('turn.failed records the reason as a failure', () => {
    useAssistantStore.getState().beginTurn('hi')
    apply({ type: 'turn.failed', reason: 'model exploded' })
    const s = useAssistantStore.getState()
    expect(s.streaming).toBe(false)
    expect(s.failure).toEqual({ kind: 'turn_failed', reason: 'model exploded' })
  })

  it('interrupted marks the turn interrupted (not failed)', () => {
    useAssistantStore.getState().beginTurn('hi')
    apply({ type: 'interrupted' })
    const s = useAssistantStore.getState()
    expect(s.streaming).toBe(false)
    expect(s.interrupted).toBe(true)
    expect(s.failure).toBeNull()
  })

  it('a terminal error frame becomes a stream_error failure', () => {
    useAssistantStore.getState().beginTurn('hi')
    apply(errorFrame)
    const s = useAssistantStore.getState()
    expect(s.streaming).toBe(false)
    expect(s.failure).toEqual({ kind: 'stream_error', frame: errorFrame })
  })

  it.each<SessionFailedHint>(['omnigent_setup', 'databricks_auth', 'unknown'])(
    'session.failed carries the %s hint into the failure state',
    (hint) => {
      useAssistantStore.getState().beginTurn('hi')
      apply({ type: 'session.failed', detail: 'runner_error: boom', hint })
      const s = useAssistantStore.getState()
      expect(s.streaming).toBe(false)
      expect(s.failure).toEqual({
        kind: 'session_failed',
        detail: 'runner_error: boom',
        hint,
      })
    },
  )

  it('heartbeat frames change nothing', () => {
    useAssistantStore.getState().beginTurn('hi')
    const before = useAssistantStore.getState().parts
    apply({ type: 'heartbeat' })
    expect(useAssistantStore.getState().parts).toBe(before)
    expect(useAssistantStore.getState().streaming).toBe(true)
  })
})

describe('assistantStore.failTransport', () => {
  it('maps a chat-gate 409 (LHP-WEB-002) to a gate failure', () => {
    useAssistantStore.getState().beginTurn('hi')
    useAssistantStore.getState().failTransport(
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
    const s = useAssistantStore.getState()
    expect(s.streaming).toBe(false)
    expect(s.failure).toEqual({
      kind: 'gate',
      code: 'LHP-WEB-002',
      message: 'LHP skill is not installed in this project',
    })
  })

  it('maps a plain transport error to a transport failure', () => {
    useAssistantStore.getState().beginTurn('hi')
    useAssistantStore.getState().failTransport(new Error('network down'))
    expect(useAssistantStore.getState().failure).toEqual({
      kind: 'transport',
      message: 'network down',
    })
  })
})

describe('snapshot normalization + hydration', () => {
  it('normalizeSnapshotItem unwraps data and merges envelope id/type/status', () => {
    const item = normalizeSnapshotItem({
      id: 'fc1',
      type: 'function_call',
      status: 'completed',
      response_id: 'resp_1',
      created_at: '2026-07-08T00:00:00Z',
      created_by: 'assistant',
      data: { name: 'run_lhp', arguments: '{}', status: 'in_progress' },
    })
    expect(item).toEqual({
      id: 'fc1',
      type: 'function_call',
      status: 'completed', // envelope wins over data.status
      name: 'run_lhp',
      arguments: '{}',
    })
  })

  it('hydrateFromSnapshot maps enveloped items onto the shared part shapes', () => {
    useAssistantStore.getState().hydrateFromSnapshot({
      session_id: 'conv_9',
      title: null,
      status: 'idle',
      items: [
        {
          id: 'msg_1',
          type: 'message',
          data: {
            role: 'user',
            content: [{ type: 'input_text', text: 'generate the pipeline' }],
          },
        },
        {
          id: 'r_1',
          type: 'reasoning',
          data: { summary: [{ type: 'summary_text', text: 'planning' }] },
        },
        {
          id: 'fc_1',
          type: 'function_call',
          status: 'completed',
          data: { name: 'Bash', arguments: '{"cmd":"lhp generate"}' },
        },
        {
          id: 'msg_2',
          type: 'message',
          data: {
            role: 'assistant',
            content: [{ type: 'output_text', text: 'Done.' }],
          },
        },
      ],
    })
    const s = useAssistantStore.getState()
    expect(s.sessionMeta).toEqual({ sessionId: 'conv_9' })
    expect(s.parts).toMatchObject([
      { kind: 'text', role: 'user', text: 'generate the pipeline' },
      { kind: 'reasoning', text: 'planning' },
      { kind: 'item', item: { id: 'fc_1', type: 'function_call', name: 'Bash' } },
      { kind: 'text', role: 'assistant', text: 'Done.' },
    ])
  })
})

describe('assistantStore persistence', () => {
  it('persists ONLY UI preferences — never conversation state', () => {
    useAssistantStore.getState().beginTurn('secret conversation')
    useAssistantStore.getState().setPanelOpen(true)
    const raw = localStorage.getItem('lhp-assistant')
    expect(raw).not.toBeNull()
    const persisted = JSON.parse(raw as string) as { state: Record<string, unknown> }
    expect(persisted.state).toEqual({
      panelOpen: true,
      panelWidth: 360,
      permissionMode: 'default',
    })
  })

  it('clamps panelWidth to the resize bounds', () => {
    useAssistantStore.getState().setPanelWidth(10)
    expect(useAssistantStore.getState().panelWidth).toBe(300)
    useAssistantStore.getState().setPanelWidth(5000)
    expect(useAssistantStore.getState().panelWidth).toBe(760)
  })
})
