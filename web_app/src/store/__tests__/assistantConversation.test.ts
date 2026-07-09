import { describe, expect, it } from 'vitest'
import {
  applyFrameToConversation,
  beginTurnInConversation,
  conversationFromSnapshot,
  emptyConversation,
  failTransportInConversation,
  finishStreamInConversation,
  normalizeSnapshotItem,
  resolveApprovalInConversation,
} from '@/store/assistantConversation'
import type { ConversationState } from '@/store/assistantConversation'
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

function apply(
  state: ConversationState,
  ...frames: AssistantFrame[]
): ConversationState {
  return frames.reduce(applyFrameToConversation, state)
}

describe('beginTurnInConversation', () => {
  it('appends the user message and opens the streaming turn', () => {
    const s = beginTurnInConversation(emptyConversation(), 'hello')
    expect(s.parts).toMatchObject([{ kind: 'text', role: 'user', text: 'hello' }])
    expect(s.streaming).toBe(true)
    expect(s.failure).toBeNull()
    expect(s.interrupted).toBe(false)
  })

  it('clears the previous turn failure and stale pending approval', () => {
    const before: ConversationState = {
      ...emptyConversation(),
      failure: { kind: 'transport', message: 'boom' },
      pendingApproval: { elicitationId: 'e1', params: {} },
    }
    const s = beginTurnInConversation(before, 'again')
    expect(s.failure).toBeNull()
    expect(s.pendingApproval).toBeNull()
  })
})

describe('applyFrameToConversation — delta coalescing', () => {
  it('coalesces consecutive text deltas into one assistant part', () => {
    const s = apply(
      beginTurnInConversation(emptyConversation(), 'hi'),
      textDelta('Hel'),
      textDelta('lo'),
    )
    expect(s.parts).toHaveLength(2)
    expect(s.parts[1]).toMatchObject({ kind: 'text', role: 'assistant', text: 'Hello' })
  })

  it('never coalesces an assistant delta into the user message', () => {
    const s = apply(beginTurnInConversation(emptyConversation(), 'hi'), textDelta('yo'))
    expect(s.parts[0]).toMatchObject({ kind: 'text', role: 'user', text: 'hi' })
    expect(s.parts[1]).toMatchObject({ kind: 'text', role: 'assistant', text: 'yo' })
  })

  it('starts new parts across text/reasoning boundaries, preserving order', () => {
    const s = apply(
      beginTurnInConversation(emptyConversation(), 'hi'),
      textDelta('a'),
      textDelta('b'),
      reasoningDelta('think'),
      reasoningDelta('ing'),
      textDelta('c'),
    )
    expect(s.parts.map((p) => p.kind)).toEqual(['text', 'text', 'reasoning', 'text'])
    expect(s.parts[1]).toMatchObject({ kind: 'text', text: 'ab' })
    expect(s.parts[2]).toMatchObject({ kind: 'reasoning', text: 'thinking' })
    expect(s.parts[3]).toMatchObject({ kind: 'text', text: 'c' })
  })

  it('an item.done between text deltas splits the text into two parts', () => {
    const s = apply(
      beginTurnInConversation(emptyConversation(), 'hi'),
      textDelta('before'),
      { type: 'item.done', item: { id: 'fc1', type: 'function_call' } },
      textDelta('after'),
    )
    expect(s.parts.map((p) => p.kind)).toEqual(['text', 'text', 'item', 'text'])
    expect(s.parts[2]).toMatchObject({ kind: 'item', item: { id: 'fc1' } })
  })

  it('mints per-conversation part ids deterministically', () => {
    const build = () =>
      apply(
        beginTurnInConversation(emptyConversation(), 'hi'),
        textDelta('a'),
        reasoningDelta('b'),
      )
    const first = build()
    const second = build()
    expect(first.parts.map((p) => p.id)).toEqual(second.parts.map((p) => p.id))
    expect(new Set(first.parts.map((p) => p.id)).size).toBe(first.parts.length)
  })
})

describe('applyFrameToConversation — tool-call lifecycle', () => {
  const started = (id: string, name = 'Bash'): AssistantFrame => ({
    type: 'item.started',
    item: {
      id,
      type: 'tool_call',
      name,
      status: 'running',
      arguments: { command: 'ls' },
    },
  })
  const done = (id: string, name = 'Bash'): AssistantFrame => ({
    type: 'item.done',
    item: {
      id,
      type: 'tool_call',
      name,
      status: 'completed',
      arguments: '{"command":"ls"}',
      output_preview: 'ok',
    },
  })

  it('item.started appends a running tool part', () => {
    const s = apply(beginTurnInConversation(emptyConversation(), 'hi'), started('t1'))
    expect(s.parts[1]).toMatchObject({
      kind: 'item',
      item: { id: 't1', status: 'running', arguments: { command: 'ls' } },
    })
  })

  it('item.done reconciles the started part IN PLACE by item id', () => {
    const s = apply(
      beginTurnInConversation(emptyConversation(), 'hi'),
      started('t1'),
      textDelta('reading…'),
      done('t1'),
    )
    // Position preserved: the tool part stays BEFORE the text part.
    expect(s.parts.map((p) => p.kind)).toEqual(['text', 'item', 'text'])
    expect(s.parts[1]).toMatchObject({
      kind: 'item',
      item: { id: 't1', status: 'completed', output_preview: 'ok' },
    })
    const before = apply(
      beginTurnInConversation(emptyConversation(), 'hi'),
      started('t1'),
      textDelta('reading…'),
    )
    expect(s.parts[1].id).toBe(before.parts[1].id)
  })

  it('item.done without a matching started part appends (omnigent path)', () => {
    const s = apply(beginTurnInConversation(emptyConversation(), 'hi'), done('t9'))
    expect(s.parts).toHaveLength(2)
    expect(s.parts[1]).toMatchObject({ kind: 'item', item: { id: 't9' } })
  })

  it.each<AssistantFrame>([
    { type: 'turn.completed' },
    { type: 'turn.failed', reason: 'boom' },
    { type: 'interrupted' },
  ])('terminal frame %j flips running tool parts to incomplete', (frame) => {
    const s = apply(
      beginTurnInConversation(emptyConversation(), 'hi'),
      started('t1'),
      done('t1'),
      started('t2'),
      frame,
    )
    expect(s.parts[1]).toMatchObject({ item: { id: 't1', status: 'completed' } })
    expect(s.parts[2]).toMatchObject({ item: { id: 't2', status: 'incomplete' } })
  })

  it('failTransportInConversation flips running tool parts to incomplete', () => {
    const s = failTransportInConversation(
      apply(beginTurnInConversation(emptyConversation(), 'hi'), started('t1')),
      new Error('network down'),
    )
    expect(s.parts[1]).toMatchObject({ item: { id: 't1', status: 'incomplete' } })
  })

  it('finishStreamInConversation flips running tool parts to incomplete', () => {
    const s = finishStreamInConversation(
      apply(beginTurnInConversation(emptyConversation(), 'hi'), started('t1')),
    )
    expect(s.parts[1]).toMatchObject({ item: { id: 't1', status: 'incomplete' } })
  })
})

describe('applyFrameToConversation — approvals', () => {
  it('approval.request sets pendingApproval and appends an approval part', () => {
    const s = apply(emptyConversation(), {
      type: 'approval.request',
      elicitation_id: 'e1',
      params: { message: 'Run this?' },
    })
    expect(s.pendingApproval).toEqual({
      elicitationId: 'e1',
      params: { message: 'Run this?' },
    })
    expect(s.parts).toMatchObject([
      { kind: 'approval', elicitationId: 'e1', resolved: null },
    ])
  })

  it('resolveApprovalInConversation marks the part resolved and clears the pending state', () => {
    const pending = apply(emptyConversation(), {
      type: 'approval.request',
      elicitation_id: 'e1',
      params: {},
    })
    const s = resolveApprovalInConversation(pending, 'e1', 'accept')
    expect(s.pendingApproval).toBeNull()
    expect(s.parts[0]).toMatchObject({ kind: 'approval', resolved: 'accept' })
  })

  it('resolving an unknown elicitation leaves a different pending one intact', () => {
    const pending = apply(emptyConversation(), {
      type: 'approval.request',
      elicitation_id: 'e1',
      params: {},
    })
    const s = resolveApprovalInConversation(pending, 'other', 'decline')
    expect(s.pendingApproval).not.toBeNull()
  })
})

describe('applyFrameToConversation — session identity', () => {
  it('records the session id', () => {
    const s = apply(emptyConversation(), {
      type: 'session',
      session_id: 'conv_1',
      created: false,
    })
    expect(s.sessionMeta).toEqual({ sessionId: 'conv_1' })
  })

  it('created=true on the FIRST turn adds no divider', () => {
    const s = apply(beginTurnInConversation(emptyConversation(), 'hi'), {
      type: 'session',
      session_id: 'conv_1',
      created: true,
    })
    expect(s.parts.every((p) => p.kind !== 'divider')).toBe(true)
  })

  it('created=true mid-conversation inserts a divider before the new user message', () => {
    const firstTurn = apply(
      beginTurnInConversation(emptyConversation(), 'first'),
      textDelta('answer'),
      { type: 'turn.completed' },
    )
    const s = apply(beginTurnInConversation(firstTurn, 'second'), {
      type: 'session',
      session_id: 'conv_2',
      created: true,
    })
    expect(s.parts.map((p) => p.kind)).toEqual(['text', 'text', 'divider', 'text'])
    expect(s.parts[2]).toMatchObject({ kind: 'divider', label: 'new session started' })
    expect(s.parts[3]).toMatchObject({ kind: 'text', role: 'user', text: 'second' })
  })
})

describe('applyFrameToConversation — terminal frames', () => {
  it('turn.completed closes the streaming turn cleanly', () => {
    const s = apply(
      beginTurnInConversation(emptyConversation(), 'hi'),
      { type: 'status', state: 'running' },
      { type: 'turn.completed' },
    )
    expect(s.streaming).toBe(false)
    expect(s.statusState).toBeNull()
    expect(s.failure).toBeNull()
  })

  it('turn.failed records the reason as a failure', () => {
    const s = apply(beginTurnInConversation(emptyConversation(), 'hi'), {
      type: 'turn.failed',
      reason: 'model exploded',
    })
    expect(s.streaming).toBe(false)
    expect(s.failure).toEqual({ kind: 'turn_failed', reason: 'model exploded' })
  })

  it('interrupted marks the turn interrupted (not failed)', () => {
    const s = apply(beginTurnInConversation(emptyConversation(), 'hi'), {
      type: 'interrupted',
    })
    expect(s.streaming).toBe(false)
    expect(s.interrupted).toBe(true)
    expect(s.failure).toBeNull()
  })

  it('a terminal error frame becomes a stream_error failure', () => {
    const s = apply(beginTurnInConversation(emptyConversation(), 'hi'), errorFrame)
    expect(s.streaming).toBe(false)
    expect(s.failure).toEqual({ kind: 'stream_error', frame: errorFrame })
  })

  it.each<SessionFailedHint>(['omnigent_setup', 'databricks_auth', 'unknown'])(
    'session.failed carries the %s hint into the failure state',
    (hint) => {
      const s = apply(beginTurnInConversation(emptyConversation(), 'hi'), {
        type: 'session.failed',
        detail: 'runner_error: boom',
        hint,
      })
      expect(s.streaming).toBe(false)
      expect(s.failure).toEqual({
        kind: 'session_failed',
        detail: 'runner_error: boom',
        hint,
      })
    },
  )

  it('heartbeat frames change nothing', () => {
    const before = beginTurnInConversation(emptyConversation(), 'hi')
    const s = applyFrameToConversation(before, { type: 'heartbeat' })
    expect(s).toBe(before)
    expect(s.parts).toBe(before.parts)
    expect(s.streaming).toBe(true)
  })
})

describe('finishStreamInConversation', () => {
  it('closes the streaming turn and clears the status', () => {
    const streaming = apply(beginTurnInConversation(emptyConversation(), 'hi'), {
      type: 'status',
      state: 'running',
    })
    const s = finishStreamInConversation(streaming)
    expect(s.streaming).toBe(false)
    expect(s.statusState).toBeNull()
  })
})

describe('failTransportInConversation', () => {
  it('maps a chat-gate 409 (LHP-WEB-002) to a gate failure', () => {
    const s = failTransportInConversation(
      beginTurnInConversation(emptyConversation(), 'hi'),
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
    expect(s.streaming).toBe(false)
    expect(s.failure).toEqual({
      kind: 'gate',
      code: 'LHP-WEB-002',
      message: 'LHP skill is not installed in this project',
    })
  })

  it('maps a plain transport error to a transport failure', () => {
    const s = failTransportInConversation(
      beginTurnInConversation(emptyConversation(), 'hi'),
      new Error('network down'),
    )
    expect(s.failure).toEqual({ kind: 'transport', message: 'network down' })
  })
})

describe('snapshot normalization + conversationFromSnapshot', () => {
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

  it('maps enveloped items onto the shared part shapes', () => {
    const s = conversationFromSnapshot({
      session_id: 'conv_9',
      title: null,
      status: 'idle',
      resumable: true,
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
    expect(s.sessionMeta).toEqual({ sessionId: 'conv_9' })
    expect(s.parts).toMatchObject([
      { kind: 'text', role: 'user', text: 'generate the pipeline' },
      { kind: 'reasoning', text: 'planning' },
      { kind: 'item', item: { id: 'fc_1', type: 'function_call', name: 'Bash' } },
      { kind: 'text', role: 'assistant', text: 'Done.' },
    ])
  })
})

describe('usage folding on terminal frames', () => {
  const turnUsage = {
    input_tokens: 100,
    output_tokens: 50,
    cache_read_input_tokens: 1000,
    cache_creation_input_tokens: 200,
  }
  const sessionTotals = {
    input_tokens: 5000,
    output_tokens: 2500,
    cache_read_input_tokens: 90000,
    cache_creation_input_tokens: 1000,
    sdk_cost_usd: 0.42,
    configured_cost_usd: null,
  }

  it('prefers session_totals from the frame over accumulation', () => {
    const before = {
      ...emptyConversation(),
      usage: { ...sessionTotals, input_tokens: 1 },
    }
    const s = apply(before, {
      type: 'turn.completed',
      usage: turnUsage,
      session_totals: sessionTotals,
    })
    expect(s.usage).toEqual(sessionTotals)
  })

  it('accumulates the frame usage when session_totals is absent', () => {
    const first = apply(emptyConversation(), {
      type: 'turn.completed',
      usage: turnUsage,
      total_cost_usd: 0.1,
    })
    expect(first.usage).toEqual({
      input_tokens: 100,
      output_tokens: 50,
      cache_read_input_tokens: 1000,
      cache_creation_input_tokens: 200,
      sdk_cost_usd: 0.1,
      configured_cost_usd: null,
    })
    const second = apply(first, {
      type: 'turn.completed',
      usage: turnUsage,
      total_cost_usd: 0.2,
      configured_cost_usd: 0.05,
    })
    expect(second.usage).toEqual({
      input_tokens: 200,
      output_tokens: 100,
      cache_read_input_tokens: 2000,
      cache_creation_input_tokens: 400,
      sdk_cost_usd: expect.closeTo(0.3, 10),
      configured_cost_usd: 0.05,
    })
  })

  it('folds usage on turn.failed and interrupted terminals too', () => {
    const failed = apply(emptyConversation(), {
      type: 'turn.failed',
      reason: 'boom',
      usage: turnUsage,
    })
    expect(failed.usage?.input_tokens).toBe(100)
    const interrupted = apply(emptyConversation(), {
      type: 'interrupted',
      usage: turnUsage,
    })
    expect(interrupted.usage?.input_tokens).toBe(100)
  })

  it('bare terminals (omnigent) leave usage untouched', () => {
    const s = apply(emptyConversation(), { type: 'turn.completed' })
    expect(s.usage).toBeNull()
    const withUsage = {
      ...emptyConversation(),
      usage: sessionTotals,
    }
    expect(apply(withUsage, { type: 'turn.completed' }).usage).toEqual(
      sessionTotals,
    )
  })

  it('conversationFromSnapshot seeds usage from usage_totals', () => {
    const s = conversationFromSnapshot({
      session_id: 'claude_1',
      status: 'active',
      resumable: true,
      items: [],
      usage_totals: sessionTotals,
    })
    expect(s.usage).toEqual(sessionTotals)
    const bare = conversationFromSnapshot({
      session_id: 'conv_1',
      status: 'active',
      resumable: true,
      items: [],
    })
    expect(bare.usage).toBeNull()
  })
})
