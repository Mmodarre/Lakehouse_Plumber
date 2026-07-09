import { describe, expect, it } from 'vitest'
import { groupParts } from '../toolGrouping'
import type { MessagePart } from '@/store/assistantConversation'

let nextId = 1
function tool(status: string, name = 'Read'): MessagePart {
  const id = nextId++
  return {
    id,
    kind: 'item',
    item: { id: `t${id}`, type: 'tool_call', name, status },
  }
}
function text(): MessagePart {
  return { id: nextId++, kind: 'text', role: 'assistant', text: 'x' }
}

describe('groupParts', () => {
  it('below the threshold (2 completed) stays flat', () => {
    const parts = [tool('completed'), tool('completed')]
    expect(groupParts(parts, false).map((e) => e.kind)).toEqual([
      'single',
      'single',
    ])
  })

  it('3+ consecutive completed tool calls collapse into one group', () => {
    const parts = [text(), tool('completed'), tool('completed'), tool('completed')]
    const entries = groupParts(parts, false)
    expect(entries.map((e) => e.kind)).toEqual(['single', 'group'])
    expect(entries[1]).toMatchObject({ id: parts[1].id })
    expect(entries[1].kind === 'group' && entries[1].parts).toHaveLength(3)
  })

  it('the trailing run stays expanded while streaming', () => {
    const run = [tool('completed'), tool('completed'), tool('completed')]
    expect(groupParts(run, true).map((e) => e.kind)).toEqual([
      'single',
      'single',
      'single',
    ])
    // The same run collapses once the stream closes.
    expect(groupParts(run, false).map((e) => e.kind)).toEqual(['group'])
  })

  it('a non-trailing run collapses even while streaming', () => {
    const parts = [
      tool('completed'),
      tool('completed'),
      tool('completed'),
      text(),
      tool('running'),
    ]
    expect(groupParts(parts, true).map((e) => e.kind)).toEqual([
      'group',
      'single',
      'single',
    ])
  })

  it('a running part breaks a run', () => {
    const parts = [
      tool('completed'),
      tool('completed'),
      tool('running'),
      tool('completed'),
      tool('completed'),
    ]
    expect(groupParts(parts, false).every((e) => e.kind === 'single')).toBe(true)
  })

  it('incomplete and failed parts never collapse', () => {
    const parts = [
      tool('completed'),
      tool('incomplete'),
      tool('completed'),
      tool('failed'),
      tool('completed'),
    ]
    expect(groupParts(parts, false).every((e) => e.kind === 'single')).toBe(true)
  })
})
