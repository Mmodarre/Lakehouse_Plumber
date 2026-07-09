import { beforeEach, describe, expect, it } from 'vitest'
import { renderHook } from '@testing-library/react'
import {
  RESUME_LOST_HINT,
  activeSessionId,
  useActiveConversation,
  useAssistantStore,
} from '@/store/assistantStore'

// Frame→state behavior is covered by the pure-reducer tests in
// assistantConversation.test.ts; these tests cover the multi-tab container
// — tab lifecycle, per-tab action routing, and persistence.

function resetStore() {
  useAssistantStore.setState({
    conversations: {},
    tabOrder: [],
    activeTabKey: null,
    tabTitles: {},
    nextDraftId: 1,
    panelOpen: false,
    panelWidth: 360,
    permissionMode: 'default',
  })
}

beforeEach(() => {
  resetStore()
})

const store = () => useAssistantStore.getState()

describe('assistantStore — tab lifecycle', () => {
  it('openTab mints draft keys, appends in order, and activates', () => {
    const first = store().openTab()
    const second = store().openTab()
    expect(first).toBe('draft:1')
    expect(second).toBe('draft:2')
    const s = store()
    expect(s.tabOrder).toEqual(['draft:1', 'draft:2'])
    expect(s.activeTabKey).toBe('draft:2')
    expect(s.conversations['draft:1'].parts).toEqual([])
  })

  it('rekeyTab moves the conversation to the session id, preserving position', () => {
    store().openTab()
    store().openTab()
    store().openTab()
    store().beginTurn('draft:2', 'hello')
    store().activateTab('draft:2')

    store().rekeyTab('draft:2', 'claude_abc')

    const s = store()
    expect(s.tabOrder).toEqual(['draft:1', 'claude_abc', 'draft:3'])
    expect(s.activeTabKey).toBe('claude_abc')
    expect(s.conversations['draft:2']).toBeUndefined()
    expect(s.conversations['claude_abc'].parts).toMatchObject([
      { kind: 'text', role: 'user', text: 'hello' },
    ])
  })

  it('rekeyTab keeps a background tab active elsewhere untouched', () => {
    store().openTab()
    store().openTab() // active: draft:2
    store().rekeyTab('draft:1', 'claude_bg')
    const s = store()
    expect(s.activeTabKey).toBe('draft:2')
    expect(s.tabOrder).toEqual(['claude_bg', 'draft:2'])
  })

  it('closeTab removes the tab and activates a neighbor', () => {
    store().openSessionTab('claude_a')
    store().openSessionTab('claude_b')
    store().openSessionTab('claude_c')
    store().activateTab('claude_b')

    store().closeTab('claude_b')

    const s = store()
    expect(s.tabOrder).toEqual(['claude_a', 'claude_c'])
    expect(s.activeTabKey).toBe('claude_c')
    expect(s.conversations['claude_b']).toBeUndefined()
  })

  it('closing the last tab auto-opens a fresh draft', () => {
    store().openSessionTab('claude_a')
    store().closeTab('claude_a')
    const s = store()
    expect(s.tabOrder).toEqual(['draft:1'])
    expect(s.activeTabKey).toBe('draft:1')
  })

  it('openSessionTab focuses an already-open tab instead of duplicating', () => {
    store().openSessionTab('claude_a', 'First')
    store().openTab()
    store().openSessionTab('claude_a', 'Renamed')
    const s = store()
    expect(s.tabOrder).toEqual(['claude_a', 'draft:1'])
    expect(s.activeTabKey).toBe('claude_a')
    expect(s.tabTitles['claude_a']).toBe('Renamed')
  })

  it('syncTabsFromSessions appends missing sessions, refreshes titles, never removes', () => {
    store().openTab() // an existing draft stays put
    store().syncTabsFromSessions([
      { session_id: 'claude_new', title: 'Bronze work' },
      { session_id: 'claude_old', title: null },
    ])
    const s = store()
    expect(s.tabOrder).toEqual(['draft:1', 'claude_new', 'claude_old'])
    expect(s.tabTitles['claude_new']).toBe('Bronze work')
    expect(s.activeTabKey).toBe('draft:1')

    // Re-sync is idempotent for known sessions.
    store().syncTabsFromSessions([{ session_id: 'claude_new', title: 'Renamed' }])
    expect(store().tabOrder).toEqual(['draft:1', 'claude_new', 'claude_old'])
    expect(store().tabTitles['claude_new']).toBe('Renamed')
  })

  it('syncTabsFromSessions with no sessions opens a draft so the panel has a target', () => {
    store().syncTabsFromSessions([])
    const s = store()
    expect(s.tabOrder).toEqual(['draft:1'])
    expect(s.activeTabKey).toBe('draft:1')
  })
})

describe('assistantStore — per-tab conversation routing', () => {
  it('frames land on their tab only; two conversations stream independently', () => {
    store().openSessionTab('claude_a')
    store().openSessionTab('claude_b')
    store().beginTurn('claude_a', 'one')
    store().beginTurn('claude_b', 'two')

    store().applyFrame('claude_a', { type: 'text.delta', delta: 'Alpha' })
    store().applyFrame('claude_b', { type: 'text.delta', delta: 'Beta' })
    store().applyFrame('claude_a', { type: 'turn.completed' })

    const s = store()
    expect(s.conversations['claude_a'].parts).toMatchObject([
      { kind: 'text', role: 'user', text: 'one' },
      { kind: 'text', role: 'assistant', text: 'Alpha' },
    ])
    expect(s.conversations['claude_a'].streaming).toBe(false)
    expect(s.conversations['claude_b'].parts).toMatchObject([
      { kind: 'text', role: 'user', text: 'two' },
      { kind: 'text', role: 'assistant', text: 'Beta' },
    ])
    expect(s.conversations['claude_b'].streaming).toBe(true)
  })

  it('actions on an unknown tab key are safe no-ops', () => {
    const before = store()
    store().applyFrame('ghost', { type: 'text.delta', delta: 'x' })
    store().beginTurn('ghost', 'x')
    store().finishStream('ghost')
    store().failTransport('ghost', new Error('x'))
    expect(store().conversations).toBe(before.conversations)
  })

  it('failTransport and resolveApprovalLocal target their tab', () => {
    store().openSessionTab('claude_a')
    store().openSessionTab('claude_b')
    store().beginTurn('claude_a', 'hi')
    store().failTransport('claude_a', new Error('network down'))
    store().applyFrame('claude_b', {
      type: 'approval.request',
      elicitation_id: 'e1',
      params: {},
    })
    store().resolveApprovalLocal('claude_b', 'e1', 'accept')

    const s = store()
    expect(s.conversations['claude_a'].failure).toEqual({
      kind: 'transport',
      message: 'network down',
    })
    expect(s.conversations['claude_b'].failure).toBeNull()
    expect(s.conversations['claude_b'].parts[0]).toMatchObject({
      kind: 'approval',
      resolved: 'accept',
    })
  })

  it('hydrateFromSnapshot fills an empty tab and skips a busy one', () => {
    store().openSessionTab('claude_a')
    store().hydrateFromSnapshot('claude_a', {
      session_id: 'claude_a',
      title: 'My chat',
      status: 'active',
      resumable: true,
      items: [
        {
          id: 'msg_1',
          type: 'message',
          data: { role: 'user', content: 'generate the pipeline' },
        },
      ],
    })
    let s = store()
    expect(s.conversations['claude_a'].parts).toMatchObject([
      { kind: 'text', role: 'user', text: 'generate the pipeline' },
    ])
    expect(s.tabTitles['claude_a']).toBe('My chat')

    // A tab that already has parts is never clobbered.
    store().hydrateFromSnapshot('claude_a', {
      session_id: 'claude_a',
      title: null,
      status: 'active',
      resumable: true,
      items: [],
    })
    s = store()
    expect(s.conversations['claude_a'].parts).toHaveLength(1)
  })

  it('hydrateFromSnapshot appends the resume-lost hint when not resumable', () => {
    store().openSessionTab('claude_a')
    store().hydrateFromSnapshot('claude_a', {
      session_id: 'claude_a',
      title: null,
      status: 'archived',
      resumable: false,
      items: [
        { id: 'm1', type: 'message', data: { role: 'user', content: 'hello' } },
      ],
    })
    const parts = store().conversations['claude_a'].parts
    expect(parts[parts.length - 1]).toMatchObject({
      kind: 'divider',
      label: RESUME_LOST_HINT,
    })
  })

  it('hydrateFromSnapshot renders no hint for an EMPTY non-resumable session', () => {
    store().openSessionTab('claude_a')
    store().hydrateFromSnapshot('claude_a', {
      session_id: 'claude_a',
      title: null,
      status: 'archived',
      resumable: false,
      items: [],
    })
    expect(store().conversations['claude_a'].parts).toEqual([])
  })
})

describe('assistantStore — active-conversation selectors', () => {
  it('useActiveConversation tracks the active tab (stable empty before any tab)', () => {
    const { result, rerender } = renderHook(() => useActiveConversation())
    expect(result.current.parts).toEqual([])
    const emptyBefore = result.current

    store().openSessionTab('claude_a')
    store().beginTurn('claude_a', 'hi')
    rerender()
    expect(result.current.parts).toMatchObject([{ kind: 'text', text: 'hi' }])

    store().openTab()
    rerender()
    expect(result.current.parts).toEqual([])
    expect(result.current).not.toBe(emptyBefore) // draft has its own state
  })

  it('activeSessionId prefers the bound session, falls back to a real key, undefined for drafts', () => {
    expect(activeSessionId()).toBeUndefined()

    store().openTab()
    expect(activeSessionId()).toBeUndefined() // draft, nothing bound yet

    store().applyFrame('draft:1', {
      type: 'session',
      session_id: 'claude_bound',
      created: true,
    })
    expect(activeSessionId()).toBe('claude_bound') // bound via session frame

    store().openSessionTab('claude_real')
    expect(activeSessionId()).toBe('claude_real') // key IS the session id
  })
})

describe('assistantStore persistence', () => {
  it('persists ONLY UI preferences — never tabs or conversations', () => {
    store().openTab()
    store().beginTurn('draft:1', 'secret conversation')
    store().setPanelOpen(true)
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
    store().setPanelWidth(10)
    expect(store().panelWidth).toBe(300)
    store().setPanelWidth(5000)
    expect(store().panelWidth).toBe(760)
  })
})
