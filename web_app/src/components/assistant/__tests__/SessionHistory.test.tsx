import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { SessionHistory } from '../SessionHistory'
import { fetchAssistantSessions } from '../../../api/assistant'
import { relativeTime } from '../../../lib/utils'
import {
  RESUME_LOST_HINT,
  useAssistantStore,
} from '../../../store/assistantStore'
import type { SessionListItem } from '../../../types/assistant'

vi.mock('../../../api/assistant', () => ({
  fetchAssistantSessions: vi.fn(),
}))

const sessionsMock = vi.mocked(fetchAssistantSessions)

function sessionOf(over: Partial<SessionListItem>): SessionListItem {
  return {
    session_id: 'claude_x',
    title: null,
    status: 'archived',
    created_at: '2026-07-01T00:00:00+00:00',
    last_used_at: new Date(Date.now() - 5 * 60_000).toISOString(),
    provider: 'claude_sdk',
    usage_totals: null,
    ...over,
  }
}

function renderHistory() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  return render(<SessionHistory />, { wrapper })
}

function resetStore() {
  useAssistantStore.setState({
    conversations: {},
    tabOrder: [],
    activeTabKey: null,
    tabTitles: {},
    nextDraftId: 1,
  })
}

const store = () => useAssistantStore.getState()

beforeEach(() => {
  vi.clearAllMocks()
  resetStore()
})

describe('SessionHistory', () => {
  it('lists ONLY archived claude sessions, with title, time, and usage', async () => {
    const user = userEvent.setup()
    sessionsMock.mockResolvedValue({
      sessions: [
        sessionOf({
          session_id: 'claude_old',
          title: 'Fix the gold pipeline',
          usage_totals: {
            input_tokens: 1200,
            output_tokens: 300,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            sdk_cost_usd: null,
            configured_cost_usd: null,
          },
        }),
        sessionOf({ session_id: 'claude_open', status: 'active', title: 'Open tab' }),
        sessionOf({ session_id: 'claude_bad', status: 'stale', title: 'Stale' }),
        sessionOf({ session_id: 'conv_1', provider: 'omnigent', title: 'Omni' }),
      ],
      total: 4,
    })
    renderHistory()

    await user.click(screen.getByRole('button', { name: 'Chat history' }))

    expect(
      await screen.findByText('Fix the gold pipeline'),
    ).toBeInTheDocument()
    expect(screen.queryByText('Open tab')).not.toBeInTheDocument()
    expect(screen.queryByText('Stale')).not.toBeInTheDocument()
    expect(screen.queryByText('Omni')).not.toBeInTheDocument()
    expect(screen.getByText(/5m ago · 1\.5k tokens/)).toBeInTheDocument()
  })

  it('reopening an archived session opens a tab keyed by its id', async () => {
    const user = userEvent.setup()
    sessionsMock.mockResolvedValue({
      sessions: [sessionOf({ session_id: 'claude_old', title: 'Old work' })],
      total: 1,
    })
    renderHistory()

    await user.click(screen.getByRole('button', { name: 'Chat history' }))
    await user.click(await screen.findByText('Old work'))

    const s = store()
    expect(s.tabOrder).toEqual(['claude_old'])
    expect(s.activeTabKey).toBe('claude_old')
    expect(s.tabTitles['claude_old']).toBe('Old work')
  })

  it('shows the empty state when nothing is archived', async () => {
    const user = userEvent.setup()
    sessionsMock.mockResolvedValue({ sessions: [], total: 0 })
    renderHistory()

    await user.click(screen.getByRole('button', { name: 'Chat history' }))

    expect(
      await screen.findByText(/No archived chats yet/),
    ).toBeInTheDocument()
  })

  it('a reopened non-resumable snapshot renders the fresh-context hint', () => {
    // The hint is hydration behavior (store-level); the history popover is
    // the flow that reaches it, so it is asserted here end-to-end-ish.
    store().openSessionTab('claude_old', 'Old work')
    store().hydrateFromSnapshot('claude_old', {
      session_id: 'claude_old',
      title: 'Old work',
      status: 'archived',
      resumable: false,
      items: [
        { id: 'm1', type: 'message', data: { role: 'user', content: 'hello' } },
      ],
    })
    const parts = store().conversations['claude_old'].parts
    expect(parts[parts.length - 1]).toMatchObject({
      kind: 'divider',
      label: RESUME_LOST_HINT,
    })
  })
})

describe('relativeTime', () => {
  it('formats coarse buckets', () => {
    const now = Date.parse('2026-07-10T12:00:00Z')
    expect(relativeTime('2026-07-10T11:59:40Z', now)).toBe('just now')
    expect(relativeTime('2026-07-10T11:35:00Z', now)).toBe('25m ago')
    expect(relativeTime('2026-07-10T07:00:00Z', now)).toBe('5h ago')
    expect(relativeTime('2026-07-03T12:00:00Z', now)).toBe('7d ago')
    expect(relativeTime('2026-04-10T12:00:00Z', now)).toBe('3mo ago')
    expect(relativeTime('not-a-date', now)).toBe('')
  })
})
