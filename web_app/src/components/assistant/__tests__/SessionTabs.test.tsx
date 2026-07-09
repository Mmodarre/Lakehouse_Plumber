import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { SessionTabs } from '../SessionTabs'
import { archiveAssistantSession } from '../../../api/assistant'
import { useAssistantStore } from '../../../store/assistantStore'

vi.mock('../../../api/assistant', () => ({
  archiveAssistantSession: vi.fn().mockResolvedValue({ message: 'ok', details: null }),
}))

const archiveMock = vi.mocked(archiveAssistantSession)

function renderTabs() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  return render(<SessionTabs />, { wrapper })
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

describe('SessionTabs', () => {
  it('renders one tab per conversation with titles and active state', () => {
    store().syncTabsFromSessions([
      { session_id: 'claude_a', title: 'Bronze layer' },
      { session_id: 'claude_b', title: null },
    ])
    renderTabs()

    const tabs = screen.getAllByRole('tab')
    expect(tabs).toHaveLength(2)
    expect(tabs[0]).toHaveTextContent('Bronze layer')
    expect(tabs[1]).toHaveTextContent('New chat') // untitled fallback
    expect(tabs[0]).toHaveAttribute('aria-selected', 'true')
    expect(tabs[1]).toHaveAttribute('aria-selected', 'false')
  })

  it('falls back to the conversation first user message before the server title lands', () => {
    store().openTab()
    store().beginTurn('draft:1', 'refactor the silver flowgroup')
    renderTabs()

    expect(screen.getByRole('tab')).toHaveTextContent(
      'refactor the silver flowgroup',
    )
  })

  it('clicking a tab activates it', async () => {
    const user = userEvent.setup()
    store().syncTabsFromSessions([
      { session_id: 'claude_a', title: 'A' },
      { session_id: 'claude_b', title: 'B' },
    ])
    renderTabs()

    await user.click(screen.getByRole('tab', { name: /B/ }))
    expect(store().activeTabKey).toBe('claude_b')
  })

  it('shows the spinner dot only on streaming tabs', () => {
    store().syncTabsFromSessions([
      { session_id: 'claude_a', title: 'Busy' },
      { session_id: 'claude_b', title: 'Idle' },
    ])
    store().beginTurn('claude_a', 'go')
    renderTabs()

    expect(screen.getByRole('status', { name: 'Turn running' })).toBeInTheDocument()
    expect(screen.getAllByRole('status')).toHaveLength(1)
  })

  it('the + button opens a fresh draft tab (no API call)', async () => {
    const user = userEvent.setup()
    store().syncTabsFromSessions([{ session_id: 'claude_a', title: 'A' }])
    renderTabs()

    await user.click(screen.getByRole('button', { name: 'New chat tab' }))

    expect(store().tabOrder).toEqual(['claude_a', 'draft:1'])
    expect(store().activeTabKey).toBe('draft:1')
    expect(archiveMock).not.toHaveBeenCalled()
  })

  it('closing a real tab archives it server-side and removes it locally', async () => {
    const user = userEvent.setup()
    store().syncTabsFromSessions([
      { session_id: 'claude_a', title: 'Keep' },
      { session_id: 'claude_b', title: 'Close me' },
    ])
    renderTabs()

    await user.click(screen.getByRole('button', { name: 'Close Close me' }))

    expect(archiveMock).toHaveBeenCalledExactlyOnceWith('claude_b')
    expect(store().tabOrder).toEqual(['claude_a'])
  })

  it('closing a draft tab never calls the archive endpoint', async () => {
    const user = userEvent.setup()
    store().syncTabsFromSessions([{ session_id: 'claude_a', title: 'A' }])
    store().openTab()
    renderTabs()

    await user.click(screen.getByRole('button', { name: 'Close New chat' }))

    expect(archiveMock).not.toHaveBeenCalled()
    expect(store().tabOrder).toEqual(['claude_a'])
  })

  it('overflow tabs collapse into a dropdown; the active tab stays visible', () => {
    store().syncTabsFromSessions(
      ['a', 'b', 'c', 'd', 'e', 'f'].map((k) => ({
        session_id: `claude_${k}`,
        title: k.toUpperCase(),
      })),
    )
    store().activateTab('claude_f')
    renderTabs()

    const tabs = screen.getAllByRole('tab')
    expect(tabs).toHaveLength(4)
    // The active overflow tab was swapped into the last visible slot.
    expect(tabs[3]).toHaveTextContent('F')
    expect(tabs[3]).toHaveAttribute('aria-selected', 'true')
    expect(
      screen.getByRole('button', { name: '2 more tabs' }),
    ).toBeInTheDocument()
  })
})
