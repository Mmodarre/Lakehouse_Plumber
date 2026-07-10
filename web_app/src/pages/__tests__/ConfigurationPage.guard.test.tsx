import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { createMemoryRouter, Link, RouterProvider } from 'react-router-dom'
import { NavigationGuard } from '../../components/layout/NavigationGuard'
import { ConfigurationPage } from '../ConfigurationPage'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

// ── Unsaved-changes guard (Task-6 review carry-over) ─────────
//
// ConfigurationPage unmounts tab state on section switch, so leaving a
// dirty form without confirmation would silently discard edits. Guarded
// three ways: tab switches prompt via onValueChange interception,
// route-level navigation prompts via the app-level NavigationGuard (the
// page contributes a dirty-guard source; the harness mounts the guard the
// way Layout does), and hard refresh/close raises the native beforeunload
// prompt.

const fetchMock =
  vi.fn<(url: string | URL | Request, init?: RequestInit) => Promise<Response>>()

function serve() {
  fetchMock.mockImplementation((url, init) => {
    const u = String(url)
    const method = init?.method ?? 'GET'
    if (method === 'GET' && u === '/api/files/lhp.yaml') {
      return Promise.resolve(
        new Response('name: acme\n', { status: 200, headers: { ETag: '"e1"' } }),
      )
    }
    if (method === 'GET' && u === '/api/files') {
      return Promise.resolve(
        new Response(JSON.stringify({ name: '', path: '', type: 'directory', children: [] }), {
          status: 200,
        }),
      )
    }
    return Promise.reject(new Error(`unexpected ${method} ${u}`))
  })
}

async function renderPage() {
  const router = createMemoryRouter(
    [
      {
        path: '/config/:section?',
        element: (
          <>
            <Link to="/other">leave config</Link>
            <Link to="/config/pipeline">jump to pipelines</Link>
            <NavigationGuard />
            <ConfigurationPage />
          </>
        ),
      },
      { path: '/other', element: <div>other page</div> },
    ],
    { initialEntries: ['/config/project'] },
  )
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  render(<RouterProvider router={router} />, { wrapper })
  await screen.findByLabelText('Name')
  return router
}

async function makeDirty(user: ReturnType<typeof userEvent.setup>) {
  const author = screen.getByLabelText('Author')
  await user.type(author, 'someone')
  await user.tab()
  await screen.findByText('Unsaved changes')
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  serve()
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('ConfigurationPage — unsaved-changes guard', () => {
  it('clean tab switch navigates without a prompt', async () => {
    await renderPage()
    const user = userEvent.setup()

    await user.click(screen.getByRole('tab', { name: 'Pipelines' }))
    expect(await screen.findByText('No config file selected')).toBeInTheDocument()
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument()
  })

  it('dirty tab switch prompts; cancel stays with the edits intact', async () => {
    await renderPage()
    const user = userEvent.setup()
    await makeDirty(user)

    await user.click(screen.getByRole('tab', { name: 'Pipelines' }))
    const dialog = await screen.findByRole('alertdialog')
    await user.click(screen.getByRole('button', { name: 'Keep editing' }))

    await waitFor(() => expect(dialog).not.toBeInTheDocument())
    expect(screen.getByLabelText('Author')).toHaveValue('someone')
    expect(screen.getByText('Unsaved changes')).toBeInTheDocument()
  })

  it('dirty tab switch prompts; confirm discards and switches', async () => {
    await renderPage()
    const user = userEvent.setup()
    await makeDirty(user)

    await user.click(screen.getByRole('tab', { name: 'Pipelines' }))
    await screen.findByRole('alertdialog')
    await user.click(screen.getByRole('button', { name: 'Discard changes' }))

    expect(await screen.findByText('No config file selected')).toBeInTheDocument()
    expect(screen.queryByText('Unsaved changes')).not.toBeInTheDocument()
  })

  it('dirty route-level navigation prompts; cancel stays, confirm leaves', async () => {
    await renderPage()
    const user = userEvent.setup()
    await makeDirty(user)

    await user.click(screen.getByRole('link', { name: 'leave config' }))
    await screen.findByRole('alertdialog')
    await user.click(screen.getByRole('button', { name: 'Keep editing' }))
    expect(screen.getByLabelText('Author')).toHaveValue('someone')

    await user.click(screen.getByRole('link', { name: 'leave config' }))
    await screen.findByRole('alertdialog')
    await user.click(screen.getByRole('button', { name: 'Discard changes' }))
    expect(await screen.findByText('other page')).toBeInTheDocument()
  })

  it('dirty navigation INTO another config section from OUTSIDE the tab strip prompts', async () => {
    // Guard invariant (Task-8 review): only a tab-switch confirmed by the
    // page's own dialog may bypass the route blocker. A /config-internal
    // navigation from anywhere else must still prompt — a blanket
    // "/config* is exempt" rule would silently discard the edits here.
    await renderPage()
    const user = userEvent.setup()
    await makeDirty(user)

    await user.click(screen.getByRole('link', { name: 'jump to pipelines' }))
    await screen.findByRole('alertdialog')
    await user.click(screen.getByRole('button', { name: 'Keep editing' }))
    expect(screen.getByLabelText('Author')).toHaveValue('someone')

    await user.click(screen.getByRole('link', { name: 'jump to pipelines' }))
    await screen.findByRole('alertdialog')
    await user.click(screen.getByRole('button', { name: 'Discard changes' }))
    expect(await screen.findByText('No config file selected')).toBeInTheDocument()
    expect(screen.queryByText('Unsaved changes')).not.toBeInTheDocument()
  })

  it('while dirty, closing/reloading the window is intercepted (beforeunload)', async () => {
    await renderPage()
    const user = userEvent.setup()

    // Clean: not intercepted.
    let event = new Event('beforeunload', { cancelable: true })
    window.dispatchEvent(event)
    expect(event.defaultPrevented).toBe(false)

    await makeDirty(user)
    event = new Event('beforeunload', { cancelable: true })
    window.dispatchEvent(event)
    expect(event.defaultPrevented).toBe(true)
  })
})
