import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { createMemoryRouter, Link, RouterProvider } from 'react-router-dom'
import { NavigationGuard } from '../NavigationGuard'
import { ConfigurationPage } from '../../../pages/ConfigurationPage'
import { WorkspaceEditor } from '../../workspace/WorkspaceEditor'
import { useDirtyGuardStore } from '../../../store/dirtyGuardStore'
import { useWorkspaceStore } from '../../../store/workspaceStore'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), info: vi.fn(), dismiss: vi.fn() },
}))

// ── NavigationGuard — the ONE app-level unsaved-changes blocker ──
//
// Task-7 documented gap (now fixed): WorkspaceEditor used to register the
// app's single react-router useBlocker whenever buffers were open, so the
// config page's own route-leave guard had to unmount itself — a dirty
// config form + ANY open workspace buffer meant in-app navigation silently
// discarded form edits. Both surfaces now contribute to the dirty-guard
// registry and Layout mounts NavigationGuard once.

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

/** Route tree mirroring Layout: guard + workspace persistently mounted. */
async function renderApp() {
  const router = createMemoryRouter(
    [
      {
        path: '/config/:section?',
        element: (
          <>
            <Link to="/other">leave config</Link>
            <NavigationGuard />
            <WorkspaceEditor />
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

async function makeConfigDirty(user: ReturnType<typeof userEvent.setup>) {
  const author = screen.getByLabelText('Author')
  await user.type(author, 'someone')
  await user.tab()
  await screen.findByText('Unsaved changes')
}

/** Open a workspace buffer without focusing it (Monaco never mounts). */
function openBuffer({ dirty }: { dirty: boolean }) {
  const store = useWorkspaceStore.getState()
  store.openBuffer('pipelines/x.yaml', {
    content: 'a: 1\n',
    exists: true,
    activate: false,
  })
  if (dirty) store.updateContent('pipelines/x.yaml', 'a: 2\n')
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  serve()
})

afterEach(() => {
  vi.unstubAllGlobals()
  useWorkspaceStore.getState().closeAllBuffers()
  useDirtyGuardStore.setState({ sources: {} })
})

describe('NavigationGuard — unified unsaved-changes blocking', () => {
  it('REGRESSION: dirty config form + open (clean) buffer → route nav prompts', async () => {
    openBuffer({ dirty: false })
    await renderApp()
    const user = userEvent.setup()
    await makeConfigDirty(user)

    // Previously: WorkspaceEditor's blocker registration forced the config
    // guard to unmount whenever ANY buffer was open — this nav silently
    // discarded the form edits.
    await user.click(screen.getByRole('link', { name: 'leave config' }))
    const dialog = await screen.findByRole('alertdialog')
    expect(dialog).toHaveTextContent('The configuration form has unsaved changes')

    await user.click(screen.getByRole('button', { name: 'Keep editing' }))
    await waitFor(() => expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument())
    expect(screen.getByLabelText('Author')).toHaveValue('someone')

    await user.click(screen.getByRole('link', { name: 'leave config' }))
    await screen.findByRole('alertdialog')
    await user.click(screen.getByRole('button', { name: 'Discard changes' }))
    expect(await screen.findByText('other page')).toBeInTheDocument()
  })

  it('dirty buffer + dirty config form → ONE prompt covering both; discard clears both', async () => {
    openBuffer({ dirty: true })
    await renderApp()
    const user = userEvent.setup()
    await makeConfigDirty(user)

    await user.click(screen.getByRole('link', { name: 'leave config' }))
    const dialog = await screen.findByRole('alertdialog')
    expect(dialog).toHaveTextContent('Unsaved changes in 1 file(s) will be lost.')
    expect(dialog).toHaveTextContent('The configuration form has unsaved changes')

    await user.click(screen.getByRole('button', { name: 'Discard changes' }))
    expect(await screen.findByText('other page')).toBeInTheDocument()
    expect(useWorkspaceStore.getState().buffers[0]?.isDirty).toBe(false)
  })

  it('dirty buffer alone still prompts (WorkspaceEditor contribution)', async () => {
    openBuffer({ dirty: true })
    await renderApp()
    const user = userEvent.setup()

    await user.click(screen.getByRole('link', { name: 'leave config' }))
    const dialog = await screen.findByRole('alertdialog')
    expect(dialog).toHaveTextContent('Unsaved changes in 1 file(s) will be lost.')
    await user.click(screen.getByRole('button', { name: 'Discard changes' }))
    expect(await screen.findByText('other page')).toBeInTheDocument()
  })

  it('nothing dirty → navigation proceeds without a prompt', async () => {
    openBuffer({ dirty: false })
    await renderApp()
    const user = userEvent.setup()

    await user.click(screen.getByRole('link', { name: 'leave config' }))
    expect(await screen.findByText('other page')).toBeInTheDocument()
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument()
  })
})
