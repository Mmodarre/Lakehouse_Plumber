import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { useMemo, useState } from 'react'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { createMemoryRouter, Link, RouterProvider } from 'react-router-dom'
import { NavigationGuard } from '../NavigationGuard'
import {
  useDirtyGuardSource,
  useDirtyGuardStore,
} from '../../../store/dirtyGuardStore'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), info: vi.fn(), dismiss: vi.fn() },
}))

// ── NavigationGuard — the ONE app-level unsaved-changes blocker ──
//
// The data router supports a single useBlocker; every surface with
// discardable unsaved state contributes a dirtyGuardStore source instead of
// registering its own blocker, and the shell mounts <NavigationGuard /> once.
// It prompts over ALL sources that block the attempted navigation and runs
// each blocking source's onDiscard before proceeding. These cases drive that
// contract directly through synthetic sources (the concrete surfaces — config
// form, workspace buffers — are exercised by their own suites).

const CONFIG_MSG = 'The configuration form has unsaved changes'
const BUFFER_MSG = 'Unsaved changes in 1 file(s) will be lost.'

/** A dirty-guard source stand-in: registers while dirty (blocks every nav),
 * clears its own dirty flag on discard, and reports discards to a spy. */
function DirtySource({
  id,
  message,
  initialDirty,
  onDiscardSpy,
}: {
  id: string
  message: string
  initialDirty: boolean
  onDiscardSpy?: () => void
}) {
  const [dirty, setDirty] = useState(initialDirty)
  const source = useMemo(
    () =>
      dirty
        ? {
            message,
            onDiscard: () => {
              onDiscardSpy?.()
              setDirty(false)
            },
          }
        : null,
    [dirty, message, onDiscardSpy],
  )
  useDirtyGuardSource(id, source)
  return null
}

function renderApp(sources: {
  config?: boolean
  buffer?: boolean
  configSpy?: () => void
  bufferSpy?: () => void
}) {
  const router = createMemoryRouter(
    [
      {
        path: '/config',
        element: (
          <>
            <Link to="/other">leave config</Link>
            <NavigationGuard />
            {sources.config !== undefined && (
              <DirtySource
                id="config-form"
                message={CONFIG_MSG}
                initialDirty={sources.config}
                onDiscardSpy={sources.configSpy}
              />
            )}
            {sources.buffer !== undefined && (
              <DirtySource
                id="workspace"
                message={BUFFER_MSG}
                initialDirty={sources.buffer}
                onDiscardSpy={sources.bufferSpy}
              />
            )}
          </>
        ),
      },
      { path: '/other', element: <div>other page</div> },
    ],
    { initialEntries: ['/config'] },
  )
  render(<RouterProvider router={router} />)
  return router
}

beforeEach(() => {
  vi.clearAllMocks()
})

afterEach(() => {
  useDirtyGuardStore.setState({ sources: {} })
})

describe('NavigationGuard — unified unsaved-changes blocking', () => {
  it('dirty config source alone → route nav prompts; keep-editing preserves it', async () => {
    renderApp({ config: true, buffer: false })
    const user = userEvent.setup()

    await user.click(screen.getByRole('link', { name: 'leave config' }))
    const dialog = await screen.findByRole('alertdialog')
    expect(dialog).toHaveTextContent(CONFIG_MSG)

    await user.click(screen.getByRole('button', { name: 'Keep editing' }))
    await waitFor(() => expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument())

    // Source stayed registered — a second attempt re-prompts, then discards.
    await user.click(screen.getByRole('link', { name: 'leave config' }))
    await screen.findByRole('alertdialog')
    await user.click(screen.getByRole('button', { name: 'Discard changes' }))
    expect(await screen.findByText('other page')).toBeInTheDocument()
  })

  it('dirty buffer + dirty config → ONE prompt covering both; discard clears both', async () => {
    const configSpy = vi.fn()
    const bufferSpy = vi.fn()
    renderApp({ config: true, buffer: true, configSpy, bufferSpy })
    const user = userEvent.setup()

    await user.click(screen.getByRole('link', { name: 'leave config' }))
    const dialog = await screen.findByRole('alertdialog')
    expect(dialog).toHaveTextContent(BUFFER_MSG)
    expect(dialog).toHaveTextContent(CONFIG_MSG)

    await user.click(screen.getByRole('button', { name: 'Discard changes' }))
    expect(await screen.findByText('other page')).toBeInTheDocument()
    expect(configSpy).toHaveBeenCalledTimes(1)
    expect(bufferSpy).toHaveBeenCalledTimes(1)
  })

  it('dirty buffer alone still prompts', async () => {
    renderApp({ buffer: true })
    const user = userEvent.setup()

    await user.click(screen.getByRole('link', { name: 'leave config' }))
    const dialog = await screen.findByRole('alertdialog')
    expect(dialog).toHaveTextContent(BUFFER_MSG)
    await user.click(screen.getByRole('button', { name: 'Discard changes' }))
    expect(await screen.findByText('other page')).toBeInTheDocument()
  })

  it('nothing dirty → navigation proceeds without a prompt', async () => {
    renderApp({ config: false, buffer: false })
    const user = userEvent.setup()

    await user.click(screen.getByRole('link', { name: 'leave config' }))
    expect(await screen.findByText('other page')).toBeInTheDocument()
    expect(screen.queryByRole('alertdialog')).not.toBeInTheDocument()
  })
})
