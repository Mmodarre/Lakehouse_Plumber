import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { TooltipProvider } from '@/components/ui/tooltip'
import { SandboxControl } from '../SandboxControl'
import { useUIStore } from '../../../store/uiStore'
import { track } from '../../../lib/telemetry-shim'
import type { SandboxScope } from '../../../types/api'

// Usage telemetry reports only that the control was toggled or the picker
// opened — never the scope, the profile or the pipelines.
vi.mock('../../../lib/telemetry-shim', () => ({ track: vi.fn() }))
const trackMock = vi.mocked(track)

// Never resolves: keeps every query pending so the seeded cache alone
// controls what the control sees (mirrors RunConfigChip's test transport).
const pendingFetch = vi.fn(() => new Promise<Response>(() => {}))

function renderControl(scope: SandboxScope, enabled: boolean) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity } },
  })
  queryClient.setQueryData(['sandbox'], scope)
  queryClient.setQueryData(['pipelines'], { pipelines: [{ name: 'bronze' }] })
  useUIStore.setState({ sandboxEnabled: enabled })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider delayDuration={0}>{children}</TooltipProvider>
    </QueryClientProvider>
  )
  return render(<SandboxControl />, { wrapper })
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', pendingFetch)
  // The scope-picker's field-label (i) tooltips pull in Radix's Popper/useSize
  // machinery, which observes layout via ResizeObserver — an API jsdom lacks
  // (same stub SandboxPickerDialog's own test installs).
  vi.stubGlobal(
    'ResizeObserver',
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  )
  useUIStore.setState({ sandboxEnabled: false })
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('SandboxControl', () => {
  it('shows no scope pill when the toggle is off', () => {
    renderControl({ profile_exists: true, resolved_pipelines: ['bronze'] }, false)
    expect(screen.queryByText(/pipeline/)).not.toBeInTheDocument()
  })

  it('shows the resolved pipeline count when on', () => {
    renderControl({ profile_exists: true, resolved_pipelines: ['bronze', 'silver', 'gold'] }, true)
    expect(screen.getByText('3 pipelines')).toBeInTheDocument()
  })

  it('pluralizes a single-pipeline scope', () => {
    renderControl({ profile_exists: true, resolved_pipelines: ['bronze'] }, true)
    expect(screen.getByText('1 pipeline')).toBeInTheDocument()
  })

  it('surfaces a zero-match scope error in the pill', () => {
    renderControl(
      { profile_exists: true, resolved_pipelines: [], error: 'LHP-VAL-064: no pipelines matched' },
      true,
    )
    const pill = screen.getByText('Scope issue')
    expect(pill).toBeInTheDocument()
    expect(pill.closest('button')).toHaveAttribute('title', 'LHP-VAL-064: no pipelines matched')
  })

  it('opens the picker instead of enabling when no profile exists yet', async () => {
    renderControl({ profile_exists: false }, false)
    await userEvent.setup().click(screen.getByRole('switch'))
    // Not enabled — the developer must set up a profile first.
    expect(useUIStore.getState().sandboxEnabled).toBe(false)
    expect(screen.getByText('Sandbox scope')).toBeInTheDocument()
  })

  it('enables directly when a profile already exists', async () => {
    renderControl({ profile_exists: true, resolved_pipelines: ['bronze'] }, false)
    await userEvent.setup().click(screen.getByRole('switch'))
    expect(useUIStore.getState().sandboxEnabled).toBe(true)
  })

  it('turns a persisted toggle off when the resolved scope reports no profile', async () => {
    // sandboxEnabled survives reloads / project switches in localStorage, so a
    // stale `true` can outlive its profile. Once the scope view resolves with
    // no profile, the control must reconcile the toggle back off.
    renderControl({ profile_exists: false }, true)
    await waitFor(() => expect(useUIStore.getState().sandboxEnabled).toBe(false))
  })
})

describe('SandboxControl telemetry', () => {
  it('reports a toggle as sandbox_control.toggled', async () => {
    renderControl({ profile_exists: true, resolved_pipelines: ['bronze'] }, false)
    await userEvent.setup().click(screen.getByRole('switch'))
    expect(trackMock.mock.calls).toEqual([['sandbox_control', 'toggled']])
  })

  it('reports the pill opening the picker as sandbox_picker.opened', async () => {
    renderControl({ profile_exists: true, resolved_pipelines: ['bronze'] }, true)
    await userEvent.setup().click(screen.getByText('1 pipeline'))
    expect(screen.getByText('Sandbox scope')).toBeInTheDocument()
    expect(trackMock.mock.calls).toEqual([['sandbox_picker', 'opened']])
  })

  it('reports a profileless toggle-on as the picker opening, not as a toggle', async () => {
    renderControl({ profile_exists: false }, false)
    await userEvent.setup().click(screen.getByRole('switch'))
    expect(trackMock.mock.calls).toEqual([['sandbox_picker', 'opened']])
  })

  it('does not report the automatic reconciliation of a stale toggle', async () => {
    renderControl({ profile_exists: false }, true)
    await waitFor(() => expect(useUIStore.getState().sandboxEnabled).toBe(false))
    expect(trackMock).not.toHaveBeenCalled()
  })

  it('never passes a pipeline name through', async () => {
    renderControl({ profile_exists: true, resolved_pipelines: ['bronze'] }, true)
    await userEvent.setup().click(screen.getByText('1 pipeline'))
    expect(JSON.stringify(trackMock.mock.calls)).not.toContain('bronze')
  })
})
