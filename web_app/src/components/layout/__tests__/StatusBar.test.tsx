import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { StatusBar } from '../StatusBar'
import { useRunStore } from '../../../store/runStore'

// The update hint is purely a projection of `health.latest_version`, which the
// server populates only when a strictly newer release exists. The frontend
// must not re-compare versions, so these cases pin the two states it can see:
// no value (hint absent) and a value (hint shown, with the exact tooltip).

const HINT_TITLE = 'A newer version is available: pip install -U lakehouse-plumber'

const mocks = vi.hoisted(() => ({
  useHealth: vi.fn(),
  useProject: vi.fn(),
}))

vi.mock('../../../hooks/useProject', () => ({
  useHealth: mocks.useHealth,
  useProject: mocks.useProject,
}))

function renderStatusBar(latestVersion?: string | null) {
  mocks.useHealth.mockReturnValue({
    data: {
      status: 'healthy',
      version: '0.9.2',
      project_state: 'ok',
      root: '/proj',
      telemetry_enabled: true,
      ...(latestVersion === undefined ? {} : { latest_version: latestVersion }),
    },
  })
  mocks.useProject.mockReturnValue({ data: { name: 'demo' } })
  return render(<StatusBar />)
}

beforeEach(() => {
  vi.clearAllMocks()
  useRunStore.getState().reset()
})

describe('StatusBar — newer-version hint', () => {
  it('shows only the installed version when the field is absent', () => {
    renderStatusBar()
    expect(screen.getByText('LHP v0.9.2')).toBeInTheDocument()
    expect(screen.queryByTitle(HINT_TITLE)).toBeNull()
  })

  it('shows no hint when the field is explicitly null', () => {
    renderStatusBar(null)
    expect(screen.queryByTitle(HINT_TITLE)).toBeNull()
  })

  it('shows no hint for an empty string', () => {
    renderStatusBar('')
    expect(screen.queryByTitle(HINT_TITLE)).toBeNull()
  })

  it('renders the hint next to the installed version when a newer release is reported', () => {
    renderStatusBar('0.9.3')
    expect(screen.getByText('LHP v0.9.2')).toBeInTheDocument()
    const hint = screen.getByTitle(HINT_TITLE)
    expect(hint).toHaveTextContent('↑ v0.9.3')
  })
})
