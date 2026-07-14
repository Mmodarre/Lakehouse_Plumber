import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { AppShell } from '../AppShell'
import { useLayoutStore } from '../../../store/layoutStore'
import { useUIStore } from '../../../store/uiStore'

// Side-effecting shell wiring is stubbed so the smoke test stays deterministic
// (no SSE, no route blocker, no modal data fetches). The subjects under test
// are the CommandBar, the restyled StatusBar and the placeholder region grid.
vi.mock('../../../hooks/usePushChannel', () => ({ usePushChannel: () => {} }))
vi.mock('../../workspace/flowgroupBuffers', () => ({ useFlowgroupEditorBridge: () => {} }))
vi.mock('../../layout/NavigationGuard', () => ({ NavigationGuard: () => null }))
vi.mock('../../editor/CreateFlowgroupDialog', () => ({ CreateFlowgroupDialog: () => null }))
vi.mock('../../sandbox/SandboxControl', () => ({ SandboxControl: () => <div data-testid="sandbox" /> }))
// The explorer (T1.3) is exercised by its own tests; here it stands in as a
// sentinel so the shell smoke test stays isolated from its data hooks.
vi.mock('../explorer/Explorer', () => ({ Explorer: () => <div data-testid="explorer" /> }))
// The center (T1.4) is exercised by its own tests; here it stands in as a
// sentinel so the shell smoke test stays isolated from its editor machinery
// (useWorkspaceSave / lazy Monaco + designer canvas).
vi.mock('../center/CenterArea', () => ({ CenterArea: () => <div data-testid="center" /> }))
// The T1.5 regions (inspector / assistant dock / bottom panel) are exercised by
// their own tests; here they stand in as sentinels so the shell smoke test
// stays isolated from their data hooks and the lazy AssistantPanel import.
vi.mock('../inspector/Inspector', () => ({ Inspector: () => <div data-testid="inspector" /> }))
vi.mock('../AssistantDock', () => ({ AssistantDock: () => <div data-testid="assistant-dock" /> }))
vi.mock('../bottom/BottomPanel', () => ({ BottomPanel: () => <div data-testid="bottom-panel" /> }))

const pendingFetch = vi.fn(() => new Promise<Response>(() => {}))

function renderShell() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity } },
  })
  queryClient.setQueryData(['project'], { name: 'perf_testing', version: '0.9.1' })
  queryClient.setQueryData(['health'], {
    status: 'healthy',
    version: '0.9.1',
    project_state: 'ready',
    root: '/proj',
  })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  return render(<AppShell />, { wrapper })
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', pendingFetch)
  useLayoutStore.setState({ assistantOpen: false, viewerMode: false })
  useUIStore.setState({ selectedEnv: 'dev', createFlowgroupDialog: false, selectedPipelineConfig: null })
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('AppShell', () => {
  it('renders the command bar, restyled status bar and placeholder regions', () => {
    renderShell()

    // Command bar: wordmark + relocated run actions.
    expect(screen.getByText('LHP')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Validate/ })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Generate/ })).toBeInTheDocument()

    // Status bar: env + LHP version (accent footer).
    expect(screen.getByText('env: dev')).toBeInTheDocument()
    expect(screen.getByText('LHP v0.9.1')).toBeInTheDocument()

    // Placeholder region grid (filled by sibling tasks).
    expect(screen.getByTestId('explorer')).toBeInTheDocument()
    expect(screen.getByTestId('center')).toBeInTheDocument()
    expect(screen.getByTestId('bottom-panel')).toBeInTheDocument()
  })

  it('assistant toggle flips layoutStore.assistantOpen', async () => {
    renderShell()
    const user = userEvent.setup()

    expect(useLayoutStore.getState().assistantOpen).toBe(false)
    await user.click(screen.getByRole('button', { name: /Open assistant dock/ }))
    expect(useLayoutStore.getState().assistantOpen).toBe(true)
  })

  it('viewer-lens toggle flips layoutStore.viewerMode and shows the Viewer badge', async () => {
    renderShell()
    const user = userEvent.setup()

    expect(screen.queryByText('Viewer')).not.toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /viewer lens/i }))
    expect(useLayoutStore.getState().viewerMode).toBe(true)
    expect(screen.getByText('Viewer')).toBeInTheDocument()
  })
})
