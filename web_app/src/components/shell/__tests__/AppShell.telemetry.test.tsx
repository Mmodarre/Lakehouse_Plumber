import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { ReactNode } from 'react'
import { act, render, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

// Telemetry wiring: the client and its store bindings are loaded lazily, and
// only once health has confirmed telemetry is on for this server process.
// A tab against an opted-out server must never fetch the telemetry chunk.

const mocks = vi.hoisted(() => ({
  clientLoads: 0,
  bindingsLoads: 0,
  setTelemetryEnabled: vi.fn(),
  installTelemetry: vi.fn(),
  installTelemetryBindings: vi.fn(),
  useHealth: vi.fn(),
}))

// Named so a test can swap in a failing factory and put this one back: the
// mock registry keeps a factory's module across vi.resetModules().
const telemetryClientModule = vi.hoisted(() => () => {
  mocks.clientLoads += 1
  return {
    setTelemetryEnabled: mocks.setTelemetryEnabled,
    installTelemetry: mocks.installTelemetry,
  }
})

vi.mock('../../../lib/telemetry', telemetryClientModule)
vi.mock('../../../lib/telemetry-bindings', () => {
  mocks.bindingsLoads += 1
  return { installTelemetryBindings: mocks.installTelemetryBindings }
})
vi.mock('../../../hooks/useProject', () => ({ useHealth: mocks.useHealth }))

// Every region is a sentinel: only the shell's own effect is under test.
vi.mock('../../../hooks/usePushChannel', () => ({ usePushChannel: () => {} }))
vi.mock('../../workspace/flowgroupBuffers', () => ({ useFlowgroupEditorBridge: () => {} }))
vi.mock('../../layout/NavigationGuard', () => ({ NavigationGuard: () => null }))
vi.mock('../../layout/OfflineBanner', () => ({ OfflineBanner: () => null }))
vi.mock('../../layout/StatusBar', () => ({ StatusBar: () => null }))
vi.mock('../../editor/CreateFlowgroupDialog', () => ({ CreateFlowgroupDialog: () => null }))
vi.mock('../../../pages/InitProjectPage', () => ({ InitProjectPage: () => null }))
vi.mock('../CommandBar', () => ({ CommandBar: () => null }))
vi.mock('../explorer/Explorer', () => ({ Explorer: () => null }))
vi.mock('../center/CenterArea', () => ({ CenterArea: () => null }))
vi.mock('../inspector/Inspector', () => ({ Inspector: () => null }))
vi.mock('../AssistantDock', () => ({ AssistantDock: () => null }))
vi.mock('../bottom/BottomPanel', () => ({ BottomPanel: () => null }))

function health(telemetryEnabled: boolean | undefined) {
  return {
    data: {
      status: 'healthy',
      version: '0.9.2',
      project_state: 'ok',
      root: '/proj',
      ...(telemetryEnabled === undefined ? {} : { telemetry_enabled: telemetryEnabled }),
    },
    isError: false,
    refetch: vi.fn(),
  }
}

async function renderShell() {
  vi.resetModules()
  const { AppShell } = await import('../AppShell')
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  const view = render(<AppShell />, { wrapper })
  return { ...view, AppShell }
}

// Lets any pending dynamic import settle.
async function settle() {
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 0))
  })
}

beforeEach(() => {
  vi.clearAllMocks()
  mocks.clientLoads = 0
  mocks.bindingsLoads = 0
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('AppShell telemetry wiring', () => {
  it('never loads the telemetry chunk while health reports telemetry off', async () => {
    mocks.useHealth.mockReturnValue(health(false))
    await renderShell()
    await settle()
    expect(mocks.clientLoads).toBe(0)
    expect(mocks.bindingsLoads).toBe(0)
    expect(mocks.setTelemetryEnabled).not.toHaveBeenCalled()
    expect(mocks.installTelemetry).not.toHaveBeenCalled()
    expect(mocks.installTelemetryBindings).not.toHaveBeenCalled()
  })

  it('never loads the telemetry chunk against a backend that does not report the flag', async () => {
    mocks.useHealth.mockReturnValue(health(undefined))
    await renderShell()
    await settle()
    expect(mocks.clientLoads).toBe(0)
    expect(mocks.setTelemetryEnabled).not.toHaveBeenCalled()
  })

  it('loads, installs and enables the client once health reports telemetry on', async () => {
    mocks.useHealth.mockReturnValue(health(true))
    await renderShell()
    await waitFor(() => expect(mocks.setTelemetryEnabled).toHaveBeenCalledWith(true))
    expect(mocks.clientLoads).toBe(1)
    expect(mocks.bindingsLoads).toBe(1)
    expect(mocks.installTelemetry).toHaveBeenCalledTimes(1)
    expect(mocks.installTelemetryBindings).toHaveBeenCalledTimes(1)
    expect(mocks.setTelemetryEnabled).toHaveBeenCalledTimes(1)
  })

  it('enables when the flag flips on after mount and disables on unmount', async () => {
    mocks.useHealth.mockReturnValue(health(false))
    const { rerender, unmount, AppShell } = await renderShell()
    await settle()
    expect(mocks.clientLoads).toBe(0)

    mocks.useHealth.mockReturnValue(health(true))
    rerender(<AppShell />)
    await waitFor(() => expect(mocks.setTelemetryEnabled).toHaveBeenCalledWith(true))
    expect(mocks.installTelemetryBindings).toHaveBeenCalledTimes(1)

    unmount()
    await waitFor(() => expect(mocks.setTelemetryEnabled).toHaveBeenLastCalledWith(false))
  })

  it('unsubscribes the store bindings when the effect is cleaned up', async () => {
    const unsubscribe = vi.fn()
    mocks.installTelemetryBindings.mockReturnValue(unsubscribe)
    mocks.useHealth.mockReturnValue(health(true))
    const { rerender, unmount, AppShell } = await renderShell()
    await waitFor(() => expect(mocks.installTelemetryBindings).toHaveBeenCalledTimes(1))
    expect(unsubscribe).not.toHaveBeenCalled()

    mocks.useHealth.mockReturnValue(health(false))
    rerender(<AppShell />)
    expect(unsubscribe).toHaveBeenCalledTimes(1)

    unmount()
    expect(unsubscribe).toHaveBeenCalledTimes(1)
  })

  it('swallows a telemetry chunk that fails to load', async () => {
    let failedLoads = 0
    vi.doMock('../../../lib/telemetry', () => {
      failedLoads += 1
      throw new Error('telemetry chunk failed to load')
    })
    const unhandled = vi.fn()
    process.on('unhandledRejection', unhandled)
    try {
      mocks.useHealth.mockReturnValue(health(true))
      const { unmount } = await renderShell()
      await waitFor(() => expect(failedLoads).toBe(1))
      unmount()
      await waitFor(() => expect(failedLoads).toBe(2))
      await settle()
      await settle()
      expect(mocks.setTelemetryEnabled).not.toHaveBeenCalled()
      expect(unhandled).not.toHaveBeenCalled()
    } finally {
      process.off('unhandledRejection', unhandled)
      vi.doMock('../../../lib/telemetry', telemetryClientModule)
    }
  })
})
