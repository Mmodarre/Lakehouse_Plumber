import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { toast } from 'sonner'
import { RunConfigChip } from '../CommandBar'
import { useUIStore } from '../../../store/uiStore'
import type { FileNode } from '../../../types/api'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), info: vi.fn(), dismiss: vi.fn() },
}))

// The chip is the always-visible run-config indicator (the command bar mounts
// it) AND the stale-selection guard: when the files tree confirms the bound
// path is gone, the selection self-clears with a toast instead of wedging
// every future run on a 404.

const PATH = 'config/pipeline_config_dev.yaml'

const TREE: FileNode = {
  name: '',
  path: '',
  type: 'directory',
  children: [
    {
      name: 'config',
      path: 'config',
      type: 'directory',
      children: [{ name: 'pipeline_config_dev.yaml', path: PATH, type: 'file' }],
    },
  ],
}

// Never resolves: keeps the files query in its loading state so cache seeds
// (or their absence) fully control what the chip sees.
const pendingFetch = vi.fn(() => new Promise<Response>(() => {}))

function renderChip(tree?: FileNode) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity } },
  })
  if (tree !== undefined) queryClient.setQueryData(['files'], tree)
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  return render(<RunConfigChip />, { wrapper })
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', pendingFetch)
  useUIStore.setState({ selectedPipelineConfig: null })
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('RunConfigChip', () => {
  it('renders nothing while no run config is selected', () => {
    const { container } = renderChip(TREE)
    expect(container).toBeEmptyDOMElement()
  })

  it('shows the filename with the full path in the tooltip; ✕ clears with a toast', async () => {
    useUIStore.setState({ selectedPipelineConfig: PATH })
    renderChip(TREE)

    expect(screen.getByText('pipeline_config_dev.yaml')).toBeInTheDocument()
    expect(screen.getByTitle(new RegExp(PATH.replace(/\//g, '\\/')))).toBeInTheDocument()

    await userEvent
      .setup()
      .click(screen.getByRole('button', { name: /Stop using pipeline_config_dev.yaml/ }))
    expect(useUIStore.getState().selectedPipelineConfig).toBeNull()
    expect(toast.info).toHaveBeenCalledTimes(1)
  })

  it('clears a stale selection (file gone from the tree) with a toast', async () => {
    useUIStore.setState({ selectedPipelineConfig: 'config/deleted.yaml' })
    renderChip(TREE)

    await waitFor(() =>
      expect(useUIStore.getState().selectedPipelineConfig).toBeNull(),
    )
    expect(toast.info).toHaveBeenCalledWith(expect.stringContaining('deleted.yaml'))
    expect(screen.queryByText('deleted.yaml')).not.toBeInTheDocument()
  })

  it('trusts the selection while the tree has not loaded (no clear, chip shown)', () => {
    useUIStore.setState({ selectedPipelineConfig: PATH })
    renderChip() // no cache seed → query stays pending

    expect(screen.getByText('pipeline_config_dev.yaml')).toBeInTheDocument()
    expect(useUIStore.getState().selectedPipelineConfig).toBe(PATH)
    expect(toast.info).not.toHaveBeenCalled()
  })
})
