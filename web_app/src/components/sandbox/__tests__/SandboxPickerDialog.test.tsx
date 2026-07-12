import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { SandboxPickerDialog } from '../SandboxPickerDialog'
import { useUIStore } from '../../../store/uiStore'
import type { SandboxScope } from '../../../types/api'

const writeFile = vi.fn((..._args: unknown[]) => Promise.resolve())
const fetchFileContentWithMeta = vi.fn((..._args: unknown[]) => undefined)
vi.mock('../../../api/files', () => ({
  writeFile: (...args: unknown[]) => writeFile(...args),
  fetchFileContentWithMeta: (...args: unknown[]) => fetchFileContentWithMeta(...args),
}))
vi.mock('sonner', () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

// The picker fires an un-awaited `invalidateQueries(['sandbox'])` on save; that
// refetch runs a slow cold flowgroup discovery. Keeping fetch pending models
// that window — the optimistic cache write is then the only thing that can make
// the saved scope visible before the refetch lands.
const pendingFetch = vi.fn(() => new Promise<Response>(() => {}))

function makeClient(scope: SandboxScope): QueryClient {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity } },
  })
  queryClient.setQueryData(['sandbox'], scope)
  queryClient.setQueryData(['pipelines'], {
    pipelines: [{ name: 'domain_a_silver' }, { name: 'domain_b_silver' }],
  })
  return queryClient
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', pendingFetch)
  // jsdom lacks the layout API Radix's ScrollArea (the pipeline list) observes.
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

describe('SandboxPickerDialog', () => {
  it('optimistically reflects the saved scope so a reopen shows it', async () => {
    const user = userEvent.setup()
    const queryClient = makeClient({ profile_exists: false })
    const onOpenChange = vi.fn()
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    )
    const { rerender } = render(
      <SandboxPickerDialog open={true} onOpenChange={onOpenChange} />,
      { wrapper },
    )

    await user.type(screen.getByLabelText('Namespace'), 'mehdi')
    await user.click(screen.getByText('domain_a_silver'))
    await user.click(screen.getByRole('button', { name: 'Save scope' }))

    await waitFor(() => expect(writeFile).toHaveBeenCalledTimes(1))

    // The saved scope is in the cache immediately, without waiting for the
    // (still-pending) refetch.
    const cached = queryClient.getQueryData<SandboxScope>(['sandbox'])
    expect(cached?.profile_exists).toBe(true)
    expect(cached?.namespace).toBe('mehdi')
    expect(cached?.patterns).toEqual(['domain_a_silver'])

    expect(useUIStore.getState().sandboxEnabled).toBe(true)
    await waitFor(() => expect(onOpenChange).toHaveBeenCalledWith(false))

    // Reopen (close → open): the picker re-seeds from the cache. The bug was an
    // empty form here because the cache still held the pre-save scope.
    rerender(<SandboxPickerDialog open={false} onOpenChange={onOpenChange} />)
    rerender(<SandboxPickerDialog open={true} onOpenChange={onOpenChange} />)
    await waitFor(() =>
      expect(screen.getByLabelText('Namespace')).toHaveValue('mehdi'),
    )
  })
})
