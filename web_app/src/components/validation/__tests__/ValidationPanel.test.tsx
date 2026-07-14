import { describe, expect, it, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { ValidationPanel } from '../ValidationPanel'
import { useRunStore } from '../../../store/runStore'

// ValidationPanel renders the run controller (→ useEventStream → react-query),
// so a QueryClientProvider must wrap it. No stream is started on mount.
function renderPanel() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity } },
  })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  )
  return render(<ValidationPanel />, { wrapper })
}

beforeEach(() => {
  useRunStore.getState().reset()
})

describe('ValidationPanel — severity log feed', () => {
  it('tags each streaming line by its prefix with the matching severity color', () => {
    useRunStore.setState({
      runKind: 'generate',
      isRunning: true,
      startedAt: Date.now(),
      infoLog: [
        'Resolved 3 pipelines, 12 flowgroups',
        '✓ Generated bronze_customers',
        '⚠ DEP-002 inferSchema may drift',
        '✗ SUB-007 unresolved ${catalog}',
      ],
    })
    renderPanel()

    const feed = screen.getByRole('log')
    // One tag per severity, in the shared token colors.
    expect(feed.querySelector('.text-info')).toBeInTheDocument()
    expect(feed.querySelector('.text-success')).toBeInTheDocument()
    expect(feed.querySelector('.text-warning')).toBeInTheDocument()
    expect(feed.querySelector('.text-error')).toBeInTheDocument()
    // Messages survive (marker stripped, remainder shown).
    expect(feed).toHaveTextContent('Resolved 3 pipelines, 12 flowgroups')
    expect(feed).toHaveTextContent('Generated bronze_customers')
  })

  it('tints a fully-qualified written target with text-kind-write', () => {
    useRunStore.setState({
      runKind: 'generate',
      isRunning: true,
      startedAt: Date.now(),
      infoLog: ['✓ write_customers → main.bronze.customers'],
    })
    renderPanel()

    const feed = screen.getByRole('log')
    const target = feed.querySelector('.text-kind-write')
    expect(target).toBeInTheDocument()
    expect(target).toHaveTextContent('main.bronze.customers')
  })

  it('puts a spinner on the last line while running, and none when finished', () => {
    useRunStore.setState({
      runKind: 'generate',
      isRunning: true,
      startedAt: Date.now(),
      infoLog: ['first', 'second'],
    })
    const { rerender } = renderPanel()
    // Exactly the active (last) line carries an inline spinner.
    expect(screen.getByRole('log').querySelectorAll('.animate-spin')).toHaveLength(1)

    useRunStore.setState({ isRunning: false, terminal: 'success' })
    rerender(<ValidationPanel />)
    expect(screen.getByRole('log').querySelectorAll('.animate-spin')).toHaveLength(0)
  })

  it('does not mistake a word marker that is only a prefix of the first word', () => {
    useRunStore.setState({
      runKind: 'validate',
      isRunning: true,
      startedAt: Date.now(),
      infoLog: ['Information about schema', 'Warnings: 3 found', 'OKlahoma pipeline'],
    })
    renderPanel()

    const feed = screen.getByRole('log')
    // All three fall through to the default (info) severity — no warn/ok/error.
    expect(feed.querySelectorAll('.text-info')).toHaveLength(3)
    expect(feed.querySelector('.text-warning')).not.toBeInTheDocument()
    expect(feed.querySelector('.text-success')).not.toBeInTheDocument()
    expect(feed.querySelector('.text-error')).not.toBeInTheDocument()
    // …and each message is left UNCHANGED (nothing mis-stripped).
    expect(feed).toHaveTextContent('Information about schema')
    expect(feed).toHaveTextContent('Warnings: 3 found')
    expect(feed).toHaveTextContent('OKlahoma pipeline')
  })

  it('still classifies and strips genuine word/symbol markers', () => {
    useRunStore.setState({
      runKind: 'validate',
      isRunning: true,
      startedAt: Date.now(),
      infoLog: ['INFO: hello', 'Failed to write', '✓ Generated x'],
    })
    renderPanel()

    const feed = screen.getByRole('log')
    expect(feed.querySelector('.text-info')).toBeInTheDocument()
    expect(feed.querySelector('.text-error')).toBeInTheDocument()
    expect(feed.querySelector('.text-success')).toBeInTheDocument()
    // Markers are stripped, remainders survive.
    expect(feed).toHaveTextContent('hello')
    expect(feed).not.toHaveTextContent('INFO:')
    expect(feed).toHaveTextContent('to write')
    expect(feed).not.toHaveTextContent('Failed')
    expect(feed).toHaveTextContent('Generated x')
    expect(feed).not.toHaveTextContent('✓')
  })
})

describe('ValidationPanel — elapsed timer', () => {
  it('renders an mm:ss elapsed timer while a run is in flight', () => {
    useRunStore.setState({
      runKind: 'validate',
      isRunning: true,
      startedAt: Date.now(),
      infoLog: [],
    })
    renderPanel()
    expect(screen.getByLabelText('Elapsed time')).toHaveTextContent(/\d{2}:\d{2}/)
  })

  it('does not render the timer in the standalone (non-running) validation view', () => {
    useRunStore.setState({
      runKind: 'validate',
      isRunning: false,
      startedAt: null,
      terminal: 'success',
    })
    renderPanel()
    expect(screen.queryByLabelText('Elapsed time')).not.toBeInTheDocument()
  })
})
