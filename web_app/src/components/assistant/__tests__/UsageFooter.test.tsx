import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { UsageFooter } from '../UsageFooter'
import type { UsageTotals } from '../../../types/assistant'

const usage: UsageTotals = {
  input_tokens: 12_300,
  output_tokens: 4_500,
  cache_read_input_tokens: 88_000,
  cache_creation_input_tokens: 1_000,
  sdk_cost_usd: 0.42,
  configured_cost_usd: null,
}

describe('UsageFooter', () => {
  it('renders compact counts with a raw-count tooltip', () => {
    render(
      <UsageFooter
        usage={usage}
        mode="claude_subscription"
        onSetPricing={() => {}}
      />,
    )
    const footer = screen.getByTestId('usage-footer')
    expect(footer).toHaveTextContent('12.3k in · 4.5k out · 89.0k cache')
    expect(footer).toHaveAttribute(
      'title',
      'input 12300 · output 4500 · cache read 88000 · cache write 1000',
    )
  })

  it('subscription mode labels the SDK cost as an estimate', () => {
    render(
      <UsageFooter
        usage={usage}
        mode="claude_subscription"
        onSetPricing={() => {}}
      />,
    )
    expect(screen.getByTestId('usage-footer')).toHaveTextContent('$0.42 est.')
  })

  it('databricks mode shows the configured cost when present (no est.)', () => {
    render(
      <UsageFooter
        usage={{ ...usage, configured_cost_usd: 1.07 }}
        mode="databricks"
        onSetPricing={() => {}}
      />,
    )
    const footer = screen.getByTestId('usage-footer')
    expect(footer).toHaveTextContent('$1.07')
    expect(footer).not.toHaveTextContent('est.')
    expect(screen.queryByRole('button', { name: 'Set pricing' })).toBeNull()
  })

  it('databricks mode without configured cost offers a Set pricing link', () => {
    const onSetPricing = vi.fn()
    render(
      <UsageFooter usage={usage} mode="databricks" onSetPricing={onSetPricing} />,
    )
    const footer = screen.getByTestId('usage-footer')
    // The SDK estimate is meaningless for Databricks billing: never shown.
    expect(footer).not.toHaveTextContent('$0.42')
    screen.getByRole('button', { name: 'Set pricing' }).click()
    expect(onSetPricing).toHaveBeenCalledTimes(1)
  })

  it('hidden when usage is null or all-zero', () => {
    const { rerender } = render(
      <UsageFooter usage={null} mode="claude_subscription" onSetPricing={() => {}} />,
    )
    expect(screen.queryByTestId('usage-footer')).toBeNull()
    rerender(
      <UsageFooter
        usage={{
          input_tokens: 0,
          output_tokens: 0,
          cache_read_input_tokens: 0,
          cache_creation_input_tokens: 0,
          sdk_cost_usd: null,
          configured_cost_usd: null,
        }}
        mode="claude_subscription"
        onSetPricing={() => {}}
      />,
    )
    expect(screen.queryByTestId('usage-footer')).toBeNull()
  })
})
