import { beforeAll, describe, expect, it, vi } from 'vitest'
import type { ReactElement } from 'react'
import { render, screen } from '@testing-library/react'
import { TooltipProvider } from '@/components/ui/tooltip'
import { FieldHelp } from '../FieldHelp'

// The Radix tooltip Arrow measures itself via ResizeObserver, which jsdom
// lacks (same stub the repo installs for other Radix components).
beforeAll(() => {
  vi.stubGlobal(
    'ResizeObserver',
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  )
})

// Unit tests don't mount main.tsx, so supply the global provider here.
// delayDuration 0 makes the tooltip open immediately on hover/focus.
function renderHelp(ui: ReactElement) {
  return render(<TooltipProvider delayDuration={0}>{ui}</TooltipProvider>)
}

describe('FieldHelp', () => {
  it('renders no button when there is no text (undefined or empty)', () => {
    const { rerender } = renderHelp(<FieldHelp />)
    expect(screen.queryByRole('button')).toBeNull()

    rerender(
      <TooltipProvider delayDuration={0}>
        <FieldHelp text="" />
      </TooltipProvider>,
    )
    expect(screen.queryByRole('button')).toBeNull()
  })

  it('exposes an accessible name built from the label', () => {
    renderHelp(<FieldHelp text="Serverless compute." label="Serverless" />)
    expect(
      screen.getByRole('button', { name: /more info about serverless/i }),
    ).toBeInTheDocument()
  })

  it('reveals the tooltip content on focus', async () => {
    renderHelp(<FieldHelp text="Serverless compute." label="Serverless" />)
    const button = screen.getByRole('button', { name: /more info about serverless/i })

    // Focus (not hover) drives the open path — the value keyboard users depend on.
    button.focus()

    // The role="tooltip" node is rendered only in the open portal subtree, so
    // finding it proves the focus→open path fired.
    const tip = await screen.findByRole('tooltip')
    expect(tip).toHaveTextContent('Serverless compute.')
  })

  it('is keyboard-focusable', () => {
    renderHelp(<FieldHelp text="Serverless compute." label="Serverless" />)
    const button = screen.getByRole('button', { name: /more info about serverless/i })

    button.focus()
    expect(button).toHaveFocus()
    expect(button).not.toHaveAttribute('tabindex', '-1')
  })
})
