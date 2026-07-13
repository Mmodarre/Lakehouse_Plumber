import { beforeAll, describe, expect, it, vi } from 'vitest'
import type { ReactElement } from 'react'
import { render, screen } from '@testing-library/react'
import { TooltipProvider } from '@/components/ui/tooltip'
import { FieldLabel } from '../FieldLabel'

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

// Use the `help` OVERRIDE path so no SchemaKindProvider is needed — the
// override wins in useFieldHelp. delayDuration 0 opens the tooltip on focus.
function renderLabel(ui: ReactElement) {
  return render(<TooltipProvider delayDuration={0}>{ui}</TooltipProvider>)
}

describe('FieldLabel', () => {
  it('renders the label plus an (i) help button when help is provided', async () => {
    renderLabel(<FieldLabel label="Serverless" help="Databricks-managed compute." />)

    expect(screen.getByText('Serverless')).toBeInTheDocument()
    const button = screen.getByRole('button', { name: /more info about serverless/i })

    button.focus()
    const tip = await screen.findByRole('tooltip')
    expect(tip).toHaveTextContent('Databricks-managed compute.')
  })

  it('renders just the label with no help icon when there is no help', () => {
    renderLabel(<FieldLabel label="Serverless" />)

    expect(screen.getByText('Serverless')).toBeInTheDocument()
    expect(screen.queryByRole('button')).toBeNull()
  })
})
