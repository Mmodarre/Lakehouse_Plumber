import { beforeEach, describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { TooltipProvider } from '@/components/ui/tooltip'
import { UseForRunsToggle } from '../UseForRunsToggle'
import { useUIStore } from '../../../../store/uiStore'

// Semantics under test (per the run-wiring design): the toggle tracks the
// file it was enabled FOR, not the picker — switching the picked file never
// silently changes the run config; the switch reads checked only while the
// CURRENT pick is the run config.

const PATH = 'config/pipeline_config_dev.yaml'
const OTHER = 'config/pipeline_config_prod.yaml'

beforeEach(() => {
  useUIStore.setState({ selectedPipelineConfig: null })
})

// The (i) help icon renders a Radix Tooltip, which needs a TooltipProvider
// ancestor (delayDuration 0 so it opens immediately).
function renderToggle() {
  return render(
    <TooltipProvider delayDuration={0}>
      <UseForRunsToggle path={PATH} />
    </TooltipProvider>,
  )
}

describe('UseForRunsToggle', () => {
  it('unchecked with the run-consequence copy; toggling on binds this file', async () => {
    renderToggle()
    const toggle = screen.getByRole('switch', { name: 'Use for runs' })
    expect(toggle).not.toBeChecked()
    // R5: enabling this makes Generate write bundle resources — the copy
    // must state that consequence plainly, not hide it behind a tooltip.
    expect(screen.getByText(/Validate\/Generate use this file/)).toBeInTheDocument()
    expect(screen.getByText(/resources\/lhp\//)).toBeInTheDocument()

    await userEvent.setup().click(toggle)
    expect(useUIStore.getState().selectedPipelineConfig).toBe(PATH)
  })

  it('checked while this file is the run config; toggling off clears it', async () => {
    useUIStore.setState({ selectedPipelineConfig: PATH })
    renderToggle()
    const toggle = screen.getByRole('switch', { name: 'Use for runs' })
    expect(toggle).toBeChecked()

    await userEvent.setup().click(toggle)
    expect(useUIStore.getState().selectedPipelineConfig).toBeNull()
  })

  it('another bound file: unchecked, names it, and toggling on rebinds to this file', async () => {
    useUIStore.setState({ selectedPipelineConfig: OTHER })
    renderToggle()
    const toggle = screen.getByRole('switch', { name: 'Use for runs' })
    expect(toggle).not.toBeChecked()
    expect(screen.getByText('pipeline_config_prod.yaml')).toBeInTheDocument()

    await userEvent.setup().click(toggle)
    expect(useUIStore.getState().selectedPipelineConfig).toBe(PATH)
  })
})
