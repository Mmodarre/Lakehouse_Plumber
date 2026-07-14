import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { AssistantDock } from '../AssistantDock'
import { useLayoutStore } from '../../../store/layoutStore'
import { useAssistantStore } from '../../../store/assistantStore'

// AssistantPanel is React.lazy inside the dock and carries the whole chat
// hook/markdown stack — stub it so the dock's rail ⇄ open/collapse bridge is
// what's under test.
vi.mock('../../assistant/AssistantPanel', () => ({
  default: () => <div data-testid="assistant-panel" />,
}))

beforeEach(() => {
  useLayoutStore.setState({ assistantOpen: false })
  useAssistantStore.setState({ panelOpen: false })
})

describe('AssistantDock', () => {
  it('shows the rail when closed and opens the dock on click', async () => {
    const user = userEvent.setup()
    render(<AssistantDock />)

    expect(screen.queryByTestId('assistant-panel')).not.toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Open assistant' }))
    expect(useLayoutStore.getState().assistantOpen).toBe(true)
  })

  it('renders AssistantPanel and arms panelOpen when the dock is open', async () => {
    useLayoutStore.setState({ assistantOpen: true })
    render(<AssistantDock />)

    expect(await screen.findByTestId('assistant-panel')).toBeInTheDocument()
    // The wrapper arms the panel flag so AssistantPanel's × produces an edge.
    await waitFor(() => expect(useAssistantStore.getState().panelOpen).toBe(true))
  })

  it("collapses the dock when AssistantPanel's × drives panelOpen to false", async () => {
    useLayoutStore.setState({ assistantOpen: true })
    useAssistantStore.setState({ panelOpen: true })
    render(<AssistantDock />)
    expect(await screen.findByTestId('assistant-panel')).toBeInTheDocument()

    // Simulate AssistantPanel's own close button (it writes panelOpen=false).
    act(() => useAssistantStore.setState({ panelOpen: false }))
    await waitFor(() => expect(useLayoutStore.getState().assistantOpen).toBe(false))
  })
})
