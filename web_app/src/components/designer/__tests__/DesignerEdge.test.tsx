import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { EdgePipeChip } from '../DesignerEdge'

// The chip is split out of DesignerEdge so its behaviour is testable without a
// React Flow render (EdgeLabelRenderer + the SVG path are visual, not asserted
// here — per the task, structure/behaviour is what we check).

describe('EdgePipeChip — data edge (named pipe)', () => {
  it('renders the view name as a focusable control that names the view + producer', () => {
    render(
      <EdgePipeChip
        designerKind="data"
        viewName="v_orders_raw"
        producerKind="load"
        sourceId="load_orders"
        onSelectView={vi.fn()}
      />,
    )
    const chip = screen.getByRole('button', { name: /view v_orders_raw/i })
    expect(chip).toHaveAccessibleName(/produced by load_orders/i)
    expect(chip).toHaveTextContent('v_orders_raw')
  })

  it('selects the producing action when clicked (the literal source/target string)', async () => {
    const user = userEvent.setup()
    const onSelectView = vi.fn()
    render(
      <EdgePipeChip
        designerKind="data"
        viewName="v_orders_raw"
        producerKind="load"
        sourceId="load_orders"
        onSelectView={onSelectView}
      />,
    )
    await user.click(screen.getByRole('button', { name: /view v_orders_raw/i }))
    expect(onSelectView).toHaveBeenCalledWith('load_orders')
  })

  it('shows a kind-colored producer dot for a known kind and none for external', () => {
    const { container, rerender } = render(
      <EdgePipeChip
        designerKind="data"
        viewName="v_x"
        producerKind="transform"
        sourceId="t"
        onSelectView={vi.fn()}
      />,
    )
    expect(container.querySelector('.bg-kind-transform')).not.toBeNull()

    rerender(
      <EdgePipeChip
        designerKind="data"
        viewName="v_x"
        producerKind=""
        sourceId="ext:src"
        onSelectView={vi.fn()}
      />,
    )
    expect(container.querySelector('[class*="bg-kind-"]')).toBeNull()
  })

  it('renders nothing when the view name is empty', () => {
    const { container } = render(
      <EdgePipeChip designerKind="data" viewName="" producerKind="load" sourceId="l" />,
    )
    expect(container.firstChild).toBeNull()
  })
})

describe('EdgePipeChip — depends_on edge (distinct visual)', () => {
  it('renders a non-interactive "depends on" tag, not a view-name chip', () => {
    const onSelectView = vi.fn()
    render(
      <EdgePipeChip
        designerKind="depends_on"
        viewName="audit_table"
        producerKind=""
        sourceId="audit"
        onSelectView={onSelectView}
      />,
    )
    // No functional view-name button.
    expect(screen.queryByRole('button')).toBeNull()
    // The relationship is shown, not the name-as-view chip.
    expect(screen.getByText(/depends on/i)).toBeInTheDocument()
    expect(screen.queryByText('audit_table')).toBeNull()
    // The referenced name is still available to assistive tech.
    expect(screen.getByLabelText(/dependency on audit_table/i)).toBeInTheDocument()
  })
})
