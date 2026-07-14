import type { ComponentProps } from 'react'
import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { DesignerActionNode } from '../DesignerActionNode'

// The node renders <Handle> from @xyflow/react, which needs the ReactFlow
// context. Stub it (Handle → null) so the node can render standalone; we only
// exercise the pencil edit affordance here.
vi.mock('@xyflow/react', () => ({
  Handle: () => null,
  Position: { Left: 'left', Right: 'right' },
}))

type NodeCompProps = ComponentProps<typeof DesignerActionNode>

// DesignerActionNode only reads id / data / selected off NodeProps; cast a
// partial rather than fabricate every React Flow node field.
function renderNode(data: Record<string, unknown>, selected = false) {
  const props = { id: 'load_orders', data, selected } as unknown as NodeCompProps
  return render(<DesignerActionNode {...props} />)
}

describe('DesignerActionNode — edit affordance (discoverability fix)', () => {
  it('renders a pencil button that opens the editor via onEditAction', () => {
    const onEditAction = vi.fn()
    renderNode({ label: 'load_orders', nodeType: 'load', onEditAction })

    const pencil = screen.getByRole('button', { name: /edit load_orders/i })
    expect(pencil).toHaveAttribute('title', 'Edit action')

    fireEvent.click(pencil)
    expect(onEditAction).toHaveBeenCalledWith('load_orders')
  })

  it('omits the pencil when no onEditAction is wired (e.g. read-only / external nodes)', () => {
    renderNode({ label: 'load_orders', nodeType: 'load' })
    expect(screen.queryByRole('button', { name: /edit load_orders/i })).not.toBeInTheDocument()
  })
})
