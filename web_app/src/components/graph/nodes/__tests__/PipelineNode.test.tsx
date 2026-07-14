import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Workflow } from 'lucide-react'
import type { NodeProps } from '@xyflow/react'

// <Handle> needs a live ReactFlow context; stub the primitives PipelineNode uses.
vi.mock('@xyflow/react', () => ({
  Handle: () => null,
  Position: { Left: 'left', Right: 'right', Top: 'top', Bottom: 'bottom' },
}))

import { PipelineNode } from '../PipelineNode'
import { NodeCard } from '../NodeCard'

describe('NodeCard — sublabel row guard (Fix #5)', () => {
  it('renders the sublabel row when the sublabel is a non-empty string', () => {
    render(
      <NodeCard label="orders" sublabel="main.bronze.orders" icon={Workflow} chipClassName="" />,
    )
    expect(screen.getByText('main.bronze.orders')).toBeInTheDocument()
  })

  it('omits the sublabel row entirely when the sublabel is an empty string', () => {
    const { container } = render(
      <NodeCard label="bronze" sublabel="" icon={Workflow} chipClassName="" />,
    )
    // The label still renders…
    expect(screen.getByText('bronze')).toBeInTheDocument()
    // …but the muted sublabel row is not in the DOM at all.
    expect(container.querySelector('.text-2xs')).toBeNull()
  })
})

describe('PipelineNode (Fix #5)', () => {
  it('shows no sublabel — neither the enrichment FQN nor the old "Pipeline" caption', () => {
    const props = {
      data: { label: 'bronze', fqn: 'main.bronze.orders' },
      selected: false,
    } as unknown as NodeProps

    const { container } = render(<PipelineNode {...props} />)

    // The pipeline name renders as the node label…
    expect(screen.getByText('bronze')).toBeInTheDocument()
    // …but the grey enrichment FQN sublabel is gone, and so is the old static
    // "Pipeline" caption it used to fall back to.
    expect(screen.queryByText('main.bronze.orders')).not.toBeInTheDocument()
    expect(screen.queryByText('Pipeline')).not.toBeInTheDocument()
    expect(container.querySelector('.text-2xs')).toBeNull()
  })
})
