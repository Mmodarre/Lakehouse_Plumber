import { describe, expect, it } from 'vitest'
import { render } from '@testing-library/react'
import { Position, type EdgeProps } from '@xyflow/react'
import { DesignerEdge } from '../DesignerEdge'

// Fix #4 removed the over-the-edge view-name chip: a designer edge is now just
// the bezier <path>. These tests prove no label/chip is rendered regardless of
// the (still-threaded) edge.data, for both data and depends_on edges.

function renderEdge(data?: Record<string, unknown>) {
  const props = {
    id: 'e1',
    source: 'load_orders',
    target: 'clean_orders',
    sourceX: 0,
    sourceY: 0,
    targetX: 100,
    targetY: 100,
    sourcePosition: Position.Right,
    targetPosition: Position.Left,
    data,
    selected: false,
  } as unknown as EdgeProps
  return render(
    <svg>
      <DesignerEdge {...props} />
    </svg>,
  )
}

describe('DesignerEdge — label-free pipe (Fix #4)', () => {
  it('renders only the bezier path — no view-name chip or label', () => {
    const { container } = renderEdge({
      designerKind: 'data',
      viewName: 'v_orders_raw',
      producerKind: 'load',
      onSelectView: () => {},
    })
    // The path is present…
    expect(container.querySelector('path')).not.toBeNull()
    // …but the old clickable view-name chip and its label are gone.
    expect(container.querySelector('button')).toBeNull()
    expect(container.textContent).not.toContain('v_orders_raw')
  })

  it('renders no "depends on" tag on a depends_on edge', () => {
    const { container } = renderEdge({ designerKind: 'depends_on', viewName: 'audit_table' })
    expect(container.querySelector('path')).not.toBeNull()
    expect(container.textContent?.toLowerCase()).not.toContain('depends on')
    expect(container.textContent).not.toContain('audit_table')
  })

  it('applies the depends-vs-data stroke class from edge.data', () => {
    const { container } = renderEdge({ designerKind: 'depends_on' })
    expect(container.querySelector('path.lhp-edge--depends')).not.toBeNull()

    const { container: dataC } = renderEdge({ designerKind: 'data', viewName: 'v_x' })
    expect(dataC.querySelector('path.lhp-edge--data')).not.toBeNull()
  })
})
