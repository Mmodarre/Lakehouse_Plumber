import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { FlowgroupModal } from '../FlowgroupModal'
import { useUIStore } from '../../../store/uiStore'
import { useWorkspaceStore } from '../../../store/workspaceStore'
import { useFlowgroupDetail } from '../../../hooks/useFlowgroups'

// The modal body is an action-level React Flow graph — irrelevant to the
// designer wiring under test and heavy in jsdom.
vi.mock('../ActionMiniGraph', () => ({
  ActionMiniGraph: () => <div data-testid="action-mini-graph" />,
}))

vi.mock('../../../hooks/useFlowgroups', () => ({
  useFlowgroupDetail: vi.fn(),
}))

const mockDetail = vi.mocked(useFlowgroupDetail)

function withDetail(sourceFile: string | undefined) {
  mockDetail.mockReturnValue({
    data:
      sourceFile === undefined
        ? undefined
        : { flowgroup: { name: 'orders' }, source_file: sourceFile },
  } as ReturnType<typeof useFlowgroupDetail>)
}

describe('FlowgroupModal — Open in Designer', () => {
  beforeEach(() => {
    useWorkspaceStore.setState({ buffers: [], tabs: [], activePath: null })
    useUIStore.setState({
      drillPipeline: 'bronze',
      drillFlowgroup: { name: 'orders', pipeline: 'bronze' },
    })
  })

  it('opens the designer tab with the source file and closes the drill stack', async () => {
    withDetail('pipelines/bronze/orders.yaml')
    const user = userEvent.setup()
    render(<FlowgroupModal />)

    await user.click(screen.getByRole('button', { name: 'Open in Designer' }))

    const ws = useWorkspaceStore.getState()
    expect(ws.tabs).toEqual([
      {
        kind: 'designer',
        id: 'designer:bronze/orders',
        pipeline: 'bronze',
        flowgroup: 'orders',
        filePath: 'pipelines/bronze/orders.yaml',
      },
    ])
    expect(ws.activePath).toBe('designer:bronze/orders')
    // No text buffer materialises — the canvas re-derives its own content.
    expect(ws.buffers).toEqual([])

    const ui = useUIStore.getState()
    expect(ui.drillFlowgroup).toBeNull()
    expect(ui.drillPipeline).toBeNull()
  })

  it('stays disabled until the flowgroup detail (source file) arrives', () => {
    withDetail(undefined)
    render(<FlowgroupModal />)

    expect(screen.getByRole('button', { name: 'Open in Designer' })).toBeDisabled()
    expect(useWorkspaceStore.getState().tabs).toEqual([])
  })
})
