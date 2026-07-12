import { describe, expect, it, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { ExternalConnection } from '../../../../types/graph'
import { ExternalBadge } from '../ExternalBadge'

const { openPipelineModal } = vi.hoisted(() => ({ openPipelineModal: vi.fn() }))
vi.mock('@/store/uiStore', () => ({ useUIStore: () => ({ openPipelineModal }) }))

async function openDropdown() {
  const user = userEvent.setup()
  await user.click(screen.getByRole('button', { name: /external connection/i }))
  return user
}

beforeEach(() => {
  openPipelineModal.mockClear()
})

describe('ExternalBadge — empty targetPipeline (Task 1 blank-label bug)', () => {
  const noPipeline: ExternalConnection[] = [
    { direction: 'downstream', targetNodeId: 'raw_events_source', targetPipeline: '' },
  ]

  it('labels the item with the node name instead of a blank pipeline', async () => {
    render(<ExternalBadge connections={noPipeline} />)
    await openDropdown()
    expect(screen.getByRole('menuitem')).toHaveTextContent('raw_events_source')
  })

  it('does NOT open a bogus empty-titled pipeline modal on click', async () => {
    render(<ExternalBadge connections={noPipeline} />)
    const user = await openDropdown()
    await user.click(screen.getByRole('menuitem'))
    expect(openPipelineModal).not.toHaveBeenCalled()
  })
})

describe('ExternalBadge — a real cross-pipeline target still drills in', () => {
  const withPipeline: ExternalConnection[] = [
    { direction: 'downstream', targetNodeId: 'orders_fg', targetPipeline: 'silver' },
  ]

  it('labels with the pipeline and opens the drill modal for it', async () => {
    render(<ExternalBadge connections={withPipeline} />)
    const user = await openDropdown()
    expect(screen.getByRole('menuitem')).toHaveTextContent('silver')
    await user.click(screen.getByRole('menuitem'))
    expect(openPipelineModal).toHaveBeenCalledWith('silver')
  })
})
