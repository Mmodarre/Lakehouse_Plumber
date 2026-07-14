import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import {
  fetchMock,
  installRadixStubs,
  renderPipelineEditor,
  servePipeline,
} from './pipelineFormTestSupport'

vi.mock('sonner', () => ({
  toast: { error: vi.fn(), success: vi.fn(), dismiss: vi.fn() },
}))

// ── GroupMembershipEditor — suggestions ∪ free text ──────────

const GROUP_FIXTURE = `pipeline:
  - bronze_a
edition: PRO
`

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  installRadixStubs()
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('GroupMembershipEditor', () => {
  it('suggests project pipelines (minus current members); selecting one adds it', async () => {
    const { bufferContent } = servePipeline(GROUP_FIXTURE, {
      pipelines: ['bronze_a', 'sales_pipeline'],
    })
    await renderPipelineEditor()
    const user = userEvent.setup()

    await user.click(screen.getByRole('combobox', { name: 'Add pipeline to group' }))
    // bronze_a is already a member → only sales_pipeline is offered.
    const listbox = await screen.findByRole('listbox')
    const option = within(listbox).getByText('sales_pipeline')
    expect(within(listbox).queryByText('bronze_a')).not.toBeInTheDocument()
    await user.click(option)

    await waitFor(() =>
      expect(bufferContent()).toBe('pipeline:\n  - bronze_a\n  - sales_pipeline\nedition: PRO\n'),
    )
  })

  it('free text adds a name not in the suggestions', async () => {
    servePipeline(GROUP_FIXTURE, { pipelines: ['bronze_a'] })
    await renderPipelineEditor()
    const user = userEvent.setup()

    await user.click(screen.getByRole('combobox', { name: 'Add pipeline to group' }))
    await user.type(screen.getByPlaceholderText('Search pipelines…'), 'brand_new')
    await user.click(await screen.findByText('Add "brand_new"'))

    expect(screen.getByRole('button', { name: 'Remove brand_new' })).toBeInTheDocument()
  })

  it('removing a member chip deletes that entry', async () => {
    const { bufferContent } = servePipeline(
      'pipeline:\n  - bronze_a\n  - bronze_b\nedition: PRO\n',
    )
    await renderPipelineEditor()
    const user = userEvent.setup()

    await user.click(screen.getByRole('button', { name: 'Remove bronze_a' }))

    await waitFor(() =>
      expect(bufferContent()).toBe('pipeline:\n  - bronze_b\nedition: PRO\n'),
    )
  })
})
