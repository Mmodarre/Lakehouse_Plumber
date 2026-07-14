import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
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

// ── PermissionsEditor — 0/1/2-principal transitions ──────────
//
// Driven through the full editor so the validator wiring (VAL_009
// exactly-one-principal, string level) and the byte-level writes are both
// exercised.

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('fetch', fetchMock)
  installRadixStubs()
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('PermissionsEditor', () => {
  it('1 principal: switching the radio type carries the value and swaps the key', async () => {
    const { bufferContent } = servePipeline(
      'pipeline: p1\npermissions:\n  - level: CAN_MANAGE\n    user_name: a@x.com\n',
    )
    await renderPipelineEditor()
    const user = userEvent.setup()

    const userRadio = screen.getByRole('radio', { name: 'User principal for permission 1' })
    expect(userRadio).toBeChecked()

    await user.click(screen.getByRole('radio', { name: 'Group principal for permission 1' }))

    await waitFor(() => {
      const body = bufferContent()
      expect(body).toContain('group_name: a@x.com')
      expect(body).not.toContain('user_name')
    })
  })

  it('0 principals: validator issue shows; picking a type sets the key', async () => {
    servePipeline('pipeline: p1\npermissions:\n  - level: CAN_VIEW\n')
    await renderPipelineEditor()
    const user = userEvent.setup()

    expect(screen.getByText(/must have exactly one of/)).toBeInTheDocument()

    await user.click(screen.getByRole('radio', { name: 'User principal for permission 1' }))
    await waitFor(() =>
      expect(screen.queryByText(/must have exactly one of/)).not.toBeInTheDocument(),
    )
  })

  it('2 principals: prune rows shown; removing one clears the issue', async () => {
    servePipeline(
      'pipeline: p1\npermissions:\n  - level: CAN_RUN\n    user_name: a\n    group_name: b\n',
    )
    await renderPipelineEditor()
    const user = userEvent.setup()

    expect(screen.getByText(/must have exactly one of/)).toBeInTheDocument()
    await user.click(
      screen.getByRole('button', { name: 'Remove group_name from permission 1' }),
    )
    await waitFor(() =>
      expect(screen.queryByText(/must have exactly one of/)).not.toBeInTheDocument(),
    )
  })

  it('2 principals: the type radios are disabled (never destructive) until pruned', async () => {
    const { bufferContent } = servePipeline(
      'pipeline: p1\npermissions:\n  - level: CAN_RUN\n    user_name: a\n    group_name: b\n',
    )
    await renderPipelineEditor()
    const user = userEvent.setup()

    // Clicking a radio here used to silently delete both principal values
    // and write '' — the radios must be inert in this state instead.
    const spRadio = screen.getByRole('radio', {
      name: 'Service principal principal for permission 1',
    })
    expect(spRadio).toBeDisabled()
    expect(
      screen.getByText(/Remove the extra principal entries below before switching the type/),
    ).toBeInTheDocument()
    await user.click(spRadio)
    // Inert: the disabled radio commits nothing — the buffer is untouched.
    expect(bufferContent()).toBe(
      'pipeline: p1\npermissions:\n  - level: CAN_RUN\n    user_name: a\n    group_name: b\n',
    )
    expect(screen.getByText(/must have exactly one of/)).toBeInTheDocument()

    // Pruning back to one principal re-enables the radios with the value intact.
    await user.click(
      screen.getByRole('button', { name: 'Remove group_name from permission 1' }),
    )
    const userRadio = screen.getByRole('radio', { name: 'User principal for permission 1' })
    await waitFor(() => expect(userRadio).toBeEnabled())
    expect(userRadio).toBeChecked()
    expect(screen.getByDisplayValue('a')).toBeInTheDocument()
  })

  it('level select writes the chosen level', async () => {
    const { bufferContent } = servePipeline(
      'pipeline: p1\npermissions:\n  - level: CAN_MANAGE\n    user_name: a@x.com\n',
    )
    await renderPipelineEditor()
    const user = userEvent.setup()

    await user.click(screen.getByLabelText('Level'))
    await user.click(await screen.findByRole('option', { name: 'CAN_VIEW' }))

    await waitFor(() => expect(bufferContent()).toContain('level: CAN_VIEW'))
  })

  it('add permission seeds a valid skeleton; removing the last entry deletes the key', async () => {
    const { bufferContent } = servePipeline('pipeline: p1\ncatalog: main\n')
    await renderPipelineEditor()
    const user = userEvent.setup()

    await user.click(screen.getByRole('button', { name: 'Add permission' }))
    expect(screen.getByText('Permission 1')).toBeInTheDocument()
    await waitFor(() => {
      const body = bufferContent()
      expect(body).toContain('permissions:')
      expect(body).toContain('level: CAN_MANAGE')
    })

    await user.click(screen.getByRole('button', { name: 'Remove permission 1' }))
    await waitFor(() => expect(bufferContent()).not.toContain('permissions'))
  })
})
