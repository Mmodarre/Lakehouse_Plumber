import { describe, expect, it, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'

// The dialog fetches the on-disk version on open; Monaco's diff editor is far
// too heavy for jsdom, so both are replaced with light stand-ins.
vi.mock('../../../api/files', () => ({
  fetchFileContentWithMeta: vi.fn(),
}))
vi.mock('../DiffEditorWrapper', () => ({
  default: () => <div data-testid="diff-editor" />,
}))

import { ConflictDialog } from '../ConflictDialog'
import { fetchFileContentWithMeta } from '../../../api/files'

const mockFetch = vi.mocked(fetchFileContentWithMeta)

const conflict = {
  path: 'pipelines/raw/orders.yaml',
  mine: 'pipeline: raw # mine\n',
  language: 'yaml',
}

function renderDialog(overrides: Partial<Parameters<typeof ConflictDialog>[0]> = {}) {
  const props = {
    conflict,
    onCancel: vi.fn(),
    onKeepMine: vi.fn(),
    onTakeTheirs: vi.fn(),
    ...overrides,
  }
  render(<ConflictDialog {...props} />)
  return props
}

describe('ConflictDialog', () => {
  beforeEach(() => {
    mockFetch.mockReset()
    mockFetch.mockResolvedValue({ content: 'pipeline: raw # theirs\n', etag: 'fresh-etag' })
  })

  it('renders nothing when there is no conflict', () => {
    renderDialog({ conflict: null })
    expect(screen.queryByText(/changed on disk/)).not.toBeInTheDocument()
  })

  it('fetches the on-disk version and shows the three resolution choices', async () => {
    renderDialog()
    expect(await screen.findByRole('button', { name: /keep my version/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /take disk version/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /merge manually/i })).toBeInTheDocument()
    expect(mockFetch).toHaveBeenCalledWith(conflict.path)
  })

  it('keep-mine re-saves my content with the FRESH on-disk etag (no double-clobber)', async () => {
    const user = userEvent.setup()
    const props = renderDialog()
    await user.click(await screen.findByRole('button', { name: /keep my version/i }))
    expect(props.onKeepMine).toHaveBeenCalledWith(conflict.path, conflict.mine, 'fresh-etag')
  })

  it('take-theirs hands back the disk content and its etag', async () => {
    const user = userEvent.setup()
    const props = renderDialog()
    await user.click(await screen.findByRole('button', { name: /take disk version/i }))
    expect(props.onTakeTheirs).toHaveBeenCalledWith(
      conflict.path,
      'pipeline: raw # theirs\n',
      'fresh-etag',
    )
  })

  it('merge mode shows the diff editor and saves the merged text with the fresh etag', async () => {
    const user = userEvent.setup()
    const props = renderDialog()
    await user.click(await screen.findByRole('button', { name: /merge manually/i }))
    expect(await screen.findByTestId('diff-editor')).toBeInTheDocument()
    // The stubbed diff editor exposes no handle, so the dialog falls back to
    // the unmodified "mine" text — still paired with the fresh etag.
    await user.click(screen.getByRole('button', { name: /save merged/i }))
    expect(props.onKeepMine).toHaveBeenCalledWith(conflict.path, conflict.mine, 'fresh-etag')
  })

  it('cancel closes without resolving', async () => {
    const user = userEvent.setup()
    const props = renderDialog()
    await user.click(await screen.findByRole('button', { name: /^cancel$/i }))
    expect(props.onCancel).toHaveBeenCalled()
    expect(props.onKeepMine).not.toHaveBeenCalled()
    expect(props.onTakeTheirs).not.toHaveBeenCalled()
  })

  it('surfaces a load failure instead of the choices', async () => {
    mockFetch.mockRejectedValue(new Error('boom'))
    renderDialog()
    expect(await screen.findByText(/failed to load the on-disk version/i)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /keep my version/i })).not.toBeInTheDocument()
  })
})
