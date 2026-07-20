import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import type { CodeTarget } from '../CodeModal'
import type { CompanionStatus } from '../useCompanionFile'

// Control the existence status per test while keeping the REAL
// `companionCheckablePath` (so the token case genuinely exercises its
// null-for-token behaviour — no network, no react-query).
const useCompanionFileMock = vi.fn()
vi.mock('../useCompanionFile', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../useCompanionFile')>()
  return {
    ...actual,
    useCompanionFile: (path: string | null) => useCompanionFileMock(path),
  }
})

import { FileRefField } from '../FileRefField'

function setStatus(status: CompanionStatus, create = vi.fn()) {
  useCompanionFileMock.mockReturnValue({ status, create })
  return create
}

beforeEach(() => {
  vi.clearAllMocks()
  setStatus('unavailable')
})

describe('FileRefField', () => {
  it('renders both Browse and New affordances for an empty value', () => {
    render(<FileRefField value="" onChange={vi.fn()} accept={['.sql']} onEditCode={vi.fn()} />)
    expect(screen.getByRole('button', { name: /browse/i })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /new/i })).toBeInTheDocument()
  })

  it('shows Edit file for an existing ref and opens the editor with the file target', () => {
    setStatus('exists')
    const onEditCode = vi.fn<(t: CodeTarget) => void>()
    render(
      <FileRefField
        value="sql/there.sql"
        onChange={vi.fn()}
        accept={['.sql']}
        onEditCode={onEditCode}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: /edit file/i }))
    expect(onEditCode).toHaveBeenCalledWith(
      expect.objectContaining({ backing: 'file', filePath: 'sql/there.sql' }),
    )
  })

  it('proposes the suggested path on New: sets the field then creates the stub there', async () => {
    const stub = 'table: orders\ncolumns:\n  - name: example_column\n    type: STRING\n    # tags:\n    #   pii: high\n'
    const create = setStatus('unavailable', vi.fn().mockResolvedValue(true))
    const onChange = vi.fn()
    const onEditCode = vi.fn<(t: CodeTarget) => void>()
    render(
      <FileRefField
        value=""
        onChange={onChange}
        accept={['.yaml', '.yml']}
        makeStub={() => stub}
        suggestedPath="schemas/orders.yaml"
        onEditCode={onEditCode}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: /new/i }))
    await Promise.resolve()
    expect(onChange).toHaveBeenCalledWith('schemas/orders.yaml')
    expect(create).toHaveBeenCalledWith(stub, 'schemas/orders.yaml')
    await vi.waitFor(() =>
      expect(onEditCode).toHaveBeenCalledWith(
        expect.objectContaining({ backing: 'file', filePath: 'schemas/orders.yaml' }),
      ),
    )
  })

  it('makeStub content wins over the extension-derived stub', async () => {
    const create = setStatus('unavailable', vi.fn().mockResolvedValue(true))
    render(
      <FileRefField
        value=""
        onChange={vi.fn()}
        accept={['.yaml', '.yml']}
        makeStub={() => 'CUSTOM SKELETON\n'}
        suggestedPath="schemas/orders.yaml"
        onEditCode={vi.fn()}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: /new/i }))
    await Promise.resolve()
    expect(create).toHaveBeenCalledTimes(1)
    expect(create.mock.calls[0][0]).toBe('CUSTOM SKELETON\n')
    expect(create.mock.calls[0][0]).not.toContain('# columns:')
  })

  it('is a no-op on New for an empty value with no suggestion (today’s behaviour)', async () => {
    const create = setStatus('unavailable', vi.fn())
    const onChange = vi.fn()
    render(<FileRefField value="" onChange={onChange} accept={['.yaml']} onEditCode={vi.fn()} />)
    fireEvent.click(screen.getByRole('button', { name: /new/i }))
    await Promise.resolve()
    expect(create).not.toHaveBeenCalled()
    expect(onChange).not.toHaveBeenCalled()
  })

  it('surfaces the spec placeholder on the input, falling back to the accept list', () => {
    render(
      <FileRefField
        value=""
        onChange={vi.fn()}
        accept={['.yaml', '.yml']}
        placeholder="schemas/customer_dim.yaml"
        onEditCode={vi.fn()}
      />,
    )
    expect(screen.getByRole('textbox')).toHaveAttribute('placeholder', 'schemas/customer_dim.yaml')
  })

  it('skips the existence check for a substitution token and stays free-text', () => {
    const onChange = vi.fn()
    render(
      <FileRefField value="${x}/f.sql" onChange={onChange} accept={['.sql']} onEditCode={vi.fn()} />,
    )
    // companionCheckablePath returns null for tokens → hook queried with null.
    expect(useCompanionFileMock).toHaveBeenCalledWith(null)
    // No existence-driven UI.
    expect(screen.queryByRole('button', { name: /edit file/i })).toBeNull()
    expect(screen.queryByText(/doesn't exist yet/i)).toBeNull()
    // The token value is shown in a free-text, typeable input.
    const input = screen.getByRole('textbox')
    expect(input).toHaveValue('${x}/f.sql')
    fireEvent.change(input, { target: { value: '${x}/g.sql' } })
    expect(onChange).toHaveBeenCalledWith('${x}/g.sql')
  })
})
