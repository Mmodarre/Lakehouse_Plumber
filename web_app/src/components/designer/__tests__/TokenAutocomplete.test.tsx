import { useState } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'

// Env tokens are mocked at the hook boundary (no react-query provider, no
// network). The current env comes from the ui store — stub it to 'dev' and
// apply the selector the component passes, so `useUIStore((s) => s.selectedEnv)`
// resolves to a string.
vi.mock('@/hooks/useEnvironments', () => ({
  useEnvironmentResolved: vi.fn(),
}))
vi.mock('@/store/uiStore', () => ({
  useUIStore: (sel: (s: { selectedEnv: string }) => unknown) => sel({ selectedEnv: 'dev' }),
}))

import { useEnvironmentResolved } from '@/hooks/useEnvironments'
import { TokenAutocomplete } from '../TokenAutocomplete'
import { DraftInput } from '@/components/config/fields/DraftInput'

const resolvedMock = useEnvironmentResolved as unknown as ReturnType<typeof vi.fn>

beforeEach(() => {
  vi.clearAllMocks()
  resolvedMock.mockReturnValue({ data: { tokens: { bronze_schema: 'acme_bronze' } } })
})

// Controlled harness — mirrors DraftInput threading the draft back in, so the
// input value and caret behave as they do in the real seam.
function Harness(props: {
  initial?: string
  onValueChange?: (v: string) => void
  onCommit?: () => void
  onRevert?: () => void
}) {
  const [value, setValue] = useState(props.initial ?? '')
  return (
    <TokenAutocomplete
      value={value}
      onValueChange={(v) => {
        setValue(v)
        props.onValueChange?.(v)
      }}
      onCommit={props.onCommit ?? vi.fn()}
      onRevert={props.onRevert ?? vi.fn()}
      aria-label="value"
    />
  )
}

describe('TokenAutocomplete', () => {
  it('opens a popover listing env tokens with their resolved values when `$` is typed', () => {
    render(<Harness />)
    const input = screen.getByLabelText('value')

    fireEvent.change(input, { target: { value: '$' } })

    expect(screen.getByRole('listbox')).toBeInTheDocument()
    // Token shown as its `${...}` form alongside the resolved value preview.
    expect(screen.getByText('${bronze_schema}')).toBeInTheDocument()
    expect(screen.getByText('acme_bronze')).toBeInTheDocument()
  })

  it('inserts `${name}` at the caret and reports it via onValueChange', () => {
    const onValueChange = vi.fn()
    render(<Harness onValueChange={onValueChange} />)
    const input = screen.getByLabelText('value')

    fireEvent.change(input, { target: { value: '$' } })
    fireEvent.click(screen.getByText('${bronze_schema}'))

    expect(onValueChange).toHaveBeenLastCalledWith(expect.stringContaining('${bronze_schema}'))
    expect(input).toHaveValue('${bronze_schema}')
    // The completion closes the popover.
    expect(screen.queryByRole('listbox')).toBeNull()
  })

  it('filters tokens by the text typed after `${`', () => {
    resolvedMock.mockReturnValue({
      data: { tokens: { bronze_schema: 'acme_bronze', silver_schema: 'acme_silver' } },
    })
    render(<Harness />)
    const input = screen.getByLabelText('value')

    fireEvent.change(input, { target: { value: '${bro' } })

    expect(screen.getByText('${bronze_schema}')).toBeInTheDocument()
    expect(screen.queryByText('${silver_schema}')).toBeNull()
  })

  it('is keyboard navigable: ArrowDown + Enter selects, and offers a static secret insert', () => {
    const onValueChange = vi.fn()
    render(<Harness onValueChange={onValueChange} />)
    const input = screen.getByLabelText('value')

    fireEvent.change(input, { target: { value: '$' } })
    // First option is the env token; the static secret affordance is last.
    fireEvent.keyDown(input, { key: 'ArrowDown' })
    fireEvent.keyDown(input, { key: 'Enter' })

    expect(onValueChange).toHaveBeenLastCalledWith(
      expect.stringContaining('${secret:scope/key}'),
    )
  })

  it('Escape closes the popover without reverting the draft', () => {
    const onRevert = vi.fn()
    render(<Harness onRevert={onRevert} />)
    const input = screen.getByLabelText('value')

    fireEvent.change(input, { target: { value: '$' } })
    expect(screen.getByRole('listbox')).toBeInTheDocument()

    fireEvent.keyDown(input, { key: 'Escape' })
    expect(screen.queryByRole('listbox')).toBeNull()
    // Escape-while-open belongs to the popover, not DraftInput's revert.
    expect(onRevert).not.toHaveBeenCalled()
  })

  it('exposes a real, keyboard-focusable token trigger button that opens the popover', () => {
    render(<Harness />)
    const button = screen.getByRole('button', { name: /insert token/i })

    expect(button.tagName).toBe('BUTTON')
    expect(button).toHaveAttribute('type', 'button')
    button.focus()
    expect(button).toHaveFocus()

    fireEvent.click(button)
    expect(screen.getByRole('listbox')).toBeInTheDocument()
  })
})

describe('DraftInput tokenComplete wiring', () => {
  it('is opt-in: selecting a token updates the draft and commits on blur, not before', () => {
    const onCommit = vi.fn()
    render(<DraftInput initial="" onCommit={onCommit} tokenComplete aria-label="value" />)
    const input = screen.getByLabelText('value')

    fireEvent.change(input, { target: { value: '$' } })
    fireEvent.click(screen.getByText('${bronze_schema}'))

    // Draft reflects the insertion, but nothing has committed yet.
    expect(input).toHaveValue('${bronze_schema}')
    expect(onCommit).not.toHaveBeenCalled()

    fireEvent.blur(input)
    expect(onCommit).toHaveBeenCalledWith('${bronze_schema}')
  })
})
