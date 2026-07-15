import { useState } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'

// Both slots opt into `${env}`-token autocomplete (Task 1.4), so the field
// tree renders TokenAutocomplete under the hood. Mock its two hook seams —
// the react-query env resolver and the ui store — exactly as
// TokenAutocomplete's own suite does, so no provider or network is needed.
vi.mock('@/hooks/useEnvironments', () => ({
  useEnvironmentResolved: vi.fn(),
}))
vi.mock('@/store/uiStore', () => ({
  useUIStore: (sel: (s: { selectedEnv: string }) => unknown) => sel({ selectedEnv: 'dev' }),
}))

import { useEnvironmentResolved } from '@/hooks/useEnvironments'
import { DualSourceField } from '../DualSourceField'

const resolvedMock = useEnvironmentResolved as unknown as ReturnType<typeof vi.fn>

beforeEach(() => {
  vi.clearAllMocks()
  resolvedMock.mockReturnValue({ data: { tokens: {} } })
})

// Controlled harness — DualSourceField is controlled, so thread `onChange`
// back into `value`. The returned spy records every emitted value.
function renderField(initial: unknown) {
  const onChange = vi.fn()
  function Harness() {
    const [value, setValue] = useState<unknown>(initial)
    return (
      <DualSourceField
        value={value}
        onChange={(next) => {
          onChange(next)
          setValue(next)
        }}
      />
    )
  }
  render(<Harness />)
  return { onChange }
}

describe('DualSourceField', () => {
  it('prefills two labeled inputs from value[0] / value[1]', () => {
    renderField(['a', 'b'])
    expect(screen.getByLabelText('Table A')).toHaveValue('a')
    expect(screen.getByLabelText('Table B')).toHaveValue('b')
  })

  it('editing slot 1 writes the whole 2-item array', async () => {
    const { onChange } = renderField(['a', 'b'])
    const user = userEvent.setup()
    const tableB = screen.getByLabelText('Table B')

    await user.clear(tableB)
    await user.type(tableB, 'c')
    await user.tab()

    expect(onChange).toHaveBeenLastCalledWith(['a', 'c'])
  })

  it('clearing BOTH slots prunes the key (onChange undefined)', async () => {
    const { onChange } = renderField(['a', 'b'])
    const user = userEvent.setup()

    await user.clear(screen.getByLabelText('Table A'))
    await user.tab()
    await user.clear(screen.getByLabelText('Table B'))
    await user.tab()

    expect(onChange).toHaveBeenLastCalledWith(undefined)
  })

  it('a non-2 (legacy) array renders both slots without throwing', () => {
    renderField(['a', 'b', 'c'])
    expect(screen.getByLabelText('Table A')).toHaveValue('a')
    expect(screen.getByLabelText('Table B')).toHaveValue('b')
  })
})
