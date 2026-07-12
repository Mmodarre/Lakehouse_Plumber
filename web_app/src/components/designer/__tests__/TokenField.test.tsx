import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { TokenField } from '../TokenField'

function noop() {}

describe('TokenField variants', () => {
  it('param variant: a pure {{ x }} reads as bound to that parameter', () => {
    render(
      <TokenField
        id="f"
        label="Target"
        value="{{ table_name }}"
        onSet={noop}
        onUnset={noop}
        variant="param"
      />,
    )
    expect(screen.getByText('Bound to template parameter table_name.')).toBeDefined()
    // The chip is labelled with the bound parameter name.
    expect(screen.getByText('table_name')).toBeDefined()
    expect(screen.getByDisplayValue('{{ table_name }}')).toBeDefined()
  })

  it('param variant: a mixed value lists every referenced parameter', () => {
    render(
      <TokenField
        id="f"
        label="Path"
        value="/land/{{ folder }}/{{ table_name }}"
        onSet={noop}
        onUnset={noop}
        variant="param"
      />,
    )
    expect(screen.getByText('param')).toBeDefined()
    expect(
      screen.getByText('References template parameters: folder, table_name.'),
    ).toBeDefined()
  })

  it('substitution variant (default): amber token badge + advisory', () => {
    render(<TokenField id="f" label="Files" value="${max_files}" onSet={noop} onUnset={noop} />)
    expect(screen.getByText('token')).toBeDefined()
    expect(screen.getByText('Holds a substitution token — resolved at generate time.')).toBeDefined()
  })
})
