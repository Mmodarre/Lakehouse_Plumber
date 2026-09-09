import { useState } from 'react'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import type { TemplateAuthoringParameter } from '@/api/template-authoring'
import { missingTemplateParameters } from '@/lib/template-invocation'
import { TemplateInvocationParams } from '../TemplateInvocationParams'

const param: TemplateAuthoringParameter = { name: 'value', required: true, has_default: true, default: false, declared_type: 'boolean' }
function Harness({ parameter = param }: { parameter?: TemplateAuthoringParameter }) {
  const [values, setValues] = useState<Record<string, unknown>>({})
  return <><TemplateInvocationParams params={[parameter]} values={values} onSet={(name, value) => setValues({ ...values, [name]: value })} onUnset={(name) => { const next = { ...values }; delete next[name]; setValues(next) }} /><output>{JSON.stringify(values)}</output></>
}
describe('template invocation parameter presence', () => {
  it('requires an explicit key even when the declaration has a default', async () => {
    const user = userEvent.setup()
    render(<Harness />)
    expect(missingTemplateParameters([param], {})).toEqual(['value'])
    await user.click(screen.getByRole('button', { name: 'Supply declared default' }))
    expect(screen.getByRole('status')).toHaveTextContent('{"value":false}')
    expect(missingTemplateParameters([param], { value: false })).toEqual([])
    await user.click(screen.getByRole('button', { name: 'Omit value' }))
    expect(screen.getByRole('status')).toHaveTextContent('{}')
  })
  it.each([null, false, 0, '', [], {}])('treats the explicit value %j as supplied', (value) => {
    expect(missingTemplateParameters([param], { value })).toEqual([])
  })
  it('preserves an explicit null default separately from no default', async () => {
    const user = userEvent.setup()
    render(<Harness parameter={{ ...param, default: null }} />)
    await user.click(screen.getByRole('button', { name: 'Supply declared default' }))
    expect(screen.getByRole('status')).toHaveTextContent('{"value":null}')
    expect(screen.getByText(/The key is present/)).toBeInTheDocument()
  })
})
