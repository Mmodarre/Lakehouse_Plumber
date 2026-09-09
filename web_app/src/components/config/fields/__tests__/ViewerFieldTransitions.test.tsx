import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { ConfigEditingContext } from '../../shared/configEditingContext'
import { DraftInput } from '../DraftInput'
import { OptionalNumberField } from '../OptionalNumberField'
import { ScheduleEditor } from '../../job/ScheduleEditor'
import { GroupMembershipEditor } from '../../pipeline/GroupMembershipEditor'
import { installRadixStubs } from '../../shared/__tests__/configFormTestSupport'
vi.mock('@/hooks/useEnvironments', () => ({ useEnvironmentResolved: () => ({data: {tokens: {catalog: 'main'}}}) }))
vi.mock('@/hooks/usePipelines', () => ({ usePipelines: () => ({data: {pipelines: [{name: 'new_pipeline'}]}}) }))
const viewer = (readOnly: boolean, children: ReactNode) => <ConfigEditingContext.Provider value={readOnly}>{children}</ConfigEditingContext.Provider>
beforeEach(installRadixStubs)

describe('viewer field transitions', () => {
  it.each([{multiline: false, tokenComplete: false}, {multiline: true, tokenComplete: false}, {multiline: false, tokenComplete: true}, {multiline: true, tokenComplete: true}])('blocks pending commits for draft variant %o', props => {
    const commit = vi.fn(), field = <DraftInput {...props} initial="saved" onCommit={commit} aria-label="Draft" disabled={false} />
    const {rerender} = render(viewer(false, field))
    const input = screen.getByLabelText('Draft')
    fireEvent.change(input, {target: {value: 'pending'}})
    rerender(viewer(true, field))
    expect(input).toBeDisabled()
    fireEvent.blur(input)
    fireEvent.keyDown(input, {key: 'Enter'})
    expect(commit).not.toHaveBeenCalled()
    if (props.tokenComplete) expect(screen.getByRole('button', {name: 'Insert token'})).toBeDisabled()
  })
  it('closes token completion and group membership portals when viewer mode starts', async () => {
    const commit = vi.fn(), user = userEvent.setup()
    const token = <DraftInput initial="" onCommit={commit} aria-label="Draft" tokenComplete />
    const {rerender, unmount} = render(viewer(false, token))
    await user.click(screen.getByRole('button', {name: 'Insert token'}))
    expect(screen.getByRole('listbox')).toBeVisible()
    rerender(viewer(true, token))
    expect(screen.queryByRole('listbox')).toBeNull()
    expect(commit).not.toHaveBeenCalled()
    unmount()
    const group = <GroupMembershipEditor id="group" members={[]} duplicates={new Set()} onAdd={commit} onRemove={commit} />
    const next = render(viewer(false, group))
    await user.click(screen.getByRole('combobox', {name: 'Add pipeline to group'}))
    expect(await screen.findByRole('listbox')).toBeVisible()
    next.rerender(viewer(true, group))
    expect(screen.queryByRole('listbox')).toBeNull()
    expect(screen.getByRole('combobox', {name: 'Add pipeline to group'})).toBeDisabled()
    expect(commit).not.toHaveBeenCalled()
  })
  it('blocks pending numeric and timezone commits after switching to viewer mode', () => {
    const change = vi.fn()
    const content = <><OptionalNumberField id="number" label="Concurrency" value={1} onSet={change} onUnset={change} /><ScheduleEditor idPrefix="job" api={{docIndex:0,base:[],settings:{schedule:{timezone_id:'UTC'}},set:change,del:change,issueAt:()=>undefined}} /></>
    const {rerender} = render(viewer(false, content))
    const number = screen.getByRole('textbox', {name:'Concurrency'}), timezone = screen.getByLabelText('Time zone')
    fireEvent.change(number, {target:{value:'2'}})
    fireEvent.change(timezone, {target:{value:'Australia/Melbourne'}})
    rerender(viewer(true, content))
    for (const field of [number,timezone]) { expect(field).toBeDisabled(); fireEvent.blur(field) }
    expect(change).not.toHaveBeenCalled()
  })
})
