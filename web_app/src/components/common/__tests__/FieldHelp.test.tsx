import { describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { FieldHelp } from '../FieldHelp'
import { SectionCard } from '../../config/SectionCard'
import { ConfigEditingContext } from '../../config/shared/configEditingContext'
import { OptionalTextField } from '../../config/fields/OptionalTextField'
import { BoolSwitch } from '../../config/fields/BoolSwitch'
import { KeyValueMapEditor } from '../../config/fields/KeyValueMapEditor'

const help = { summary: 'Choose how pipeline code is shipped.', details: ['Wheel packaging needs an artifact volume.'], unsetBehavior: 'Inherit project defaults.', examples: [{label: 'Wheel example', yaml: 'packaging: wheel'}], sources: [{file: 'docs/reference/config/bundle.rst'}] }

describe('FieldHelp', () => {
  it('renders no control without content', () => {
    const { rerender } = render(<FieldHelp />)
    expect(screen.queryByRole('button')).toBeNull()
    rerender(<FieldHelp text="" />)
    expect(screen.queryByRole('button')).toBeNull()
  })
  it('opens by keyboard, copies examples, and restores focus after Escape', async () => {
    const user = userEvent.setup()
    render(<FieldHelp label="Packaging" help={help} />)
    const trigger = screen.getByRole('button', {name: 'More info about Packaging'})
    await user.tab()
    expect(trigger).toHaveFocus()
    expect(screen.queryByRole('dialog')).toBeNull()
    await user.keyboard('{Enter}')
    expect(await screen.findByRole('dialog', {name: 'Packaging'})).toHaveTextContent(help.summary)
    expect(await screen.findByText(help.details[0])).toBeVisible()
    expect(screen.getByRole('link')).toHaveAttribute('href', 'https://lakehouse-plumber.readthedocs.io/en/latest/reference/config/bundle.html')
    await user.click(screen.getByRole('button', {name: 'Copy example'}))
    expect(await screen.findByRole('status')).toHaveTextContent('Example copied.')
    expect(await navigator.clipboard.readText()).toBe('packaging: wheel')
    expect(screen.getByRole('dialog')).toBeVisible()
    await user.keyboard('{Escape}')
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
    expect(trigger).toHaveFocus()
  })
  it('supports pointer activation and explicit close', async () => {
    const user = userEvent.setup()
    render(<FieldHelp label="Catalog" text="Choose the output catalog." />)
    const trigger = screen.getByRole('button', {name: 'More info about Catalog'})
    await user.click(trigger)
    expect(await screen.findByRole('dialog')).toHaveTextContent('Choose the output catalog.')
    await user.click(screen.getByRole('button', {name: 'Close field help'}))
    expect(trigger).toHaveFocus()
  })
  it('keeps help keyboard-accessible in the real viewer SectionCard without enabling edits', async () => {
    const user = userEvent.setup(), change = vi.fn()
    render(<ConfigEditingContext.Provider value={true}><SectionCard title="Settings">
      <OptionalTextField id="catalog" label="Catalog" help="Choose the output catalog." value="main" onSet={change} onUnset={change} />
      <BoolSwitch id="serverless" label="Serverless" value={true} defaultValue={true} onSet={change} onReset={change} />
      <KeyValueMapEditor id="tags" label="Tags" value={{owner: 'team'}} onSetEntry={change} onRenameEntry={change} onRemoveEntry={change} onDeleteKey={change} />
    </SectionCard></ConfigEditingContext.Provider>)
    const field = screen.getByRole('textbox', {name: 'Catalog'})
    expect(field).toBeDisabled()
    expect(screen.getByRole('switch', {name: 'Serverless'})).toBeDisabled()
    await user.type(field, 'new')
    await user.click(screen.getByRole('button', {name: 'Add Tags entry'}))
    await user.click(screen.getByRole('button', {name: 'Remove owner'}))
    const trigger = screen.getByRole('button', {name: 'More info about Catalog'})
    await user.tab()
    if (document.activeElement !== trigger) await user.tab()
    expect(trigger).toHaveFocus()
    await user.keyboard(' ')
    expect(await screen.findByRole('dialog', {name: 'Catalog'})).toHaveTextContent('Choose the output catalog.')
    await user.keyboard('{Escape}')
    expect(trigger).toHaveFocus()
    expect(change).not.toHaveBeenCalled()
    expect(field).toHaveValue('main')
  })
})
