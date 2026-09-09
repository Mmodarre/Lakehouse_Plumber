import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
vi.mock('../../../api/help', () => ({loadHelpCached: vi.fn()}))
vi.mock('../../../api/schemas', () => ({loadSchemaCached: vi.fn()}))
import { loadHelpCached } from '../../../api/help'
import { loadSchemaCached } from '../../../api/schemas'
import { SchemaKindProvider } from '../SchemaKindContext'
import { FieldHelp } from '../FieldHelp'
import { OptionalTextField } from '../../config/fields/OptionalTextField'

beforeEach(() => {
  vi.mocked(loadSchemaCached).mockResolvedValue({definitions: {Action: {properties: {source: {properties: {schema: {description: 'Schema fallback guidance.'}}}}}}})
})
function form(subtype: string, client: QueryClient, change = vi.fn()) {
  return <QueryClientProvider client={client}><SchemaKindProvider kind="flowgroup" subtype={subtype}><OptionalTextField id="schema" label="Schema" helpPath={['source','schema']} value="bronze" description="Loading fallback." issue="A sample issue." onSet={change} onUnset={change} /></SchemaKindProvider></QueryClientProvider>
}
describe('help content in actual fields', () => {
  it('loads related guidance from its category only after help is opened', async () => {
    vi.mocked(loadHelpCached).mockClear()
    vi.mocked(loadHelpCached).mockResolvedValue({version:1,entries:[{id:'project.wheel.artifact_volume',summary:'Choose a Unity Catalog volume for wheel artifacts.',details:['Provide a /Volumes/catalog/schema/volume path.'],sources:[{file:'docs/reference/config/project.rst'}],bindings:[{path:['wheel','artifact_volume']}]}]})
    const user = userEvent.setup()
    render(<FieldHelp label="Packaging" help={{summary:'Choose how pipeline code is shipped.',relatedHelpIds:['project.wheel.artifact_volume']}} />)
    expect(loadHelpCached).not.toHaveBeenCalled()
    await user.click(screen.getByRole('button',{name:'More info about Packaging'}))
    await user.click(await screen.findByText('Choose a Unity Catalog volume for wheel artifacts.'))
    expect(screen.getByText('Provide a /Volumes/catalog/schema/volume path.')).toBeVisible()
    expect(loadHelpCached).toHaveBeenCalledWith('project')
  })

  it('refreshes an open help panel when the action subtype changes', async () => {
    vi.mocked(loadHelpCached).mockResolvedValue({version: 1, entries: [
      {id:'delta',summary:'Choose the source catalog schema.',sources:[],bindings:[{path:['source','schema'],subtype:'load:delta'}]},
      {id:'cloud',summary:'Choose an Auto Loader schema file.',sources:[],bindings:[{path:['source','schema'],subtype:'load:cloudfiles'}]},
    ]})
    const client = new QueryClient({defaultOptions:{queries:{retry:false}}}), user = userEvent.setup()
    const {rerender} = render(form('load:delta',client))
    await screen.findByText('Choose the source catalog schema.')
    await user.click(screen.getByRole('button',{name:'More info about Schema'}))
    expect(await screen.findByRole('dialog')).toHaveTextContent('Choose the source catalog schema.')
    rerender(form('load:cloudfiles',client))
    expect(screen.getByRole('dialog')).toHaveTextContent('Choose an Auto Loader schema file.')
  })
  it('keeps schema fallback, input hints and errors usable when the catalog fails', async () => {
    vi.mocked(loadHelpCached).mockRejectedValue(new Error('catalog unavailable'))
    const change = vi.fn(), client = new QueryClient({defaultOptions:{queries:{retry:false}}})
    render(form('load:delta',client,change))
    const input = screen.getByRole('textbox',{name:'Schema'})
    await waitFor(()=>expect(screen.getByText('Schema fallback guidance.')).toBeVisible())
    const descriptions = input.getAttribute('aria-describedby')!.split(' ').map(id=>document.getElementById(id)?.textContent).join(' ')
    expect(descriptions).toContain('Schema fallback guidance.')
    expect(descriptions).toContain('A sample issue.')
    await userEvent.setup().type(input,'_new{Enter}')
    expect(change).toHaveBeenCalledWith('bronze_new')
  })
})
