import { describe, expect, it, vi } from 'vitest'
import type * as Monaco from 'monaco-editor'
vi.mock('../../api/help', () => ({loadHelpCached: vi.fn()}))
import { loadHelpCached } from '../../api/help'
import { registerFieldHelp } from '../monaco-field-help'

function setup() {
  let provider: Monaco.languages.HoverProvider
  let version = 1
  const source = 'actions:\n  - name: load\n    type: load\n    source:\n      type: delta\n      schema: bronze\n'
  const monaco = {languages: {registerHoverProvider: vi.fn((_lang, value) => {provider = value; return {dispose: vi.fn()}})}, Range: class {}} as unknown as typeof Monaco
  registerFieldHelp(monaco)
  const model = {uri: {path: '/pipelines/orders.yaml'}, getValue: () => source, getVersionId: () => version, getOffsetAt: () => source.indexOf('schema:') + 2, getPositionAt: () => ({lineNumber: 7, column: 7}), isDisposed: () => false} as unknown as Monaco.editor.ITextModel
  return {hover: () => provider.provideHover(model, {lineNumber: 7, column: 7} as Monaco.Position, {isCancellationRequested: false} as Monaco.CancellationToken, {} as Monaco.languages.HoverContext), edit: () => version++}
}
describe('Monaco field help', () => {
  it('discards a help response if the source changed while loading', async () => {
    let finish!: (value: {version: 1; entries: []}) => void
    vi.mocked(loadHelpCached).mockReturnValue(new Promise(resolve => {finish = resolve}))
    const editor = setup(), pending = editor.hover()
    editor.edit()
    finish({version: 1, entries: []})
    expect(await pending).toBeUndefined()
  })
  it('falls back to the YAML service when the local help request fails', async () => {
    vi.mocked(loadHelpCached).mockRejectedValue(new Error('offline'))
    expect(await setup().hover()).toBeUndefined()
  })
})
