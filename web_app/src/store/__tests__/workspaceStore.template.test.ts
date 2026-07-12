import { describe, expect, it, vi } from 'vitest'

// Same fresh-module pattern as workspaceStore.test.ts: the store persists to
// localStorage and runs a hydration fixup at import, so each scenario loads a
// clean copy.
async function loadStore() {
  vi.resetModules()
  localStorage.clear()
  return import('@/store/workspaceStore')
}

const TPL = 'templates/csv_ingestion_template.yaml'

describe('workspaceStore — template designer tabs', () => {
  it('opens a template designer tab keyed by file path, with docKind template', async () => {
    const { useWorkspaceStore, designerTemplateTabId } = await loadStore()
    useWorkspaceStore.getState().openDesignerTemplateTab('csv_ingestion_template', TPL)

    const s = useWorkspaceStore.getState()
    const id = designerTemplateTabId(TPL)
    expect(s.activePath).toBe(id)
    expect(s.tabs).toHaveLength(1)
    const tab = s.tabs[0]
    expect(tab.kind).toBe('designer')
    if (tab.kind !== 'designer') throw new Error('expected designer tab')
    expect(tab.docKind).toBe('template')
    expect(tab.pipeline).toBe('')
    expect(tab.flowgroup).toBe('csv_ingestion_template')
    expect(tab.filePath).toBe(TPL)
  })

  it('re-opening focuses the existing tab (no duplicate) and refreshes the name', async () => {
    const { useWorkspaceStore, designerTemplateTabId } = await loadStore()
    const store = useWorkspaceStore.getState()
    store.openDesignerTemplateTab('old_name', TPL)
    store.setActive(null)
    store.openDesignerTemplateTab('new_name', TPL)

    const s = useWorkspaceStore.getState()
    expect(s.tabs).toHaveLength(1)
    expect(s.activePath).toBe(designerTemplateTabId(TPL))
    const tab = s.tabs[0]
    if (tab.kind !== 'designer') throw new Error('expected designer tab')
    expect(tab.flowgroup).toBe('new_name')
  })

  it('a template tab id never collides with a flowgroup designer tab id', async () => {
    const { designerTabId, designerTemplateTabId } = await loadStore()
    expect(designerTemplateTabId(TPL)).not.toBe(designerTabId('', 'csv_ingestion_template'))
  })
})
