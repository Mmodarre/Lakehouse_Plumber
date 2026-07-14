import { describe, expect, it, vi } from 'vitest'

// layoutStore persists to localStorage, so each scenario imports a fresh copy
// of the module (same fresh-module pattern as themeStore.test.ts /
// workspaceStore.test.ts).
async function loadStore(opts: { keepStorage?: boolean } = {}) {
  vi.resetModules()
  if (!opts.keepStorage) localStorage.clear()
  return import('@/store/layoutStore')
}

describe('layoutStore — defaults', () => {
  it('boots with the §3 geometry defaults', async () => {
    const { useLayoutStore } = await loadStore()
    const s = useLayoutStore.getState()
    expect(s.explorerWidth).toBe(260)
    expect(s.explorerLens).toBe('files')
    expect(s.explorerCollapsed).toBe(false)
    expect(s.inspectorWidth).toBe(300)
    expect(s.inspectorCollapsed).toBe(false)
    expect(s.inspectorTab).toBe('validation')
    expect(s.assistantOpen).toBe(false)
    expect(s.assistantWidth).toBe(320)
    expect(s.bottomHeight).toBe(240)
    expect(s.bottomCollapsed).toBe(true)
    expect(s.bottomTab).toBe('problems')
    expect(s.viewerMode).toBe(false)
  })
})

describe('layoutStore — setters', () => {
  it('each plain setter updates only its field', async () => {
    const { useLayoutStore } = await loadStore()
    const st = () => useLayoutStore.getState()
    st().setExplorerWidth(300)
    st().setExplorerLens('tables')
    st().setExplorerCollapsed(true)
    st().setInspectorWidth(360)
    st().setInspectorCollapsed(true)
    st().setInspectorTab('help')
    st().setAssistantOpen(true)
    st().setAssistantWidth(420)
    st().setBottomHeight(180)
    st().setBottomCollapsed(false)
    st().setBottomTab('run')
    st().setViewerMode(true)
    const s = st()
    expect(s.explorerWidth).toBe(300)
    expect(s.explorerLens).toBe('tables')
    expect(s.explorerCollapsed).toBe(true)
    expect(s.inspectorWidth).toBe(360)
    expect(s.inspectorCollapsed).toBe(true)
    expect(s.inspectorTab).toBe('help')
    expect(s.assistantOpen).toBe(true)
    expect(s.assistantWidth).toBe(420)
    expect(s.bottomHeight).toBe(180)
    expect(s.bottomCollapsed).toBe(false)
    expect(s.bottomTab).toBe('run')
    expect(s.viewerMode).toBe(true)
  })

  it('toggles flip their boolean', async () => {
    const { useLayoutStore } = await loadStore()
    const st = () => useLayoutStore.getState()
    st().toggleExplorer()
    expect(st().explorerCollapsed).toBe(true)
    st().toggleInspector()
    expect(st().inspectorCollapsed).toBe(true)
    st().toggleAssistant()
    expect(st().assistantOpen).toBe(true)
    st().toggleBottom()
    expect(st().bottomCollapsed).toBe(false)
    st().toggleViewerMode()
    expect(st().viewerMode).toBe(true)
    // Toggling back returns to the default.
    st().toggleExplorer()
    expect(st().explorerCollapsed).toBe(false)
  })
})

describe('layoutStore — persistence', () => {
  it('persists exactly the geometry slice (no functions leak)', async () => {
    const { useLayoutStore } = await loadStore()
    const { partialize } = useLayoutStore.persist.getOptions()
    const slice = partialize!(useLayoutStore.getState())
    expect(slice).toEqual({
      explorerWidth: 260,
      explorerLens: 'files',
      explorerCollapsed: false,
      inspectorWidth: 300,
      inspectorCollapsed: false,
      inspectorTab: 'validation',
      assistantOpen: false,
      assistantWidth: 320,
      bottomHeight: 240,
      bottomCollapsed: true,
      bottomTab: 'problems',
      viewerMode: false,
    })
  })

  it('round-trips geometry through localStorage on a fresh boot', async () => {
    const first = await loadStore()
    first.useLayoutStore.getState().setExplorerWidth(288)
    first.useLayoutStore.getState().setExplorerLens('files')
    first.useLayoutStore.getState().setAssistantOpen(true)

    const second = await loadStore({ keepStorage: true })
    const s = second.useLayoutStore.getState()
    expect(s.explorerWidth).toBe(288)
    expect(s.explorerLens).toBe('files')
    expect(s.assistantOpen).toBe(true)
    // Untouched fields keep their defaults.
    expect(s.inspectorWidth).toBe(300)
  })
})

describe('layoutStore — persist migrate (v0 → v1 lens reset)', () => {
  it('resets explorerLens to files for a pre-version (v0) session, keeping other geometry', async () => {
    localStorage.clear()
    localStorage.setItem(
      'lhp-layout',
      JSON.stringify({
        state: {
          explorerWidth: 288,
          explorerLens: 'structure',
          explorerCollapsed: false,
          inspectorWidth: 340,
          inspectorCollapsed: false,
          inspectorTab: 'help',
          assistantOpen: true,
          assistantWidth: 400,
          bottomHeight: 200,
          bottomCollapsed: false,
          bottomTab: 'run',
          viewerMode: true,
        },
        version: 0,
      }),
    )
    const { useLayoutStore } = await loadStore({ keepStorage: true })
    const s = useLayoutStore.getState()
    // The lens is reset to the new default ONCE for a pre-version session…
    expect(s.explorerLens).toBe('files')
    // …every other persisted field is preserved through the migration.
    expect(s.explorerWidth).toBe(288)
    expect(s.inspectorWidth).toBe(340)
    expect(s.inspectorTab).toBe('help')
    expect(s.assistantOpen).toBe(true)
    expect(s.bottomTab).toBe('run')
    expect(s.viewerMode).toBe(true)
  })

  it('respects an explicit lens choice once the session is already at v1 (lens migrate no-ops)', async () => {
    localStorage.clear()
    localStorage.setItem(
      'lhp-layout',
      JSON.stringify({
        state: { explorerLens: 'structure', explorerWidth: 300 },
        version: 1,
      }),
    )
    const { useLayoutStore } = await loadStore({ keepStorage: true })
    const s = useLayoutStore.getState()
    // The v1 → v2 migrate runs but only touches a persisted inspectorTab
    // 'action'; the user's lens choice survives (only the one-time v0 → v1 hop
    // resets the lens).
    expect(s.explorerLens).toBe('structure')
    expect(s.explorerWidth).toBe(300)
  })
})

describe('layoutStore — persist migrate (v1 → v2 inspectorTab normalize)', () => {
  it('heals a persisted inspectorTab "action" to "validation", keeping other geometry', async () => {
    localStorage.clear()
    localStorage.setItem(
      'lhp-layout',
      JSON.stringify({
        state: {
          explorerLens: 'files',
          explorerWidth: 300,
          inspectorTab: 'action',
          inspectorWidth: 340,
        },
        version: 1,
      }),
    )
    const { useLayoutStore } = await loadStore({ keepStorage: true })
    const s = useLayoutStore.getState()
    // The retired Action tab is normalized to Validation (Fix #3)…
    expect(s.inspectorTab).toBe('validation')
    // …and unrelated geometry survives the migration.
    expect(s.explorerLens).toBe('files')
    expect(s.explorerWidth).toBe(300)
    expect(s.inspectorWidth).toBe(340)
  })

  it('leaves a non-action inspectorTab untouched across the v1 → v2 hop', async () => {
    localStorage.clear()
    localStorage.setItem(
      'lhp-layout',
      JSON.stringify({
        state: { inspectorTab: 'help' },
        version: 1,
      }),
    )
    const { useLayoutStore } = await loadStore({ keepStorage: true })
    expect(useLayoutStore.getState().inspectorTab).toBe('help')
  })
})
