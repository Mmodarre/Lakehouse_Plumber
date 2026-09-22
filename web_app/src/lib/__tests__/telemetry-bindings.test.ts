import { describe, expect, it, vi } from 'vitest'
import type { WorkspaceTabRef } from '@/store/workspaceStore'

// Store bindings: which zustand transitions become a `surface.action` and,
// just as important, which do not. The client is mocked so only the mapping
// is under test; every store is re-imported per scenario (persisted stores
// hydrate from localStorage at creation, so a fresh module is a clean store).

vi.mock('../telemetry', () => ({ track: vi.fn() }))

async function load() {
  vi.resetModules()
  localStorage.clear()
  const [{ track }, bindings, ws, layout, ui] = await Promise.all([
    import('../telemetry'),
    import('../telemetry-bindings'),
    import('@/store/workspaceStore'),
    import('@/store/layoutStore'),
    import('@/store/uiStore'),
  ])
  // The mocked client outlives the module registry; start each scenario clean.
  vi.mocked(track).mockClear()
  bindings.installTelemetryBindings()
  return {
    track: vi.mocked(track),
    surfaceForTab: bindings.surfaceForTab,
    installTelemetryBindings: bindings.installTelemetryBindings,
    workspace: ws.useWorkspaceStore,
    layout: layout.useLayoutStore,
    ui: ui.useUIStore,
    entityTabId: ws.entityTabId,
  }
}

describe('surfaceForTab', () => {
  const cases: [WorkspaceTabRef, string][] = [
    [{ kind: 'file', path: 'pipelines/x.sql' }, 'file_editor'],
    [
      { kind: 'entity', pipeline: 'p', flowgroup: 'f', filePath: 'pipelines/p/f.yaml', docKind: 'flowgroup', view: 'graph' },
      'flowgroup_graph',
    ],
    [
      { kind: 'entity', pipeline: 'p', flowgroup: 'f', filePath: 'pipelines/p/f.yaml', docKind: 'flowgroup', view: 'code' },
      'flowgroup_code',
    ],
    [
      { kind: 'entity', pipeline: '', flowgroup: 't', filePath: 'templates/t.yaml', docKind: 'template', view: 'builder' },
      'template_builder',
    ],
    [
      { kind: 'entity', pipeline: '', flowgroup: 't', filePath: 'templates/t.yaml', docKind: 'template', view: 'preview' },
      'template_preview',
    ],
    [
      { kind: 'entity', pipeline: '', flowgroup: 't', filePath: 'templates/t.yaml', docKind: 'template', view: 'graph' },
      'template_builder',
    ],
    [
      { kind: 'entity', pipeline: 'p', flowgroup: 'f', filePath: 'pipelines/p/f.yaml', docKind: 'flowgroup', view: 'preview' },
      'flowgroup_graph',
    ],
    [
      { kind: 'entity', pipeline: '', flowgroup: 't', filePath: 'templates/t.yaml', docKind: 'template', view: 'code' },
      'template_code',
    ],
    [{ kind: 'designer', id: 'designer:p/f', pipeline: 'p', flowgroup: 'f', filePath: 'pipelines/p/f.yaml' }, 'flowgroup_graph'],
    [
      { kind: 'designer', id: 'designer:tpl:templates/t.yaml', pipeline: '', flowgroup: 't', filePath: 'templates/t.yaml', docKind: 'template' },
      'template_graph',
    ],
    [{ kind: 'config', path: 'lhp.yaml', configKind: 'project', view: 'form' }, 'config_form_project'],
    [{ kind: 'config', path: 'lhp.yaml', configKind: 'project', view: 'yaml' }, 'config_yaml_project'],
    [{ kind: 'config', path: 'config/pipeline_config.yaml', configKind: 'pipeline', view: 'form' }, 'config_form_pipeline'],
    [{ kind: 'config', path: 'config/pipeline_config.yaml', configKind: 'pipeline', view: 'yaml' }, 'config_yaml_pipeline'],
    [{ kind: 'config', path: 'config/job_config.yaml', configKind: 'job', view: 'form' }, 'config_form_job'],
    [{ kind: 'config', path: 'config/job_config.yaml', configKind: 'job', view: 'yaml' }, 'config_yaml_job'],
    [{ kind: 'project-map' }, 'project_map'],
    [{ kind: 'pipeline-dag', pipeline: 'p' }, 'pipeline_dag'],
    [{ kind: 'table-detail', fqn: 'c.s.t' }, 'table_detail'],
    [{ kind: 'resource', resourceKind: 'preset', name: 'n', filePath: 'presets/n.yaml' }, 'resource_preset'],
    [{ kind: 'resource', resourceKind: 'template', name: 'n', filePath: 'templates/n.yaml' }, 'resource_template'],
    [{ kind: 'resource', resourceKind: 'blueprint', name: 'n', filePath: 'blueprints/n.yaml' }, 'resource_blueprint'],
    [{ kind: 'resource', resourceKind: 'environment', name: 'n', filePath: 'substitutions/n.yaml' }, 'resource_environment'],
  ]

  it.each(cases)('maps %o to %s', async (tab, surface) => {
    const { surfaceForTab } = await load()
    expect(surfaceForTab(tab)).toBe(surface)
  })
})

describe('workspace binding', () => {
  it('reports the surface of every newly focused tab', async () => {
    const { track, workspace, entityTabId } = await load()
    const s = workspace.getState()

    s.openProjectMap()
    s.openEntityTab('sales', 'orders', 'pipelines/sales/orders.yaml')
    s.setTabView(entityTabId('sales', 'orders'), 'code')
    s.openEntityTab('', 'tpl', 'templates/tpl.yaml', { docKind: 'template', view: 'code' })
    s.openConfigTab('lhp.yaml', 'project')
    s.setTabView('config:lhp.yaml', 'yaml')
    s.openConfigTab('config/pipeline_config.yaml', 'pipeline')
    s.openConfigTab('config/job_config.yaml', 'job', { view: 'yaml' })
    s.openPipelineDag('sales')
    s.openTableDetail('cat.sch.tbl')
    s.openResourceTab('preset', 'bronze', 'presets/bronze.yaml')
    s.openResourceTab('environment', 'dev', 'substitutions/dev.yaml')
    s.openBuffer('pipelines/sales/orders.sql')
    s.setActive('project-map')

    expect(track.mock.calls).toEqual([
      ['project_map', 'opened'],
      ['flowgroup_graph', 'opened'],
      ['flowgroup_code', 'opened'],
      ['template_code', 'opened'],
      ['config_form_project', 'opened'],
      ['config_yaml_project', 'opened'],
      ['config_form_pipeline', 'opened'],
      ['config_yaml_job', 'opened'],
      ['pipeline_dag', 'opened'],
      ['table_detail', 'opened'],
      ['resource_preset', 'opened'],
      ['resource_environment', 'opened'],
      ['file_editor', 'opened'],
      ['project_map', 'opened'],
    ])
  })

  it('stays silent on buffer edits, background opens, view no-ops and closes that focus nothing', async () => {
    const { track, workspace } = await load()
    const s = workspace.getState()
    s.openBuffer('pipelines/sales/orders.sql', { content: 'select 1' })
    track.mockClear()

    s.updateContent('pipelines/sales/orders.sql', 'select 2')
    s.setDirty('pipelines/sales/orders.sql', true)
    s.setSaving('pipelines/sales/orders.sql', true)
    s.openTableDetail('cat.sch.tbl', { activate: false })
    s.openBuffer('pipelines/sales/other.sql', { activate: false })
    s.setTabView('pipelines/sales/orders.sql', 'code')
    s.setActive('pipelines/sales/orders.sql')
    expect(track).not.toHaveBeenCalled()

    // Closing the only active tab leaves page view: nothing was opened.
    s.closeTab('table:cat.sch.tbl')
    s.closeBuffer('pipelines/sales/other.sql')
    s.closeBuffer('pipelines/sales/orders.sql')
    expect(workspace.getState().activePath).toBeNull()
    expect(track).not.toHaveBeenCalled()
  })

  it('reports the neighbour that a close focuses', async () => {
    const { track, workspace } = await load()
    const s = workspace.getState()
    s.openProjectMap()
    s.openPipelineDag('sales')
    track.mockClear()
    s.closeTab('pipeline-dag:sales')
    expect(track.mock.calls).toEqual([['project_map', 'opened']])
  })

  it('never passes a name or path through', async () => {
    const { track, workspace } = await load()
    const s = workspace.getState()
    s.openEntityTab('secret_pipeline', 'secret_flowgroup', 'pipelines/secret_pipeline/secret_flowgroup.yaml')
    s.openTableDetail('secret_catalog.secret_schema.secret_table')
    s.openBuffer('pipelines/secret_pipeline/secret.sql', { content: 'select secret' })
    const serialized = JSON.stringify(track.mock.calls)
    expect(serialized).not.toContain('secret')
    expect(serialized).not.toContain('/')
  })
})

describe('layout binding', () => {
  it('reports lens, inspector and bottom tabs when they become visible, the assistant on open, viewer mode on every flip', async () => {
    const { track, layout } = await load()
    const s = layout.getState()

    s.setExplorerLens('tables')
    s.setExplorerLens('structure')
    s.setExplorerLens('files')
    s.setInspectorTab('help')
    s.setInspectorCollapsed(true)
    s.setInspectorTab('validation')
    s.setInspectorCollapsed(false)
    s.setBottomTab('run')
    s.setBottomCollapsed(false)
    s.setBottomTab('history')
    s.setBottomTab('problems')
    s.setBottomCollapsed(true)
    s.setBottomTab('run')
    s.setAssistantOpen(true)
    s.setAssistantOpen(true)
    s.setAssistantOpen(false)
    s.toggleAssistant()
    s.toggleViewerMode()
    s.setViewerMode(false)
    s.setViewerMode(false)

    expect(track.mock.calls).toEqual([
      ['tables_lens', 'opened'],
      ['structure_lens', 'opened'],
      ['files_lens', 'opened'],
      ['inspector_help', 'opened'],
      ['inspector_validation', 'opened'],
      ['run_stream', 'opened'],
      ['run_history', 'opened'],
      ['problems', 'opened'],
      ['assistant_panel', 'opened'],
      ['assistant_panel', 'opened'],
      ['viewer_mode', 'toggled'],
      ['viewer_mode', 'toggled'],
    ])
  })

  it('ignores geometry', async () => {
    const { track, layout } = await load()
    const s = layout.getState()
    s.setExplorerWidth(300)
    s.setInspectorWidth(320)
    s.setAssistantWidth(400)
    s.setBottomHeight(200)
    s.setExplorerCollapsed(true)
    s.toggleExplorer()
    expect(track).not.toHaveBeenCalled()
  })
})

describe('ui binding', () => {
  it('reports the create-flowgroup dialog opening and nothing else in the store', async () => {
    const { track, ui } = await load()
    const s = ui.getState()
    s.openCreateFlowgroupDialog({ pipeline: 'sales' })
    s.closeCreateFlowgroupDialog()
    s.openCreateFlowgroupDialog()
    s.setSelectedEnv('prod')
    s.setPipelineFilter('sales')
    s.setSelectedPipelineConfig('config/pipeline_config_dev.yaml')
    s.setSandboxEnabled(true)
    s.toggleSandbox()
    expect(track.mock.calls).toEqual([
      ['create_flowgroup_dialog', 'opened'],
      ['create_flowgroup_dialog', 'opened'],
    ])
    expect(JSON.stringify(track.mock.calls)).not.toContain('sales')
  })
})

describe('installTelemetryBindings', () => {
  it('subscribes once however often it is called', async () => {
    const { track, installTelemetryBindings, workspace, layout, ui } = await load()
    installTelemetryBindings()
    installTelemetryBindings()
    workspace.getState().openProjectMap()
    layout.getState().toggleViewerMode()
    ui.getState().openCreateFlowgroupDialog()
    expect(track).toHaveBeenCalledTimes(3)
  })
})
