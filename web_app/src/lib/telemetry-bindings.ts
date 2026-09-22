import {
  useLayoutStore,
  type BottomTab,
  type ExplorerLens,
  type InspectorTab,
} from '../store/layoutStore'
import { useUIStore } from '../store/uiStore'
import {
  normalizeEntityView,
  useWorkspaceStore,
  workspaceTabId,
  type FlowgroupEntityView,
  type TemplateEntityView,
  type WorkspaceTabRef,
} from '../store/workspaceStore'
import { track, type UiSurface } from './telemetry'

/**
 * Store-driven usage telemetry: turns the zustand transitions that show a
 * surface into `opened` / `toggled` events. Only the shape of the change is
 * read (which tab kind and view, which lens or panel tab, a flag flipping);
 * the identity fields on those same states — paths, flowgroup and table
 * names, the env, the pipeline filter, panel geometry — are never passed on.
 * Loaded lazily with the client; nothing subscribes at import.
 */

const LENS_SURFACE: Record<ExplorerLens, UiSurface> = {
  files: 'files_lens',
  structure: 'structure_lens',
  tables: 'tables_lens',
}

const INSPECTOR_SURFACE: Record<InspectorTab, UiSurface> = {
  validation: 'inspector_validation',
  help: 'inspector_help',
}

const TEMPLATE_VIEW_SURFACE: Record<TemplateEntityView, UiSurface> = {
  builder: 'template_builder',
  code: 'template_code',
  preview: 'template_preview',
}

const FLOWGROUP_VIEW_SURFACE: Record<FlowgroupEntityView, UiSurface> = {
  graph: 'flowgroup_graph',
  code: 'flowgroup_code',
}

const BOTTOM_SURFACE: Record<BottomTab, UiSurface> = {
  problems: 'problems',
  run: 'run_stream',
  history: 'run_history',
}

/** The surface a center tab shows, from its kind and view alone. */
export function surfaceForTab(tab: WorkspaceTabRef): UiSurface {
  switch (tab.kind) {
    case 'file':
      return 'file_editor'
    case 'entity': {
      // normalizeEntityView maps a stored view onto the views that doc kind
      // offers (a legacy template 'graph' becomes 'builder'), so each lookup
      // only ever sees a key of its own table.
      const view = normalizeEntityView(tab.docKind, tab.view)
      return tab.docKind === 'template'
        ? TEMPLATE_VIEW_SURFACE[view as TemplateEntityView]
        : FLOWGROUP_VIEW_SURFACE[view as FlowgroupEntityView]
    }
    case 'designer':
      return tab.docKind === 'template' ? 'template_graph' : 'flowgroup_graph'
    case 'config':
      return `config_${tab.view}_${tab.configKind}`
    case 'project-map':
      return 'project_map'
    case 'pipeline-dag':
      return 'pipeline_dag'
    case 'table-detail':
      return 'table_detail'
    case 'resource':
      return `resource_${tab.resourceKind}`
  }
}

function activeTab(s: {
  tabs: WorkspaceTabRef[]
  activePath: string | null
}): WorkspaceTabRef | undefined {
  if (s.activePath === null) return undefined
  return s.tabs.find((t) => workspaceTabId(t) === s.activePath)
}

let uninstall: (() => void) | null = null

/**
 * Subscribe the three stores once; returns the unsubscribe. Calling it again
 * while installed returns the same unsubscribe without doubling anything.
 */
export function installTelemetryBindings(): () => void {
  if (uninstall !== null) return uninstall

  const unsubscribeWorkspace = useWorkspaceStore.subscribe((s, prev) => {
    // Every set() notifies, including per-keystroke buffer patches; the tab
    // strip's identity fields are the only ones that can show a surface.
    if (s.activePath === prev.activePath && s.tabs === prev.tabs) return
    const tab = activeTab(s)
    if (tab === undefined) return
    const surface = surfaceForTab(tab)
    if (s.activePath === prev.activePath) {
      // Same tab: only a view switch (graph↔code, form↔yaml) shows a new surface.
      const before = activeTab(prev)
      if (before !== undefined && surfaceForTab(before) === surface) return
    }
    track(surface, 'opened')
  })

  const unsubscribeLayout = useLayoutStore.subscribe((s, prev) => {
    if (s.explorerLens !== prev.explorerLens) track(LENS_SURFACE[s.explorerLens], 'opened')
    // A panel's tab is shown when it changes while visible or when the panel
    // expands; switching tabs on a collapsed panel shows nothing.
    if (!s.inspectorCollapsed && (prev.inspectorCollapsed || s.inspectorTab !== prev.inspectorTab)) {
      track(INSPECTOR_SURFACE[s.inspectorTab], 'opened')
    }
    if (!s.bottomCollapsed && (prev.bottomCollapsed || s.bottomTab !== prev.bottomTab)) {
      track(BOTTOM_SURFACE[s.bottomTab], 'opened')
    }
    if (s.assistantOpen && !prev.assistantOpen) track('assistant_panel', 'opened')
    if (s.viewerMode !== prev.viewerMode) track('viewer_mode', 'toggled')
  })

  const unsubscribeUi = useUIStore.subscribe((s, prev) => {
    if (s.createFlowgroupDialog && !prev.createFlowgroupDialog) {
      track('create_flowgroup_dialog', 'opened')
    }
  })

  uninstall = () => {
    unsubscribeWorkspace()
    unsubscribeLayout()
    unsubscribeUi()
    uninstall = null
  }
  return uninstall
}
