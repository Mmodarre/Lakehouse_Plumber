import { lazy, Suspense, useEffect, useRef, useState } from 'react'
import { Toaster } from '../ui/sonner'
import { CommandBar } from './CommandBar'
import { WorkspaceNavigation } from './WorkspaceNavigation'
import { CenterArea } from './center/CenterArea'
import { Explorer } from './explorer/Explorer'
import { Inspector } from './inspector/Inspector'
import { AssistantDock } from './AssistantDock'
import { BottomPanel } from './bottom/BottomPanel'
import { StatusBar } from '../layout/StatusBar'
import { NavigationGuard } from '../layout/NavigationGuard'
import { OfflineBanner } from '../layout/OfflineBanner'
const CreateFlowgroupDialog = lazy(() => import('../editor/CreateFlowgroupDialog').then((m) => ({ default: m.CreateFlowgroupDialog })))
import { useFlowgroupEditorBridge } from '../workspace/flowgroupBuffers'
import { ErrorBoundary } from '../common/ErrorBoundary'
import { ModalErrorFallback } from '../common/ModalErrorFallback'
const InitProjectPage = lazy(() => import('../../pages/InitProjectPage').then((m) => ({ default: m.InitProjectPage })))
import { useHealth } from '../../hooks/useProject'
import { usePushChannel } from '../../hooks/usePushChannel'
import { useUIStore } from '../../store/uiStore'
import { useNavigationStore } from '@/workspace/navigation'
import { useWorkspaceStore } from '../../store/workspaceStore'
import { useLayoutStore } from '../../store/layoutStore'

// ── AppShell — the routed unified-workspace shell (§3 / §6.4) ─────
//
// The single routed surface (router `*` → AppShell); only /init is a separate
// route. Realises the §3 layout — 44px CommandBar / 1fr main (explorer ·
// center · inspector · assistant) / auto bottom panel / 26px StatusBar — as a
// flex column (CommandBar and StatusBar own their fixed heights; the main row
// flexes). Mounts the always-on shell wiring relocated from the old Layout:
// usePushChannel, the flowgroup-editor bridge, the project-scope guard, the
// health/no_project gate, OfflineBanner, NavigationGuard, CreateFlowgroupDialog
// and the Toaster. Region bodies are filled by the explorer/center/inspector/
// assistant/bottom surfaces.

// Usage telemetry is loaded only after health confirms this server process
// has it on: the client, its store bindings and the post helper stay out of
// the eager app chunk, and a tab against an opted-out server never fetches
// them. Until then components report through lib/telemetry-shim, which holds
// their calls for the client.
function loadTelemetry() {
  return Promise.all([import('../../lib/telemetry'), import('../../lib/telemetry-bindings')])
}

export function AppShell() {
  const { data: health, isError: healthError, refetch } = useHealth()

  // Server-push channel: turns /api/events into query invalidations. Mounted
  // once here so every surface stays fresh.
  usePushChannel()
  // Flowgroup open/create requests (create dialog, future explorer rows)
  // become workspace buffers instead of the retired editor modals.
  useFlowgroupEditorBridge()

  const createFlowgroupDialog = useUIStore((s) => s.createFlowgroupDialog)
  const closeCreateFlowgroupDialog = useUIStore((s) => s.closeCreateFlowgroupDialog)
  const ensureProjectScope = useWorkspaceStore((s) => s.ensureProjectScope)

  const explorerWidth = useLayoutStore((s) => s.explorerWidth)
  const explorerCollapsed = useLayoutStore((s) => s.explorerCollapsed)
  const inspectorWidth = useLayoutStore((s) => s.inspectorWidth)
  const inspectorCollapsed = useLayoutStore((s) => s.inspectorCollapsed)
  const assistantOpen = useLayoutStore((s) => s.assistantOpen)
  const assistantWidth = useLayoutStore((s) => s.assistantWidth)
  const bottomCollapsed = useLayoutStore((s) => s.bottomCollapsed)
  const bottomHeight = useLayoutStore((s) => s.bottomHeight)
  const viewerMode = useLayoutStore((s) => s.viewerMode)
  const focusMode = useLayoutStore((s) => s.focusMode)
  const density = useLayoutStore((s) => s.density)
  const [viewport, setViewport] = useState(() => ({ width: window.innerWidth, height: window.innerHeight }))
  const previousWidth = useRef(Infinity)
  const workspaceRoot = useWorkspaceStore((s) => s.projectRoot)
  const activePath = useWorkspaceStore((s) => s.activePath)

  useEffect(() => {
    const resize = () => setViewport({ width: window.innerWidth, height: window.innerHeight })
    window.addEventListener('resize', resize)
    return () => window.removeEventListener('resize', resize)
  }, [])
  // Free space when crossing into a narrower layout. Users can then reopen a
  // side panel as a drawer without squeezing the active document.
  useEffect(() => {
    const layout = useLayoutStore.getState()
    if (previousWidth.current >= 1380 && viewport.width < 1380) layout.setInspectorCollapsed(true)
    if (previousWidth.current >= 1000 && viewport.width < 1000) layout.setExplorerCollapsed(true)
    previousWidth.current = viewport.width
  }, [viewport.width])

  // Persisted buffers are keyed to a project root: a different served project
  // drops the restored workspace instead of leaking it across.
  const projectRoot = health?.root
  useEffect(() => {
    if (!projectRoot) return
    const previousRoot = useWorkspaceStore.getState().projectRoot
    if (previousRoot && previousRoot !== projectRoot) useNavigationStore.getState().reset()
    ensureProjectScope(projectRoot)
  }, [projectRoot, ensureProjectScope])

  // A telemetry chunk that fails to load (offline, a redeployed bundle) is
  // swallowed: the workspace never depends on it.
  const telemetryEnabled = health?.telemetry_enabled === true
  useEffect(() => {
    if (!telemetryEnabled) return
    let cancelled = false
    let unbind: (() => void) | undefined
    void loadTelemetry()
      .then(([client, bindings]) => {
        if (cancelled) return
        client.setTelemetryEnabled(true)
        client.installTelemetry()
        unbind = bindings.installTelemetryBindings()
      })
      .catch(() => {})
    return () => {
      cancelled = true
      unbind?.()
      void loadTelemetry()
        .then(([client]) => client.setTelemetryEnabled(false))
        .catch(() => {})
    }
  }, [telemetryEnabled])

  // Health gate (verbatim from the old Layout): while the server reports no
  // project, the first-run wizard fills the main area in place of the
  // workspace. On success it invalidates all queries, health flips out of
  // no_project, and the normal shell replaces this branch automatically.
  const noProject = health?.project_state === 'no_project'

  const explorerDrawer = viewport.width < 1000
  const inspectorDrawer = viewport.width - (explorerCollapsed ? 0 : explorerWidth) - (assistantOpen ? assistantWidth : 44) - inspectorWidth < 480
  const assistantDrawer = viewport.width < 1100
  const explorerCol = focusMode || explorerCollapsed || explorerDrawer ? '0px' : `${explorerWidth}px`
  const inspectorCol = focusMode ? '0px' : inspectorCollapsed || inspectorDrawer ? '42px' : `${inspectorWidth}px`
  const assistantCol = focusMode ? '0px' : assistantOpen && !assistantDrawer ? `${assistantWidth}px` : '44px'

  return (
    <div className="flex h-dvh min-h-0 flex-col overflow-hidden bg-background" data-density={density}>
      <CommandBar />
      {!noProject && projectRoot && workspaceRoot === projectRoot && <WorkspaceNavigation />}
      {healthError && <OfflineBanner onRetry={() => void refetch()} />}

      {noProject ? (
        <div className="min-h-0 flex-1">
          <ErrorBoundary>
            <Suspense fallback={<p className="p-6 text-sm text-muted-foreground" role="status">Loading project setup…</p>}><InitProjectPage /></Suspense>
          </ErrorBoundary>
        </div>
      ) : (
        <>
          {/* Main 4-column region: explorer / center / inspector / assistant.
              Widths come from layoutStore; sibling tasks fill the bodies. */}
          <div
            className="relative grid min-h-0 flex-1"
            style={{
              gridTemplateColumns: `${explorerCol} minmax(0,1fr) ${inspectorCol} ${assistantCol}`,
            }}
          >
            <div className={focusMode ? 'hidden' : explorerDrawer && !explorerCollapsed ? 'absolute inset-y-0 left-0 z-30 shadow-xl' : 'min-h-0 min-w-0 overflow-hidden'} style={{ gridColumn: 1, ...(explorerDrawer && !explorerCollapsed ? { width: Math.min(explorerWidth, viewport.width - 86) } : {}) }}>
              <Explorer />
            </div>
            <div className="min-h-0 min-w-0" data-workspace-center tabIndex={-1} style={{ gridColumn: 2 }}><ErrorBoundary resetKeys={[activePath]}><CenterArea /></ErrorBoundary></div>
            <div className={focusMode ? 'hidden' : inspectorDrawer && !inspectorCollapsed ? 'absolute inset-y-0 right-11 z-30 shadow-xl' : 'min-h-0 min-w-0 overflow-hidden'} style={{ gridColumn: 3, ...(inspectorDrawer && !inspectorCollapsed ? { width: Math.min(inspectorWidth, viewport.width - 86) } : {}) }}>
              <ErrorBoundary><Inspector /></ErrorBoundary>
            </div>
            <div className={focusMode ? 'hidden' : assistantDrawer && assistantOpen ? 'absolute inset-y-0 right-0 z-40 shadow-xl' : 'min-h-0 min-w-0 overflow-hidden'} style={{ gridColumn: 4, ...(assistantDrawer && assistantOpen ? { width: Math.min(assistantWidth, viewport.width - 42) } : {}) }}>
              <ErrorBoundary><AssistantDock /></ErrorBoundary>
            </div>
          </div>

          {/* Bottom panel row (collapsed by default) — sits between main and
              StatusBar; AppShell owns the row height, BottomPanel fills it. */}
          <div
            className="flex shrink-0 border-t border-border bg-surface"
            style={{ height: focusMode || bottomCollapsed ? 28 : Math.min(bottomHeight, Math.max(120, viewport.height - 330)) }}
          >
            <BottomPanel forceCollapsed={focusMode} />
          </div>
        </>
      )}

      <StatusBar />

      {/* The app's single route blocker: prompts over every dirty-guard source
          (workspace buffers, config forms) in one dialog. */}
      <NavigationGuard />

      <ErrorBoundary
        fallback={<ModalErrorFallback onClose={closeCreateFlowgroupDialog} />}
        resetKeys={[createFlowgroupDialog]}
      >
        {!viewerMode && createFlowgroupDialog && <Suspense fallback={null}><CreateFlowgroupDialog /></Suspense>}
      </ErrorBoundary>

      {/* ui/sonner wrapper syncs its theme to the resolved app theme */}
      <Toaster position="bottom-right" richColors toastOptions={{ duration: 4000 }} />
    </div>
  )
}
