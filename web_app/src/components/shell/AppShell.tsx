import { useEffect } from 'react'
import { Toaster } from '../ui/sonner'
import { CommandBar } from './CommandBar'
import { CenterArea } from './center/CenterArea'
import { Explorer } from './explorer/Explorer'
import { Inspector } from './inspector/Inspector'
import { AssistantDock } from './AssistantDock'
import { BottomPanel } from './bottom/BottomPanel'
import { StatusBar } from '../layout/StatusBar'
import { NavigationGuard } from '../layout/NavigationGuard'
import { OfflineBanner } from '../layout/OfflineBanner'
import { CreateFlowgroupDialog } from '../editor/CreateFlowgroupDialog'
import { useFlowgroupEditorBridge } from '../workspace/flowgroupBuffers'
import { ErrorBoundary } from '../common/ErrorBoundary'
import { ModalErrorFallback } from '../common/ModalErrorFallback'
import { InitProjectPage } from '../../pages/InitProjectPage'
import { useHealth } from '../../hooks/useProject'
import { usePushChannel } from '../../hooks/usePushChannel'
import { useUIStore } from '../../store/uiStore'
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

  // Persisted buffers are keyed to a project root: a different served project
  // drops the restored workspace instead of leaking it across.
  const projectRoot = health?.root
  useEffect(() => {
    if (projectRoot) ensureProjectScope(projectRoot)
  }, [projectRoot, ensureProjectScope])

  // Health gate (verbatim from the old Layout): while the server reports no
  // project, the first-run wizard fills the main area in place of the
  // workspace. On success it invalidates all queries, health flips out of
  // no_project, and the normal shell replaces this branch automatically.
  const noProject = health?.project_state === 'no_project'

  const explorerCol = explorerCollapsed ? '0px' : `${explorerWidth}px`
  const inspectorCol = inspectorCollapsed ? '42px' : `${inspectorWidth}px`
  const assistantCol = assistantOpen ? `${assistantWidth}px` : '44px'

  return (
    <div className="flex h-screen flex-col bg-background">
      <CommandBar />
      {healthError && <OfflineBanner onRetry={() => void refetch()} />}

      {noProject ? (
        <div className="min-h-0 flex-1">
          <ErrorBoundary>
            <InitProjectPage />
          </ErrorBoundary>
        </div>
      ) : (
        <>
          {/* Main 4-column region: explorer / center / inspector / assistant.
              Widths come from layoutStore; sibling tasks fill the bodies. */}
          <div
            className="grid min-h-0 flex-1"
            style={{
              gridTemplateColumns: `${explorerCol} minmax(0,1fr) ${inspectorCol} ${assistantCol}`,
            }}
          >
            <Explorer />
            <CenterArea />
            <Inspector />
            <AssistantDock />
          </div>

          {/* Bottom panel row (collapsed by default) — sits between main and
              StatusBar; AppShell owns the row height, BottomPanel fills it. */}
          <div
            className="flex shrink-0 border-t border-border bg-surface"
            style={{ height: bottomCollapsed ? 28 : bottomHeight }}
          >
            <BottomPanel />
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
        <CreateFlowgroupDialog />
      </ErrorBoundary>

      {/* ui/sonner wrapper syncs its theme to the resolved app theme */}
      <Toaster position="bottom-right" richColors toastOptions={{ duration: 4000 }} />
    </div>
  )
}
