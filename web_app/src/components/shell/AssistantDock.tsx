import { Suspense, lazy, useEffect, useRef } from 'react'
import { MessageSquare, Sparkles } from 'lucide-react'
import { useLayoutStore } from '../../store/layoutStore'
import { useAssistantStore } from '../../store/assistantStore'
import { LoadingSpinner } from '../common/LoadingSpinner'

// ── AssistantDock — the D3 collapsible assistant dock ───────────
//
// The assistant is its own dock to the right of the inspector (§3 / D3),
// driven by layoutStore.assistantOpen. Collapsed = ~44px vertical rail whose
// icon expands the dock; expanded = the existing AssistantPanel filling the
// grid column (layoutStore.assistantWidth is applied by AppShell's grid, so
// the wrapper only fills 100%).
//
// AssistantPanel is React.lazy so its markdown stack stays out of the eager
// app chunk (it was lazily hosted by the old Layout for the same reason).
//
// WRAPPER ADAPTATION (AssistantPanel internals untouched): the panel's own
// header × button writes assistantStore.panelOpen=false — the dock's source
// of truth is layoutStore.assistantOpen instead. The effect below bridges the
// two: it detects the panel's × click as a falling edge of panelOpen and
// collapses the dock, and arms panelOpen=true whenever the dock is opened
// from elsewhere (the CommandBar toggle or the rail) so the next × click
// produces that edge. Chat behaviour is untouched.

const AssistantPanel = lazy(() => import('../assistant/AssistantPanel'))

export function AssistantDock() {
  const assistantOpen = useLayoutStore((s) => s.assistantOpen)
  const setAssistantOpen = useLayoutStore((s) => s.setAssistantOpen)
  const panelOpen = useAssistantStore((s) => s.panelOpen)
  const setPanelOpen = useAssistantStore((s) => s.setPanelOpen)

  const prevPanelOpen = useRef(panelOpen)
  useEffect(() => {
    const wasOpen = prevPanelOpen.current
    prevPanelOpen.current = panelOpen
    if (!assistantOpen) return
    if (wasOpen && !panelOpen) {
      // Falling edge = AssistantPanel's own × close button was clicked.
      setAssistantOpen(false)
      return
    }
    if (!panelOpen) {
      // Dock is open but panelOpen is stale-closed (opened via the CommandBar
      // toggle / rail, or a persisted mismatch): arm it.
      setPanelOpen(true)
    }
  }, [assistantOpen, panelOpen, setAssistantOpen, setPanelOpen])

  if (!assistantOpen) {
    return (
      <div className="flex h-full w-full flex-col items-center gap-1.5 border-l border-border bg-surface py-2">
        <button
          type="button"
          onClick={() => {
            setPanelOpen(true)
            setAssistantOpen(true)
          }}
          aria-label="Open assistant"
          title="Open assistant"
          className="flex size-7 items-center justify-center rounded-md text-kind-transform transition-colors hover:bg-muted/60"
        >
          <Sparkles className="size-4" aria-hidden="true" />
        </button>
        <button
          type="button"
          onClick={() => {
            setPanelOpen(true)
            setAssistantOpen(true)
          }}
          aria-label="Open assistant chat"
          title="Open chat"
          className="flex size-7 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-muted/60 hover:text-foreground"
        >
          <MessageSquare className="size-4" aria-hidden="true" />
        </button>
        <span className="mt-1 text-[10px] font-bold tracking-[0.12em] text-faint uppercase [writing-mode:vertical-rl]">
          Assistant
        </span>
      </div>
    )
  }

  return (
    <div className="flex h-full w-full min-w-0 flex-col border-l border-border bg-surface">
      <Suspense fallback={<LoadingSpinner className="flex-1" />}>
        <AssistantPanel />
      </Suspense>
    </div>
  )
}
