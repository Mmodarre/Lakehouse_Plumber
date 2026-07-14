import { useCallback, useRef } from 'react'
import type { LucideIcon } from 'lucide-react'
import { FileText, Table2, Workflow } from 'lucide-react'
import { useLayoutStore } from '../../../store/layoutStore'
import type { ExplorerLens } from '../../../store/layoutStore'
import { StructureLens } from './StructureLens'
import { TablesLens } from './TablesLens'
import { FilesLens } from './FilesLens'
import { clampExplorerWidth } from './explorerData'
import { cn } from '@/lib/utils'

// ── Explorer — the multi-lens left navigator (T1.3 / D5) ─────
//
// Hosts the lens switcher (Structure | Tables | Files → layoutStore.
// explorerLens), the active lens body, and a right-edge drag handle that
// writes layoutStore.explorerWidth (clamped ~200–420px; the AppShell grid
// consumes that width). Collapsed (⌘B → explorerCollapsed) renders nothing —
// the grid column is already 0px.

const LENSES: { id: ExplorerLens; label: string; icon: LucideIcon }[] = [
  { id: 'structure', label: 'Structure', icon: Workflow },
  { id: 'tables', label: 'Tables', icon: Table2 },
  { id: 'files', label: 'Files', icon: FileText },
]

function LensSwitcher({
  lens,
  onSelect,
}: {
  lens: ExplorerLens
  onSelect: (lens: ExplorerLens) => void
}) {
  return (
    <div
      role="tablist"
      aria-label="Explorer lens"
      className="m-1.5 flex gap-0.5 rounded-sm border border-border bg-background p-0.5"
    >
      {LENSES.map(({ id, label, icon: Icon }) => {
        const active = lens === id
        return (
          <button
            key={id}
            type="button"
            role="tab"
            aria-selected={active}
            onClick={() => onSelect(id)}
            className={cn(
              'flex h-[26px] flex-1 items-center justify-center gap-1.5 rounded-xs text-2xs font-semibold transition-colors',
              active
                ? 'bg-primary text-primary-foreground'
                : 'text-muted-foreground hover:text-foreground',
            )}
          >
            <Icon className="size-3.5" aria-hidden="true" />
            {label}
          </button>
        )
      })}
    </div>
  )
}

export function Explorer() {
  const explorerLens = useLayoutStore((s) => s.explorerLens)
  const setExplorerLens = useLayoutStore((s) => s.setExplorerLens)
  const setExplorerWidth = useLayoutStore((s) => s.setExplorerWidth)
  const collapsed = useLayoutStore((s) => s.explorerCollapsed)
  const rootRef = useRef<HTMLElement>(null)

  const onHandlePointerDown = useCallback(
    (e: React.PointerEvent<HTMLDivElement>) => {
      e.preventDefault()
      const handle = e.currentTarget
      handle.setPointerCapture(e.pointerId)
      const left = rootRef.current?.getBoundingClientRect().left ?? 0
      const onMove = (ev: PointerEvent) => setExplorerWidth(clampExplorerWidth(ev.clientX - left))
      const onUp = () => {
        try {
          handle.releasePointerCapture(e.pointerId)
        } catch {
          // pointer already released
        }
        window.removeEventListener('pointermove', onMove)
        window.removeEventListener('pointerup', onUp)
      }
      window.addEventListener('pointermove', onMove)
      window.addEventListener('pointerup', onUp)
    },
    [setExplorerWidth],
  )

  if (collapsed) return null

  return (
    <nav
      ref={rootRef}
      aria-label="Entity explorer"
      className="relative flex h-full min-w-0 flex-col overflow-hidden border-r border-border bg-surface"
    >
      <LensSwitcher lens={explorerLens} onSelect={setExplorerLens} />

      {/* Flex column so the Structure lens can pin its own bottom region; the
          Structure lens owns its scroll, while Tables/Files scroll here. */}
      <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
        {explorerLens === 'structure' && <StructureLens />}
        {explorerLens === 'tables' && (
          <div className="min-h-0 flex-1 overflow-auto">
            <TablesLens />
          </div>
        )}
        {explorerLens === 'files' && (
          <div className="min-h-0 flex-1 overflow-auto">
            <FilesLens />
          </div>
        )}
      </div>

      {/* Right-edge drag-resize handle → layoutStore.explorerWidth. */}
      <div
        role="separator"
        aria-orientation="vertical"
        aria-label="Resize explorer"
        onPointerDown={onHandlePointerDown}
        className="group absolute inset-y-0 -right-0.5 z-10 w-1.5 cursor-col-resize"
      >
        <span className="absolute inset-y-0 right-0.5 w-px bg-transparent transition-colors group-hover:bg-accent-line" />
      </div>
    </nav>
  )
}
