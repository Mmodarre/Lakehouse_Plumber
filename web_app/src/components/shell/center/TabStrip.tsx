import type { LucideIcon } from 'lucide-react'
import {
  Boxes,
  Braces,
  CircleCheckBig,
  Database,
  FileCode2,
  FileText,
  LayoutTemplate,
  Layers,
  Network,
  Package,
  Plus,
  SlidersHorizontal,
  Table2,
  Waypoints,
  Workflow,
  X,
} from 'lucide-react'
import {
  categoryForPath,
  tabBufferPath,
  useWorkspaceStore,
  workspaceTabId,
  type EditorBuffer,
  type ResourceKind,
  type WorkspaceTabRef,
} from '../../../store/workspaceStore'
import { cn } from '../../../lib/utils'

// ── TabStrip — the unified center tab strip (§3 / §6.4) ──────
//
// Recomposed from components/workspace/EditorTabBar.tsx: the same underline
// tabs with a per-kind icon, dirty-dot, close affordance and middle-click
// close — now rendering EVERY workspace tab kind (file / entity / config /
// project-map / table-detail / resource), not just file + designer. Pure
// presentation over the workspaceStore tab union; open/close/select behaviour
// lives in the parent CenterArea.

const CATEGORY_ICON: Record<string, LucideIcon> = {
  yaml: FileText,
  sql: Database,
  python: FileCode2,
  schema: Braces,
  expectations: CircleCheckBig,
}

const RESOURCE_ICON: Record<ResourceKind, LucideIcon> = {
  preset: Package,
  template: LayoutTemplate,
  blueprint: Boxes,
  environment: Layers,
}

interface TabDisplay {
  id: string
  label: string
  title: string
  Icon: LucideIcon
  isDirty: boolean
  /** A file tab whose buffer references a not-yet-created file. */
  missing: boolean
}

/** Derive the strip's per-tab display fields across every tab kind. */
function describeTab(tab: WorkspaceTabRef, buffers: EditorBuffer[]): TabDisplay {
  const id = workspaceTabId(tab)
  const bufPath = tabBufferPath(tab)
  const buf = bufPath ? buffers.find((b) => b.path === bufPath) : undefined
  const isDirty = buf?.isDirty ?? false

  switch (tab.kind) {
    case 'file': {
      const label = tab.path.split('/').pop() ?? tab.path
      const Icon = CATEGORY_ICON[buf?.category ?? categoryForPath(tab.path)] ?? FileText
      return { id, label, title: tab.path, Icon, isDirty, missing: buf ? !buf.exists : false }
    }
    case 'entity':
    case 'designer': {
      const isTemplate = tab.docKind === 'template'
      const label = isTemplate
        ? tab.flowgroup
        : tab.pipeline
          ? `${tab.pipeline}·${tab.flowgroup}`
          : tab.flowgroup
      const title = isTemplate
        ? `Template — ${tab.flowgroup}`
        : `${tab.pipeline}/${tab.flowgroup}`
      return { id, label, title, Icon: isTemplate ? LayoutTemplate : Waypoints, isDirty, missing: false }
    }
    case 'config': {
      const label = tab.path.split('/').pop() ?? tab.path
      return {
        id,
        label,
        title: `${tab.configKind} config — ${tab.path}`,
        Icon: SlidersHorizontal,
        isDirty,
        missing: false,
      }
    }
    case 'project-map':
      return {
        id,
        label: 'Project map',
        title: 'Project dependency map',
        Icon: Network,
        isDirty: false,
        missing: false,
      }
    case 'pipeline-dag':
      return {
        id,
        label: tab.pipeline,
        title: `Pipeline DAG — ${tab.pipeline}`,
        Icon: Workflow,
        isDirty: false,
        missing: false,
      }
    case 'table-detail': {
      const short = tab.fqn.split(/[./]/).filter(Boolean).pop() ?? tab.fqn
      return { id, label: short, title: tab.fqn, Icon: Table2, isDirty: false, missing: false }
    }
    case 'resource':
      return {
        id,
        label: tab.name,
        title: `${tab.resourceKind} — ${tab.name}`,
        Icon: RESOURCE_ICON[tab.resourceKind] ?? Package,
        isDirty,
        missing: false,
      }
  }
}

interface TabStripProps {
  onSelect: (id: string) => void
  onClose: (id: string) => void
}

export function TabStrip({ onSelect, onClose }: TabStripProps) {
  const tabs = useWorkspaceStore((s) => s.tabs)
  const buffers = useWorkspaceStore((s) => s.buffers)
  const activePath = useWorkspaceStore((s) => s.activePath)

  return (
    <div className="flex min-w-0 flex-1 overflow-x-auto px-2">
      {tabs.map((tab) => {
        const { id, label, title, Icon, isDirty, missing } = describeTab(tab, buffers)
        const isActive = id === activePath
        return (
          // Select and close are SIBLING buttons (an interactive close nested
          // inside the tab button is invalid HTML and keyboard-unreachable);
          // the wrapper carries the shared chrome + middle-click-to-close.
          <div
            key={id}
            onAuxClick={(e) => {
              if (e.button === 1) {
                e.preventDefault()
                onClose(id)
              }
            }}
            className={cn(
              'group relative flex shrink-0 items-center border-b-2 text-xs font-medium transition-colors',
              isActive
                ? 'border-primary bg-card text-foreground'
                : 'border-transparent text-muted-foreground hover:text-foreground',
              missing && 'italic opacity-60',
            )}
          >
            <button
              onClick={() => onSelect(id)}
              title={title}
              className="flex items-center gap-1.5 py-2 pl-3 pr-1"
            >
              <Icon className="size-3.5 shrink-0" aria-hidden="true" />
              {missing && (
                <span title="File doesn't exist — edit and save to create">
                  <Plus className="size-3" aria-hidden="true" />
                </span>
              )}
              <span className="max-w-60 truncate">{label}</span>
              {isDirty && (
                <span className="ml-0.5 inline-block size-1.5 shrink-0 rounded-full bg-primary">
                  <span className="sr-only">(unsaved changes)</span>
                </span>
              )}
            </button>
            <button
              aria-label={`Close ${label}`}
              onClick={() => onClose(id)}
              className={cn(
                'mr-2 cursor-pointer text-muted-foreground hover:text-foreground',
                isActive ? 'inline-flex' : 'hidden group-hover:inline-flex',
              )}
              title="Close"
            >
              <X className="size-3" aria-hidden="true" />
            </button>
          </div>
        )
      })}
    </div>
  )
}
