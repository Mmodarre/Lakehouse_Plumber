import { useEffect, useRef, useState } from 'react'
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
  MoreHorizontal,
  ListFilter,
  Pin,
  RotateCcw,
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
import { tabCloseTargets, type TabCloseCommand } from '../../../workspace/tabCommands'
import { DropdownMenu, DropdownMenuTrigger, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuLabel } from '../../ui/dropdown-menu'
import { Dialog, DialogContent, DialogTitle, DialogDescription } from '../../ui/dialog'
import { Input } from '../../ui/input'

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
        : `${tab.pipeline}/${tab.flowgroup} — ${tab.filePath}`
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
  onCloseMany?: (ids: string[]) => void
}

const CLOSE_COMMANDS: [TabCloseCommand, string][] = [
  ['close', 'Close'], ['others', 'Close other tabs'], ['right', 'Close tabs to the right'],
  ['saved', 'Close saved tabs'], ['all', 'Close all tabs'],
]

export function TabStrip({ onSelect, onClose, onCloseMany }: TabStripProps) {
  const tabs = useWorkspaceStore((s) => s.tabs)
  const buffers = useWorkspaceStore((s) => s.buffers)
  const activePath = useWorkspaceStore((s) => s.activePath)
  const pins = useWorkspaceStore((s) => s.pinnedTabIds)
  const closedCount = useWorkspaceStore((s) => s.closedTabs.length)
  const [menuTab, setMenuTab] = useState<string | null>(null)
  const [pickerOpen, setPickerOpen] = useState(false)
  const [search, setSearch] = useState('')
  const activeRef = useRef<HTMLDivElement>(null)

  useEffect(() => { activeRef.current?.scrollIntoView?.({ block: 'nearest', inline: 'nearest' }) }, [activePath, tabs])

  const close = (id: string, command: TabCloseCommand) => {
    const targets = tabCloseTargets(tabs, buffers, pins, id, command)
    if (command === 'close') onClose(id)
    else onCloseMany?.(targets)
  }

  return (
    <div className="flex min-w-0 flex-1 items-stretch">
      <div className="flex min-w-0 flex-1 overflow-x-auto px-2" aria-label="Open documents">
        {tabs.map((tab, index) => {
          const { id, label, title, Icon, isDirty, missing } = describeTab(tab, buffers)
          const isActive = id === activePath
          const pinned = pins.includes(id)
          return (
            <DropdownMenu key={id} modal={false} open={menuTab === id} onOpenChange={(open) => setMenuTab(open ? id : null)}>
              <div
                ref={isActive ? activeRef : undefined}
                onContextMenu={(event) => { event.preventDefault(); setMenuTab(id) }}
                onAuxClick={(event) => { if (event.button === 1) { event.preventDefault(); onClose(id) } }}
                className={cn('group relative flex shrink-0 items-center border-b-2 text-xs font-medium transition-colors',
                  isActive ? 'border-primary bg-card text-foreground' : 'border-transparent text-muted-foreground hover:text-foreground', missing && 'italic opacity-60')}
              >
                <button
                  onClick={() => onSelect(id)} title={title} aria-current={isActive ? 'page' : undefined}
                  onKeyDown={(event) => {
                    if (event.key === 'ContextMenu' || (event.shiftKey && event.key === 'F10')) { event.preventDefault(); setMenuTab(id) }
                    if (event.altKey && (event.key === 'ArrowLeft' || event.key === 'ArrowRight')) {
                      event.preventDefault(); useWorkspaceStore.getState().moveTab(id, event.key === 'ArrowLeft' ? -1 : 1)
                    }
                  }}
                  className="flex items-center gap-1.5 py-2 pl-3 pr-1"
                >
                  {pinned ? <Pin className="size-3.5 shrink-0" aria-label="Pinned" /> : <Icon className="size-3.5 shrink-0" aria-hidden="true" />}
                  {missing && <span title="File doesn't exist — edit and save to create"><Plus className="size-3" aria-hidden="true" /></span>}
                  <span className="max-w-60 truncate">{label}</span>
                  {isDirty && <span className="ml-0.5 inline-block size-1.5 shrink-0 rounded-full bg-primary"><span className="sr-only">(unsaved changes)</span></span>}
                </button>
                <DropdownMenuTrigger asChild>
                  <button aria-label={`Tab actions for ${label}`} title="Tab actions" className="rounded p-1 opacity-0 hover:bg-muted group-hover:opacity-100 focus-visible:opacity-100 data-[state=open]:opacity-100">
                    <MoreHorizontal className="size-3.5" aria-hidden="true" />
                  </button>
                </DropdownMenuTrigger>
                <button aria-label={`Close ${label}`} onClick={() => onClose(id)} title="Close"
                  className={cn('mr-1 rounded p-1 text-muted-foreground hover:bg-muted hover:text-foreground focus-visible:opacity-100', isActive ? 'opacity-100' : 'opacity-0 group-hover:opacity-100')}>
                  <X className="size-3.5" aria-hidden="true" />
                </button>
              </div>
              <DropdownMenuContent align="start" onCloseAutoFocus={(event) => { if (document.querySelector('[role="dialog"]')) event.preventDefault() }}>
                <DropdownMenuLabel className="max-w-80 truncate text-xs" title={title}>{title}</DropdownMenuLabel>
                {CLOSE_COMMANDS.map(([command, text]) => <DropdownMenuItem key={command}
                  disabled={(command !== 'close' && !onCloseMany) || tabCloseTargets(tabs, buffers, pins, id, command).length === 0}
                  onSelect={() => close(id, command)}>{command === 'all' && pins.length > 0 ? 'Close all unpinned tabs' : text}</DropdownMenuItem>)}
                <DropdownMenuSeparator />
                <DropdownMenuItem onSelect={() => useWorkspaceStore.getState().togglePinned(id)}>{pinned ? 'Unpin tab' : 'Pin tab'}</DropdownMenuItem>
                <DropdownMenuItem disabled={index === 0} onSelect={() => useWorkspaceStore.getState().moveTab(id, -1)}>Move tab left</DropdownMenuItem>
                <DropdownMenuItem disabled={index === tabs.length - 1} onSelect={() => useWorkspaceStore.getState().moveTab(id, 1)}>Move tab right</DropdownMenuItem>
                {tabBufferPath(tab) && <DropdownMenuItem onSelect={() => { void navigator.clipboard?.writeText(tabBufferPath(tab)!) }}>Copy relative path</DropdownMenuItem>}
                <DropdownMenuSeparator />
                <DropdownMenuItem disabled={closedCount === 0} onSelect={() => useWorkspaceStore.getState().reopenClosedTab()}><RotateCcw />Reopen closed tab</DropdownMenuItem>
                {pins.length > 0 && <DropdownMenuLabel className="text-xs font-normal text-muted-foreground">Bulk close keeps pinned tabs.</DropdownMenuLabel>}
              </DropdownMenuContent>
            </DropdownMenu>
          )
        })}
      </div>
      <button aria-label="Search open tabs" title="Search open tabs" className="shrink-0 border-l border-border px-2 text-muted-foreground hover:bg-muted" onClick={() => { setSearch(''); setPickerOpen(true) }}><ListFilter className="size-4" /></button>
      {tabs.length === 0 && closedCount > 0 && <button className="px-3 text-xs" onClick={() => useWorkspaceStore.getState().reopenClosedTab()}>Reopen closed tab</button>}
      <Dialog open={pickerOpen} onOpenChange={setPickerOpen}>
        <DialogContent className="sm:max-w-xl">
          <DialogTitle>Open tabs</DialogTitle>
          <DialogDescription>Find a document by name or relative path.</DialogDescription>
          <Input autoFocus aria-label="Search open tabs" value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search name or path…" />
          <div className="max-h-80 overflow-auto">
            {tabs.filter((tab) => { const d = describeTab(tab, buffers); return `${d.label} ${d.title}`.toLowerCase().includes(search.toLowerCase()) }).map((tab) => {
              const d = describeTab(tab, buffers)
              return <button key={d.id} className={cn('flex w-full flex-col rounded px-3 py-2 text-left hover:bg-accent focus-visible:bg-accent', d.id === activePath && 'bg-accent')} onClick={() => { onSelect(d.id); setPickerOpen(false) }}>
                <span className="text-sm">{d.label}{d.isDirty ? ' •' : ''}</span><span className="text-xs text-muted-foreground">{d.title}</span>
              </button>
            })}
            {!tabs.some((tab) => { const d = describeTab(tab, buffers); return `${d.label} ${d.title}`.toLowerCase().includes(search.toLowerCase()) }) && <p className="p-3 text-sm text-muted-foreground">No matching tabs.</p>}
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}
