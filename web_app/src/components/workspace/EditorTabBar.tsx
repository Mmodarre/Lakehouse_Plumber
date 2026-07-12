import type { LucideIcon } from 'lucide-react'
import {
  Braces,
  CircleCheckBig,
  Database,
  FileCode2,
  FileText,
  LayoutTemplate,
  Plus,
  Waypoints,
  X,
} from 'lucide-react'
import { Button } from '../ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '../ui/dropdown-menu'
import { cn } from '../../lib/utils'

/** Lucide icon per editor-tab file category. */
const CATEGORY_ICON: Record<string, LucideIcon> = {
  yaml: FileText,
  sql: Database,
  python: FileCode2,
  schema: Braces,
  expectations: CircleCheckBig,
}

/** Display slice of a workspace tab. File tabs carry the buffer-derived
 * fields the strip needs; designer tabs only their identity (never dirty). */
export type WorkspaceTabInfo =
  | { kind: 'file'; path: string; category: string; exists: boolean; isDirty: boolean }
  | {
      kind: 'designer'
      id: string
      pipeline: string
      flowgroup: string
      docKind?: 'flowgroup' | 'template'
    }

interface EditorTabBarProps {
  tabs: WorkspaceTabInfo[]
  activePath: string | null
  addFileOptions: ReadonlyArray<{ label: string; category: string }>
  onSelectTab: (id: string) => void
  onCloseTab: (id: string) => void
  onAddFile: (category: string) => void
}

/** Presentational tab strip for the persistent workspace: underline tabs with
 * per-kind lucide icons (per-category for files, canvas icon for designer
 * tabs), file-only dirty-dot indicators, a close affordance on every tab, and
 * an add-file dropdown. All state stays in the parent. */
export function EditorTabBar({
  tabs,
  activePath,
  addFileOptions,
  onSelectTab,
  onCloseTab,
  onAddFile,
}: EditorTabBarProps) {
  return (
    <div className="flex min-w-0 flex-1 overflow-x-auto px-2">
      {tabs.map((tab) => {
        const isFile = tab.kind === 'file'
        const isTemplateTab = !isFile && tab.docKind === 'template'
        const id = isFile ? tab.path : tab.id
        const label = isFile ? (tab.path.split('/').pop() ?? tab.path) : tab.flowgroup
        const title = isFile
          ? tab.path
          : isTemplateTab
            ? `Template — ${tab.flowgroup}`
            : `Designer — ${tab.pipeline}/${tab.flowgroup}`
        const Icon = isFile
          ? (CATEGORY_ICON[tab.category] ?? FileText)
          : isTemplateTab
            ? LayoutTemplate
            : Waypoints
        const isActive = id === activePath
        const missing = isFile && !tab.exists
        return (
          // The select and close controls are SIBLING buttons (nesting an
          // interactive close inside the tab button is invalid HTML and
          // keyboard-unreachable); the wrapper carries the shared tab chrome.
          <div
            key={id}
            className={cn(
              'group relative flex shrink-0 items-center border-b-2 text-xs font-medium transition-colors',
              isActive
                ? 'border-primary bg-card text-foreground'
                : 'border-transparent text-muted-foreground hover:text-foreground',
              missing && 'italic opacity-60',
            )}
          >
            <button
              onClick={() => onSelectTab(id)}
              title={title}
              className="flex items-center gap-1.5 py-2 pl-3 pr-1"
            >
              <Icon className="size-3.5 shrink-0" aria-hidden="true" />
              {missing && (
                <span title="File doesn't exist — edit and save to create">
                  <Plus className="size-3" aria-hidden="true" />
                </span>
              )}
              <span>{label}</span>
              {isFile && tab.isDirty && (
                <span className="ml-0.5 inline-block size-1.5 shrink-0 rounded-full bg-primary">
                  <span className="sr-only">(unsaved changes)</span>
                </span>
              )}
            </button>
            <button
              aria-label={`Close ${label}`}
              onClick={() => onCloseTab(id)}
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

      {/* Add file dropdown */}
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button
            variant="ghost"
            size="icon-xs"
            className="shrink-0 self-center text-muted-foreground"
            aria-label="Add a file tab"
          >
            <Plus aria-hidden="true" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end">
          {addFileOptions.map((opt) => {
            const Icon = CATEGORY_ICON[opt.category] ?? FileText
            return (
              <DropdownMenuItem key={opt.category} onSelect={() => onAddFile(opt.category)}>
                <Icon aria-hidden="true" />
                {opt.label}
              </DropdownMenuItem>
            )
          })}
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  )
}
