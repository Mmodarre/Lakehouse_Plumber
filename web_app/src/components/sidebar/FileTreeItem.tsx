import type { LucideIcon } from 'lucide-react'
import {
  Braces,
  ChevronRight,
  Database,
  File,
  FileCode2,
  FileText,
  Folder,
  FolderOpen,
  Trash2,
} from 'lucide-react'
import { cn } from '../../lib/utils'
import type { FileNode } from '../../types/api'

/** Client-side junk filter: hides dotfiles/dot-directories (.DS_Store, .git, …).
 * FileBrowser applies the same name-based predicate to the root children. */
function isVisibleNode(node: FileNode): boolean {
  return !node.name.startsWith('.')
}

type FileIconSpec = {
  icon: LucideIcon
  className: string
}

const FILE_ICONS: Record<string, FileIconSpec> = {
  yaml: { icon: FileText, className: 'text-kind-load/70' },
  yml: { icon: FileText, className: 'text-kind-load/70' },
  sql: { icon: Database, className: 'text-kind-write/70' },
  py: { icon: FileCode2, className: 'text-kind-transform/70' },
  json: { icon: Braces, className: 'text-kind-test/70' },
}

function fileIconFor(name: string): FileIconSpec {
  const dot = name.lastIndexOf('.')
  const ext = dot > 0 ? name.slice(dot + 1).toLowerCase() : ''
  return FILE_ICONS[ext] ?? { icon: File, className: 'text-muted-foreground/70' }
}

/** Vertical guide line per ancestor level (12px per indent unit). */
function IndentGuides({ depth }: { depth: number }) {
  if (depth <= 1) return null
  return (
    <>
      {Array.from({ length: depth - 1 }, (_, i) => (
        <span
          key={i}
          aria-hidden="true"
          className="h-full w-3 shrink-0 self-stretch border-l border-border/50"
        />
      ))}
    </>
  )
}

interface FileTreeItemProps {
  node: FileNode
  depth: number
  expandedPaths: Set<string>
  /** Path of the file currently open in the editor (active-row rail). */
  activePath: string | null
  onToggle: (path: string) => void
  onClick: (path: string) => void
  onDelete: (path: string) => void
}

export function FileTreeItem({
  node,
  depth,
  expandedPaths,
  activePath,
  onToggle,
  onClick,
  onDelete,
}: FileTreeItemProps) {
  const { name, path, type } = node
  const isExpanded = expandedPaths.has(path)

  if (type === 'directory') {
    return (
      <div>
        <button
          type="button"
          className="flex h-[26px] w-full items-center gap-1.5 rounded-sm px-2 text-left text-sm text-foreground transition-colors duration-150 hover:bg-muted/60 motion-reduce:transition-none"
          onClick={() => onToggle(path)}
        >
          <IndentGuides depth={depth} />
          <ChevronRight
            aria-hidden="true"
            className={cn(
              'size-3.5 shrink-0 text-muted-foreground transition-transform duration-150 motion-reduce:transition-none',
              isExpanded && 'rotate-90',
            )}
          />
          {isExpanded ? (
            <FolderOpen aria-hidden="true" className="size-3.5 shrink-0 text-muted-foreground/70" />
          ) : (
            <Folder aria-hidden="true" className="size-3.5 shrink-0 text-muted-foreground/70" />
          )}
          <span className="truncate font-medium">{name}</span>
        </button>

        {isExpanded && (
          <div>
            {node.children?.filter(isVisibleNode).map((child) => (
              <FileTreeItem
                key={child.path}
                node={child}
                depth={depth + 1}
                expandedPaths={expandedPaths}
                activePath={activePath}
                onToggle={onToggle}
                onClick={onClick}
                onDelete={onDelete}
              />
            ))}
          </div>
        )}
      </div>
    )
  }

  const { icon: Icon, className: iconClass } = fileIconFor(name)
  const isActive = activePath === path

  return (
    <div
      className={cn(
        'group relative flex h-[26px] items-center rounded-sm transition-colors duration-150 motion-reduce:transition-none',
        isActive ? 'bg-accent' : 'hover:bg-muted/60',
      )}
    >
      {/* Active-row rail */}
      {isActive && (
        <span aria-hidden="true" className="absolute inset-y-0.5 left-0 w-0.5 rounded-full bg-primary" />
      )}
      <button
        type="button"
        className="flex h-full min-w-0 flex-1 items-center gap-1.5 px-2 text-left text-sm"
        onClick={() => onClick(path)}
      >
        <IndentGuides depth={depth} />
        {/* Chevron-width spacer keeps file icons aligned with folder icons */}
        <span aria-hidden="true" className="w-3.5 shrink-0" />
        <Icon aria-hidden="true" className={cn('size-3.5 shrink-0', iconClass)} />
        <span
          className={cn(
            'truncate',
            isActive
              ? 'text-accent-foreground'
              : 'text-muted-foreground group-hover:text-foreground',
          )}
        >
          {name}
        </span>
      </button>
      <button
        type="button"
        aria-label={`Delete ${name}`}
        title="Delete file"
        onClick={(e) => {
          e.stopPropagation()
          onDelete(path)
        }}
        className="mr-1 shrink-0 rounded-sm p-0.5 text-muted-foreground opacity-0 transition-opacity duration-150 group-hover:opacity-100 hover:bg-muted hover:text-destructive focus-visible:opacity-100 motion-reduce:transition-none"
      >
        <Trash2 aria-hidden="true" className="size-3.5" />
      </button>
    </div>
  )
}
