import type { FileNode } from '../../types/api'

interface FileTreeItemProps {
  node: FileNode
  depth: number
  expandedPaths: Set<string>
  onToggle: (path: string) => void
  onClick: (path: string) => void
  onDelete: (path: string) => void
}

export function FileTreeItem({
  node,
  depth,
  expandedPaths,
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
          className="flex w-full items-center gap-1.5 rounded px-2 py-1 text-left text-xs text-slate-700 hover:bg-slate-50"
          style={{ paddingLeft: depth * 16 }}
          onClick={() => onToggle(path)}
        >
          <svg
            className={`h-3 w-3 shrink-0 text-slate-400 transition-transform ${isExpanded ? 'rotate-90' : ''}`}
            fill="none" viewBox="0 0 24 24" stroke="currentColor"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
          </svg>
          <svg className="h-3.5 w-3.5 shrink-0 text-amber-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z" />
          </svg>
          <span className="truncate font-medium">{name}</span>
        </button>

        {isExpanded && (
          <div>
            {node.children?.map((child) => (
              <FileTreeItem
                key={child.path}
                node={child}
                depth={depth + 1}
                expandedPaths={expandedPaths}
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

  return (
    <div className="group flex items-center rounded hover:bg-slate-50">
      <button
        className="flex min-w-0 flex-1 items-center gap-1.5 px-2 py-1 text-left text-xs text-slate-600"
        style={{ paddingLeft: depth * 16 }}
        onClick={() => onClick(path)}
      >
        <svg className="h-3.5 w-3.5 shrink-0 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
        </svg>
        <span className="truncate">{name}</span>
      </button>
      <button
        type="button"
        title="Delete file"
        onClick={(e) => {
          e.stopPropagation()
          onDelete(path)
        }}
        className="mr-1 shrink-0 rounded p-0.5 text-slate-400 opacity-0 hover:bg-slate-200 hover:text-red-600 focus:opacity-100 group-hover:opacity-100"
      >
        <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
        </svg>
      </button>
    </div>
  )
}
