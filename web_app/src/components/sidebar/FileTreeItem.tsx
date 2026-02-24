import { useFilePath } from '../../hooks/useFiles'
import { SkeletonLoader } from '../common/SkeletonLoader'

type GitBadge = 'M' | 'S' | 'U'

interface FileTreeItemProps {
  name: string
  path: string
  type: 'file' | 'directory'
  depth: number
  gitStatusMap: Map<string, GitBadge>
  expandedPaths: Set<string>
  onToggle: (path: string) => void
  onClick: (path: string) => void
}

const badgeColors: Record<GitBadge, string> = {
  M: 'text-amber-500',
  S: 'text-green-500',
  U: 'text-slate-400',
}

export function FileTreeItem({
  name,
  path,
  type,
  depth,
  gitStatusMap,
  expandedPaths,
  onToggle,
  onClick,
}: FileTreeItemProps) {
  const isExpanded = expandedPaths.has(path)
  const { data, isLoading } = useFilePath(type === 'directory' && isExpanded ? path : null)
  const badge = gitStatusMap.get(path) ?? null

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
            {isLoading && <SkeletonLoader lines={2} />}
            {data?.items?.map((child) => (
              <FileTreeItem
                key={child.name}
                name={child.name}
                path={`${path}/${child.name}`}
                type={child.type}
                depth={depth + 1}
                gitStatusMap={gitStatusMap}
                expandedPaths={expandedPaths}
                onToggle={onToggle}
                onClick={onClick}
              />
            ))}
          </div>
        )}
      </div>
    )
  }

  return (
    <button
      className="flex w-full items-center gap-1.5 rounded px-2 py-1 text-left text-xs text-slate-600 hover:bg-slate-50"
      style={{ paddingLeft: depth * 16 }}
      onClick={() => onClick(path)}
    >
      <svg className="h-3.5 w-3.5 shrink-0 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
      </svg>
      <span className="truncate">{name}</span>
      {badge && (
        <span className={`ml-auto shrink-0 text-[10px] font-semibold ${badgeColors[badge]}`}>
          {badge}
        </span>
      )}
    </button>
  )
}
