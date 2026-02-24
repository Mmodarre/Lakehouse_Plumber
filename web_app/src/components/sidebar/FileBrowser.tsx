import { useState, useMemo, useCallback } from 'react'
import { useFileList } from '../../hooks/useFiles'
import { useGitStatus } from '../../hooks/useGitStatus'
import { useUIStore } from '../../store/uiStore'
import { fetchFilePath } from '../../api/files'
import { SkeletonLoader } from '../common/SkeletonLoader'
import { FileTreeItem } from './FileTreeItem'

type GitBadge = 'M' | 'S' | 'U'

export function FileBrowser() {
  const { data, isLoading } = useFileList()
  const { data: gitStatus } = useGitStatus()
  const openFilePath = useUIStore((s) => s.openFilePath)
  const [expandedPaths, setExpandedPaths] = useState<Set<string>>(new Set())

  const gitStatusMap = useMemo(() => {
    const map = new Map<string, GitBadge>()
    if (!gitStatus) return map
    for (const path of gitStatus.untracked) map.set(path, 'U')
    for (const path of gitStatus.modified) map.set(path, 'M')
    for (const path of gitStatus.staged) map.set(path, 'S')
    return map
  }, [gitStatus])

  const handleToggle = useCallback((path: string) => {
    setExpandedPaths((prev) => {
      const next = new Set(prev)
      if (next.has(path)) {
        next.delete(path)
      } else {
        next.add(path)
      }
      return next
    })
  }, [])

  const handleFileClick = useCallback(
    async (path: string) => {
      try {
        const result = await fetchFilePath(path)
        if (result.type === 'binary') return
        if (result.content != null) {
          openFilePath(path, result.content)
        }
      } catch {
        // 413 or network error — silently skip
      }
    },
    [openFilePath],
  )

  if (isLoading) return <SkeletonLoader lines={6} />

  return (
    <div className="space-y-0.5 px-1 py-2">
      <div className="px-2 pb-1 text-[10px] font-semibold uppercase tracking-wider text-slate-400">
        File Browser
      </div>
      {data?.items.map((item) => (
        <FileTreeItem
          key={item.name}
          name={item.name}
          path={item.name}
          type={item.type}
          depth={1}
          gitStatusMap={gitStatusMap}
          expandedPaths={expandedPaths}
          onToggle={handleToggle}
          onClick={handleFileClick}
        />
      ))}
    </div>
  )
}
