import { useMemo, useState } from 'react'
import { useFileList } from '@/hooks/useFiles'
import type { FileNode } from '@/types/api'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { FileTreeItem } from '../sidebar/FileTreeItem'

interface FilePickerProps {
  /** Extensions to keep, each including the dot (e.g. ['.sql', '.py']).
   * Matched case-insensitively against the file name. */
  accept: string[]
  /** Restrict the visible tree to files under this project-relative path. */
  baseDir?: string
  /** Called with the selected file's project-relative path. */
  onPick: (path: string) => void
  /** Called when the dialog is dismissed without a selection. */
  onClose: () => void
}

/** Prune a copy of the tree to files matching `accept` (and under `baseDir`
 * if given), dropping non-matching files and any directory left empty.
 * The presentational `FileTreeItem` is never forked — it renders whatever
 * pruned nodes it is handed. Returns null when nothing survives. */
function pruneTree(
  node: FileNode,
  matchesAccept: (name: string) => boolean,
  underBaseDir: (path: string) => boolean,
): FileNode | null {
  if (node.type === 'file') {
    return matchesAccept(node.name) && underBaseDir(node.path) ? node : null
  }
  const kept = (node.children ?? [])
    .map((child) => pruneTree(child, matchesAccept, underBaseDir))
    .filter((child): child is FileNode => child !== null)
  if (kept.length === 0) return null
  return { ...node, children: kept }
}

/** All directory paths in the pruned tree — expanded by default so the
 * filtered results are visible without manual clicking. */
function collectDirPaths(node: FileNode, into: Set<string>): void {
  if (node.type !== 'directory') return
  into.add(node.path)
  node.children?.forEach((child) => collectDirPaths(child, into))
}

export function FilePicker({ accept, baseDir, onPick, onClose }: FilePickerProps) {
  const { data, isLoading, error } = useFileList()

  const prunedRoot = useMemo(() => {
    if (!data) return null
    const exts = accept.map((ext) => ext.toLowerCase())
    const matchesAccept = (name: string) => {
      const lower = name.toLowerCase()
      return exts.some((ext) => lower.endsWith(ext))
    }
    const underBaseDir = (path: string) =>
      !baseDir || path === baseDir || path.startsWith(`${baseDir}/`)
    return pruneTree(data, matchesAccept, underBaseDir)
  }, [data, accept, baseDir])

  const defaultExpanded = useMemo(() => {
    const paths = new Set<string>()
    if (prunedRoot) collectDirPaths(prunedRoot, paths)
    return paths
  }, [prunedRoot])

  // `null` = follow the default (all pruned dirs expanded); once the user
  // toggles anything we track their explicit set instead.
  const [override, setOverride] = useState<Set<string> | null>(null)
  const expandedPaths = override ?? defaultExpanded

  const handleToggle = (path: string) => {
    setOverride((prev) => {
      const next = new Set(prev ?? defaultExpanded)
      if (next.has(path)) next.delete(path)
      else next.add(path)
      return next
    })
  }

  const topLevel = prunedRoot?.children?.filter((node) => !node.name.startsWith('.')) ?? []

  let body: React.ReactNode
  if (isLoading) {
    body = <p className="px-2 py-6 text-center text-sm text-muted-foreground">Loading files…</p>
  } else if (error) {
    body = <p className="px-2 py-6 text-center text-sm text-destructive">Could not load files.</p>
  } else if (topLevel.length === 0) {
    body = (
      <p className="px-2 py-6 text-center text-sm text-muted-foreground">
        No matching files.
      </p>
    )
  } else {
    body = topLevel.map((node) => (
      <FileTreeItem
        key={node.path}
        node={node}
        depth={1}
        expandedPaths={expandedPaths}
        activePath={null}
        onToggle={handleToggle}
        // FileTreeItem only fires onClick for file rows (directories fire
        // onToggle), so every path reaching here is a real file to pick.
        onClick={onPick}
        onDelete={() => {}}
      />
    ))
  }

  return (
    <Dialog
      open
      onOpenChange={(next) => {
        if (!next) onClose()
      }}
    >
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Choose a file</DialogTitle>
          <DialogDescription>
            Select an existing {accept.join(', ')} file from the project.
          </DialogDescription>
        </DialogHeader>
        <div className="max-h-[50vh] overflow-y-auto">{body}</div>
      </DialogContent>
    </Dialog>
  )
}
