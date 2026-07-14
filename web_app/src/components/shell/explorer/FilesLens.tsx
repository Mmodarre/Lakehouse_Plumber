import { FileBrowser } from '../../sidebar/FileBrowser'

// ── FilesLens — the Files explorer lens (T1.3) ───────────────
//
// The raw file tree — the escape hatch. Reuses components/sidebar/FileBrowser
// as-is (it already opens buffers via workspaceStore); no wrapping logic.

export function FilesLens() {
  return <FileBrowser />
}
