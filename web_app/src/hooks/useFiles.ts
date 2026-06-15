import { useQuery } from '@tanstack/react-query'
import { fetchFiles } from '../api/files'

// The full recursive project file tree. Expand/collapse of directories is
// now purely client-side — the whole tree arrives in this single query, so
// there is no per-directory lazy fetch.
export function useFileList() {
  return useQuery({
    queryKey: ['files'],
    queryFn: fetchFiles,
  })
}
