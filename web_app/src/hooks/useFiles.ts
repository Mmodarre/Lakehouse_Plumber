import { useQuery } from '@tanstack/react-query'
import { fetchFiles, fetchFilePath } from '../api/files'

export function useFileList() {
  return useQuery({
    queryKey: ['files'],
    queryFn: fetchFiles,
  })
}

export function useFilePath(path: string | null) {
  return useQuery({
    queryKey: ['files', path],
    queryFn: () => fetchFilePath(path!),
    enabled: !!path,
  })
}
