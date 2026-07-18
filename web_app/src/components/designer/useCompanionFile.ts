import { useCallback } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { ApiError } from '@/api/client'
import { IF_MATCH_CREATE_ONLY, fetchFileContentWithMeta, writeFile } from '@/api/files'
import { errorMessage } from '@/lib/errors'

// ── useCompanionFile — existence + scaffolding for file-backed refs ─
//
// LHP resolves `sql_path` / `module_path` / `expectations_file` relative to
// the project root (generators + external_file_loader.resolve_external_file_path
// join `spec_dir`/`project_root` with the relative path). That is exactly the
// project-relative space the files API addresses, so the field value IS the
// files-API path — no rebasing. A GET that 404s is the definitive
// "missing" signal (files.py).
//
// Only static, project-relative paths are checkable: a substitution token
// can't be resolved client-side, and an absolute path is outside the tree.

export type CompanionStatus = 'unavailable' | 'checking' | 'exists' | 'missing'

export interface CompanionFile {
  status: CompanionStatus
  /** Create the file with `stub` content (auto-creates parent dirs). Writes to
   * `atPath` when given (a path proposed for a field that is still empty), else
   * the hook's own `path`. Resolves true when the file exists afterwards
   * (created, or already there). */
  create: (stub: string, atPath?: string) => Promise<boolean>
}

/** The checkable project-relative path for a field value, or `null`. */
export function companionCheckablePath(value: unknown): string | null {
  if (typeof value !== 'string') return null
  const path = value.trim()
  if (path === '') return null
  if (path.startsWith('/') || /^[A-Za-z]:[\\/]/.test(path)) return null // absolute
  if (/\$\{[^}]*\}|\{\{[^}]*\}\}|%\{[^}]*\}/.test(path)) return null // substitution token
  return path
}

export function useCompanionFile(path: string | null): CompanionFile {
  const queryClient = useQueryClient()

  const query = useQuery({
    queryKey: ['file-exists', path],
    enabled: path !== null,
    queryFn: async () => {
      try {
        await fetchFileContentWithMeta(path as string)
        return true
      } catch (err) {
        if (err instanceof ApiError && err.status === 404) return false
        throw err
      }
    },
  })

  const create = useCallback(
    async (stub: string, atPath?: string): Promise<boolean> => {
      const target = atPath ?? path
      if (target === null) return false
      try {
        // Create-only If-Match: 412 means it already exists (nothing to do).
        await writeFile(target, stub, IF_MATCH_CREATE_ONLY)
      } catch (err) {
        if (!(err instanceof ApiError && err.status === 412)) {
          toast.error(errorMessage(err, 'Failed to create file'))
          return false
        }
      }
      await queryClient.invalidateQueries({ queryKey: ['file-exists', target] })
      await queryClient.invalidateQueries({ queryKey: ['files'] })
      return true
    },
    [path, queryClient],
  )

  let status: CompanionStatus
  if (path === null) status = 'unavailable'
  else if (query.isPending) status = 'checking'
  else if (query.isError) status = 'unavailable'
  else status = query.data ? 'exists' : 'missing'

  return { status, create }
}
