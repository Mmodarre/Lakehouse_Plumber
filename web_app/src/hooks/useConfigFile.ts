import { useCallback, useEffect, useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import type { YAMLError } from 'yaml'
import { fetchFileContentWithMeta, writeFile } from '../api/files'
import { ApiError } from '../api/client'
import { errorMessage } from '../lib/errors'
import { parseConfigFile, serializeConfigFile } from '../lib/yaml-doc'
import type { ConfigFileHandle } from '../lib/yaml-doc'
import type { FileWriteResponse } from '../types/api'

// ── useConfigFile — the Config UI's shared file-state core ───
//
// One hook instance owns the round-trip lifecycle of a single config file
// (lhp.yaml / pipeline_config* / job_config*): fetch + parse into a
// comment-preserving yaml-doc handle, dirty tracking across form edits,
// optimistic-concurrency save (If-Match / 412), and external-change
// detection via the push channel (usePushChannel invalidates
// ['file-content', <path>] when SSE reports the file changed on disk).
//
// IMPORTANT: this file must only ever be imported from the lazy Config
// chunk (pages/ConfigurationPage and components/config/*) — it pulls in
// lib/yaml-doc and with it the `yaml` package, which must stay out of the
// eager app bundle.
//
// Validation issues (config-model) are deliberately NOT this hook's job:
// the per-surface forms compute them from `handle` + `version`.

/** Structured YAML diagnostic from a write response (1-based line/column). */
export type YamlWriteError = NonNullable<FileWriteResponse['yaml_error']>

export interface UseConfigFileResult {
  /** The project-relative path this state belongs to (`null` = no file). */
  path: string | null
  /** First content fetch for this path still in flight. */
  isLoading: boolean
  /** Content fetch failed — user-facing message, `null` otherwise. */
  loadError: string | null
  /**
   * Comment-preserving parsed handle (see lib/yaml-doc), `null` until
   * loaded. The handle is MUTABLE state: read derived data in a
   * `useMemo` keyed on `[handle, version]`, never on `handle` alone.
   */
  handle: ConfigFileHandle | null
  /** Bumped on every mutation/adoption — memo dependency for `handle`. */
  version: number
  /** YAML parse errors on the current handle; mutations throw while non-empty. */
  errors: readonly YAMLError[]
  /** Unsaved local edits exist. */
  dirty: boolean
  /** A save/overwrite is in flight. */
  saving: boolean
  /** Last save was rejected with 412 — render ConfigConflictDialog. */
  conflict: boolean
  /** File changed on disk while dirty — render ExternalChangeBanner. */
  externalChange: boolean
  /** `yaml_error` of the last write response (the write persisted). */
  yamlError: YamlWriteError | null
  /** Optimistic-concurrency token of the content the handle was parsed from. */
  etag: string | null
  /**
   * Apply a form edit: `fn` mutates the handle in place (setPath /
   * deletePath / addDocument …), then the hook marks the file dirty and
   * bumps `version`. Throws (from yaml-doc) if the handle has parse
   * errors or nothing is loaded — callers must gate editing on
   * `errors.length === 0 && handle !== null`.
   */
  mutate: (fn: (handle: ConfigFileHandle) => void) => void
  /**
   * Serialize the handle and PUT it with `If-Match: etag`. On success the
   * handle is re-parsed FROM THE SERIALIZED TEXT (so later edits stay
   * byte-surgical against the new on-disk source), `etag` advances,
   * `dirty` clears. A 412 sets `conflict` instead. Returns success.
   */
  save: () => Promise<boolean>
  /**
   * Discard local edits: refetch the on-disk content and adopt it
   * (clears dirty / conflict / externalChange). Never writes.
   */
  reload: () => Promise<void>
  /**
   * Conflict resolution "save anyway": GET the file for a FRESH etag
   * (never the stale one — a further concurrent change still 412s),
   * then re-PUT my serialized content. Returns success.
   */
  overwrite: () => Promise<boolean>
  /** Banner "Keep my changes": dismiss the external-change flag. */
  keepMine: () => void
  /**
   * Dialog "Cancel": clear the conflict flag without resolving. The next
   * save still carries the stale etag, so it 412s and re-raises the flag.
   */
  dismissConflict: () => void
}

interface LocalState {
  /** Path the handle/etag below belong to (guards path switches). */
  path: string | null
  handle: ConfigFileHandle | null
  etag: string | null
  dirty: boolean
  conflict: boolean
  externalChange: boolean
  yamlError: YamlWriteError | null
  version: number
}

const EMPTY_STATE: LocalState = {
  path: null,
  handle: null,
  etag: null,
  dirty: false,
  conflict: false,
  externalChange: false,
  yamlError: null,
  version: 0,
}

const NO_ERRORS: readonly YAMLError[] = []

/** Adoption of a freshly fetched/saved source into local state. */
function adopted(prev: LocalState, path: string, content: string, etag: string | null): LocalState {
  return {
    path,
    handle: parseConfigFile(content),
    etag,
    dirty: false,
    conflict: false,
    externalChange: false,
    yamlError: null,
    version: prev.version + 1,
  }
}

export function useConfigFile(path: string | null): UseConfigFileResult {
  const queryClient = useQueryClient()
  const query = useQuery({
    // Coordinate with usePushChannel: SSE file-changed events invalidate
    // ['file-content', <path>] per changed path.
    queryKey: ['file-content', path],
    queryFn: () => fetchFileContentWithMeta(path as string),
    enabled: path !== null,
  })

  const [saving, setSaving] = useState(false)
  const [local, setLocal] = useState<LocalState>(EMPTY_STATE)

  // Callbacks read the CURRENT state through this ref (not through their
  // closure) so a save right after a mutate serializes the latest handle.
  const stateRef = useRef(local)
  stateRef.current = local

  // Adopt fetched content. Three cases:
  //  • different path (first load / picker switch): adopt unconditionally;
  //  • same path, same etag: no-op (our own save already adopted);
  //  • same path, new etag: the file changed on disk — adopt silently
  //    when clean, or raise the externalChange flag when dirty (never
  //    clobber unsaved form edits).
  const data = query.data
  useEffect(() => {
    if (path === null) {
      setLocal((prev) => (prev.path === null ? prev : EMPTY_STATE))
      return
    }
    if (data === undefined) return
    setLocal((prev) => {
      if (prev.path !== path) return adopted(prev, path, data.content, data.etag)
      if (data.etag === prev.etag) return prev
      if (prev.dirty) return prev.externalChange ? prev : { ...prev, externalChange: true }
      return adopted(prev, path, data.content, data.etag)
    })
  }, [path, data])

  const mutate = useCallback((fn: (handle: ConfigFileHandle) => void) => {
    const s = stateRef.current
    if (s.handle === null) {
      throw new Error('useConfigFile.mutate: no file is loaded')
    }
    // Mutations run OUTSIDE the state updater (updaters must stay pure —
    // StrictMode double-invokes them, which would apply edits twice).
    fn(s.handle)
    setLocal((prev) => ({ ...prev, dirty: true, version: prev.version + 1 }))
  }, [])

  /** Shared success path of save()/overwrite(). */
  const adoptWrite = useCallback(
    (targetPath: string, text: string, res: FileWriteResponse) => {
      const etag = res.etag ?? null
      // Seed the query cache with what is now on disk, so the refetch the
      // push channel triggers for our own write is an etag-equal no-op.
      queryClient.setQueryData(['file-content', targetPath], { content: text, etag })
      const filename = targetPath.split('/').pop() ?? targetPath
      if (res.yaml_error) {
        toast.error(`Saved ${filename} with a YAML syntax error`)
      } else {
        toast.success(`Saved ${filename}`)
      }
      setLocal((prev) => ({
        ...adopted(prev, targetPath, text, etag),
        yamlError: res.yaml_error ?? null,
      }))
    },
    [queryClient],
  )

  /** PUT `text` with `etag`; maps 412 → conflict, other failures → toast. */
  const putContent = useCallback(
    async (targetPath: string, text: string, etag: string | null): Promise<boolean> => {
      setSaving(true)
      try {
        const res = await writeFile(targetPath, text, etag)
        adoptWrite(targetPath, text, res)
        return true
      } catch (err) {
        if (err instanceof ApiError && err.status === 412) {
          // Attribute the conflict to the file the save actually targeted:
          // if the hook has since switched paths, the late 412 belongs to
          // the OLD file and must not flag the new one.
          setLocal((prev) => (prev.path === targetPath ? { ...prev, conflict: true } : prev))
        } else {
          toast.error(errorMessage(err, 'Failed to save file'))
        }
        return false
      } finally {
        setSaving(false)
      }
    },
    [adoptWrite],
  )

  const save = useCallback(async (): Promise<boolean> => {
    const s = stateRef.current
    if (path === null || s.path !== path || s.handle === null) return false
    if (s.handle.errors.length > 0) return false
    return putContent(path, serializeConfigFile(s.handle), s.etag)
  }, [path, putContent])

  const overwrite = useCallback(async (): Promise<boolean> => {
    const s = stateRef.current
    if (path === null || s.path !== path || s.handle === null) return false
    if (s.handle.errors.length > 0) return false
    setSaving(true)
    try {
      // Fresh on-disk etag, NEVER the stale one: if the file changes again
      // between this GET and the PUT, the write still 412s (no clobber).
      const fresh = await fetchFileContentWithMeta(path)
      return await putContent(path, serializeConfigFile(s.handle), fresh.etag)
    } catch (err) {
      toast.error(errorMessage(err, 'Failed to load the on-disk version'))
      return false
    } finally {
      setSaving(false)
    }
  }, [path, putContent])

  const reload = useCallback(async (): Promise<void> => {
    if (path === null) return
    const result = await query.refetch()
    const fresh = result.data
    if (fresh === undefined) {
      toast.error(errorMessage(result.error, 'Failed to reload file'))
      return
    }
    setLocal((prev) => adopted(prev, path, fresh.content, fresh.etag))
  }, [path, query])

  const keepMine = useCallback(() => {
    setLocal((prev) => (prev.externalChange ? { ...prev, externalChange: false } : prev))
  }, [])

  const dismissConflict = useCallback(() => {
    setLocal((prev) => (prev.conflict ? { ...prev, conflict: false } : prev))
  }, [])

  // Guard against the render window where the path changed but the
  // adoption effect has not run yet: never expose another path's state.
  const active = local.path === path ? local : EMPTY_STATE

  return {
    path,
    isLoading: path !== null && query.isPending,
    loadError: query.isError ? errorMessage(query.error, 'Failed to load file') : null,
    handle: active.handle,
    version: active.version,
    errors: active.handle?.errors ?? NO_ERRORS,
    dirty: active.dirty,
    saving,
    conflict: active.conflict,
    externalChange: active.externalChange,
    yamlError: active.yamlError,
    etag: active.etag,
    mutate,
    save,
    reload,
    overwrite,
    keepMine,
    dismissConflict,
  }
}
