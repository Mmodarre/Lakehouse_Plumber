import { useCallback, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { ApiError } from '@/api/client'
import { fetchFileContentWithMeta, writeFile } from '@/api/files'
import { errorMessage } from '@/lib/errors'
import {
  parseFlowgroupFile,
  renameAction,
  selectFlowgroup,
  selectTemplate,
  serializeFlowgroupFile,
} from '@/lib/flowgroup-doc'
import type { FlowgroupDocHandle } from '@/lib/flowgroup-doc'
import type { FileWriteResponse } from '@/types/api'
import type { DesignerDocKind } from './useDesignerDoc'

// ── useDesignerWrite — immediate write-through for the designer ─
//
// Every committed form edit PUTs the flowgroup file instantly. The
// authoritative content lives in the SAME react-query cache the read path
// (useDesignerDoc) subscribes to under ['file-content', <path>], so a
// successful write updates the cache and the canvas + inspector re-derive
// for free — no second source of truth, no manual dirty buffer.
//
// A commit is `(doc) => void`: it applies one or more flowgroup-doc mutators
// (setActionField / deleteActionField / renameAction …) to a handle parsed
// from the latest cached content, serializes it, and PUTs with If-Match.
// Commits are serialized on a promise chain, so a second commit that lands
// while the first PUT is in flight applies on top of the first's result
// (no interleaved lost updates). A 412 raises the conflict flag; resolution
// mirrors the Config UI (reload = discard, overwrite = re-PUT with a fresh
// etag) — see ConfigConflictDialog.

/** Mutator applied to a freshly parsed handle inside a single write. */
export type DesignerMutator = (doc: FlowgroupDocHandle) => void

type YamlWriteError = NonNullable<FileWriteResponse['yaml_error']>

interface FileContent {
  content: string
  etag: string | null
}

export interface DesignerWrite {
  /**
   * Apply `mutator` and write through. No-op when `readOnly` (hard block,
   * not just a disabled control). Fire-and-forget: errors surface as toasts,
   * a 412 raises `conflict`.
   */
  commit: (mutator: DesignerMutator) => void
  /**
   * Rename an action (addressed by canvas node id). Resolves `true` only
   * when the rename actually persisted, so the caller can re-select by the
   * new name without blanking the panel on a clash / conflict / read-only.
   */
  rename: (actionId: string, newName: string) => Promise<boolean>
  /** A PUT is in flight. */
  saving: boolean
  /** Last write was rejected with 412 — render the conflict dialog. */
  conflict: boolean
  /** `yaml_error` from the last write (the write persisted); `null` otherwise. */
  yamlError: YamlWriteError | null
  /** Conflict "Overwrite": re-PUT my edit with a freshly fetched etag. */
  overwrite: () => Promise<void>
  /** Conflict "Reload from disk": discard my edit, refetch the on-disk version. */
  reload: () => Promise<void>
  /** Dismiss the conflict without resolving (the next edit re-raises it). */
  dismissConflict: () => void
}

export function useDesignerWrite(
  filePath: string,
  flowgroup: string,
  readOnly: boolean,
  docKind: DesignerDocKind = 'flowgroup',
): DesignerWrite {
  const queryClient = useQueryClient()
  const [saving, setSaving] = useState(false)
  const [conflict, setConflict] = useState(false)
  const [yamlError, setYamlError] = useState<YamlWriteError | null>(null)

  // Refs the (stable) commit closure reads: latest readOnly, the text of a
  // write that 412'd (for overwrite), and the serialization chain tail.
  const readOnlyRef = useRef(readOnly)
  readOnlyRef.current = readOnly
  const pendingTextRef = useRef<string | null>(null)
  const chainRef = useRef<Promise<void>>(Promise.resolve())

  const put = useCallback(
    async (text: string, etag: string | null): Promise<boolean> => {
      setSaving(true)
      try {
        const res = await writeFile(filePath, text, etag)
        // Seed the shared cache with what is now on disk: the read path
        // adopts it, and the SSE-triggered refetch for our own write is an
        // etag-equal no-op.
        queryClient.setQueryData<FileContent>(['file-content', filePath], {
          content: text,
          etag: res.etag ?? null,
        })
        setYamlError(res.yaml_error ?? null)
        pendingTextRef.current = null
        setConflict(false)
        if (res.yaml_error) {
          const filename = filePath.split('/').pop() ?? filePath
          toast.error(`Saved ${filename} with a YAML syntax error`)
        }
        // No success toast: immediate write-through fires one per field edit.
        return true
      } catch (err) {
        if (err instanceof ApiError && err.status === 412) {
          pendingTextRef.current = text
          setConflict(true)
        } else {
          toast.error(errorMessage(err, 'Failed to save file'))
        }
        return false
      } finally {
        setSaving(false)
      }
    },
    [filePath, queryClient],
  )

  const applyAndPut = useCallback(
    async (mutator: DesignerMutator): Promise<boolean> => {
      const base = queryClient.getQueryData<FileContent>(['file-content', filePath])
      if (base === undefined) return false
      const file = parseFlowgroupFile(base.content)
      if (file.errors.length > 0) {
        toast.error('Cannot edit: fix the YAML syntax errors first.')
        return false
      }
      // A template tab edits the template body (its action mutators + the
      // template-param mutators both target this same document).
      const doc =
        docKind === 'template' ? selectTemplate(file)?.body : selectFlowgroup(file, flowgroup)
      if (doc === undefined) {
        toast.error(
          docKind === 'template'
            ? 'This file is no longer a template.'
            : `Flowgroup '${flowgroup}' is no longer in this file.`,
        )
        return false
      }
      let text: string
      try {
        mutator(doc)
        text = serializeFlowgroupFile(file)
      } catch (err) {
        // A mutator can throw (e.g. renameAction on a name clash).
        toast.error(errorMessage(err, 'Edit could not be applied'))
        return false
      }
      if (text === base.content) return true // no-op edit
      return put(text, base.etag)
    },
    [filePath, queryClient, flowgroup, docKind, put],
  )

  /** Enqueue a mutation on the serialization chain; resolve its success. */
  const runInChain = useCallback(
    (mutator: DesignerMutator): Promise<boolean> => {
      const result = chainRef.current.then(() => applyAndPut(mutator))
      // The chain tail must never reject, or a later commit's .then is skipped.
      chainRef.current = result.then(
        () => undefined,
        () => undefined,
      )
      return result.catch(() => false)
    },
    [applyAndPut],
  )

  const commit = useCallback(
    (mutator: DesignerMutator): void => {
      if (readOnlyRef.current) return
      void runInChain(mutator)
    },
    [runInChain],
  )

  const rename = useCallback(
    (actionId: string, newName: string): Promise<boolean> => {
      if (readOnlyRef.current) return Promise.resolve(false)
      return runInChain((doc) => renameAction(doc, actionId, newName))
    },
    [runInChain],
  )

  const overwrite = useCallback(async (): Promise<void> => {
    const text = pendingTextRef.current
    if (text === null) return
    setConflict(false)
    try {
      // Fresh on-disk etag, never the stale one: a further concurrent change
      // still 412s (no clobber).
      const fresh = await fetchFileContentWithMeta(filePath)
      await put(text, fresh.etag)
    } catch (err) {
      toast.error(errorMessage(err, 'Failed to load the on-disk version'))
      setConflict(true)
    }
  }, [filePath, put])

  const reload = useCallback(async (): Promise<void> => {
    pendingTextRef.current = null
    setConflict(false)
    await queryClient.invalidateQueries({ queryKey: ['file-content', filePath] })
  }, [filePath, queryClient])

  const dismissConflict = useCallback(() => setConflict(false), [])

  return { commit, rename, saving, conflict, yamlError, overwrite, reload, dismissConflict }
}
