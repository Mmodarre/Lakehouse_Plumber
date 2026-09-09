import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { previewTemplate } from '@/api/template-authoring'
import { errorMessage } from '@/lib/errors'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { newTemplatePreviewSession, useTemplatePreviewStore, type TemplatePreviewSession } from '@/store/templatePreviewStore'
import { captureWorkspaceEditors, focusInvalidWorkspaceDraft } from '@/workspace/editorCommands'

const EMPTY = newTemplatePreviewSession()
function signature(source: string, state: TemplatePreviewSession, revision: number) {
  return JSON.stringify([source, state.values, state.context, state.stage, revision])
}
export function useTemplatePreview(path: string) {
  const project = useWorkspaceStore((s) => s.projectRoot)
  const buffer = useWorkspaceStore((s) => s.buffers.find((b) => b.path === path))
  const key = `${project ?? ''}::${path}`
  const session = useTemplatePreviewStore((s) => s.sessions[key] ?? EMPTY)
  const revision = useTemplatePreviewStore((s) => s.dependenciesRevision)
  const update = useCallback((patch: Partial<TemplatePreviewSession>) => useTemplatePreviewStore.getState().update(key, patch), [key])
  const currentSignature = useMemo(() => signature(buffer?.content ?? '', session, revision), [buffer?.content, session, revision])
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const request = useRef(0)
  const controller = useRef<AbortController | null>(null)
  useEffect(() => () => { request.current++; controller.current?.abort() }, [key])
  const run = async () => {
    captureWorkspaceEditors()
    if (focusInvalidWorkspaceDraft()) { setError('Correct the sample value before previewing.'); return }
    const ws = useWorkspaceStore.getState()
    const current = ws.buffers.find((b) => b.path === path)
    if (!current || current.loading || current.loadFailed || ws.projectRoot !== project) return
    const state = useTemplatePreviewStore.getState()
    const sample = state.sessions[key] ?? EMPTY
    const requestedSignature = signature(current.content, sample, state.dependenciesRevision)
    controller.current?.abort()
    const abort = new AbortController(); controller.current = abort
    const id = ++request.current
    setBusy(true); setError(null)
    try {
      const result = await previewTemplate({ source_path: path, source_yaml: current.content,
        request_revision: `${Date.now()}-${id}`, stage: sample.stage, sample_parameters: sample.values,
        ...(sample.stage === 'resolved' ? { context: sample.context } : {}) }, abort.signal)
      const now = useWorkspaceStore.getState()
      const latest = useTemplatePreviewStore.getState()
      const source = now.buffers.find((b) => b.path === path)
      if (id !== request.current || now.projectRoot !== project || !source) return
      if (requestedSignature !== signature(source.content, latest.sessions[key] ?? EMPTY, latest.dependenciesRevision)) {
        setError('Inputs or saved dependencies changed during preview. Refresh to check the current draft.')
        return
      }
      update({ result, resultSignature: requestedSignature })
    } catch (cause) {
      if (id === request.current && !abort.signal.aborted) setError(errorMessage(cause, 'Could not preview this template'))
    } finally { if (id === request.current) setBusy(false) }
  }
  const cancel = () => { request.current++; controller.current?.abort(); setBusy(false) }
  return { buffer, session, update, busy, error, run, cancel,
    stale: !!session.result && (session.resultSignature !== currentSignature || session.result.status === 'stale') }
}
