import { lazy, Suspense, useEffect, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { fetchTemplateSource } from '@/api/template-authoring'
import { errorMessage } from '@/lib/errors'
import { templateReferenceForPath } from '@/lib/template-document'
import { useWorkspaceStore, isReadOnlyPath } from '@/store/workspaceStore'
import { useLayoutStore } from '@/store/layoutStore'
import { useUIStore } from '@/store/uiStore'
import { captureWorkspaceEditors, focusInvalidWorkspaceDraft } from '@/workspace/editorCommands'
import { openWorkspaceFile } from '@/workspace/openWorkspaceFile'
import { Button } from '@/components/ui/button'

const CreateTemplateDialog = lazy(() => import('./CreateTemplateDialog').then((m) => ({ default: m.CreateTemplateDialog })))
export function TemplateUseActions({ path, onSave }: { path: string; onSave: () => Promise<boolean> }) {
  const queryClient = useQueryClient()
  const buffer = useWorkspaceStore((s) => s.buffers.find((b) => b.path === path))
  const viewer = useLayoutStore((s) => s.viewerMode)
  const [busy, setBusy] = useState(false)
  const [duplicate, setDuplicate] = useState<{ sourcePath: string; sourceYaml: string } | null>(null)
  const alive = useRef(false)
  useEffect(() => { alive.current = true; return () => { alive.current = false } }, [])
  const reference = templateReferenceForPath(path)
  const unavailable = !buffer || buffer.loading || buffer.loadFailed || busy || buffer.isSaving || viewer || isReadOnlyPath(path)
  const handleUseTemplate = async () => {
    if (unavailable || !reference) return
    captureWorkspaceEditors()
    if (focusInvalidWorkspaceDraft()) { toast.error('Correct this value or press Escape to restore it before using the template.'); return }
    const project = useWorkspaceStore.getState().projectRoot
    setBusy(true)
    try {
      const current = useWorkspaceStore.getState().buffers.find((b) => b.path === path)
      if (!current) return
      if ((current.isDirty || !current.exists) && !await onSave()) return
      captureWorkspaceEditors()
      const saved = useWorkspaceStore.getState().buffers.find((b) => b.path === path)
      if (!alive.current || project !== useWorkspaceStore.getState().projectRoot || !saved || useLayoutStore.getState().viewerMode) return
      if (saved.isDirty) { toast.info('The template changed while saving. Save these edits before using it.'); return }
      const stillCurrent = () => {
        if (!alive.current || project !== useWorkspaceStore.getState().projectRoot || useLayoutStore.getState().viewerMode || isReadOnlyPath(path)) return false
        captureWorkspaceEditors()
        const latest = useWorkspaceStore.getState().buffers.find((b) => b.path === path)
        return !!latest && !latest.isDirty && latest.exists && latest.content === saved.content && latest.etag === saved.etag
      }
      const detail = await fetchTemplateSource(path)
      if (!stillCurrent()) { if (alive.current) toast.info('The template changed. Save the current draft before using it.'); return }
      if (detail.template.state !== 'ready' || !detail.template.reference) { toast.error(detail.template.diagnostics[0]?.message ?? 'Fix the template before using it.'); return }
      queryClient.setQueryData(['template', path], detail)
      await queryClient.invalidateQueries({ queryKey: ['templates'] })
      if (!stillCurrent()) return
      useUIStore.getState().openCreateFlowgroupDialog({ templatePath: path })
    } catch (error) { if (alive.current) toast.error(errorMessage(error, 'Could not use this template')) }
    finally { if (alive.current) setBusy(false) }
  }
  return <div className="flex flex-wrap items-center gap-2 border-b border-border px-4 py-2 text-xs">
    <span className="min-w-0 flex-1 break-all font-mono text-muted-foreground">{path}</span>
    {!reference && <span className="text-warning">Use a .yaml copy to invoke this template.</span>}
    <Button size="sm" variant="outline" disabled={unavailable} onClick={() => { captureWorkspaceEditors(); const current = useWorkspaceStore.getState().buffers.find((b) => b.path === path); if (current) setDuplicate({ sourcePath: path, sourceYaml: current.content }) }}>Duplicate</Button>
    <Button size="sm" disabled={unavailable || !reference} onClick={() => { void handleUseTemplate() }}>{busy ? 'Preparing…' : buffer?.isDirty || !buffer?.exists ? 'Save and use' : 'Use template'}</Button>
    {duplicate && <Suspense fallback={null}><CreateTemplateDialog open onOpenChange={(open) => { if (!open) setDuplicate(null) }} duplicate={duplicate} onCreated={(created) => { void openWorkspaceFile(created) }} /></Suspense>}
  </div>
}
