import { useMemo, useState } from 'react'
import { useFileList } from '@/hooks/useFiles'
import { flattenFilePaths } from '@/components/shell/explorer/explorerData'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { useLayoutStore } from '@/store/layoutStore'
import { buildTemplateDraft, templateReferenceForPath, validateTemplatePath } from '@/lib/template-document'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
export interface CreateTemplateDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  onCreated: (path: string) => void
  duplicate?: { sourcePath: string; sourceYaml: string }
}
export function CreateTemplateDialog(props: CreateTemplateDialogProps) {
  if (!props.open) return null
  return <Dialog open onOpenChange={props.onOpenChange}><DialogContent className="sm:max-w-lg"><CreateForm {...props} /></DialogContent></Dialog>
}
function CreateForm({ onOpenChange, onCreated, duplicate }: CreateTemplateDialogProps) {
  const { data: tree, isLoading, error: listError } = useFileList()
  const buffers = useWorkspaceStore((s) => s.buffers)
  const viewer = useLayoutStore((s) => s.viewerMode)
  const initial = duplicate?.sourcePath.replace(/\.ya?ml$/, '_copy.yaml') ?? 'templates/new_template.yaml'
  const [path, setPath] = useState(initial)
  const [name, setName] = useState(initial.split('/').pop()!.replace(/\.yaml$/, ''))
  const [nameTouched, setNameTouched] = useState(false)
  const [error, setError] = useState<string>()
  const occupied = useMemo(() => [...flattenFilePaths(tree), ...buffers.map((b) => b.path)], [tree, buffers])
  const pathError = validateTemplatePath(path, occupied)
  const submit = () => {
    if (viewer || useLayoutStore.getState().viewerMode || isLoading || listError || pathError || !name.trim()) return
    const latest = useWorkspaceStore.getState()
    if (validateTemplatePath(path, [...flattenFilePaths(tree), ...latest.buffers.map((b) => b.path)])) { setError('This path is now occupied. Choose another file name.'); return }
    try {
      const content = buildTemplateDraft(name.trim(), duplicate?.sourceYaml)
      latest.openBuffer(path, { content, originalContent: '', exists: false, isNew: true, isDirty: true })
      onCreated(path)
      onOpenChange(false)
    } catch (err) { setError(err instanceof Error ? err.message : 'Could not create this draft.') }
  }
  return <form onSubmit={(event) => { event.preventDefault(); submit() }}><DialogHeader><DialogTitle>{duplicate ? 'Duplicate template' : 'New template'}</DialogTitle><DialogDescription>{duplicate ? 'Copy the source and its parameters into a new editable draft.' : 'Start a reusable action pattern, then declare its inputs and build actions.'} Save writes the new file.</DialogDescription></DialogHeader>
    <div className="space-y-4 py-4"><label className="block space-y-1 text-sm">File path<input className="w-full rounded-md border border-input bg-background px-3 py-2 font-mono text-xs" aria-label="Template file path" disabled={viewer} value={path} onChange={(event) => { setPath(event.target.value); if (!nameTouched) setName(event.target.value.split('/').pop()!.replace(/\.yaml$/, '')) }} /></label><p className="break-all text-xs text-muted-foreground">Reference: {templateReferenceForPath(path) ?? 'Choose a .yaml path under templates/'}</p>{pathError && <p role="alert" className="text-xs text-destructive">{pathError}</p>}
      <label className="block space-y-1 text-sm">Template name<input className="w-full rounded-md border border-input bg-background px-3 py-2 text-sm" aria-label="Template name" disabled={viewer} value={name} onChange={(event) => { setNameTouched(true); setName(event.target.value) }} /></label>
      {duplicate && <p className="break-all text-xs text-muted-foreground">Copying {duplicate.sourcePath}. Existing flowgroups keep using the original.</p>}{(error || listError) && <p role="alert" className="text-xs text-destructive">{error ?? 'Could not check existing files. Close this dialog and retry after the file list loads.'}</p>}
    </div><DialogFooter><Button type="button" variant="ghost" onClick={() => onOpenChange(false)}>Cancel</Button><Button type="submit" disabled={viewer || isLoading || !!listError || !!pathError || !name.trim()}>{isLoading ? 'Checking files…' : 'Create draft'}</Button></DialogFooter>
  </form>
}
