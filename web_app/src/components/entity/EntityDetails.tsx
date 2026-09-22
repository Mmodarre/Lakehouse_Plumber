import { useState } from 'react'
import { toast } from 'sonner'
import { parseDocument } from 'yaml'
import { useFileList } from '@/hooks/useFiles'
import { flattenFilePaths, resolveResourceFilePath } from '@/components/shell/explorer/explorerData'
import { deleteFlowgroupField, setFlowgroupField, type FlowgroupMeta, type TemplateParamRead } from '@/lib/flowgroup-doc'
import type { DocKind } from '@/store/workspaceStore'
import { openWorkspaceFile } from '@/workspace/openWorkspaceFile'
import { OptionalTextField } from '@/components/config/fields/OptionalTextField'
import { DraftInput } from '@/components/config/fields/DraftInput'
import { Button } from '@/components/ui/button'
import { TemplateParamsCard } from './TemplateParamsCard'
import type { FlowgroupMutator } from './useFlowgroupDoc'

const expandedFiles = new Set<string>()
export function EntityDetails({ filePath, docKind, meta, params, commit, readOnly }: {
  filePath: string; docKind: DocKind; meta: FlowgroupMeta | null; params: readonly TemplateParamRead[];
  commit: (fn: FlowgroupMutator) => boolean; readOnly: boolean;
}) {
  const [open, setOpen] = useState(() => expandedFiles.has(filePath))
  const { data: tree } = useFileList()
  const openTemplate = () => {
    if (meta?.use_template) void openWorkspaceFile(resolveResourceFilePath(flattenFilePaths(tree), 'templates', meta.use_template), { source: true })
  }
  const mapField = (key: 'variables' | 'template_parameters', text: string) => {
    if (!text.trim()) { commit((doc) => deleteFlowgroupField(doc, [key])); return }
    const parsed = parseDocument(text)
    const value = parsed.toJS()
    if (parsed.errors.length || !value || Array.isArray(value) || typeof value !== 'object' || key === 'variables' && Object.values(value).some((v) => typeof v !== 'string')) {
      toast.error(key === 'variables' ? 'Variables must be a YAML mapping of string values.' : 'Template parameters must be a YAML mapping.'); return
    }
    commit((doc) => setFlowgroupField(doc, [key], value))
  }
  return <div className="shrink-0 border-b border-border">
    <div className="flex items-center gap-2 px-3 py-1">
      <Button size="xs" variant="ghost" aria-expanded={open} onClick={() => { setOpen(!open); if (open) expandedFiles.delete(filePath); else expandedFiles.add(filePath) }}>{open ? 'Hide' : 'Show'} {docKind === 'template' ? 'template details & parameters' : 'flowgroup details'}</Button>
      {meta?.use_template && <Button variant="link" size="xs" onClick={openTemplate}>Open template: {meta.use_template}</Button>}
    </div>
    {open && <div className="grid max-h-[40vh] gap-3 overflow-auto px-4 pb-3 sm:grid-cols-2">
      <p className="text-xs text-muted-foreground sm:col-span-2">These fields update the shared document. Save the file to apply them to the next run. {meta?.inherited.length ? `Inherited from the file root: ${meta.inherited.join(', ')}.` : ''}</p>
      {(['description', 'job_name', 'use_template'] as const).map((key) => <OptionalTextField key={key} id={`entity-${key}`} label={key === 'job_name' ? 'Job name' : key === 'use_template' ? 'Template' : 'Description'} value={meta?.[key]} disabled={readOnly} onSet={(value) => commit((doc) => setFlowgroupField(doc, [key], value))} onUnset={() => commit((doc) => deleteFlowgroupField(doc, [key]))} />)}
      <label className="space-y-1 text-xs">Presets (comma separated)<DraftInput initial={meta?.presets?.join(', ') ?? ''} disabled={readOnly} aria-label="Presets" onCommit={(value) => commit((doc) => value.trim() ? setFlowgroupField(doc, ['presets'], value.split(',').map((v) => v.trim()).filter(Boolean)) : deleteFlowgroupField(doc, ['presets']))} /></label>
      {(['variables', 'template_parameters'] as const).map((key) => <label key={key} className="space-y-1 text-xs">{key === 'variables' ? 'Variables' : 'Template parameter values'} (YAML mapping)<DraftInput multiline monospace initial={meta?.[key] ? JSON.stringify(meta[key], null, 2) : ''} disabled={readOnly} aria-label={key} onCommit={(value) => mapField(key, value)} /></label>)}
      {docKind === 'template' && <div className="sm:col-span-2"><TemplateParamsCard params={params} templateName={filePath.split('/').pop() ?? filePath} commit={commit} readOnly={readOnly} /></div>}
    </div>}
  </div>
}
