import { lazy, Suspense, useId, useMemo, useState } from 'react'
import { stringify } from 'yaml'
import type { TemplateAuthoringParameter, TemplatePreviewRequest } from '@/api/template-authoring'
import { useTemplatePreview } from '@/hooks/useTemplatePreview'
import { useEnvironments } from '@/hooks/useEnvironments'
import { parseFlowgroupFile, readTemplateParams, selectTemplate } from '@/lib/flowgroup-doc'
import { openWorkspaceFile } from '@/workspace/openWorkspaceFile'
import { Button } from '@/components/ui/button'
import { TemplateInvocationParams } from './TemplateInvocationParams'
import { StringListEditor } from '@/components/config/fields/StringListEditor'
import { KeyValueMapEditor } from '@/components/config/fields/KeyValueMapEditor'
import { loadBufferContent } from '@/components/workspace/flowgroupBuffers'

const TemplatePreviewGraph = lazy(() => import('./TemplatePreviewGraph').then((m) => ({ default: m.TemplatePreviewGraph })))
const labels = { inspect: 'Check structure', expanded: 'Expanded actions', resolved: 'Resolved flowgroup' } as const
export function TemplatePreview({ path, tabId }: { path: string; tabId: string }) {
  const sampleId = useId()
  const preview = useTemplatePreview(path)
  const environments = useEnvironments()
  const [valuesValid, setValuesValid] = useState(true)
  const [editingSample, setEditingSample] = useState(false)
  const [showGraph, setShowGraph] = useState(false)
  const draft = useMemo(() => {
    if (!preview.buffer) return { params: [], error: null }
    const file = parseFlowgroupFile(preview.buffer.content)
    if (file.errors.length) return { params: [], error: 'Fix YAML syntax in Code, or use Check structure for diagnostics.' }
    const template = selectTemplate(file)
    if (!template) return { params: [], error: 'This file does not contain a recognised template. Check its structure or open Code.' }
    const params: TemplateAuthoringParameter[] = readTemplateParams(template).map((p) => ({ name: p.name, required: p.required === true, has_default: Object.hasOwn(p.raw, 'default'), default: p.default, declared_type: p.type, description: p.description }))
    return { params, error: null }
  }, [preview.buffer])
  if (preview.buffer?.loadFailed) return <div role="alert" className="p-4">Could not load template. <Button onClick={() => { void loadBufferContent(path) }}>Retry</Button></div>
  if (!preview.buffer || preview.buffer.loading) return <p role="status" className="p-4 text-sm">Loading template…</p>
  const { session } = preview
  const result = session.result
  const stale = preview.stale || (!!result && (!valuesValid || editingSample))
  const output = result?.stage === 'resolved' ? result.resolved_flowgroup : result?.expanded_actions
  const actions = result?.stage === 'resolved' && Array.isArray(result.resolved_flowgroup?.actions) ? result.resolved_flowgroup.actions as Record<string, unknown>[] : result?.expanded_actions
  const envNames = environments.data?.environments ?? []
  return <div className="min-h-0 flex-1 overflow-auto p-4" data-tab-id={tabId}
    onChangeCapture={(event) => { if (event.target instanceof HTMLElement && event.target.hasAttribute('data-workspace-draft')) setEditingSample(true) }}
    onBlurCapture={() => setEditingSample(false)}
    onKeyUpCapture={(event) => { if (event.key === 'Escape' || event.key === 'Enter' && event.target instanceof HTMLInputElement && !event.nativeEvent.isComposing) setEditingSample(false) }}>

    <div className="mx-auto max-w-5xl space-y-4">
      <div><h2 className="text-base font-semibold">Try this template</h2><p className="mt-1 text-sm text-muted-foreground">Check the current draft with sample values. Samples stay in this editor and do not change template defaults.</p></div>
      <div className="flex flex-wrap items-end gap-3">
        <label className="space-y-1 text-xs"><span className="block">Preview level</span><select aria-label="Preview level" value={session.stage} onChange={(event) => preview.update({ stage: event.target.value as TemplatePreviewRequest['stage'] })} className="rounded-md border border-input bg-background p-2">{Object.entries(labels).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
        <Button disabled={preview.busy || !valuesValid} onClick={() => { void preview.run() }}>{preview.busy ? 'Checking…' : result ? 'Refresh preview' : 'Preview draft'}</Button>
        {preview.busy && <Button variant="outline" onClick={preview.cancel}>Cancel preview</Button>}
        {stale && <span role="status" className="text-xs text-warning">Preview is out of date. Refresh to check current inputs.</span>}
      </div>
      {session.stage === 'resolved' && <div className="grid gap-3 rounded-md border border-border p-3 sm:grid-cols-3">
        {(['pipeline', 'flowgroup'] as const).map((field) => <label key={field} className="text-xs"><span className="mb-1 block capitalize">Sample {field}</span><input aria-label={`Sample ${field}`} value={session.context[field]} onChange={(event) => preview.update({ context: { ...session.context, [field]: event.target.value } })} className="w-full rounded-md border border-input bg-background px-2 py-2" /></label>)}
        <label className="text-xs"><span className="mb-1 block">Environment</span><select aria-label="Preview environment" value={session.context.environment} onChange={(event) => preview.update({ context: { ...session.context, environment: event.target.value } })} className="w-full rounded-md border border-input bg-background px-2 py-2"><option value="">Select environment</option>{envNames.map((env) => <option key={env}>{env}</option>)}</select></label>
        <details className="space-y-3 sm:col-span-3">
          <summary className="cursor-pointer text-xs">Additional presets and runtime variables</summary>
          <p className="text-xs text-muted-foreground">Preset names refer to saved project files. Runtime variables are text values used during resolution.</p>
          <StringListEditor id={`${sampleId}-presets`} label="Additional presets" value={session.context.presets ?? []} allowEmpty
            onAddItem={(value) => preview.update({ context: { ...session.context, presets: [...(session.context.presets ?? []), value] } })}
            onEditItem={(index, value) => preview.update({ context: { ...session.context, presets: (session.context.presets ?? []).map((item, i) => i === index ? value : item) } })}
            onRemoveItem={(index) => preview.update({ context: { ...session.context, presets: (session.context.presets ?? []).filter((_, i) => i !== index) } })}
            onDeleteKey={() => preview.update({ context: { ...session.context, presets: [] } })} />
          <KeyValueMapEditor id={`${sampleId}-variables`} label="Runtime variables" value={session.context.variables ?? {}} allowEmpty
            onSetEntry={(key, value) => preview.update({ context: { ...session.context, variables: { ...session.context.variables, [key]: value } } })}
            onRenameEntry={(oldKey, newKey) => { const variables = { ...session.context.variables }; variables[newKey] = variables[oldKey]; delete variables[oldKey]; preview.update({ context: { ...session.context, variables } }) }}
            onRemoveEntry={(key) => { const variables = { ...session.context.variables }; delete variables[key]; preview.update({ context: { ...session.context, variables } }) }}
            onDeleteKey={() => preview.update({ context: { ...session.context, variables: {} } })} />
        </details>
        {environments.isError && <p role="alert" className="text-xs">Could not load environments. <Button variant="ghost" size="sm" onClick={() => { void environments.refetch() }}>Retry environments</Button></p>}
      </div>}
      <div className="grid min-w-0 gap-4 lg:grid-cols-[minmax(240px,1fr)_minmax(0,1.5fr)]">
        <section className="min-w-0 space-y-3" aria-label="Sample parameters">
          <h3 className="text-sm font-semibold">Sample parameters</h3>
          {draft.error && <p role="alert" className="text-xs text-warning">{draft.error} <Button size="sm" variant="ghost" onClick={() => { void openWorkspaceFile(path, { source: true }) }}>Open Code</Button></p>}
          <TemplateInvocationParams idPrefix={sampleId} params={draft.params} values={session.values} onSet={(name, value) => preview.update({ values: { ...session.values, [name]: value } })} onUnset={(name) => { const values = { ...session.values }; delete values[name]; preview.update({ values }) }} onValidityChange={setValuesValid} />
        </section>
        <section className="min-w-0 space-y-3" aria-label="Preview result">
          <h3 className="text-sm font-semibold">Result</h3>
          <p className="text-xs text-muted-foreground">Expanded actions retain environment tokens. Resolved flowgroups use saved project settings, presets and substitutions. This check does not execute a pipeline.</p>
          {preview.error && <p role="alert" className="text-sm text-destructive">{preview.error}</p>}
          {!result && !preview.busy && <p className="rounded-md border border-dashed border-border p-4 text-sm text-muted-foreground">Supply sample values and preview the draft.</p>}
          {result && <>
            <p role="status" className={`text-sm ${result.status === 'ready' && !stale ? 'text-success' : 'text-warning'}`}>{stale ? 'Previous result' : result.status === 'ready' ? `${labels[result.stage]} checked` : result.status === 'needs_parameters' ? `Supply required parameters: ${result.missing_parameters.join(', ')}` : result.status === 'needs_context' ? 'Select a sample pipeline, flowgroup and environment.' : result.status === 'stale' ? 'Saved files changed. Refresh preview.' : 'The template needs attention.'}</p>
            {result.diagnostics.length > 0 && <ul aria-label="Preview diagnostics" className="space-y-2">{result.diagnostics.map((issue, index) => <li key={`${issue.code}:${index}`} className="rounded-md border border-border p-3 text-xs"><span className="font-medium">{issue.severity}: {issue.message}</span>{issue.suggestion && <p className="mt-1 text-muted-foreground">{issue.suggestion}</p>}{issue.field_path?.[0] === 'parameters' && typeof issue.field_path[1] === 'number' && draft.params[issue.field_path[1]] && <Button size="sm" variant="ghost" onClick={() => { const element = document.getElementById(`${sampleId}-parameter-${issue.field_path![1]}`); element?.scrollIntoView({ block: 'nearest' }); element?.focus() }}>Go to sample parameter</Button>}<Button size="sm" variant="ghost" onClick={() => { void openWorkspaceFile(issue.source_path || path, { source: true, line: issue.line ?? undefined }) }}>Open source{issue.line ? ` line ${issue.line}` : ''}</Button></li>)}</ul>}
            {output != null && <><pre className="max-h-[32rem] overflow-auto rounded-md border border-border bg-muted/30 p-3 text-xs" aria-label="Rendered YAML">{stringify(output, { lineWidth: 0 })}</pre>{actions && actions.length > 0 && <><Button size="sm" variant="outline" onClick={() => setShowGraph(!showGraph)}>{showGraph ? 'Hide graph' : 'Show action graph'}</Button>{showGraph && <Suspense fallback={<p role="status">Loading graph…</p>}><TemplatePreviewGraph actions={actions} /></Suspense>}</>}</>}
            {result.saved_dependencies.length > 0 && <details className="text-xs text-muted-foreground"><summary className="cursor-pointer">Saved files used ({result.saved_dependencies.length})</summary><ul>{result.saved_dependencies.map((item) => <li key={item.path} className="break-all font-mono">{item.path}</li>)}</ul></details>}
          </>}
        </section>
      </div>
    </div>
  </div>
}
