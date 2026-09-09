import { useMemo, useRef, useState } from 'react'
import { SchemaKindProvider } from '@/components/common/SchemaKindContext'
import { captureWorkspaceEditors } from '@/workspace/editorCommands'
import { toast } from 'sonner'
import { BookOpen, Braces, List, Waypoints } from 'lucide-react'
import { useFlowgroupDoc } from '@/components/entity/useFlowgroupDoc'
import { GraphView } from '@/components/entity/GraphView'
import { TemplateParamsCard, ReferenceList } from '@/components/entity/TemplateParamsCard'
import { OptionalTextField } from '@/components/config/fields/OptionalTextField'
import { descriptionIds } from '@/components/config/fields/fieldSupport'
import { DraftInput } from '@/components/config/fields/DraftInput'
import { FieldChrome } from '@/components/config/fields/FieldChrome'
import { StringListEditor } from '@/components/config/fields/StringListEditor'
import { Button } from '@/components/ui/button'
import { LoadingSpinner } from '@/components/common/LoadingSpinner'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { readFlowgroupValue } from '@/lib/flowgroup-doc'
import { deleteTemplateMetadata, inspectTemplateReferences, setTemplateMetadata, templateReferenceForPath, templateStructuredEditIssue } from '@/lib/template-document'

export interface TemplateBuilderProps { path: string; tabId: string }
type Section = 'basics' | 'parameters' | 'actions'
const SECTIONS = [{ key: 'basics', label: 'Basics', icon: BookOpen }, { key: 'parameters', label: 'Parameters', icon: Braces }, { key: 'actions', label: 'Actions', icon: Waypoints }] as const
export function TemplateBuilder(props: TemplateBuilderProps) {
  return <SchemaKindProvider kind="template"><TemplateBuilderBody {...props} /></SchemaKindProvider>
}
function TemplateBuilderBody({ path, tabId }: TemplateBuilderProps) {
  const { doc, params, actions, commit, readOnly, readOnlyReason, version } = useFlowgroupDoc(path, 'template')
  const buffer = useWorkspaceStore((state) => state.buffers.find((item) => item.path === path))
  const [section, setSection] = useState<Section>('basics')
  const rootRef = useRef<HTMLDivElement>(null)
  const changeSection = (next: Section) => {
    captureWorkspaceEditors()
    const invalid = rootRef.current?.querySelector<HTMLInputElement | HTMLTextAreaElement>('[data-workspace-draft][aria-invalid="true"]')
    if (invalid) { invalid.focus(); toast.error('Correct this value or press Escape to restore it before changing sections.'); return }
    setSection(next)
  }
  const [presentation, setPresentation] = useState<'list' | 'graph'>('list')
  const source = buffer?.content ?? ''
  const safetyIssue = useMemo(() => templateStructuredEditIssue(source), [source])
  // Source is the shared buffer; derive fresh snapshots rather than retaining a reduced form model.
  const references = useMemo(() => doc ? inspectTemplateReferences(doc) : [], [doc, version]) // eslint-disable-line react-hooks/exhaustive-deps
  const openCode = () => useWorkspaceStore.getState().setTabView(tabId, 'code')
  if (readOnlyReason === 'loading' || !buffer) return <LoadingSpinner className="h-full" />
  if (!doc || safetyIssue) return <div className="space-y-3 p-6" role="status"><h2 className="text-sm font-semibold">Continue editing in Code</h2><p className="max-w-xl text-sm text-muted-foreground">{safetyIssue ?? 'This YAML does not contain an editable template definition. A template has a name and actions or parameters.'}</p><Button variant="outline" onClick={openCode}>Open Code</Button></div>
  const name = String(readFlowgroupValue(doc, ['name']) ?? '')
  const storedVersion = readFlowgroupValue(doc, ['version'])
  const description = readFlowgroupValue(doc, ['description'])
  const presets = readFlowgroupValue(doc, ['presets'])
  const presetList = Array.isArray(presets) && presets.every((item) => typeof item === 'string') ? presets as string[] : undefined
  const reference = templateReferenceForPath(path)
  const unknownReferences = references.filter((ref) => ref.kind !== 'direct' || !params.some((param) => param.name === ref.parameter))
  return <div ref={rootRef} className="flex h-full min-h-0 flex-col">
    <div className="flex shrink-0 flex-wrap items-center gap-3 border-b border-border px-4 py-3">
      <nav aria-label="Template builder sections" className="hidden gap-1 sm:flex">{SECTIONS.map(({ key, label, icon: Icon }) => <Button key={key} variant={section === key ? 'secondary' : 'ghost'} size="sm" aria-current={section === key ? 'page' : undefined} onClick={() => changeSection(key)}><Icon aria-hidden="true" />{label}{key === 'parameters' ? ` (${params.length})` : key === 'actions' ? ` (${actions.length})` : ''}</Button>)}</nav>
      <select className="rounded border border-input bg-background px-3 py-2 text-sm sm:hidden" aria-label="Template builder section" value={section} onChange={(e) => changeSection(e.target.value as Section)}>{SECTIONS.map((item) => <option key={item.key} value={item.key}>{item.label}</option>)}</select>
      <span className="ml-auto text-xs text-muted-foreground">{readOnly ? 'Read-only' : 'Changes stay in this draft until Save'}</span>
    </div>
    {buffer.isNew && <div className="flex shrink-0 flex-wrap gap-x-4 gap-y-1 border-b border-border bg-muted/30 px-4 py-2 text-xs text-muted-foreground" aria-label="Template setup checklist"><span>{name ? '✓' : '○'} Describe the template</span><span>{params.length ? '✓' : '○'} Declare inputs, if needed</span><span>{actions.length ? '✓' : '○'} Build actions</span><span>Try sample values in Preview</span></div>}
    {section === 'basics' && <div className="min-h-0 flex-1 overflow-auto p-4 sm:p-6"><div className="mx-auto max-w-3xl space-y-5">
      <div><h2 className="text-base font-semibold">Template basics</h2><p className="mt-1 text-sm text-muted-foreground">Build a reusable action pattern. Flowgroups choose this template and supply the values that vary.</p></div>
      <dl className="space-y-2 rounded-md border border-border bg-muted/25 p-4 text-xs"><div><dt className="text-muted-foreground">Source file</dt><dd className="mt-1 break-all font-mono">{path}</dd></div><div><dt className="text-muted-foreground">Invocation reference</dt><dd className="mt-1 break-all font-mono">{reference ? `use_template: ${reference}` : 'This extension cannot be invoked by the current runtime. Save a copy with .yaml.'}</dd></div></dl>
      <FieldChrome id="template-name" label="Template name" helpPath={['name']} description="A descriptive name inside the definition. Flowgroups reference its file path above." issue={!name.trim() ? 'Enter a template name.' : undefined}><DraftInput id="template-name" aria-describedby={descriptionIds('template-name')} initial={name} disabled={readOnly} onCommit={(value) => commit((body) => setTemplateMetadata(body, 'name', value))} /></FieldChrome>
      <OptionalTextField id="template-description" label="Description" helpPath={['description']} value={description} multiline disabled={readOnly} onSet={(value) => commit((body) => setTemplateMetadata(body, 'description', value))} onUnset={() => commit((body) => deleteTemplateMetadata(body, 'description'))} />
      <OptionalTextField id="template-version" label="Version" helpPath={['version']} value={storedVersion === undefined ? undefined : String(storedVersion)} disabled={readOnly} onSet={(value) => commit((body) => setTemplateMetadata(body, 'version', value))} onUnset={() => commit((body) => deleteTemplateMetadata(body, 'version'))} />
      {presets !== undefined && !presetList ? <div className="text-sm" role="status">The presets value has a shape this editor cannot represent. <Button variant="link" onClick={openCode}>Edit presets in Code</Button></div> : <StringListEditor id="template-presets" label="Presets" helpPath={['presets']} value={presetList} disabled={readOnly} allowEmpty onEditItem={(index, value) => commit((body) => { const next = [...(presetList ?? [])]; next[index] = value; setTemplateMetadata(body, 'presets', next) })} onAddItem={(value) => commit((body) => setTemplateMetadata(body, 'presets', [...(presetList ?? []), value]))} onRemoveItem={(index) => commit((body) => setTemplateMetadata(body, 'presets', (presetList ?? []).filter((_, i) => i !== index)))} onDeleteKey={() => commit((body) => deleteTemplateMetadata(body, 'presets'))} />}
    </div></div>}
    {section === 'parameters' && <div className="min-h-0 flex-1 overflow-auto p-4 sm:p-6"><div className="mx-auto max-w-3xl"><TemplateParamsCard params={params} templateName={name} commit={commit} readOnly={readOnly} references={references} onOpenCode={openCode} />{unknownReferences.length > 0 && <details className="mt-5 rounded border border-border p-3 text-xs"><summary className="cursor-pointer">Expressions to review ({unknownReferences.length})</summary><p className="mt-2 text-muted-foreground">Some expressions use undeclared inputs, need full Jinja inspection, or are not rendered by the current runtime. Preview checks the actual engine behavior.</p><ReferenceList references={unknownReferences} /></details>}</div></div>}
    {section === 'actions' && <div className="flex min-h-0 flex-1 flex-col"><div className="flex shrink-0 flex-wrap items-center justify-between gap-2 border-b border-border px-4 py-2"><p className="text-xs text-muted-foreground">Build actions, then switch their fields to Expression to insert a parameter.</p><div className="flex gap-1" role="group" aria-label="Action presentation"><Button size="xs" variant={presentation === 'list' ? 'secondary' : 'ghost'} aria-pressed={presentation === 'list'} onClick={() => setPresentation('list')}><List aria-hidden="true" />List</Button><Button size="xs" variant={presentation === 'graph' ? 'secondary' : 'ghost'} aria-pressed={presentation === 'graph'} onClick={() => setPresentation('graph')}><Waypoints aria-hidden="true" />Graph</Button></div></div><GraphView tabId={tabId} filePath={path} docKind="template" hideDetails actionSaveMode="apply" presentation={presentation} /></div>}
  </div>
}
export default TemplateBuilder
