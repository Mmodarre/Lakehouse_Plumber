import { lazy, Suspense, useState } from 'react'
import { ChevronRight, LayoutTemplate, Plus } from 'lucide-react'
import { templateReferenceForPath } from '@/lib/template-document'
import { useTemplateCatalog } from '@/hooks/useTemplateAuthoring'
import { useLayoutStore } from '@/store/layoutStore'
import { useWorkspaceStore, entityTemplateTabId } from '@/store/workspaceStore'
import { openWorkspaceFile } from '@/workspace/openWorkspaceFile'
import { Button } from '@/components/ui/button'

const CreateTemplateDialog = lazy(() => import('./CreateTemplateDialog').then((m) => ({ default: m.CreateTemplateDialog })))
export function TemplateResources() {
  const [expanded, setExpanded] = useState(false)
  const [creating, setCreating] = useState(false)
  const [search, setSearch] = useState('')
  const catalog = useTemplateCatalog()
  const active = useWorkspaceStore((s) => s.activePath)
  const buffers = useWorkspaceStore((s) => s.buffers)
  const viewer = useLayoutStore((s) => s.viewerMode)
  const query = search.trim().toLowerCase()
  const saved = catalog.data?.templates ?? []
  const drafts = buffers.filter((b) => !b.exists && /^templates\/.+\.ya?ml$/i.test(b.path) && !saved.some((t) => t.source_path === b.path))
  const entries = [...drafts.map((b) => ({ source_path: b.path, reference: templateReferenceForPath(b.path), declared_name: null, description: null, state: 'draft', parameters: [], action_count: null, diagnostics: [] })), ...saved]
  const templates = entries.filter((t) => !query || [t.source_path, t.declared_name, t.description].some((v) => v?.toLowerCase().includes(query)))
  return <section aria-label="Templates">
    <div className="flex items-center pr-2">
      <button type="button" aria-expanded={expanded} onClick={() => setExpanded(!expanded)} className="flex min-w-0 flex-1 items-center gap-1.5 px-2 py-1 text-left text-xs font-semibold hover:bg-card">
        <ChevronRight className={`size-3.5 text-muted-foreground ${expanded ? 'rotate-90' : ''}`} aria-hidden="true" />
        <LayoutTemplate className="size-3.5 text-muted-foreground" aria-hidden="true" />Templates
        {catalog.data && <span className="ml-auto text-muted-foreground">{entries.length}</span>}
      </button>
      <Button size="icon-sm" variant="ghost" aria-label="New template" title="New template" disabled={viewer} onClick={() => setCreating(true)}><Plus className="size-3.5" /></Button>
    </div>
    {expanded && <div className="space-y-1 px-2 pb-2">
      <input aria-label="Search templates" placeholder="Search templates" value={search} onChange={(event) => setSearch(event.target.value)} className="my-1 w-full rounded border border-input bg-background px-2 py-1 text-xs" />
      {catalog.isPending && <p role="status" className="px-2 text-xs text-muted-foreground">Loading templates…</p>}
      {catalog.isError && <div role="alert" className="px-2 text-xs">Could not load templates. <Button size="sm" variant="ghost" onClick={() => { void catalog.refetch() }}>Retry</Button></div>}
      {catalog.isSuccess && templates.length === 0 && <p className="px-2 text-xs text-muted-foreground">{search ? 'No matching templates.' : 'No templates yet. Create one to reuse actions.'}</p>}
      {templates.map((template) => <button key={template.source_path} type="button" aria-current={active === entityTemplateTabId(template.source_path) ? 'page' : undefined}
        title={[template.source_path, template.description, ...template.diagnostics.map((item) => item.message)].filter(Boolean).join('\n')}
        onClick={() => { void openWorkspaceFile(template.source_path, { source: template.state === 'invalid' ? true : undefined }) }}
        className="block w-full rounded px-2 py-1.5 text-left hover:bg-card aria-[current=page]:bg-accent-weak">
        <span className="block truncate font-mono text-xs">{template.declared_name || template.reference || template.source_path}</span>
        <span className="block truncate text-2xs text-muted-foreground">{template.source_path}</span>
        {template.description && <span className="block truncate text-2xs text-muted-foreground">{template.description}</span>}
        <span className="block text-2xs text-muted-foreground">{template.state === 'draft' ? 'Unsaved draft' : template.state === 'invalid' ? 'Invalid YAML · Open code' : template.state === 'unsupported_extension' ? '.yml · Edit only' : `${template.parameters.length} parameters · ${template.action_count ?? 0} actions`}</span>
      </button>)}
    </div>}
    {creating && <Suspense fallback={null}><CreateTemplateDialog open onOpenChange={setCreating} onCreated={(path) => { setExpanded(true); void openWorkspaceFile(path) }} /></Suspense>}
  </section>
}
