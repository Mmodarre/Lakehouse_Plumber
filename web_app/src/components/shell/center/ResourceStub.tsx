import { useBlueprints } from '@/hooks/useBlueprints'
import { useWorkspaceStore, workspaceTabId, type ResourceKind } from '@/store/workspaceStore'
import { openWorkspaceFile } from '@/workspace/openWorkspaceFile'
import { PresetDetail } from '../../detail/PresetDetail'
import { TemplateDetail } from '../../detail/TemplateDetail'
import { Button } from '../../ui/button'

function BlueprintDetails({ name }: { name: string }) {
  const { data, isLoading, error, refetch } = useBlueprints(true)
  const blueprint = data?.blueprints.find((item) => item.name === name)
  if (isLoading) return <p>Loading blueprint…</p>
  if (error) return <div role="alert"><p>Could not load blueprint details.</p><Button onClick={() => void refetch()}>Retry</Button></div>
  if (!blueprint) return <p>This blueprint was not found in the current project index.</p>
  return <div className="space-y-3 text-sm">
    <p>{blueprint.description || 'Reusable flowgroup definitions and parameters.'}</p>
    <dl className="grid grid-cols-2 gap-2"><dt>Version</dt><dd>{blueprint.version}</dd><dt>Flowgroups</dt><dd>{blueprint.flowgroup_count}</dd><dt>Parameters</dt><dd>{blueprint.parameter_count}</dd><dt>Instances</dt><dd>{blueprint.instance_count}</dd></dl>
    {!!blueprint.instances?.length && <details><summary className="cursor-pointer">Instances</summary><pre className="overflow-auto py-2 text-xs">{JSON.stringify(blueprint.instances, null, 2)}</pre></details>}
  </div>
}

export function ResourceStub({ resourceKind, name }: { resourceKind: ResourceKind; name: string }) {
  const active = useWorkspaceStore((s) => s.tabs.find((tab) => workspaceTabId(tab) === s.activePath))
  const source = active?.kind === 'resource' ? active.filePath : undefined
  return <div className="h-full overflow-auto p-6"><div className="mx-auto max-w-2xl space-y-4">
    <div className="flex flex-wrap items-center gap-3"><h2 className="flex-1 text-sm font-semibold">{name}</h2>{source && <Button size="sm" variant="outline" onClick={() => void openWorkspaceFile(source, { source: true })}>Open source YAML</Button>}</div>
    {source && <p className="break-all font-mono text-xs text-muted-foreground">{source}</p>}
    {resourceKind === 'preset' && <PresetDetail name={name} />}
    {resourceKind === 'template' && <TemplateDetail name={name} />}
    {resourceKind === 'blueprint' && <BlueprintDetails name={name} />}
    {resourceKind === 'environment' && <p className="text-sm text-muted-foreground">This environment supplies substitutions used when resolving pipeline files and configuration. Open its source YAML to inspect or edit values, then select this environment in the command bar for validation and generation.</p>}
  </div></div>
}
export default ResourceStub
