import type { ResourceKind } from '../../../store/workspaceStore'
import { PresetDetail } from '../../detail/PresetDetail'
import { TemplateDetail } from '../../detail/TemplateDetail'

// ── ResourceStub — interim resource center view (§6.5) ───────
//
// Wave-1 host for a preset/template/blueprint/environment resource tab. Reuses
// the existing detail/ content components (PresetDetail / TemplateDetail) where
// they exist; blueprint/environment get a plain identity card until their
// detail views land.

export function ResourceStub({
  resourceKind,
  name,
}: {
  resourceKind: ResourceKind
  name: string
}) {
  return (
    <div className="h-full overflow-auto p-6">
      <div className="mx-auto max-w-2xl">
        <h2 className="mb-3 text-sm font-semibold text-foreground">{name}</h2>
        {resourceKind === 'preset' && <PresetDetail name={name} />}
        {resourceKind === 'template' && <TemplateDetail name={name} />}
        {(resourceKind === 'blueprint' || resourceKind === 'environment') && (
          <p className="text-xs text-muted-foreground">
            The detail view for {resourceKind} “{name}” arrives in a later wave.
          </p>
        )}
      </div>
    </div>
  )
}

export default ResourceStub
