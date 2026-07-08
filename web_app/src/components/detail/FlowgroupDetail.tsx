import type { LucideIcon } from 'lucide-react'
import { ArrowDownToLine, Database, FlaskConical, Wand2 } from 'lucide-react'
import { useFlowgroupDetail, useFlowgroupResolved } from '../../hooks/useFlowgroups'
import { useUIStore } from '../../store/uiStore'
import { LoadingSpinner } from '../common/LoadingSpinner'
import { Badge } from '../ui/badge'
import { cn } from '../../lib/utils'
import { JsonTree } from './JsonTree'

/** Unified action-kind badge recipe — the same `--kind-*` tokens the DAG
 * accents and table badges use (12% tinted fill, 25% border, kind-colored
 * text + icon), so "transform" is violet and "write" is green everywhere. */
const ACTION_KIND_BADGE: Record<string, { icon: LucideIcon; className: string }> = {
  load: {
    icon: ArrowDownToLine,
    className: 'border-kind-load/25 bg-kind-load/12 text-kind-load',
  },
  transform: {
    icon: Wand2,
    className: 'border-kind-transform/25 bg-kind-transform/12 text-kind-transform',
  },
  write: {
    icon: Database,
    className: 'border-kind-write/25 bg-kind-write/12 text-kind-write',
  },
  test: {
    icon: FlaskConical,
    className: 'border-kind-test/25 bg-kind-test/12 text-kind-test',
  },
}

function ActionKindBadge({ kind }: { kind: string }) {
  const spec = ACTION_KIND_BADGE[kind]
  if (!spec) {
    return (
      <Badge variant="outline" className="rounded-sm px-1.5 text-2xs">
        {kind}
      </Badge>
    )
  }
  const Icon = spec.icon
  return (
    <Badge className={cn('h-5 rounded-sm border px-1.5 text-2xs', spec.className)}>
      <Icon className="size-2.5" aria-hidden="true" />
      {kind}
    </Badge>
  )
}

export function FlowgroupDetail({ name }: { name: string }) {
  const { selectedEnv, openModal } = useUIStore()
  const { data: detail, isLoading: loadingDetail } = useFlowgroupDetail(name)
  const { data: resolved, isLoading: loadingResolved } = useFlowgroupResolved(name, selectedEnv)

  if (loadingDetail) return <LoadingSpinner className="py-8" />

  const fg = detail?.flowgroup as Record<string, unknown> | undefined
  const actions = (fg?.actions ?? []) as Array<Record<string, unknown>>

  return (
    <div className="space-y-4">
      {/* Source file */}
      {detail?.source_file && (
        <p className="break-all font-mono text-xs text-muted-foreground">{detail.source_file}</p>
      )}

      {/* Actions table */}
      <div>
        <h3 className="mb-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
          Actions ({actions.length})
        </h3>
        <div className="space-y-1">
          {actions.map((action, i) => {
            const actionType = (action.type as string) ?? 'unknown'
            const actionName = (action.name as string) ?? `action-${i}`
            const target = (action.target as string) ?? ''
            return (
              <div key={i} className="flex items-center gap-2 rounded-md bg-muted/50 px-2 py-1.5">
                <ActionKindBadge kind={actionType} />
                <span className="truncate text-xs text-foreground">{actionName}</span>
                {target && (
                  <span className="ml-auto truncate font-mono text-2xs text-muted-foreground">
                    {target}
                  </span>
                )}
              </div>
            )
          })}
        </div>
      </div>

      {/* Applied presets */}
      {resolved?.applied_presets && resolved.applied_presets.length > 0 && (
        <div>
          <h3 className="mb-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            Applied Presets
          </h3>
          <div className="flex flex-wrap gap-1">
            {resolved.applied_presets.map((preset) => (
              <button
                key={preset}
                className="rounded-sm bg-secondary px-2 py-0.5 text-2xs text-secondary-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
                onClick={() => openModal({ type: 'preset', name: preset })}
              >
                {preset}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Applied template */}
      {resolved?.applied_template && (
        <div>
          <h3 className="mb-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            Template
          </h3>
          <button
            className="rounded-sm bg-secondary px-2 py-0.5 text-2xs text-secondary-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
            onClick={() => openModal({ type: 'template', name: resolved.applied_template! })}
          >
            {resolved.applied_template}
          </button>
        </div>
      )}

      {/* Resolved config */}
      <div>
        <h3 className="mb-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
          Resolved Config
          {loadingResolved && <LoadingSpinner className="ml-2 inline-block" />}
        </h3>
        {resolved?.flowgroup && (
          <JsonTree data={resolved.flowgroup} />
        )}
      </div>
    </div>
  )
}
