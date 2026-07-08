import { usePipelineDetail } from '../../hooks/usePipelines'
import { useUIStore } from '../../store/uiStore'
import { LoadingSpinner } from '../common/LoadingSpinner'
import { JsonTree } from './JsonTree'

export function PipelineDetail({ name }: { name: string }) {
  const { data, isLoading } = usePipelineDetail(name)
  const openModal = useUIStore((s) => s.openModal)

  if (isLoading) return <LoadingSpinner className="py-8" />

  return (
    <div className="space-y-4">
      {/* Summary */}
      <div className="flex items-center justify-between rounded-md bg-muted/50 px-3 py-2">
        <span className="text-xs text-muted-foreground">Flowgroups</span>
        <span className="text-sm font-semibold tabular-nums text-foreground">
          {data?.flowgroup_count}
        </span>
      </div>

      {/* Flowgroup list */}
      {data?.flowgroups && data.flowgroups.length > 0 && (
        <div>
          <h3 className="mb-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            Flowgroups
          </h3>
          <div className="space-y-0.5">
            {data.flowgroups.map((fg) => (
              <button
                key={fg}
                className="w-full rounded-md px-2 py-1 text-left text-xs text-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
                onClick={() => openModal({ type: 'flowgroup', name: fg })}
              >
                {fg}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Pipeline config */}
      {data?.config && Object.keys(data.config).length > 0 && (
        <div>
          <h3 className="mb-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            Configuration
          </h3>
          <JsonTree data={data.config} />
        </div>
      )}
    </div>
  )
}
