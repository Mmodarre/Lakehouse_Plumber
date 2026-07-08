import { usePresetDetail } from '../../hooks/usePresets'
import { LoadingSpinner } from '../common/LoadingSpinner'
import { JsonTree } from './JsonTree'

export function PresetDetail({ name }: { name: string }) {
  const { data, isLoading } = usePresetDetail(name)

  if (isLoading) return <LoadingSpinner className="py-8" />

  return (
    <div className="space-y-4">
      {/* Raw config */}
      <div>
        <h3 className="mb-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
          Raw Configuration
        </h3>
        {data?.raw && <JsonTree data={data.raw} />}
      </div>

      {/* Resolved config (with inheritance) */}
      <div>
        <h3 className="mb-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
          Resolved (with inheritance)
        </h3>
        {data?.resolved && <JsonTree data={data.resolved} />}
      </div>
    </div>
  )
}
