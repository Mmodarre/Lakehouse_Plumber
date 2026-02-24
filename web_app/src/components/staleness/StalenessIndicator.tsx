import { useStaleness } from '../../hooks/useStaleness'
import { useUIStore } from '../../store/uiStore'

export function StalenessIndicator() {
  const selectedEnv = useUIStore((s) => s.selectedEnv)
  const { data } = useStaleness(selectedEnv)

  if (!data) return null

  const total = data.total_new + data.total_stale

  if (total === 0) {
    return (
      <div className="flex items-center gap-1.5">
        <div className="h-2 w-2 rounded-full bg-green-500" />
        <span className="text-[10px] text-slate-500">Up to date</span>
      </div>
    )
  }

  return (
    <div className="flex items-center gap-1.5">
      <div className="h-2 w-2 rounded-full bg-amber-400" />
      <span className="text-[10px] text-amber-700">{total} stale</span>
    </div>
  )
}
