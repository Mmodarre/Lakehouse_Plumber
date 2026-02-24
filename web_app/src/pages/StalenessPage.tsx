import { StalenessPanel } from '../components/staleness/StalenessPanel'

export function StalenessPage() {
  return (
    <div className="h-full overflow-y-auto p-6">
      <h1 className="mb-4 text-lg font-semibold text-slate-800">Staleness</h1>
      <StalenessPanel />
    </div>
  )
}
