import { ValidationPanel } from '../components/validation/ValidationPanel'

export function ValidationPage() {
  return (
    <div className="h-full overflow-y-auto p-6">
      <h1 className="mb-4 text-lg font-semibold text-slate-800">Validation</h1>
      <ValidationPanel />
    </div>
  )
}
