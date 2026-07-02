import { ValidationPanel } from '../components/validation/ValidationPanel'
import { ProblemsPanel } from '../components/validation/ProblemsPanel'

export function ValidationPage() {
  return (
    <div className="h-full space-y-6 overflow-y-auto p-6">
      <div>
        <h1 className="mb-4 text-lg font-semibold text-slate-800">Validation</h1>
        <ValidationPanel />
      </div>
      <ProblemsPanel />
    </div>
  )
}
