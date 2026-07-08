import { ValidationPanel } from '../components/validation/ValidationPanel'
import { ProblemsPanel } from '../components/validation/ProblemsPanel'

export function ValidationPage() {
  return (
    <div className="h-full overflow-y-auto bg-background p-6">
      <div className="mx-auto w-full max-w-4xl space-y-4">
        <div>
          <h1 className="text-lg font-semibold text-foreground">Validation</h1>
          <p className="mt-1 text-xs text-muted-foreground">
            Validate and generate runs stream their progress and issues here.
          </p>
        </div>
        <ValidationPanel />
        <ProblemsPanel />
      </div>
    </div>
  )
}
