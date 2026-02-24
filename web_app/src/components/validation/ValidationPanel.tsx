import { useValidation } from '../../hooks/useValidation'
import { useUIStore } from '../../store/uiStore'
import { usePipelines } from '../../hooks/usePipelines'
import { ValidationResults } from './ValidationResults'
import { LoadingSpinner } from '../common/LoadingSpinner'

export function ValidationPanel({ compact = false }: { compact?: boolean }) {
  const selectedEnv = useUIStore((s) => s.selectedEnv)
  const { data: pipelines } = usePipelines()
  const validation = useValidation()

  const handleValidate = (pipeline?: string) => {
    validation.mutate({ env: selectedEnv, pipeline })
  }

  if (compact) {
    return (
      <div className="flex items-center gap-2">
        <button
          className="rounded bg-blue-600 px-3 py-1 text-xs font-medium text-white hover:bg-blue-700 disabled:opacity-50"
          onClick={() => handleValidate()}
          disabled={validation.isPending}
        >
          {validation.isPending ? 'Validating...' : 'Validate'}
        </button>
        {validation.data && (
          <span className={`text-xs ${validation.data.success ? 'text-green-600' : 'text-red-600'}`}>
            {validation.data.success ? 'Passed' : `${validation.data.errors.length} errors`}
          </span>
        )}
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Controls */}
      <div className="flex items-center gap-3">
        <button
          className="rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
          onClick={() => handleValidate()}
          disabled={validation.isPending}
        >
          {validation.isPending ? 'Validating...' : `Validate All (${selectedEnv})`}
        </button>

        {/* Per-pipeline buttons */}
        {pipelines?.pipelines.map((p) => (
          <button
            key={p.name}
            className="rounded border border-slate-200 px-3 py-2 text-xs text-slate-600 hover:bg-slate-50 disabled:opacity-50"
            onClick={() => handleValidate(p.name)}
            disabled={validation.isPending}
          >
            {p.name}
          </button>
        ))}
      </div>

      {validation.isPending && <LoadingSpinner className="py-8" />}

      {validation.data && <ValidationResults result={validation.data} />}

      {validation.error && (
        <div className="rounded border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {validation.error.message}
        </div>
      )}
    </div>
  )
}
