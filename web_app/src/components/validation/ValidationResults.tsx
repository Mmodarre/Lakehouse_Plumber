import type { ValidateResponse } from '../../types/api'

export function ValidationResults({ result }: { result: ValidateResponse }) {
  return (
    <div className="space-y-4">
      {/* Success/failure banner */}
      <div
        className={`rounded-lg px-4 py-3 text-sm font-medium ${
          result.success
            ? 'bg-green-50 text-green-800 border border-green-200'
            : 'bg-red-50 text-red-800 border border-red-200'
        }`}
      >
        {result.success
          ? 'All pipelines validated successfully'
          : result.error_message ?? 'Validation failed'}
      </div>

      {/* Errors */}
      {result.errors.length > 0 && (
        <div>
          <h3 className="mb-2 text-xs font-semibold text-red-700">
            Errors ({result.errors.length})
          </h3>
          <div className="space-y-1">
            {result.errors.map((err, i) => (
              <div key={i} className="rounded border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
                {err}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Warnings */}
      {result.warnings.length > 0 && (
        <div>
          <h3 className="mb-2 text-xs font-semibold text-amber-700">
            Warnings ({result.warnings.length})
          </h3>
          <div className="space-y-1">
            {result.warnings.map((warn, i) => (
              <div key={i} className="rounded border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700">
                {warn}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Validated pipelines */}
      {result.validated_pipelines.length > 0 && (
        <div>
          <h3 className="mb-2 text-xs font-semibold text-slate-600">
            Validated Pipelines
          </h3>
          <div className="flex flex-wrap gap-1.5">
            {result.validated_pipelines.map((name) => (
              <span
                key={name}
                className="flex items-center gap-1 rounded bg-slate-100 px-2 py-1 text-xs text-slate-700"
              >
                <svg className="h-3 w-3 text-green-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
                {name}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
