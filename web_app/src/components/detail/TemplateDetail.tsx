import { useTemplateDetail } from '../../hooks/useTemplates'
import { LoadingSpinner } from '../common/LoadingSpinner'

export function TemplateDetail({ name }: { name: string }) {
  const { data, isLoading } = useTemplateDetail(name)

  if (isLoading) return <LoadingSpinner className="py-8" />

  const template = data?.template

  return (
    <div className="space-y-4">
      {/* Metadata */}
      <div className="space-y-2">
        {template?.version && (
          <div className="flex items-center justify-between">
            <span className="text-[11px] text-slate-400">Version</span>
            <span className="text-xs font-medium text-slate-700">{template.version}</span>
          </div>
        )}
        {template?.description && (
          <div>
            <span className="text-[11px] text-slate-400">Description</span>
            <p className="mt-0.5 text-xs text-slate-600">{template.description}</p>
          </div>
        )}
        {template?.action_count !== undefined && (
          <div className="flex items-center justify-between">
            <span className="text-[11px] text-slate-400">Actions</span>
            <span className="text-xs font-medium text-slate-700">{template.action_count}</span>
          </div>
        )}
      </div>

      {/* Parameters */}
      {template?.parameters && template.parameters.length > 0 && (
        <div>
          <h3 className="mb-1.5 text-[11px] font-semibold uppercase tracking-wider text-slate-400">
            Parameters ({template.parameters.length})
          </h3>
          <div className="space-y-1">
            {template.parameters.map((param, i) => {
              const paramName = (param.name as string) ?? `param-${i}`
              const paramDefault = param.default as string | undefined
              const paramRequired = param.required as boolean | undefined
              return (
                <div key={i} className="rounded bg-slate-50 px-2 py-1.5">
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-slate-700">{paramName}</span>
                    {paramRequired && (
                      <span className="rounded bg-red-50 px-1 py-0.5 text-[9px] text-red-600">required</span>
                    )}
                  </div>
                  {paramDefault !== undefined && (
                    <p className="mt-0.5 text-[10px] text-slate-400">
                      Default: <code className="text-slate-500">{String(paramDefault)}</code>
                    </p>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}
