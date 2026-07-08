import { useTemplateDetail } from '../../hooks/useTemplates'
import { LoadingSpinner } from '../common/LoadingSpinner'
import { Badge } from '../ui/badge'

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
            <span className="text-2xs text-muted-foreground">Version</span>
            <span className="text-xs font-medium text-foreground">{template.version}</span>
          </div>
        )}
        {template?.description && (
          <div>
            <span className="text-2xs text-muted-foreground">Description</span>
            <p className="mt-0.5 text-xs text-muted-foreground">{template.description}</p>
          </div>
        )}
        {template?.action_count !== undefined && (
          <div className="flex items-center justify-between">
            <span className="text-2xs text-muted-foreground">Actions</span>
            <span className="text-xs font-medium tabular-nums text-foreground">
              {template.action_count}
            </span>
          </div>
        )}
      </div>

      {/* Parameters */}
      {template?.parameters && template.parameters.length > 0 && (
        <div>
          <h3 className="mb-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            Parameters ({template.parameters.length})
          </h3>
          <div className="space-y-1">
            {template.parameters.map((param, i) => {
              const paramName = (param.name as string) ?? `param-${i}`
              const paramDefault = param.default as string | undefined
              const paramRequired = param.required as boolean | undefined
              return (
                <div key={i} className="rounded-md bg-muted/50 px-2 py-1.5">
                  <div className="flex items-center gap-2">
                    <span className="font-mono text-xs font-medium text-foreground">
                      {paramName}
                    </span>
                    {paramRequired && (
                      <Badge className="h-4 rounded-sm border border-destructive/25 bg-destructive/12 px-1 text-2xs text-destructive">
                        required
                      </Badge>
                    )}
                  </div>
                  {paramDefault !== undefined && (
                    <p className="mt-0.5 text-2xs text-muted-foreground">
                      Default:{' '}
                      <code className="font-mono text-foreground/80">{String(paramDefault)}</code>
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
