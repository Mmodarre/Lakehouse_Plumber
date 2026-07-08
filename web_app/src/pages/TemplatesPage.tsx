import { useState } from 'react'
import { LayoutTemplate } from 'lucide-react'
import { useTemplates, useTemplateDetail } from '../hooks/useTemplates'
import { EmptyState } from '../components/common/EmptyState'
import { SkeletonLoader } from '../components/common/SkeletonLoader'
import { errorMessage } from '../lib/errors'
import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'

/** One row of `TemplateInfoResponse.parameters` — the wire type is an
 * untyped object list; these are the fields the template engine emits. */
interface TemplateParameter {
  name?: string
  type?: string
  required?: boolean
  default?: unknown
  description?: string
}

function ParametersTable({ parameters }: { parameters: TemplateParameter[] }) {
  return (
    <div className="overflow-x-auto rounded-md border border-border">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-border bg-muted/50 text-left text-2xs uppercase tracking-[0.05em] text-muted-foreground">
            <th className="px-3 py-2 font-semibold">Name</th>
            <th className="px-3 py-2 font-semibold">Type</th>
            <th className="px-3 py-2 font-semibold">Required</th>
            <th className="px-3 py-2 font-semibold">Default</th>
            <th className="px-3 py-2 font-semibold">Description</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/60">
          {parameters.map((param, i) => (
            <tr key={param.name ?? i}>
              <td className="px-3 py-1.5 font-mono font-medium text-foreground">
                {param.name ?? '—'}
              </td>
              <td className="px-3 py-1.5 font-mono text-muted-foreground">
                {param.type ?? '—'}
              </td>
              <td className="px-3 py-1.5">
                {param.required ? (
                  <Badge className="h-4 rounded-sm border border-destructive/25 bg-destructive/12 px-1 text-2xs text-destructive">
                    required
                  </Badge>
                ) : (
                  <span className="text-muted-foreground">optional</span>
                )}
              </td>
              <td className="px-3 py-1.5 font-mono text-muted-foreground">
                {param.default !== undefined ? String(param.default) : '—'}
              </td>
              <td className="px-3 py-1.5 text-muted-foreground">
                {param.description ?? '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function TemplateDetailPanel({ name }: { name: string }) {
  const { data, isLoading, isError, error } = useTemplateDetail(name)

  if (isLoading) return <SkeletonLoader lines={8} />
  if (isError) {
    return (
      <EmptyState
        title="Failed to load template"
        message={errorMessage(error, `Could not load template '${name}'.`)}
        icon={LayoutTemplate}
      />
    )
  }
  const template = data?.template
  if (!template) return null

  const parameters = template.parameters as TemplateParameter[]

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-mono text-sm font-medium text-foreground">{template.name}</span>
        {template.version && (
          <Badge variant="outline" className="rounded-sm px-1.5 text-2xs text-muted-foreground">
            v{template.version}
          </Badge>
        )}
        <Badge variant="outline" className="ml-auto rounded-sm px-1.5 text-2xs text-muted-foreground">
          <span className="tabular-nums font-medium text-foreground">
            {template.action_count}
          </span>
          {template.action_count === 1 ? 'action' : 'actions'}
        </Badge>
      </div>

      {template.description && (
        <p className="text-xs text-muted-foreground">{template.description}</p>
      )}

      <div>
        <h3 className="mb-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
          Parameters ({parameters.length})
        </h3>
        {parameters.length > 0 ? (
          <ParametersTable parameters={parameters} />
        ) : (
          <p className="text-xs text-muted-foreground">This template takes no parameters.</p>
        )}
      </div>
    </div>
  )
}

export function TemplatesPage() {
  const { data, isLoading, isError, error } = useTemplates()
  const [selected, setSelected] = useState<string | null>(null)

  const templates = data?.templates ?? []
  const active = selected ?? templates[0] ?? null

  return (
    <div className="h-full overflow-y-auto bg-background p-6">
      <div className="mx-auto w-full max-w-5xl space-y-4">
        <div>
          <h1 className="text-lg font-semibold text-foreground">Templates</h1>
          <p className="mt-1 text-xs text-muted-foreground">
            Parameterized flowgroup generators — one template stamps out many
            flowgroups from a parameter list.
          </p>
        </div>

        {isLoading && <SkeletonLoader lines={6} />}

        {isError && (
          <EmptyState
            title="Failed to load templates"
            message={errorMessage(error, 'The templates endpoint is unavailable.')}
            icon={LayoutTemplate}
          />
        )}

        {data && templates.length === 0 && (
          <EmptyState
            title="No templates"
            message="Add YAML files under templates/ to reuse a flowgroup pattern with parameters."
            icon={LayoutTemplate}
          />
        )}

        {templates.length > 0 && (
          <div className="grid grid-cols-[200px_minmax(0,1fr)] gap-4">
            <nav aria-label="Templates" className="rounded-lg border border-border bg-card p-1">
              <ul className="space-y-0.5">
                {templates.map((name) => (
                  <li key={name}>
                    <button
                      type="button"
                      onClick={() => setSelected(name)}
                      aria-current={name === active ? 'true' : undefined}
                      className={cn(
                        'w-full truncate rounded-md px-2 py-1.5 text-left font-mono text-xs transition-colors',
                        name === active
                          ? 'bg-muted font-medium text-foreground'
                          : 'text-muted-foreground hover:bg-muted/50 hover:text-foreground',
                      )}
                    >
                      {name}
                    </button>
                  </li>
                ))}
              </ul>
            </nav>
            <div className="rounded-lg border border-border bg-card p-4">
              {active && <TemplateDetailPanel name={active} />}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
