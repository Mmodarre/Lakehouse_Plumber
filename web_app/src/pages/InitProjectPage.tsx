import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  CircleCheck,
  CircleX,
  FolderPlus,
  FolderTree,
  Loader2,
  Package,
} from 'lucide-react'
import { toast } from 'sonner'
import { initProject } from '../api/project'
import { errorMessage } from '../lib/errors'
import type { InitProjectResponse } from '../types/api'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Separator } from '@/components/ui/separator'

// ── InitProjectPage — first-run scaffolding wizard ──────────
//
// Self-contained wizard for `POST /api/project/init`, valid while health
// reports `project_state === 'no_project'`. Routed at /init, and rendered
// automatically by Layout's no_project branch so the first-run experience
// is the wizard rather than a dead-end notice.

function CreatedList({
  title,
  icon: Icon,
  items,
}: {
  title: string
  icon: typeof FolderTree
  items: string[]
}) {
  if (items.length === 0) return null
  return (
    <div>
      <h3 className="mb-1.5 flex items-center gap-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
        <Icon className="size-3" aria-hidden="true" />
        {title} ({items.length})
      </h3>
      <ul className="space-y-0.5">
        {items.map((item) => (
          <li key={item} className="font-mono text-xs text-foreground">
            {item}
          </li>
        ))}
      </ul>
    </div>
  )
}

export function InitProjectPage() {
  const queryClient = useQueryClient()
  const [projectName, setProjectName] = useState('')
  const [bundle, setBundle] = useState(true)
  const [result, setResult] = useState<InitProjectResponse | null>(null)
  const [requestError, setRequestError] = useState<string | null>(null)

  const mutation = useMutation({
    mutationFn: initProject,
    onSuccess: (response) => {
      setResult(response)
      setRequestError(null)
      if (response.success) {
        toast.success('Project initialized')
        // Reload everything: health flips out of no_project and every
        // resource list now has a project behind it.
        void queryClient.invalidateQueries()
      }
    },
    onError: (err) => {
      setResult(null)
      setRequestError(
        errorMessage(err, 'Project initialization failed unexpectedly.'),
      )
    },
  })

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault()
    setResult(null)
    setRequestError(null)
    const trimmed = projectName.trim()
    mutation.mutate({ project_name: trimmed === '' ? null : trimmed, bundle })
  }

  const succeeded = result?.success === true
  const scaffoldError = result && !result.success ? result : null

  return (
    <div className="h-full overflow-y-auto bg-background p-6">
      <div className="mx-auto w-full max-w-lg space-y-4">
        <div className="text-center">
          <div className="mx-auto mb-3 flex size-10 items-center justify-center rounded-lg bg-muted">
            <FolderPlus className="size-5 text-muted-foreground" aria-hidden="true" />
          </div>
          <h1 className="text-lg font-semibold text-foreground">Initialize a project</h1>
          <p className="mt-1 text-xs text-muted-foreground">
            No LHP project was found at the server root. Scaffold one to get started.
          </p>
        </div>

        {!succeeded && (
          <form
            onSubmit={handleSubmit}
            className="space-y-4 rounded-lg border border-border bg-card p-4"
          >
            <div className="space-y-1.5">
              <label htmlFor="init-project-name" className="text-xs font-medium text-foreground">
                Project name
              </label>
              <Input
                id="init-project-name"
                value={projectName}
                onChange={(e) => setProjectName(e.target.value)}
                placeholder="Defaults to the project directory name"
                autoComplete="off"
              />
            </div>

            <label className="flex items-start gap-2">
              <input
                type="checkbox"
                checked={bundle}
                onChange={(e) => setBundle(e.target.checked)}
                className="mt-0.5 size-3.5 accent-primary"
              />
              <span>
                <span className="flex items-center gap-1.5 text-xs font-medium text-foreground">
                  <Package className="size-3.5 text-muted-foreground" aria-hidden="true" />
                  Enable Declarative Automation Bundle support
                </span>
                <span className="mt-0.5 block text-2xs text-muted-foreground">
                  Adds databricks.yml so generated pipelines deploy as a Databricks
                  Declarative Automation Bundle.
                </span>
              </span>
            </label>

            {(scaffoldError || requestError) && (
              <p role="alert" className="flex items-start gap-1.5 text-xs text-error">
                <CircleX className="mt-0.5 size-3.5 shrink-0" aria-hidden="true" />
                <span>
                  {scaffoldError
                    ? (scaffoldError.error_message ?? 'Project initialization failed.')
                    : requestError}
                  {scaffoldError?.error_code && (
                    <span className="ml-1 font-mono text-2xs text-muted-foreground">
                      [{scaffoldError.error_code}]
                    </span>
                  )}
                </span>
              </p>
            )}

            <Button type="submit" className="w-full" disabled={mutation.isPending}>
              {mutation.isPending ? (
                <Loader2 className="animate-spin" aria-hidden="true" />
              ) : (
                <FolderPlus aria-hidden="true" />
              )}
              Create project
            </Button>
          </form>
        )}

        {succeeded && result && (
          <div className="space-y-4 rounded-lg border border-border bg-card p-4">
            <p className="flex items-center gap-1.5 text-sm font-medium text-success">
              <CircleCheck className="size-4" aria-hidden="true" />
              Project initialized
              {result.bundle_enabled && (
                <span className="text-2xs font-normal text-muted-foreground">
                  (bundle support enabled)
                </span>
              )}
            </p>
            <Separator />
            <CreatedList title="Created directories" icon={FolderTree} items={result.created_dirs} />
            <CreatedList title="Created files" icon={FolderPlus} items={result.created_files} />
            <p className="text-2xs text-muted-foreground">
              The app is reloading its project data — use the navigation above to explore.
            </p>
          </div>
        )}
      </div>
    </div>
  )
}
