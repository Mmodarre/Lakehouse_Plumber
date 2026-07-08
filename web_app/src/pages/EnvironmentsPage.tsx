import { useCallback, useState } from 'react'
import { KeyRound, Layers, PencilLine } from 'lucide-react'
import { toast } from 'sonner'
import { useEnvironments, useEnvironmentResolved } from '../hooks/useEnvironments'
import { EmptyState } from '../components/common/EmptyState'
import { SkeletonLoader } from '../components/common/SkeletonLoader'
import { JsonTree } from '../components/detail/JsonTree'
import { useWorkspaceStore } from '../store/workspaceStore'
import { fetchFileContentWithMeta } from '../api/files'
import { errorMessage } from '../lib/errors'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'

// Read-only resolved-substitution view, plus an "Edit substitutions"
// opener: the source file (`substitutions/<env>.yaml`) opens as a
// workspace editor buffer (same openBuffer flow as the file browser),
// and saved edits round-trip through `PUT /api/files/...`.

function TokensTable({ tokens }: { tokens: Record<string, string> }) {
  const entries = Object.entries(tokens)
  if (entries.length === 0) {
    return <p className="text-xs text-muted-foreground">No tokens resolved for this environment.</p>
  }
  return (
    <div className="overflow-x-auto rounded-md border border-border">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-border bg-muted/50 text-left text-2xs uppercase tracking-[0.05em] text-muted-foreground">
            <th className="px-3 py-2 font-semibold">Token</th>
            <th className="px-3 py-2 font-semibold">Value</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/60">
          {entries.map(([key, value]) => (
            <tr key={key}>
              <td className="whitespace-nowrap px-3 py-1.5 font-mono font-medium text-foreground">
                {key}
              </td>
              <td className="break-all px-3 py-1.5 font-mono text-muted-foreground">{value}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function ResolvedPanel({ env }: { env: string }) {
  const { data, isLoading, isError, error } = useEnvironmentResolved(env)

  if (isLoading) return <SkeletonLoader lines={8} />
  if (isError) {
    return (
      <EmptyState
        title="Failed to resolve substitutions"
        message={errorMessage(error, `Could not resolve environment '${env}'.`)}
        icon={Layers}
      />
    )
  }
  if (!data) return null

  const secretReferences = data.secret_references ?? []

  return (
    <div className="space-y-4">
      <div>
        <h3 className="mb-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
          Resolved tokens
        </h3>
        <TokensTable tokens={data.tokens} />
      </div>

      <div className="flex items-center justify-between rounded-md bg-muted/50 px-3 py-2">
        <span className="flex items-center gap-1.5 text-2xs text-muted-foreground">
          <KeyRound className="size-3" aria-hidden="true" />
          Default secret scope
        </span>
        <span className="font-mono text-xs text-foreground">
          {data.default_secret_scope ?? '—'}
        </span>
      </div>

      {secretReferences.length > 0 && (
        <div>
          <h3 className="mb-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            Secret references ({secretReferences.length})
          </h3>
          <ul className="space-y-1">
            {secretReferences.map((ref, i) => (
              <li
                key={`${ref.scope}-${ref.key}-${i}`}
                className="rounded-md bg-muted/50 px-2 py-1.5 font-mono text-xs text-foreground"
              >
                <span className="text-muted-foreground">{ref.scope}/</span>
                {ref.key}
              </li>
            ))}
          </ul>
        </div>
      )}

      {Object.keys(data.raw_mappings).length > 0 && (
        <div>
          <h3 className="mb-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            Raw mappings
          </h3>
          <JsonTree data={data.raw_mappings} />
        </div>
      )}

      <p className="flex items-center gap-1.5 text-2xs text-muted-foreground">
        <PencilLine className="size-3 shrink-0" aria-hidden="true" />
        <span>
          These values come from{' '}
          <code className="font-mono text-foreground/80">substitutions/{env}.yaml</code> — use
          “Edit substitutions” above to change them.
        </span>
      </p>
    </div>
  )
}

export function EnvironmentsPage() {
  const { data, isLoading, isError, error } = useEnvironments()
  const [selected, setSelected] = useState<string | null>(null)
  const openBuffer = useWorkspaceStore((s) => s.openBuffer)
  const setActiveBuffer = useWorkspaceStore((s) => s.setActive)

  const environments = data?.environments ?? []
  const active = selected ?? environments[0] ?? null

  // Open `substitutions/<env>.yaml` as a workspace buffer (mirrors the
  // file-browser opener: focus if already open, otherwise fetch + seed).
  const handleEdit = useCallback(
    async (env: string) => {
      const path = `substitutions/${env}.yaml`
      if (useWorkspaceStore.getState().buffers.some((b) => b.path === path)) {
        setActiveBuffer(path)
        return
      }
      try {
        const { content, etag } = await fetchFileContentWithMeta(path)
        openBuffer(path, { content, etag, exists: true })
      } catch (err) {
        toast.error(errorMessage(err, 'Failed to open substitutions file'))
      }
    },
    [openBuffer, setActiveBuffer],
  )

  return (
    <div className="h-full overflow-y-auto bg-background p-6">
      <div className="mx-auto w-full max-w-4xl space-y-4">
        <div>
          <h1 className="text-lg font-semibold text-foreground">Environments</h1>
          <p className="mt-1 text-xs text-muted-foreground">
            Per-environment substitution tokens, resolved exactly as generation sees them.
          </p>
        </div>

        {isLoading && <SkeletonLoader lines={6} />}

        {isError && (
          <EmptyState
            title="Failed to load environments"
            message={errorMessage(error, 'The environments endpoint is unavailable.')}
            icon={Layers}
          />
        )}

        {data && environments.length === 0 && (
          <EmptyState
            title="No environments"
            message="Add substitution files under substitutions/ to define environments."
            icon={Layers}
          />
        )}

        {environments.length > 0 && (
          <>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="flex flex-wrap items-center gap-1.5" role="tablist" aria-label="Environments">
                {environments.map((env) => (
                  <button
                    key={env}
                    type="button"
                    role="tab"
                    aria-selected={env === active}
                    onClick={() => setSelected(env)}
                    className="rounded-md"
                  >
                    <Badge
                      className={cn(
                        'h-6 rounded-md border px-2 font-mono text-xs transition-colors',
                        env === active
                          ? 'border-primary/25 bg-primary/12 text-primary'
                          : 'border-border bg-card text-muted-foreground hover:text-foreground',
                      )}
                    >
                      {env}
                    </Badge>
                  </button>
                ))}
              </div>
              {active && (
                <Button
                  type="button"
                  variant="outline"
                  size="xs"
                  onClick={() => void handleEdit(active)}
                >
                  <PencilLine aria-hidden="true" />
                  Edit substitutions
                </Button>
              )}
            </div>
            <div className="rounded-lg border border-border bg-card p-4">
              {active && <ResolvedPanel env={active} />}
            </div>
          </>
        )}
      </div>
    </div>
  )
}
