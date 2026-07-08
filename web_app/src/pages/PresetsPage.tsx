import { useState } from 'react'
import { ChevronRight, GitMerge, SlidersHorizontal } from 'lucide-react'
import { usePresets, usePresetDetail } from '../hooks/usePresets'
import { EmptyState } from '../components/common/EmptyState'
import { SkeletonLoader } from '../components/common/SkeletonLoader'
import { JsonTree } from '../components/detail/JsonTree'
import { errorMessage } from '../lib/errors'
import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'

/** Base→leaf inheritance breadcrumb from the detail's `chain` — the
 * requested preset is last (highlighted). */
function ChainBreadcrumb({ chain }: { chain: string[] }) {
  if (chain.length === 0) return null
  return (
    <div className="flex flex-wrap items-center gap-1" aria-label="Inheritance chain, base to leaf">
      {chain.map((name, i) => {
        const isLeaf = i === chain.length - 1
        return (
          <span key={`${name}-${i}`} className="flex items-center gap-1">
            {i > 0 && (
              <ChevronRight className="size-3 text-muted-foreground" aria-hidden="true" />
            )}
            <Badge
              className={cn(
                'h-5 rounded-sm border px-1.5 font-mono text-2xs',
                isLeaf
                  ? 'border-primary/25 bg-primary/12 text-primary'
                  : 'border-border bg-muted/50 text-muted-foreground',
              )}
            >
              {name}
            </Badge>
          </span>
        )
      })}
    </div>
  )
}

function PresetDetailPanel({ name }: { name: string }) {
  const { data, isLoading, isError, error } = usePresetDetail(name)

  if (isLoading) return <SkeletonLoader lines={8} />
  if (isError) {
    return (
      <EmptyState
        title="Failed to load preset"
        message={errorMessage(error, `Could not load preset '${name}'.`)}
        icon={SlidersHorizontal}
      />
    )
  }
  if (!data) return null

  return (
    <div className="space-y-4">
      {data.chain.length > 1 && (
        <div>
          <h3 className="mb-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            Inheritance chain (base → leaf)
          </h3>
          <ChainBreadcrumb chain={data.chain} />
        </div>
      )}

      <div>
        <h3 className="mb-1.5 flex items-center gap-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
          <GitMerge className="size-3" aria-hidden="true" />
          Resolved configuration (merged{data.chain.length > 1 ? ', base → leaf' : ''})
        </h3>
        {data.resolved && Object.keys(data.resolved).length > 0 ? (
          <JsonTree data={data.resolved} />
        ) : (
          <p className="text-xs text-muted-foreground">
            This preset resolves to an empty configuration.
          </p>
        )}
      </div>

      <div>
        <h3 className="mb-1.5 text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
          Raw file content
        </h3>
        <JsonTree data={data.raw} />
      </div>
    </div>
  )
}

export function PresetsPage() {
  const { data, isLoading, isError, error } = usePresets()
  const [selected, setSelected] = useState<string | null>(null)

  const presets = data?.presets ?? []
  const active = selected ?? presets[0] ?? null

  return (
    <div className="h-full overflow-y-auto bg-background p-6">
      <div className="mx-auto w-full max-w-5xl space-y-4">
        <div>
          <h1 className="text-lg font-semibold text-foreground">Presets</h1>
          <p className="mt-1 text-xs text-muted-foreground">
            Reusable action defaults — each preset shows its raw file plus the
            inheritance-resolved merge applied at generation time.
          </p>
        </div>

        {isLoading && <SkeletonLoader lines={6} />}

        {isError && (
          <EmptyState
            title="Failed to load presets"
            message={errorMessage(error, 'The presets endpoint is unavailable.')}
            icon={SlidersHorizontal}
          />
        )}

        {data && presets.length === 0 && (
          <EmptyState
            title="No presets"
            message="Add YAML files under presets/ to share defaults across actions."
            icon={SlidersHorizontal}
          />
        )}

        {presets.length > 0 && (
          <div className="grid grid-cols-[200px_minmax(0,1fr)] gap-4">
            <nav aria-label="Presets" className="rounded-lg border border-border bg-card p-1">
              <ul className="space-y-0.5">
                {presets.map((name) => (
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
              {active && <PresetDetailPanel name={active} />}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
