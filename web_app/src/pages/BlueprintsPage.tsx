import { useState } from 'react'
import { Boxes, ChevronDown, ChevronRight, FileCode2, PackageOpen } from 'lucide-react'
import { useBlueprints } from '../hooks/useBlueprints'
import { EmptyState } from '../components/common/EmptyState'
import { SkeletonLoader } from '../components/common/SkeletonLoader'
import { errorMessage } from '../lib/errors'
import type { BlueprintSummary } from '../types/api'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/utils'

function CountBadge({ label, count }: { label: string; count: number }) {
  return (
    <Badge variant="outline" className="rounded-sm px-1.5 text-2xs text-muted-foreground">
      <span className="tabular-nums font-medium text-foreground">{count}</span>
      {label}
    </Badge>
  )
}

function BlueprintCard({ blueprint }: { blueprint: BlueprintSummary }) {
  const [expanded, setExpanded] = useState(false)
  const instances = blueprint.instances ?? []
  const hasInstances = instances.length > 0

  return (
    <div className="rounded-lg border border-border bg-card">
      <div className="flex items-start gap-2 px-3 py-2.5">
        <Button
          variant="ghost"
          size="icon-xs"
          onClick={() => setExpanded((e) => !e)}
          disabled={!hasInstances}
          aria-expanded={expanded}
          aria-label={expanded ? 'Collapse instances' : 'Expand instances'}
          className={cn('mt-0.5 text-muted-foreground', !hasInstances && 'invisible')}
        >
          {expanded ? <ChevronDown /> : <ChevronRight />}
        </Button>
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <span className="font-mono text-sm font-medium text-foreground">
              {blueprint.name}
            </span>
            <Badge variant="outline" className="rounded-sm px-1.5 text-2xs text-muted-foreground">
              v{blueprint.version}
            </Badge>
            <span className="ml-auto flex items-center gap-1.5">
              <CountBadge label="params" count={blueprint.parameter_count} />
              <CountBadge label="flowgroups" count={blueprint.flowgroup_count} />
              <CountBadge label="instances" count={blueprint.instance_count} />
            </span>
          </div>
          {blueprint.description && (
            <p className="mt-1 text-xs text-muted-foreground">{blueprint.description}</p>
          )}
        </div>
      </div>

      {expanded && hasInstances && (
        <ul className="divide-y divide-border/60 border-t border-border">
          {instances.map((instance) => (
            <li
              key={instance.instance_file_path}
              className="grid grid-cols-[16px_minmax(0,1fr)_max-content] items-center gap-2 px-3 py-1.5 pl-10"
            >
              <FileCode2 className="size-3.5 text-muted-foreground" aria-hidden="true" />
              <span className="truncate font-mono text-xs text-foreground">
                {instance.instance_file_path}
              </span>
              <span className="flex items-center gap-1.5">
                <span className="text-2xs tabular-nums text-muted-foreground">
                  {instance.flowgroup_count}{' '}
                  {instance.flowgroup_count === 1 ? 'flowgroup' : 'flowgroups'}
                </span>
                {instance.pipelines.map((pipeline) => (
                  <Badge
                    key={pipeline}
                    className="h-4 rounded-sm border border-info/25 bg-info/12 px-1 text-2xs text-info"
                  >
                    <Boxes className="size-2.5" aria-hidden="true" />
                    {pipeline}
                  </Badge>
                ))}
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

export function BlueprintsPage() {
  const { data, isLoading, isError, error } = useBlueprints(true)

  return (
    <div className="h-full overflow-y-auto bg-background p-6">
      <div className="mx-auto w-full max-w-4xl space-y-4">
        <div>
          <h1 className="text-lg font-semibold text-foreground">Blueprints</h1>
          <p className="mt-1 text-xs text-muted-foreground">
            Reusable multi-flowgroup patterns and the instance files that expand them.
          </p>
        </div>

        {isLoading && <SkeletonLoader lines={6} />}

        {isError && (
          <EmptyState
            title="Failed to load blueprints"
            message={errorMessage(error, 'The blueprints endpoint is unavailable.')}
            icon={PackageOpen}
          />
        )}

        {data && data.blueprints.length === 0 && (
          <EmptyState
            title="No blueprints"
            message="Define blueprints under blueprints/ to stamp out repeated flowgroup patterns."
            icon={PackageOpen}
          />
        )}

        {data && data.blueprints.length > 0 && (
          <div className="space-y-2">
            {data.blueprints.map((blueprint) => (
              <BlueprintCard key={blueprint.name} blueprint={blueprint} />
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
