import { Plus, X } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { isPlainObject } from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { OptionalNumberField } from '../fields/OptionalNumberField'
import { OptionalTextField } from '../fields/OptionalTextField'
import type { DocFormApi } from '../shared/docFormSupport'

// ── ClustersEditor — the clusters[] list ─────────────────────
//
// First-class fields are EXACTLY the cluster keys the bundle template
// renders (templates/bundle/pipeline_resource.yml.j2): label,
// node_type_id, instance_pool_id, driver_node_type_id,
// driver_instance_pool_id, policy_id, autoscale{min_workers, max_workers,
// mode}. Any other cluster key is kept in the YAML but shown as a
// read-only chip — note the template does NOT pass unknown cluster keys
// through to the bundle resource (unlike unknown TOP-LEVEL keys), so the
// caption says they are ignored at render time.

const CLUSTER_RENDERED_KEYS = new Set([
  'label',
  'node_type_id',
  'instance_pool_id',
  'driver_node_type_id',
  'driver_instance_pool_id',
  'policy_id',
  'autoscale',
])

const TEXT_FIELDS: { key: string; label: string }[] = [
  { key: 'label', label: 'Label' },
  { key: 'node_type_id', label: 'Node type ID' },
  { key: 'instance_pool_id', label: 'Instance pool ID' },
  { key: 'driver_node_type_id', label: 'Driver node type ID' },
  { key: 'driver_instance_pool_id', label: 'Driver instance pool ID' },
  { key: 'policy_id', label: 'Policy ID' },
]

function ClusterEntry({
  api,
  idPrefix,
  index,
  cluster,
  isLast,
}: {
  api: DocFormApi
  idPrefix: string
  index: number
  cluster: Record<string, unknown>
  isLast: boolean
}) {
  const autoscale = isPlainObject(cluster.autoscale) ? cluster.autoscale : undefined
  const unknownKeys = Object.keys(cluster).filter((key) => !CLUSTER_RENDERED_KEYS.has(key))
  const id = `${idPrefix}-cluster-${index}`

  return (
    <Card className="gap-0 py-3">
      <CardContent className="space-y-3 px-4">
        <div className="flex items-center justify-between">
          <p className="text-2xs font-medium text-muted-foreground">Cluster {index + 1}</p>
          <Button
            type="button"
            variant="ghost"
            size="icon-xs"
            aria-label={`Remove cluster ${index + 1}`}
            onClick={() =>
              isLast ? api.del(['clusters']) : api.del(['clusters', index])
            }
          >
            <X aria-hidden="true" />
          </Button>
        </div>
        {TEXT_FIELDS.map(({ key, label }) => (
          <OptionalTextField
            key={key}
            id={`${id}-${key}`}
            label={label}
            value={cluster[key]}
            onSet={(value) => api.set(['clusters', index, key], value)}
            onUnset={() => api.del(['clusters', index, key])}
            monospace
            placeholder={key === 'label' ? 'default' : undefined}
          />
        ))}
        {autoscale ? (
          <div className="space-y-3 rounded-md border border-border p-3">
            <div className="flex items-center justify-between">
              <p className="text-2xs font-medium text-muted-foreground">Autoscale</p>
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="h-6 px-1.5 text-2xs text-muted-foreground"
                onClick={() => api.del(['clusters', index, 'autoscale'])}
              >
                Remove autoscale
              </Button>
            </div>
            <OptionalNumberField
              id={`${id}-autoscale-min`}
              label="Min workers"
              value={autoscale.min_workers}
              min={0}
              onSet={(value) => api.set(['clusters', index, 'autoscale', 'min_workers'], value)}
              onUnset={() => api.del(['clusters', index, 'autoscale', 'min_workers'])}
            />
            <OptionalNumberField
              id={`${id}-autoscale-max`}
              label="Max workers"
              value={autoscale.max_workers}
              min={0}
              onSet={(value) => api.set(['clusters', index, 'autoscale', 'max_workers'], value)}
              onUnset={() => api.del(['clusters', index, 'autoscale', 'max_workers'])}
            />
            <OptionalTextField
              id={`${id}-autoscale-mode`}
              label="Mode"
              value={autoscale.mode}
              onSet={(value) => api.set(['clusters', index, 'autoscale', 'mode'], value)}
              onUnset={() => api.del(['clusters', index, 'autoscale', 'mode'])}
              placeholder="ENHANCED"
              monospace
            />
          </div>
        ) : (
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-6 px-2 text-2xs"
            onClick={() =>
              api.set(['clusters', index, 'autoscale'], { min_workers: 1, max_workers: 4 })
            }
          >
            <Plus aria-hidden="true" />
            Add autoscale
          </Button>
        )}
        {unknownKeys.length > 0 && (
          <div className="flex flex-wrap items-center gap-1.5">
            <span className="text-2xs text-muted-foreground">
              Kept in the file, but not rendered by LHP's bundle template:
            </span>
            {unknownKeys.map((key) => (
              <Badge
                key={key}
                variant="outline"
                className="rounded-sm px-1.5 font-mono text-2xs font-normal text-muted-foreground"
              >
                {key}
              </Badge>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

export function ClustersEditor({ api, idPrefix }: { api: DocFormApi; idPrefix: string }) {
  const raw = api.settings.clusters
  const clusters = Array.isArray(raw) ? raw : undefined

  return (
    <SectionCard
      title="Clusters"
      description="Compute for classic (non-serverless) pipelines — rendered only when serverless is off."
    >
      {raw !== undefined && clusters === undefined ? (
        <p className="text-2xs text-warning">
          clusters is not a list — edit it via "Open raw YAML".
        </p>
      ) : (
        <>
          {(clusters ?? []).map((cluster, index) =>
            isPlainObject(cluster) ? (
              <ClusterEntry
                key={index}
                api={api}
                idPrefix={idPrefix}
                index={index}
                cluster={cluster}
                isLast={clusters?.length === 1}
              />
            ) : (
              <p key={index} className="text-2xs text-warning">
                Cluster {index + 1} is not a mapping — edit it via "Open raw YAML".
              </p>
            ),
          )}
          {clusters === undefined && (
            <p className="text-2xs text-muted-foreground">
              Not set — serverless pipelines need no clusters.
            </p>
          )}
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-6 px-2 text-2xs"
            onClick={() =>
              clusters === undefined
                ? api.set(['clusters'], [{ label: 'default' }])
                : api.set(['clusters', clusters.length], { label: 'default' })
            }
          >
            <Plus aria-hidden="true" />
            Add cluster
          </Button>
        </>
      )}
    </SectionCard>
  )
}
