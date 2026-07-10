import { Badge } from '@/components/ui/badge'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { isPlainObject, JOB_BUILTIN_DEFAULTS } from '../../../lib/config-model'
import { flatPassthroughKeys } from './jobFormSupport'

// ── JobEditorCards — the job editor's static/read-only cards ─
//
// Split out of JobConfigEditor so the orchestration file stays lean: the
// built-in defaults ghost card, the card for documents the loader skips,
// and the monitoring flat-format explainer.

/** `queue: {enabled: true}` etc. need JSON rendering, not String(). */
function builtinValue(value: unknown): string {
  if (value !== null && typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

/** Read-only card for the rail's ghost row (DEFAULT_JOB_CONFIG). */
export function JobBuiltinDefaultsCard() {
  return (
    <Card className="gap-3 py-4" data-testid="builtin-defaults-card">
      <CardHeader className="px-4">
        <CardTitle className="text-xs">Built-in defaults</CardTitle>
        <CardDescription className="text-2xs">
          Built into LHP — the lowest merge layer. A value here applies whenever
          neither project defaults nor a job's own document sets the key.
        </CardDescription>
      </CardHeader>
      <CardContent className="px-4">
        <dl className="space-y-1">
          {Object.entries(JOB_BUILTIN_DEFAULTS).map(([key, value]) => (
            <div key={key} className="flex items-center gap-3">
              <dt className="w-40 shrink-0 font-mono text-xs text-muted-foreground">{key}</dt>
              <dd className="font-mono text-xs">{builtinValue(value)}</dd>
            </div>
          ))}
        </dl>
        <p className="mt-2 text-2xs text-muted-foreground">
          master_job_name: null means the master job is named
          &lt;project&gt;_master; generate_master_job and master_job_name are
          LHP control knobs, never emitted into the job resource.
        </p>
      </CardContent>
    </Card>
  )
}

/** Passthrough-only card for documents the loader skips. */
export function UnrecognizedJobDocCard({ doc }: { doc: unknown }) {
  const keys = isPlainObject(doc) ? flatPassthroughKeys(doc) : []
  return (
    <Card className="gap-3 py-4">
      <CardHeader className="px-4">
        <CardTitle className="text-xs">Unrecognized document</CardTitle>
        <CardDescription className="text-2xs">
          This document is skipped by LHP — it has neither project_defaults nor
          job_name. Its content is kept exactly as written on save.
        </CardDescription>
      </CardHeader>
      {keys.length > 0 && (
        <CardContent className="flex flex-wrap gap-1.5 px-4">
          {keys.map((key) => (
            <Badge
              key={key}
              variant="outline"
              className="rounded-sm px-1.5 font-mono text-2xs font-normal text-muted-foreground"
            >
              {key}
            </Badge>
          ))}
        </CardContent>
      )}
    </Card>
  )
}

/** Flat-format explainer above the monitoring settings form. */
export function MonitoringFormatCard() {
  return (
    <Card className="gap-3 py-4">
      <CardHeader className="px-4">
        <CardTitle className="text-xs">Monitoring job settings</CardTitle>
        <CardDescription className="text-2xs">
          Flat single-document format: the whole file is the monitoring job's
          settings, merged over LHP's built-in defaults — no project_defaults
          wrapper, no job_name (the job is named after the monitoring
          pipeline).
        </CardDescription>
      </CardHeader>
    </Card>
  )
}
