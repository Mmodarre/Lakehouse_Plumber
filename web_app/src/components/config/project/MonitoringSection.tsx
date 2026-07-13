import { Plus, X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { isPlainObject, parseLaxBool } from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { BoolSwitch } from '../fields/BoolSwitch'
import { OptionalNumberField } from '../fields/OptionalNumberField'
import { OptionalTextField } from '../fields/OptionalTextField'
import { SectionIssues } from './SectionIssues'
import type { ProjectFormApi } from './projectFormSupport'
import { issueText, issuesAtExactly } from './projectFormSupport'

// monitoring section (models/_monitoring.py MonitoringConfig, parsed by
// loaders/_monitoring_config_parser.py). Presence skeleton: {enabled: true}.
// A bare `monitoring:` (null) is valid — all defaults, ENABLED — so the
// fields render over `{}` and the first write replaces the null.
// Each materialized view takes AT MOST ONE of sql / sql_path; the
// config-model validator surfaces a per-row error when both are set.

const BASE = ['monitoring'] as const

/** [key, label, placeholder?] for the plain string fields. */
const TEXT_FIELDS: [string, string, string?][] = [
  ['pipeline_name', 'Pipeline name', 'default: <project>_event_log_monitoring'],
  ['catalog', 'Catalog', 'default: event_log catalog'],
  ['schema', 'Schema', 'default: event_log schema'],
  ['streaming_table', 'Streaming table', 'default: all_pipelines_event_log'],
  ['checkpoint_path', 'Checkpoint path'],
  ['job_config_path', 'Job config path'],
]

function MaterializedViewRow({
  form,
  view,
  index,
  count,
}: {
  form: ProjectFormApi
  view: Record<string, unknown>
  index: number
  count: number
}) {
  const path = [...BASE, 'materialized_views', index]
  const remove = () =>
    count === 1 ? form.del([...BASE, 'materialized_views']) : form.del(path)
  return (
    <div
      role="group"
      aria-label={`Materialized view ${index + 1}`}
      className="space-y-2 rounded-md border border-border p-3"
    >
      <div className="flex items-center justify-between gap-2">
        <p className="text-2xs font-medium text-muted-foreground">View {index + 1}</p>
        <Button
          type="button"
          variant="ghost"
          size="icon-sm"
          onClick={remove}
          aria-label={`Remove materialized view ${index + 1}`}
        >
          <X aria-hidden="true" />
        </Button>
      </div>
      <SectionIssues issues={issuesAtExactly(form.issues, path)} />
      <OptionalTextField
        id={`monitoring-mv-${index}-name`}
        label="Name"
        value={view.name}
        onSet={(v) => form.set([...path, 'name'], v)}
        onUnset={() => form.del([...path, 'name'])}
        monospace
      />
      <OptionalTextField
        id={`monitoring-mv-${index}-sql`}
        label="SQL"
        value={view.sql}
        onSet={(v) => form.set([...path, 'sql'], v)}
        onUnset={() => form.del([...path, 'sql'])}
        monospace
        multiline
        helpPath={[...path, 'sql']}
      />
      <OptionalTextField
        id={`monitoring-mv-${index}-sql-path`}
        label="SQL file path"
        value={view.sql_path}
        onSet={(v) => form.set([...path, 'sql_path'], v)}
        onUnset={() => form.del([...path, 'sql_path'])}
        monospace
        helpPath={[...path, 'sql_path']}
      />
    </div>
  )
}

export function MonitoringSection({ form }: { form: ProjectFormApi }) {
  const raw = form.doc.monitoring
  const present = 'monitoring' in form.doc
  const section = isPlainObject(raw) ? raw : {}
  const broken = present && raw !== null && !isPlainObject(raw)
  const rawViews = section.materialized_views
  const views = Array.isArray(rawViews) ? rawViews : undefined

  const addView = () => {
    if (views === undefined) form.setField([...BASE], 'materialized_views', [{ name: '' }])
    else form.set([...BASE, 'materialized_views', views.length], { name: '' })
  }

  return (
    <SectionCard
      title="Monitoring"
      description="Event-log union pipeline and monitoring materialized views."
      presence={{
        present,
        onEnable: () => form.set([...BASE], { enabled: true }),
        onDisable: () => form.del([...BASE]),
        confirmText: 'Removes the whole monitoring section from lhp.yaml.',
      }}
    >
      <SectionIssues issues={issuesAtExactly(form.issues, [...BASE])} />
      {!broken && (
        <>
          <BoolSwitch
            id="monitoring-enabled"
            label="Enabled"
            value={'enabled' in section ? parseLaxBool(section.enabled) : undefined}
            defaultValue={true}
            onSet={(v) => form.setField([...BASE], 'enabled', v)}
            onReset={() => form.del([...BASE, 'enabled'])}
            issue={issueText(form.issues, [...BASE, 'enabled'])?.message}
          />
          {TEXT_FIELDS.map(([key, label, placeholder]) => (
            <OptionalTextField
              key={key}
              id={`monitoring-${key}`}
              label={label}
              value={section[key]}
              onSet={(v) => form.setField([...BASE], key, v)}
              onUnset={() => form.del([...BASE, key])}
              monospace
              helpPath={[...BASE, key]}
              placeholder={placeholder}
              issue={issueText(form.issues, [...BASE, key])?.message}
            />
          ))}
          <OptionalNumberField
            id="monitoring-max-concurrent-streams"
            label="Max concurrent streams"
            value={section.max_concurrent_streams}
            min={1}
            max={20}
            onSet={(v) => form.setField([...BASE], 'max_concurrent_streams', v)}
            onUnset={() => form.del([...BASE, 'max_concurrent_streams'])}
            placeholder="default: 10"
            issue={issueText(form.issues, [...BASE, 'max_concurrent_streams'])?.message}
          />
          <BoolSwitch
            id="monitoring-enable-job-monitoring"
            label="Enable job monitoring"
            helpPath={[...BASE, 'enable_job_monitoring']}
            value={
              'enable_job_monitoring' in section
                ? parseLaxBool(section.enable_job_monitoring)
                : undefined
            }
            defaultValue={false}
            onSet={(v) => form.setField([...BASE], 'enable_job_monitoring', v)}
            onReset={() => form.del([...BASE, 'enable_job_monitoring'])}
            issue={issueText(form.issues, [...BASE, 'enable_job_monitoring'])?.message}
          />

          <div className="space-y-2">
            <p className="text-xs font-medium text-foreground">Materialized views</p>
            <SectionIssues
              issues={issuesAtExactly(form.issues, [...BASE, 'materialized_views'])}
            />
            {views?.map((view, index) =>
              isPlainObject(view) ? (
                <MaterializedViewRow
                  key={index}
                  form={form}
                  view={view}
                  index={index}
                  count={views.length}
                />
              ) : (
                <SectionIssues
                  key={index}
                  issues={issuesAtExactly(form.issues, [...BASE, 'materialized_views', index])}
                />
              ),
            )}
            <Button type="button" variant="outline" size="sm" onClick={addView}>
              <Plus aria-hidden="true" />
              Add materialized view
            </Button>
          </div>
        </>
      )}
    </SectionCard>
  )
}
