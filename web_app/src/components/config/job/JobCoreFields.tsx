import { isPlainObject, JOB_BUILTIN_DEFAULTS } from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { BoolSwitch } from '../fields/BoolSwitch'
import { EnumSelect } from '../fields/EnumSelect'
import { KeyValueMapEditor } from '../fields/KeyValueMapEditor'
import { OptionalNumberField } from '../fields/OptionalNumberField'
import { OptionalTextField } from '../fields/OptionalTextField'
import type { DocFormApi } from '../shared/docFormSupport'
import type { JobFormVariant } from './JobDocForm'
import { delWithCascade } from './jobFormSupport'

// ── JobCoreFields — execution / tags / LHP knobs / cluster ───
//
// The scalar-ish settings the job templates render explicitly
// (EXPLICITLY_RENDERED_JOB_CONFIG_KEYS, job_generator.py:38-56), minus
// the blocks with their own editors (schedule, notifications,
// permissions). Variant differences mirror the consumers:
//   • generate_master_job / master_job_name are read from
//     project_defaults ONLY (job_generator.py:153-159) → editable on the
//     defaults form, an inert-key note elsewhere;
//   • notebook_cluster is rendered only by monitoring_job_resource.yml.j2
//     → an editor on the monitoring form, an inert-key note elsewhere
//     (it is a KNOWN key, so it never appears in the passthrough chips).

/** template renders performance_target verbatim; these are the API values. */
const PERFORMANCE_TARGETS = ['STANDARD', 'PERFORMANCE_OPTIMIZED'] as const

function kvHandlers(api: DocFormApi, key: string) {
  const raw = api.settings[key]
  const map = isPlainObject(raw) ? raw : undefined
  return {
    value: map,
    onSetEntry: (entryKey: string, value: string) => api.set([key, entryKey], value),
    // Rename preserves the RAW value (a rename alone never coerces).
    onRenameEntry: (oldKey: string, newKey: string) => {
      const rawValue = map?.[oldKey]
      api.del([key, oldKey])
      api.set([key, newKey], rawValue)
    },
    onRemoveEntry: (entryKey: string) => api.del([key, entryKey]),
    onDeleteKey: () => api.del([key]),
  }
}

/** Present values display coerced even when invalid (issue supplies why). */
function stringAt(settings: Record<string, unknown>, key: string): string | undefined {
  return key in settings ? String(settings[key]) : undefined
}

function InertKeysNote({ keys, reason }: { keys: string[]; reason: string }) {
  if (keys.length === 0) return null
  return (
    <p className="text-2xs text-warning">
      <span className="font-mono">{keys.join(', ')}</span> {reason}
    </p>
  )
}

export function JobCoreFields({
  api,
  variant,
  idPrefix,
}: {
  api: DocFormApi
  variant: JobFormVariant
  idPrefix: string
}) {
  const queueRaw = api.settings.queue
  const queue = isPlainObject(queueRaw) ? queueRaw : undefined
  const queueNotAMapping = queueRaw !== undefined && queue === undefined
  const notebookRaw = api.settings.notebook_cluster
  const notebook = isPlainObject(notebookRaw) ? notebookRaw : undefined
  const newCluster =
    notebook !== undefined && isPlainObject(notebook.new_cluster)
      ? notebook.new_cluster
      : undefined

  return (
    <>
      <SectionCard
        title="Execution"
        description="Unset fields inherit from project defaults, then LHP's built-ins."
      >
        <OptionalNumberField
          id={`${idPrefix}-max-concurrent-runs`}
          label="Maximum concurrent runs"
          value={api.settings.max_concurrent_runs}
          min={1}
          onSet={(value) => api.set(['max_concurrent_runs'], value)}
          onUnset={() => api.del(['max_concurrent_runs'])}
          placeholder={`${JOB_BUILTIN_DEFAULTS.max_concurrent_runs} (built-in default)`}
          issue={api.issueAt(['max_concurrent_runs'])?.message}
        />
        <EnumSelect
          id={`${idPrefix}-performance-target`}
          label="Performance target"
          value={stringAt(api.settings, 'performance_target')}
          options={PERFORMANCE_TARGETS}
          unsetLabel={`Not set (default: ${JOB_BUILTIN_DEFAULTS.performance_target})`}
          onSet={(value) => api.set(['performance_target'], value)}
          onUnset={() => api.del(['performance_target'])}
          helpPath={['performance_target']}
          issue={api.issueAt(['performance_target'])?.message}
        />
        <OptionalNumberField
          id={`${idPrefix}-timeout-seconds`}
          label="Timeout (seconds)"
          value={api.settings.timeout_seconds}
          min={1}
          onSet={(value) => api.set(['timeout_seconds'], value)}
          onUnset={() => api.del(['timeout_seconds'])}
          placeholder="No timeout"
          helpPath={['timeout_seconds']}
          issue={api.issueAt(['timeout_seconds'])?.message}
        />
        {queueNotAMapping ? (
          <p className="text-2xs text-warning">
            queue is not a mapping — edit it in the YAML view.
          </p>
        ) : (
          <BoolSwitch
            id={`${idPrefix}-queue`}
            label="Queue runs"
            value={typeof queue?.enabled === 'boolean' ? queue.enabled : undefined}
            defaultValue={JOB_BUILTIN_DEFAULTS.queue.enabled}
            onSet={(value) => api.set(['queue', 'enabled'], value)}
            onReset={() => delWithCascade(api, 'queue', 'enabled')}
            helpPath={['queue', 'enabled']}
            issue={api.issueAt(['queue', 'enabled'])?.message}
          />
        )}
      </SectionCard>

      <SectionCard title="Tags">
        <KeyValueMapEditor
          id={`${idPrefix}-tags`}
          label="Tags"
          {...kvHandlers(api, 'tags')}
          helpPath={['tags']}
          issue={api.issueAt(['tags'])?.message}
        />
      </SectionCard>

      {variant === 'defaults' ? (
        <SectionCard
          title="Master job"
          description="LHP control knobs — read from project defaults only, never emitted into the job resource."
        >
          <BoolSwitch
            id={`${idPrefix}-generate-master-job`}
            label="Generate master job"
            value={
              typeof api.settings.generate_master_job === 'boolean'
                ? api.settings.generate_master_job
                : undefined
            }
            defaultValue={JOB_BUILTIN_DEFAULTS.generate_master_job}
            onSet={(value) => api.set(['generate_master_job'], value)}
            onReset={() => api.del(['generate_master_job'])}
            helpPath={['generate_master_job']}
            issue={api.issueAt(['generate_master_job'])?.message}
          />
          <OptionalTextField
            id={`${idPrefix}-master-job-name`}
            label="Master job name"
            value={api.settings.master_job_name}
            onSet={(value) => api.set(['master_job_name'], value)}
            onUnset={() => api.del(['master_job_name'])}
            placeholder="<project>_master (auto-derived)"
            monospace
            issue={api.issueAt(['master_job_name'])?.message}
          />
        </SectionCard>
      ) : (
        <InertKeysNote
          keys={['generate_master_job', 'master_job_name'].filter(
            (key) => key in api.settings,
          )}
          reason={
            variant === 'monitoring'
              ? // Monitoring configs never feed master-job generation (it reads
                // job_config project_defaults only), and these knobs are excluded
                // from the monitoring template's verbatim passthrough.
                'has no effect in a monitoring job config — master-job settings are read only from job_config project defaults, and this key is not rendered into the job.'
              : 'only takes effect in the project defaults document — the value here is ignored by LHP and not rendered into the job.'
          }
        />
      )}

      {variant === 'monitoring' ? (
        <SectionCard
          title="Notebook cluster"
          description="Compute override for the union-event-logs notebook task. new_cluster keys render verbatim; existing_cluster_id applies only when new_cluster is absent."
        >
          {notebookRaw !== undefined && notebook === undefined ? (
            <p className="text-2xs text-warning">
              notebook_cluster is not a mapping — edit it in the YAML view.
            </p>
          ) : (
            <>
              <KeyValueMapEditor
                id={`${idPrefix}-new-cluster`}
                label="New cluster"
                value={newCluster}
                onSetEntry={(entryKey, value) =>
                  api.set(['notebook_cluster', 'new_cluster', entryKey], value)
                }
                onRenameEntry={(oldKey, newKey) => {
                  const rawValue = newCluster?.[oldKey]
                  api.del(['notebook_cluster', 'new_cluster', oldKey])
                  api.set(['notebook_cluster', 'new_cluster', newKey], rawValue)
                }}
                onRemoveEntry={(entryKey) =>
                  api.del(['notebook_cluster', 'new_cluster', entryKey])
                }
                onDeleteKey={() => delWithCascade(api, 'notebook_cluster', 'new_cluster')}
                helpPath={['notebook_cluster', 'new_cluster']}
                issue={api.issueAt(['notebook_cluster', 'new_cluster'])?.message}
              />
              <OptionalTextField
                id={`${idPrefix}-existing-cluster-id`}
                label="Existing cluster id"
                value={notebook?.existing_cluster_id}
                onSet={(value) => api.set(['notebook_cluster', 'existing_cluster_id'], value)}
                onUnset={() => delWithCascade(api, 'notebook_cluster', 'existing_cluster_id')}
                monospace
                issue={api.issueAt(['notebook_cluster', 'existing_cluster_id'])?.message}
              />
            </>
          )}
        </SectionCard>
      ) : (
        <InertKeysNote
          keys={'notebook_cluster' in api.settings ? ['notebook_cluster'] : []}
          reason="is only rendered for monitoring jobs — the orchestration job template ignores it (it is not passed through)."
        />
      )}
    </>
  )
}
