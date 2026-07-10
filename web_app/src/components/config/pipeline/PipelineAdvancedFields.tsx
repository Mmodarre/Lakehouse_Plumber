import { Badge } from '@/components/ui/badge'
import { isPlainObject } from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { EnumSelect } from '../fields/EnumSelect'
import { OptionalTextField } from '../fields/OptionalTextField'
import { StringListEditor } from '../fields/StringListEditor'
import type { DocFormApi } from '../shared/docFormSupport'

// ── PipelineAdvancedFields — event_log override + environment ─
//
// event_log has three loader states (bundle/manager.py
// _inject_project_event_log): absent → the project-level event_log from
// lhp.yaml is injected; `false` → explicit opt-out; a mapping → full
// replacement ({name, schema, catalog}). The mode select maps exactly
// onto those three states — switching to "Inherit" deletes the key.
//
// environment is rendered verbatim (toyaml) into the bundle resource;
// `dependencies` is the shape LHP itself consumes (wheel injection), so
// it gets a first-class list and every other environment key stays as a
// read-only chip.

const EVENT_LOG_MODES = [
  'Inherit from lhp.yaml',
  'Disabled for this pipeline',
  'Custom override',
] as const

function eventLogMode(value: unknown, present: boolean): (typeof EVENT_LOG_MODES)[number] {
  if (!present) return EVENT_LOG_MODES[0]
  if (value === false) return EVENT_LOG_MODES[1]
  return EVENT_LOG_MODES[2]
}

export function PipelineAdvancedFields({ api, idPrefix }: { api: DocFormApi; idPrefix: string }) {
  const eventLog = api.settings.event_log
  const eventLogPresent = 'event_log' in api.settings
  const mode = eventLogMode(eventLog, eventLogPresent)
  const eventLogMap = isPlainObject(eventLog) ? eventLog : undefined

  const environment = api.settings.environment
  const environmentMap = isPlainObject(environment) ? environment : undefined
  const dependencies = Array.isArray(environmentMap?.dependencies)
    ? environmentMap.dependencies
    : undefined
  const extraEnvKeys = environmentMap
    ? Object.keys(environmentMap).filter((key) => key !== 'dependencies')
    : []

  return (
    <>
      <SectionCard
        title="Event log"
        description="Overrides the project-level event_log injection for this document's pipelines."
      >
        <EnumSelect
          id={`${idPrefix}-event-log-mode`}
          label="Event log mode"
          value={mode}
          options={EVENT_LOG_MODES}
          onSet={(next) => {
            if (next === EVENT_LOG_MODES[0]) api.del(['event_log'])
            else if (next === EVENT_LOG_MODES[1]) api.set(['event_log'], false)
            else if (!isPlainObject(eventLog)) api.set(['event_log'], {})
          }}
          issue={api.issueAt(['event_log'])?.message}
        />
        {eventLogPresent && mode === EVENT_LOG_MODES[2] && !eventLogMap && (
          <p className="text-2xs text-warning">
            event_log has an unexpected value — choosing a mode above replaces it.
          </p>
        )}
        {eventLogMap && (
          <>
            <OptionalTextField
              id={`${idPrefix}-event-log-name`}
              label="Event log table name"
              value={eventLogMap.name}
              onSet={(value) => api.set(['event_log', 'name'], value)}
              onUnset={() => api.del(['event_log', 'name'])}
              monospace
            />
            <OptionalTextField
              id={`${idPrefix}-event-log-catalog`}
              label="Event log catalog"
              value={eventLogMap.catalog}
              onSet={(value) => api.set(['event_log', 'catalog'], value)}
              onUnset={() => api.del(['event_log', 'catalog'])}
              monospace
            />
            <OptionalTextField
              id={`${idPrefix}-event-log-schema`}
              label="Event log schema"
              value={eventLogMap.schema}
              onSet={(value) => api.set(['event_log', 'schema'], value)}
              onUnset={() => api.del(['event_log', 'schema'])}
              monospace
            />
          </>
        )}
      </SectionCard>

      <SectionCard
        title="Environment"
        description="pip dependencies installed for the pipeline (rendered verbatim into the bundle resource)."
      >
        <StringListEditor
          id={`${idPrefix}-dependencies`}
          label="Dependencies"
          value={dependencies}
          onEditItem={(index, value) => api.set(['environment', 'dependencies', index], value)}
          onAddItem={(value) =>
            dependencies === undefined
              ? api.set(['environment', 'dependencies'], [value])
              : api.set(['environment', 'dependencies', dependencies.length], value)
          }
          onRemoveItem={(index) => api.del(['environment', 'dependencies', index])}
          onDeleteKey={() =>
            // Removing the last dependency: drop the whole environment key
            // when dependencies was its only member (pristine absence).
            extraEnvKeys.length === 0
              ? api.del(['environment'])
              : api.del(['environment', 'dependencies'])
          }
          placeholder="package==1.0.0"
          monospace
          issue={api.issueAt(['environment'])?.message}
        />
        {extraEnvKeys.length > 0 && (
          <div className="flex flex-wrap items-center gap-1.5">
            <span className="text-2xs text-muted-foreground">Other environment keys:</span>
            {extraEnvKeys.map((key) => (
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
      </SectionCard>
    </>
  )
}
