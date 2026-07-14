import { useState } from 'react'
import { Input } from '@/components/ui/input'
import { isPlainObject } from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { EnumSelect } from '../fields/EnumSelect'
import { FieldChrome } from '../fields/FieldChrome'
import { OptionalTextField } from '../fields/OptionalTextField'
import { displayString, issueId } from '../fields/fieldSupport'
import type { DocFormApi } from '../shared/docFormSupport'
import { delWithCascade } from './jobFormSupport'

// ── ScheduleEditor — the job `schedule` block ────────────────
//
// quartz_cron_expression / timezone_id / pause_status. The template
// (job_resource.yml.j2:91-95) renders ALL THREE lines whenever `schedule`
// is present, so the card nudges toward setting them together. Writes are
// per-field (setting one field on a doc without `schedule` splices
// exactly the schedule block); clearing a field deletes it, and clearing
// the LAST one deletes the whole `schedule` key (pristine absence — via
// delWithCascade). No cron parser: a static format hint + link-out.

/** Databricks pause_status enum (template comment: UNPAUSED). */
const PAUSE_STATUSES = ['UNPAUSED', 'PAUSED'] as const

/** Suggestions only — any IANA zone id is accepted as free text. */
const COMMON_TIMEZONES = [
  'UTC',
  'America/New_York',
  'America/Chicago',
  'America/Denver',
  'America/Los_Angeles',
  'America/Sao_Paulo',
  'Europe/London',
  'Europe/Paris',
  'Europe/Berlin',
  'Asia/Kolkata',
  'Asia/Singapore',
  'Asia/Tokyo',
  'Australia/Sydney',
] as const

/** Commit-on-blur text input with a datalist of common zone ids. */
function TimezoneInput({
  id,
  value,
  onSet,
  onUnset,
  issue,
}: {
  id: string
  value: unknown
  onSet: (value: string) => void
  onUnset: () => void
  issue?: { message: string; severity: 'error' | 'warning' }
}) {
  const initial = displayString(value)
  const [draft, setDraft] = useState(initial)
  // Re-sync when the committed value changes — render-phase adjustment.
  const [lastInitial, setLastInitial] = useState(initial)
  if (initial !== lastInitial) {
    setLastInitial(initial)
    setDraft(initial)
  }
  const commit = () => {
    const next = draft.trim()
    if (next === initial) return
    if (next === '') onUnset()
    else onSet(next)
  }
  return (
    <FieldChrome
      id={id}
      label="Time zone"
      helpPath={['schedule', 'timezone_id']}
      issue={issue?.message}
      issueSeverity={issue?.severity}
    >
      <>
        <Input
          id={id}
          list={`${id}-zones`}
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onBlur={commit}
          onKeyDown={(e) => {
            if (e.key === 'Enter') {
              e.preventDefault()
              commit()
            } else if (e.key === 'Escape') {
              setDraft(initial)
            }
          }}
          placeholder="UTC"
          spellCheck={false}
          autoComplete="off"
          aria-describedby={issueId(id)}
          className="max-w-64 font-mono text-xs"
        />
        <datalist id={`${id}-zones`}>
          {COMMON_TIMEZONES.map((zone) => (
            <option key={zone} value={zone} />
          ))}
        </datalist>
      </>
    </FieldChrome>
  )
}

export function ScheduleEditor({ api, idPrefix }: { api: DocFormApi; idPrefix: string }) {
  const raw = api.settings.schedule
  const schedule = isPlainObject(raw) ? raw : undefined
  const notAMapping = raw !== undefined && schedule === undefined

  const setField = (key: string, value: string) => api.set(['schedule', key], value)
  const unsetField = (key: string) => delWithCascade(api, 'schedule', key)
  // Present-but-invalid values display coerced (never as "Not set") — the
  // validator's warning supplies the issue line.
  const pauseValue =
    schedule !== undefined && 'pause_status' in schedule
      ? String(schedule.pause_status)
      : undefined

  return (
    <SectionCard
      title="Schedule"
      description="Cron trigger for the job. When a schedule is present the template renders all three fields — set them together. Clearing all three removes the schedule from the file."
    >
      {notAMapping ? (
        <p className="text-2xs text-warning">
          schedule is not a mapping — edit it in the YAML view.
        </p>
      ) : (
        <>
          <OptionalTextField
            id={`${idPrefix}-cron`}
            label="Cron expression"
            value={schedule?.quartz_cron_expression}
            onSet={(value) => setField('quartz_cron_expression', value)}
            onUnset={() => unsetField('quartz_cron_expression')}
            placeholder="0 0 8 * * ?"
            monospace
            helpPath={['schedule', 'quartz_cron_expression']}
            issue={api.issueAt(['schedule', 'quartz_cron_expression'])?.message}
            issueSeverity={api.issueAt(['schedule', 'quartz_cron_expression'])?.severity}
          />
          <p className="-mt-2 text-2xs text-muted-foreground">
            <a
              href="https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html"
              target="_blank"
              rel="noreferrer"
              className="underline underline-offset-2"
            >
              Quartz cron reference
            </a>
          </p>
          <TimezoneInput
            id={`${idPrefix}-timezone`}
            value={schedule?.timezone_id}
            onSet={(value) => setField('timezone_id', value)}
            onUnset={() => unsetField('timezone_id')}
            issue={api.issueAt(['schedule', 'timezone_id'])}
          />
          <EnumSelect
            id={`${idPrefix}-pause-status`}
            label="Pause status"
            value={pauseValue}
            options={PAUSE_STATUSES}
            unsetLabel="Not set"
            onSet={(value) => setField('pause_status', value)}
            onUnset={() => unsetField('pause_status')}
            issue={api.issueAt(['schedule', 'pause_status'])?.message}
          />
        </>
      )}
    </SectionCard>
  )
}
