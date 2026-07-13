import { Trash2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { SchemaKindProvider } from '../../common/SchemaKindContext'
import type { ValidationIssue } from '../../../lib/config-model'
import { PassthroughKeysCard } from '../PassthroughKeysCard'
import { SectionCard } from '../SectionCard'
import { DraftInput } from '../fields/DraftInput'
import { FieldChrome } from '../fields/FieldChrome'
import { PermissionsEditor } from '../fields/PermissionsEditor'
import { StringListEditor } from '../fields/StringListEditor'
import type { DocFormApi } from '../shared/docFormSupport'
import { JobCoreFields } from './JobCoreFields'
import { JobNotificationsEditor } from './JobNotificationsEditor'
import { ScheduleEditor } from './ScheduleEditor'

// ── JobDocForm — detail form for one job settings mapping ────
//
// One form serves all three shapes, differing only in `variant`:
//   • 'defaults'   — a project_defaults doc (base ['project_defaults'])
//                    OR a legacy flat doc (base []): the loader reads both
//                    as project defaults. Master-job knobs are editable
//                    here (they are read from project defaults only).
//   • 'job'        — a job_name doc (base []): name/membership header on
//                    top, master-job knobs shown only as inert-key notes.
//   • 'monitoring' — a flat monitoring config (base []): notebook_cluster
//                    becomes editable, no name header, no delete.
// Everything writes through the DocFormApi (byte-surgical; deleting a key
// restores inheritance from project defaults / built-ins).

export type JobFormVariant = 'defaults' | 'job' | 'monitoring'

const FORM_TITLES: Record<JobFormVariant, string> = {
  defaults: 'Project defaults',
  job: 'Job document',
  monitoring: 'Settings',
}

export interface JobDocFormProps {
  api: DocFormApi
  variant: JobFormVariant
  /** Full doc snapshot (settings + the `job_name` key for job docs). */
  docSnapshot: Record<string, unknown>
  /** Names involved in a duplicate clash anywhere in the file. */
  duplicates: ReadonlySet<string>
  /** Issues scoped to the document itself (path [] or the defaults base). */
  docScopeIssues: ValidationIssue[]
  /** Passthrough chip keys (computed per file shape by the editor). */
  passthroughKeys: string[]
  /** Omit to hide the delete button (flat monitoring / legacy forms). */
  onDelete?: () => void
}

/** job_name header: single name field, membership list, or a type warning. */
function JobNameHeader({
  api,
  idPrefix,
  rawName,
  duplicates,
}: {
  api: DocFormApi
  idPrefix: string
  rawName: unknown
  duplicates: ReadonlySet<string>
}) {
  if (Array.isArray(rawName)) {
    return (
      <StringListEditor
        id={`${idPrefix}-job-names`}
        label="Job names"
        value={rawName}
        onEditItem={(index, value) => api.set(['job_name', index], value)}
        onAddItem={(value) => api.set(['job_name', rawName.length], value)}
        onRemoveItem={(index) => api.del(['job_name', index])}
        // Never delete job_name itself — that would reclassify the doc.
        // An empty list stays (VAL_003 blocks Save until a name exists).
        onDeleteKey={() => {}}
        allowEmpty
        monospace
        placeholder="job name"
        help="This document applies to every job listed here (each gets an independent copy)."
        issue={api.issueAt(['job_name'])?.message}
        itemIssue={(index) =>
          api.issueAt(['job_name', index])?.message ??
          (duplicates.has(String(rawName[index]))
            ? `'${String(rawName[index])}' is also defined in another document`
            : undefined)
        }
      />
    )
  }
  if (typeof rawName === 'string' || rawName === null || rawName === undefined) {
    return (
      <FieldChrome
        id={`${idPrefix}-job-name`}
        label="Job name"
        issue={
          api.issueAt(['job_name'])?.message ??
          (duplicates.has(String(rawName ?? ''))
            ? `'${String(rawName)}' is also defined in another document`
            : undefined)
        }
      >
        <DraftInput
          id={`${idPrefix}-job-name`}
          initial={typeof rawName === 'string' ? rawName : ''}
          // An empty name never deletes the key (that would change the
          // doc's classification) — the commit is simply refused.
          onCommit={(next) => {
            if (next.trim() !== '') api.set(['job_name'], next.trim())
          }}
          monospace
          aria-describedby={`${idPrefix}-job-name-issue`}
        />
      </FieldChrome>
    )
  }
  return (
    <p role="alert" className="text-2xs text-warning">
      {api.issueAt(['job_name'])?.message ??
        "job_name must be a string or a list — the loader skips this document."}
    </p>
  )
}

export function JobDocForm({
  api,
  variant,
  docSnapshot,
  duplicates,
  docScopeIssues,
  passthroughKeys,
  onDelete,
}: JobDocFormProps) {
  const idPrefix = `jobdoc${api.docIndex}`

  return (
    <SchemaKindProvider kind="job_config">
    <div className="space-y-4">
      <div className="flex items-start justify-between gap-3">
        <h3 className="text-sm font-semibold text-foreground">{FORM_TITLES[variant]}</h3>
        {onDelete && (
          <Button type="button" variant="outline" size="sm" onClick={onDelete}>
            <Trash2 aria-hidden="true" />
            Delete document
          </Button>
        )}
      </div>

      {docScopeIssues.length > 0 && (
        <div className="space-y-1">
          {docScopeIssues.map((issue, i) => (
            <p
              key={i}
              role="alert"
              className={
                issue.severity === 'error'
                  ? 'text-2xs text-destructive'
                  : 'text-2xs text-warning'
              }
            >
              {issue.message}
            </p>
          ))}
        </div>
      )}

      {variant === 'job' && (
        <JobNameHeader
          api={api}
          idPrefix={idPrefix}
          rawName={docSnapshot.job_name}
          duplicates={duplicates}
        />
      )}

      <JobCoreFields api={api} variant={variant} idPrefix={idPrefix} />
      <ScheduleEditor api={api} idPrefix={idPrefix} />
      <JobNotificationsEditor api={api} idPrefix={idPrefix} />
      <SectionCard
        title="Permissions"
        description="Each entry: a level plus exactly one principal. The job template renders only user_name and group_name — service-principal entries stay in the file but are not emitted."
      >
        <PermissionsEditor
          id={`${idPrefix}-permissions`}
          value={api.settings.permissions}
          issueAt={(rel) => api.issueAt(['permissions', ...rel])}
          set={(rel, value) => api.set(['permissions', ...rel], value)}
          del={(rel) => api.del(['permissions', ...rel])}
          onDeleteKey={() => api.del(['permissions'])}
        />
      </SectionCard>
      <PassthroughKeysCard
        keys={passthroughKeys}
        description="Not rendered explicitly by LHP — passed through into the Databricks job resource exactly as written."
      />
    </div>
    </SchemaKindProvider>
  )
}
