import { Trash2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { SchemaKindProvider } from '../../common/SchemaKindContext'
import type { ValidationIssue } from '../../../lib/config-model'
import { listPipelinePassthroughKeys } from '../../../lib/config-model'
import { PassthroughKeysCard } from '../PassthroughKeysCard'
import { DraftInput } from '../fields/DraftInput'
import { FieldChrome } from '../fields/FieldChrome'
import { PermissionsEditor } from '../fields/PermissionsEditor'
import { SectionCard } from '../SectionCard'
import { ClustersEditor } from './ClustersEditor'
import { GroupMembershipEditor } from './GroupMembershipEditor'
import { NotificationsEditor } from './NotificationsEditor'
import { PipelineAdvancedFields } from './PipelineAdvancedFields'
import { PipelineCoreFields } from './PipelineCoreFields'
import { PipelineMapsFields } from './PipelineMapsFields'
import type { DocFormApi } from '../shared/docFormSupport'

// ── PipelineDocForm — detail form for one document ───────────
//
// The active rail selection's form: a name/membership header for pipeline
// docs (project_defaults docs go straight to settings), the shared
// settings sections, and the per-doc passthrough chips. Everything writes
// through the DocFormApi (byte-surgical; deleting a key restores
// inheritance from project defaults / built-ins).

export interface PipelineDocFormProps {
  api: DocFormApi
  kind: 'defaults' | 'pipeline'
  /** Full doc snapshot (settings + the `pipeline` key for pipeline docs). */
  docSnapshot: Record<string, unknown>
  /** Names involved in a duplicate clash anywhere in the file. */
  duplicates: ReadonlySet<string>
  /** Issues scoped to the document itself (path [] or the defaults base). */
  docScopeIssues: ValidationIssue[]
  onDelete: () => void
  /** Focus the group-membership combobox (doc just added via "Add group"). */
  focusMembership?: boolean
}

export function PipelineDocForm({
  api,
  kind,
  docSnapshot,
  duplicates,
  docScopeIssues,
  onDelete,
  focusMembership = false,
}: PipelineDocFormProps) {
  const idPrefix = `doc${api.docIndex}`
  const rawPipeline = docSnapshot.pipeline
  const passthroughKeys = listPipelinePassthroughKeys(docSnapshot)

  return (
    <SchemaKindProvider kind="pipeline_config">
    <div className="space-y-4">
      <div className="flex items-start justify-between gap-3">
        <h3 className="text-sm font-semibold text-foreground">
          {kind === 'defaults' ? 'Project defaults' : 'Pipeline document'}
        </h3>
        <Button type="button" variant="outline" size="sm" onClick={onDelete}>
          <Trash2 aria-hidden="true" />
          Delete document
        </Button>
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

      {kind === 'pipeline' &&
        (Array.isArray(rawPipeline) ? (
          <GroupMembershipEditor
            id={`${idPrefix}-group`}
            members={rawPipeline}
            duplicates={duplicates}
            onAdd={(name) => api.set(['pipeline', rawPipeline.length], name)}
            onRemove={(index) => api.del(['pipeline', index])}
            issue={api.issueAt(['pipeline'])?.message}
            memberIssue={(index) => api.issueAt(['pipeline', index])?.message}
            autoFocus={focusMembership}
          />
        ) : (
          <FieldChrome
            id={`${idPrefix}-name`}
            label="Pipeline name"
            issue={
              api.issueAt(['pipeline'])?.message ??
              (duplicates.has(String(rawPipeline ?? ''))
                ? `'${String(rawPipeline)}' is also defined in another document`
                : undefined)
            }
          >
            <DraftInput
              id={`${idPrefix}-name`}
              initial={typeof rawPipeline === 'string' ? rawPipeline : String(rawPipeline ?? '')}
              // An empty name never deletes the key (that would change the
              // doc's classification) — the commit is simply refused.
              onCommit={(next) => {
                if (next.trim() !== '') api.set(['pipeline'], next.trim())
              }}
              monospace
              aria-describedby={`${idPrefix}-name-issue`}
            />
          </FieldChrome>
        ))}

      <PipelineCoreFields api={api} idPrefix={idPrefix} />
      <ClustersEditor api={api} idPrefix={idPrefix} />
      <PipelineMapsFields api={api} idPrefix={idPrefix} />
      <NotificationsEditor api={api} idPrefix={idPrefix} />
      <SectionCard
        title="Permissions"
        description="Each entry: a level plus exactly one user / group / service principal."
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
      <PipelineAdvancedFields api={api} idPrefix={idPrefix} />
      <PassthroughKeysCard keys={passthroughKeys} />
    </div>
    </SchemaKindProvider>
  )
}
