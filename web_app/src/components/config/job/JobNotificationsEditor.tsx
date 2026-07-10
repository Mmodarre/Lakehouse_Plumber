import { useState } from 'react'
import { Plus, X } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { isPlainObject } from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { DraftInput } from '../fields/DraftInput'
import { StringListEditor } from '../fields/StringListEditor'
import { displayString } from '../fields/fieldSupport'
import type { DocFormApi } from '../shared/docFormSupport'
import { delWithCascade } from './jobFormSupport'

// ── JobNotificationsEditor — email + webhook notifications ───
//
// Shapes per job_resource.yml.j2: `email_notifications` holds on_start /
// on_success / on_failure STRING lists (:37-57); `webhook_notifications`
// holds the same three keys as lists of `{id}` entries — the template
// renders ONLY each entry's `id` (:58-78). Every list is rendered only
// when non-empty (`.get(...)` guards), so absent and empty are
// equivalent; the editors keep pristine absence — removing the last item
// deletes the list key, and deleting the block's last list deletes the
// block (delWithCascade). Unknown keys inside a block (e.g.
// no_alert_for_skipped_runs) are kept in the file but NOT rendered by the
// template — chipped for honesty.

const LIST_KEYS = ['on_start', 'on_success', 'on_failure'] as const
type ListKey = (typeof LIST_KEYS)[number]
type BlockKey = 'email_notifications' | 'webhook_notifications'

const LIST_LABELS: Record<ListKey, string> = {
  on_start: 'On start',
  on_success: 'On success',
  on_failure: 'On failure',
}

function listOf(block: Record<string, unknown> | undefined, key: ListKey): unknown[] | undefined {
  const value = block?.[key]
  return Array.isArray(value) ? value : undefined
}

function UnknownKeyChips({ block }: { block: Record<string, unknown> | undefined }) {
  const unknown = Object.keys(block ?? {}).filter(
    (key) => !(LIST_KEYS as readonly string[]).includes(key),
  )
  if (unknown.length === 0) return null
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      <span className="text-2xs text-muted-foreground">
        Other keys (kept, but not rendered by the job template):
      </span>
      {unknown.map((key) => (
        <Badge
          key={key}
          variant="outline"
          className="rounded-sm px-1.5 font-mono text-2xs font-normal text-muted-foreground"
        >
          {key}
        </Badge>
      ))}
    </div>
  )
}

/** One webhook `{id}` list (rows edit the id; other entry keys are kept). */
function WebhookIdList({
  api,
  id,
  listKey,
  block,
}: {
  api: DocFormApi
  id: string
  listKey: ListKey
  block: Record<string, unknown> | undefined
}) {
  const [addDraft, setAddDraft] = useState('')
  const items = listOf(block, listKey)
  const base: (string | number)[] = ['webhook_notifications', listKey]
  // Present-but-not-a-list: never render the add-flow over it — an add
  // would silently clobber the existing value.
  if (block !== undefined && listKey in block && items === undefined) {
    return (
      <p className="text-2xs text-warning">
        {LIST_LABELS[listKey]} ({listKey}) is not a list — edit it via "Open raw YAML".
      </p>
    )
  }

  const removeRow = (index: number) => {
    if (items !== undefined && items.length === 1) {
      delWithCascade(api, 'webhook_notifications', listKey)
    } else {
      api.del([...base, index])
    }
  }
  const commitAdd = () => {
    const next = addDraft.trim()
    if (next === '') return
    if (items === undefined) api.set(base, [{ id: next }])
    else api.set([...base, items.length], { id: next })
    setAddDraft('')
  }

  return (
    <div className="space-y-1.5">
      <Label htmlFor={`${id}-add`} className="text-xs">
        {LIST_LABELS[listKey]}
      </Label>
      {items === undefined ? (
        <p className="text-2xs text-muted-foreground">Not set</p>
      ) : (
        <ul className="space-y-1">
          {items.map((entry, index) => (
            <li key={index} className="flex items-center gap-1.5">
              {isPlainObject(entry) ? (
                <>
                  <DraftInput
                    initial={displayString(entry.id)}
                    onCommit={(next) =>
                      next.trim() === ''
                        ? removeRow(index)
                        : api.set([...base, index, 'id'], next.trim())
                    }
                    monospace
                    placeholder="notification destination id"
                    aria-label={`${LIST_LABELS[listKey]} webhook ${index + 1} id`}
                  />
                  {Object.keys(entry).some((key) => key !== 'id') && (
                    <Badge
                      variant="outline"
                      className="shrink-0 rounded-sm border-warning/50 px-1.5 text-2xs text-warning"
                      title="This entry has keys besides id — kept in the file, but the template renders only id"
                    >
                      +keys
                    </Badge>
                  )}
                </>
              ) : (
                <p className="flex-1 text-2xs text-warning">
                  Entry {index + 1} is not a mapping — edit it via "Open raw YAML".
                </p>
              )}
              <Button
                type="button"
                variant="ghost"
                size="icon-sm"
                onClick={() => removeRow(index)}
                aria-label={`Remove ${LIST_LABELS[listKey]} webhook ${index + 1}`}
              >
                <X aria-hidden="true" />
              </Button>
            </li>
          ))}
        </ul>
      )}
      <div className="flex items-center gap-1.5">
        <Input
          id={`${id}-add`}
          value={addDraft}
          onChange={(e) => setAddDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') {
              e.preventDefault()
              commitAdd()
            }
          }}
          placeholder="Add webhook id…"
          spellCheck={false}
          autoComplete="off"
          className="font-mono text-xs"
        />
        <Button
          type="button"
          variant="outline"
          size="icon-sm"
          onClick={commitAdd}
          disabled={addDraft.trim() === ''}
          aria-label={`Add ${LIST_LABELS[listKey]} webhook`}
        >
          <Plus aria-hidden="true" />
        </Button>
      </div>
    </div>
  )
}

function blockOf(api: DocFormApi, key: BlockKey): {
  block: Record<string, unknown> | undefined
  notAMapping: boolean
} {
  const raw = api.settings[key]
  const block = isPlainObject(raw) ? raw : undefined
  return { block, notAMapping: raw !== undefined && block === undefined }
}

export function JobNotificationsEditor({
  api,
  idPrefix,
}: {
  api: DocFormApi
  idPrefix: string
}) {
  const email = blockOf(api, 'email_notifications')
  const webhook = blockOf(api, 'webhook_notifications')

  return (
    <>
      <SectionCard
        title="Email notifications"
        description="Recipient lists per job event — a list is rendered only when it has entries."
      >
        {email.notAMapping ? (
          <p className="text-2xs text-warning">
            email_notifications is not a mapping — edit it via "Open raw YAML".
          </p>
        ) : (
          <>
            {LIST_KEYS.map((key) => {
              const items = listOf(email.block, key)
              // Present-but-not-a-list: never render "Not set" over it —
              // an add would silently clobber the existing value.
              if (email.block !== undefined && key in email.block && items === undefined) {
                return (
                  <p key={key} className="text-2xs text-warning">
                    {LIST_LABELS[key]} ({key}) is not a list — edit it via "Open raw YAML".
                  </p>
                )
              }
              return (
                <StringListEditor
                  key={key}
                  id={`${idPrefix}-email-${key}`}
                  label={LIST_LABELS[key]}
                  value={items}
                  onEditItem={(index, value) =>
                    api.set(['email_notifications', key, index], value)
                  }
                  onAddItem={(value) =>
                    items === undefined
                      ? api.set(['email_notifications', key], [value])
                      : api.set(['email_notifications', key, items.length], value)
                  }
                  onRemoveItem={(index) => api.del(['email_notifications', key, index])}
                  onDeleteKey={() => delWithCascade(api, 'email_notifications', key)}
                  placeholder="team@company.com"
                  monospace
                  issue={api.issueAt(['email_notifications', key])?.message}
                />
              )
            })}
            <UnknownKeyChips block={email.block} />
          </>
        )}
      </SectionCard>

      <SectionCard
        title="Webhook notifications"
        description="Notification-destination ids per job event — the template renders each entry's id."
      >
        {webhook.notAMapping ? (
          <p className="text-2xs text-warning">
            webhook_notifications is not a mapping — edit it via "Open raw YAML".
          </p>
        ) : (
          <>
            {LIST_KEYS.map((key) => (
              <WebhookIdList
                key={key}
                api={api}
                id={`${idPrefix}-webhook-${key}`}
                listKey={key}
                block={webhook.block}
              />
            ))}
            <UnknownKeyChips block={webhook.block} />
          </>
        )}
      </SectionCard>
    </>
  )
}
