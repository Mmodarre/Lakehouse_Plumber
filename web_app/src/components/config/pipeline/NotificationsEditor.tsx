import { Plus, X } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { isPlainObject } from '../../../lib/config-model'
import { SectionCard } from '../SectionCard'
import { StringListEditor } from '../fields/StringListEditor'
import type { DocFormApi } from '../shared/docFormSupport'

// ── NotificationsEditor — the notifications[] list ───────────
//
// Shape per the bundle template (pipeline_resource.yml.j2): a list of
// {email_recipients: [...], alerts: [...]} entries — the template
// iterates BOTH keys unconditionally, so new entries are seeded with both
// lists present (empty) and the row lists keep `[]` rather than deleting
// the keys (allowEmpty). Unknown entry keys are chipped read-only.

const ALERT_PLACEHOLDER = 'on-update-failure'

function NotificationEntry({
  api,
  idPrefix,
  index,
  entry,
  isLast,
}: {
  api: DocFormApi
  idPrefix: string
  index: number
  entry: Record<string, unknown>
  isLast: boolean
}) {
  const id = `${idPrefix}-notification-${index}`
  const unknownKeys = Object.keys(entry).filter(
    (key) => key !== 'email_recipients' && key !== 'alerts',
  )

  const list = (key: 'email_recipients' | 'alerts') => {
    const value = entry[key]
    return Array.isArray(value) ? value : undefined
  }
  const listHandlers = (key: 'email_recipients' | 'alerts') => {
    const items = list(key)
    return {
      value: items,
      onEditItem: (itemIndex: number, value: string) =>
        api.set(['notifications', index, key, itemIndex], value),
      onAddItem: (value: string) =>
        items === undefined
          ? api.set(['notifications', index, key], [value])
          : api.set(['notifications', index, key, items.length], value),
      onRemoveItem: (itemIndex: number) => api.del(['notifications', index, key, itemIndex]),
      onDeleteKey: () => api.del(['notifications', index, key]),
      allowEmpty: true,
    }
  }

  return (
    <Card className="gap-0 py-3">
      <CardContent className="space-y-3 px-4">
        <div className="flex items-center justify-between">
          <p className="text-2xs font-medium text-muted-foreground">
            Notification {index + 1}
          </p>
          <Button
            type="button"
            variant="ghost"
            size="icon-xs"
            aria-label={`Remove notification ${index + 1}`}
            onClick={() =>
              isLast ? api.del(['notifications']) : api.del(['notifications', index])
            }
          >
            <X aria-hidden="true" />
          </Button>
        </div>
        <StringListEditor
          id={`${id}-emails`}
          label="Email recipients"
          {...listHandlers('email_recipients')}
          placeholder="team@company.com"
          monospace
        />
        <StringListEditor
          id={`${id}-alerts`}
          label="Alerts"
          {...listHandlers('alerts')}
          placeholder={ALERT_PLACEHOLDER}
          monospace
          description="e.g. on-update-success, on-update-failure, on-update-fatal-failure, on-flow-failure"
        />
        {unknownKeys.length > 0 && (
          <div className="flex flex-wrap items-center gap-1.5">
            <span className="text-2xs text-muted-foreground">Other keys:</span>
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

export function NotificationsEditor({ api, idPrefix }: { api: DocFormApi; idPrefix: string }) {
  const raw = api.settings.notifications
  const notifications = Array.isArray(raw) ? raw : undefined

  return (
    <SectionCard title="Notifications" description="Email alerts for pipeline events.">
      {raw !== undefined && notifications === undefined ? (
        <p className="text-2xs text-warning">
          notifications is not a list — edit it via "Open raw YAML".
        </p>
      ) : (
        <>
          {(notifications ?? []).map((entry, index) =>
            isPlainObject(entry) ? (
              <NotificationEntry
                key={index}
                api={api}
                idPrefix={idPrefix}
                index={index}
                entry={entry}
                isLast={notifications?.length === 1}
              />
            ) : (
              <p key={index} className="text-2xs text-warning">
                Notification {index + 1} is not a mapping — edit it via "Open raw YAML".
              </p>
            ),
          )}
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-6 px-2 text-2xs"
            onClick={() =>
              notifications === undefined
                ? api.set(['notifications'], [{ email_recipients: [], alerts: [] }])
                : api.set(['notifications', notifications.length], {
                    email_recipients: [],
                    alerts: [],
                  })
            }
          >
            <Plus aria-hidden="true" />
            Add notification
          </Button>
        </>
      )}
    </SectionCard>
  )
}
