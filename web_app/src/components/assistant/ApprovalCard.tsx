import { ShieldQuestion } from 'lucide-react'
import { Button } from '../ui/button'
import { useResolveApproval } from '../../hooks/useAssistant'
import type { ApprovalAction, ApprovalParams } from '../../types/assistant'

const KNOWN_FIELDS: Array<{ key: keyof ApprovalParams; label: string }> = [
  { key: 'phase', label: 'Phase' },
  { key: 'policy_name', label: 'Policy' },
]

/**
 * One elicitation (approval request). Renders the documented MCP fields
 * when present; the raw params always stay reachable behind a disclosure
 * (the exact shape was never observed live — spike S5).
 */
export function ApprovalCard({
  elicitationId,
  params,
  resolved,
}: {
  elicitationId: string
  params: ApprovalParams
  resolved: ApprovalAction | null
}) {
  const approval = useResolveApproval()

  const respond = (action: ApprovalAction) => {
    approval.mutate({ elicitation_id: elicitationId, action })
  }

  const disabled = resolved !== null || approval.isPending
  const message =
    typeof params.message === 'string' ? params.message : 'Approval required'

  return (
    <div className="rounded-md border border-warning/50 bg-warning/10 px-2.5 py-2">
      <div className="flex items-start gap-1.5">
        <ShieldQuestion
          className="mt-0.5 size-3.5 shrink-0 text-warning"
          aria-hidden="true"
        />
        <div className="min-w-0 flex-1 text-xs text-foreground">
          <p className="font-medium">{message}</p>
          {KNOWN_FIELDS.map(({ key, label }) =>
            typeof params[key] === 'string' ? (
              <p key={String(key)} className="mt-0.5 text-muted-foreground">
                {label}: {params[key]}
              </p>
            ) : null,
          )}
          {typeof params.content_preview === 'string' && (
            <pre className="mt-1 max-h-32 overflow-auto rounded bg-muted p-1.5 text-2xs">
              {params.content_preview}
            </pre>
          )}
          <details className="mt-1">
            <summary className="cursor-pointer text-2xs text-muted-foreground select-none">
              Raw request
            </summary>
            <pre className="mt-1 max-h-32 overflow-auto rounded bg-muted p-1.5 text-2xs">
              {JSON.stringify(params, null, 2)}
            </pre>
          </details>
        </div>
      </div>
      {resolved !== null ? (
        <p className="mt-1.5 text-xs text-muted-foreground">
          Response sent: {resolved}
        </p>
      ) : (
        <div className="mt-2 flex gap-1.5">
          <Button size="sm" disabled={disabled} onClick={() => respond('accept')}>
            Accept
          </Button>
          <Button
            size="sm"
            variant="outline"
            disabled={disabled}
            onClick={() => respond('decline')}
          >
            Decline
          </Button>
          <Button
            size="sm"
            variant="ghost"
            disabled={disabled}
            onClick={() => respond('cancel')}
          >
            Cancel
          </Button>
        </div>
      )}
      {approval.isError && (
        <p className="mt-1 text-xs text-error">
          Could not send the response: {(approval.error as Error).message}
        </p>
      )}
    </div>
  )
}
