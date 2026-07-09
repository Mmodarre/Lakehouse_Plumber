import { Check, ShieldQuestion, X } from 'lucide-react'
import { Button } from '../ui/button'
import { useResolveApproval } from '../../hooks/useAssistant'
import { useHealth } from '../../hooks/useProject'
import type { ApprovalAction, ApprovalParams } from '../../types/assistant'
import { toolDisplay } from './toolDisplay'

const RESOLUTION_LABEL: Record<ApprovalAction, string> = {
  accept: 'Accepted',
  decline: 'Declined',
  cancel: 'Cancelled — turn stopped',
}

/** Parse the params' content_preview (the tool input as JSON, possibly
 * truncated at the backend's preview cap — then it fails to parse). */
function previewArgs(params: ApprovalParams): Record<string, unknown> | null {
  if (typeof params.content_preview !== 'string') return null
  try {
    const parsed: unknown = JSON.parse(params.content_preview)
    return parsed !== null && typeof parsed === 'object'
      ? (parsed as Record<string, unknown>)
      : null
  } catch {
    return null
  }
}

/**
 * One elicitation (approval request). Shows the same friendly tool summary
 * as ToolCallCard, with the full arguments behind a disclosure.
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
  const { data: health } = useHealth()

  const respond = (action: ApprovalAction) => {
    approval.mutate({ elicitation_id: elicitationId, action })
  }

  const disabled = resolved !== null || approval.isPending
  const toolName =
    typeof params.tool_name === 'string' ? params.tool_name : null
  const args = previewArgs(params)
  const display =
    toolName === null ? null : toolDisplay(toolName, args ?? {}, health?.root)
  const message =
    display !== null
      ? `Allow ${display.label.toLowerCase()}?`
      : typeof params.message === 'string'
        ? params.message
        : 'Approval required'

  return (
    <div className="rounded-md border border-warning/50 bg-warning/10 px-2.5 py-2">
      <div className="flex items-start gap-1.5">
        <ShieldQuestion
          className="mt-0.5 size-3.5 shrink-0 text-warning"
          aria-hidden="true"
        />
        <div className="min-w-0 flex-1 text-xs text-foreground">
          <p className="font-medium">{message}</p>
          {display?.detail && (
            <p className="mt-1 truncate rounded bg-muted px-1.5 py-1 font-mono text-2xs text-foreground">
              {display.detail}
            </p>
          )}
          <details className="mt-1">
            <summary className="cursor-pointer text-2xs text-muted-foreground select-none">
              Full request
            </summary>
            <pre className="mt-1 max-h-32 overflow-auto rounded bg-muted p-1.5 text-2xs">
              {args !== null
                ? JSON.stringify(args, null, 2)
                : JSON.stringify(params, null, 2)}
            </pre>
          </details>
        </div>
      </div>
      {resolved !== null ? (
        <p className="mt-1.5 flex items-center gap-1 text-xs text-muted-foreground">
          {resolved === 'accept' ? (
            <Check className="size-3.5 text-success" aria-hidden="true" />
          ) : (
            <X className="size-3.5" aria-hidden="true" />
          )}
          {RESOLUTION_LABEL[resolved]}
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
