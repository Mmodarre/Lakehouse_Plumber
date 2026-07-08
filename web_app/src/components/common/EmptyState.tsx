import { Inbox } from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import { Button } from '@/components/ui/button'

export interface EmptyStateAction {
  label: string
  onClick: () => void
  /** Button styling for the action — primary fill by default. */
  variant?: 'default' | 'outline'
}

export function EmptyState({
  title = 'No data',
  message = 'Nothing to display yet.',
  icon: Icon = Inbox,
  action,
}: {
  title?: string
  message?: string
  icon?: LucideIcon
  action?: EmptyStateAction
}) {
  return (
    <div className="flex flex-col items-center justify-center py-12 text-center">
      <div className="mb-3 flex size-10 items-center justify-center rounded-lg bg-muted">
        <Icon className="size-5 text-muted-foreground" aria-hidden="true" />
      </div>
      <p className="text-sm font-semibold text-foreground">{title}</p>
      <p className="mt-1 text-xs text-muted-foreground">{message}</p>
      {action && (
        <Button
          size="sm"
          variant={action.variant ?? 'default'}
          className="mt-4"
          onClick={action.onClick}
        >
          {action.label}
        </Button>
      )}
    </div>
  )
}
