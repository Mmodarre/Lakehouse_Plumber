import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '../ui/alert-dialog'

interface DiscardChangesDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  description: string
  onDiscard: () => void
}

/** Unsaved-changes confirmation (replaces the former native browser confirm
 * prompt). Shown when a close is attempted while an editor buffer is dirty:
 * "Discard changes" runs the caller's close action, while cancel/Escape
 * dismisses only this prompt and keeps the editor open. */
export function DiscardChangesDialog({
  open,
  onOpenChange,
  description,
  onDiscard,
}: DiscardChangesDialogProps) {
  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle className="text-base">Discard unsaved changes?</AlertDialogTitle>
          <AlertDialogDescription className="text-xs">{description}</AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel size="sm">Keep editing</AlertDialogCancel>
          <AlertDialogAction variant="destructive" size="sm" onClick={onDiscard}>
            Discard changes
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}
