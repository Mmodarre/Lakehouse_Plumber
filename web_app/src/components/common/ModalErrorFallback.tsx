import { Button } from '../ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogTitle } from '../ui/dialog'

/**
 * Fallback panel shown when a modal's subtree throws.
 *
 * Rendered as the `fallback` of a per-modal {@link ErrorBoundary} so a crash
 * inside one dialog cannot take down the rest of the app. The "Close" button
 * calls `onClose`, which the caller wires to the matching uiStore close action;
 * that state change flips the boundary's `resetKeys`, clearing the error so the
 * (now-closed) modal renders cleanly again.
 */
export function ModalErrorFallback({ onClose }: { onClose: () => void }) {
  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
    >
      <DialogContent showCloseButton={false} className="sm:max-w-md">
        <DialogTitle className="text-sm font-semibold">This dialog crashed</DialogTitle>
        <DialogDescription className="text-xs">
          Something went wrong while rendering this dialog. Closing it will
          return you to the app.
        </DialogDescription>
        <DialogFooter>
          <Button size="sm" onClick={onClose}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
