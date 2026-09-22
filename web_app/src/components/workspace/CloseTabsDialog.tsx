import { Button } from '../ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogTitle } from '../ui/dialog'

export function CloseTabsDialog({ paths, busy, onCancel, onDiscard, onSave }: {
  paths: string[] | null; busy: boolean; onCancel: () => void; onDiscard: () => void; onSave: () => void
}) {
  return <Dialog open={paths !== null} onOpenChange={(open) => { if (!open && !busy) onCancel() }}>
    <DialogContent onEscapeKeyDown={(e) => { if (busy) e.preventDefault() }} onPointerDownOutside={(e) => { if (busy) e.preventDefault() }}>
      <DialogTitle>Save changes before closing?</DialogTitle>
      <DialogDescription>Review the unsaved files below. Files that cannot be saved will stay open.</DialogDescription>
      <ul className="max-h-64 space-y-1 overflow-auto text-sm">{paths?.map((path) => <li key={path} className="break-all font-mono">{path}</li>)}</ul>
      <DialogFooter>
        <Button variant="outline" disabled={busy} onClick={onCancel}>Cancel</Button>
        <Button variant="destructive" disabled={busy} onClick={onDiscard}>Discard changes</Button>
        <Button disabled={busy} onClick={onSave}>{busy ? 'Saving…' : 'Save and close'}</Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>
}
