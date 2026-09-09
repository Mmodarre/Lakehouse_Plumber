import { lazy, Suspense, useState } from 'react'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'

const DiffEditor = lazy(() => import('../editor/DiffEditorWrapper'))

export function ConfigChangeReview({ original, modified }: { original: string; modified: string }) {
  const [open, setOpen] = useState(false)
  return (
    <>
      <Button type="button" variant="outline" size="sm" onClick={() => setOpen(true)}>
        Review unsaved changes
      </Button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="flex h-[75vh] max-w-[90vw] flex-col sm:max-w-[90vw]">
          <DialogHeader>
            <DialogTitle>Review configuration changes</DialogTitle>
            <DialogDescription>
              Saved source on the left; your working source on the right. This comparison is
              read-only.
            </DialogDescription>
          </DialogHeader>
          {open && (
            <div className="min-h-0 flex-1">
              <Suspense fallback={<p role="status">Loading change review…</p>}>
                <DiffEditor original={original} modified={modified} language="yaml" readOnly />
              </Suspense>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </>
  )
}
