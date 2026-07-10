import { useState } from 'react'
import { FileWarning } from 'lucide-react'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'

// ── LegacyConvertBanner — legacy flat job_config notice ──────
//
// Shown above the form when a job_config file uses the legacy
// single-document layout (the whole file = project defaults). Conversion
// to the multi-document layout is EXPLICIT ONLY: this button + confirm
// dialog is the single code path that calls convertLegacyJobDoc — a save
// never converts, and nothing auto-prompts.

export function LegacyConvertBanner({ onConvert }: { onConvert: () => void }) {
  const [confirming, setConfirming] = useState(false)
  return (
    <>
      <div
        role="status"
        className="flex flex-wrap items-center gap-2 rounded-md border border-warning/40 bg-warning/10 px-3 py-2"
      >
        <FileWarning className="size-3.5 shrink-0 text-warning" aria-hidden="true" />
        <span className="text-xs text-foreground">
          This file uses the legacy single-document format — the whole file is
          read as project defaults. Per-job overrides need the multi-document
          format.
        </span>
        <Button
          type="button"
          variant="outline"
          size="xs"
          className="ml-auto shrink-0"
          onClick={() => setConfirming(true)}
        >
          Convert to multi-document format
        </Button>
      </div>

      <AlertDialog open={confirming} onOpenChange={setConfirming}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="text-base">
              Convert to the multi-document format?
            </AlertDialogTitle>
            <AlertDialogDescription className="text-xs">
              Moves everything in this file under a project_defaults: key. The
              file is rewritten and reformatted in the process — comments move
              with their settings, but their exact placement and spacing may
              shift. For job_config files the loader reads both formats
              identically, so generated output does not change. You can review
              the result before saving.
            </AlertDialogDescription>
            <AlertDialogDescription className="text-xs text-warning">
              Do not convert a file referenced by monitoring.job_config_path in
              lhp.yaml — monitoring configs must stay flat.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel size="sm">Keep current format</AlertDialogCancel>
            <AlertDialogAction
              size="sm"
              onClick={() => {
                setConfirming(false)
                onConvert()
              }}
            >
              Convert file
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}
