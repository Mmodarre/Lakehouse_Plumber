import { useEffect, useRef, useState } from 'react'
import type { ReactNode } from 'react'
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
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Switch } from '@/components/ui/switch'

// ── SectionCard — one config section, with presence toggle ───
//
// Card frame shared by every form section. For OPTIONAL sections, the
// header switch controls the KEY'S PRESENCE in the file:
//   • OFF→ON calls `onEnable` (the section inserts its minimal skeleton)
//     and moves focus to the first field of the freshly shown content;
//   • ON→OFF asks for confirmation first — disabling deletes the whole
//     section key (pristine absence: no `key: null` / `key: {}` residue).
// Sections without a `presence` prop are always-present (no switch).

export interface SectionPresence {
  /** The section key currently exists in the document. */
  present: boolean
  /** Insert the minimal skeleton for this section. */
  onEnable: () => void
  /** Delete the section key (called after the user confirms). */
  onDisable: () => void
  /** Confirm-dialog body, e.g. `Removes the whole sandbox section from lhp.yaml.` */
  confirmText: string
  disabled?: boolean
}

export interface SectionCardProps {
  title: string
  description?: string
  /** Presence toggle for optional sections; omit for always-present cards. */
  presence?: SectionPresence
  /** Section body — rendered only while present. */
  children?: ReactNode
}

export function SectionCard({ title, description, presence, children }: SectionCardProps) {
  const [confirming, setConfirming] = useState(false)
  const contentRef = useRef<HTMLDivElement | null>(null)
  const present = presence?.present ?? true
  const prevPresent = useRef(present)

  // Focus the first field when the section just got enabled (not on mount).
  useEffect(() => {
    if (present && !prevPresent.current) {
      contentRef.current
        ?.querySelector<HTMLElement>('input, textarea, [role="switch"], [role="combobox"]')
        ?.focus()
    }
    prevPresent.current = present
  }, [present])

  return (
    <Card className="gap-3 py-4">
      <CardHeader className="px-4">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 space-y-1">
            <CardTitle className="text-xs">{title}</CardTitle>
            {description && <CardDescription className="text-2xs">{description}</CardDescription>}
          </div>
          {presence && (
            <Switch
              size="sm"
              checked={present}
              disabled={presence.disabled}
              onCheckedChange={(checked) => {
                if (checked) presence.onEnable()
                else setConfirming(true)
              }}
              aria-label={`Enable ${title} section`}
            />
          )}
        </div>
      </CardHeader>
      {present && children !== undefined && (
        <CardContent ref={contentRef} className="space-y-3 px-4">
          {children}
        </CardContent>
      )}

      {presence && (
        <AlertDialog open={confirming} onOpenChange={setConfirming}>
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle className="text-base">Remove this section?</AlertDialogTitle>
              <AlertDialogDescription className="text-xs">
                {presence.confirmText}
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel size="sm">Keep section</AlertDialogCancel>
              <AlertDialogAction
                variant="destructive"
                size="sm"
                onClick={() => {
                  setConfirming(false)
                  presence.onDisable()
                }}
              >
                Remove section
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
      )}
    </Card>
  )
}
