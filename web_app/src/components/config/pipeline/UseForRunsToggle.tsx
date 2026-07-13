import { FieldLabel } from '@/components/config/fields/FieldLabel'
import { Switch } from '@/components/ui/switch'
import { useUIStore } from '../../../store/uiStore'

// ── UseForRunsToggle — bind the picked file to Validate/Generate ──
//
// Rendered in the pipeline editor's header-actions slot. The binding
// (uiStore.selectedPipelineConfig) tracks the file it was enabled FOR, not
// the picker: switching the picked file never silently changes what runs
// use — the switch simply reads unchecked until re-enabled on the new file
// (the caption then names the file that is still bound). Clearing works
// here (toggle off) and from the header's run-config chip.
//
// R5 consequence stated in the caption, not a tooltip: on bundle projects
// an IDE Generate with a bound config also writes resources/lhp/.

export function UseForRunsToggle({ path }: { path: string }) {
  const selected = useUIStore((s) => s.selectedPipelineConfig)
  const setSelected = useUIStore((s) => s.setSelectedPipelineConfig)
  const checked = selected === path
  const otherName =
    selected !== null && selected !== path ? (selected.split('/').pop() ?? selected) : null

  return (
    <div className="flex max-w-72 flex-col items-end gap-0.5">
      <div className="flex items-center gap-2">
        <FieldLabel
          htmlFor="use-for-runs"
          label="Use for runs"
          help="Use this file for Validate and Generate. The binding sticks to this file until you enable another."
        />
        <Switch
          id="use-for-runs"
          size="sm"
          checked={checked}
          onCheckedChange={(on) => setSelected(on ? path : null)}
          aria-describedby="use-for-runs-note"
        />
      </div>
      <p id="use-for-runs-note" className="text-right text-2xs text-muted-foreground">
        {otherName !== null ? (
          <>
            Runs currently use <span className="font-mono">{otherName}</span> — toggle on to
            switch to this file.
          </>
        ) : (
          <>
            Validate/Generate use this file. On bundle projects (databricks.yml), Generate
            also writes resources/lhp/.
          </>
        )}
      </p>
    </div>
  )
}
