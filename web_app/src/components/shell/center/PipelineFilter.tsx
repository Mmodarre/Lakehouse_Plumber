import { useMemo, useState } from 'react'
import { Boxes, Check, ChevronsUpDown } from 'lucide-react'
import { useFlowgroups } from '../../../hooks/useFlowgroups'
import { useUIStore } from '../../../store/uiStore'
import { useSandboxScope } from '../../sandbox/useSandboxScope'
import { filterFlowgroupsForScope } from '../../sandbox/scopeFilter'
import { Button } from '../../ui/button'
import { Popover, PopoverContent, PopoverTrigger } from '../../ui/popover'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '../../ui/command'
import { cn } from '../../../lib/utils'

/** Pipeline scope picker for the project map's single toolbar row.
 *
 * SHOWS the sandbox-scoped subset of pipelines (via the same `useSandboxScope` +
 * `filterFlowgroupsForScope` the DAG filters by) rather than disabling under
 * sandbox mode; shows all pipelines when sandbox is off. Drives
 * `uiStore.pipelineFilter`, which DependencyGraphWithControls reads. */
export function PipelineFilter() {
  const { data: flowgroupData } = useFlowgroups()
  const scope = useSandboxScope()
  const pipelineFilter = useUIStore((s) => s.pipelineFilter)
  const setPipelineFilter = useUIStore((s) => s.setPipelineFilter)
  const [open, setOpen] = useState(false)

  const pipelines = useMemo(() => {
    const scoped = filterFlowgroupsForScope(flowgroupData?.flowgroups ?? [], scope)
    return [...new Set(scoped.map((fg) => fg.pipeline))].sort()
  }, [flowgroupData, scope])

  // Display-only: never surface a stale filter that fell out of the (scoped)
  // list — the store keeps the raw value, but the trigger/check-marks reflect
  // only what this scoped dropdown can actually show.
  const shownFilter =
    pipelineFilter && pipelines.includes(pipelineFilter) ? pipelineFilter : null

  const select = (value: string | null) => {
    setPipelineFilter(value)
    setOpen(false)
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          role="combobox"
          aria-expanded={open}
          aria-label="Filter by pipeline"
          className="w-44 justify-between font-normal"
        >
          <span className="flex min-w-0 items-center gap-1.5">
            <Boxes className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
            <span className="truncate">{shownFilter ?? 'All pipelines'}</span>
          </span>
          <ChevronsUpDown className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-56 p-0" align="start">
        <Command>
          <CommandInput placeholder="Search pipelines…" />
          <CommandList>
            <CommandEmpty>No pipelines found.</CommandEmpty>
            <CommandGroup>
              <CommandItem value="__all__" onSelect={() => select(null)}>
                <Check
                  className={cn('size-3.5', shownFilter === null ? 'opacity-100' : 'opacity-0')}
                />
                All pipelines
              </CommandItem>
              {pipelines.map((name) => (
                <CommandItem key={name} value={name} onSelect={() => select(name)}>
                  <Check
                    className={cn(
                      'size-3.5',
                      shownFilter === name ? 'opacity-100' : 'opacity-0',
                    )}
                  />
                  {name}
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}
