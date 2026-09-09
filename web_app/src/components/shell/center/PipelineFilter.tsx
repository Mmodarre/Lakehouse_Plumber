import { lazy, Suspense, useEffect, useMemo, useState } from 'react'
import { Boxes, ChevronsUpDown } from 'lucide-react'
import { useFlowgroups } from '../../../hooks/useFlowgroups'
import { useUIStore } from '../../../store/uiStore'
import { useSandboxScope } from '../../sandbox/useSandboxScope'
import { filterFlowgroupsForScope } from '../../sandbox/scopeFilter'
import { Button } from '../../ui/button'
import { Popover, PopoverContent, PopoverTrigger } from '../../ui/popover'
const PipelineFilterOptions = lazy(() => import('./PipelineFilterOptions'))

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

  // Reconcile the actual store, including when this picker lives in the
  // persistent command bar and the project map is closed.
  useEffect(() => {
    if (flowgroupData && pipelineFilter && !pipelines.includes(pipelineFilter)) {
      setPipelineFilter(null)
    }
  }, [flowgroupData, pipelineFilter, pipelines, setPipelineFilter])
  const shownFilter = pipelineFilter

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
        {open && <Suspense fallback={<p role="status" className="p-3 text-xs text-muted-foreground">Loading pipeline search…</p>}><PipelineFilterOptions pipelines={pipelines} shownFilter={shownFilter} select={select} /></Suspense>}
      </PopoverContent>
    </Popover>
  )
}
