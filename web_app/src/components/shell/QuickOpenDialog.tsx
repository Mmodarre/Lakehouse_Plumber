import { useMemo, useState } from 'react'
import { Settings2 } from 'lucide-react'
import { CommandDialog, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from '../ui/command'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { useLayoutStore } from '@/store/layoutStore'
import { useFileList } from '@/hooks/useFiles'
import { useFlowgroups } from '@/hooks/useFlowgroups'
import { flattenFilePaths } from './explorer/explorerData'
import { openWorkspaceFile } from '@/workspace/openWorkspaceFile'

export default function QuickOpenDialog({ open, onOpenChange }: { open: boolean; onOpenChange: (open: boolean) => void }) {
  const [query, setQuery] = useState('')
  const { data: tree } = useFileList()
  const { data: flowgroups } = useFlowgroups()
  const paths = useMemo(() => flattenFilePaths(tree), [tree])
  const visiblePaths = useMemo(() => paths.filter((p) => p.toLowerCase().includes(query.toLowerCase())).slice(0, 100), [paths, query])
  const select = (fn: () => void) => { onOpenChange(false); fn() }
  return (
      <CommandDialog open={open} onOpenChange={onOpenChange} title="Quick open" description="Find a file, flowgroup or workspace command.">
        <CommandInput value={query} onValueChange={setQuery} placeholder="Search files, flowgroups and commands…" />
        <CommandList>
          <CommandEmpty>No matches.</CommandEmpty>
          <CommandGroup heading="Workspace">
            <CommandItem onSelect={() => select(() => useWorkspaceStore.getState().openProjectMap())}>Open project map</CommandItem>
            <CommandItem onSelect={() => select(() => useWorkspaceStore.getState().openConfigTab('lhp.yaml', 'project'))}><Settings2 />Project settings</CommandItem>
            <CommandItem onSelect={() => select(() => useLayoutStore.getState().toggleFocusMode())}>Toggle focus mode (Ctrl/⌘Shift+F)</CommandItem>
            <CommandItem onSelect={() => select(() => useLayoutStore.getState().toggleExplorer())}>Toggle explorer (Ctrl/⌘B)</CommandItem>
            <CommandItem onSelect={() => select(() => useLayoutStore.getState().toggleInspector())}>Toggle inspector (Ctrl/⌘I)</CommandItem>
            <CommandItem onSelect={() => select(() => useLayoutStore.getState().toggleBottom())}>Toggle problems and runs (Ctrl/⌘J)</CommandItem>
            <CommandItem onSelect={() => select(() => { const l = useLayoutStore.getState(); l.setDensity(l.density === 'compact' ? 'comfortable' : 'compact') })}>Toggle comfortable / compact density</CommandItem>
          </CommandGroup>
          <CommandGroup heading="Flowgroups">
            {(flowgroups?.flowgroups ?? []).filter((f) => `${f.pipeline}/${f.name}`.toLowerCase().includes(query.toLowerCase())).slice(0, 60).map((f) => <CommandItem key={`${f.pipeline}/${f.name}`} value={`flowgroup ${f.pipeline}/${f.name}`} onSelect={() => select(() => useWorkspaceStore.getState().openEntityTab(f.pipeline, f.name, f.source_file))}>{f.pipeline} / {f.name}</CommandItem>)}
          </CommandGroup>
          <CommandGroup heading="Files (up to 100 matches)">{visiblePaths.map((path) => <CommandItem key={path} value={`file ${path}`} onSelect={() => select(() => void openWorkspaceFile(path))}>{path}</CommandItem>)}</CommandGroup>
        </CommandList>
      </CommandDialog>
  )
}
