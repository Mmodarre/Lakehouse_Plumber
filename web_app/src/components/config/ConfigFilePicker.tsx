import { useMemo, useState } from 'react'
import { Check, ChevronsUpDown, FileCog } from 'lucide-react'
import { Button } from '@/components/ui/button'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { cn } from '@/lib/utils'
import { useFileList } from '../../hooks/useFiles'
import { listConfigYamlFiles } from './configFileSupport'
import type { ConfigTabKind } from './configFileSupport'

// ── ConfigFilePicker — choose the tab's config file ──────────
//
// Combobox (popover + command, same pattern as the header's pipeline
// filter) over the shared files-tree query (['files']), filtered to
// `config/**/*.yaml|yml`. The tab's preferred kind sorts first
// (pipeline_config* on the pipeline tab, job_config* /
// monitoring_job_config* on the job tab) but ANY config/ YAML stays
// choosable. Controlled: the page persists the selection per tab in
// uiStore.selectedConfigFiles.

export interface ConfigFilePickerProps {
  /** Which tab's preference ordering to apply. */
  kind: ConfigTabKind
  /** Currently selected config file path (project-relative), or null. */
  value: string | null
  onSelect: (path: string) => void
}

export function ConfigFilePicker({ kind, value, onSelect }: ConfigFilePickerProps) {
  const { data: tree } = useFileList()
  const [open, setOpen] = useState(false)

  const files = useMemo(() => listConfigYamlFiles(tree, kind), [tree, kind])

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          role="combobox"
          aria-expanded={open}
          aria-label="Select config file"
          className="w-72 justify-between font-normal"
        >
          <span className="flex min-w-0 items-center gap-1.5">
            <FileCog className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
            <span className={cn('truncate', value && 'font-mono text-xs')}>
              {value ?? 'Select config file…'}
            </span>
          </span>
          <ChevronsUpDown className="size-3.5 shrink-0 text-muted-foreground" aria-hidden="true" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-80 p-0" align="end">
        <Command>
          <CommandInput placeholder="Search config files…" />
          <CommandList>
            <CommandEmpty>No YAML files under config/.</CommandEmpty>
            <CommandGroup>
              {files.map((path) => (
                <CommandItem
                  key={path}
                  value={path}
                  onSelect={() => {
                    onSelect(path)
                    setOpen(false)
                  }}
                >
                  <Check
                    className={cn('size-3.5', value === path ? 'opacity-100' : 'opacity-0')}
                  />
                  <span className="truncate font-mono text-xs">{path}</span>
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}
