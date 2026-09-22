import { Check } from 'lucide-react'
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from '../../ui/command'
import { cn } from '../../../lib/utils'

export default function PipelineFilterOptions({ pipelines, shownFilter, select }: {
  pipelines: string[]; shownFilter: string | null; select: (value: string | null) => void
}) {
  return (
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
  )
}
