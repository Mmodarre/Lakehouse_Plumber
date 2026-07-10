import { useEffect, useRef, useState } from 'react'
import { Check, Plus, X } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command'
import { Label } from '@/components/ui/label'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { cn } from '../../../lib/utils'
import { usePipelines } from '../../../hooks/usePipelines'

// ── GroupMembershipEditor — `pipeline: [a, b]` membership ────
//
// Chip list of the group's members plus a combobox that adds from the
// project's known pipelines (usePipelines) ∪ free text. Members whose
// name clashes with another document (or repeats inside this list) carry
// the duplicate badge inline, mirroring the rail.

export interface GroupMembershipEditorProps {
  /** DOM id prefix. */
  id: string
  /** Raw list items (non-strings display coerced). */
  members: readonly unknown[]
  /** Names involved in a duplicate clash anywhere in the file. */
  duplicates: ReadonlySet<string>
  onAdd: (name: string) => void
  onRemove: (index: number) => void
  /** List-level validation message (VAL_005 / VAL_011 / type issues). */
  issue?: string
  /** Per-member validation message (VAL_006 sits on the member index). */
  memberIssue?: (index: number) => string | undefined
  /** Focus the add-combobox on mount (freshly added group). */
  autoFocus?: boolean
  disabled?: boolean
}

export function GroupMembershipEditor({
  id,
  members,
  duplicates,
  onAdd,
  onRemove,
  issue,
  memberIssue,
  autoFocus = false,
  disabled,
}: GroupMembershipEditorProps) {
  const { data } = usePipelines()
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')
  const triggerRef = useRef<HTMLButtonElement | null>(null)

  useEffect(() => {
    if (autoFocus) triggerRef.current?.focus()
  }, [autoFocus])

  const memberNames = members.map((member) => String(member))
  const suggestions = (data?.pipelines ?? [])
    .map((pipeline) => pipeline.name)
    .filter((name) => !memberNames.includes(name))
  const trimmed = query.trim()
  const showFreeText =
    trimmed !== '' && !memberNames.includes(trimmed) && !suggestions.includes(trimmed)

  const add = (name: string) => {
    onAdd(name)
    setQuery('')
    setOpen(false)
  }

  const memberIssues = memberIssue
    ? members.map((_, index) => memberIssue(index)).filter((text): text is string => !!text)
    : []

  return (
    <div className="space-y-1.5">
      <Label className="text-xs" htmlFor={`${id}-members`}>
        Group members
      </Label>
      <div id={`${id}-members`} className="flex flex-wrap items-center gap-1.5">
        {memberNames.map((name, index) => (
          <Badge
            key={`${name}-${index}`}
            variant="outline"
            className={cn(
              'gap-1 rounded-sm px-1.5 font-mono text-2xs font-normal',
              duplicates.has(name) && 'border-destructive text-destructive',
            )}
          >
            {name}
            {duplicates.has(name) && <span className="font-sans">(duplicate)</span>}
            <button
              type="button"
              aria-label={`Remove ${name}`}
              className="ml-0.5 rounded-sm hover:text-foreground"
              onClick={() => onRemove(index)}
              disabled={disabled}
            >
              <X className="size-3" aria-hidden="true" />
            </button>
          </Badge>
        ))}
        <Popover open={open} onOpenChange={setOpen}>
          <PopoverTrigger asChild>
            <Button
              ref={triggerRef}
              type="button"
              variant="outline"
              size="sm"
              role="combobox"
              aria-expanded={open}
              aria-label="Add pipeline to group"
              className="h-6 px-2 text-2xs font-normal"
              disabled={disabled}
            >
              <Plus className="size-3" aria-hidden="true" />
              Add pipeline
            </Button>
          </PopoverTrigger>
          <PopoverContent className="w-72 p-0" align="start">
            <Command shouldFilter={false}>
              <CommandInput
                placeholder="Search pipelines…"
                value={query}
                onValueChange={setQuery}
              />
              <CommandList>
                <CommandEmpty>Type a name to add it.</CommandEmpty>
                <CommandGroup>
                  {suggestions
                    .filter((name) => trimmed === '' || name.includes(trimmed))
                    .map((name) => (
                      <CommandItem key={name} value={name} onSelect={() => add(name)}>
                        <Check className="size-3.5 opacity-0" aria-hidden="true" />
                        <span className="truncate font-mono text-xs">{name}</span>
                      </CommandItem>
                    ))}
                  {showFreeText && (
                    <CommandItem value={trimmed} onSelect={() => add(trimmed)}>
                      <Plus className="size-3.5" aria-hidden="true" />
                      <span className="truncate text-xs">Add "{trimmed}"</span>
                    </CommandItem>
                  )}
                </CommandGroup>
              </CommandList>
            </Command>
          </PopoverContent>
        </Popover>
      </div>
      {(issue !== undefined || memberIssues.length > 0) && (
        <div className="space-y-0.5">
          {issue !== undefined && (
            <p role="alert" className="text-2xs text-destructive">
              {issue}
            </p>
          )}
          {memberIssues.map((text, i) => (
            <p key={i} role="alert" className="text-2xs text-destructive">
              {text}
            </p>
          ))}
        </div>
      )}
    </div>
  )
}
