import { useEffect, useMemo, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { Plus, Search, TriangleAlert, X } from 'lucide-react'
import { useSandbox } from '../../hooks/useSandbox'
import { usePipelines } from '../../hooks/usePipelines'
import { useUIStore } from '../../store/uiStore'
import { fetchFileContentWithMeta, writeFile } from '../../api/files'
import { errorMessage } from '../../lib/errors'
import type { SandboxScope } from '../../types/api'
import { assembleProfileYaml, normalizePipelines } from './profileYaml'
import { Button } from '../ui/button'
import { Input } from '../ui/input'
import { Checkbox } from '../ui/checkbox'
import { FieldLabel } from '../config/fields/FieldLabel'
import { ScrollArea } from '../ui/scroll-area'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '../ui/dialog'
import { cn } from '../../lib/utils'

const PROFILE_PATH = '.lhp/profile.yaml'

// namespace is lowercased, letter-first, ≤64 chars (SandboxProfile in
// src/lhp/models/_sandbox.py). Validated here so Save is blocked before a PUT
// the loader would reject.
const NAMESPACE_RE = /^[a-z][a-z0-9_]{0,63}$/

interface SandboxPickerDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

/**
 * Edit the personal sandbox scope and write `.lhp/profile.yaml`.
 *
 * Pipelines can be picked from the checklist and/or entered as glob patterns
 * (`acmi_edw_silv*`); both merge into the profile's `pipelines` list. Saving
 * assembles the YAML (comment-preserving when editing an existing file), PUTs
 * it, refreshes the scope, and turns sandbox mode on.
 */
export function SandboxPickerDialog({ open, onOpenChange }: SandboxPickerDialogProps) {
  const { data: scope } = useSandbox()
  const { data: pipelinesData } = usePipelines()
  const setSandboxEnabled = useUIStore((s) => s.setSandboxEnabled)
  const queryClient = useQueryClient()

  const [namespace, setNamespace] = useState('')
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [patterns, setPatterns] = useState<string[]>([])
  const [search, setSearch] = useState('')
  const [newPattern, setNewPattern] = useState('')
  const [saving, setSaving] = useState(false)

  // Seed once per open, from the current profile view — re-seeding on every
  // data tick would clobber the user's edits mid-session.
  const seededRef = useRef(false)
  useEffect(() => {
    if (!open) {
      seededRef.current = false
      return
    }
    if (seededRef.current || !pipelinesData) return
    const known = new Set(pipelinesData.pipelines.map((p) => p.name))
    const seedSelected = new Set<string>()
    const seedPatterns: string[] = []
    for (const pat of scope?.patterns ?? []) {
      if (known.has(pat)) seedSelected.add(pat)
      else seedPatterns.push(pat)
    }
    setNamespace(scope?.namespace ?? '')
    setSelected(seedSelected)
    setPatterns(seedPatterns)
    setSearch('')
    setNewPattern('')
    seededRef.current = true
  }, [open, scope, pipelinesData])

  const pipelines = useMemo(() => pipelinesData?.pipelines ?? [], [pipelinesData])
  const shown = useMemo(() => {
    const q = search.trim().toLowerCase()
    return q ? pipelines.filter((p) => p.name.toLowerCase().includes(q)) : pipelines
  }, [pipelines, search])

  const resolvedPipelines = useMemo(
    () => normalizePipelines([...selected, ...patterns]),
    [selected, patterns],
  )
  const namespaceValid = NAMESPACE_RE.test(namespace)
  const namespaceTouched = namespace !== ''
  const canSave = namespaceValid && resolvedPipelines.length > 0 && !saving

  const togglePipeline = (name: string, checked: boolean) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (checked) next.add(name)
      else next.delete(name)
      return next
    })
  }

  const addPattern = () => {
    const entry = newPattern.trim()
    if (entry === '') return
    setPatterns((prev) => (prev.includes(entry) ? prev : [...prev, entry]))
    setNewPattern('')
  }

  const removePattern = (entry: string) => {
    setPatterns((prev) => prev.filter((p) => p !== entry))
  }

  const save = async () => {
    if (!canSave) return
    setSaving(true)
    try {
      // Preserve comments/other keys when a profile already exists by editing
      // its source; otherwise write a fresh file.
      let existingSource: string | null = null
      let etag: string | null = null
      if (scope?.profile_exists) {
        try {
          const meta = await fetchFileContentWithMeta(PROFILE_PATH)
          existingSource = meta.content
          etag = meta.etag
        } catch {
          // The view said it exists but we can't read it — fall back to a
          // fresh write rather than blocking the save.
        }
      }
      const yaml = assembleProfileYaml(
        { namespace, pipelines: resolvedPipelines },
        existingSource,
      )
      await writeFile(PROFILE_PATH, yaml, etag)
      // Reflect the just-saved scope in the cache immediately. The ['sandbox']
      // refetch below runs a cold, slow flowgroup discovery; until it lands,
      // react-query keeps serving the pre-save scope, so the header pill and a
      // reopened picker would otherwise show the OLD (or empty) scope.
      queryClient.setQueryData<SandboxScope>(['sandbox'], (prev) => ({
        ...prev,
        profile_exists: true,
        namespace,
        patterns: resolvedPipelines,
        resolved_pipelines: resolvedPipelines,
        error: null,
      }))
      queryClient.invalidateQueries({ queryKey: ['sandbox'] })
      queryClient.invalidateQueries({ queryKey: ['files'] })
      setSandboxEnabled(true)
      toast.success('Sandbox scope saved')
      onOpenChange(false)
    } catch (err) {
      toast.error(errorMessage(err, 'Failed to save sandbox scope'))
    } finally {
      setSaving(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>Sandbox scope</DialogTitle>
          <DialogDescription>
            Generate only your pipelines, namespaced to you. Saved to{' '}
            <span className="font-mono text-2xs">.lhp/profile.yaml</span> (gitignored).
          </DialogDescription>
        </DialogHeader>

        {scope?.error && (
          <div className="flex items-start gap-2 rounded-md border border-warning/25 bg-warning/12 px-3 py-2 text-xs text-foreground">
            <TriangleAlert className="mt-0.5 size-3.5 shrink-0 text-warning" aria-hidden="true" />
            <span>{scope.error}</span>
          </div>
        )}

        <div className="space-y-4">
          {/* Namespace */}
          <div className="space-y-1.5">
            <FieldLabel
              htmlFor="sandbox-namespace"
              label="Namespace"
              help="Prefixes your produced tables ({namespace}_{table}) so parallel developers don't collide on the same tables."
            />
            <Input
              id="sandbox-namespace"
              value={namespace}
              onChange={(e) => setNamespace(e.target.value)}
              placeholder="e.g. alice"
              autoComplete="off"
              spellCheck={false}
              aria-invalid={namespaceTouched && !namespaceValid}
              className="font-mono text-sm"
            />
            <p
              className={cn(
                'text-2xs',
                namespaceTouched && !namespaceValid ? 'text-error' : 'text-muted-foreground',
              )}
            >
              Lowercase letter first, then letters, digits, or underscores (max 64).
            </p>
          </div>

          {/* Pipelines */}
          <div className="space-y-1.5">
            <div className="flex items-center justify-between">
              <FieldLabel
                label="Pipelines"
                help="Only these pipelines are generated while sandbox mode is on."
              />
              <span className="text-2xs tabular-nums text-muted-foreground">
                {resolvedPipelines.length} selected
              </span>
            </div>
            <div className="relative">
              <Search
                className="pointer-events-none absolute inset-y-0 left-2.5 my-auto size-3.5 text-muted-foreground"
                aria-hidden="true"
              />
              <Input
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search pipelines…"
                className="h-8 pl-8 text-sm"
              />
            </div>
            <ScrollArea className="h-44 rounded-md border border-border">
              <div className="space-y-0.5 p-1.5">
                {shown.length === 0 ? (
                  <p className="px-1.5 py-3 text-center text-xs text-muted-foreground">
                    {pipelines.length === 0 ? 'No pipelines to select yet.' : 'No matches.'}
                  </p>
                ) : (
                  shown.map((p) => (
                    <label
                      key={p.name}
                      className="flex cursor-pointer items-center gap-2 rounded-sm px-1.5 py-1 text-sm hover:bg-muted/60"
                    >
                      <Checkbox
                        checked={selected.has(p.name)}
                        onCheckedChange={(c) => togglePipeline(p.name, c === true)}
                      />
                      <span className="truncate">{p.name}</span>
                    </label>
                  ))
                )}
              </div>
            </ScrollArea>
          </div>

          {/* Glob patterns */}
          <div className="space-y-1.5">
            <FieldLabel
              htmlFor="sandbox-pattern"
              label="Patterns (optional)"
              help="Glob patterns matched against pipeline names (e.g. acmi_edw_silv*)."
            />
            <div className="flex items-center gap-1.5">
              <Input
                id="sandbox-pattern"
                type="text"
                value={newPattern}
                onChange={(e) => setNewPattern(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') {
                    e.preventDefault()
                    addPattern()
                  }
                }}
                placeholder="e.g. acmi_edw_silv*"
                className="h-8 font-mono text-sm"
              />
              <Button
                type="button"
                variant="outline"
                size="icon-sm"
                onClick={addPattern}
                disabled={newPattern.trim() === ''}
                aria-label="Add pattern"
              >
                <Plus aria-hidden="true" />
              </Button>
            </div>
            {patterns.length > 0 && (
              <div className="flex flex-wrap gap-1.5 pt-0.5">
                {patterns.map((p) => (
                  <span
                    key={p}
                    className="inline-flex items-center gap-1 rounded-sm border border-border bg-muted/50 px-1.5 py-0.5 font-mono text-2xs text-muted-foreground"
                  >
                    {p}
                    <button
                      type="button"
                      onClick={() => removePattern(p)}
                      aria-label={`Remove pattern ${p}`}
                      className="rounded-xs text-muted-foreground transition-colors hover:text-foreground"
                    >
                      <X className="size-3" aria-hidden="true" />
                    </button>
                  </span>
                ))}
              </div>
            )}
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={saving}>
            Cancel
          </Button>
          <Button onClick={() => void save()} disabled={!canSave}>
            Save scope
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
