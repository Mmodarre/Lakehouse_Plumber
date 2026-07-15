import { useState } from 'react'
import { FilePlus2, FolderOpen, Loader2, SquarePen } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import type { CodeTarget } from './CodeModal'
import { FilePicker } from './FilePicker'
import { companionCheckablePath, useCompanionFile } from './useCompanionFile'

// ── FileRefField — browse ⊕ create ⊕ edit for a file-ref field ─
//
// The control the ActionModalEditor shell renders when `fileRefForField(spec)`
// is non-null. It COMPOSES the existing pieces rather than re-implementing
// them: a free-text input (so a `${...}` token or a not-yet-created path is
// still typeable), the `FilePicker` browse dialog (Task 1.2), and the
// `useCompanionFile` existence/create-stub flow (shared with
// `ActionFormCodeField`'s CompanionFileField). Existence-driven affordances
// mirror that template — "Edit file" when present, "Create file" when missing,
// a "Checking…" spinner in flight — and are skipped entirely for token /
// absolute paths (`companionCheckablePath` returns null → status
// 'unavailable'), leaving just the input plus Browse/New.

export interface FileRefFieldProps {
  /** Current field value — a project-relative path, a `${...}` token, or absent. */
  value: unknown
  /** Set the field value (typing OR a Browse pick both call this). */
  onChange: (next: string) => void
  /** Extensions to accept (each including the dot), for FilePicker + the stub. */
  accept: string[]
  /** Restrict the FilePicker tree to this project-relative directory. */
  baseDir?: string
  /** Open the shell's existing CodeModal on a file target. */
  onEditCode: (target: CodeTarget) => void
}

/** Stub content seeded from the ref's extension. FileRefField backs
 * schema/columns files, so `.yaml`/`.yml` gets `# columns:\n` (diverging from
 * the SQL-expectations-oriented `stubFor` in ActionFormCodeField). */
function stubForExt(ext: string): string {
  switch (ext) {
    case 'sql':
      return '-- SQL\n'
    case 'py':
      return '# Python\n'
    case 'json':
      return '{}\n'
    case 'yaml':
    case 'yml':
      return '# columns:\n'
    default:
      return '\n'
  }
}

/** The extension to seed the stub from: the value's own extension when it has
 * one, else the first accept entry (with any leading dot dropped). */
function stubExtension(value: unknown, accept: string[]): string {
  if (typeof value === 'string') {
    const dot = value.lastIndexOf('.')
    if (dot !== -1 && dot < value.length - 1) return value.slice(dot + 1).toLowerCase()
  }
  const first = accept[0] ?? ''
  return first.replace(/^\./, '').toLowerCase()
}

export function FileRefField({ value, onChange, accept, baseDir, onEditCode }: FileRefFieldProps) {
  const [browsing, setBrowsing] = useState(false)
  const path = companionCheckablePath(value)
  const companion = useCompanionFile(path)

  const openEditor = () => {
    if (path === null) return
    onEditCode({ backing: 'file', title: path, filePath: path })
  }

  const handleCreate = async () => {
    const created = await companion.create(stubForExt(stubExtension(value, accept)))
    if (created) openEditor()
  }

  return (
    <div className="space-y-1.5">
      <div className="flex items-center gap-1.5">
        <Input
          value={typeof value === 'string' ? value : ''}
          onChange={(e) => onChange(e.target.value)}
          placeholder={accept.join(', ')}
          spellCheck={false}
          autoComplete="off"
          className="font-mono text-xs"
        />
        <Button type="button" variant="outline" size="sm" onClick={() => setBrowsing(true)}>
          <FolderOpen aria-hidden="true" />
          Browse
        </Button>
        <Button type="button" variant="outline" size="sm" onClick={() => void handleCreate()}>
          <FilePlus2 aria-hidden="true" />
          New
        </Button>
      </div>

      {path !== null && companion.status === 'exists' && (
        <div className="flex justify-end">
          <Button type="button" variant="ghost" size="xs" onClick={openEditor}>
            <SquarePen aria-hidden="true" />
            Edit file
          </Button>
        </div>
      )}
      {path !== null && companion.status === 'missing' && (
        <div className="flex items-center justify-between gap-2 rounded-sm border border-dashed border-border px-2 py-1.5">
          <span className="text-2xs text-muted-foreground">This file doesn&apos;t exist yet.</span>
          <Button type="button" variant="outline" size="xs" onClick={() => void handleCreate()}>
            <FilePlus2 aria-hidden="true" />
            Create file
          </Button>
        </div>
      )}
      {path !== null && companion.status === 'checking' && (
        <div className="flex items-center gap-1 text-2xs text-muted-foreground">
          <Loader2 className="size-3 animate-spin" aria-hidden="true" />
          Checking…
        </div>
      )}

      {browsing && (
        <FilePicker
          accept={accept}
          baseDir={baseDir}
          onPick={(picked) => {
            onChange(picked)
            setBrowsing(false)
          }}
          onClose={() => setBrowsing(false)}
        />
      )}
    </div>
  )
}
