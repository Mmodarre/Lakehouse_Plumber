import { useMemo, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { FilePlus2, Loader2 } from 'lucide-react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { fetchConfigTemplate } from '../../api/config-templates'
import { IF_MATCH_CREATE_ONLY, writeFile } from '../../api/files'
import { ApiError } from '../../api/client'
import { errorMessage } from '../../lib/errors'
import { useFileList } from '../../hooks/useFiles'
import { useUIStore } from '../../store/uiStore'
import {
  defaultTemplatePath,
  listAllFilePaths,
  TEMPLATE_KIND_LABELS,
  TEMPLATE_KINDS_BY_TAB,
  validateNewConfigPath,
} from './configFileSupport'
import type { ConfigTabKind, ConfigTemplateKind } from './configFileSupport'

// ── CreateFromTemplateDialog — scaffold a new config file ────
//
// Fetches the packaged template text (GET /api/config-templates/{kind})
// and writes it to a new file under config/. Two existence guards:
//   • client-side, the path is validated against the files tree
//     (inline error before any request);
//   • on the wire, the PUT carries the never-matching
//     IF_MATCH_CREATE_ONLY etag, so a file that appeared since the tree
//     was fetched atomically 412s instead of being overwritten.
// On success: files tree invalidated, `onCreated(path)` selects the new
// file, dialog closes.

export interface CreateFromTemplateDialogProps {
  /** Tab the dialog was opened from — decides the offered template kinds. */
  kind: ConfigTabKind
  open: boolean
  onOpenChange: (open: boolean) => void
  /** Called with the created path (the page selects it in the picker). */
  onCreated: (path: string) => void
}

export function CreateFromTemplateDialog({
  kind,
  open,
  onOpenChange,
  onCreated,
}: CreateFromTemplateDialogProps) {
  // Render nothing while closed so the form state below resets on each
  // open via the useState initializers (CreateFlowgroupDialog pattern).
  if (!open) return null
  return (
    <Dialog
      open
      onOpenChange={(o) => {
        if (!o) onOpenChange(false)
      }}
    >
      <DialogContent className="sm:max-w-md">
        <CreateFromTemplateForm kind={kind} onOpenChange={onOpenChange} onCreated={onCreated} />
      </DialogContent>
    </Dialog>
  )
}

function CreateFromTemplateForm({
  kind,
  onOpenChange,
  onCreated,
}: Omit<CreateFromTemplateDialogProps, 'open'>) {
  const queryClient = useQueryClient()
  const { data: tree } = useFileList()
  const selectedEnv = useUIStore((s) => s.selectedEnv)

  const kinds = TEMPLATE_KINDS_BY_TAB[kind]
  const [templateKind, setTemplateKind] = useState<ConfigTemplateKind>(kinds[0])
  const [path, setPath] = useState(() => defaultTemplatePath(kinds[0], selectedEnv))
  /** The user edited the filename — stop tracking the kind default. */
  const [pathTouched, setPathTouched] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [submitError, setSubmitError] = useState<string | null>(null)

  const existingPaths = useMemo(() => new Set(listAllFilePaths(tree)), [tree])
  const validationError = validateNewConfigPath(path, existingPaths)

  const changeKind = (next: ConfigTemplateKind) => {
    setTemplateKind(next)
    if (!pathTouched) setPath(defaultTemplatePath(next, selectedEnv))
  }

  const submit = async () => {
    if (validationError !== null || submitting) return
    const targetPath = path.trim()
    setSubmitting(true)
    setSubmitError(null)
    try {
      const template = await fetchConfigTemplate(templateKind)
      await writeFile(targetPath, template, IF_MATCH_CREATE_ONLY)
      await queryClient.invalidateQueries({ queryKey: ['files'] })
      toast.success(`Created ${targetPath}`)
      onCreated(targetPath)
      onOpenChange(false)
    } catch (err) {
      if (err instanceof ApiError && err.status === 412) {
        setSubmitError('This file already exists on disk.')
      } else {
        setSubmitError(errorMessage(err, 'Failed to create the file'))
      }
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <form
      onSubmit={(e) => {
        e.preventDefault()
        void submit()
      }}
    >
      <DialogHeader>
        <DialogTitle className="text-sm">New config file from template</DialogTitle>
        <DialogDescription className="text-xs">
          Starts from the packaged template — every setting inside is a commented example you can
          enable in the form.
        </DialogDescription>
      </DialogHeader>

      <div className="space-y-3 py-4">
        {kinds.length > 1 && (
          <div className="space-y-1.5">
            <Label htmlFor="config-template-kind" className="text-xs">
              Template
            </Label>
            <Select value={templateKind} onValueChange={(v) => changeKind(v as ConfigTemplateKind)}>
              <SelectTrigger id="config-template-kind" size="sm" className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {kinds.map((k) => (
                  <SelectItem key={k} value={k}>
                    {TEMPLATE_KIND_LABELS[k]}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        )}

        <div className="space-y-1.5">
          <Label htmlFor="config-template-path" className="text-xs">
            File name
          </Label>
          <Input
            id="config-template-path"
            value={path}
            onChange={(e) => {
              setPath(e.target.value)
              setPathTouched(true)
              setSubmitError(null)
            }}
            spellCheck={false}
            autoComplete="off"
            className="font-mono text-xs"
            aria-invalid={validationError !== null}
            aria-describedby="config-template-path-error"
          />
          <p
            id="config-template-path-error"
            role="alert"
            className="min-h-4 text-2xs text-destructive"
          >
            {submitError ?? validationError ?? ''}
          </p>
        </div>
      </div>

      <DialogFooter>
        <Button type="button" variant="ghost" size="sm" onClick={() => onOpenChange(false)}>
          Cancel
        </Button>
        <Button type="submit" size="sm" disabled={validationError !== null || submitting}>
          {submitting ? (
            <Loader2 className="animate-spin" aria-hidden="true" />
          ) : (
            <FilePlus2 aria-hidden="true" />
          )}
          Create
        </Button>
      </DialogFooter>
    </form>
  )
}
