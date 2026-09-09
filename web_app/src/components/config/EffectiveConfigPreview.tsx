import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Button } from '@/components/ui/button'
import { fetchConfigurationPreview } from '@/api/configuration'
import { useUIStore } from '@/store/uiStore'
import { useWorkspaceStore } from '@/store/workspaceStore'
import { errorMessage } from '@/lib/errors'

/** Requests authoritative saved values on demand. Drafts never masquerade as resolved values. */
export function EffectiveConfigPreview({ path, kind }: { path: string; kind: 'pipeline' | 'job' }) {
  const [open, setOpen] = useState(false)
  const [targetDraft, setTargetDraft] = useState('')
  const [target, setTarget] = useState('')
  const env = useUIStore((state) => state.selectedEnv)
  const buffer = useWorkspaceStore((state) => state.buffers.find((item) => item.path === path))
  const preview = useQuery({
    queryKey: ['configuration-preview', path, kind, env, target, buffer?.etag],
    queryFn: () => fetchConfigurationPreview(path, kind, env, target),
    enabled: open && !!env,
    retry: false,
  })
  return (
    <div className="space-y-2 rounded-md border border-border p-3">
      <Button
        type="button"
        variant="ghost"
        size="sm"
        aria-expanded={open}
        onClick={() => setOpen(!open)}
      >
        {open ? 'Hide' : 'Preview'} effective saved settings
      </Button>
      {open && (
        <div className="space-y-3 text-xs">
          <p className="text-muted-foreground">
            Saved configuration for{' '}
            <span className="font-mono">{env || 'no environment selected'}</span>.{' '}
            {buffer?.isDirty
              ? 'Save your edits to include them in this preview.'
              : 'Values come from LHP’s configuration resolver.'}
          </p>
          <form
            className="flex flex-wrap items-end gap-2"
            onSubmit={(event) => {
              event.preventDefault()
              if (targetDraft.trim() === target) void preview.refetch()
              else setTarget(targetDraft.trim())
            }}
          >
            <label className="flex min-w-0 flex-1 flex-col gap-1">
              {kind === 'pipeline' ? 'Pipeline target' : 'Job target'}
              <input
                aria-label={kind === 'pipeline' ? 'Pipeline target' : 'Job target'}
                value={targetDraft}
                list={`preview-targets-${kind}`}
                onChange={(event) => setTargetDraft(event.target.value)}
                placeholder={preview.data?.target || 'Enter a target name'}
                className="rounded-sm border border-border bg-background px-2 py-1.5 font-mono"
              />
              <datalist id={`preview-targets-${kind}`}>
                {preview.data?.targets.map((name) => (
                  <option key={name} value={name} />
                ))}
              </datalist>
            </label>
            <Button type="submit" variant="outline" size="sm" disabled={preview.isFetching || !env}>
              Refresh preview
            </Button>
          </form>
          {preview.isFetching ? (
            <p role="status">Resolving saved settings…</p>
          ) : preview.error ? (
            <p role="alert" className="text-destructive">
              {errorMessage(preview.error, 'Could not resolve saved settings')}
            </p>
          ) : (
            preview.data && (
              <>
                <p className="text-muted-foreground">
                  {preview.data.target
                    ? `Target: ${preview.data.target}. `
                    : 'No explicit target; file defaults are shown. '}
                  Layers: {preview.data.tiers.join(' → ')}.
                </p>
                <pre
                  aria-label="Effective saved settings"
                  className="max-h-80 overflow-auto rounded-sm bg-muted p-3 font-mono text-xs"
                >
                  {JSON.stringify(preview.data.values, null, 2)}
                </pre>
                <ul className="list-disc space-y-1 pl-4 text-2xs text-muted-foreground">
                  {preview.data.warnings.map((warning) => (
                    <li key={warning}>{warning}</li>
                  ))}
                </ul>
              </>
            )
          )}
        </div>
      )}
    </div>
  )
}
