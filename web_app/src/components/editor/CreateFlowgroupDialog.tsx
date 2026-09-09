import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { FilePlus2, FileText, LayoutTemplate, Loader2, Package, X } from 'lucide-react'
import { toast } from 'sonner'
import { cn } from '../../lib/utils'
import { errorMessage } from '../../lib/errors'
import { useUIStore } from '../../store/uiStore'
import { useLayoutStore } from '../../store/layoutStore'
import { useWorkspaceStore } from '../../store/workspaceStore'
import { usePipelines } from '../../hooks/usePipelines'
import { useFlowgroups } from '../../hooks/useFlowgroups'
import { useFileList } from '../../hooks/useFiles'
import { useTemplateCatalog, useTemplateSource } from '../../hooks/useTemplateAuthoring'
import type { TemplateCatalogEntry } from '../../api/template-authoring'
import { TemplateInvocationParams } from '../template/TemplateInvocationParams'
import { missingTemplateParameters } from '../../lib/template-invocation'
import { openWorkspaceFile } from '../../workspace/openWorkspaceFile'
import { captureWorkspaceEditors, focusInvalidWorkspaceDraft } from '../../workspace/editorCommands'
import { useBlueprints } from '../../hooks/useBlueprints'
import { ApiError } from '../../api/client'
import { IF_MATCH_CREATE_ONLY, writeFile } from '../../api/files'
import { Button } from '../ui/button'
import { Dialog, DialogContent, DialogTitle } from '../ui/dialog'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../ui/select'
import { KeyValueMapEditor } from '../config/fields/KeyValueMapEditor'
import { ParamsForm } from './ParamsForm'
import { FlowgroupTargetPicker, PathPreview, type FlowgroupTarget } from './FlowgroupTargetPicker'
import { BlueprintFields } from './BlueprintFields'
import { fetchBlueprintParams } from './blueprintParams'
import { requiredParamsMissing } from './createFlowgroupSupport'
import {
  blueprintInstancePath,
  buildBlankFlowgroupYaml,
  buildBlueprintInstanceYaml,
  buildTemplateFlowgroupYaml,
  validateName,
} from './newFlowgroupDoc'

// ── CreateFlowgroupDialog — the flowgroup creation surface ───
//
// Creates a flowgroup from blank | template | blueprint, into an existing or
// new pipeline, by writing the YAML through the files API (PUT auto-creates
// parent dirs — a new pipeline folder falls out for free) and opening the
// designer on the result. All client-side: no create endpoint. The atomic
// backstop is IF_MATCH_CREATE_ONLY (a never-matching etag → 412 if the file
// already exists), which the dialog turns into an explicit overwrite confirm.

type Mode = 'blank' | 'template' | 'blueprint'

const EMPTY_TARGET: FlowgroupTarget = { pipeline: '', name: '', path: '', valid: false }

/** Outer shell: owns open/close + data, and remounts the form on each open so
 * its field state resets via the useState initializers (no reset effect). */
export function CreateFlowgroupDialog() {
  const open = useUIStore((s) => s.createFlowgroupDialog)
  const seed = useUIStore((s) => s.createFlowgroupSeed)
  const close = useUIStore((s) => s.closeCreateFlowgroupDialog)
  const submitting = useRef(false)
  const onSubmittingChange = useCallback((value: boolean) => { submitting.current = value }, [])
  const requestClose = useCallback(() => { if (!submitting.current) close() }, [close])
  const complete = useCallback(() => { submitting.current = false; close() }, [close])

  const { data: pipelineData } = usePipelines()
  const { data: flowgroupData } = useFlowgroups()
  const { data: fileTree, isLoading: loadingDirs } = useFileList()
  const templateCatalog = useTemplateCatalog()
  const { data: blueprintData } = useBlueprints()

  if (!open) return null

  const pipelines = pipelineData?.pipelines.map((p) => p.name) ?? []
  // Collision names grouped by pipeline: LHP keys flowgroup uniqueness on
  // (pipeline, flowgroup), so the pre-check must be pipeline-scoped.
  const existingNamesByPipeline = new Map<string, Set<string>>()
  for (const f of flowgroupData?.flowgroups ?? []) {
    const set = existingNamesByPipeline.get(f.pipeline) ?? new Set<string>()
    set.add(f.name.toLowerCase())
    existingNamesByPipeline.set(f.pipeline, set)
  }
  const templates = templateCatalog.data?.templates ?? []
  const blueprints = blueprintData?.blueprints.map((b) => b.name) ?? []

  return (
    <Dialog
      open
      onOpenChange={(o) => {
        if (!o) requestClose()
      }}
    >
      <DialogContent
        showCloseButton={false}
        aria-describedby={undefined}
        className="flex max-h-[85vh] flex-col gap-0 overflow-hidden p-0 sm:max-w-lg"
      >
        <CreateFlowgroupForm
          pipelines={pipelines}
          existingNamesByPipeline={existingNamesByPipeline}
          templates={templates}
          blueprints={blueprints}
          fileTree={fileTree}
          loadingDirs={loadingDirs}
          seedPipeline={seed?.pipeline}
          seedTemplatePath={seed?.templatePath}
          templateCatalogError={templateCatalog.isError}
          retryCatalog={() => { void templateCatalog.refetch() }}
          close={requestClose}
          complete={complete}
          onSubmittingChange={onSubmittingChange}
        />
      </DialogContent>
    </Dialog>
  )
}

interface CreateFlowgroupFormProps {
  pipelines: string[]
  existingNamesByPipeline: Map<string, Set<string>>
  templates: TemplateCatalogEntry[]
  blueprints: string[]
  fileTree: ReturnType<typeof useFileList>['data']
  loadingDirs: boolean
  seedPipeline?: string
  seedTemplatePath?: string
  templateCatalogError: boolean
  retryCatalog: () => void
  close: () => void
  complete: () => void
  onSubmittingChange: (value: boolean) => void
}

const MODES: { mode: Mode; icon: typeof FileText; label: string; desc: string }[] = [
  { mode: 'blank', icon: FileText, label: 'Blank', desc: 'Empty flowgroup, compose on the canvas.' },
  { mode: 'template', icon: LayoutTemplate, label: 'Template', desc: 'Instantiate a reusable template.' },
  { mode: 'blueprint', icon: Package, label: 'Blueprint', desc: 'Expand a multi-flowgroup blueprint.' },
]

function CreateFlowgroupForm({
  pipelines,
  existingNamesByPipeline,
  templates,
  blueprints,
  fileTree,
  loadingDirs,
  seedPipeline,
  seedTemplatePath,
  templateCatalogError,
  retryCatalog,
  close,
  complete,
  onSubmittingChange,
}: CreateFlowgroupFormProps) {
  const queryClient = useQueryClient()
  const viewerMode = useLayoutStore((s) => s.viewerMode)
  const openEntityTab = useWorkspaceStore((s) => s.openEntityTab)

  const [mode, setMode] = useState<Mode>(seedTemplatePath ? 'template' : 'blank')
  const [target, setTarget] = useState<FlowgroupTarget>(EMPTY_TARGET)
  const handleTargetChange = useCallback((t: FlowgroupTarget) => setTarget(t), [])

  // Template mode.
  const [template, setTemplate] = useState(seedTemplatePath ?? '')
  const [templateValues, setTemplateValues] = useState<Record<string, unknown>>({})
  const [templateValuesValid, setTemplateValuesValid] = useState(true)
  const templateQuery = useTemplateSource(mode === 'template' && template ? template : null)
  const templateInfo = templateQuery.data?.template
  const templateParams = useMemo(() => templateInfo?.parameters ?? [], [templateInfo])
  useEffect(() => setTemplateValues({}), [template])

  // Blueprint mode.
  const [blueprint, setBlueprint] = useState('')
  const [instanceName, setInstanceName] = useState('')
  const [blueprintValues, setBlueprintValues] = useState<Record<string, unknown>>({})
  useEffect(() => setBlueprintValues({}), [blueprint])
  const blueprintParamsQuery = useQuery({
    queryKey: ['blueprint-params', blueprint],
    queryFn: () => fetchBlueprintParams(blueprint),
    enabled: mode === 'blueprint' && blueprint !== '',
    retry: false,
  })
  const blueprintParams = blueprintParamsQuery.data ?? []

  // Submit / overwrite state.
  const [submitting, setSubmitting] = useState(false)
  const pending = useRef(false)
  const setSubmission = (value: boolean) => {
    pending.current = value
    onSubmittingChange(value)
    setSubmitting(value)
  }
  const disabled = submitting || viewerMode
  const [submitError, setSubmitError] = useState<string | null>(null)
  const [overwrite, setOverwrite] = useState<{
    path: string
    yaml: string
    pipeline: string
    flowgroup: string
  } | null>(null)

  // A captured overwrite (from a 412) targets a SPECIFIC path/content. Any edit
  // to mode/target/template/params/blueprint/instance makes that capture stale,
  // so drop it — the footer falls back to Create against the edited request.
  // (setState with the same value is a no-op, so this is inert when idle.)
  useEffect(() => {
    setOverwrite(null)
    setSubmitError(null)
  }, [mode, target, template, templateValues, blueprint, instanceName, blueprintValues])

  const setTemplateValue = useCallback(
    (name: string, value: unknown) => setTemplateValues((p) => ({ ...p, [name]: value })),
    [],
  )
  const unsetTemplateValue = useCallback(
    (name: string) =>
      setTemplateValues((p) => {
        const next = { ...p }
        delete next[name]
        return next
      }),
    [],
  )
  const setBlueprintValue = useCallback(
    (name: string, value: unknown) => setBlueprintValues((p) => ({ ...p, [name]: value })),
    [],
  )
  const unsetBlueprintValue = useCallback(
    (name: string) =>
      setBlueprintValues((p) => {
        const next = { ...p }
        delete next[name]
        return next
      }),
    [],
  )

  const instanceNameError = instanceName ? validateName(instanceName) : null

  const canCreate = (() => {
    if (submitting || viewerMode) return false
    if (mode === 'blank') return target.valid
    if (mode === 'template') {
      return (
        target.valid && template !== '' && !templateQuery.isPending && !templateQuery.isFetching && !templateQuery.isError && templateInfo?.state === 'ready' && !!templateInfo.reference && templateValuesValid && missingTemplateParameters(templateParams, templateValues).length === 0
      )
    }
    return (
      blueprint !== '' &&
      instanceName !== '' &&
      instanceNameError === null &&
      !requiredParamsMissing(blueprintParams, blueprintValues)
    )
  })()

  // Query notifications are scheduled: a refetch can start or finish between
  // the last render and a click. Never submit against that superseded snapshot.
  const templateMetadataCurrent = () => {
    if (mode !== 'template') return true
    const current = queryClient.getQueryState<{ template: TemplateCatalogEntry }>(['template', template])
    return current?.status === 'success' && current.fetchStatus === 'idle'
      && current.data?.template === templateInfo
  }

  /** Build (path, yaml, pipeline, flowgroup) for the current mode. */
  const buildRequest = (): { path: string; yaml: string; pipeline: string; flowgroup: string } => {
    if (mode === 'blank') {
      return {
        path: target.path,
        yaml: buildBlankFlowgroupYaml(target.pipeline, target.name),
        pipeline: target.pipeline,
        flowgroup: target.name,
      }
    }
    if (mode === 'template') {
      return {
        path: target.path,
        yaml: buildTemplateFlowgroupYaml(target.pipeline, target.name, templateInfo?.reference ?? '', templateValues),
        pipeline: target.pipeline,
        flowgroup: target.name,
      }
    }
    return {
      path: blueprintInstancePath(instanceName),
      yaml: buildBlueprintInstanceYaml(blueprint, blueprintValues),
      pipeline: '',
      flowgroup: instanceName,
    }
  }

  const persist = async (
    req: { path: string; yaml: string; pipeline: string; flowgroup: string },
    createOnly: boolean,
  ) => {
    if (useLayoutStore.getState().viewerMode) {
      throw new Error('Viewer mode prevents creating or overwriting files')
    }
    if (!templateMetadataCurrent()) {
      throw new Error('Wait for the current template parameters before creating the flowgroup')
    }
    await writeFile(req.path, req.yaml, createOnly ? IF_MATCH_CREATE_ONLY : undefined)
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ['files'] }),
      queryClient.invalidateQueries({ queryKey: ['pipelines'] }),
      queryClient.invalidateQueries({ queryKey: ['flowgroups'] }),
    ])
    toast.success(`Created ${req.path}`)
    openEntityTab(req.pipeline, req.flowgroup, req.path)
    complete()
  }

  const submit = async () => {
    if (useLayoutStore.getState().viewerMode || pending.current || !canCreate || !templateMetadataCurrent()) return
    captureWorkspaceEditors()
    if (focusInvalidWorkspaceDraft()) return
    if (useLayoutStore.getState().viewerMode || pending.current || !canCreate || !templateMetadataCurrent()) return
    const req = buildRequest()
    setSubmission(true)
    setSubmitError(null)
    try {
      await persist(req, true)
    } catch (err) {
      if (err instanceof ApiError && err.status === 412) {
        setOverwrite(req)
        setSubmitError(`A file already exists at ${req.path}.`)
      } else {
        setSubmitError(errorMessage(err, 'Failed to create the flowgroup'))
      }
    } finally {
      setSubmission(false)
    }
  }

  const confirmOverwrite = async () => {
    if (useLayoutStore.getState().viewerMode || pending.current || overwrite === null || !canCreate || !templateMetadataCurrent()) return
    setSubmission(true)
    setSubmitError(null)
    try {
      await persist(overwrite, false)
      setOverwrite(null)
    } catch (err) {
      setSubmitError(errorMessage(err, 'Failed to overwrite the file'))
    } finally {
      setSubmission(false)
    }
  }

  const changeMode = (next: Mode) => {
    setMode(next)
    setSubmitError(null)
    setOverwrite(null)
  }

  return (
    <>
      <div className="flex items-center justify-between border-b border-border px-5 py-3">
        <div>
          <span className="text-2xs font-semibold uppercase tracking-[0.05em] text-muted-foreground">
            New
          </span>
          <DialogTitle className="text-sm font-semibold text-foreground">
            Create flowgroup
          </DialogTitle>
        </div>
        <Button
          variant="ghost"
          size="icon-sm"
          className="text-muted-foreground"
          aria-label="Close"
          onClick={close}
          disabled={submitting}
        >
          <X aria-hidden="true" />
        </Button>
      </div>

      {viewerMode && <p role="status" className="px-5 pt-3 text-xs text-muted-foreground">Viewer mode: creation and overwrite are disabled.</p>}
      <fieldset disabled={disabled} className="min-h-0 flex-1 space-y-4 overflow-y-auto px-5 py-4">
        {/* Start-from cards */}
        <div>
          <span className="mb-1.5 block text-xs font-medium text-muted-foreground">Start from</span>
          <div className="flex gap-2">
            {MODES.map((m) => (
              <button
                key={m.mode}
                type="button"
                onClick={() => changeMode(m.mode)}
                aria-pressed={mode === m.mode}
                className={cn(
                  'flex flex-1 flex-col gap-1 rounded-md border px-3 py-2.5 text-left transition-colors',
                  mode === m.mode
                    ? 'border-primary bg-primary/5'
                    : 'border-border hover:bg-muted/60',
                )}
              >
                <m.icon
                  className={cn(
                    'size-4',
                    mode === m.mode ? 'text-primary' : 'text-muted-foreground',
                  )}
                  aria-hidden="true"
                />
                <span className="text-xs font-medium text-foreground">{m.label}</span>
                <span className="text-2xs leading-snug text-muted-foreground">{m.desc}</span>
              </button>
            ))}
          </div>
        </div>

        {mode !== 'blueprint' ? (
          <FlowgroupTargetPicker
            pipelines={pipelines}
            existingNamesByPipeline={existingNamesByPipeline}
            fileTree={fileTree}
            loadingDirs={loadingDirs}
            seedPipeline={seedPipeline}
            onChange={handleTargetChange}
            disabled={disabled}
          />
        ) : (
          <BlueprintFields
            blueprints={blueprints}
            blueprint={blueprint}
            onBlueprint={setBlueprint}
            instanceName={instanceName}
            onInstanceName={setInstanceName}
            instanceNameError={instanceNameError}
            disabled={disabled}
          />
        )}

        {/* Template selection + params */}
        {mode === 'template' && (
          <div className="space-y-3 border-t border-border pt-4">
            <div>
              <label className="mb-1 block text-xs font-medium text-muted-foreground">Template</label>
              <Select value={template} onValueChange={setTemplate} disabled={disabled}>
                <SelectTrigger size="sm" className="w-full">
                  <SelectValue placeholder={templates.length ? 'Select template' : 'No templates found'} />
                </SelectTrigger>
                <SelectContent>
                  {templates.map((t) => (
                    <SelectItem key={t.source_path} value={t.source_path} disabled={t.state !== 'ready' || !t.reference}>
                      {t.declared_name || t.reference || t.source_path} · {t.source_path}{t.state !== 'ready' ? ' (unavailable)' : ''}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            {templateCatalogError && <p role="alert" className="text-xs text-destructive">Could not load templates. <Button size="sm" variant="ghost" onClick={retryCatalog}>Retry templates</Button></p>}
            {template && templateQuery.isFetching && <p role="status" className="text-xs text-muted-foreground">Reading template parameters…</p>}
            {template && templateQuery.isError && <p role="alert" className="text-xs text-destructive">Could not read this template. <Button size="sm" variant="ghost" onClick={() => { void templateQuery.refetch() }}>Retry template</Button></p>}
            {templateInfo && <>
              <Button size="sm" variant="ghost" onClick={() => { captureWorkspaceEditors(); if (focusInvalidWorkspaceDraft()) return; void openWorkspaceFile(template); close() }}>Edit template</Button>
              {templateInfo.state !== 'ready' && <p role="alert" className="text-xs text-destructive">{templateInfo.diagnostics[0]?.message ?? 'This template cannot be invoked.'}</p>}
              <TemplateInvocationParams key={template} params={templateParams} values={templateValues} onSet={setTemplateValue} onUnset={unsetTemplateValue} disabled={disabled} onValidityChange={setTemplateValuesValid} />
              {templateInfo.reference && <details className="text-xs"><summary className="cursor-pointer text-muted-foreground">Flowgroup YAML</summary><pre className="mt-2 overflow-auto rounded bg-muted p-2">{buildTemplateFlowgroupYaml(target.pipeline || 'pipeline', target.name || 'flowgroup', templateInfo.reference, templateValues)}</pre></details>}
            </>}
          </div>
        )}

        {/* Blueprint params */}
        {mode === 'blueprint' && blueprint && (
          <div className="space-y-3 border-t border-border pt-4">
            <span className="block text-xs font-medium text-muted-foreground">Parameters</span>
            {blueprintParamsQuery.isLoading ? (
              <div className="flex items-center gap-2 text-2xs text-muted-foreground">
                <Loader2 className="size-3 animate-spin" aria-hidden="true" /> Reading blueprint…
              </div>
            ) : blueprintParamsQuery.isError ? (
              <div className="space-y-2">
                <p className="text-2xs text-muted-foreground">
                  Couldn&apos;t read this blueprint&apos;s declared parameters — enter them as
                  key/value pairs, or edit the instance file after creating.
                </p>
                <KeyValueMapEditor
                  id="bp-freeform-params"
                  label="Parameters"
                  value={blueprintValues}
                  onSetEntry={(k, v) => setBlueprintValue(k, v)}
                  onRenameEntry={(oldK, newK) => {
                    const held = blueprintValues[oldK]
                    unsetBlueprintValue(oldK)
                    setBlueprintValue(newK, held)
                  }}
                  onRemoveEntry={(k) => unsetBlueprintValue(k)}
                  onDeleteKey={() => setBlueprintValues({})}
                  allowEmpty
                  disabled={disabled}
                />
              </div>
            ) : (
              <ParamsForm
                params={blueprintParams}
                values={blueprintValues}
                onSet={setBlueprintValue}
                onUnset={unsetBlueprintValue}
                disabled={disabled}
              />
            )}
          </div>
        )}

        {mode === 'blueprint' && blueprint && instanceName && instanceNameError === null && (
          <PathPreview path={blueprintInstancePath(instanceName)} />
        )}

        {submitError && (
          <p role="alert" className="text-xs text-destructive">
            {submitError}
          </p>
        )}
      </fieldset>

      <div className="flex items-center justify-end gap-2 border-t border-border px-5 py-3">
        <Button variant="ghost" size="sm" onClick={close} disabled={submitting}>
          Cancel
        </Button>
        {overwrite !== null ? (
          <Button
            variant="destructive"
            size="sm"
            onClick={() => void confirmOverwrite()}
            disabled={disabled || !canCreate}
          >
            {submitting ? <Loader2 className="animate-spin" aria-hidden="true" /> : null}
            Overwrite
          </Button>
        ) : (
          <Button size="sm" onClick={() => void submit()} disabled={!canCreate}>
            {submitting ? (
              <Loader2 className="animate-spin" aria-hidden="true" />
            ) : (
              <FilePlus2 aria-hidden="true" />
            )}
            Create
          </Button>
        )}
      </div>
    </>
  )
}
