import { useEffect, useMemo, useState } from 'react'
import { X } from 'lucide-react'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover'
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from '@/components/ui/command'
import { usePipelines } from '@/hooks/usePipelines'
import { useFlowgroups } from '@/hooks/useFlowgroups'
import { usePresetsDetail } from '@/hooks/usePresets'
import { fetchFilePath } from '@/api/files'
import { useBuilderStore } from '../hooks/useBuilderStore'

const NAME_PATTERN = /^[a-zA-Z0-9_-]+$/

export default function BasicInfoStep() {
  const { basicInfo, updateBasicInfo } = useBuilderStore()
  const { data: pipelineData } = usePipelines()
  const { data: flowgroupData } = useFlowgroups()
  const { data: presetsData } = usePresetsDetail()

  const [subdirs, setSubdirs] = useState<string[]>([])
  const [loadingDirs, setLoadingDirs] = useState(false)
  const [presetOpen, setPresetOpen] = useState(false)

  const pipelines = useMemo(
    () => pipelineData?.pipelines.map((p) => p.name) ?? [],
    [pipelineData],
  )

  const existingNames = useMemo(
    () => new Set(flowgroupData?.flowgroups.map((f) => f.name.toLowerCase()) ?? []),
    [flowgroupData],
  )

  const activePipeline = basicInfo.isNewPipeline ? basicInfo.newPipeline : basicInfo.pipeline

  // Initialize pipeline selection
  useEffect(() => {
    if (!basicInfo.pipeline && pipelines.length > 0) {
      updateBasicInfo({ pipeline: pipelines[0] })
    }
  }, [pipelines, basicInfo.pipeline, updateBasicInfo])

  // Fetch subdirectories when pipeline changes
  useEffect(() => {
    if (!activePipeline) {
      setSubdirs([])
      return
    }
    setLoadingDirs(true)
    fetchFilePath(`pipelines/${activePipeline}`)
      .then((res) => {
        const dirs = (res.items ?? [])
          .filter((item) => item.type === 'directory')
          .map((item) => item.name)
        setSubdirs(dirs)
      })
      .catch(() => setSubdirs([]))
      .finally(() => setLoadingDirs(false))
  }, [activePipeline])

  // Validation
  const nameError = useMemo(() => {
    if (!basicInfo.flowgroupName) return ''
    if (!NAME_PATTERN.test(basicInfo.flowgroupName))
      return 'Only letters, numbers, hyphens, and underscores'
    if (existingNames.has(basicInfo.flowgroupName.toLowerCase()))
      return 'A flowgroup with this name already exists'
    return ''
  }, [basicInfo.flowgroupName, existingNames])

  const pipelineError = useMemo(() => {
    if (basicInfo.isNewPipeline && basicInfo.newPipeline && !NAME_PATTERN.test(basicInfo.newPipeline))
      return 'Only letters, numbers, hyphens, and underscores'
    return ''
  }, [basicInfo.isNewPipeline, basicInfo.newPipeline])

  const activeSubdir = basicInfo.isNewSubdir ? basicInfo.newSubdir : basicInfo.subdirectory
  const computedPath = useMemo(() => {
    if (!activePipeline || !basicInfo.flowgroupName) return ''
    const parts = ['pipelines', activePipeline]
    if (activeSubdir) parts.push(activeSubdir)
    parts.push(`${basicInfo.flowgroupName}.yaml`)
    return parts.join('/')
  }, [activePipeline, activeSubdir, basicInfo.flowgroupName])

  const presetList = presetsData?.presets ?? []

  const togglePreset = (name: string) => {
    const current = basicInfo.presets
    if (current.includes(name)) {
      updateBasicInfo({ presets: current.filter((p) => p !== name) })
    } else {
      updateBasicInfo({ presets: [...current, name] })
    }
  }

  return (
    <div className="mx-auto w-full max-w-lg space-y-6 p-8">
      <div>
        <h2 className="text-lg font-semibold text-slate-800">Basic Information</h2>
        <p className="text-sm text-slate-500">Set up the essentials for your flowgroup.</p>
      </div>

      {/* Pipeline selector */}
      <div className="space-y-2">
        <Label>Pipeline</Label>
        {basicInfo.isNewPipeline ? (
          <div className="flex items-center gap-2">
            <Input
              value={basicInfo.newPipeline}
              onChange={(e) => updateBasicInfo({ newPipeline: e.target.value })}
              placeholder="New pipeline name"
              autoFocus
            />
            <Button
              variant="ghost"
              size="sm"
              onClick={() => updateBasicInfo({ isNewPipeline: false, newPipeline: '' })}
            >
              Cancel
            </Button>
          </div>
        ) : (
          <div className="flex items-center gap-2">
            <Select
              value={basicInfo.pipeline}
              onValueChange={(v) => updateBasicInfo({ pipeline: v })}
            >
              <SelectTrigger>
                <SelectValue placeholder="Select a pipeline" />
              </SelectTrigger>
              <SelectContent>
                {pipelines.map((p) => (
                  <SelectItem key={p} value={p}>{p}</SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button
              variant="link"
              size="sm"
              className="whitespace-nowrap text-xs"
              onClick={() => updateBasicInfo({ isNewPipeline: true })}
            >
              + New
            </Button>
          </div>
        )}
        {pipelineError && <p className="text-xs text-red-500">{pipelineError}</p>}
      </div>

      {/* Flowgroup name */}
      <div className="space-y-2">
        <Label>Flowgroup name</Label>
        <Input
          value={basicInfo.flowgroupName}
          onChange={(e) => updateBasicInfo({ flowgroupName: e.target.value })}
          placeholder="e.g. customer_orders"
        />
        {nameError && <p className="text-xs text-red-500">{nameError}</p>}
      </div>

      {/* Directory picker */}
      {activePipeline && (
        <div className="space-y-2">
          <Label>
            Directory <span className="font-normal text-slate-400">(optional subfolder)</span>
          </Label>
          {loadingDirs ? (
            <div className="flex items-center gap-2 py-1 text-xs text-slate-400">
              <div className="h-3 w-3 animate-spin rounded-full border border-slate-300 border-t-slate-500" />
              Loading...
            </div>
          ) : basicInfo.isNewSubdir ? (
            <div className="flex items-center gap-2">
              <Input
                value={basicInfo.newSubdir}
                onChange={(e) => updateBasicInfo({ newSubdir: e.target.value })}
                placeholder="New subfolder name"
              />
              <Button
                variant="ghost"
                size="sm"
                onClick={() => updateBasicInfo({ isNewSubdir: false, newSubdir: '' })}
              >
                Cancel
              </Button>
            </div>
          ) : (
            <div className="flex items-center gap-2">
              <Select
                value={basicInfo.subdirectory}
                onValueChange={(v) => updateBasicInfo({ subdirectory: v })}
              >
                <SelectTrigger>
                  <SelectValue placeholder="(pipeline root)" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value=" ">(pipeline root)</SelectItem>
                  {subdirs.map((d) => (
                    <SelectItem key={d} value={d}>{d}/</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Button
                variant="link"
                size="sm"
                className="whitespace-nowrap text-xs"
                onClick={() => updateBasicInfo({ isNewSubdir: true })}
              >
                + New
              </Button>
            </div>
          )}
        </div>
      )}

      {/* Preset picker */}
      <div className="space-y-2">
        <Label>
          Presets <span className="font-normal text-slate-400">(optional)</span>
        </Label>
        <Popover open={presetOpen} onOpenChange={setPresetOpen}>
          <PopoverTrigger asChild>
            <Button
              variant="outline"
              className="w-full justify-start text-sm font-normal"
            >
              {basicInfo.presets.length > 0
                ? `${basicInfo.presets.length} preset(s) selected`
                : 'Select presets...'}
            </Button>
          </PopoverTrigger>
          <PopoverContent className="w-[300px] p-0" align="start">
            <Command>
              <CommandInput placeholder="Search presets..." />
              <CommandList>
                <CommandEmpty>No presets found.</CommandEmpty>
                <CommandGroup>
                  {presetList.map((p) => (
                    <CommandItem
                      key={p.name}
                      onSelect={() => togglePreset(p.name)}
                    >
                      <div className="flex items-center gap-2">
                        <div
                          className={`h-4 w-4 rounded border ${
                            basicInfo.presets.includes(p.name)
                              ? 'border-blue-500 bg-blue-500'
                              : 'border-slate-300'
                          }`}
                        >
                          {basicInfo.presets.includes(p.name) && (
                            <svg className="h-4 w-4 text-white" viewBox="0 0 24 24" fill="none" stroke="currentColor">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" />
                            </svg>
                          )}
                        </div>
                        <div>
                          <div className="text-sm">{p.name}</div>
                          {p.description && (
                            <div className="text-xs text-slate-400">{p.description}</div>
                          )}
                        </div>
                      </div>
                    </CommandItem>
                  ))}
                </CommandGroup>
              </CommandList>
            </Command>
          </PopoverContent>
        </Popover>
        {basicInfo.presets.length > 0 && (
          <div className="flex flex-wrap gap-1">
            {basicInfo.presets.map((p) => (
              <Badge key={p} variant="secondary" className="gap-1">
                {p}
                <button onClick={() => togglePreset(p)}>
                  <X className="h-3 w-3" />
                </button>
              </Badge>
            ))}
          </div>
        )}
      </div>

      {/* Path preview */}
      {computedPath && (
        <div className="rounded-md border border-slate-200 bg-slate-50 px-4 py-3">
          <span className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">
            File path
          </span>
          <p className="mt-0.5 font-mono text-xs text-slate-700">{computedPath}</p>
        </div>
      )}
    </div>
  )
}
