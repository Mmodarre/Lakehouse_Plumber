import { useEffect } from 'react'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { Separator } from '@/components/ui/separator'
import { ScrollArea } from '@/components/ui/scroll-area'
import { useTemplatesDetail, useTemplateDetail } from '@/hooks/useTemplates'
import { useBuilderStore } from '../hooks/useBuilderStore'
import { ArrayInput } from '../config/forms/shared/ArrayInput'
import { KeyValueInput } from '../config/forms/shared/KeyValueInput'

function inferFieldType(defaultValue: unknown): 'string' | 'object' | 'array' | 'boolean' {
  if (Array.isArray(defaultValue)) return 'array'
  if (typeof defaultValue === 'object' && defaultValue !== null) return 'object'
  if (typeof defaultValue === 'boolean') return 'boolean'
  return 'string'
}

export default function TemplateWizardStep() {
  const { templateInfo, updateTemplateInfo } = useBuilderStore()
  const { data: templatesData } = useTemplatesDetail()
  const { data: templateDetail } = useTemplateDetail(templateInfo.templateName || null)

  const templates = templatesData?.templates ?? []
  const parameters = templateDetail?.template.parameters ?? []

  // Initialize parameters with defaults when template changes
  useEffect(() => {
    if (!templateDetail) return
    const defaults: Record<string, unknown> = {}
    for (const param of templateDetail.template.parameters) {
      const name = param.name as string
      if (param.default !== undefined) {
        defaults[name] = param.default
      } else {
        defaults[name] = ''
      }
    }
    // Merge with existing values (preserve user input)
    updateTemplateInfo({
      parameters: { ...defaults, ...templateInfo.parameters },
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [templateDetail])

  const updateParam = (name: string, value: unknown) => {
    updateTemplateInfo({
      parameters: { ...templateInfo.parameters, [name]: value },
    })
  }

  return (
    <div className="flex flex-1 overflow-hidden">
      {/* Left: Template selector + parameter form */}
      <div className="flex flex-1 flex-col overflow-hidden border-r border-slate-200">
        <div className="space-y-4 p-6">
          <div>
            <h2 className="text-lg font-semibold text-slate-800">Template Wizard</h2>
            <p className="text-sm text-slate-500">
              Select a template and fill in the parameters.
            </p>
          </div>

          {/* Template selector */}
          <div className="space-y-2">
            <Label>Template</Label>
            <Select
              value={templateInfo.templateName}
              onValueChange={(v) => {
                updateTemplateInfo({ templateName: v, parameters: {} })
              }}
            >
              <SelectTrigger>
                <SelectValue placeholder="Choose a template..." />
              </SelectTrigger>
              <SelectContent>
                {templates.map((t) => (
                  <SelectItem key={t.name} value={t.name}>
                    <div className="flex items-center gap-2">
                      <span>{t.name}</span>
                      <div className="flex gap-1">
                        {t.action_types.map((at) => (
                          <Badge key={at} variant="outline" className="text-[10px]">
                            {at}
                          </Badge>
                        ))}
                      </div>
                    </div>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {templateInfo.templateName && templateDetail && (
              <p className="text-xs text-slate-500">
                {templateDetail.template.description}
              </p>
            )}
          </div>
        </div>

        <Separator />

        {/* Parameter form */}
        <ScrollArea className="flex-1">
          <div className="space-y-4 p-6">
            {parameters.length === 0 && templateInfo.templateName && (
              <p className="text-sm text-slate-400">
                This template has no parameters.
              </p>
            )}
            {parameters.map((param) => {
              const name = param.name as string
              const required = param.required as boolean | undefined
              const description = param.description as string | undefined
              const defaultValue = param.default
              const fieldType = inferFieldType(defaultValue)
              const value = templateInfo.parameters[name]

              return (
                <div key={name} className="space-y-1.5">
                  <Label className="flex items-center gap-1">
                    {name}
                    {required && <span className="text-red-500">*</span>}
                  </Label>
                  {description && (
                    <p className="text-xs text-slate-400">{description}</p>
                  )}

                  {fieldType === 'string' && (
                    <Input
                      value={(value as string) ?? ''}
                      onChange={(e) => updateParam(name, e.target.value)}
                      placeholder={
                        defaultValue !== undefined
                          ? `Default: ${String(defaultValue)}`
                          : undefined
                      }
                    />
                  )}

                  {fieldType === 'boolean' && (
                    <Select
                      value={String(value ?? defaultValue ?? 'false')}
                      onValueChange={(v) => updateParam(name, v === 'true')}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="true">true</SelectItem>
                        <SelectItem value="false">false</SelectItem>
                      </SelectContent>
                    </Select>
                  )}

                  {fieldType === 'array' && (
                    <ArrayInput
                      value={(value as string[]) ?? []}
                      onChange={(v) => updateParam(name, v)}
                      placeholder={`Add ${name}...`}
                    />
                  )}

                  {fieldType === 'object' && (
                    <KeyValueInput
                      value={(value as Record<string, string>) ?? {}}
                      onChange={(v) => updateParam(name, v)}
                    />
                  )}
                </div>
              )
            })}
          </div>
        </ScrollArea>
      </div>

      {/* Right: YAML preview (read-only placeholder — will be replaced by Monaco in PreviewSave) */}
      <div className="flex w-[400px] flex-col bg-slate-50">
        <div className="border-b border-slate-200 px-4 py-3">
          <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
            Preview
          </span>
        </div>
        <ScrollArea className="flex-1">
          <pre className="p-4 font-mono text-xs text-slate-600 whitespace-pre-wrap">
            {templateInfo.templateName
              ? generatePreview(templateInfo.templateName, templateInfo.parameters)
              : 'Select a template to see a preview...'}
          </pre>
        </ScrollArea>
      </div>
    </div>
  )
}

function generatePreview(
  templateName: string,
  parameters: Record<string, unknown>,
): string {
  const lines: string[] = []
  lines.push(`use_template: ${templateName}`)
  lines.push('template_parameters:')

  for (const [key, value] of Object.entries(parameters)) {
    if (value === '' || value === undefined || value === null) continue

    if (Array.isArray(value)) {
      if (value.length === 0) continue
      lines.push(`  ${key}:`)
      for (const item of value) {
        lines.push(`    - ${item}`)
      }
    } else if (typeof value === 'object') {
      const entries = Object.entries(value as Record<string, string>)
      if (entries.length === 0) continue
      lines.push(`  ${key}:`)
      for (const [k, v] of entries) {
        lines.push(`    ${k}: ${v}`)
      }
    } else {
      lines.push(`  ${key}: ${String(value)}`)
    }
  }

  return lines.join('\n')
}
