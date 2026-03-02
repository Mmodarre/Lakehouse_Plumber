import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { FormField } from './shared/FormField'
import { FormSection } from './shared/FormSection'
import { KeyValueInput } from './shared/KeyValueInput'
import { ArrayInput } from './shared/ArrayInput'

interface PythonTransformFormProps {
  config: Record<string, unknown>
  onChange: (updates: Record<string, unknown>) => void
  upstreamNames: string[]
  target: string
  onTargetChange: (target: string) => void
}

export function PythonTransformForm({ config, onChange, upstreamNames, target, onTargetChange }: PythonTransformFormProps) {
  const update = (key: string, value: unknown) => onChange({ [key]: value })

  // Support multiple sources
  const sources = (config.sources as string[]) ?? []

  const handleSourceToggle = (name: string, checked: boolean) => {
    const updated = checked
      ? [...sources, name]
      : sources.filter((s) => s !== name)
    update('sources', updated)
  }

  return (
    <div className="space-y-4">
      <FormField label="Target view" description="Temporary view name created by this transform">
        <Input
          value={target}
          onChange={(e) => onTargetChange(e.target.value)}
          placeholder="v_my_python_transform"
        />
      </FormField>

      <FormField label="Source(s)" description="Upstream view names (supports multiple)">
        {upstreamNames.length > 0 ? (
          <div className="space-y-2">
            {upstreamNames.map((n) => (
              <label key={n} className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={sources.includes(n)}
                  onChange={(e) => handleSourceToggle(n, e.target.checked)}
                  className="rounded border-slate-300"
                />
                {n}
              </label>
            ))}
          </div>
        ) : (
          <ArrayInput
            value={sources}
            onChange={(v) => update('sources', v)}
            placeholder="Add source view name..."
          />
        )}
      </FormField>

      <FormField label="Module path" required description="Python module containing the transform function">
        <Input
          value={(config.module_path as string) ?? ''}
          onChange={(e) => update('module_path', e.target.value)}
          placeholder="e.g. transforms.custom_transform"
        />
      </FormField>

      <FormField label="Function name" required description="Name of the transform function">
        <Input
          value={(config.function_name as string) ?? ''}
          onChange={(e) => update('function_name', e.target.value)}
          placeholder="e.g. transform_data"
        />
      </FormField>

      <FormField label="Read mode">
        <Select
          value={(config.read_mode as string) ?? ''}
          onValueChange={(v) => update('read_mode', v || undefined)}
        >
          <SelectTrigger>
            <SelectValue placeholder="Default" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value=" ">Default</SelectItem>
            <SelectItem value="batch">Batch</SelectItem>
            <SelectItem value="stream">Stream</SelectItem>
          </SelectContent>
        </Select>
      </FormField>

      <FormField label="Description">
        <Textarea
          value={(config.description as string) ?? ''}
          onChange={(e) => update('description', e.target.value)}
          placeholder="Optional description..."
          rows={2}
        />
      </FormField>

      <FormSection title="Parameters">
        <FormField label="Function parameters" description="Key-value pairs passed to the transform function">
          <KeyValueInput
            value={(config.parameters as Record<string, string>) ?? {}}
            onChange={(v) => update('parameters', v)}
            keyPlaceholder="Parameter name"
            valuePlaceholder="Parameter value"
          />
        </FormField>
      </FormSection>

      <FormSection title="Operational Metadata">
        <FormField label="Metadata columns" description="Columns added to output">
          <ArrayInput
            value={(config.operational_metadata as string[]) ?? []}
            onChange={(v) => update('operational_metadata', v)}
            placeholder="Add metadata column..."
          />
        </FormField>
      </FormSection>
    </div>
  )
}
