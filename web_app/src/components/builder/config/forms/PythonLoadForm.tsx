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

interface PythonLoadFormProps {
  config: Record<string, unknown>
  onChange: (updates: Record<string, unknown>) => void
  target: string
  onTargetChange: (target: string) => void
}

export function PythonLoadForm({ config, onChange, target, onTargetChange }: PythonLoadFormProps) {
  const update = (key: string, value: unknown) => onChange({ [key]: value })

  return (
    <div className="space-y-4">
      <FormField label="Target view" description="Temporary view name created by this load">
        <Input
          value={target}
          onChange={(e) => onTargetChange(e.target.value)}
          placeholder="v_my_python_load"
        />
      </FormField>

      <FormField label="Module path" required description="Python module containing the loader function">
        <Input
          value={(config.module_path as string) ?? ''}
          onChange={(e) => update('module_path', e.target.value)}
          placeholder="e.g. loaders.custom_loader"
        />
      </FormField>

      <FormField label="Function name" description="Name of the function to call (defaults to get_df)">
        <Input
          value={(config.function_name as string) ?? ''}
          onChange={(e) => update('function_name', e.target.value || undefined)}
          placeholder="get_df"
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
            <SelectItem value="batch">Batch (read)</SelectItem>
            <SelectItem value="stream">Stream (readStream)</SelectItem>
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
        <FormField label="Function parameters" description="Key-value pairs passed to the loader function">
          <KeyValueInput
            value={(config.parameters as Record<string, string>) ?? {}}
            onChange={(v) => update('parameters', v)}
            keyPlaceholder="Parameter name"
            valuePlaceholder="Parameter value"
          />
        </FormField>
      </FormSection>

      <FormSection title="Operational Metadata">
        <FormField label="Metadata columns" description="Columns added to output (e.g. _processing_timestamp)">
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
