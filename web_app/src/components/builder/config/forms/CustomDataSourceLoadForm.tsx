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

interface CustomDataSourceLoadFormProps {
  config: Record<string, unknown>
  onChange: (updates: Record<string, unknown>) => void
  target: string
  onTargetChange: (target: string) => void
}

export function CustomDataSourceLoadForm({ config, onChange, target, onTargetChange }: CustomDataSourceLoadFormProps) {
  const update = (key: string, value: unknown) => onChange({ [key]: value })

  return (
    <div className="space-y-4">
      <FormField label="Target view" description="Temporary view name created by this load">
        <Input
          value={target}
          onChange={(e) => onTargetChange(e.target.value)}
          placeholder="v_my_custom_load"
        />
      </FormField>

      <FormField label="Module path" required description="Python module containing the custom data source class">
        <Input
          value={(config.module_path as string) ?? ''}
          onChange={(e) => update('module_path', e.target.value)}
          placeholder="e.g. sources.custom_source"
        />
      </FormField>

      <FormField label="DataSource class" required description="Name of the custom Spark DataSource class">
        <Input
          value={(config.custom_datasource_class as string) ?? ''}
          onChange={(e) => update('custom_datasource_class', e.target.value)}
          placeholder="e.g. MyCustomDataSource"
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

      <FormSection title="Options">
        <FormField label="DataSource options" description="Key-value configuration for the custom data source">
          <KeyValueInput
            value={(config.options as Record<string, string>) ?? {}}
            onChange={(v) => update('options', v)}
            keyPlaceholder="Option name"
            valuePlaceholder="Option value"
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
