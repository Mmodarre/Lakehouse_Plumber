import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Switch } from '@/components/ui/switch'
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

interface CloudFilesLoadFormProps {
  config: Record<string, unknown>
  onChange: (updates: Record<string, unknown>) => void
  target: string
  onTargetChange: (target: string) => void
}

export function CloudFilesLoadForm({ config, onChange, target, onTargetChange }: CloudFilesLoadFormProps) {
  const update = (key: string, value: unknown) => onChange({ [key]: value })

  return (
    <div className="space-y-4">
      <FormField label="Target view" description="Temporary view name created by this load (referenced by downstream actions)">
        <Input
          value={target}
          onChange={(e) => onTargetChange(e.target.value)}
          placeholder="v_my_load"
        />
      </FormField>

      <FormField label="Source path" required description="Cloud storage path (e.g. /Volumes/catalog/schema/volume/data/)">
        <Input
          value={(config.source_path as string) ?? ''}
          onChange={(e) => update('source_path', e.target.value)}
          placeholder="/Volumes/catalog/schema/volume/data/"
        />
      </FormField>

      <FormField label="File format" required>
        <Select
          value={(config.file_format as string) ?? 'json'}
          onValueChange={(v) => update('file_format', v)}
        >
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {['json', 'parquet', 'csv', 'avro', 'xml', 'text'].map((f) => (
              <SelectItem key={f} value={f}>{f}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </FormField>

      <FormField label="Read mode">
        <Select
          value={(config.read_mode as string) ?? 'stream'}
          onValueChange={(v) => update('read_mode', v)}
        >
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="stream">Stream (readStream)</SelectItem>
            <SelectItem value="batch">Batch (read)</SelectItem>
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

      <FormSection title="Operational Metadata">
        <FormField label="Metadata columns" description="Columns added to output (e.g. _processing_timestamp, _source_file_path)">
          <ArrayInput
            value={(config.operational_metadata as string[]) ?? []}
            onChange={(v) => update('operational_metadata', v)}
            placeholder="Add metadata column..."
          />
        </FormField>
      </FormSection>

      <FormSection title="Advanced Options">
        <FormField label="Schema hints path" description="Path to schema hints JSON file">
          <Input
            value={(config.schema_hints as string) ?? ''}
            onChange={(e) => update('schema_hints', e.target.value)}
            placeholder="schemas/my_schema.json"
          />
        </FormField>

        <FormField label="Max files per trigger">
          <Input
            type="number"
            value={(config.max_files_per_trigger as string) ?? ''}
            onChange={(e) => update('max_files_per_trigger', e.target.value || undefined)}
            placeholder="Default (all)"
          />
        </FormField>

        <FormField label="Schema evolution mode">
          <Select
            value={(config.schema_evolution_mode as string) ?? ''}
            onValueChange={(v) => update('schema_evolution_mode', v || undefined)}
          >
            <SelectTrigger>
              <SelectValue placeholder="Default" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value=" ">Default</SelectItem>
              <SelectItem value="addNewColumns">addNewColumns</SelectItem>
              <SelectItem value="rescue">rescue</SelectItem>
              <SelectItem value="failOnNewColumns">failOnNewColumns</SelectItem>
              <SelectItem value="none">none</SelectItem>
            </SelectContent>
          </Select>
        </FormField>

        <FormField label="Infer column types" description="Let Auto Loader infer column data types">
          <div className="flex items-center gap-2">
            <Switch
              checked={config.infer_column_types !== false}
              onCheckedChange={(v) => update('infer_column_types', v ? undefined : false)}
            />
            <span className="text-xs text-slate-500">
              {config.infer_column_types === false ? 'Disabled' : 'Enabled (default)'}
            </span>
          </div>
        </FormField>

        <FormField label="Rescued data column" description="Column name to capture unparseable data">
          <Input
            value={(config.rescued_data_column as string) ?? ''}
            onChange={(e) => update('rescued_data_column', e.target.value || undefined)}
            placeholder="_rescued_data"
          />
        </FormField>

        <FormField label="Reader options" description="Additional Spark reader options">
          <KeyValueInput
            value={(config.reader_options as Record<string, string>) ?? {}}
            onChange={(v) => update('reader_options', v)}
          />
        </FormField>
      </FormSection>
    </div>
  )
}
