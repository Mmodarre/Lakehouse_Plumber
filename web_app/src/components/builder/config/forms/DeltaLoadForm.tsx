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
import { ArrayInput } from './shared/ArrayInput'

interface DeltaLoadFormProps {
  config: Record<string, unknown>
  onChange: (updates: Record<string, unknown>) => void
  target: string
  onTargetChange: (target: string) => void
}

export function DeltaLoadForm({ config, onChange, target, onTargetChange }: DeltaLoadFormProps) {
  const update = (key: string, value: unknown) => onChange({ [key]: value })

  return (
    <div className="space-y-4">
      <FormField label="Target view" description="Temporary view name created by this load">
        <Input
          value={target}
          onChange={(e) => onTargetChange(e.target.value)}
          placeholder="v_my_delta_load"
        />
      </FormField>

      <FormField label="Source database" required description="Catalog.schema of the source Delta table">
        <Input
          value={(config.database as string) ?? ''}
          onChange={(e) => update('database', e.target.value)}
          placeholder="catalog.schema"
        />
      </FormField>

      <FormField label="Source table" required description="Name of the source Delta table">
        <Input
          value={(config.source_table as string) ?? ''}
          onChange={(e) => update('source_table', e.target.value)}
          placeholder="source_table_name"
        />
      </FormField>

      <FormField label="Read mode">
        <Select
          value={(config.read_mode as string) ?? 'batch'}
          onValueChange={(v) => update('read_mode', v)}
        >
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
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

      <FormSection title="Operational Metadata">
        <FormField label="Metadata columns" description="Columns added to output (e.g. _processing_timestamp)">
          <ArrayInput
            value={(config.operational_metadata as string[]) ?? []}
            onChange={(v) => update('operational_metadata', v)}
            placeholder="Add metadata column..."
          />
        </FormField>
      </FormSection>

      <FormSection title="Filtering">
        <FormField label="WHERE clause" description="SQL filter applied to the source">
          <Input
            value={(config.where_clause as string) ?? ''}
            onChange={(e) => update('where_clause', e.target.value || undefined)}
            placeholder="e.g. status = 'active'"
          />
        </FormField>

        <FormField label="Select columns" description="Specific columns to select">
          <ArrayInput
            value={(config.select_columns as string[]) ?? []}
            onChange={(v) => update('select_columns', v)}
            placeholder="Add column name..."
          />
        </FormField>
      </FormSection>

      <FormSection title="Delta Reader Options">
        <FormField label="Read change feed" description="Enable Change Data Feed for streaming">
          <div className="flex items-center gap-2">
            <Switch
              checked={config.readChangeFeed === true}
              onCheckedChange={(v) => update('readChangeFeed', v || undefined)}
            />
            <span className="text-xs text-slate-500">
              {config.readChangeFeed === true ? 'Enabled' : 'Disabled'}
            </span>
          </div>
        </FormField>

        <FormField label="Starting version" description="Start reading from this Delta version">
          <Input
            value={(config.startingVersion as string) ?? ''}
            onChange={(e) => update('startingVersion', e.target.value || undefined)}
            placeholder="e.g. 0"
          />
        </FormField>

        <FormField label="Starting timestamp" description="Start reading from this timestamp">
          <Input
            value={(config.startingTimestamp as string) ?? ''}
            onChange={(e) => update('startingTimestamp', e.target.value || undefined)}
            placeholder="e.g. 2024-01-01T00:00:00Z"
          />
        </FormField>

        <FormField label="Version as of" description="Read specific Delta version (batch only)">
          <Input
            value={(config.versionAsOf as string) ?? ''}
            onChange={(e) => update('versionAsOf', e.target.value || undefined)}
            placeholder="e.g. 5"
          />
        </FormField>

        <FormField label="Timestamp as of" description="Read Delta table as of a timestamp (batch only)">
          <Input
            value={(config.timestampAsOf as string) ?? ''}
            onChange={(e) => update('timestampAsOf', e.target.value || undefined)}
            placeholder="e.g. 2024-01-01"
          />
        </FormField>

        <FormField label="Ignore deletes" description="Ignore delete operations in change feed">
          <div className="flex items-center gap-2">
            <Switch
              checked={config.ignoreDeletes === true}
              onCheckedChange={(v) => update('ignoreDeletes', v || undefined)}
            />
            <span className="text-xs text-slate-500">
              {config.ignoreDeletes === true ? 'Enabled' : 'Disabled'}
            </span>
          </div>
        </FormField>

        <FormField label="Skip change commits" description="Skip entire change feed commits">
          <div className="flex items-center gap-2">
            <Switch
              checked={config.skipChangeCommits === true}
              onCheckedChange={(v) => update('skipChangeCommits', v || undefined)}
            />
            <span className="text-xs text-slate-500">
              {config.skipChangeCommits === true ? 'Enabled' : 'Disabled'}
            </span>
          </div>
        </FormField>

        <FormField label="Max files per trigger" description="Limit files processed per micro-batch">
          <Input
            type="number"
            value={(config.maxFilesPerTrigger as string) ?? ''}
            onChange={(e) => update('maxFilesPerTrigger', e.target.value || undefined)}
            placeholder="Default (all)"
          />
        </FormField>
      </FormSection>
    </div>
  )
}
