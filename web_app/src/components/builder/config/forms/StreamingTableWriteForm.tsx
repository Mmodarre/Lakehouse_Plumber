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
import { KeyValueInput } from './shared/KeyValueInput'

interface StreamingTableWriteFormProps {
  config: Record<string, unknown>
  onChange: (updates: Record<string, unknown>) => void
  upstreamNames: string[]
}

export function StreamingTableWriteForm({ config, onChange, upstreamNames }: StreamingTableWriteFormProps) {
  const update = (key: string, value: unknown) => onChange({ [key]: value })
  const cdcMode = (config.cdc_mode as string) ?? 'none'

  return (
    <div className="space-y-4">
      <FormField label="Source" description="Upstream action providing data">
        {upstreamNames.length > 0 ? (
          <Select
            value={(config.source as string) ?? upstreamNames[0] ?? ''}
            onValueChange={(v) => update('source', v)}
          >
            <SelectTrigger>
              <SelectValue placeholder="Select source..." />
            </SelectTrigger>
            <SelectContent>
              {upstreamNames.map((n) => (
                <SelectItem key={n} value={n}>{n}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        ) : (
          <Input
            value={(config.source as string) ?? ''}
            onChange={(e) => update('source', e.target.value)}
            placeholder="Source action name"
          />
        )}
      </FormField>

      <FormField label="Database" description="Target catalog.schema or database name">
        <Input
          value={(config.database as string) ?? ''}
          onChange={(e) => update('database', e.target.value)}
          placeholder="catalog.schema"
        />
      </FormField>

      <FormField label="Table" required>
        <Input
          value={(config.table as string) ?? ''}
          onChange={(e) => update('table', e.target.value)}
          placeholder="target_table_name"
        />
      </FormField>

      <FormField label="Comment">
        <Input
          value={(config.comment as string) ?? ''}
          onChange={(e) => update('comment', e.target.value)}
          placeholder="Table description"
        />
      </FormField>

      <FormField label="Description">
        <Textarea
          value={(config.description as string) ?? ''}
          onChange={(e) => update('description', e.target.value)}
          placeholder="Optional description of this write action..."
          rows={2}
        />
      </FormField>

      <FormField label="Read mode">
        <Select
          value={(config.read_mode as string) ?? ''}
          onValueChange={(v) => update('read_mode', v || undefined)}
        >
          <SelectTrigger>
            <SelectValue placeholder="Default (stream)" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value=" ">Default</SelectItem>
            <SelectItem value="stream">Stream</SelectItem>
            <SelectItem value="batch">Batch</SelectItem>
          </SelectContent>
        </Select>
      </FormField>

      <FormSection title="Partitioning & Clustering">
        <FormField label="Partition columns">
          <ArrayInput
            value={(config.partition_columns as string[]) ?? []}
            onChange={(v) => update('partition_columns', v)}
            placeholder="Add partition column..."
          />
        </FormField>
        <FormField label="Cluster columns">
          <ArrayInput
            value={(config.cluster_columns as string[]) ?? []}
            onChange={(v) => update('cluster_columns', v)}
            placeholder="Add cluster column..."
          />
        </FormField>
      </FormSection>

      <FormSection title="Table Properties">
        <KeyValueInput
          value={(config.table_properties as Record<string, string>) ?? {}}
          onChange={(v) => update('table_properties', v)}
        />
      </FormSection>

      <FormSection title="CDC (Change Data Capture)">
        <FormField label="CDC Mode">
          <Select
            value={cdcMode}
            onValueChange={(v) => update('cdc_mode', v === 'none' ? undefined : v)}
          >
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="none">None</SelectItem>
              <SelectItem value="cdc">CDC (apply_changes)</SelectItem>
              <SelectItem value="snapshot_cdc">Snapshot CDC (apply_changes_from_snapshot)</SelectItem>
            </SelectContent>
          </Select>
        </FormField>

        {(cdcMode === 'cdc' || cdcMode === 'snapshot_cdc') && (
          <>
            <FormField label="Keys" required description="Primary key columns for CDC merge">
              <ArrayInput
                value={(config.cdc_keys as string[]) ?? []}
                onChange={(v) => update('cdc_keys', v)}
                placeholder="Add key column..."
              />
            </FormField>
            <FormField label="Sequence by" description="Column to order changes">
              <Input
                value={(config.sequence_by as string) ?? ''}
                onChange={(e) => update('sequence_by', e.target.value)}
                placeholder="e.g. updated_at"
              />
            </FormField>
            <FormField label="SCD Type">
              <Select
                value={String((config.scd_type as number) ?? 1)}
                onValueChange={(v) => update('scd_type', Number(v))}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="1">Type 1 (overwrite)</SelectItem>
                  <SelectItem value="2">Type 2 (history)</SelectItem>
                </SelectContent>
              </Select>
            </FormField>

            <FormField label="Ignore null updates" description="Skip updates where all non-key columns are null">
              <div className="flex items-center gap-2">
                <Switch
                  checked={config.ignore_null_updates === true}
                  onCheckedChange={(v) => update('ignore_null_updates', v || undefined)}
                />
                <span className="text-xs text-slate-500">
                  {config.ignore_null_updates === true ? 'Enabled' : 'Disabled'}
                </span>
              </div>
            </FormField>

            <FormField label="Apply as deletes" description="SQL expression identifying deletes">
              <Input
                value={(config.apply_as_deletes as string) ?? ''}
                onChange={(e) => update('apply_as_deletes', e.target.value || undefined)}
                placeholder="e.g. operation = 'DELETE'"
              />
            </FormField>

            <FormField label="Apply as truncates" description="SQL expression identifying truncates">
              <Input
                value={(config.apply_as_truncates as string) ?? ''}
                onChange={(e) => update('apply_as_truncates', e.target.value || undefined)}
                placeholder="e.g. operation = 'TRUNCATE'"
              />
            </FormField>

            <FormField label="Track history columns" description="Columns to track history for (SCD Type 2)">
              <ArrayInput
                value={(config.track_history_column_list as string[]) ?? []}
                onChange={(v) => update('track_history_column_list', v)}
                placeholder="Add column..."
              />
            </FormField>

            <FormField label="Track history except columns" description="Columns to exclude from history tracking">
              <ArrayInput
                value={(config.track_history_except_column_list as string[]) ?? []}
                onChange={(v) => update('track_history_except_column_list', v)}
                placeholder="Add column..."
              />
            </FormField>

            <FormField label="Except columns" description="Columns to exclude from output">
              <ArrayInput
                value={(config.except_column_list as string[]) ?? []}
                onChange={(v) => update('except_column_list', v)}
                placeholder="Add column..."
              />
            </FormField>
          </>
        )}
      </FormSection>

      <FormSection title="Advanced Options">
        <FormField label="Create table" description="Set to false to skip table creation (use existing table)">
          <div className="flex items-center gap-2">
            <Switch
              checked={config.create_table !== false}
              onCheckedChange={(v) => update('create_table', v ? undefined : false)}
            />
            <span className="text-xs text-slate-500">
              {config.create_table === false ? 'Disabled' : 'Enabled (default)'}
            </span>
          </div>
        </FormField>

        <FormField label="Table schema" description="DDL schema definition for the target table">
          <Textarea
            value={(config.table_schema as string) ?? ''}
            onChange={(e) => update('table_schema', e.target.value || undefined)}
            placeholder="col1 STRING, col2 INT, ..."
            rows={3}
            className="font-mono text-xs"
          />
        </FormField>

        <FormField label="Row filter" description="SQL expression to filter rows">
          <Input
            value={(config.row_filter as string) ?? ''}
            onChange={(e) => update('row_filter', e.target.value || undefined)}
            placeholder="e.g. status != 'deleted'"
          />
        </FormField>

        <FormField label="Once" description="Process each record only once (no reprocessing)">
          <div className="flex items-center gap-2">
            <Switch
              checked={config.once === true}
              onCheckedChange={(v) => update('once', v || undefined)}
            />
            <span className="text-xs text-slate-500">
              {config.once === true ? 'Enabled' : 'Disabled'}
            </span>
          </div>
        </FormField>

        <FormField label="Spark config" description="Spark configuration for this write action">
          <KeyValueInput
            value={(config.spark_conf as Record<string, string>) ?? {}}
            onChange={(v) => update('spark_conf', v)}
            keyPlaceholder="spark.key"
            valuePlaceholder="value"
          />
        </FormField>
      </FormSection>
    </div>
  )
}
