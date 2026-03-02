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
import { ArrayInput } from './shared/ArrayInput'
import { KeyValueInput } from './shared/KeyValueInput'
import { SqlOrFileToggle } from './shared/SqlOrFileToggle'

interface MaterializedViewWriteFormProps {
  config: Record<string, unknown>
  onChange: (updates: Record<string, unknown>) => void
  upstreamNames: string[]
}

export function MaterializedViewWriteForm({ config, onChange, upstreamNames }: MaterializedViewWriteFormProps) {
  const update = (key: string, value: unknown) => onChange({ [key]: value })

  return (
    <div className="space-y-4">
      <FormField label="Source" description="Upstream action or table name">
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
            placeholder="Source action or table"
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
          placeholder="target_view_name"
        />
      </FormField>

      <FormField label="SQL definition" description="Define the view query (overrides source when set)">
        <SqlOrFileToggle
          sql={(config.wt_sql as string) ?? ''}
          sqlFile={(config.wt_sql_file as string) ?? ''}
          onSqlChange={(v) => { update('wt_sql', v); update('wt_sql_file', '') }}
          onSqlFileChange={(v) => { update('wt_sql_file', v); if (v.trim()) update('wt_sql', '') }}
          sqlPlaceholder="SELECT * FROM source_table WHERE ..."
          filePlaceholder="sql/materialized_view.sql"
        />
      </FormField>

      <FormField label="Comment">
        <Input
          value={(config.comment as string) ?? ''}
          onChange={(e) => update('comment', e.target.value)}
          placeholder="View description"
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
        <FormField label="Table properties">
          <KeyValueInput
            value={(config.table_properties as Record<string, string>) ?? {}}
            onChange={(v) => update('table_properties', v)}
          />
        </FormField>
      </FormSection>

      <FormSection title="Advanced Options">
        <FormField label="Table schema" description="DDL schema definition for the materialized view">
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
      </FormSection>
    </div>
  )
}
