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
import { SqlOrFileToggle } from './shared/SqlOrFileToggle'

interface SQLLoadFormProps {
  config: Record<string, unknown>
  onChange: (updates: Record<string, unknown>) => void
  target: string
  onTargetChange: (target: string) => void
}

export function SQLLoadForm({ config, onChange, target, onTargetChange }: SQLLoadFormProps) {
  const update = (key: string, value: unknown) => onChange({ [key]: value })

  return (
    <div className="space-y-4">
      <FormField label="Target view" description="Temporary view name created by this load">
        <Input
          value={target}
          onChange={(e) => onTargetChange(e.target.value)}
          placeholder="v_my_sql_load"
        />
      </FormField>

      <FormField label="SQL definition" required description="SQL query defining the load source">
        <SqlOrFileToggle
          sql={(config.sql as string) ?? ''}
          sqlFile={(config.sql_file as string) ?? ''}
          onSqlChange={(v) => { update('sql', v); update('sql_file', '') }}
          onSqlFileChange={(v) => { update('sql_file', v); if (v.trim()) update('sql', '') }}
          sqlPlaceholder="SELECT * FROM catalog.schema.table WHERE ..."
          filePlaceholder="sql/load_query.sql"
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
    </div>
  )
}
