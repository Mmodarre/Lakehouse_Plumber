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

interface SQLTransformFormProps {
  config: Record<string, unknown>
  onChange: (updates: Record<string, unknown>) => void
  upstreamNames: string[]
  target: string
  onTargetChange: (target: string) => void
}

export function SQLTransformForm({ config, onChange, upstreamNames, target, onTargetChange }: SQLTransformFormProps) {
  const update = (key: string, value: unknown) => onChange({ [key]: value })

  const sqlMode = config.sql_file ? 'file' : 'inline'

  return (
    <div className="space-y-4">
      <FormField label="Target view" description="Temporary view name created by this transform (referenced by downstream actions)">
        <Input
          value={target}
          onChange={(e) => onTargetChange(e.target.value)}
          placeholder="v_my_transform"
        />
      </FormField>

      <FormField label="Source" description="Upstream view name">
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
            placeholder="Source view or table name"
          />
        )}
      </FormField>

      <FormField label="SQL definition">
        <div className="mb-2 flex gap-2">
          <button
            className={`rounded px-2 py-1 text-xs ${sqlMode === 'inline' ? 'bg-blue-100 text-blue-700' : 'text-slate-500'}`}
            onClick={() => { update('sql_file', undefined) }}
          >
            Inline SQL
          </button>
          <button
            className={`rounded px-2 py-1 text-xs ${sqlMode === 'file' ? 'bg-blue-100 text-blue-700' : 'text-slate-500'}`}
            onClick={() => { update('sql', undefined); update('sql_file', '') }}
          >
            SQL File
          </button>
        </div>

        {sqlMode === 'inline' ? (
          <Textarea
            value={(config.sql as string) ?? ''}
            onChange={(e) => update('sql', e.target.value)}
            placeholder="SELECT * FROM STREAM(source_view)"
            rows={6}
            className="font-mono text-xs"
          />
        ) : (
          <Input
            value={(config.sql_file as string) ?? ''}
            onChange={(e) => update('sql_file', e.target.value)}
            placeholder="sql/transform.sql"
          />
        )}
      </FormField>

      <FormField label="Description">
        <Input
          value={(config.description as string) ?? ''}
          onChange={(e) => update('description', e.target.value)}
          placeholder="Optional description..."
        />
      </FormField>
    </div>
  )
}
