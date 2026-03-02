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

interface SchemaTransformFormProps {
  config: Record<string, unknown>
  onChange: (updates: Record<string, unknown>) => void
  upstreamNames: string[]
  target: string
  onTargetChange: (target: string) => void
}

export function SchemaTransformForm({ config, onChange, upstreamNames, target, onTargetChange }: SchemaTransformFormProps) {
  const update = (key: string, value: unknown) => onChange({ [key]: value })

  const schemaMode = config.schema_file ? 'file' : 'inline'

  return (
    <div className="space-y-4">
      <FormField label="Target view" description="Temporary view name created by this transform">
        <Input
          value={target}
          onChange={(e) => onTargetChange(e.target.value)}
          placeholder="v_my_schema_enforced"
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

      <FormField label="Schema definition" required>
        <div className="mb-2 flex gap-2">
          <button
            className={`rounded px-2 py-1 text-xs ${schemaMode === 'inline' ? 'bg-blue-100 text-blue-700' : 'text-slate-500'}`}
            onClick={() => { update('schema_file', undefined) }}
          >
            Inline Schema
          </button>
          <button
            className={`rounded px-2 py-1 text-xs ${schemaMode === 'file' ? 'bg-blue-100 text-blue-700' : 'text-slate-500'}`}
            onClick={() => { update('schema_inline', undefined); update('schema_file', '') }}
          >
            Schema File
          </button>
        </div>

        {schemaMode === 'inline' ? (
          <Textarea
            value={(config.schema_inline as string) ?? ''}
            onChange={(e) => update('schema_inline', e.target.value)}
            placeholder="col1 STRING NOT NULL, col2 INT, col3 TIMESTAMP"
            rows={4}
            className="font-mono text-xs"
          />
        ) : (
          <Input
            value={(config.schema_file as string) ?? ''}
            onChange={(e) => update('schema_file', e.target.value)}
            placeholder="schemas/my_schema.yaml"
          />
        )}
      </FormField>

      <FormField label="Enforcement" description="How strictly to enforce the schema">
        <Select
          value={(config.enforcement as string) ?? ''}
          onValueChange={(v) => update('enforcement', v || undefined)}
        >
          <SelectTrigger>
            <SelectValue placeholder="Default" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value=" ">Default</SelectItem>
            <SelectItem value="strict">Strict (reject non-conforming)</SelectItem>
            <SelectItem value="permissive">Permissive (best effort)</SelectItem>
          </SelectContent>
        </Select>
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
            <SelectItem value="stream">Stream</SelectItem>
            <SelectItem value="batch">Batch</SelectItem>
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
