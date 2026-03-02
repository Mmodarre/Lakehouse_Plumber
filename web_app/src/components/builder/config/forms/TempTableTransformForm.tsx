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

interface TempTableTransformFormProps {
  config: Record<string, unknown>
  onChange: (updates: Record<string, unknown>) => void
  upstreamNames: string[]
  target: string
  onTargetChange: (target: string) => void
}

export function TempTableTransformForm({ config, onChange, upstreamNames, target, onTargetChange }: TempTableTransformFormProps) {
  const update = (key: string, value: unknown) => onChange({ [key]: value })

  return (
    <div className="space-y-4">
      <FormField label="Target view" description="Temporary view name (referenced by downstream actions)">
        <Input
          value={target}
          onChange={(e) => onTargetChange(e.target.value)}
          placeholder="temp_my_intermediate"
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

      <FormField label="SQL" description="Optional SQL query for the temp table (if not just passing through source)">
        <Textarea
          value={(config.sql as string) ?? ''}
          onChange={(e) => update('sql', e.target.value || undefined)}
          placeholder="SELECT * FROM source_view WHERE ..."
          rows={4}
          className="font-mono text-xs"
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
