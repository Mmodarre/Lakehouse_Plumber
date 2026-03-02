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

interface DataQualityTransformFormProps {
  config: Record<string, unknown>
  onChange: (updates: Record<string, unknown>) => void
  upstreamNames: string[]
  target: string
  onTargetChange: (target: string) => void
}

export function DataQualityTransformForm({ config, onChange, upstreamNames, target, onTargetChange }: DataQualityTransformFormProps) {
  const update = (key: string, value: unknown) => onChange({ [key]: value })

  return (
    <div className="space-y-4">
      <FormField label="Target view" description="Temporary view name created by this transform">
        <Input
          value={target}
          onChange={(e) => onTargetChange(e.target.value)}
          placeholder="v_my_dq_check"
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

      <FormField label="Expectations file" required description="Path to the expectations YAML file">
        <Input
          value={(config.expectations_file as string) ?? ''}
          onChange={(e) => update('expectations_file', e.target.value)}
          placeholder="expectations/quality_rules.yaml"
        />
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
    </div>
  )
}
