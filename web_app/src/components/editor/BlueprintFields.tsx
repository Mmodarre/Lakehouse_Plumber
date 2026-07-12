import { Input } from '../ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../ui/select'

// ── BlueprintFields — blueprint picker + instance file name ──
//
// A blueprint expands into several flowgroups across pipelines, so an instance
// has no pipeline/flowgroup of its own — only a blueprint reference and an
// instance-file name.

export interface BlueprintFieldsProps {
  blueprints: string[]
  blueprint: string
  onBlueprint: (name: string) => void
  instanceName: string
  onInstanceName: (name: string) => void
  instanceNameError: string | null
  disabled?: boolean
}

export function BlueprintFields({
  blueprints,
  blueprint,
  onBlueprint,
  instanceName,
  onInstanceName,
  instanceNameError,
  disabled,
}: BlueprintFieldsProps) {
  return (
    <div className="space-y-4">
      <div>
        <label className="mb-1 block text-xs font-medium text-muted-foreground">Blueprint</label>
        <Select value={blueprint} onValueChange={onBlueprint} disabled={disabled}>
          <SelectTrigger size="sm" className="w-full">
            <SelectValue placeholder={blueprints.length ? 'Select blueprint' : 'No blueprints found'} />
          </SelectTrigger>
          <SelectContent>
            {blueprints.map((b) => (
              <SelectItem key={b} value={b}>
                {b}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div>
        <label className="mb-1 block text-xs font-medium text-muted-foreground">
          Instance file name
        </label>
        <Input
          value={instanceName}
          onChange={(e) => onInstanceName(e.target.value)}
          placeholder="e.g. site1_end_to_end"
          className="h-8 w-full"
          disabled={disabled}
          aria-invalid={instanceNameError !== null}
        />
        <p className="mt-1 text-2xs text-muted-foreground">
          A blueprint expands into several flowgroups across pipelines; this names the instance
          file only.
        </p>
        {instanceNameError && <p className="mt-1 text-xs text-destructive">{instanceNameError}</p>}
      </div>
    </div>
  )
}
