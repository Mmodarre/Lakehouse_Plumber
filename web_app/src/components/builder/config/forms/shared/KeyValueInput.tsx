import { Plus, X } from 'lucide-react'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'

interface KeyValueInputProps {
  value: Record<string, string>
  onChange: (value: Record<string, string>) => void
  keyPlaceholder?: string
  valuePlaceholder?: string
}

export function KeyValueInput({
  value,
  onChange,
  keyPlaceholder = 'Key',
  valuePlaceholder = 'Value',
}: KeyValueInputProps) {
  const entries = Object.entries(value)

  const updateKey = (oldKey: string, newKey: string) => {
    const newValue = { ...value }
    const val = newValue[oldKey]
    delete newValue[oldKey]
    newValue[newKey] = val
    onChange(newValue)
  }

  const updateVal = (key: string, val: string) => {
    onChange({ ...value, [key]: val })
  }

  const addRow = () => {
    // Find a unique empty key
    let key = ''
    let i = 0
    while (key in value) {
      i++
      key = `key_${i}`
    }
    onChange({ ...value, [key]: '' })
  }

  const removeRow = (key: string) => {
    const newValue = { ...value }
    delete newValue[key]
    onChange(newValue)
  }

  return (
    <div className="space-y-2">
      {entries.map(([k, v], idx) => (
        <div key={idx} className="flex items-center gap-2">
          <Input
            value={k}
            onChange={(e) => updateKey(k, e.target.value)}
            placeholder={keyPlaceholder}
            className="flex-1"
          />
          <Input
            value={v}
            onChange={(e) => updateVal(k, e.target.value)}
            placeholder={valuePlaceholder}
            className="flex-1"
          />
          <Button variant="ghost" size="icon" className="h-8 w-8 shrink-0" onClick={() => removeRow(k)}>
            <X className="h-3 w-3" />
          </Button>
        </div>
      ))}
      <Button variant="outline" size="sm" onClick={addRow} className="gap-1">
        <Plus className="h-3 w-3" />
        Add
      </Button>
    </div>
  )
}
