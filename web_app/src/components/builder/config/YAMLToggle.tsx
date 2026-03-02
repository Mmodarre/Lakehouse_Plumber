import { Button } from '@/components/ui/button'
import { Code, FormInput } from 'lucide-react'

interface YAMLToggleProps {
  isYAMLMode: boolean
  onToggle: () => void
  hasMVPForm: boolean
}

export function YAMLToggle({ isYAMLMode, onToggle, hasMVPForm }: YAMLToggleProps) {
  if (!hasMVPForm) return null

  return (
    <Button
      variant="ghost"
      size="sm"
      onClick={onToggle}
      className="gap-1.5 text-xs"
    >
      {isYAMLMode ? (
        <>
          <FormInput className="h-3 w-3" />
          Form
        </>
      ) : (
        <>
          <Code className="h-3 w-3" />
          YAML
        </>
      )}
    </Button>
  )
}
