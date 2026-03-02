import { Label } from '@/components/ui/label'

interface FormFieldProps {
  label: string
  required?: boolean
  description?: string
  error?: string
  children: React.ReactNode
}

export function FormField({ label, required, description, error, children }: FormFieldProps) {
  return (
    <div className="space-y-1.5">
      <Label className="flex items-center gap-1">
        {label}
        {required && <span className="text-red-500">*</span>}
      </Label>
      {description && <p className="text-xs text-slate-400">{description}</p>}
      {children}
      {error && <p className="text-xs text-red-500">{error}</p>}
    </div>
  )
}
