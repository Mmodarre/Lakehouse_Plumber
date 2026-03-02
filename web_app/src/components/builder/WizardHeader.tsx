import { ChevronLeft, ChevronRight, X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import type { WizardStep, BuilderPath } from './types/builder'

const STEP_LABELS: Record<WizardStep, string> = {
  'basic-info': 'Basic Info',
  'choose-path': 'Choose Path',
  'template-wizard': 'Template',
  'flow-canvas': 'Canvas',
  'preview-save': 'Preview & Save',
}

function getSteps(path: BuilderPath): WizardStep[] {
  const base: WizardStep[] = ['basic-info', 'choose-path']
  if (path === 'template') return [...base, 'template-wizard', 'preview-save']
  if (path === 'canvas') return [...base, 'flow-canvas', 'preview-save']
  return base
}

interface WizardHeaderProps {
  currentStep: WizardStep
  chosenPath: BuilderPath
  canGoNext: boolean
  onBack: () => void
  onNext: () => void
  onClose: () => void
}

export function WizardHeader({
  currentStep,
  chosenPath,
  canGoNext,
  onBack,
  onNext,
  onClose,
}: WizardHeaderProps) {
  const steps = getSteps(chosenPath)
  const currentIndex = steps.indexOf(currentStep)
  const isFirst = currentIndex <= 0
  const isLast = currentIndex === steps.length - 1

  return (
    <div className="flex items-center justify-between border-b border-slate-200 bg-white px-6 py-3">
      {/* Breadcrumb */}
      <div className="flex items-center gap-1">
        {steps.map((step, i) => (
          <div key={step} className="flex items-center">
            {i > 0 && <ChevronRight className="mx-1 h-3 w-3 text-slate-300" />}
            <span
              className={`text-xs font-medium ${
                step === currentStep
                  ? 'text-blue-600'
                  : i < currentIndex
                    ? 'text-slate-600'
                    : 'text-slate-400'
              }`}
            >
              {STEP_LABELS[step]}
            </span>
          </div>
        ))}
      </div>

      {/* Actions */}
      <div className="flex items-center gap-2">
        {!isFirst && (
          <Button variant="ghost" size="sm" onClick={onBack}>
            <ChevronLeft className="mr-1 h-3 w-3" />
            Back
          </Button>
        )}
        {!isLast && (
          <Button size="sm" onClick={onNext} disabled={!canGoNext}>
            Next
            <ChevronRight className="ml-1 h-3 w-3" />
          </Button>
        )}
        <Button variant="ghost" size="icon" onClick={onClose} className="ml-2 h-8 w-8">
          <X className="h-4 w-4" />
        </Button>
      </div>
    </div>
  )
}
