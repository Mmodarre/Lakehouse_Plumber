import { useCallback, useEffect, lazy, Suspense } from 'react'
import { useUIStore } from '@/store/uiStore'
import { useBuilderStore } from './hooks/useBuilderStore'
import { WizardHeader } from './WizardHeader'
import type { WizardStep, BuilderPath } from './types/builder'

const BasicInfoStep = lazy(() => import('./steps/BasicInfoStep'))
const ChoosePathStep = lazy(() => import('./steps/ChoosePathStep'))
const TemplateWizardStep = lazy(() => import('./steps/TemplateWizardStep'))
const FlowCanvasStep = lazy(() => import('./steps/FlowCanvasStep'))
const PreviewSaveStep = lazy(() => import('./steps/PreviewSaveStep'))

function getSteps(path: BuilderPath): WizardStep[] {
  const base: WizardStep[] = ['basic-info', 'choose-path']
  if (path === 'template') return [...base, 'template-wizard', 'preview-save']
  if (path === 'canvas') return [...base, 'flow-canvas', 'preview-save']
  return base
}

function StepFallback() {
  return (
    <div className="flex flex-1 items-center justify-center">
      <div className="h-6 w-6 animate-spin rounded-full border-2 border-slate-300 border-t-slate-600" />
    </div>
  )
}

export function FlowgroupBuilderWizard() {
  const open = useUIStore((s) => s.flowgroupBuilderOpen)
  const close = useUIStore((s) => s.closeFlowgroupBuilder)
  const { currentStep, chosenPath, setStep, reset, hasData } = useBuilderStore()

  const handleClose = useCallback(() => {
    if (hasData()) {
      if (!confirm('You have unsaved changes. Close the builder?')) return
    }
    reset()
    close()
  }, [hasData, reset, close])

  // Escape to close
  useEffect(() => {
    if (!open) return
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') handleClose()
    }
    document.addEventListener('keydown', handler)
    return () => document.removeEventListener('keydown', handler)
  }, [open, handleClose])

  // Reset store when opening
  useEffect(() => {
    if (open) reset()
  }, [open, reset])

  const steps = getSteps(chosenPath)
  const currentIndex = steps.indexOf(currentStep)

  const canGoNext = useCallback((): boolean => {
    const state = useBuilderStore.getState()
    switch (currentStep) {
      case 'basic-info': {
        const { basicInfo } = state
        const pipe = basicInfo.isNewPipeline ? basicInfo.newPipeline : basicInfo.pipeline
        return pipe !== '' && basicInfo.flowgroupName !== ''
      }
      case 'choose-path':
        return state.chosenPath !== null
      case 'template-wizard':
        return state.templateInfo.templateName !== ''
      case 'flow-canvas':
        return state.nodes.length > 0
      default:
        return true
    }
  }, [currentStep])

  const handleBack = useCallback(() => {
    if (currentIndex > 0) {
      setStep(steps[currentIndex - 1])
    }
  }, [currentIndex, steps, setStep])

  const handleNext = useCallback(() => {
    if (currentIndex < steps.length - 1) {
      setStep(steps[currentIndex + 1])
    }
  }, [currentIndex, steps, setStep])

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex flex-col bg-white">
      <WizardHeader
        currentStep={currentStep}
        chosenPath={chosenPath}
        canGoNext={canGoNext()}
        onBack={handleBack}
        onNext={handleNext}
        onClose={handleClose}
      />

      <div className="flex flex-1 overflow-hidden">
        <Suspense fallback={<StepFallback />}>
          {currentStep === 'basic-info' && <BasicInfoStep />}
          {currentStep === 'choose-path' && <ChoosePathStep />}
          {currentStep === 'template-wizard' && <TemplateWizardStep />}
          {currentStep === 'flow-canvas' && <FlowCanvasStep />}
          {currentStep === 'preview-save' && <PreviewSaveStep />}
        </Suspense>
      </div>
    </div>
  )
}
