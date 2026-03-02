import { FileText, Workflow } from 'lucide-react'
import { Card, CardHeader, CardTitle, CardDescription } from '@/components/ui/card'
import { useBuilderStore } from '../hooks/useBuilderStore'
import type { BuilderPath } from '../types/builder'

interface PathCardProps {
  title: string
  description: string
  icon: React.ReactNode
  selected: boolean
  onClick: () => void
}

function PathCard({ title, description, icon, selected, onClick }: PathCardProps) {
  return (
    <Card
      className={`cursor-pointer transition-all hover:border-blue-300 hover:shadow-md ${
        selected ? 'border-blue-500 ring-2 ring-blue-200' : ''
      }`}
      onClick={onClick}
    >
      <CardHeader className="flex flex-col items-center gap-3 p-8 text-center">
        <div
          className={`rounded-lg p-3 ${
            selected ? 'bg-blue-100 text-blue-600' : 'bg-slate-100 text-slate-500'
          }`}
        >
          {icon}
        </div>
        <CardTitle className="text-base">{title}</CardTitle>
        <CardDescription className="text-sm">{description}</CardDescription>
      </CardHeader>
    </Card>
  )
}

export default function ChoosePathStep() {
  const { chosenPath, setPath, setStep } = useBuilderStore()

  const handleSelect = (path: BuilderPath) => {
    setPath(path)
    // Auto-advance on double-click or re-click
    if (path === chosenPath) {
      setStep(path === 'template' ? 'template-wizard' : 'flow-canvas')
    }
  }

  return (
    <div className="mx-auto flex w-full max-w-2xl flex-col items-center justify-center gap-8 p-8">
      <div className="text-center">
        <h2 className="text-lg font-semibold text-slate-800">How do you want to build?</h2>
        <p className="text-sm text-slate-500">
          Choose a starting point for your flowgroup.
        </p>
      </div>

      <div className="grid w-full grid-cols-2 gap-6">
        <PathCard
          title="Use Template"
          description="Start from a pre-built template and fill in parameters. Best for common patterns like CloudFiles-to-Table."
          icon={<FileText className="h-8 w-8" />}
          selected={chosenPath === 'template'}
          onClick={() => handleSelect('template')}
        />
        <PathCard
          title="Build from Scratch"
          description="Design your flowgroup visually with drag-and-drop actions. Full control over every action and connection."
          icon={<Workflow className="h-8 w-8" />}
          selected={chosenPath === 'canvas'}
          onClick={() => handleSelect('canvas')}
        />
      </div>
    </div>
  )
}
