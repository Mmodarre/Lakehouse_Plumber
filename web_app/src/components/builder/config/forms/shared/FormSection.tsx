import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from '@/components/ui/accordion'

interface FormSectionProps {
  title: string
  defaultOpen?: boolean
  children: React.ReactNode
}

export function FormSection({ title, defaultOpen = false, children }: FormSectionProps) {
  return (
    <Accordion type="single" collapsible defaultValue={defaultOpen ? 'section' : undefined}>
      <AccordionItem value="section" className="border-none">
        <AccordionTrigger className="py-2 text-xs font-semibold uppercase tracking-wider text-slate-500 hover:no-underline">
          {title}
        </AccordionTrigger>
        <AccordionContent>
          <div className="space-y-4 pt-1">{children}</div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  )
}
