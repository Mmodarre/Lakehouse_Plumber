import { ScrollArea } from '@/components/ui/scroll-area'
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from '@/components/ui/accordion'
import { getCatalogByType } from '../hooks/useActionCatalog'
import { PaletteItem } from './PaletteItem'

const SECTIONS = [
  { type: 'load', label: 'Load' },
  { type: 'transform', label: 'Transform' },
  { type: 'write', label: 'Write' },
] as const

export function ActionPalette() {
  return (
    <div className="flex h-full w-56 flex-col border-r border-slate-200 bg-white">
      <div className="border-b border-slate-200 px-4 py-3">
        <span className="text-xs font-semibold uppercase tracking-wider text-slate-500">
          Actions
        </span>
        <p className="mt-0.5 text-[10px] text-slate-400">Drag onto canvas</p>
      </div>
      <ScrollArea className="flex-1">
        <Accordion type="multiple" defaultValue={['load', 'transform', 'write']}>
          {SECTIONS.map(({ type, label }) => (
            <AccordionItem key={type} value={type} className="border-none px-3">
              <AccordionTrigger className="py-2 text-xs font-semibold uppercase tracking-wider text-slate-500 hover:no-underline">
                {label}
              </AccordionTrigger>
              <AccordionContent>
                <div className="space-y-2 pb-2">
                  {getCatalogByType(type).map((entry) => (
                    <PaletteItem key={entry.subtype} entry={entry} />
                  ))}
                </div>
              </AccordionContent>
            </AccordionItem>
          ))}
        </Accordion>
      </ScrollArea>
    </div>
  )
}
