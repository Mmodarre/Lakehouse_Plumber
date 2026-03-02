import { useMemo } from 'react'
import { Trash2 } from 'lucide-react'
import { Sheet, SheetContent, SheetHeader, SheetTitle } from '@/components/ui/sheet'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Separator } from '@/components/ui/separator'
import { useBuilderStore } from '../hooks/useBuilderStore'
import { ACTION_CATALOG, ACTION_TYPE_COLORS } from '../hooks/useActionCatalog'
import { ActionFormRouter } from './ActionFormRouter'
import { YAMLToggle } from './YAMLToggle'
import { GenericYAMLActionForm } from './forms/GenericYAMLActionForm'
import { ExtrasDisplay } from './forms/shared/ExtrasDisplay'

export function ActionConfigDrawer() {
  const {
    selectedNodeId,
    setSelectedNode,
    actionConfigs,
    updateActionConfig,
    removeNode,
    edges,
  } = useBuilderStore()

  const config = selectedNodeId ? actionConfigs.get(selectedNodeId) : undefined
  const catalogEntry = config
    ? ACTION_CATALOG.find((e) => e.type === config.actionType && e.subtype === config.actionSubtype)
    : undefined

  // Find upstream target view names (nodes that have edges pointing to this node)
  const upstreamNames = useMemo(() => {
    if (!selectedNodeId) return []
    return edges
      .filter((e) => e.target === selectedNodeId)
      .map((e) => {
        const sourceConfig = actionConfigs.get(e.source)
        return sourceConfig?.target || sourceConfig?.actionName || e.source
      })
  }, [selectedNodeId, edges, actionConfigs])

  const handleClose = () => setSelectedNode(null)

  const handleDelete = () => {
    if (selectedNodeId) {
      removeNode(selectedNodeId)
    }
  }

  const handleConfigChange = (updates: Partial<Record<string, unknown>>) => {
    if (!selectedNodeId || !config) return
    updateActionConfig(selectedNodeId, {
      config: { ...config.config, ...updates },
    })
  }

  const handleTargetChange = (target: string) => {
    if (!selectedNodeId || !config) return
    updateActionConfig(selectedNodeId, { target })
  }

  const handleToggleYAML = () => {
    if (!selectedNodeId || !config) return
    updateActionConfig(selectedNodeId, { isYAMLMode: !config.isYAMLMode })
  }

  const colors = config ? ACTION_TYPE_COLORS[config.actionType] : undefined

  return (
    <Sheet open={!!config} onOpenChange={(open) => { if (!open) handleClose() }}>
      <SheetContent className="flex w-[400px] flex-col p-0 sm:max-w-[400px]">
        {config && colors && (
          <>
            <SheetHeader className="space-y-3 px-6 pt-6">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <Badge variant="outline" className={colors.text}>
                    {catalogEntry?.label ?? config.actionSubtype}
                  </Badge>
                  <YAMLToggle
                    isYAMLMode={config.isYAMLMode}
                    onToggle={handleToggleYAML}
                    hasMVPForm={catalogEntry?.hasMVPForm ?? false}
                  />
                </div>
                <Button variant="ghost" size="icon" className="h-8 w-8 text-red-500" onClick={handleDelete}>
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
              <SheetTitle className="sr-only">Configure {config.actionName}</SheetTitle>
              <div className="space-y-1.5">
                <Label>Action name</Label>
                <Input
                  value={config.actionName}
                  onChange={(e) =>
                    updateActionConfig(selectedNodeId!, { actionName: e.target.value })
                  }
                  placeholder="Action name"
                />
              </div>
            </SheetHeader>

            <Separator className="my-3" />

            <ScrollArea className="flex-1 px-6 pb-6">
              {config.isYAMLMode ? (
                <GenericYAMLActionForm config={config} />
              ) : (
                <>
                  <ActionFormRouter
                    config={config}
                    onChange={handleConfigChange}
                    upstreamNames={upstreamNames}
                    target={config.target}
                    onTargetChange={handleTargetChange}
                  />
                  <ExtrasDisplay extras={config._extras} />
                </>
              )}
            </ScrollArea>
          </>
        )}
      </SheetContent>
    </Sheet>
  )
}
