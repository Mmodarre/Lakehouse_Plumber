import { FileBrowser } from '../sidebar/FileBrowser'
import { GitPanel } from '../sidebar/GitPanel'
import { useUIStore } from '../../store/uiStore'

export function Sidebar() {
  const sidebarOpen = useUIStore((s) => s.sidebarOpen)

  return (
    <aside
      className={`flex shrink-0 flex-col border-r border-slate-200 bg-white panel-transition ${
        sidebarOpen ? 'w-60' : 'w-0 overflow-hidden'
      }`}
    >
      <div className="flex min-h-0 flex-1 flex-col w-60">
        <div className="custom-scrollbar flex-1 overflow-y-auto">
          <FileBrowser />
        </div>
        <GitPanel />
      </div>
    </aside>
  )
}
