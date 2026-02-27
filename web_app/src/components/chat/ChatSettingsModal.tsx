/** Modal for configuring AI provider and model — backed by server config. */

import { useEffect, useState } from 'react'
import { useChatStore } from '../../store/chatStore'
import { fetchAIConfig, updateAIConfig, type AIConfigSummary } from '../../api/ai'

export function ChatSettingsModal({ open, onClose }: { open: boolean; onClose: () => void }) {
  const { selectedProvider, selectedModel, setProvider } = useChatStore()
  const [provider, setLocalProvider] = useState(selectedProvider)
  const [model, setLocalModel] = useState(selectedModel)
  const [config, setConfig] = useState<AIConfigSummary | null>(null)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Fetch allowed models from backend on open
  useEffect(() => {
    if (!open) return
    setError(null)
    fetchAIConfig()
      .then((c) => {
        setConfig(c)
        setLocalProvider(c.provider)
        setLocalModel(c.model)
      })
      .catch(() => setError('Failed to load AI config'))
  }, [open])

  if (!open) return null

  const allowedModels = config?.allowed_models ?? {}
  const providers = Object.keys(allowedModels)
  const modelsForProvider = allowedModels[provider] ?? []

  async function handleSave() {
    setSaving(true)
    setError(null)
    try {
      const updated = await updateAIConfig(provider, model)
      setProvider(updated.provider, updated.model)
      onClose()
    } catch {
      setError('Failed to update AI config')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30" onClick={onClose}>
      <div
        className="w-80 rounded-lg border border-slate-200 bg-white shadow-lg"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="border-b border-slate-200 px-4 py-3">
          <h3 className="text-xs font-semibold text-slate-800">AI Settings</h3>
          <p className="mt-0.5 text-[10px] text-slate-400">
            Configure your preferred LLM provider and model
          </p>
        </div>

        <div className="space-y-3 px-4 py-3">
          {/* Provider */}
          <div>
            <label className="mb-1 block text-[10px] font-medium text-slate-600">Provider</label>
            <select
              value={provider}
              onChange={(e) => {
                const newProvider = e.target.value
                setLocalProvider(newProvider)
                const newModels = allowedModels[newProvider] ?? []
                setLocalModel(newModels[0] ?? '')
              }}
              className="w-full rounded border border-slate-200 px-2 py-1.5 text-[11px] focus:border-blue-400 focus:outline-none"
            >
              {providers.map((p) => (
                <option key={p} value={p}>{p}</option>
              ))}
            </select>
          </div>

          {/* Model */}
          <div>
            <label className="mb-1 block text-[10px] font-medium text-slate-600">Model</label>
            <select
              value={model}
              onChange={(e) => setLocalModel(e.target.value)}
              className="w-full rounded border border-slate-200 px-2 py-1.5 text-[11px] focus:border-blue-400 focus:outline-none"
            >
              {modelsForProvider.map((m) => (
                <option key={m} value={m}>{m}</option>
              ))}
            </select>
          </div>

          {error && (
            <p className="text-[10px] text-red-500">{error}</p>
          )}

          <p className="text-[9px] text-slate-400">
            Requires the provider's API key set in OpenCode config or environment variables.
          </p>
        </div>

        <div className="flex justify-end gap-2 border-t border-slate-200 px-4 py-2.5">
          <button
            onClick={onClose}
            className="rounded px-3 py-1 text-[11px] text-slate-500 hover:bg-slate-50"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={saving}
            className="rounded bg-slate-800 px-3 py-1 text-[11px] font-medium text-white hover:bg-slate-700 disabled:opacity-50"
          >
            {saving ? 'Saving...' : 'Save'}
          </button>
        </div>
      </div>
    </div>
  )
}
