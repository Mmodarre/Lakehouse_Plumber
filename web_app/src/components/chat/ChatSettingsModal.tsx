/** Modal for configuring AI provider and model. */

import { useState } from 'react'
import { useChatStore } from '../../store/chatStore'

const providers = [
  { id: 'anthropic', name: 'Anthropic', models: ['claude-sonnet-4-20250514', 'claude-haiku-4-5-20251001', 'claude-opus-4-20250514'] },
  { id: 'openai', name: 'OpenAI', models: ['gpt-4o', 'gpt-4o-mini', 'o3-mini'] },
  { id: 'google', name: 'Google', models: ['gemini-2.5-pro', 'gemini-2.5-flash'] },
]

export function ChatSettingsModal({ open, onClose }: { open: boolean; onClose: () => void }) {
  const { selectedProvider, selectedModel, setProvider } = useChatStore()
  const [provider, setLocalProvider] = useState(selectedProvider)
  const [model, setLocalModel] = useState(selectedModel)

  if (!open) return null

  const providerConfig = providers.find((p) => p.id === provider)
  const availableModels = providerConfig?.models ?? []

  function handleSave() {
    setProvider(provider, model)
    onClose()
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
                setLocalProvider(e.target.value)
                // Reset model to first available for new provider
                const newModels = providers.find((p) => p.id === e.target.value)?.models ?? []
                setLocalModel(newModels[0] ?? '')
              }}
              className="w-full rounded border border-slate-200 px-2 py-1.5 text-[11px] focus:border-blue-400 focus:outline-none"
            >
              {providers.map((p) => (
                <option key={p.id} value={p.id}>{p.name}</option>
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
              {availableModels.map((m) => (
                <option key={m} value={m}>{m}</option>
              ))}
            </select>
          </div>

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
            className="rounded bg-slate-800 px-3 py-1 text-[11px] font-medium text-white hover:bg-slate-700"
          >
            Save
          </button>
        </div>
      </div>
    </div>
  )
}
