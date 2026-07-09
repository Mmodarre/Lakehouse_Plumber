import { useState } from 'react'
import { Loader2, Settings2 } from 'lucide-react'
import { Button } from '../ui/button'
import { Input } from '../ui/input'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../ui/select'
import {
  useDatabricksProfiles,
  useUpdateExecutorConfig,
} from '../../hooks/useAssistant'
import type { ExecutorConfig, ExecutorMode } from '../../types/assistant'

// Mirrors the backend's env-var NAME validation (schemas/assistant.py):
// POSIX identifier only — pasted key material never passes.
const ENV_VAR_NAME_RE = /^[A-Za-z_][A-Za-z0-9_]*$/

const MODE_LABELS: Record<ExecutorMode, string> = {
  omnigent_defaults: 'omnigent defaults',
  databricks: 'Databricks profile',
  api_key_env: 'API key from environment variable',
}

/**
 * Executor choice card. First use: shown until a config is stored, blocking
 * the composer (no `initial`, no `onDone`). Reconfigure: opened from the
 * panel's gear button with the stored config prefilled; saving (or Cancel)
 * returns to the chat. Saving PUTs the config — the backend marks the
 * active session stale, so the next message reprovisions the agent.
 */
export function SetupCard({
  initial,
  onDone,
}: {
  initial?: ExecutorConfig | null
  onDone?: () => void
}) {
  const [mode, setMode] = useState<ExecutorMode>(initial?.mode ?? 'omnigent_defaults')
  const [profile, setProfile] = useState(initial?.profile ?? '')
  const [model, setModel] = useState(initial?.model ?? '')
  const [apiKeyEnv, setApiKeyEnv] = useState(initial?.api_key_env ?? '')

  const profiles = useDatabricksProfiles({ enabled: mode === 'databricks' })
  const update = useUpdateExecutorConfig()

  const valid =
    mode === 'omnigent_defaults' ||
    (mode === 'databricks' && profile !== '') ||
    (mode === 'api_key_env' && ENV_VAR_NAME_RE.test(apiKeyEnv))

  const save = () => {
    update.mutate(
      {
        mode,
        profile: mode === 'databricks' ? profile : null,
        model: mode !== 'omnigent_defaults' && model !== '' ? model : null,
        api_key_env: mode === 'api_key_env' ? apiKeyEnv : null,
      },
      { onSuccess: () => onDone?.() },
    )
  }

  return (
    <div className="p-3 text-xs text-muted-foreground">
      <div className="mb-1.5 flex items-center gap-1.5 text-sm font-medium text-foreground">
        <Settings2 className="size-4" aria-hidden="true" />
        Choose how the assistant runs
      </div>
      <p>
        One-time setup: pick the model executor the assistant uses. You can
        change it later from this panel.
      </p>

      <label className="mt-3 block font-medium text-foreground" htmlFor="assistant-mode">
        Executor
      </label>
      <Select value={mode} onValueChange={(v) => setMode(v as ExecutorMode)}>
        <SelectTrigger
          id="assistant-mode"
          size="sm"
          aria-label="Executor mode"
          className="mt-1 w-full"
        >
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          {(Object.keys(MODE_LABELS) as ExecutorMode[]).map((m) => (
            <SelectItem key={m} value={m}>
              {MODE_LABELS[m]}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      {mode === 'databricks' && (
        <>
          <label
            className="mt-3 block font-medium text-foreground"
            htmlFor="assistant-profile"
          >
            Databricks profile
          </label>
          <Select value={profile} onValueChange={setProfile}>
            <SelectTrigger
              id="assistant-profile"
              size="sm"
              aria-label="Databricks profile"
              className="mt-1 w-full"
            >
              <SelectValue
                placeholder={
                  profiles.data && profiles.data.profiles.length === 0
                    ? 'No profiles in ~/.databrickscfg'
                    : 'Select a profile'
                }
              />
            </SelectTrigger>
            <SelectContent>
              {(profiles.data?.profiles ?? []).map((p) => (
                <SelectItem key={p} value={p}>
                  {p}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </>
      )}

      {mode === 'api_key_env' && (
        <>
          <label
            className="mt-3 block font-medium text-foreground"
            htmlFor="assistant-api-key-env"
          >
            Environment variable name
          </label>
          <Input
            id="assistant-api-key-env"
            value={apiKeyEnv}
            onChange={(e) => setApiKeyEnv(e.target.value)}
            placeholder="e.g. ANTHROPIC_API_KEY"
            aria-invalid={apiKeyEnv !== '' && !ENV_VAR_NAME_RE.test(apiKeyEnv)}
            className="mt-1 h-8"
          />
          <p className="mt-1">
            The NAME of an environment variable on the omnigent server —
            never paste the key itself.
          </p>
        </>
      )}

      {mode !== 'omnigent_defaults' && (
        <>
          <label
            className="mt-3 block font-medium text-foreground"
            htmlFor="assistant-model"
          >
            Model <span className="font-normal text-muted-foreground">(optional)</span>
          </label>
          <Input
            id="assistant-model"
            value={model}
            onChange={(e) => setModel(e.target.value)}
            placeholder="gateway model override"
            className="mt-1 h-8"
          />
        </>
      )}

      <div className="mt-3 flex items-center gap-2">
        <Button size="sm" onClick={save} disabled={!valid || update.isPending}>
          {update.isPending && (
            <Loader2 className="animate-spin" aria-hidden="true" />
          )}
          {initial ? 'Save' : 'Save and start chatting'}
        </Button>
        {onDone && (
          <Button
            size="sm"
            variant="ghost"
            onClick={onDone}
            disabled={update.isPending}
          >
            Cancel
          </Button>
        )}
      </div>
      {update.isError && (
        <p className="mt-1.5 text-error">
          Could not save the configuration: {(update.error as Error).message}
        </p>
      )}
    </div>
  )
}
