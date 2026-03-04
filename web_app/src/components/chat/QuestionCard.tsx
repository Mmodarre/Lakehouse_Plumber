/** Interactive question card for AskUserQuestion tool calls.
 *
 * Renders structured questions with clickable options instead of raw JSON.
 * On submit, replies via OpenCode's dedicated question API so the LLM
 * receives the answer and continues processing.
 *
 * Falls back to ToolCallCard rendering when the question format is unexpected.
 */

import { useState, useCallback } from 'react'
import { replyToQuestion } from '../../api/ai'
import { useChatStore } from '../../store/chatStore'
import type { ChatMessagePart, ToolState } from '../../types/chat'

// ── Types ────────────────────────────────────────────────────

interface QuestionOption {
  label: string
  description?: string
}

interface Question {
  question: string
  header?: string
  options: QuestionOption[]
  multiSelect?: boolean
  custom?: boolean
}

// ── Helpers ──────────────────────────────────────────────────

/** Try to extract a questions array from tool args. Returns null if format is unexpected. */
export function parseQuestions(toolArgs?: Record<string, unknown>): Question[] | null {
  if (!toolArgs) return null
  const raw = toolArgs.questions
  if (!Array.isArray(raw) || raw.length === 0) return null

  const questions: Question[] = []
  for (const q of raw) {
    if (typeof q !== 'object' || q === null) return null
    const obj = q as Record<string, unknown>
    if (typeof obj.question !== 'string') return null
    if (!Array.isArray(obj.options) || obj.options.length === 0) return null

    const options: QuestionOption[] = []
    for (const opt of obj.options) {
      if (typeof opt === 'string') {
        options.push({ label: opt })
      } else if (typeof opt === 'object' && opt !== null) {
        const o = opt as Record<string, unknown>
        if (typeof o.label !== 'string') return null
        options.push({
          label: o.label,
          description: typeof o.description === 'string' ? o.description : undefined,
        })
      } else {
        return null
      }
    }

    // Accept both `multiple` (SSE wire format) and `multiSelect` (tool args format)
    const multi = typeof obj.multiple === 'boolean' ? obj.multiple
      : typeof obj.multiSelect === 'boolean' ? obj.multiSelect
      : false

    // `custom` defaults to true per OpenCode SDK spec (allows free-text "Other")
    const custom = typeof obj.custom === 'boolean' ? obj.custom : true

    questions.push({
      question: obj.question,
      header: typeof obj.header === 'string' ? obj.header : undefined,
      options,
      multiSelect: multi,
      custom,
    })
  }

  return questions
}

/** Check whether a tool name represents a question-type tool. */
export function isQuestionTool(toolName?: string): boolean {
  if (!toolName) return false
  const lower = toolName.toLowerCase()
  return lower === 'question' || lower === 'askuserquestion' || lower === 'ask_user_question'
}

// ── Status styling ──────────────────────────────────────────

function statusBorder(state: ToolState): string {
  if (state === 'pending' || state === 'running') return 'border-blue-200'
  if (state === 'error') return 'border-red-200'
  return 'border-slate-200'
}

// ── Component ───────────────────────────────────────────────

export function QuestionCard({ part }: { part: ChatMessagePart }) {
  const state: ToolState = part.toolState ?? 'completed'
  const questions = parseQuestions(part.toolArgs)
  const [submitted, setSubmitted] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [submitError, setSubmitError] = useState<string | null>(null)

  // selections[questionIndex] → Set of selected option indices
  const [selections, setSelections] = useState<Map<number, Set<number>>>(new Map())
  // customText[questionIndex] → free-text input for "Other" option
  const [customText, setCustomText] = useState<Map<number, string>>(new Map())

  // Read the question request ID from the store (mapped by sessionId)
  const requestId = useChatStore(
    (s) => (s.activeSessionId ? s.pendingQuestionIds[s.activeSessionId] : undefined),
  )

  const toggle = useCallback((questionIdx: number, optionIdx: number, multiSelect: boolean) => {
    setSelections((prev) => {
      const next = new Map(prev)
      const current = new Set(next.get(questionIdx) ?? [])
      if (multiSelect) {
        if (current.has(optionIdx)) current.delete(optionIdx)
        else current.add(optionIdx)
      } else {
        current.clear()
        current.add(optionIdx)
      }
      next.set(questionIdx, current)
      return next
    })
    // In single-select mode, clear custom text when an option is picked
    if (!multiSelect) {
      setCustomText((prev) => {
        const next = new Map(prev)
        next.delete(questionIdx)
        return next
      })
    }
  }, [])

  const updateCustomText = useCallback((questionIdx: number, text: string) => {
    setCustomText((prev) => {
      const next = new Map(prev)
      if (text) next.set(questionIdx, text)
      else next.delete(questionIdx)
      return next
    })
    // In single-select mode, deselect options when typing custom text
    // (handled by SingleQuestion clearing selection on focus)
  }, [])

  const handleSubmit = useCallback(async () => {
    if (!questions || submitted || submitting) return

    // Build answers in OpenCode's format: string[][] (parallel to questions[])
    const answers: string[][] = []
    for (let qi = 0; qi < questions.length; qi++) {
      const sel = selections.get(qi)
      const custom = customText.get(qi)?.trim()
      const chosen = sel && sel.size > 0
        ? [...sel].map((i) => questions[qi].options[i]?.label).filter(Boolean)
        : []

      if (questions[qi].multiSelect) {
        // Multi-select: append custom text alongside selected options
        if (custom) chosen.push(custom)
        answers.push(chosen)
      } else {
        // Single-select: custom text replaces selected option
        answers.push(custom ? [custom] : chosen)
      }
    }

    if (answers.every((a) => a.length === 0)) return

    setSubmitting(true)
    setSubmitError(null)

    const { activeSessionId } = useChatStore.getState()

    if (!requestId || !activeSessionId) {
      setSubmitError('Waiting for question handshake... try again in a moment.')
      setSubmitting(false)
      return
    }

    try {
      await replyToQuestion(activeSessionId, requestId, answers)
      setSubmitted(true)
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Unknown error'
      setSubmitError(`Failed to send answer: ${msg}`)
    } finally {
      setSubmitting(false)
    }
  }, [questions, selections, customText, submitted, submitting, requestId])

  // Check if at least one option is selected (or custom text entered) per question
  const allAnswered = questions
    ? questions.every((_, qi) => {
        const sel = selections.get(qi)
        const custom = customText.get(qi)?.trim()
        return (sel && sel.size > 0) || !!custom
      })
    : false

  const isAnswerable = !submitted && !submitting && (state === 'pending' || state === 'running')

  // Fallback: if we can't parse the questions, show raw JSON in a simple card
  if (!questions) {
    return (
      <div className={`my-1 rounded border ${statusBorder(state)} bg-slate-50 text-[11px]`}>
        <div className="flex items-center gap-1.5 px-2 py-1.5">
          <QuestionIcon state={state} />
          <span className="font-medium text-slate-600">Question</span>
        </div>
        <div className="border-t border-slate-200 bg-white p-2">
          <pre className="max-h-48 overflow-auto whitespace-pre-wrap break-all font-mono text-[10px] text-slate-600">
            {JSON.stringify(part.toolArgs, null, 2)}
          </pre>
        </div>
      </div>
    )
  }

  return (
    <div className={`my-1 rounded border ${statusBorder(state)} bg-gradient-to-b from-blue-50/40 to-white text-[11px]`}>
      <div className="flex items-center gap-1.5 px-2 py-1.5 border-b border-slate-100">
        <QuestionIcon state={state} />
        <span className="font-medium text-slate-600">Question</span>
        {submitted ? (
          <span className="ml-auto text-[10px] text-green-500 flex items-center gap-1">
            <svg className="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
            </svg>
            Answered
          </span>
        ) : isAnswerable ? (
          <span className="ml-auto text-[10px] text-blue-400">Waiting for answer...</span>
        ) : null}
      </div>

      <div className="p-2 space-y-3">
        {questions.map((q, qi) => (
          <SingleQuestion
            key={qi}
            question={q}
            questionIdx={qi}
            selected={selections.get(qi) ?? new Set()}
            onToggle={toggle}
            customText={customText.get(qi) ?? ''}
            onCustomTextChange={updateCustomText}
            onCustomTextFocus={() => {
              // In single-select: deselect options when user starts typing
              if (!q.multiSelect) {
                setSelections((prev) => {
                  const next = new Map(prev)
                  next.delete(qi)
                  return next
                })
              }
            }}
            disabled={!isAnswerable}
          />
        ))}

        {/* Error message */}
        {submitError && (
          <p className="text-[10px] text-red-500 px-1">{submitError}</p>
        )}

        {/* Submit button */}
        {isAnswerable && (
          <button
            onClick={handleSubmit}
            disabled={!allAnswered || submitting || !requestId}
            className={`w-full rounded py-1.5 px-3 text-[11px] font-medium transition-colors ${
              allAnswered && !submitting && requestId
                ? 'bg-blue-500 text-white hover:bg-blue-600 active:bg-blue-700'
                : 'bg-slate-100 text-slate-400 cursor-not-allowed'
            }`}
          >
            {submitting ? 'Submitting...' : !requestId ? 'Connecting...' : 'Submit Answers'}
          </button>
        )}
      </div>
    </div>
  )
}

// ── Single question block ───────────────────────────────────

function SingleQuestion({
  question,
  questionIdx,
  selected,
  onToggle,
  customText,
  onCustomTextChange,
  onCustomTextFocus,
  disabled,
}: {
  question: Question
  questionIdx: number
  selected: Set<number>
  onToggle: (questionIdx: number, optionIdx: number, multiSelect: boolean) => void
  customText: string
  onCustomTextChange: (questionIdx: number, text: string) => void
  onCustomTextFocus: () => void
  disabled: boolean
}) {
  const [showCustom, setShowCustom] = useState(false)

  // Mode label
  const modeLabel = question.multiSelect
    ? 'Select all that apply'
    : 'Select one'

  return (
    <div>
      {question.header && (
        <span className="inline-block mb-1 rounded bg-slate-100 px-1.5 py-0.5 text-[10px] font-medium text-slate-500 uppercase tracking-wide">
          {question.header}
        </span>
      )}
      <p className="text-[12px] font-medium text-slate-700 mb-0.5">{question.question}</p>
      <p className="text-[10px] text-slate-400 mb-1.5">{modeLabel}</p>
      <div className="space-y-1">
        {question.options.map((opt, oi) => {
          const isSelected = selected.has(oi)
          return (
            <button
              key={oi}
              onClick={() => onToggle(questionIdx, oi, !!question.multiSelect)}
              disabled={disabled}
              className={`w-full text-left rounded px-2 py-1.5 transition-colors ${
                isSelected
                  ? 'bg-blue-50 ring-1 ring-blue-300 text-blue-700'
                  : !disabled
                    ? 'bg-white ring-1 ring-slate-200 hover:bg-slate-50 text-slate-600'
                    : 'bg-white ring-1 ring-slate-200 text-slate-400'
              }`}
            >
              <div className="flex items-start gap-1.5">
                <span className={`mt-0.5 shrink-0 h-3 w-3 rounded-${question.multiSelect ? 'sm' : 'full'} border ${
                  isSelected
                    ? 'border-blue-400 bg-blue-400'
                    : 'border-slate-300 bg-white'
                } flex items-center justify-center`}>
                  {isSelected && (
                    <svg className="h-2 w-2 text-white" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                    </svg>
                  )}
                </span>
                <div className="min-w-0">
                  <span className="font-medium text-[11px]">{opt.label}</span>
                  {opt.description && (
                    <p className="text-[10px] text-slate-400 mt-0.5 leading-snug">{opt.description}</p>
                  )}
                </div>
              </div>
            </button>
          )
        })}
      </div>

      {/* Free-text "Other" input when custom is enabled */}
      {question.custom && !disabled && (
        <div className="mt-1.5">
          {!showCustom ? (
            <button
              onClick={() => setShowCustom(true)}
              className="text-[10px] text-blue-500 hover:text-blue-600 hover:underline"
            >
              Or type your own...
            </button>
          ) : (
            <div className="space-y-1">
              <textarea
                value={customText}
                onChange={(e) => onCustomTextChange(questionIdx, e.target.value)}
                onFocus={onCustomTextFocus}
                placeholder="Type your answer..."
                rows={2}
                className="w-full rounded border border-slate-200 bg-white px-2 py-1.5 text-[11px] text-slate-700 placeholder:text-slate-300 focus:border-blue-300 focus:outline-none focus:ring-1 focus:ring-blue-200 resize-y"
              />
              <button
                onClick={() => {
                  setShowCustom(false)
                  onCustomTextChange(questionIdx, '')
                }}
                className="text-[10px] text-slate-400 hover:text-slate-500"
              >
                Cancel
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ── Question icon ──────────────────────────────────────────

function QuestionIcon({ state }: { state: ToolState }) {
  const isRunning = state === 'pending' || state === 'running'
  const color = isRunning ? 'text-blue-500' : state === 'error' ? 'text-red-400' : 'text-slate-400'
  return (
    <svg className={`h-3.5 w-3.5 ${color} ${isRunning ? 'animate-pulse' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
      <path strokeLinecap="round" strokeLinejoin="round" d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
  )
}
