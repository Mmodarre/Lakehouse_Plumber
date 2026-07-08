import { Component, type ReactNode, type ErrorInfo } from 'react'
import { Button } from '../ui/button'

interface Props {
  children: ReactNode
  fallback?: ReactNode
  /** Called when the boundary resets (via the default "Try again" button or a
   * `resetKeys` change after a crash). */
  onReset?: () => void
  /** When any entry changes identity while the boundary is in its error state,
   * the boundary clears the error and re-renders `children`. */
  resetKeys?: unknown[]
}

interface State {
  hasError: boolean
  error: Error | null
}

function resetKeysChanged(prev: unknown[] = [], next: unknown[] = []): boolean {
  if (prev.length !== next.length) return true
  return prev.some((item, i) => !Object.is(item, next[i]))
}

export class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false, error: null }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('ErrorBoundary caught:', error, info.componentStack)
  }

  componentDidUpdate(prevProps: Props) {
    if (this.state.hasError && resetKeysChanged(prevProps.resetKeys, this.props.resetKeys)) {
      this.reset()
    }
  }

  reset = () => {
    this.props.onReset?.()
    this.setState({ hasError: false, error: null })
  }

  render() {
    if (this.state.hasError) {
      return (
        this.props.fallback ?? (
          <div className="flex flex-col items-center justify-center p-8 text-error">
            <p className="text-sm font-medium">Something went wrong</p>
            <p className="mt-1 text-xs text-error/80">
              {this.state.error?.message}
            </p>
            <Button
              variant="outline"
              size="xs"
              onClick={this.reset}
              className="mt-3 border-error/30 bg-transparent text-error hover:bg-error/10 hover:text-error"
            >
              Try again
            </Button>
          </div>
        )
      )
    }
    return this.props.children
  }
}
