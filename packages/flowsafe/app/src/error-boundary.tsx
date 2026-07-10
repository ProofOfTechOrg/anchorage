// A render throw anywhere used to white-screen the whole demo — the worst
// possible teaching surface. The fallback is deliberately PLAIN HTML with
// inline styles: if the theme or an Astryx component is what threw, rendering
// Astryx again inside the boundary would throw again.

import { Component, type ErrorInfo, type ReactNode } from 'react';

interface ErrorBoundaryProps {
  children: ReactNode;
}

interface ErrorBoundaryState {
  error: Error | null;
}

export class AppErrorBoundary extends Component<
  ErrorBoundaryProps,
  ErrorBoundaryState
> {
  state: ErrorBoundaryState = { error: null };

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error('showcase render crash', error, info.componentStack);
  }

  render(): ReactNode {
    if (this.state.error === null) return this.props.children;
    return (
      <div
        role="alert"
        style={{
          maxWidth: 640,
          margin: '4rem auto',
          padding: '1.5rem',
          fontFamily: 'sans-serif',
          border: '1px solid #c33',
          borderRadius: 8,
        }}
      >
        <h1 style={{ fontSize: '1.25rem', margin: '0 0 0.5rem' }}>
          The demo hit a rendering error
        </h1>
        <p style={{ margin: '0 0 1rem' }}>
          Nothing server-side is affected — your runs and approvals are intact.
          Reload to continue.
        </p>
        <pre
          style={{
            whiteSpace: 'pre-wrap',
            fontSize: '0.8rem',
            opacity: 0.8,
            margin: '0 0 1rem',
          }}
        >
          {this.state.error.message}
        </pre>
        <button
          type="button"
          onClick={() => window.location.reload()}
          style={{ padding: '0.5rem 1rem', cursor: 'pointer' }}
        >
          Reload
        </button>
      </div>
    );
  }
}
