// @vitest-environment jsdom

import {
  act,
  cleanup,
  fireEvent,
  render,
  renderHook,
  screen,
} from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { RunCards } from '@/run-cards';
import { RunClient, type RunSummary } from '@/run-client';
import {
  type RunEntry,
  type RunResult,
  UNAVAILABLE,
  useRunPolling,
} from '@/use-run-polling';

const run: RunEntry = {
  workflowId: 'wire-transfer',
  runId: 'run-1',
  title: 'Wire transfer',
  startedAt: 0,
};
const runs = [run];

function response(status: number, payload: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => payload,
  };
}

function cards(result: RunResult, onRetryPolling = vi.fn()) {
  return render(
    <RunCards
      runs={runs}
      results={{ [run.runId]: result }}
      records={[]}
      onReview={vi.fn()}
      onRetryPolling={onRetryPolling}
    />,
  );
}

afterEach(() => {
  cleanup();
  vi.clearAllTimers();
  vi.useRealTimers();
});

describe('RunCards polling errors', () => {
  it('preserves and renders an empty hard error from the real polling client, then recovers', async () => {
    vi.useFakeTimers();
    const initial: RunSummary = { runId: run.runId, status: 'running' };
    const recovered: RunSummary = { runId: run.runId, status: 'suspended' };
    const fetchRequest = vi
      .fn()
      .mockResolvedValueOnce(response(200, initial))
      .mockResolvedValueOnce(response(503, { error: '' }))
      .mockResolvedValueOnce(response(200, recovered));
    const client = new RunClient({ fetch: fetchRequest });
    const hook = renderHook(
      ({ retryNonce }) => useRunPolling(client, runs, retryNonce),
      { initialProps: { retryNonce: 0 } },
    );

    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(hook.result.current[run.runId]).toEqual({ summary: initial });

    await act(async () => {
      await vi.advanceTimersByTimeAsync(3000);
    });
    const failed = hook.result.current[run.runId];
    expect(failed).toEqual({ summary: initial, error: '', stopped: 'hard' });

    const onRetryPolling = vi.fn();
    const rendered = cards(failed as RunResult, onRetryPolling);
    expect(screen.getByText(UNAVAILABLE)).toBeInTheDocument();
    expect(
      screen.getByText(`Could not read run status: ${UNAVAILABLE}`),
    ).toBeInTheDocument();
    expect(screen.getByText('Polling stopped.')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Retry live updates' }));
    expect(onRetryPolling).toHaveBeenCalledTimes(1);
    rendered.unmount();

    await act(async () => {
      hook.rerender({ retryNonce: 1 });
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(hook.result.current[run.runId]).toEqual({ summary: recovered });
    expect(fetchRequest).toHaveBeenCalledTimes(3);
    hook.unmount();
  });

  it('renders an empty hard error without a prior summary as unavailable', () => {
    cards({ error: '', stopped: 'hard' });
    expect(screen.getByText(UNAVAILABLE)).toBeInTheDocument();
    expect(screen.queryByText('pending')).not.toBeInTheDocument();
    expect(
      screen.getByRole('button', { name: 'Retry live updates' }),
    ).toBeInTheDocument();
  });

  it('renders an empty transient error without a retry action', () => {
    cards({ error: '' });
    expect(
      screen.getByText(`Could not read run status: ${UNAVAILABLE}`),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: 'Retry live updates' }),
    ).not.toBeInTheDocument();
  });

  it('renders an empty terminal diagnostic without a polling retry', () => {
    cards({ summary: { runId: run.runId, status: 'failed', error: '' } });
    expect(screen.getByText('No error details available')).toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: 'Retry live updates' }),
    ).not.toBeInTheDocument();
  });
});
