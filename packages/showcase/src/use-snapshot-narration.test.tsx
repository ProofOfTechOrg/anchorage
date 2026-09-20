// @vitest-environment jsdom

import { renderHook } from '@testing-library/react';
import { expect, it, vi } from 'vitest';
import type { RunEntry, RunResult } from '@/use-run-polling';
import { useSnapshotNarration } from '@/use-snapshot-narration';

const RUN: RunEntry = {
  workflowId: 'product-launch',
  runId: 'run-1',
  title: 'Product launch',
  startedAt: 0,
};

it('narrates an empty hard polling error with fallback detail', () => {
  const narrate = vi.fn();
  const initial: Record<string, RunResult> = {
    [RUN.runId]: { summary: { runId: RUN.runId, status: 'running' } },
  };
  const hook = renderHook(
    ({ results }) => useSnapshotNarration([RUN], results, [], narrate, true),
    { initialProps: { results: initial } },
  );

  hook.rerender({
    results: {
      [RUN.runId]: {
        summary: initial[RUN.runId]?.summary,
        error: '',
        stopped: 'hard',
      },
    },
  });

  expect(narrate).toHaveBeenCalledTimes(1);
  expect(narrate.mock.calls[0]?.[0]).toEqual([
    expect.objectContaining({
      kind: 'poll.stopped',
      detail: expect.stringContaining('status reads are failing'),
    }),
  ]);
});
