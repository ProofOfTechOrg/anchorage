// Node-testable coverage of the DL-021 poll/stream reconciliation, pulled out
// of useRunPolling's effect as pure functions (pollableRuns, mergeRunResults,
// allRunsSettled) precisely so the merge/done logic can be pinned without
// mounting the hook or driving its self-scheduling setTimeout chain — the
// same "prefer pure extraction over a renderer" convention the flowsafe
// approval-ui package documents (use-approval-dashboard.render.test.ts).

import { describe, expect, it } from 'vitest';
import type { RunSummary } from '@/run-client';
import {
  allRunsSettled,
  mergeRunResults,
  pollableRuns,
  type RunEntry,
  type RunResult,
} from '@/use-run-polling';

function run(runId: string, workflowId = 'gtm-outbound'): RunEntry {
  return { workflowId, runId, title: runId, startedAt: 0 };
}

function summaryResult(status: string): RunResult {
  return { summary: { runId: 'x', status } as RunSummary };
}

describe('pollableRuns', () => {
  it('returns every run when nothing is stream-healthy (poll-only, unchanged behavior)', () => {
    const runs = [run('a'), run('b')];
    expect(pollableRuns(runs, new Set())).toEqual(runs);
  });

  it('excludes only the runs whose socket is currently healthy', () => {
    const runs = [run('a'), run('b'), run('c')];
    const healthy = new Set(['b']);
    expect(pollableRuns(runs, healthy).map((r) => r.runId)).toEqual(['a', 'c']);
  });

  it('returns an empty list when every run is stream-covered', () => {
    const runs = [run('a'), run('b')];
    const healthy = new Set(['a', 'b']);
    expect(pollableRuns(runs, healthy)).toEqual([]);
  });
});

describe('mergeRunResults', () => {
  it('carries a stream-covered run (absent from entries) forward from previous', () => {
    // #given a run whose socket already set a result; the poll only fetched
    // its OWN pollable run this tick, so `entries` never mentions 'streamed'.
    const previous: Record<string, RunResult> = {
      streamed: summaryResult('running'),
    };
    const runs = [run('streamed'), run('polled')];
    const entries: Array<[string, RunResult]> = [
      ['polled', summaryResult('success')],
    ];

    // #when
    const next = mergeRunResults(previous, runs, entries);

    // #then the streamed run's prior result survives untouched
    expect(next.streamed).toBe(previous.streamed);
    expect(next.polled).toEqual(summaryResult('success'));
  });

  it('overwrites a carried-forward result with a freshly polled one for the SAME run', () => {
    const previous: Record<string, RunResult> = { a: summaryResult('running') };
    const runs = [run('a')];
    const entries: Array<[string, RunResult]> = [
      ['a', summaryResult('success')],
    ];

    const next = mergeRunResults(previous, runs, entries);

    expect(next.a).toEqual(summaryResult('success'));
  });

  it('keeps the last good summary beneath a fresh error, and clears stale error state on recovery', () => {
    const previous: Record<string, RunResult> = {
      a: summaryResult('running'),
    };
    const runs = [run('a')];

    // A transient failure keeps the last summary visible under the error.
    const failed = mergeRunResults(previous, runs, [
      ['a', { error: 'network blip', stopped: undefined }],
    ]);
    expect(failed.a).toEqual({
      summary: previous.a?.summary,
      error: 'network blip',
      stopped: undefined,
    });

    // A subsequent success entry (no `error`) replaces the errored entry
    // wholesale — the merge does not carry the error forward once cleared.
    const recovered = mergeRunResults(failed, runs, [
      ['a', summaryResult('running')],
    ]);
    expect(recovered.a).toEqual(summaryResult('running'));
  });

  it('drops an untracked run: a result present in previous but not in `runs` never survives the merge', () => {
    const previous: Record<string, RunResult> = {
      gone: summaryResult('success'),
    };
    const next = mergeRunResults(previous, [], []);
    expect(next).toEqual({});
  });
});

describe('allRunsSettled', () => {
  it('is false when nothing has settled or been abandoned', () => {
    const runs = [run('a')];
    expect(allRunsSettled(runs, new Map(), new Map())).toBe(false);
  });

  it('is true once every run is settled, whether via poll or stream (both populate the same map)', () => {
    const runs = [run('a'), run('b')];
    const settled = new Map<string, RunResult>([
      ['a', summaryResult('success')], // poll-observed terminal
      ['b', summaryResult('failed')], // stream-observed terminal
    ]);
    expect(allRunsSettled(runs, settled, new Map())).toBe(true);
  });

  it('is true once every run is abandoned (poll-only give-up path)', () => {
    const runs = [run('a'), run('b')];
    const abandoned = new Map<string, 'hard' | 'degraded'>([
      ['a', 'hard'],
      ['b', 'degraded'],
    ]);
    expect(allRunsSettled(runs, new Map(), abandoned)).toBe(true);
  });

  it('is false while even one tracked run is neither settled nor abandoned', () => {
    const runs = [run('a'), run('b')];
    const settled = new Map<string, RunResult>([
      ['a', summaryResult('success')],
    ]);
    expect(allRunsSettled(runs, settled, new Map())).toBe(false);
  });

  it('reads settled/abandoned directly, independent of any React results map (the state-race fix)', () => {
    // The whole point of this signature: it takes the plain-JS trackers, not a
    // `results` state snapshot, so it is correct even when a `setResults`
    // updater has not (yet) run — the scenario a concurrent stream frame's
    // setResults call can cause via React's eager-bailout optimization.
    const runs = [run('a')];
    const settled = new Map<string, RunResult>([
      ['a', summaryResult('success')],
    ]);
    expect(allRunsSettled(runs, settled, new Map())).toBe(true);
  });
});
