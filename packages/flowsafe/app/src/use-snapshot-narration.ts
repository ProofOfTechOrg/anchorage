// Diffs each polled snapshot (run results + approval records) against the
// previous one and feeds the pure derivers. The previous snapshot lives in a
// ref that is advanced BEFORE deriving returns, so:
//   - the first snapshot after mount/reload emits nothing (no toast storm),
//   - StrictMode's second effect invoke sees the advanced ref and derives
//     prev === next, which yields no flips.

import { useEffect, useRef } from 'react';

import type { ApprovalRecord } from '../../src/approval-api/types.js';
import {
  deriveApprovalEvents,
  deriveRunEvents,
  type NarrationEvent,
  pollTroubleEvent,
} from './narration.js';
import type { RunEntry, RunResult } from './use-run-polling.js';

interface Snapshot {
  runResults: Record<string, RunResult>;
  records: readonly ApprovalRecord[];
}

export function useSnapshotNarration(
  runs: readonly RunEntry[],
  runResults: Record<string, RunResult>,
  records: readonly ApprovalRecord[],
  narrate: (events: readonly NarrationEvent[]) => void,
  /**
   * False until the FIRST dashboard refresh has settled. The pre-fetch state
   * (records: []) must not become the baseline — diffing the first fetch
   * against it would narrate every pre-existing approval as freshly queued
   * on reload.
   */
  ready: boolean,
): void {
  const previousRef = useRef<Snapshot | null>(null);

  useEffect(() => {
    if (!ready) return;
    const previous = previousRef.current;
    previousRef.current = { runResults, records };
    if (!previous) return;

    const events: NarrationEvent[] = [];
    for (const run of runs) {
      const next = runResults[run.runId];
      if (!next) continue;
      const before = previous.runResults[run.runId];
      if (next.summary) {
        events.push(...deriveRunEvents(before?.summary, next.summary, run));
      }
      if (next.stopped && before?.stopped !== next.stopped) {
        events.push(
          pollTroubleEvent(run.runId, {
            stopped: next.stopped === 'hard',
            message: next.error ?? 'status reads are failing',
          }),
        );
      }
    }
    const previousRecords = new Map(
      previous.records.map((record) => [record.id, record]),
    );
    events.push(...deriveApprovalEvents(previousRecords, records));
    if (events.length > 0) narrate(events);
  }, [runs, runResults, records, narrate, ready]);
}
