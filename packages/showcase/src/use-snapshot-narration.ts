// Diffs each snapshot (run results + approval records) against the previous one
// and feeds the pure derivers. The inputs now update from the live stream as
// well as the interval poll (Part B) — a decision reaches this diff within one
// round-trip instead of a poll cycle — but the diffing is unchanged: it narrates
// whatever moved since the last snapshot, whichever channel moved it. The
// previous snapshot lives in a ref that is advanced BEFORE deriving returns, so:
//   - the first snapshot after mount/reload emits nothing (no toast storm),
//   - StrictMode's second effect invoke sees the advanced ref and derives
//     prev === next, which yields no flips.

import type { ApprovalRecord } from '@flowsafe/approval-api/types';
import { useEffect, useRef } from 'react';
import {
  deriveApprovalEvents,
  deriveRunEvents,
  type NarrationEvent,
  pollTroubleEvent,
} from '@/narration';
import type { RunEntry, RunResult } from '@/use-run-polling';

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
        // ACCEPTED DEVIATION from "never derive one poll stream from the
        // other": these hints read the APPROVAL records to refine RUN
        // narration. The root cause is a server wire gap — resumeCount and
        // fingerprints cover only
        // CURRENT suspensions — so the summary alone cannot prove a past
        // gate. The hints only refine labels/suppress false lines; event
        // KEYS stay run-scoped, so dedup is unaffected. Known residual: if
        // gate 1 suspends AND is decided entirely between two run polls
        // while the 5s approval poll also hasn't caught up, gate 2 titles as
        // a first gate (imprecise, never dishonest) and dedup keeps that
        // title. Do not "simplify" the hints away without fixing the wire.
        //
        // An approval anywhere in this run's history proves a gate suspended
        // it; a DECIDED approval further proves any new suspension is a
        // later gate.
        const everSuspendedHint =
          run.approvalId !== undefined ||
          records.some((record) => record.runId === run.runId);
        const laterGateHint = records.some(
          (record) =>
            record.runId === run.runId &&
            (record.status === 'approved' || record.status === 'rejected'),
        );
        events.push(
          ...deriveRunEvents(before?.summary, next.summary, run, {
            everSuspendedHint,
            laterGateHint,
          }),
        );
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
