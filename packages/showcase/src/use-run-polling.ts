// The run status poll, extracted from the old showcase-panels RunStatusPanel.
// Self-scheduling 3s chain per effect run; a run stops being polled when it
// reaches a terminal status or is abandoned (hard API error, or 5 consecutive
// transient failures). Abandonment is surfaced on the RunResult (`stopped`) so
// the narration layer can say "live updates degraded" without a side channel.

import type {
  StreamConnection,
  StreamTransport,
} from '@flowsafe/approval-ui/stream';
import { subscribeApprovalStream } from '@flowsafe/approval-ui/use-approval-dashboard';
import { useEffect, useState } from 'react';

import {
  RunApiError,
  type RunClient,
  type RunSummary,
  TERMINAL_RUN_STATUSES,
} from '@/run-client';

/**
 * Live per-run streaming (DL-021): a browser-WebSocket transport plus a thunk
 * that mints + shapes a run-channel address for one (workflowId, runId). The
 * run channel is WHOLESALE — each frame is the complete authoritative
 * RunSummary — so a healthy socket fully owns a run's status and its poll
 * pauses; the poll resumes on socket close/error. Absent => poll-only.
 */
export interface RunStreamOption {
  transport: StreamTransport;
  ticket: (
    workflowId: string,
    runId: string,
  ) => Promise<{ url: string; ticket: string }>;
}

/** A run the launcher started, tracked so the status panel can poll it. */
export interface RunEntry {
  workflowId: string;
  runId: string;
  title: string;
  /** The approval auto-queued by a start that suspended (run → queue link). */
  approvalId?: string;
  /** Client clock at launch — display/ordering only, never sent anywhere. */
  startedAt: number;
}

/** What the last poll of a run produced. `summary` survives a later error. */
export interface RunResult {
  summary?: RunSummary;
  error?: string;
  /** Polling gave up: a hard API error, or too many transient failures. */
  stopped?: 'hard' | 'degraded';
}

/** Rendered when the status endpoint, not the run, is what failed. */
export const UNAVAILABLE = 'unavailable';

const POLL_INTERVAL_MS = 3000;

/**
 * Consecutive transient failures tolerated before a run stops being polled.
 * At POLL_INTERVAL_MS that is ~15s of grace — ample for a just-started run's
 * snapshot to materialize.
 */
const MAX_TRANSIENT_FAILURES = 5;

/**
 * A 404 is transient: the run was accepted but its snapshot may not be readable
 * yet. So is a network blip (no RunApiError at all). Everything else — 401 after
 * an actor switch, 403, 500 — is a hard failure that will not fix itself, so
 * polling it forever is pure noise.
 */
function isTransient(error: unknown): boolean {
  return !(error instanceof RunApiError) || error.status === 404;
}

// ---- Pure DL-021 reconciliation (node-testable, no React/timers involved) --

/**
 * Runs NOT covered by a healthy stream socket — what the poll must still
 * fetch. DL-021: the run channel is WHOLESALE, so a healthy socket fully owns
 * its run's result and the poll skips it; `healthy` stays empty in poll-only
 * mode, so this returns every run and the poll behaves exactly as before.
 */
export function pollableRuns(
  runs: readonly RunEntry[],
  healthy: ReadonlySet<string>,
): RunEntry[] {
  return runs.filter((run) => !healthy.has(run.runId));
}

/**
 * Merge one poll's fetched entries on top of the previous results: every
 * tracked run's last result carries forward FIRST (so a stream-covered run,
 * absent from `entries`, keeps the result its socket already set), then the
 * freshly polled entries overwrite. Pure — no React state read or written.
 */
export function mergeRunResults(
  previous: Readonly<Record<string, RunResult>>,
  runs: readonly RunEntry[],
  entries: ReadonlyArray<readonly [string, RunResult]>,
): Record<string, RunResult> {
  const next: Record<string, RunResult> = {};
  for (const run of runs) {
    const existing = previous[run.runId];
    if (existing) next[run.runId] = existing;
  }
  for (const [runId, result] of entries) {
    // Keep the last good summary visible beneath the error banner.
    next[runId] = result.error
      ? {
          summary: previous[runId]?.summary,
          error: result.error,
          stopped: result.stopped,
        }
      : result;
  }
  return next;
}

/**
 * True once every tracked run reached a terminal status — recorded in
 * `settled` by EITHER the poll's `probe()` or the stream's `onFrame` — or was
 * abandoned (poll-only). Reads the plain-JS trackers directly rather than the
 * React `results` state, so it is correct regardless of whether/when a
 * `setResults` updater has actually run: React only runs a functional updater
 * SYNCHRONOUSLY via its eager-bailout optimization, which is skipped whenever
 * another update for that state is already queued — exactly what a concurrent
 * stream frame's `setResults` call (`onFrame`, above) can cause once a run
 * stream is active. `settled`/`abandoned` are mutated directly inside
 * `probe()`/`onFrame`, so they are already accurate the instant polling's
 * `Promise.all` resolves, independent of React's scheduling.
 */
export function allRunsSettled(
  runs: readonly RunEntry[],
  settled: ReadonlyMap<string, RunResult>,
  abandoned: ReadonlyMap<string, 'hard' | 'degraded'>,
): boolean {
  return runs.every(
    (run) => settled.has(run.runId) || abandoned.has(run.runId),
  );
}

export function useRunPolling(
  runClient: RunClient,
  runs: readonly RunEntry[],
  // Bumping the nonce re-arms the effect, which forgives every abandonment by
  // construction (the failure/abandoned maps are per-effect-run) — the "Retry
  // live updates" affordance on a degraded run card. Required (not defaulted):
  // biome's exhaustive-deps misreads a defaulted parameter as an outer-scope
  // value.
  retryNonce: number,
  // Optional live per-run stream. Absent => the poll below is the sole source
  // (today's behavior, unchanged). Present => each run gets a WebSocket that
  // pauses its poll while healthy (DL-021).
  stream?: RunStreamOption,
): Record<string, RunResult> {
  const [results, setResults] = useState<Record<string, RunResult>>({});

  // Poll each tracked run's status. External sync — a legitimate effect; re-runs
  // when the run set, the acting client, or the retry nonce changes.
  useEffect(() => {
    // The nonce's value is meaningless — its CHANGE is the re-arm signal, and
    // re-arming forgives abandonments because the maps below are per-effect-run.
    void retryNonce;
    if (runs.length === 0) return;
    let alive = true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    // Per-effect-run bookkeeping, deliberately NOT a ref: re-arming (a new run,
    // an actor switch) should forgive earlier failures, and StrictMode's double
    // invoke gets its own counters instead of sharing one set.
    const failures = new Map<string, number>();
    const abandoned = new Map<string, 'hard' | 'degraded'>();
    const lastError = new Map<string, string>();
    // A run observed terminal keeps its final result without further fetches:
    // while a LIVE sibling keeps the chain ticking, re-probing a finished run
    // every 3s is a DO wake + snapshot read for an answer that cannot change.
    const settled = new Map<string, RunResult>();
    // Runs whose per-run WebSocket is currently OPEN. DL-021: the run channel is
    // WHOLESALE (every frame is the complete authoritative RunSummary), so while
    // a run's socket is healthy the poll PAUSES for it and the socket owns its
    // result; the poll resumes covering it on socket close/error. Per-effect-run
    // (like `abandoned`), so StrictMode's double invoke gets its own set + its
    // own sockets. Never populated in poll-only mode, so the poll then covers
    // every run exactly as before.
    const healthy = new Set<string>();
    const connections: StreamConnection[] = [];

    // Additive live subscription: one run-channel socket per tracked run. The
    // ticket thunk mints + shapes the run address; subscribeApprovalStream
    // reconnects with backoff and parses each frame. Absent stream => skipped
    // (poll-only, unchanged).
    if (stream) {
      for (const run of runs) {
        const connection = subscribeApprovalStream({
          transport: stream.transport,
          ticket: () => stream.ticket(run.workflowId, run.runId),
          onFrame: (frame) => {
            if (!alive || frame.type !== 'run') return;
            // The wire summary is the DO's status() projection — structurally
            // the RunSummary the run cards render.
            const summary = frame.summary as unknown as RunSummary;
            const result: RunResult = { summary };
            if (TERMINAL_RUN_STATUSES.has(summary.status)) {
              settled.set(run.runId, result);
            }
            setResults((previous) => ({ ...previous, [run.runId]: result }));
          },
          // Socket healthy: pause polling this run (the stream owns it).
          onOpen: () => {
            if (alive) healthy.add(run.runId);
          },
          // Socket dropped: the poll resumes covering this run as the fallback.
          onClose: () => {
            healthy.delete(run.runId);
          },
        });
        connections.push(connection);
      }
    }

    async function probe(run: RunEntry): Promise<[string, RunResult]> {
      const finished = settled.get(run.runId);
      if (finished) return [run.runId, finished];
      const alreadyStopped = abandoned.get(run.runId);
      if (alreadyStopped) {
        return [
          run.runId,
          {
            error: lastError.get(run.runId) ?? UNAVAILABLE,
            stopped: alreadyStopped,
          },
        ];
      }
      try {
        const summary = await runClient.status(run.workflowId, run.runId);
        failures.delete(run.runId);
        const result: RunResult = { summary };
        if (TERMINAL_RUN_STATUSES.has(summary.status)) {
          settled.set(run.runId, result);
        }
        return [run.runId, result];
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const count = (failures.get(run.runId) ?? 0) + 1;
        failures.set(run.runId, count);
        lastError.set(run.runId, message);
        if (!isTransient(error)) {
          abandoned.set(run.runId, 'hard');
        } else if (count >= MAX_TRANSIENT_FAILURES) {
          abandoned.set(run.runId, 'degraded');
        }
        return [
          run.runId,
          { error: message, stopped: abandoned.get(run.runId) },
        ];
      }
    }

    // Returns true once every tracked run is finished or abandoned — the only
    // two ways polling can stop. A swallowed error used to render as a plausible
    // 'pending' that never became terminal, so a broken run polled forever.
    async function poll(): Promise<boolean> {
      const pollable = pollableRuns(runs, healthy);
      if (pollable.length === 0) {
        // Every run is stream-covered: the sockets own the results, so the poll
        // makes no request and no state write.
        return allRunsSettled(runs, settled, abandoned);
      }
      const entries = await Promise.all(pollable.map(probe));
      if (!alive) return true;
      setResults((previous) => mergeRunResults(previous, runs, entries));
      // Read `settled`/`abandoned` directly rather than the map `setResults`
      // just built — see allRunsSettled's doc for why that read would be
      // unreliable once a run stream is active.
      return allRunsSettled(runs, settled, abandoned);
    }

    // Self-scheduling rather than setInterval: a poll slower than the interval
    // cannot stack behind itself, and the chain simply stops when done.
    async function tick(): Promise<void> {
      const done = await poll();
      if (!alive || done) return;
      timer = setTimeout(() => void tick(), POLL_INTERVAL_MS);
    }
    void tick();

    return () => {
      alive = false;
      if (timer !== undefined) clearTimeout(timer);
      for (const connection of connections) connection.close();
    };
  }, [runClient, runs, retryNonce, stream]);

  return results;
}
