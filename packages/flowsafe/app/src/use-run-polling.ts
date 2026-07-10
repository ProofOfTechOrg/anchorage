// The run status poll, extracted from the old showcase-panels RunStatusPanel.
// Self-scheduling 3s chain per effect run; a run stops being polled when it
// reaches a terminal status or is abandoned (hard API error, or 5 consecutive
// transient failures). Abandonment is surfaced on the RunResult (`stopped`) so
// the narration layer can say "live updates degraded" without a side channel.

import { useEffect, useState } from 'react';

import {
  RunApiError,
  type RunClient,
  type RunSummary,
  TERMINAL_RUN_STATUSES,
} from './run-client.js';

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

export function useRunPolling(
  runClient: RunClient,
  runs: readonly RunEntry[],
  // Bumping the nonce re-arms the effect, which forgives every abandonment by
  // construction (the failure/abandoned maps are per-effect-run) — the "Retry
  // live updates" affordance on a degraded run card. Required (not defaulted):
  // biome's exhaustive-deps misreads a defaulted parameter as an outer-scope
  // value.
  retryNonce: number,
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
      const entries = await Promise.all(runs.map(probe));
      if (!alive) return true;
      setResults((previous) => {
        const next: Record<string, RunResult> = {};
        for (const [runId, result] of entries) {
          // Keep the last good summary visible beneath the error banner; an
          // untracked run drops out of the map entirely.
          next[runId] = result.error
            ? {
                summary: previous[runId]?.summary,
                error: result.error,
                stopped: result.stopped,
              }
            : result;
        }
        return next;
      });
      return entries.every(([runId, result]) =>
        result.summary && !result.error
          ? TERMINAL_RUN_STATUSES.has(result.summary.status)
          : abandoned.has(runId),
      );
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
    };
  }, [runClient, runs, retryNonce]);

  return results;
}
