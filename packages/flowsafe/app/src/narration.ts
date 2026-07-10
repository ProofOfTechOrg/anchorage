// Client-derived narration: pure functions that turn polled snapshots and
// one-shot API results into feed/toast events. Nothing here talks to the
// network or the DOM — deriving the same facts twice yields the same event
// KEYS, and the feed/toast layers dedup on key, which is what makes StrictMode
// double-invokes and overlapping poll streams safe by construction.
//
// Honesty contract: `observed: true` events restate an API response the
// browser actually saw; `observed: false` events describe what the deployed
// architecture does BY DESIGN between those observations (DO spawn, D1
// snapshots, server-side grant derivation) and are only ever emitted anchored
// to an observed event.

import type {
  ApprovalRecord,
  DecideResult,
} from '../../src/approval-api/types.js';
import {
  RunApiError,
  type RunSummary,
  type StartRunResponse,
} from './run-client.js';

export type NarrationZone = 'browser' | 'worker' | 'do' | 'd1' | 'cron';
export type NarrationTone =
  | 'neutral'
  | 'info'
  | 'success'
  | 'warning'
  | 'danger';

export interface NarrationEvent {
  /** Dedup id — re-deriving the same fact must reproduce the same key. */
  key: string;
  /** Client clock at derivation; display/ordering only. */
  at: number;
  zone: NarrationZone;
  kind: string;
  title: string;
  detail?: string;
  tone: NarrationTone;
  runId?: string;
  approvalId?: string;
  /** true = restates an observed API response; false = true by design. */
  observed: boolean;
  toast: boolean;
  /** Toast never auto-hides (hard errors). */
  toastSticky?: boolean;
  /** Longer auto-hide for load-bearing info (10s vs the 6s default). */
  toastLong?: boolean;
  /** Replaces the live toast with this uniqueID instead of stacking. */
  toastReplaceId?: string;
}

/** The identity of a tracked run — structural subset of the app's RunEntry. */
export interface NarrationRunRef {
  workflowId: string;
  runId: string;
  title: string;
}

const TERMINAL_STATUSES = new Set([
  'success',
  'failed',
  'tripwire',
  'canceled',
  'bailed',
  'skipped',
]);

/** `${tenant}_${uuid}` → first 8 uuid chars; anything else → first 8. */
export function shortId(id: string): string {
  const tail = id.includes('_') ? id.slice(id.lastIndexOf('_') + 1) : id;
  return tail.slice(0, 8);
}

interface Suspension {
  step: string;
  ordinal: number;
  suspendedAt?: number;
  reason?: string;
  connectors?: readonly string[];
}

function suspensionOf(summary: RunSummary): Suspension | undefined {
  const path = summary.suspended?.[0];
  if (!path || path.length === 0) return undefined;
  const step = path.join('.');
  const payload = summary.suspendPayload?.[step];
  return {
    step,
    // The runtime's per-step resume ordinal: absent on a first suspension.
    ordinal: summary.resumeCount?.[step] ?? 0,
    suspendedAt: summary.suspendedAt?.[step],
    reason: typeof payload?.reason === 'string' ? payload.reason : undefined,
    connectors: Array.isArray(payload?.connectors)
      ? payload.connectors
      : undefined,
  };
}

// ---- result interpretation --------------------------------------------------

export type ResultFlavor =
  | 'simulated'
  | 'real-write'
  | 'declined'
  | 'preview'
  | 'delivered'
  | 'plain';

export interface ResultInterpretation {
  flavor: ResultFlavor;
  /** One human sentence for the terminal feed/toast/card line. */
  line?: string;
  /** The result carried replayed:true — an idempotent replay, not a re-execution. */
  replayed: boolean;
}

/**
 * Read the showcase result shapes ({outcome}, {published, key}, {granted},
 * replayed flags) into a truthful one-liner. Unknown shapes stay 'plain' with
 * no line — the card always renders the raw JSON as proof either way.
 */
export function interpretRunResult(result: unknown): ResultInterpretation {
  if (result === null || typeof result !== 'object') {
    return { flavor: 'plain', replayed: false };
  }
  const r = result as Record<string, unknown>;
  const replayed =
    r.replayed === true ||
    Object.values(r).some(
      (value) =>
        value !== null &&
        typeof value === 'object' &&
        (value as Record<string, unknown>).replayed === true,
    );
  const outcome = typeof r.outcome === 'string' ? r.outcome : undefined;

  if (outcome === 'declined' || r.granted === false) {
    return {
      flavor: 'declined',
      replayed,
      line: 'Outcome: declined — the rejection resumed the run to a clean stop; no side effect ran.',
    };
  }
  if (outcome === 'preview') {
    return {
      flavor: 'preview',
      replayed,
      line: 'Outcome: preview — a dry-run pass through the real connector path; nothing changed.',
    };
  }
  if (r.published === true) {
    const key = typeof r.key === 'string' ? ` — key ${r.key}` : '';
    return {
      flavor: 'real-write',
      replayed,
      line: `Published to the sandbox artifact store${key}. In production this write goes to R2.`,
    };
  }
  if (outcome === 'simulated') {
    return {
      flavor: 'simulated',
      replayed,
      line: 'Outcome: simulated — the connector ran its full code path; the external call was skipped (connectors are offline here).',
    };
  }
  if (outcome === 'sent' || outcome === 'deployed') {
    return {
      flavor: 'delivered',
      replayed,
      line: `Outcome: ${outcome}.`,
    };
  }
  if (r.granted === true) {
    return {
      flavor: 'plain',
      replayed,
      line: "Access granted — the grant exists only in this run's result; no real system was touched.",
    };
  }
  return { flavor: 'plain', replayed };
}

// ---- shared builders (one key discipline for polls AND one-shots) -----------

function suspendedEvents(
  next: RunSummary,
  run: NarrationRunRef,
  at: number,
  laterGateHint = false,
): NarrationEvent[] {
  const susp = next.status === 'suspended' ? suspensionOf(next) : undefined;
  if (!susp) return [];
  // A later gate or a re-suspension of the same step: either way the earlier
  // approval is spent. The wire's resumeCount covers only CURRENTLY-suspended
  // steps, so a different-step later gate is invisible in the summary alone —
  // the caller passes laterGateHint when it observed the earlier suspension
  // (poll diff) or a decided approval for this run (records).
  const again =
    laterGateHint ||
    susp.ordinal > 0 ||
    Object.keys(next.resumeCount ?? {}).length > 0;
  const fingerprint = `suspension ${susp.suspendedAt ?? '?'} · resume #${susp.ordinal}`;
  const connectors = susp.connectors?.join(', ');
  return [
    {
      key: `run:${run.runId}:status:suspended:${susp.step}:${susp.ordinal}`,
      at,
      zone: 'do',
      kind: 'run.suspended',
      title: again
        ? `Second gate — ${run.title} suspended again at ${susp.step}`
        : `Approval needed — ${run.title} suspended at ${susp.step}`,
      detail: [
        susp.reason ? `'${susp.reason}'` : undefined,
        connectors ? `connectors to grant: ${connectors}` : undefined,
        again
          ? 'The earlier approval is spent — this new suspension needs its own decision and mints its own grant.'
          : 'The request is in the queue.',
        `(${fingerprint})`,
      ]
        .filter(Boolean)
        .join(' — '),
      tone: 'warning',
      runId: run.runId,
      observed: true,
      toast: true,
      toastLong: true,
    },
  ];
}

function resumedEvents(
  run: NarrationRunRef,
  step: string,
  ordinal: number,
  at: number,
  options: {
    connectors?: readonly string[];
    declined: boolean;
    /** Own decide already told the story inline — feed line only. */
    toast: boolean;
    ok?: boolean;
  },
): NarrationEvent[] {
  const events: NarrationEvent[] = [
    {
      key: `resumed:${run.runId}:${step}:${ordinal}`,
      at,
      zone: 'do',
      kind: 'run.resumed',
      title: `Run resumed — ${run.title}`,
      detail: options.declined
        ? 'The rejection resumed the run server-side toward a declined outcome — no grant was minted.'
        : options.connectors?.length
          ? `The approval unlocked ${options.connectors.join(', ')} for this resumed leg only.`
          : `Resume attempted inside the decide call — ok: ${options.ok ?? true}.`,
      tone: 'info',
      runId: run.runId,
      observed: true,
      toast: options.toast && !options.declined,
    },
  ];
  if (!options.declined) {
    events.push({
      key: `grant:${run.runId}:${step}:${ordinal}`,
      at,
      zone: 'worker',
      kind: 'grant.derived',
      title: 'Grant derived server-side',
      detail: `Recomputed from APPROVED records bound to (${step}, suspendedAt, resume #${ordinal}) — grants never travel in HTTP bodies.`,
      tone: 'neutral',
      runId: run.runId,
      observed: false,
      toast: false,
    });
  }
  return events;
}

function terminalEvents(
  next: RunSummary,
  run: NarrationRunRef,
  at: number,
  connectorsHint: readonly string[] | undefined,
  everSuspended: boolean,
): NarrationEvent[] {
  const events: NarrationEvent[] = [];
  const interp = interpretRunResult(next.result);
  if (next.status === 'success') {
    if (connectorsHint?.length && interp.flavor !== 'declined') {
      events.push({
        key: `connector:${run.runId}`,
        at,
        zone: 'do',
        kind: 'connector.executed',
        title: `Connector ${connectorsHint.join(', ')} executed`,
        detail:
          'Ran behind 4 tenant-scoped gates: egress allowlist → write-approval grant → idempotent replay → rate limit.',
        tone: 'neutral',
        runId: run.runId,
        observed: false,
        toast: false,
      });
    }
    if (interp.replayed) {
      events.push({
        key: `replayed:${run.runId}`,
        at,
        zone: 'do',
        kind: 'connector.replayed',
        title: 'Idempotent replay',
        detail:
          'The result reports replayed: true — a retry with the same idempotency key returned the recorded result instead of executing again.',
        tone: 'info',
        runId: run.runId,
        observed: true,
        toast: false,
      });
    }
    if (!everSuspended) {
      events.push({
        key: `done:${run.runId}:nogate`,
        at,
        zone: 'do',
        kind: 'run.no-gate',
        title: `Gate short-circuited — ${run.title}`,
        detail:
          'The run completed without ever suspending: its approval gate was never reached, so no approval was queued and no connector ran.',
        tone: 'info',
        runId: run.runId,
        observed: true,
        toast: false,
      });
    }
    // A gate-less run labelled 'declined' by its workflow (lead-generation's
    // all-cold path) was never rejected by anyone — say what actually
    // happened instead of the rejection copy.
    const line =
      !everSuspended && interp.flavor === 'declined'
        ? 'Completed without reaching its gate — nothing needed approval and no connector ran.'
        : interp.line;
    events.push({
      key: `done:${run.runId}`,
      at,
      zone: 'do',
      kind: 'run.succeeded',
      title: `Run finished — ${run.title}`,
      detail: line,
      tone: 'success',
      runId: run.runId,
      observed: true,
      toast: true,
    });
  } else {
    events.push({
      key: `done:${run.runId}`,
      at,
      zone: 'do',
      kind: 'run.failed',
      title: `Run ${next.status} — ${run.title}`,
      detail: `${next.error ?? 'no error detail'}. The D1 snapshot is retained; last known state stays on the run card.`,
      tone: 'danger',
      runId: run.runId,
      observed: true,
      toast: true,
      toastSticky: true,
    });
  }
  return events;
}

// ---- snapshot derivers -------------------------------------------------------

export interface DeriveRunOptions {
  /**
   * The caller knows a gate DID suspend this run (an approval exists for it).
   * Needed because the runtime's resume ledger — the resumeCount source — is
   * dropped at terminal status, so a running→success flip can look
   * gate-less even when a gate was approved between polls.
   */
  everSuspendedHint?: boolean;
  /**
   * The caller knows an earlier gate of this run was already DECIDED, so any
   * new suspension is a later gate whose approval must be fresh. Needed for
   * the running→suspended two-poll path, where neither summary shows the
   * spent gate (the wire's fingerprints cover only current suspensions).
   */
  laterGateHint?: boolean;
}

/**
 * Diff two polled summaries of one run into events. `prev === undefined`
 * yields [] — a run's first appearance is narrated by startEvent (the start
 * response IS its settled first state), never by the poll.
 */
export function deriveRunEvents(
  prev: RunSummary | undefined,
  next: RunSummary,
  run: NarrationRunRef,
  options: DeriveRunOptions = {},
): NarrationEvent[] {
  if (!prev) return [];
  const at = Date.now();
  const events: NarrationEvent[] = [];
  const prevSusp = prev.status === 'suspended' ? suspensionOf(prev) : undefined;
  const nextSusp = next.status === 'suspended' ? suspensionOf(next) : undefined;

  // The previously-observed suspension ended (to running, terminal, or a NEW
  // suspension — product-launch gate 1 → gate 2 keeps status 'suspended').
  if (
    prevSusp &&
    (!nextSusp ||
      nextSusp.step !== prevSusp.step ||
      nextSusp.ordinal > prevSusp.ordinal)
  ) {
    const ordinal = next.resumeCount?.[prevSusp.step] ?? prevSusp.ordinal + 1;
    const declined =
      TERMINAL_STATUSES.has(next.status) &&
      interpretRunResult(next.result).flavor === 'declined';
    events.push(
      ...resumedEvents(run, prevSusp.step, ordinal, at, {
        connectors: prevSusp.connectors,
        declined,
        toast: true,
      }),
    );
  }

  if (
    nextSusp &&
    (!prevSusp ||
      prevSusp.step !== nextSusp.step ||
      prevSusp.ordinal !== nextSusp.ordinal)
  ) {
    // A previously-observed different suspension is direct proof of a later
    // gate; otherwise trust the caller's decided-approval hint.
    const laterGate =
      (prevSusp !== undefined &&
        (prevSusp.step !== nextSusp.step ||
          nextSusp.ordinal > prevSusp.ordinal)) ||
      options.laterGateHint === true;
    events.push(...suspendedEvents(next, run, at, laterGate));
  }

  if (
    !TERMINAL_STATUSES.has(prev.status) &&
    TERMINAL_STATUSES.has(next.status)
  ) {
    const everSuspended =
      options.everSuspendedHint === true ||
      prevSusp !== undefined ||
      Object.keys(next.resumeCount ?? {}).length > 0 ||
      Object.keys(next.suspendedAt ?? {}).length > 0;
    events.push(
      ...terminalEvents(next, run, at, prevSusp?.connectors, everSuspended),
    );
  }

  return events;
}

/**
 * Diff the polled approval list against the previously-seen records.
 * `prev === undefined` yields [] (first snapshot after mount/reload).
 */
export function deriveApprovalEvents(
  prev: ReadonlyMap<string, ApprovalRecord> | undefined,
  next: readonly ApprovalRecord[],
): NarrationEvent[] {
  if (!prev) return [];
  const at = Date.now();
  const events: NarrationEvent[] = [];
  for (const record of next) {
    const before = prev.get(record.id);
    if (!before) {
      events.push(
        {
          key: `approval:${record.id}:status:${record.status}`,
          at,
          zone: 'worker',
          kind: 'approval.queued',
          title: `Approval queued: '${record.title}'`,
          detail: [
            record.connectors.length > 0
              ? `connectors to grant: ${record.connectors.join(', ')}`
              : undefined,
            `requested by ${record.requestedBy}`,
          ]
            .filter(Boolean)
            .join(' — '),
          tone: 'info',
          runId: record.runId,
          approvalId: record.id,
          observed: true,
          toast: false,
        },
        cronMentionEvent(at),
      );
      continue;
    }
    if (before.status !== record.status) {
      if (record.status === 'claimed') {
        events.push({
          key: `approval:${record.id}:status:claimed`,
          at,
          zone: 'worker',
          kind: 'approval.claimed',
          title: `Claimed by ${record.claimedBy ?? 'a reviewer'}`,
          detail: `'${record.title}' is being reviewed. Claiming is optional bookkeeping — deciding does not require it.`,
          tone: 'info',
          runId: record.runId,
          approvalId: record.id,
          observed: true,
          toast: true,
        });
      } else if (record.status === 'approved' || record.status === 'rejected') {
        events.push({
          // Same key family as the decider's own decideEvents — first wins,
          // so the decider never sees a duplicate "decided" via the poll.
          key: `decide:${record.id}:${record.decision ?? record.status}`,
          at,
          zone: 'worker',
          kind: 'approval.decided',
          title: `Approval ${record.status} — '${record.title}'`,
          detail: [
            `${record.decision ?? record.status} by ${record.decidedBy ?? 'unknown'}`,
            record.comment ? `'${record.comment}'` : undefined,
            record.status === 'rejected'
              ? 'the run resumes to a declined outcome'
              : undefined,
          ]
            .filter(Boolean)
            .join(' — '),
          tone: record.status === 'approved' ? 'success' : 'warning',
          runId: record.runId,
          approvalId: record.id,
          observed: true,
          toast: true,
        });
      } else if (record.status === 'escalated') {
        events.push({
          key: `approval:${record.id}:status:escalated`,
          at,
          zone: 'cron',
          kind: 'approval.escalated',
          title: `Approval ${shortId(record.id)} escalated — SLA breached`,
          detail:
            'Flagged by the 15-minute sweep. Escalation raises visibility only — the request is still fully decidable.',
          tone: 'warning',
          runId: record.runId,
          approvalId: record.id,
          observed: true,
          toast: false,
        });
      }
    } else if (
      record.claimedBy !== undefined &&
      before.claimedBy !== record.claimedBy
    ) {
      events.push({
        key: `approval:${record.id}:claimedBy:${record.claimedBy}`,
        at,
        zone: 'worker',
        kind: 'approval.delegated',
        title: `Delegated to ${record.claimedBy}`,
        detail: `'${record.title}' — last write wins by design: delegation moves a pointer and guards no side effect.`,
        tone: 'neutral',
        runId: record.runId,
        approvalId: record.id,
        observed: true,
        toast: false,
      });
    }
  }
  return events;
}

// ---- one-shot builders -------------------------------------------------------

/** Emitted once per session, anchored to the first approval sighting. */
function cronMentionEvent(at: number): NarrationEvent {
  return {
    key: 'cron:mention',
    at,
    zone: 'worker',
    kind: 'cron.mention',
    title: 'Background machinery (not client-observable)',
    detail:
      'An SLA sweep every 15 min escalates overdue approvals; scheduled purges reap expired sandboxes and old snapshots; audit streams to Workers Logs.',
    tone: 'neutral',
    observed: false,
    toast: false,
  };
}

export interface StartEventOptions {
  actor?: { id: string; role: string };
  /** Step ids in definition order (from the workflow guide) for the ○ line. */
  steps?: readonly string[];
}

/** Narrate a successful POST /runs — including an immediate suspension/terminal. */
export function startEvent(
  run: NarrationRunRef,
  response: StartRunResponse,
  options: StartEventOptions = {},
): NarrationEvent[] {
  const at = Date.now();
  const settledElsewhere =
    response.status === 'suspended' || TERMINAL_STATUSES.has(response.status);
  const events: NarrationEvent[] = [
    {
      key: `start:${run.runId}:requested`,
      at,
      zone: 'browser',
      kind: 'run.start-requested',
      title: `POST /runs ${run.workflowId}`,
      detail: options.actor
        ? `as ${options.actor.id} (${options.actor.role})`
        : undefined,
      tone: 'neutral',
      runId: run.runId,
      observed: true,
      toast: false,
    },
    {
      key: `start:${run.runId}`,
      at,
      zone: 'worker',
      kind: 'run.started',
      title: `Run started — ${run.title}`,
      detail: `Worker verified the caller and minted runId ${shortId(run.runId)} server-side (tenant-prefixed). Executing in its own Durable Object.`,
      tone: 'info',
      runId: run.runId,
      observed: true,
      // Merge rule: a start that already suspended (or finished) toasts as
      // that state, never as start+suspend both.
      toast: !settledElsewhere,
    },
    {
      key: `do:${run.runId}`,
      at,
      zone: 'do',
      kind: 'do.spawned',
      title: `Durable Object ${run.workflowId}:${shortId(run.runId)} instantiated`,
      detail: 'One DO per run, with a per-run serialization lock.',
      tone: 'neutral',
      runId: run.runId,
      observed: false,
      toast: false,
    },
    {
      key: `d1:${run.runId}`,
      at,
      zone: 'd1',
      kind: 'd1.snapshot',
      title: 'Run state snapshotted to D1 after each step',
      detail:
        'The run now survives isolate eviction and resumes from this snapshot.',
      tone: 'neutral',
      runId: run.runId,
      observed: false,
      toast: false,
    },
  ];
  if (options.steps?.length) {
    events.splice(3, 0, {
      key: `steps:${run.runId}`,
      at,
      zone: 'do',
      kind: 'do.steps-executed',
      title: `Steps ${options.steps.join(' → ')} execute inside the DO`,
      tone: 'neutral',
      runId: run.runId,
      observed: false,
      toast: false,
    });
  }
  events.push(...suspendedEvents(response, run, at));
  if (response.approval) {
    events.push(
      {
        key: `approval:${response.approval.id}:status:pending`,
        at,
        zone: 'worker',
        kind: 'approval.queued',
        title: 'Worker observed the suspension and queued an approval',
        detail: `approval ${shortId(response.approval.id)} — decide it in the queue below.`,
        tone: 'info',
        runId: run.runId,
        approvalId: response.approval.id,
        observed: true,
        toast: false,
      },
      cronMentionEvent(at),
    );
  }
  if (TERMINAL_STATUSES.has(response.status)) {
    events.push(...terminalEvents(response, run, at, undefined, false));
  }
  return events;
}

/** Narrate a failed POST /runs (429 budget, 401/403 authz, 503 kill switch). */
export function startErrorEvent(
  workflowId: string,
  error: unknown,
  actorRole?: string,
): NarrationEvent {
  const at = Date.now();
  const message = error instanceof Error ? error.message : String(error);
  if (error instanceof RunApiError) {
    if (error.status === 429) {
      return {
        key: '429',
        at,
        zone: 'worker',
        kind: 'budget.refused',
        title: 'Run budget refused',
        detail: message,
        tone: 'danger',
        observed: true,
        toast: true,
        toastSticky: true,
      };
    }
    if (error.status === 401 || error.status === 403) {
      return {
        key: `403:${workflowId}:${actorRole ?? 'unknown'}`,
        at,
        zone: 'worker',
        kind: 'authz.denied',
        title: `Role can't start this — ${workflowId}`,
        detail: `${message}. The server enforces this even if the button renders.`,
        tone: 'danger',
        observed: true,
        toast: true,
        toastLong: true,
      };
    }
    if (error.status === 503) {
      return {
        key: '503',
        at,
        zone: 'worker',
        kind: 'demo.disabled',
        title: 'Demo temporarily disabled',
        detail:
          'The operator kill switch is on — even issued tokens are refused. Nothing is wrong with your sandbox; check back later.',
        tone: 'danger',
        observed: true,
        toast: true,
        toastSticky: true,
      };
    }
  }
  return {
    key: `startfail:${workflowId}:${at}`,
    at,
    zone: 'worker',
    kind: 'run.start-failed',
    title: `Could not start ${workflowId}`,
    detail: message,
    tone: 'danger',
    observed: true,
    toast: true,
    toastSticky: true,
  };
}

/**
 * Narrate the decider's own decide result. Pre-records the resumed/grant keys
 * the poll will re-derive, so the decider never gets a second toast for the
 * resume their decision caused.
 */
export function decideEvents(result: DecideResult): NarrationEvent[] {
  const at = Date.now();
  const { record, resume } = result;
  const decision = record.decision ?? 'approve';
  const events: NarrationEvent[] = [
    {
      key: `decide:${record.id}:${decision}`,
      at,
      zone: 'worker',
      kind: 'approval.decided',
      title:
        decision === 'approve'
          ? 'Approved — grant derived'
          : 'Rejected — no grant minted',
      detail:
        decision === 'approve'
          ? `Decision recorded. The Worker derived the connector grant and resumed the run inline (resume ok: ${resume.ok ?? 'not attempted'}).`
          : 'Decision recorded. The run resumes server-side and completes with a declined outcome.',
      tone: decision === 'approve' ? 'success' : 'warning',
      runId: record.runId,
      approvalId: record.id,
      observed: true,
      toast: true,
    },
  ];
  if (resume.attempted && resume.ok === false) {
    events.push({
      key: `resume-failed:${record.id}`,
      at,
      zone: 'worker',
      kind: 'run.resume-failed',
      title: "Decision saved; resume didn't complete",
      detail:
        'The decision is durable and the grant re-derives from the store — re-driving this run is safe.',
      tone: 'danger',
      runId: record.runId,
      approvalId: record.id,
      observed: true,
      toast: true,
      toastLong: true,
    });
  }
  if (resume.attempted && resume.ok === true) {
    const step = record.stepPath?.join('.');
    if (step) {
      const summary = resume.summary as RunSummary | undefined;
      const ordinal =
        summary?.resumeCount?.[step] ?? (record.resumeCount ?? 0) + 1;
      events.push(
        ...resumedEvents(
          {
            workflowId: record.workflowId,
            runId: record.runId,
            title: record.title,
          },
          step,
          ordinal,
          at,
          {
            connectors: record.connectors,
            declined: decision === 'reject',
            // The decide toast above already told the decider — feed only.
            toast: false,
            ok: true,
          },
        ),
      );
    }
  }
  return events;
}

/**
 * A refused decide (403): separation of duties for reviewer/admin (the
 * requester deciding their own request), the role gate for viewer/operator.
 * The server message names which; the hint applies to both.
 */
export function decideDeniedEvent(
  approvalId: string,
  message: string,
): NarrationEvent {
  return {
    key: `sod:${approvalId}`,
    at: Date.now(),
    zone: 'worker',
    kind: 'authz.denied',
    title: 'Decision refused (403)',
    detail: `${message}. Switch role — each demo role is a distinct actor id, so another role can decide this.`,
    tone: 'danger',
    approvalId,
    observed: true,
    toast: true,
    toastLong: true,
  };
}

/** Narrate the actor's own successful claim (feed only — the button shows it). */
export function claimEvent(record: ApprovalRecord): NarrationEvent {
  return {
    key: `approval:${record.id}:status:claimed`,
    at: Date.now(),
    zone: 'worker',
    kind: 'approval.claimed',
    title: `Claimed '${record.title}'`,
    detail: 'Marked as yours to review — deciding does not require a claim.',
    tone: 'info',
    runId: record.runId,
    approvalId: record.id,
    observed: true,
    toast: false,
  };
}

/** Narrate the actor's own delegation (feed only). */
export function delegateEvent(record: ApprovalRecord): NarrationEvent {
  return {
    key: `approval:${record.id}:claimedBy:${record.claimedBy ?? 'unknown'}`,
    at: Date.now(),
    zone: 'worker',
    kind: 'approval.delegated',
    title: `Delegated '${record.title}' to ${record.claimedBy ?? 'unknown'}`,
    detail:
      'Last write wins by design: delegation moves a pointer and guards no side effect.',
    tone: 'neutral',
    runId: record.runId,
    approvalId: record.id,
    observed: true,
    toast: false,
  };
}

export function actorSwitchedEvent(
  actorId: string,
  role: string,
): NarrationEvent {
  const at = Date.now();
  return {
    key: `actor:${actorId}:${at}`,
    at,
    zone: 'browser',
    kind: 'session.actor-switched',
    title: `Now acting as ${actorId} (${role})`,
    detail:
      "API clients rebuilt with this role's token. The server re-verifies identity on every call.",
    tone: 'info',
    observed: true,
    toast: true,
    // Rapid switching replaces the live toast instead of stacking three.
    toastReplaceId: 'actor-switch',
  };
}

export function sessionReadyEvent(options: {
  provider: string;
  tenantId: string;
  expiresAtMs?: number;
}): NarrationEvent {
  const at = Date.now();
  const expires =
    options.expiresAtMs !== undefined
      ? ` — expires ${new Date(options.expiresAtMs).toLocaleTimeString()}`
      : '';
  return {
    key: `session:${options.tenantId}`,
    at,
    zone: 'browser',
    kind: 'session.ready',
    title: `Sandbox ${options.tenantId} ready`,
    detail: `Signed in via ${options.provider}. Four switchable role tokens held in tab memory only${expires}. Nothing you do here is visible to other visitors.`,
    tone: 'success',
    observed: true,
    toast: true,
    toastLong: true,
  };
}

export function sessionExpiringEvent(minutesLeft: number): NarrationEvent {
  return {
    key: 'expiring:15',
    at: Date.now(),
    zone: 'browser',
    kind: 'session.expiring',
    title: `Sandbox expires in ${minutesLeft} min`,
    detail:
      'Runs, approvals, and tokens will be purged. Finish the walkthrough, or sign in later for a fresh tenant.',
    tone: 'warning',
    observed: true,
    toast: true,
    toastLong: true,
  };
}

export function sessionExpiredEvent(): NarrationEvent {
  return {
    key: 'expired',
    at: Date.now(),
    zone: 'browser',
    kind: 'session.expired',
    title: 'Sandbox expired',
    detail:
      'Tokens are no longer valid and the data is queued for purge. Sign in to mint a new isolated tenant.',
    tone: 'danger',
    observed: true,
    toast: true,
    toastSticky: true,
  };
}

export function tokenRefreshedEvent(): NarrationEvent {
  const at = Date.now();
  return {
    key: `refresh:${at}`,
    at,
    zone: 'browser',
    kind: 'session.token-refreshed',
    title: 'JWTs silently refreshed',
    detail: 'Tokens rotate every 30 minutes while the sandbox lives (1h TTL).',
    tone: 'neutral',
    observed: true,
    toast: false,
  };
}

/** Polling for a run degraded (transient failures) or stopped (hard error). */
export function pollTroubleEvent(
  runId: string,
  options: { stopped: boolean; message: string },
): NarrationEvent {
  return {
    // One key family per run — degraded then stopped keeps the first line.
    key: `poll:${runId}`,
    at: Date.now(),
    zone: 'browser',
    kind: options.stopped ? 'poll.stopped' : 'poll.degraded',
    title: options.stopped ? 'Run polling stopped' : 'Live updates degraded',
    detail: options.stopped
      ? `${options.message}. This affects only this tab's view — the run itself is unaffected server-side.`
      : 'Run status reads are failing; showing last known state.',
    tone: options.stopped ? 'danger' : 'warning',
    runId,
    observed: true,
    toast: true,
    toastSticky: options.stopped,
    toastLong: !options.stopped,
  };
}
