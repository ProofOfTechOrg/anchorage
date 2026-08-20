// SPDX-License-Identifier: Apache-2.0
// Per-suspension deadlines — the durable wake schedule that lets a run resume
// itself when the signal a suspended step waits for never arrives.
//
// `suspend()` is Mastra's, not flowsafe's, and a suspension is the one
// lifecycle transition the runtime persists nothing of its own for: the
// snapshot it writes is Mastra's workflow state. So the arming value cannot be
// a flowsafe option — it travels inside the payload the step hands `suspend()`,
// under a reserved dotted key (the `flowsafe.` convention the request-context
// keys already use), and this module reads it back out of the authoritative
// RunSummary that start, resume, and status already project.
//
// The derived record lives in Durable Object storage, NOT in the snapshot's
// requestContext beside RunLifecycleState (run-lifecycle.ts). A suspension
// deadline is one object's wake schedule: it has no meaning once that object's
// run leaves 'suspended', and the alarm that consumes it must read it before it
// has loaded any snapshot. Storage is also why the parse below is strict —
// stored state is untrusted input, so every field is validated and a malformed
// record throws instead of being coerced, exactly as parseRunLifecycle does.
//
// Everything here is pure; arming, wake scheduling, and the timeout resume live
// in durable-object.ts, so a non-DO host (tests, local runners) is unaffected.

import { isPathSafeId } from './path-safe-id.js';
import type { RunSummary } from './runtime.js';

/** Reserved key a step sets in its `suspend()` payload to arm a deadline. */
export const SUSPENSION_DEADLINE_PAYLOAD_KEY = 'flowsafe.deadlineMs';

/** Reserved key wrapping the resume data an expired deadline resumes with. */
export const SUSPENSION_TIMEOUT_RESUME_KEY = 'flowsafe.suspensionTimeout';

/** Durable Object storage key holding one run's armed deadlines. */
export const SUSPENSION_DEADLINE_STORAGE_KEY =
  'flowsafe:suspension-deadline:v1';

/**
 * Requester recorded on a timeout resume. A self-initiated resume must be
 * distinguishable in run provenance from the human or automation that advanced
 * the run to the suspension, so it files under a reserved system id of its own
 * rather than borrowing the recorded requester's identity.
 */
export const SUSPENSION_DEADLINE_PRINCIPAL_ID = 'flowsafe-suspension-deadline';

/** Shortest armable deadline; anything smaller is a tight arm-then-wake loop. */
export const MIN_SUSPENSION_DEADLINE_MS = 1_000;

/** Longest armable deadline (365 days), so week-scale waits still fit. */
export const MAX_SUSPENSION_DEADLINE_MS = 31_536_000_000;

/** Per-run entry cap; a run cannot grow its object's storage without bound. */
export const MAX_SUSPENSION_DEADLINES_PER_RUN = 32;

/** Failed timeout resumes tolerated before an entry is abandoned. */
export const MAX_SUSPENSION_DEADLINE_ATTEMPTS = 5;

/**
 * Mastra's marker for a suspension that happened inside a NESTED workflow: the
 * persisted payload carries the inner step path under `__workflow_meta.path`.
 * Verified in Mastra 1.50.0; `runtime.test.ts` pins it, because a rename would
 * otherwise silently re-open the arming of unfenceable nested suspensions.
 */
export const MASTRA_WORKFLOW_META_KEY = '__workflow_meta';

// Step ids are bounded for the same reason every other stored free-text field
// is: a stored record must stay small enough to read back on every alarm wake.
const MAX_SUSPENSION_DEADLINE_STEP_LENGTH = 500;

export interface SuspensionDeadlineEntry {
  /**
   * The suspended step's DOT-JOINED path, which is how both RunSummary
   * projections key `suspendPayload`, `suspendedAt` and `resumeCount`. Nested
   * paths are never armed, so this is a top-level step id — one that may
   * itself contain a dot.
   */
  step: string;
  /** Epoch milliseconds: the suspension's own time plus its deadline. */
  deadlineAt: number;
  /** Fence: the step's suspension time in the summary that armed this entry. */
  suspendedAt: number;
  /** Fence: the step's resume ordinal, normalized to 0 on a first suspension. */
  resumeCount: number;
  /**
   * Failed timeout resumes so far; absent until the first failure. At the full
   * budget the entry is a TOMBSTONE: abandoned for the suspension it was armed
   * against, never selected and never armed again, and kept in the record only
   * so that suspension cannot re-derive itself a fresh budget.
   */
  attempts?: number;
  /**
   * Epoch milliseconds; paired with `attempts` as its backoff floor. A
   * tombstone carries none, because it is never retried.
   */
  nextAttemptAt?: number;
  /**
   * Epoch milliseconds of the FIRST wake whose authoritative read did not
   * succeed, absent while reads are working. Such a wake charges nothing — an
   * unread run is no evidence about the entry — so this is the only thing
   * bounding an entry whose run became permanently unreadable; the wake that
   * owns it abandons the entry once it has stood for a day. Any successful
   * read clears it.
   */
  unreadableSince?: number;
}

export interface SuspensionDeadlineRecord {
  version: 1;
  workflowId: string;
  runId: string;
  entries: SuspensionDeadlineEntry[];
}

export interface SuspensionTimeoutEnvelope {
  /** Id of the step whose deadline expired. */
  step: string;
  /** Epoch milliseconds the deadline was armed for. */
  deadlineAt: number;
  /** Epoch milliseconds the wake acted on it. */
  expiredAt: number;
}

/** Resume data a timeout resume carries, distinguishable from a real signal. */
export type SuspensionTimeoutResumeData = {
  [SUSPENSION_TIMEOUT_RESUME_KEY]: SuspensionTimeoutEnvelope;
};

/** A derived deadline that will NOT be armed, with the reason to log. */
export interface RejectedSuspensionDeadline {
  step: string;
  reason: string;
}

const MALFORMED = 'stored suspension deadline is malformed';

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

// Every number this module persists — an epoch millisecond and the resume
// ordinal alike — has to clear the same bar, so the predicate is named for the
// bar rather than for one of its callers. run-lifecycle.ts names the identical
// check `validTime` because there it only ever guards timestamps.
function nonNegativeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

function validStep(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length > 0 &&
    value.length <= MAX_SUSPENSION_DEADLINE_STEP_LENGTH
  );
}

// Read Mastra's nesting marker defensively: the payload is author data carrying
// one framework-owned field, so a renamed, missing or reshaped marker must
// degrade to "not nested" here rather than throw on a suspension flowsafe has
// already been handed. The tripwire test is what makes such a rename loud.
function nestedPath(
  payload: Record<string, unknown>,
): readonly unknown[] | undefined {
  const path = record(payload[MASTRA_WORKFLOW_META_KEY])?.path;
  return Array.isArray(path) && path.length > 0 ? path : undefined;
}

/**
 * Detector for the "exclusively" half of the arming invariant (see
 * suspensionDeadlinesOf): every joined key that more than one suspension of
 * this run answers to. Each projection shows a collision differently, so both
 * rules are needed: two suspended paths joining to one key can only appear on
 * the live projection (the rehydrated one splits unique stored keys), and a
 * key implied by another suspended key's nesting marker only on the rehydrated
 * one (live nested payloads carry no marker).
 *
 * Both rules read only each suspended key's OWN payload, never the
 * nested-payload fallback below, which deliberately reads a different step's
 * payload.
 */
function ambiguousSuspendedKeys(
  suspended: readonly (readonly string[])[],
  payloads: Record<string, unknown> | undefined,
): Set<string> {
  const seen = new Set<string>();
  const ambiguous = new Set<string>();
  for (const path of suspended) {
    const key = path.join('.');
    if (seen.has(key)) ambiguous.add(key);
    seen.add(key);
    const payload = record(payloads?.[key]);
    const nested = payload && nestedPath(payload);
    if (nested) ambiguous.add([key, ...nested].join('.'));
  }
  return ambiguous;
}

function storedEntry(value: unknown): SuspensionDeadlineEntry {
  const stored = record(value);
  if (!stored) throw new Error(MALFORMED);
  const { attempts, nextAttemptAt, unreadableSince } = stored;
  // A ledger below the budget is still retrying and needs its floor; one AT the
  // budget is a tombstone, which is never selected again and so must carry no
  // floor. The two fields are otherwise one fact — a count with no floor would
  // retry immediately, a floor with no count would retry forever.
  const retrying =
    attempts !== undefined && attempts !== MAX_SUSPENSION_DEADLINE_ATTEMPTS;
  if (
    !validStep(stored.step) ||
    !nonNegativeInteger(stored.deadlineAt) ||
    !nonNegativeInteger(stored.suspendedAt) ||
    !nonNegativeInteger(stored.resumeCount) ||
    // The budget itself reads back, because a spent one stays as a tombstone.
    // More than the budget cannot have been written here, and accepting it
    // would let a foreign writer make this module's own log say "after 6".
    (attempts !== undefined &&
      (!Number.isSafeInteger(attempts) ||
        (attempts as number) < 1 ||
        (attempts as number) > MAX_SUSPENSION_DEADLINE_ATTEMPTS)) ||
    (nextAttemptAt !== undefined) !== retrying ||
    (nextAttemptAt !== undefined && !nonNegativeInteger(nextAttemptAt)) ||
    // Unpaired with the ledger, unlike the floor above: it records how long
    // reads have been failing, which is independent of how many resumes have.
    (unreadableSince !== undefined && !nonNegativeInteger(unreadableSince))
  ) {
    throw new Error(MALFORMED);
  }
  return {
    step: stored.step,
    deadlineAt: stored.deadlineAt,
    suspendedAt: stored.suspendedAt,
    resumeCount: stored.resumeCount,
    ...(attempts === undefined ? {} : { attempts: attempts as number }),
    ...(nextAttemptAt === undefined
      ? {}
      : { nextAttemptAt: nextAttemptAt as number }),
    ...(unreadableSince === undefined
      ? {}
      : { unreadableSince: unreadableSince as number }),
  };
}

// Total order on (deadlineAt, step) so the entry a wake acts on is
// deterministic even for equal deadlines. It does not order the stored record:
// a retry charge appends the entry it re-armed.
function byDueThenStep(
  left: SuspensionDeadlineEntry,
  right: SuspensionDeadlineEntry,
): number {
  if (left.deadlineAt !== right.deadlineAt) {
    return left.deadlineAt - right.deadlineAt;
  }
  return left.step < right.step ? -1 : left.step > right.step ? 1 : 0;
}

/**
 * Read every armable deadline out of an authoritative suspended summary.
 *
 * NEVER throws on author data. By the time this runs Mastra has already
 * persisted the suspension, so a malformed `flowsafe.deadlineMs` cannot be
 * rejected by failing the caller: that would leave the run suspended AND the
 * caller erroring. The entry is reported in `rejected` for the caller to log,
 * and the suspension stays an ordinary one.
 *
 * Works on either projection of the same suspension — the live result and the
 * snapshot-rehydrated `status()` — and must agree with itself across the two,
 * because the boundary that arms an entry and the wake that fences it do not
 * read the same one.
 *
 * The arming invariant: an entry may be armed only for a suspension that
 * EXCLUSIVELY OWNS the dot-joined key its fence fields are read from. A
 * nested suspension fails "owns" — the key's fence belongs to the enclosing
 * step — and a key collision fails "exclusively" — two suspensions answer to
 * one key, which Mastra's own snapshot namespace also collides. Refusing is
 * all flowsafe can do there: arming would let a wake deliver one suspension's
 * timeout envelope to an unrelated step. Each refusal below is a detector for
 * one projection's way of breaking that invariant.
 */
export function suspensionDeadlinesOf(summary: RunSummary): {
  entries: SuspensionDeadlineEntry[];
  rejected: RejectedSuspensionDeadline[];
} {
  const entries: SuspensionDeadlineEntry[] = [];
  const rejected: RejectedSuspensionDeadline[] = [];
  if (summary.status !== 'suspended') return { entries, rejected };
  const payloads = record(summary.suspendPayload);
  const suspended = summary.suspended ?? [];
  const ambiguous = ambiguousSuspendedKeys(suspended, payloads);
  for (const path of suspended) {
    // The DOT-JOINED path is the entry key, because it is what indexes
    // `suspendPayload`, `suspendedAt` and `resumeCount` in BOTH projections
    // (['a.b'] live, ['a','b'] rehydrated), so the wake's fence recognizes
    // what a boundary armed.
    const step = path.join('.');
    const payload = record(payloads?.[step]);
    // A LIVE nested suspension is the one shape whose payload is NOT under its
    // own path: Mastra reports the nested path but keys the payload by the
    // TOP-LEVEL step. Reading it there is what turns a nested arming request
    // into a loud rejection instead of silence.
    const nestedPayload =
      payload === undefined && path.length > 1
        ? record(payloads?.[path[0] as string])
        : undefined;
    const armed = payload ?? nestedPayload;
    // No payload, or no reserved key in it: an ordinary suspension, silently.
    if (!armed) continue;
    const deadlineMs = armed[SUSPENSION_DEADLINE_PAYLOAD_KEY];
    if (deadlineMs === undefined) continue;
    // "Exclusively" is checked before "owns" so the log names the sharper
    // reason, and after the silent exits above so a run that asked for nothing
    // still says nothing.
    if (ambiguous.has(step)) {
      rejected.push({ step, reason: 'ambiguous suspended step path' });
      continue;
    }
    // Detector for the "owns" half of the invariant, and a LOUD one — silence
    // here would look to the author exactly like a deadline that was accepted.
    // Each projection presents a nested suspension its own way: the live one
    // as a multi-segment path whose payload sits under its first segment
    // (`nestedPayload` above), the rehydrated one as the top-level step alone,
    // recognizable only by Mastra's nesting marker inside the payload.
    if (nestedPayload !== undefined || nestedPath(armed) !== undefined) {
      rejected.push({
        step,
        reason: 'nested suspension paths are not supported',
      });
      continue;
    }
    if (!validStep(step)) {
      rejected.push({ step, reason: 'suspended step id is unusable' });
      continue;
    }
    if (!Number.isSafeInteger(deadlineMs)) {
      rejected.push({
        step,
        reason: `${SUSPENSION_DEADLINE_PAYLOAD_KEY} must be a safe integer`,
      });
      continue;
    }
    if (
      (deadlineMs as number) < MIN_SUSPENSION_DEADLINE_MS ||
      (deadlineMs as number) > MAX_SUSPENSION_DEADLINE_MS
    ) {
      rejected.push({
        step,
        reason: `${SUSPENSION_DEADLINE_PAYLOAD_KEY} must be between ${MIN_SUSPENSION_DEADLINE_MS} and ${MAX_SUSPENSION_DEADLINE_MS} ms`,
      });
      continue;
    }
    const suspendedAt = summary.suspendedAt?.[step];
    // A deadline that cannot be fenced must not be armed: without suspendedAt
    // the alarm could not tell this suspension from a later one at the same
    // step, and would resume a run a real signal had already moved on.
    if (!nonNegativeInteger(suspendedAt)) {
      rejected.push({ step, reason: 'no suspendedAt fence' });
      continue;
    }
    // Absent resumeCount is normal on a step's FIRST suspension; the runtime
    // only stamps the ordinal once the step has been resumed.
    const resumeCount = summary.resumeCount?.[step];
    if (resumeCount !== undefined && !nonNegativeInteger(resumeCount)) {
      rejected.push({ step, reason: 'resumeCount fence is malformed' });
      continue;
    }
    // suspendedAt + deadlineMs, never now + deadlineMs: re-deriving from an
    // unchanged suspension must yield a byte-identical entry, or repeated
    // reconciliation would walk the deadline forward and it would never fire.
    const deadlineAt = suspendedAt + (deadlineMs as number);
    if (!Number.isSafeInteger(deadlineAt)) {
      rejected.push({
        step,
        reason: `${SUSPENSION_DEADLINE_PAYLOAD_KEY} exceeds the supported range`,
      });
      continue;
    }
    entries.push({
      step,
      deadlineAt,
      suspendedAt,
      resumeCount: resumeCount ?? 0,
    });
  }
  entries.sort(byDueThenStep);
  for (const overflow of entries.splice(MAX_SUSPENSION_DEADLINES_PER_RUN)) {
    rejected.push({
      step: overflow.step,
      reason: `run already arms ${MAX_SUSPENSION_DEADLINES_PER_RUN} suspension deadlines`,
    });
  }
  return { entries, rejected };
}

/**
 * Whether a summary is self-consistent enough to reconcile from: a suspended
 * run always has at least one suspended path, so a 'suspended' status with
 * none describes no suspension anyone could act on. Reconciling from it would
 * derive nothing and wipe the stored record of a run that is still suspended.
 *
 * The SECOND line of defence, not the primary one. It rejects that projection
 * whatever produced it, but it does not cover Mastra's in-memory fallback in
 * general: the fallback reports the Run object's own status, which is
 * 'pending' for a run that has never been resumed — the dominant case, and one
 * this predicate calls readable. The primary guard is the `isFromInMemory`
 * marker check inside `RunnerRuntime.authoritativeStatus`, which is keyed on
 * the read having reached storage rather than on the shape it returned. The
 * two signals are independent, so both are kept.
 */
export function isReadableRunSummary(summary: RunSummary): boolean {
  return !(
    summary.status === 'suspended' && (summary.suspended ?? []).length === 0
  );
}

/** Strict hydration of the stored record; `undefined` in, `undefined` out. */
export function parseSuspensionDeadlineRecord(
  value: unknown,
): SuspensionDeadlineRecord | undefined {
  if (value === undefined) return undefined;
  const stored = record(value);
  if (
    stored?.version !== 1 ||
    !isPathSafeId(stored.workflowId) ||
    !isPathSafeId(stored.runId) ||
    !Array.isArray(stored.entries) ||
    stored.entries.length > MAX_SUSPENSION_DEADLINES_PER_RUN
  ) {
    throw new Error(MALFORMED);
  }
  return {
    version: 1,
    workflowId: stored.workflowId,
    runId: stored.runId,
    entries: stored.entries.map(storedEntry),
  };
}

/**
 * A spent entry, kept for the suspension it was armed against: the ledger at
 * the budget, and every field that only means something while it is still
 * being retried — the backoff floor, the unreadable-state clock — dropped,
 * because a tombstone is never selected, never armed and never read again.
 * Lives beside its two recognizers, `abandoned()` and `storedEntry`'s ledger
 * rule, so the shape they accept and the shape written here cannot drift.
 * Exported from this module for the run object that writes it; deliberately
 * NOT from the package barrel, which exposes no part of the stored record.
 */
export function tombstoned(
  entry: SuspensionDeadlineEntry,
): SuspensionDeadlineEntry {
  return {
    step: entry.step,
    deadlineAt: entry.deadlineAt,
    suspendedAt: entry.suspendedAt,
    resumeCount: entry.resumeCount,
    attempts: MAX_SUSPENSION_DEADLINE_ATTEMPTS,
  };
}

// A spent ledger is a tombstone: the entry stays stored so its suspension
// cannot re-derive itself a fresh budget, but both selectors below must skip
// it — selecting it would retry past the budget, and arming for it would wake
// the object at its long-past deadline forever.
function abandoned(entry: SuspensionDeadlineEntry): boolean {
  return (entry.attempts ?? 0) >= MAX_SUSPENSION_DEADLINE_ATTEMPTS;
}

/** When an entry needs its wake: the deadline, or the retry floor past it. */
function dueAt(entry: SuspensionDeadlineEntry): number {
  return Math.max(entry.deadlineAt, entry.nextAttemptAt ?? 0);
}

/** Earliest wake the record needs, honouring a retry floor over a due entry. */
export function nextSuspensionDeadlineAt(
  stored: SuspensionDeadlineRecord | undefined,
): number | undefined {
  let next: number | undefined;
  for (const entry of stored?.entries ?? []) {
    if (abandoned(entry)) continue;
    const due = dueAt(entry);
    if (next === undefined || due < next) next = due;
  }
  return next;
}

/**
 * Whether a wake at `now` owes this entry work: it has reached its deadline
 * (or the retry floor past it) and its budget is not spent. One wake resumes
 * ONE such entry, but a failed authoritative read is one fact about all of
 * them, so the unreadable-state clock is stamped across the whole due batch.
 */
export function isSuspensionDeadlineDue(
  entry: SuspensionDeadlineEntry,
  now: number,
): boolean {
  return !abandoned(entry) && dueAt(entry) <= now;
}

/** The one entry a wake at `now` should act on, earliest deadline first. */
export function dueSuspensionDeadline(
  stored: SuspensionDeadlineRecord | undefined,
  now: number,
): SuspensionDeadlineEntry | undefined {
  let due: SuspensionDeadlineEntry | undefined;
  for (const entry of stored?.entries ?? []) {
    if (!isSuspensionDeadlineDue(entry, now)) continue;
    if (!due || byDueThenStep(entry, due) < 0) due = entry;
  }
  return due;
}

/** The resume data a timeout resume delivers to the expired step. */
export function suspensionTimeoutResumeData(
  entry: SuspensionDeadlineEntry,
  expiredAt: number,
): SuspensionTimeoutResumeData {
  return {
    [SUSPENSION_TIMEOUT_RESUME_KEY]: {
      step: entry.step,
      deadlineAt: entry.deadlineAt,
      expiredAt,
    },
  };
}

/**
 * Structural guard a step uses to tell a timeout resume from a real signal,
 * so consumer code branches on this contract instead of a string literal.
 */
export function isSuspensionTimeoutResumeData(
  value: unknown,
): value is SuspensionTimeoutResumeData {
  const envelope = record(record(value)?.[SUSPENSION_TIMEOUT_RESUME_KEY]);
  return (
    envelope !== undefined &&
    validStep(envelope.step) &&
    nonNegativeInteger(envelope.deadlineAt) &&
    nonNegativeInteger(envelope.expiredAt)
  );
}

/**
 * Carry the retry ledger of a stored entry onto the freshly derived one for
 * the SAME suspension. An entry whose fence or deadline moved is a different
 * suspension, so it starts with a clean ledger; a stored entry with no derived
 * counterpart is gone and is dropped. This carry is also the tombstone's whole
 * lifecycle: a spent ledger rides every reconcile of the unchanged suspension
 * — which is what keeps the suspension from re-deriving a fresh budget — and
 * is dropped the moment the suspension moves on, so a later suspension of the
 * same step starts over.
 *
 * `unreadableSince` is deliberately NOT carried, and that is what clears it: a
 * reconcile only ever follows evidence that the run is real and current — an
 * authoritative read that succeeded, or a live summary handed back by a
 * lifecycle boundary — and either ends the unreadable stretch the clock was
 * counting. A host that drives boundaries oftener than daily therefore keeps
 * the clock from maturing even while the row read keeps failing; in the case
 * the bound exists for — a registration a deploy dropped for good — the
 * boundaries throw too, so nothing clears it and the bound holds.
 */
export function mergeSuspensionDeadlines(
  previous: SuspensionDeadlineRecord | undefined,
  derived: readonly SuspensionDeadlineEntry[],
): SuspensionDeadlineEntry[] {
  return derived.map((entry) => {
    const carried = previous?.entries.find(
      (stored) =>
        stored.step === entry.step &&
        stored.suspendedAt === entry.suspendedAt &&
        stored.resumeCount === entry.resumeCount &&
        stored.deadlineAt === entry.deadlineAt,
    );
    return carried?.attempts === undefined
      ? entry
      : {
          ...entry,
          attempts: carried.attempts,
          // A tombstone carries no floor; spreading `undefined` back in would
          // store a property the strict parser then has to treat as absent.
          ...(carried.nextAttemptAt === undefined
            ? {}
            : { nextAttemptAt: carried.nextAttemptAt }),
        };
  });
}
