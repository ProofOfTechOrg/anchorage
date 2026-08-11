// SPDX-License-Identifier: Apache-2.0
// Audit — structured audit logging shared by every breakwater gate (RBAC,
// policy engine, connector SDK) and external consumers (flowsafe approval-api
// adapts its events onto this sink). Mastra ships no audit logging at any
// tier, which is why this module exists.
//
// AuditLogger is a shared sink — NOT a peer processor — because a denial
// aborts the processor chain, so a processor placed after a gate could never
// observe the denial. Each gate records its own audit event as it fires.

import type { RequestContext } from '@mastra/core/request-context';

import type { Actor } from '../rbac/index.js';

/** Request-context key for trusted agent and run correlation fields. */
export const AGENT_AUDIT_CONTEXT_KEY = 'breakwater.auditContext';

/**
 * Trusted scalar correlation attached to agent authorization and policy
 * events. Hosts must derive these values instead of accepting them from
 * prompts, tool inputs, or other untrusted payloads.
 */
export interface AgentAuditContext {
  /** Guarded agent identifier. */
  agentId: string;
  /** Tenant identifier asserted by the authenticated host. */
  tenantId?: string;
  /** Server-minted run identifier. */
  runId?: string;
  /** Server-minted or tenant-owned thread identifier. */
  threadId?: string;
  /** Server-derived memory resource identifier. */
  resourceId?: string;
  /** Trusted invocation path, such as an HTTP start or approval resume. */
  entryPath: string;
  /** Which kind of principal is executing: human, service, agent, or system. */
  principalKind?: string;
  /**
   * Stable principal identifier. Distinct from the audit event's `actor.id`,
   * which is the identity the gate evaluated: on an approval resume the actor
   * is the restored original principal while the decision belongs to a human
   * reviewer, and a correlated trail needs to name both.
   */
  principalId?: string;
  /** Why an automated principal exists, as declared by the host. */
  purpose?: string;
  /** The principal that delegated this execution, for agent-to-agent work. */
  delegatedBy?: string;
}

const AGENT_AUDIT_OPTIONAL_FIELDS = [
  'tenantId',
  'runId',
  'threadId',
  'resourceId',
  'principalKind',
  'principalId',
  'purpose',
  'delegatedBy',
] as const;

/**
 * Read only the documented scalar fields from trusted request context.
 *
 * Unknown properties are ignored, so prompts, tool inputs, URLs, secrets, and
 * model output cannot reach audit detail through this correlation seam.
 */
export function agentAuditContextFromRequestContext(
  requestContext: RequestContext | undefined,
): AgentAuditContext | undefined {
  const value = requestContext?.get(AGENT_AUDIT_CONTEXT_KEY);
  if (!value || typeof value !== 'object') return undefined;
  try {
    const candidate = value as Record<string, unknown>;
    if (
      typeof candidate.agentId !== 'string' ||
      candidate.agentId.length === 0 ||
      typeof candidate.entryPath !== 'string' ||
      candidate.entryPath.length === 0
    ) {
      return undefined;
    }
    const context: AgentAuditContext = {
      agentId: candidate.agentId,
      entryPath: candidate.entryPath,
    };
    for (const field of AGENT_AUDIT_OPTIONAL_FIELDS) {
      const fieldValue = candidate[field];
      if (typeof fieldValue === 'string' && fieldValue.length > 0) {
        context[field] = fieldValue;
      }
    }
    return context;
  } catch {
    return undefined;
  }
}

/**
 * Merge trusted correlation with boundary-specific detail.
 *
 * Boundary fields win if names collide so a policy or channel decision cannot
 * be relabeled by request context.
 *
 * @internal
 */
export function agentAuditDetail(
  requestContext: RequestContext | undefined,
  detail?: Record<string, unknown>,
): Record<string, unknown> | undefined {
  const correlation = agentAuditContextFromRequestContext(requestContext);
  if (!correlation) return detail;
  return { ...correlation, ...detail };
}

/** Structured record emitted by a breakwater enforcement boundary. */
export interface AuditEvent {
  /** ISO 8601 */
  timestamp: string;
  /** Actor attributed to the decision, or `null` when none was resolved. */
  actor: Actor | null;
  /** Dotted verb, e.g. 'agent.input.authorize' */
  action: string;
  /** What was gated, e.g. the processor id */
  resource: string;
  /** 'error' = the gate itself failed (evaluator/getActor threw), not a denial. */
  decision: 'allowed' | 'denied' | 'error';
  /** Human-readable decision or failure reason. */
  reason?: string;
  /** Additional structured fields supplied by the emitting boundary. */
  detail?: Record<string, unknown>;
}

/** Destination for one synchronous or asynchronous audit event. */
export type AuditSink = (event: AuditEvent) => void | Promise<void>;

/** Configuration for `AuditLogger`. */
export interface AuditLoggerOptions {
  /** Optional external destination. The logger buffers records regardless. */
  sink?: AuditSink;
  /** Sink failures must not break the agent path; surface them here instead. */
  onSinkError?: (error: unknown, event: AuditEvent) => void;
  /** In-memory ring buffer capacity. Oldest events drop first. */
  maxBuffered?: number;
}

/** In-memory audit ring buffer with an optional failure-isolated sink. */
export class AuditLogger {
  readonly #sink?: AuditSink;
  readonly #onSinkError?: (error: unknown, event: AuditEvent) => void;
  readonly #maxBuffered: number;
  #buffer: AuditEvent[] = [];

  constructor(options: AuditLoggerOptions = {}) {
    this.#sink = options.sink;
    this.#onSinkError = options.onSinkError;
    this.#maxBuffered = options.maxBuffered ?? 1000;
  }

  /** Stamp, buffer, and optionally export an audit event. */
  record(event: Omit<AuditEvent, 'timestamp'>): AuditEvent {
    const stamped: AuditEvent = {
      timestamp: new Date().toISOString(),
      ...event,
    };
    this.#buffer.push(stamped);
    if (this.#buffer.length > this.#maxBuffered) {
      this.#buffer.splice(0, this.#buffer.length - this.#maxBuffered);
    }
    if (this.#sink) {
      // Availability over export reliability: a failing sink must not abort
      // the agent run. The buffer keeps the event; the error goes to
      // onSinkError.
      try {
        const result = this.#sink(stamped);
        if (result instanceof Promise) {
          result.catch((error: unknown) => this.#onSinkError?.(error, stamped));
        }
      } catch (error) {
        this.#onSinkError?.(error, stamped);
      }
    }
    return stamped;
  }

  /** Return a snapshot of the currently buffered events. */
  events(): readonly AuditEvent[] {
    return [...this.#buffer];
  }

  /** Whether this logger exports records beyond its in-memory ring buffer. */
  hasExternalSink(): boolean {
    return this.#sink !== undefined;
  }

  /** Remove every buffered event without changing the configured sink. */
  clear(): void {
    this.#buffer = [];
  }
}

/** Counters/histograms sink — any metrics client (StatsD, Prometheus push, OTel) can implement this. */
export interface MetricsRecorder {
  /** Increment a named counter with optional string tags. */
  increment(name: string, tags?: Record<string, string>): void;
  /** Observe a numeric value for a named metric with optional string tags. */
  observe(name: string, value: number, tags?: Record<string, string>): void;
}

/**
 * Adapts every AuditEvent onto a MetricsRecorder: `breakwater.audit.decision`
 * increments with `{action, decision}` tags — denials, tripwires
 * (`decision: 'error'`), rate-limit rejections, escalations, and SLA
 * breaches all become queryable via those two dimensions without enumerating
 * actions. When `event.detail?.durationSeconds` is a finite number, it also
 * observes `breakwater.audit.duration_seconds` with a `{action}` tag — a
 * generic convention any emitter can adopt (this module does not emit one
 * itself). No Mastra-tracing duplication: counters/histograms over audit
 * events only, never spans.
 *
 * Never throws on a missing, absent, or malformed `detail` — a non-number,
 * NaN, Infinity, or NEGATIVE `durationSeconds` simply skips the observe call
 * (a duration histogram must stay non-negative; cross-isolate clock skew can
 * stamp a decide before its create, and that skew is not a duration).
 */
export function metricsAuditSink(metrics: MetricsRecorder): AuditSink {
  return (event: AuditEvent): void => {
    metrics.increment('breakwater.audit.decision', {
      action: event.action,
      decision: event.decision,
    });
    const duration = event.detail?.durationSeconds;
    if (
      typeof duration === 'number' &&
      Number.isFinite(duration) &&
      duration >= 0
    ) {
      metrics.observe('breakwater.audit.duration_seconds', duration, {
        action: event.action,
      });
    }
  };
}

/**
 * Fans one event out to every sink. Each sink runs even if an earlier one
 * throws: a sink's SYNCHRONOUS throw is caught and isolated so the rest still
 * run, and every collected sync error is rethrown together — after all sinks
 * have run — as one AggregateError, so AuditLogger.record's existing
 * try/catch -> onSinkError still surfaces them (matching the containment a
 * single sink already gets).
 *
 * A sink MAY return a promise. Every such promise is collected and awaited
 * via Promise.allSettled rather than Promise.all: allSettled always waits
 * for every pending sink to SETTLE (fulfilled or rejected) before this
 * function's own returned promise resolves, so one async sink rejecting
 * early never short-circuits — and therefore never hides — a later sink's
 * failure the way Promise.all's reject-on-first-rejection would. Every
 * pending promise still gets a handler attached (via allSettled itself, not
 * a hand-rolled per-promise .catch), so nothing is ever left as an unhandled
 * rejection — including the sync-throw-plus-pending-promises case: sync
 * errors collected during the loop are merged with any async rejections
 * once every pending promise has settled, and the combined AggregateError
 * (if any) is thrown from the returned promise. When no sink returned a
 * promise, the sync errors (if any) are thrown immediately and the function
 * returns void synchronously, keeping AuditLogger.record's plain sync
 * try/catch path for an all-sync sink set.
 */
export function combineAuditSinks(...sinks: readonly AuditSink[]): AuditSink {
  return (event: AuditEvent): void | Promise<void> => {
    const syncErrors: unknown[] = [];
    const pending: Promise<void>[] = [];
    for (const sink of sinks) {
      try {
        const result = sink(event);
        if (result instanceof Promise) pending.push(result);
      } catch (error) {
        syncErrors.push(error);
      }
    }
    if (pending.length === 0) {
      if (syncErrors.length > 0) {
        throw new AggregateError(
          syncErrors,
          'combineAuditSinks: one or more sinks failed',
        );
      }
      return;
    }
    return Promise.allSettled(pending).then((results) => {
      const errors = [...syncErrors];
      for (const result of results) {
        if (result.status === 'rejected') errors.push(result.reason);
      }
      if (errors.length > 0) {
        throw new AggregateError(
          errors,
          'combineAuditSinks: one or more sinks failed',
        );
      }
    });
  };
}
