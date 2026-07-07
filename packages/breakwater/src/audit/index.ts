// Audit — structured audit logging shared by every breakwater gate (RBAC,
// policy engine, connector SDK) and external consumers (flowsafe approval-api
// adapts its events onto this sink). Mastra ships no audit logging at any
// tier, which is why this module exists.
//
// AuditLogger is a shared sink — NOT a peer processor — because a denial
// aborts the processor chain, so a processor placed after a gate could never
// observe the denial. Each gate records its own audit event as it fires.

import type { Actor } from '../rbac/index.js';

export interface AuditEvent {
  /** ISO 8601 */
  timestamp: string;
  actor: Actor | null;
  /** Dotted verb, e.g. 'agent.input.authorize' */
  action: string;
  /** What was gated, e.g. the processor id */
  resource: string;
  /** 'error' = the gate itself failed (evaluator/getActor threw), not a denial. */
  decision: 'allowed' | 'denied' | 'error';
  reason?: string;
  detail?: Record<string, unknown>;
}

export type AuditSink = (event: AuditEvent) => void | Promise<void>;

export interface AuditLoggerOptions {
  /** External export path (D1 / Queues in Phase 3). Buffer records regardless. */
  sink?: AuditSink;
  /** Sink failures must not break the agent path; surface them here instead. */
  onSinkError?: (error: unknown, event: AuditEvent) => void;
  /** In-memory ring buffer capacity. Oldest events drop first. */
  maxBuffered?: number;
}

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

  events(): readonly AuditEvent[] {
    return [...this.#buffer];
  }

  clear(): void {
    this.#buffer = [];
  }
}
