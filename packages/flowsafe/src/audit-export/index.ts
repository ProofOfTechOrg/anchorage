// Audit export — Cloudflare Queues integration shipping audit events to a
// SIEM (Phase 4 "Audit export to SIEM via Queues"). Producer side:
// queueAuditSink() adapts a Queue producer binding onto the audit-sink
// contracts (breakwater AuditLogger `sink` and approval-api `audit` both
// accept it — per-event send() is deliberate: producer-side buffering would
// lose events on isolate death, and Queues does the delivery batching).
// Consumer side: createAuditQueueConsumer() turns queue batches into one
// authenticated HTTP POST (NDJSON — the generic HTTP event-collector shape;
// `transform` reshapes per SIEM envelope), acking on 2xx and retrying the
// whole batch otherwise so Queues' backoff/DLQ semantics own reliability.
//
// All types are structural subsets of @cloudflare/workers-types (Queue,
// MessageBatch, fetch) so the module tests off-Workers and never forces the
// types package on consumers.

/**
 * Producer subset of a Cloudflare `Queue<TEvent>` binding (send returns
 * `Promise<QueueSendResponse>` on Workers — anything awaitable works here).
 */
export interface AuditQueue<TEvent = unknown> {
  send(message: TEvent): Promise<unknown>;
}

/**
 * Adapt a Queue producer binding onto the audit-sink contracts. Generic so
 * the returned sink assigns to any event-typed sink signature (breakwater's
 * `AuditSink`, approval-api's `ApprovalAuditSink` wrapper) without casts.
 */
export function queueAuditSink<TEvent>(
  queue: AuditQueue<TEvent>,
): (event: TEvent) => Promise<void> {
  return async (event) => {
    await queue.send(event);
  };
}

/** Consumer subset of a Cloudflare queue `Message<TEvent>`. */
export interface AuditQueueMessage<TEvent = unknown> {
  readonly body: TEvent;
  ack(): void;
  retry(): void;
}

/** Consumer subset of a Cloudflare `MessageBatch<TEvent>`. */
export interface AuditMessageBatch<TEvent = unknown> {
  readonly messages: readonly AuditQueueMessage<TEvent>[];
  ackAll(): void;
  retryAll(): void;
}

/** Structural subset of fetch — injectable for tests. */
export type AuditExportFetch = (
  url: string,
  init: {
    method: string;
    headers: Record<string, string>;
    body: string;
  },
) => Promise<{ ok: boolean; status: number }>;

export interface AuditExportOptions<TEvent = unknown> {
  /** HTTP event-collector URL (Splunk HEC, Datadog intake, Elastic, ...). */
  endpoint: string;
  /**
   * Merged over the NDJSON content-type (so it can be overridden), e.g.
   * `{ authorization: 'Splunk <token>' }`.
   */
  headers?: Record<string, string>;
  /**
   * Per-event reshaping for SIEM-specific envelopes — Splunk HEC wants
   * `(event) => ({ event })`. Default: the event itself.
   */
  transform?: (event: TEvent) => unknown;
  /** Defaults to the global fetch. */
  fetch?: AuditExportFetch;
}

/**
 * Build a Workers queue consumer exporting audit batches to a SIEM. The
 * export is all-or-nothing per batch: a failed POST retries the whole batch
 * (Queues backoff, then the configured dead-letter queue), so no event is
 * acked before the collector confirmed it.
 */
export function createAuditQueueConsumer<TEvent = unknown>(
  options: AuditExportOptions<TEvent>,
): (batch: AuditMessageBatch<TEvent>) => Promise<void> {
  const transform = options.transform ?? ((event: TEvent) => event as unknown);
  const doFetch = options.fetch ?? (fetch as unknown as AuditExportFetch);
  return async (batch) => {
    if (batch.messages.length === 0) return;
    const lines: string[] = [];
    for (const message of batch.messages) {
      // A transform or serialization that fails must drop only THAT event,
      // never reject the batch — a rejection retries every co-batched good
      // event and drags them all to the DLQ. Two failure shapes, both
      // dropped loudly so the rest keep flowing: a throw (a throwing
      // transform, or an unserializable event — BigInt, circular ref) and a
      // silent swallow (transform returns undefined → stringify undefined).
      let line: string | undefined;
      try {
        line = JSON.stringify(transform(message.body));
      } catch (error) {
        console.error(
          JSON.stringify({
            type: 'audit-export-error',
            reason: `event dropped — transform/serialize threw: ${String(error)}`,
          }),
        );
        continue;
      }
      if (typeof line === 'string') {
        lines.push(line);
      } else {
        console.error(
          JSON.stringify({
            type: 'audit-export-error',
            reason: 'transform produced no serializable event — dropped',
          }),
        );
      }
    }
    if (lines.length === 0) {
      batch.ackAll();
      return;
    }
    let ok = false;
    let status: number | undefined;
    try {
      const response = await doFetch(options.endpoint, {
        method: 'POST',
        headers: {
          'content-type': 'application/x-ndjson',
          ...options.headers,
        },
        body: lines.join('\n'),
      });
      ok = response.ok;
      status = response.status;
    } catch (error) {
      console.error(
        JSON.stringify({ type: 'audit-export-error', reason: String(error) }),
      );
    }
    if (ok) {
      batch.ackAll();
    } else {
      if (status !== undefined) {
        console.error(JSON.stringify({ type: 'audit-export-error', status }));
      }
      batch.retryAll();
    }
  };
}
