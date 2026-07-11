// SPDX-License-Identifier: Apache-2.0
import { describe, expect, it } from 'vitest';

import type { AuditEvent, AuditSink, MetricsRecorder } from './index.js';
import { AuditLogger, combineAuditSinks, metricsAuditSink } from './index.js';

describe('AuditLogger', () => {
  it('caps the buffer at maxBuffered, dropping oldest first', () => {
    // #given
    const audit = new AuditLogger({ maxBuffered: 2 });

    // #when
    for (const n of [1, 2, 3]) {
      audit.record({
        actor: null,
        action: `a${n}`,
        resource: 'r',
        decision: 'allowed',
      });
    }

    // #then
    expect(audit.events().map((e) => e.action)).toEqual(['a2', 'a3']);
  });

  it('keeps the event and reports via onSinkError when a sync sink throws', () => {
    // #given
    const sinkErrors: unknown[] = [];
    const audit = new AuditLogger({
      sink: () => {
        throw new Error('sink down');
      },
      onSinkError: (error) => sinkErrors.push(error),
    });

    // #when
    audit.record({
      actor: null,
      action: 'a',
      resource: 'r',
      decision: 'allowed',
    });

    // #then
    expect(audit.events()).toHaveLength(1);
    expect(sinkErrors).toHaveLength(1);
  });

  it('reports async sink rejections via onSinkError', async () => {
    // #given
    const sinkErrors: unknown[] = [];
    const audit = new AuditLogger({
      sink: () => Promise.reject(new Error('async sink down')),
      onSinkError: (error) => sinkErrors.push(error),
    });

    // #when
    audit.record({
      actor: null,
      action: 'a',
      resource: 'r',
      decision: 'allowed',
    });
    await new Promise((resolve) => setTimeout(resolve, 0));

    // #then
    expect(sinkErrors).toHaveLength(1);
    expect(audit.events()).toHaveLength(1);
  });
});

function makeEvent(overrides: Partial<AuditEvent> = {}): AuditEvent {
  return {
    timestamp: '2026-01-01T00:00:00.000Z',
    actor: null,
    action: 'agent.input.policy',
    resource: 'breakwater-policy-engine',
    decision: 'allowed',
    ...overrides,
  };
}

function mockMetrics(): MetricsRecorder & {
  increments: Array<{ name: string; tags?: Record<string, string> }>;
  observes: Array<{
    name: string;
    value: number;
    tags?: Record<string, string>;
  }>;
} {
  const increments: Array<{ name: string; tags?: Record<string, string> }> = [];
  const observes: Array<{
    name: string;
    value: number;
    tags?: Record<string, string>;
  }> = [];
  return {
    increments,
    observes,
    increment(name, tags) {
      increments.push({ name, tags });
    },
    observe(name, value, tags) {
      observes.push({ name, value, tags });
    },
  };
}

describe('metricsAuditSink', () => {
  it('increments breakwater.audit.decision tagged with action and decision for an allowed event', () => {
    // #given
    const metrics = mockMetrics();
    const sink = metricsAuditSink(metrics);

    // #when
    sink(makeEvent({ action: 'agent.input.policy', decision: 'allowed' }));

    // #then
    expect(metrics.increments).toEqual([
      {
        name: 'breakwater.audit.decision',
        tags: { action: 'agent.input.policy', decision: 'allowed' },
      },
    ]);
  });

  it('tags a denied event with its own action and decision', () => {
    // #given
    const metrics = mockMetrics();
    const sink = metricsAuditSink(metrics);

    // #when
    sink(makeEvent({ action: 'tool.execute.policy', decision: 'denied' }));

    // #then
    expect(metrics.increments).toEqual([
      {
        name: 'breakwater.audit.decision',
        tags: { action: 'tool.execute.policy', decision: 'denied' },
      },
    ]);
  });

  it('tags an error (tripwire) event with decision: error', () => {
    // #given
    const metrics = mockMetrics();
    const sink = metricsAuditSink(metrics);

    // #when
    sink(makeEvent({ action: 'agent.output.policy', decision: 'error' }));

    // #then
    expect(metrics.increments).toEqual([
      {
        name: 'breakwater.audit.decision',
        tags: { action: 'agent.output.policy', decision: 'error' },
      },
    ]);
  });

  it('does not observe duration when detail is absent', () => {
    // #given
    const metrics = mockMetrics();
    const sink = metricsAuditSink(metrics);

    // #when
    sink(makeEvent());

    // #then
    expect(metrics.observes).toEqual([]);
  });

  it.each([
    ['a non-number', 'not-a-number' as unknown as number],
    ['NaN', Number.NaN],
    ['Infinity', Number.POSITIVE_INFINITY],
    ['-Infinity', Number.NEGATIVE_INFINITY],
    // Cross-isolate clock skew can stamp decide before create; skew is not a
    // duration, and a histogram must stay non-negative.
    ['negative', -5],
  ])('does not observe duration when durationSeconds is %s', (_label, value) => {
    // #given
    const metrics = mockMetrics();
    const sink = metricsAuditSink(metrics);

    // #when
    sink(makeEvent({ detail: { durationSeconds: value } }));

    // #then
    expect(metrics.observes).toEqual([]);
  });

  it('observes breakwater.audit.duration_seconds tagged with action when durationSeconds is finite', () => {
    // #given
    const metrics = mockMetrics();
    const sink = metricsAuditSink(metrics);

    // #when
    sink(
      makeEvent({
        action: 'agent.output.policy',
        detail: { durationSeconds: 1.5 },
      }),
    );

    // #then
    expect(metrics.observes).toEqual([
      {
        name: 'breakwater.audit.duration_seconds',
        value: 1.5,
        tags: { action: 'agent.output.policy' },
      },
    ]);
  });

  it('never throws on a missing or malformed detail', () => {
    // #given
    const metrics = mockMetrics();
    const sink = metricsAuditSink(metrics);

    // #when / #then
    expect(() => sink(makeEvent())).not.toThrow();
    expect(() => sink(makeEvent({ detail: {} }))).not.toThrow();
    expect(() =>
      sink(
        makeEvent({
          detail: { durationSeconds: 'nope' as unknown as number },
        }),
      ),
    ).not.toThrow();
  });
});

describe('combineAuditSinks', () => {
  it('runs every sink for one event', () => {
    // #given
    const calls: string[] = [];
    const a: AuditSink = () => {
      calls.push('a');
    };
    const b: AuditSink = () => {
      calls.push('b');
    };
    const combined = combineAuditSinks(a, b);

    // #when
    combined(makeEvent());

    // #then
    expect(calls).toEqual(['a', 'b']);
  });

  it('runs every sink past one that throws synchronously, then rethrows an AggregateError', () => {
    // #given
    const calls: string[] = [];
    const throwing: AuditSink = () => {
      calls.push('throwing');
      throw new Error('sink A down');
    };
    const clean: AuditSink = () => {
      calls.push('clean');
    };
    const combined = combineAuditSinks(throwing, clean);

    // #when / #then
    expect(() => combined(makeEvent())).toThrow(AggregateError);
    expect(calls).toEqual(['throwing', 'clean']);
  });

  it('surfaces an async-rejecting sink through the returned promise', async () => {
    // #given
    const rejecting: AuditSink = () =>
      Promise.reject(new Error('async sink down'));
    const combined = combineAuditSinks(rejecting);

    // #when
    const result = combined(makeEvent());

    // #then
    expect(result).toBeInstanceOf(Promise);
    await expect(result).rejects.toThrow(AggregateError);
  });

  it('waits for every pending sink to settle before aggregating (does not short-circuit on the first rejection)', async () => {
    // #given — one sink rejects immediately; the other's rejection is
    // deferred until we have confirmed the combined promise is still
    // pending — pinning that allSettled waited rather than settling on the
    // first rejection the way Promise.all would.
    let rejectDeferred: ((reason: Error) => void) | undefined;
    const immediateRejecting: AuditSink = () =>
      Promise.reject(new Error('immediate failure'));
    const deferredRejecting: AuditSink = () =>
      new Promise((_resolve, reject) => {
        rejectDeferred = reject;
      });
    const combined = combineAuditSinks(immediateRejecting, deferredRejecting);

    // #when
    const result = combined(makeEvent()) as Promise<void>;
    let settled = false;
    result.catch(() => {
      settled = true;
    });
    await Promise.resolve();
    await Promise.resolve();
    expect(settled).toBe(false);

    // #then — once the deferred sink also rejects, the combined promise settles
    rejectDeferred?.(new Error('deferred failure'));
    const error = await result.catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(AggregateError);
    const messages = (error as AggregateError).errors.map((e: unknown) =>
      e instanceof Error ? e.message : String(e),
    );
    expect(messages).toEqual(
      expect.arrayContaining(['immediate failure', 'deferred failure']),
    );
  });

  it('combines a synchronous throw with an async rejection into one AggregateError', async () => {
    // #given
    const throwing: AuditSink = () => {
      throw new Error('sync failure');
    };
    const rejecting: AuditSink = () =>
      Promise.reject(new Error('async failure'));
    const combined = combineAuditSinks(throwing, rejecting);

    // #when
    const result = combined(makeEvent());

    // #then
    expect(result).toBeInstanceOf(Promise);
    const error = await (result as Promise<void>).catch(
      (caught: unknown) => caught,
    );
    expect(error).toBeInstanceOf(AggregateError);
    const messages = (error as AggregateError).errors.map((e: unknown) =>
      e instanceof Error ? e.message : String(e),
    );
    expect(messages).toEqual(
      expect.arrayContaining(['sync failure', 'async failure']),
    );
  });

  it('returns void synchronously (not a promise) when every sink is sync and clean', () => {
    // #given
    const combined = combineAuditSinks(
      () => {},
      () => {},
    );

    // #when
    const result = combined(makeEvent());

    // #then
    expect(result).toBeUndefined();
  });

  it('feeds every sink the same event', () => {
    // #given
    const seen: AuditEvent[] = [];
    const combined = combineAuditSinks((event) => {
      seen.push(event);
    });
    const givenEvent = makeEvent({ action: 'custom.action' });

    // #when
    combined(givenEvent);

    // #then
    expect(seen).toEqual([givenEvent]);
  });
});
