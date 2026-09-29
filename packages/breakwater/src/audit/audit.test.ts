// SPDX-License-Identifier: Apache-2.0
import { RequestContext } from '@mastra/core/request-context';
import { describe, expect, it } from 'vitest';

import type {
  AuditEvent,
  AuditLoggerOptions,
  AuditSink,
  MetricsRecorder,
} from './index.js';
import {
  AGENT_AUDIT_CONTEXT_KEY,
  AuditLogger,
  agentAuditContextFromRequestContext,
  agentAuditDetail,
  combineAuditSinks,
  malformedAgentAuditContextEvent,
  metricsAuditSink,
} from './index.js';

describe('agentAuditContextFromRequestContext', () => {
  it.each([
    'agentId',
    'entryPath',
  ] as const)('rejects an invalid required %s', (field) => {
    for (const value of [undefined, null, '', 1, {}]) {
      const requestContext = new RequestContext();
      requestContext.set(AGENT_AUDIT_CONTEXT_KEY, {
        agentId: 'agent-trusted',
        entryPath: 'approval-resume',
        [field]: value,
      });
      expect(
        agentAuditContextFromRequestContext(requestContext),
      ).toBeUndefined();
    }
  });

  it.each([
    ['agentId', 'agent-trusted'],
    ['entryPath', 'approval-resume'],
  ] as const)('uses the validated %s value without rereading it', (field, expected) => {
    const requestContext = new RequestContext();
    const candidate: Record<string, unknown> = {
      agentId: 'agent-trusted',
      entryPath: 'approval-resume',
    };
    let reads = 0;
    Object.defineProperty(candidate, field, {
      enumerable: true,
      get() {
        reads += 1;
        return reads === 1 ? expected : { invalid: true };
      },
    });
    requestContext.set(AGENT_AUDIT_CONTEXT_KEY, candidate);

    expect(agentAuditContextFromRequestContext(requestContext)).toEqual({
      agentId: 'agent-trusted',
      entryPath: 'approval-resume',
    });
    expect(reads).toBe(1);
  });

  it.each([
    'agentId',
    'entryPath',
  ] as const)('rejects a throwing required %s getter', (field) => {
    const requestContext = new RequestContext();
    const candidate: Record<string, unknown> = {
      agentId: 'agent-trusted',
      entryPath: 'approval-resume',
    };
    Object.defineProperty(candidate, field, {
      enumerable: true,
      get() {
        throw new Error('getter failure');
      },
    });
    requestContext.set(AGENT_AUDIT_CONTEXT_KEY, candidate);

    expect(agentAuditContextFromRequestContext(requestContext)).toBeUndefined();
  });
});

describe('agentAuditDetail', () => {
  it('keeps trusted correlation authoritative over spoofed boundary detail', () => {
    const requestContext = new RequestContext();
    requestContext.set(AGENT_AUDIT_CONTEXT_KEY, {
      agentId: 'agent-trusted',
      entryPath: 'approval-resume',
      runId: 'run-trusted',
      threadId: 'thread-trusted',
      resourceId: 'resource-trusted',
      principalKind: 'human',
      principalId: 'principal-trusted',
    });

    expect(
      agentAuditDetail(requestContext, {
        sideEffect: 'write',
        agentId: 'agent-spoofed',
        entryPath: 'spoofed',
        runId: 'run-spoofed',
        threadId: 'thread-spoofed',
        resourceId: 'resource-spoofed',
        principalKind: 'system',
        principalId: 'principal-spoofed',
      }),
    ).toEqual({
      sideEffect: 'write',
      agentId: 'agent-trusted',
      entryPath: 'approval-resume',
      runId: 'run-trusted',
      threadId: 'thread-trusted',
      resourceId: 'resource-trusted',
      principalKind: 'human',
      principalId: 'principal-trusted',
    });
  });
});

describe('AuditLogger', () => {
  it.each([
    ['throw', 'throw'],
    ['throw', 'reject'],
    ['reject', 'throw'],
    ['reject', 'reject'],
  ] as const)('keeps records when the sink %s and observer %s', async (sinkMode, observerMode) => {
    const sinkFailure = new Error('sink failure');
    const observerFailure = new Error('observer failure');
    const observed: Array<{
      error: unknown;
      event: AuditEvent;
      receiver: unknown;
    }> = [];
    const audit = new AuditLogger({
      sink: () => {
        if (sinkMode === 'throw') throw sinkFailure;
        return Promise.reject(sinkFailure);
      },
      onSinkError: function (this: unknown, error, event) {
        observed.push({ error, event, receiver: this });
        if (observerMode === 'throw') throw observerFailure;
        return Promise.reject(observerFailure);
      },
    });
    let recorded: AuditEvent | undefined;
    expect(() => {
      recorded = audit.record({
        actor: null,
        action: 'connector.execute',
        resource: 'local',
        decision: 'allowed',
      });
    }).not.toThrow();
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect(recorded).toMatchObject({ decision: 'allowed' });
    expect(audit.events()).toEqual([recorded]);
    expect(observed).toEqual([
      { error: sinkFailure, event: recorded, receiver: audit },
    ]);
  });

  it('preserves connector taxonomy in both exported and buffered events', () => {
    const exported: AuditEvent[] = [];
    const audit = new AuditLogger({
      sink: (event) => {
        exported.push(event);
      },
    });
    audit.record({
      actor: null,
      action: 'connector.execute',
      resource: 'example.read',
      decision: 'denied',
      decisionCode: 'RATE_LIMIT_EXCEEDED',
      policyKind: 'rate-limit',
      retryable: true,
    });
    expect(exported).toEqual([
      expect.objectContaining({
        decisionCode: 'RATE_LIMIT_EXCEEDED',
        policyKind: 'rate-limit',
        retryable: true,
      }),
    ]);
    expect(audit.events()).toEqual(exported);
  });

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

  it.each<[string, unknown, string]>([
    ['null', null, 'null'],
    [
      'a URL string',
      'https://audit.example.com/ingest',
      '"https://audit.example.com/ingest"',
    ],
    ['a plain object', {}, 'object'],
    ['true', true, 'boolean'],
    ['a number', 1, 'number'],
    ['an empty list', [], 'object'],
  ])('refuses %s as the sink, which would count as external and export nothing', (_label, sink, got) => {
    // #when / #then
    expect(() => new AuditLogger({ sink: sink as AuditSink })).toThrow(
      new TypeError(
        `AuditLogger: sink must be a function when provided (got ${got})`,
      ),
    );
  });

  it('refuses a non-function onSinkError', () => {
    // #when / #then
    expect(
      () =>
        new AuditLogger({
          onSinkError: 'log' as unknown as AuditLoggerOptions['onSinkError'],
        }),
    ).toThrow(
      new TypeError(
        'AuditLogger: onSinkError must be a function when provided (got "log")',
      ),
    );
  });

  it('reports an external sink only for a function, and exports to it', () => {
    // #given
    const exported: AuditEvent[] = [];
    const withSink = new AuditLogger({
      sink: (event) => {
        exported.push(event);
      },
    });
    // #when
    withSink.record({
      actor: null,
      action: 'a',
      resource: 'r',
      decision: 'allowed',
    });
    // #then
    expect(withSink.hasExternalSink()).toBe(true);
    expect(exported).toHaveLength(1);
    expect(new AuditLogger().hasExternalSink()).toBe(false);
  });

  it.each<[string, unknown, string]>([
    ['a negative number', -1, '-1'],
    ['a fraction', 1.5, '1.5'],
    ['NaN, which keeps every event', Number.NaN, 'NaN'],
    ['Infinity', Number.POSITIVE_INFINITY, 'Infinity'],
    ['an empty string', '', '""'],
    ['an empty list', [], 'object'],
    ['false', false, 'boolean'],
  ])('refuses %s as maxBuffered', (_label, maxBuffered, got) => {
    // #when / #then
    expect(
      () => new AuditLogger({ maxBuffered: maxBuffered as number }),
    ).toThrow(
      new TypeError(
        `AuditLogger: maxBuffered must be a non-negative safe integer (got ${got})`,
      ),
    );
  });

  it('keeps no event under maxBuffered 0', () => {
    // #given
    const audit = new AuditLogger({ maxBuffered: 0 });
    // #when
    audit.record({
      actor: null,
      action: 'a',
      resource: 'r',
      decision: 'allowed',
    });
    // #then
    expect(audit.events()).toEqual([]);
  });

  it('refuses a misspelled option, which would drop the sink it spells', () => {
    // #when / #then
    expect(
      () => new AuditLogger({ sinks: () => undefined } as AuditLoggerOptions),
    ).toThrow(
      new TypeError(
        'AuditLogger: options has unknown field "sinks" (valid fields: sink, onSinkError, maxBuffered)',
      ),
    );
  });

  it('constructs with every declared option', () => {
    // #given — the Required type fails to compile while a declared option is
    // missing here
    const options: Required<AuditLoggerOptions> = {
      sink: () => undefined,
      onSinkError: () => undefined,
      maxBuffered: 10,
    };
    // #when / #then
    expect(new AuditLogger(options).hasExternalSink()).toBe(true);
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

  it('refuses no sinks, which would count as external and export nothing', () => {
    // #given — a sink list from configuration that came back empty
    const configured: AuditSink[] = [];

    // #when / #then
    for (const build of [
      () => combineAuditSinks(),
      () => combineAuditSinks(...configured),
    ]) {
      expect(build).toThrow(
        new TypeError('combineAuditSinks: sinks must not be empty'),
      );
    }
  });

  it.each<[string, unknown[], string]>([
    [
      'undefined',
      [undefined],
      'sinks entry 0 must be a function (got undefined)',
    ],
    ['null', [null], 'sinks entry 0 must be a function (got null)'],
    [
      'a URL string',
      ['https://audit.example.com/ingest'],
      'sinks entry 0 must be a function (got "https://audit.example.com/ingest")',
    ],
    ['a plain object', [{}], 'sinks entry 0 must be a function (got object)'],
    [
      'a list passed instead of spread',
      [[() => undefined]],
      'sinks entry 0 must be a function (got an array)',
    ],
    [
      'an unset second sink',
      [() => undefined, undefined],
      'sinks entry 1 must be a function (got undefined)',
    ],
  ])('refuses %s as a sink', (_label, sinks, message) => {
    // #when / #then
    expect(() => combineAuditSinks(...(sinks as AuditSink[]))).toThrow(
      new TypeError(`combineAuditSinks: ${message}`),
    );
  });
});

describe('metricsAuditSink recorder', () => {
  it.each<[string, unknown, string]>([
    [
      'undefined',
      undefined,
      'metrics must be an object with increment and observe functions (got undefined)',
    ],
    [
      'a URL string',
      'statsd://localhost:8125',
      'metrics must be an object with increment and observe functions (got "statsd://localhost:8125")',
    ],
    [
      'a plain object',
      {},
      'metrics.increment must be a function (got undefined)',
    ],
    [
      'a misspelled increment',
      { Increment: () => undefined, observe: () => undefined },
      'metrics.increment must be a function (got undefined)',
    ],
    [
      'a recorder without observe',
      { increment: () => undefined },
      'metrics.observe must be a function (got undefined)',
    ],
  ])('refuses %s, which would count as external and export nothing', (_label, metrics, message) => {
    // #when / #then
    expect(() => metricsAuditSink(metrics as MetricsRecorder)).toThrow(
      new TypeError(`metricsAuditSink: ${message}`),
    );
  });

  it('calls a class recorder with its own receiver', () => {
    // #given
    class Recorder implements MetricsRecorder {
      readonly names: string[] = [];
      increment(name: string): void {
        this.names.push(name);
      }
      observe(name: string): void {
        this.names.push(name);
      }
    }
    const recorder = new Recorder();
    const sink = metricsAuditSink(recorder);

    // #when
    sink(makeEvent({ detail: { durationSeconds: 1 } }));

    // #then
    expect(recorder.names).toEqual([
      'breakwater.audit.decision',
      'breakwater.audit.duration_seconds',
    ]);
  });
});

describe('malformedAgentAuditContextEvent', () => {
  function contextWith(value: unknown): RequestContext {
    const requestContext = new RequestContext();
    requestContext.set(AGENT_AUDIT_CONTEXT_KEY, value);
    return requestContext;
  }
  const valid = { agentId: 'agent-1', entryPath: 'http-start' };

  it.each<[string, unknown, Record<string, unknown> | undefined]>([
    ['a numeric tenant id', { ...valid, tenantId: 42 }, valid],
    ['a misspelled tenant id', { ...valid, tenantID: 'acme' }, valid],
    ['an empty tenant id', { ...valid, tenantId: '' }, valid],
    ['a null optional field', { ...valid, purpose: null }, valid],
    ['a numeric agent id', { ...valid, agentId: 42 }, undefined],
    [
      'a misspelled agent id',
      { agentID: 'agent-1', entryPath: 'x' },
      undefined,
    ],
    [
      'a misspelled entry path',
      { agentId: 'agent-1', entrypath: 'x' },
      undefined,
    ],
    ['a JSON string', JSON.stringify(valid), undefined],
    ['a list', [], undefined],
    ['a number', 42, undefined],
    ['null', null, undefined],
  ])('builds an error event for %s, keeping the fields it accepts', (_label, value, detail) => {
    // #when
    const event = malformedAgentAuditContextEvent(
      contextWith(value),
      'connector.c1',
      null,
    );

    // #then
    expect(event).toEqual({
      actor: null,
      action: 'audit.context',
      resource: 'connector.c1',
      decision: 'error',
      reason: "request context 'breakwater.auditContext' is malformed",
      ...(detail === undefined ? {} : { detail }),
    });
  });

  it('builds none for an absent key or a well-formed context', () => {
    // #when / #then
    expect(
      malformedAgentAuditContextEvent(new RequestContext(), 'r', null),
    ).toBeUndefined();
    expect(
      malformedAgentAuditContextEvent(contextWith(valid), 'r', null),
    ).toBeUndefined();
    expect(
      malformedAgentAuditContextEvent(
        contextWith({
          ...valid,
          tenantId: 'acme',
          purpose: undefined,
          principalKind: 'service',
        }),
        'r',
        null,
      ),
    ).toBeUndefined();
  });

  it('keeps the reader copying the fields it accepts', () => {
    // #when / #then
    expect(
      agentAuditContextFromRequestContext(
        contextWith({ ...valid, tenantId: 42, runId: 'run-1', prompt: 'x' }),
      ),
    ).toEqual({ ...valid, runId: 'run-1' });
  });
});
