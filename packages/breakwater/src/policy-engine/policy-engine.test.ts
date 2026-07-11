// SPDX-License-Identifier: Apache-2.0
import type { MastraDBMessage } from '@mastra/core/agent/message-list';
import { MessageList } from '@mastra/core/agent/message-list';
import {
  type OutputResult,
  type ProcessInputArgs,
  type ProcessOutputResultArgs,
  type ProcessOutputStreamArgs,
  ProcessorRunner,
  type ProcessorState,
} from '@mastra/core/processors';
import { RequestContext } from '@mastra/core/request-context';
import { ChunkFrom, type ChunkType } from '@mastra/core/stream';
import { describe, expect, it } from 'vitest';

import { AuditLogger } from '../audit/index.js';
import { ACTOR_CONTEXT_KEY, type Actor } from '../rbac/index.js';
import {
  denyPatterns,
  extractMessageText,
  maxTextLength,
  PolicyEngine,
  type PolicyEvaluator,
} from './index.js';

class Tripwire extends Error {}

let messageSeq = 0;

function makeMessage(
  text: string,
  role: MastraDBMessage['role'] = 'user',
): MastraDBMessage {
  return {
    id: `msg-${++messageSeq}`,
    role,
    createdAt: new Date(),
    content: { format: 2, parts: [{ type: 'text', text }] },
  };
}

function abortThrowing(reason?: string): never {
  throw new Tripwire(reason ?? 'aborted');
}

function makeInputArgs(text: string, actor?: Actor): ProcessInputArgs {
  const requestContext = new RequestContext();
  if (actor) requestContext.set(ACTOR_CONTEXT_KEY, actor);
  return {
    messages: [makeMessage(text)],
    messageList: new MessageList(),
    systemMessages: [],
    state: {},
    retryCount: 0,
    requestContext,
    abort: abortThrowing,
  };
}

function makeOutputArgs(
  resultText: string,
  priorMessages: MastraDBMessage[] = [],
  steps: OutputResult['steps'] = [],
): ProcessOutputResultArgs {
  const result: OutputResult = {
    text: resultText,
    usage: {} as OutputResult['usage'],
    finishReason: 'stop',
    steps,
  };
  return {
    messages: [...priorMessages, makeMessage(resultText, 'assistant')],
    messageList: new MessageList(),
    state: {},
    retryCount: 0,
    requestContext: new RequestContext(),
    abort: abortThrowing,
    result,
  };
}

function textDelta(text: string, id = 'out'): ChunkType {
  return {
    runId: 'run',
    from: ChunkFrom.AGENT,
    type: 'text-delta',
    payload: { id, text },
  };
}

function reasoningDelta(text: string): ChunkType {
  return {
    runId: 'run',
    from: ChunkFrom.AGENT,
    type: 'reasoning-delta',
    payload: { id: 'reason', text },
  };
}

// 'object' chunks carry the parsed value on `.object`, not `.payload`
// (ChunkType, stream/types.d.ts); the casts erase the OUTPUT generic the
// tests do not model (ChunkType defaults OUTPUT to undefined).
function objectChunk(partial: unknown): ChunkType {
  return {
    runId: 'run',
    from: ChunkFrom.AGENT,
    type: 'object',
    object: partial,
  } as unknown as ChunkType;
}

function objectResult(object: unknown): ChunkType {
  return {
    runId: 'run',
    from: ChunkFrom.AGENT,
    type: 'object-result',
    object,
  } as unknown as ChunkType;
}

// Minimal LLMStepResult stand-in: the engine reads only reasoningText.
function reasoningStep(reasoningText: string): OutputResult['steps'][number] {
  return { reasoningText } as unknown as OutputResult['steps'][number];
}

// Simulates one processOutputStream call: `part` is the newest chunk,
// `streamParts` every chunk seen so far. Pass ONE `state` object through all
// calls of a simulated stream — core persists it across the chunks of a
// request, and the engine accumulates its per-channel text there.
function makeStreamArgs(
  streamParts: ChunkType[],
  state: Record<string, unknown> = {},
): ProcessOutputStreamArgs {
  const part = streamParts.at(-1);
  if (!part) throw new Error('makeStreamArgs needs at least one chunk');
  return {
    part,
    streamParts,
    state,
    retryCount: 0,
    requestContext: new RequestContext(),
    abort: abortThrowing,
  };
}

describe('PolicyEngine', () => {
  it('passes input through when no policies are registered and audits the allowed evaluation', async () => {
    // #given
    const audit = new AuditLogger();
    const engine = new PolicyEngine({ policies: [], audit });
    const args = makeInputArgs('anything at all');

    // #when / #then
    await expect(engine.processInput(args)).resolves.toBe(args.messages);
    expect(audit.events()[0]).toMatchObject({
      action: 'agent.input.policy',
      decision: 'allowed',
      detail: { evaluated: [] },
    });
  });

  it('aborts on the first denying policy and audits which policy fired', async () => {
    // #given
    const audit = new AuditLogger();
    const engine = new PolicyEngine({
      policies: [denyPatterns(['drop table'])],
      audit,
    });

    // #when / #then
    await expect(
      engine.processInput(makeInputArgs('please DROP TABLE users')),
    ).rejects.toThrowError(/deny-patterns: matched blocked pattern/);
    expect(audit.events()[0]).toMatchObject({
      decision: 'denied',
      detail: { policy: 'deny-patterns' },
    });
  });

  it('attributes audit events to the actor from requestContext', async () => {
    // #given
    const actor: Actor = { id: 'op-7', role: 'operator' };
    const audit = new AuditLogger();
    const engine = new PolicyEngine({ policies: [], audit });

    // #when
    await engine.processInput(makeInputArgs('hello', actor));

    // #then
    expect(audit.events()[0]).toMatchObject({ decision: 'allowed', actor });
  });

  it('respects policy phases: output-only policies do not gate input', async () => {
    // #given
    const engine = new PolicyEngine({ policies: [maxTextLength(5)] });
    const longInput = makeInputArgs('a'.repeat(100));

    // #when / #then
    await expect(engine.processInput(longInput)).resolves.toBe(
      longInput.messages,
    );
    await expect(
      engine.processOutputResult(makeOutputArgs('a'.repeat(100))),
    ).rejects.toThrowError(/max-text-length: text length 100 exceeds limit 5/);
  });

  it('gates output on result.text, not on prior conversation messages', async () => {
    // #given
    const engine = new PolicyEngine({
      policies: [denyPatterns(['forbidden'], { phases: ['output'] })],
    });
    const args = makeOutputArgs('clean answer', [
      makeMessage('user said forbidden earlier'),
    ]);

    // #when / #then
    await expect(engine.processOutputResult(args)).resolves.toBe(args.messages);
  });

  it('supports RegExp patterns', async () => {
    // #given
    const engine = new PolicyEngine({
      policies: [denyPatterns([/secret-\d+/])],
    });

    // #when / #then
    await expect(
      engine.processInput(makeInputArgs('leak secret-42 now')),
    ).rejects.toThrowError(Tripwire);
  });

  it('supports async evaluators', async () => {
    // #given
    const asyncDeny: PolicyEvaluator = {
      name: 'async-deny',
      evaluate: async () => ({ allowed: false, reason: 'nope' }),
    };
    const engine = new PolicyEngine({ policies: [asyncDeny] });

    // #when / #then
    await expect(engine.processInput(makeInputArgs('x'))).rejects.toThrowError(
      /async-deny: nope/,
    );
  });

  it('denies consistently across repeated calls with a g-flagged RegExp', async () => {
    // #given — g-flagged regexes carry lastIndex state; a shared engine must
    // not let the same blocked text through on the second call.
    const engine = new PolicyEngine({ policies: [denyPatterns([/danger/g])] });

    // #when / #then — both calls deny, not just the first
    await expect(
      engine.processInput(makeInputArgs('this is danger')),
    ).rejects.toThrowError(Tripwire);
    await expect(
      engine.processInput(makeInputArgs('this is danger')),
    ).rejects.toThrowError(Tripwire);
  });

  it('records an error audit event and rethrows when an evaluator throws', async () => {
    // #given
    const audit = new AuditLogger();
    const crashing: PolicyEvaluator = {
      name: 'crashy',
      evaluate: () => {
        throw new Error('evaluator internal failure');
      },
    };
    const engine = new PolicyEngine({ policies: [crashing], audit });

    // #when / #then — the crash propagates (fail closed) AND leaves an audit
    // record; an internal error must not leave less evidence than a denial.
    await expect(engine.processInput(makeInputArgs('x'))).rejects.toThrowError(
      'evaluator internal failure',
    );
    expect(audit.events()).toHaveLength(1);
    expect(audit.events()[0]).toMatchObject({
      decision: 'error',
      reason: 'crashy threw: evaluator internal failure',
      detail: { policy: 'crashy' },
    });
  });

  it('records an error audit event when an async evaluator rejects', async () => {
    // #given
    const audit = new AuditLogger();
    const rejecting: PolicyEvaluator = {
      name: 'async-crashy',
      evaluate: async () => {
        throw new Error('boom');
      },
    };
    const engine = new PolicyEngine({ policies: [rejecting], audit });

    // #when / #then
    await expect(engine.processInput(makeInputArgs('x'))).rejects.toThrowError(
      'boom',
    );
    expect(audit.events()[0]).toMatchObject({ decision: 'error' });
  });
});

describe('PolicyEngine constructor validation (K2)', () => {
  it('rejects a policy whose explicit phases include input but explicit channels exclude answer', () => {
    // #given — processInput hardcodes channel: 'answer', so this policy
    // would silently never run on input
    const misconfigured: PolicyEvaluator = {
      name: 'reasoning-only-input',
      phases: ['input'],
      channels: ['reasoning'],
      evaluate: () => ({ allowed: true }),
    };

    // #when / #then
    expect(() => new PolicyEngine({ policies: [misconfigured] })).toThrow(
      /reasoning-only-input.*phases.*input.*channels.*answer/is,
    );
  });

  it('does not throw when phases is left to its own default (both)', () => {
    // #given — channels excludes answer, but phases was never narrowed. A
    // reasoning-only policy pins the K2 guard ALONE: an object-only policy
    // would additionally trip the D1 audit-sink guard (see the D1 cases
    // below), so use reasoning to isolate K2 — it must NOT reject a
    // defaulted-phases policy merely because channels exclude answer.
    const policy: PolicyEvaluator = {
      name: 'reasoning-only',
      channels: ['reasoning'],
      evaluate: () => ({ allowed: true }),
    };

    // #when / #then
    expect(() => new PolicyEngine({ policies: [policy] })).not.toThrow();
  });

  it('does not throw when channels is left to its own default (answer)', () => {
    // #given — phases explicitly includes input, but channels was never set
    const policy: PolicyEvaluator = {
      name: 'input-only',
      phases: ['input'],
      evaluate: () => ({ allowed: true }),
    };

    // #when / #then
    expect(() => new PolicyEngine({ policies: [policy] })).not.toThrow();
  });

  it('does not throw when explicit channels include answer alongside object', () => {
    // #given — denyPatterns' own default channels: ['answer','reasoning','object']
    // #when / #then
    expect(
      () =>
        new PolicyEngine({
          policies: [denyPatterns(['x'], { phases: ['input'] })],
        }),
    ).not.toThrow();
  });

  it('rejects an object-only policy constructed without an audit sink (D1)', () => {
    // #given — channels include 'object' but not 'answer': zero result-phase
    // coverage AND no sink to carry the one-time warning, so the gap is silent
    const policy: PolicyEvaluator = {
      name: 'object-only',
      channels: ['object'],
      evaluate: () => ({ allowed: true }),
    };

    // #when / #then — fail closed at construction with a TypeError naming it
    expect(() => new PolicyEngine({ policies: [policy] })).toThrow(
      /object-only.*'object'.*without 'answer'.*audit sink/is,
    );
  });

  it('allows an object-only policy when an audit sink is provided (D1)', () => {
    // #given — the same policy, now with a sink to carry the one-time warning
    const audit = new AuditLogger();
    const policy: PolicyEvaluator = {
      name: 'object-only',
      channels: ['object'],
      evaluate: () => ({ allowed: true }),
    };

    // #when / #then
    expect(() => new PolicyEngine({ policies: [policy], audit })).not.toThrow();
  });
});

describe('PolicyEngine object-channel result-phase fence (D1)', () => {
  it('warns once per engine instance when a policy is scoped to object without answer', async () => {
    // #given — zero result-phase coverage: OutputResult has no object field,
    // and this policy never sees the answer channel either
    const audit = new AuditLogger();
    const engine = new PolicyEngine({
      policies: [denyPatterns(['x'], { channels: ['object'] })],
      audit,
    });

    // #when — two result-phase calls
    await engine.processOutputResult(makeOutputArgs('clean'));
    await engine.processOutputResult(makeOutputArgs('clean'));

    // #then — exactly one warning, not one per call. #recordAllowed also
    // writes 'agent.output.policy' events (decision: 'allowed') on every
    // call, so the fence warning is distinguished by decision: 'error'.
    const warnings = audit
      .events()
      .filter(
        (event) =>
          event.action === 'agent.output.policy' && event.decision === 'error',
      );
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toMatchObject({
      decision: 'error',
      detail: { policies: ['deny-patterns'] },
    });
  });

  it('does not warn for a policy scoped to object alongside answer (the designed cover)', async () => {
    // #given — denyPatterns' default channels include 'answer'
    const audit = new AuditLogger();
    const engine = new PolicyEngine({
      policies: [denyPatterns(['x'])],
      audit,
    });

    // #when
    await engine.processOutputResult(makeOutputArgs('clean'));

    // #then — no fence warning fired (the normal 'allowed' record for this
    // call is expected and is not the fence warning)
    expect(
      audit
        .events()
        .some(
          (event) =>
            event.action === 'agent.output.policy' &&
            event.decision === 'error',
        ),
    ).toBe(false);
  });

  it('does not warn when no policy is object-scoped at all', async () => {
    // #given
    const audit = new AuditLogger();
    const engine = new PolicyEngine({
      policies: [maxTextLength(100)],
      audit,
    });

    // #when
    await engine.processOutputResult(makeOutputArgs('clean'));

    // #then
    expect(
      audit
        .events()
        .some(
          (event) =>
            event.action === 'agent.output.policy' &&
            event.decision === 'error',
        ),
    ).toBe(false);
  });
});

describe('PolicyEngine.processOutputStream', () => {
  it('aborts a denied pattern that completes across chunks, before the chunk is emitted', async () => {
    // #given — an output deny policy; "secret" straddles two text-delta
    // chunks of one stream (one shared state object). String patterns take
    // the incremental-scan path, so this also pins that a match straddling
    // the scan frontier is still caught.
    const engine = new PolicyEngine({
      policies: [denyPatterns(['secret'], { phases: ['output'] })],
    });
    const state: Record<string, unknown> = {};
    const first = textDelta('the sec');
    const second = textDelta('ret is safe');

    // #then — the first chunk carries no full match and is emitted...
    await expect(
      engine.processOutputStream(makeStreamArgs([first], state)),
    ).resolves.toBe(first);
    // ...the chunk that completes "secret" aborts before it reaches the client
    await expect(
      engine.processOutputStream(makeStreamArgs([first, second], state)),
    ).rejects.toThrowError(/deny-patterns: matched blocked pattern/);
  });

  it('enforces maxTextLength on cumulative output, not per chunk', async () => {
    // #given — a 5-char cap; no single delta exceeds it but their sum does
    const engine = new PolicyEngine({
      policies: [maxTextLength(5)],
    });
    const state: Record<string, unknown> = {};
    const parts = [textDelta('aa'), textDelta('aa'), textDelta('aa')];

    // #then — chunks 1-2 (cumulative 2, 4) pass; chunk 3 (cumulative 6) trips
    await expect(
      engine.processOutputStream(makeStreamArgs(parts.slice(0, 1), state)),
    ).resolves.toBe(parts[0]);
    await expect(
      engine.processOutputStream(makeStreamArgs(parts.slice(0, 2), state)),
    ).resolves.toBe(parts[1]);
    await expect(
      engine.processOutputStream(makeStreamArgs(parts, state)),
    ).rejects.toThrowError(/max-text-length: text length 6 exceeds limit 5/);
  });

  it('catches a RegExp pattern split across chunks via the full-scan fallback', async () => {
    // #given — any RegExp in the pattern list forces a full accumulated-text
    // scan per chunk (no bounded lookbehind window for arbitrary regexes)
    const engine = new PolicyEngine({
      policies: [denyPatterns([/secret-\d+/], { phases: ['output'] })],
    });
    const state: Record<string, unknown> = {};
    const first = textDelta('secret-');
    const second = textDelta('42');

    // #then
    await expect(
      engine.processOutputStream(makeStreamArgs([first], state)),
    ).resolves.toBe(first);
    await expect(
      engine.processOutputStream(makeStreamArgs([first, second], state)),
    ).rejects.toThrowError(/deny-patterns: matched blocked pattern/);
  });

  it('passes non-text chunks through without evaluating policies', async () => {
    // #given — a policy that would deny, but a text-start chunk carries no text
    const engine = new PolicyEngine({
      policies: [denyPatterns(['anything'], { phases: ['output'] })],
    });
    const startChunk: ChunkType = {
      runId: 'run',
      from: ChunkFrom.AGENT,
      type: 'text-start',
      payload: { id: 'out' },
    };

    // #then — returned untouched, no abort
    await expect(
      engine.processOutputStream(makeStreamArgs([startChunk])),
    ).resolves.toBe(startChunk);
  });

  it('emits no per-chunk allowed audit record while streaming', async () => {
    // #given — a clean stream; the terminal "allowed" record comes from
    // processOutputResult, not once per chunk
    const audit = new AuditLogger();
    const engine = new PolicyEngine({
      policies: [denyPatterns(['nope'], { phases: ['output'] })],
      audit,
    });

    // #when — two clean chunks stream through
    const state: Record<string, unknown> = {};
    await engine.processOutputStream(
      makeStreamArgs([textDelta('all ')], state),
    );
    await engine.processOutputStream(
      makeStreamArgs([textDelta('all '), textDelta('good')], state),
    );

    // #then — no audit noise during streaming
    expect(audit.events()).toHaveLength(0);
  });

  it('fails closed on an evaluator crash mid-stream: aborts, not rethrows', async () => {
    // #given — Mastra's stream driver emits the chunk on a raw throw and only
    // suppresses it on an abort (TripWire), so a crash must surface as an abort
    const audit = new AuditLogger();
    const crashing: PolicyEvaluator = {
      name: 'crashy',
      phases: ['output'],
      evaluate: () => {
        throw new Error('evaluator internal failure');
      },
    };
    const engine = new PolicyEngine({ policies: [crashing], audit });

    // #then — the crash becomes an abort (Tripwire), not the raw Error, and is
    // still audited as an error
    await expect(
      engine.processOutputStream(makeStreamArgs([textDelta('anything')])),
    ).rejects.toThrowError(Tripwire);
    expect(audit.events()[0]).toMatchObject({
      decision: 'error',
      reason: 'crashy threw: evaluator internal failure',
      detail: { policy: 'crashy' },
    });
  });

  it('fails closed when streamParts omits the current part', async () => {
    // #given — a driver/caller whose streamParts does not include the current
    // chunk; the forbidden text lives only in `part`. Accumulation reads
    // args.part directly (state-based), so the streamParts contract cannot
    // slip a chunk's own text past the gate.
    const engine = new PolicyEngine({
      policies: [denyPatterns(['secret'], { phases: ['output'] })],
    });
    const args: ProcessOutputStreamArgs = {
      part: textDelta('secret'),
      streamParts: [], // omits the current part
      state: {},
      retryCount: 0,
      requestContext: new RequestContext(),
      abort: abortThrowing,
    };

    // #then — this chunk's own text is still gated
    await expect(engine.processOutputStream(args)).rejects.toThrowError(
      /deny-patterns: matched blocked pattern/,
    );
  });

  it('ignores a text-delta whose payload.text is not a string', async () => {
    // #given — a malformed chunk; the literal "undefined" must not be coerced
    // into the gated text and trip a policy
    const engine = new PolicyEngine({
      policies: [denyPatterns(['undefined'], { phases: ['output'] })],
    });
    // deliberately runtime-invalid to exercise the typeof guard
    const malformed = {
      runId: 'run',
      from: ChunkFrom.AGENT,
      type: 'text-delta',
      payload: { id: 'out', text: undefined },
    } as unknown as ChunkType;

    // #then — no coercion, no false positive; the chunk passes through
    await expect(
      engine.processOutputStream(makeStreamArgs([malformed])),
    ).resolves.toBe(malformed);
  });
});

describe('PolicyEngine output channels — streaming', () => {
  it('aborts a deny pattern completing in the reasoning channel and audits the channel', async () => {
    // #given — denyPatterns gates all channels by default; "secret"
    // straddles two reasoning-delta chunks
    const audit = new AuditLogger();
    const engine = new PolicyEngine({
      policies: [denyPatterns(['secret'], { phases: ['output'] })],
      audit,
    });
    const state: Record<string, unknown> = {};
    const first = reasoningDelta('the sec');
    const second = reasoningDelta('ret plan');

    // #then — the completing chunk aborts before emission...
    await expect(
      engine.processOutputStream(makeStreamArgs([first], state)),
    ).resolves.toBe(first);
    await expect(
      engine.processOutputStream(makeStreamArgs([first, second], state)),
    ).rejects.toThrowError(/deny-patterns: matched blocked pattern/);
    // ...and the denial names the channel it fired on
    expect(audit.events()[0]).toMatchObject({
      decision: 'denied',
      detail: { policy: 'deny-patterns', channel: 'reasoning' },
    });
  });

  it('does not evaluate an answer-only policy against reasoning chunks', async () => {
    // #given — a deny policy narrowed to the answer channel
    const engine = new PolicyEngine({
      policies: [
        denyPatterns(['secret'], { phases: ['output'], channels: ['answer'] }),
      ],
    });
    const part = reasoningDelta('the secret plan');

    // #when / #then — reasoning text passes an answer-only policy untouched
    await expect(
      engine.processOutputStream(makeStreamArgs([part])),
    ).resolves.toBe(part);
  });

  it('aborts on a denied pattern inside a structured-object snapshot', async () => {
    // #given — the object channel gates the stringified snapshot
    const engine = new PolicyEngine({
      policies: [denyPatterns(['hunter2'], { phases: ['output'] })],
    });
    const part = objectChunk({ password: 'hunter2' });

    // #when / #then
    await expect(
      engine.processOutputStream(makeStreamArgs([part])),
    ).rejects.toThrowError(/deny-patterns: matched blocked pattern/);
  });

  it('evaluates object snapshots as replacements, not concatenations', async () => {
    // #given — a cap that the CONCATENATION of the two snapshots would
    // exceed but the latest snapshot alone does not (partials are growing
    // snapshots of the same object, not deltas)
    const audit = new AuditLogger();
    const engine = new PolicyEngine({
      policies: [maxTextLength(30, { channels: ['object'] })],
      audit,
    });
    const state: Record<string, unknown> = {};
    const partial = objectChunk({ a: 'aaaaaaaaaa' });
    const final = objectResult({ a: 'aaaaaaaaaa', b: 1 });

    // #then — both pass because the second REPLACES the first
    await expect(
      engine.processOutputStream(makeStreamArgs([partial], state)),
    ).resolves.toBe(partial);
    await expect(
      engine.processOutputStream(makeStreamArgs([partial, final], state)),
    ).resolves.toBe(final);
  });

  it('keeps channel caps independent: long reasoning does not trip an answer cap', async () => {
    // #given — a 10-char answer cap; 100 chars of reasoning stream first
    const engine = new PolicyEngine({ policies: [maxTextLength(10)] });
    const state: Record<string, unknown> = {};
    const reasoning = reasoningDelta('r'.repeat(100));
    const answer = textDelta('short');

    // #then — reasoning text never counts toward the answer cap
    await expect(
      engine.processOutputStream(makeStreamArgs([reasoning], state)),
    ).resolves.toBe(reasoning);
    await expect(
      engine.processOutputStream(makeStreamArgs([reasoning, answer], state)),
    ).resolves.toBe(answer);
  });

  it('caps the reasoning channel with an explicit reasoning instance', async () => {
    // #given
    const engine = new PolicyEngine({
      policies: [maxTextLength(10, { channels: ['reasoning'] })],
    });

    // #when / #then
    await expect(
      engine.processOutputStream(
        makeStreamArgs([reasoningDelta('r'.repeat(11))]),
      ),
    ).rejects.toThrowError(/max-text-length: text length 11 exceeds limit 10/);
  });
});

describe('PolicyEngine output channels — result phase', () => {
  it('gates result-phase reasoning from the per-step aggregates', async () => {
    // #given — clean answer text; the deny pattern hides in a step's
    // reasoningText
    const engine = new PolicyEngine({
      policies: [denyPatterns(['secret'], { phases: ['output'] })],
    });
    const args = makeOutputArgs(
      'clean answer',
      [],
      [reasoningStep('the secret plan')],
    );

    // #when / #then
    await expect(engine.processOutputResult(args)).rejects.toThrowError(
      /deny-patterns: matched blocked pattern/,
    );
  });

  it('does not count reasoning toward an answer-channel length cap', async () => {
    // #given — a 20-char answer cap and 100 chars of step reasoning
    const engine = new PolicyEngine({ policies: [maxTextLength(20)] });
    const args = makeOutputArgs(
      'short answer',
      [],
      [reasoningStep('r'.repeat(100))],
    );

    // #when / #then
    await expect(engine.processOutputResult(args)).resolves.toBe(args.messages);
  });

  it('emits one terminal allowed record aggregating the channel passes', async () => {
    // #given — an all-channel policy plus an answer-only cap, and a result
    // carrying both answer text and reasoning
    const audit = new AuditLogger();
    const engine = new PolicyEngine({
      policies: [
        denyPatterns(['nope'], { phases: ['output'] }),
        maxTextLength(1000),
      ],
      audit,
    });
    const args = makeOutputArgs('fine', [], [reasoningStep('also fine')]);

    // #when
    await engine.processOutputResult(args);

    // #then — one record; names deduplicated across the answer+reasoning
    // passes (deny-patterns ran in both)
    expect(audit.events()).toHaveLength(1);
    expect(audit.events()[0]).toMatchObject({
      decision: 'allowed',
      detail: { evaluated: ['deny-patterns', 'max-text-length'] },
    });
  });
});

describe('PolicyEngine hold-back buffering', () => {
  // Literal per @mastra/core dist/processors/stream-reprocess.d.ts — the
  // runner takes-and-clears this key, then re-drives the stashed part.
  const REPROCESS_KEY = '__mastraReprocessPart';

  function finishChunk(): ChunkType {
    return {
      runId: 'run',
      from: ChunkFrom.AGENT,
      type: 'finish',
      payload: {},
    } as unknown as ChunkType;
  }

  function errorChunk(): ChunkType {
    return {
      runId: 'run',
      from: ChunkFrom.AGENT,
      type: 'error',
      payload: { error: 'boom' },
    } as unknown as ChunkType;
  }

  function textEnd(id = 'out'): ChunkType {
    return {
      runId: 'run',
      from: ChunkFrom.AGENT,
      type: 'text-end',
      payload: { id },
    } as unknown as ChunkType;
  }

  function reasoningEnd(): ChunkType {
    return {
      runId: 'run',
      from: ChunkFrom.AGENT,
      type: 'reasoning-end',
      payload: { id: 'reason' },
    } as unknown as ChunkType;
  }

  function textOf(chunk: ChunkType | null | undefined): string {
    const text = (chunk as { payload?: { text?: unknown } } | null | undefined)
      ?.payload?.text;
    return typeof text === 'string' ? text : '';
  }

  function idOf(chunk: ChunkType | null | undefined): string | undefined {
    return (chunk as { payload?: { id?: string } } | null | undefined)?.payload
      ?.id;
  }

  it('emits no char of a violating span: pattern split across three chunks', async () => {
    // #given — holdBack on; "secret" (window 5) straddles chunks 1-3
    const engine = new PolicyEngine({
      policies: [denyPatterns(['secret'], { phases: ['output'] })],
      holdBack: true,
    });
    const state: Record<string, unknown> = {};
    const chunks = [
      textDelta('the se'),
      textDelta('cr'),
      textDelta('et leaked'),
    ];
    const emitted: string[] = [];

    // #when — the first two chunks evaluate clean and release only text
    // outside the held window...
    emitted.push(
      textOf(
        await engine.processOutputStream(
          makeStreamArgs(chunks.slice(0, 1), state),
        ),
      ),
    );
    emitted.push(
      textOf(
        await engine.processOutputStream(
          makeStreamArgs(chunks.slice(0, 2), state),
        ),
      ),
    );
    // ...the third completes the pattern and aborts
    await expect(
      engine.processOutputStream(makeStreamArgs(chunks, state)),
    ).rejects.toThrowError(/deny-patterns: matched blocked pattern/);

    // #then — the zero-leak win: nothing emitted contains ANY char of the
    // match ("secret" starts at index 4; only "the" ever left the engine)
    expect(emitted.join('')).toBe('the');
  });

  it('round-trips a clean stream: released text + finish flush equals the input', async () => {
    // #given
    const engine = new PolicyEngine({
      policies: [denyPatterns(['secret'], { phases: ['output'] })],
      holdBack: true,
    });
    const state: Record<string, unknown> = {};
    const inputs = ['hello ', 'wor', 'ld!'];
    const parts: ChunkType[] = [];
    let emitted = '';

    // #when — stream the deltas...
    for (const text of inputs) {
      parts.push(textDelta(text));
      emitted += textOf(
        await engine.processOutputStream(makeStreamArgs([...parts], state)),
      );
    }
    // ...then drive finish exactly like the runner's drainReprocessParts:
    // while the processor returned a substitute chunk and stashed the finish
    // part, take-and-clear the stash and re-feed it through the chain
    let outcome = await engine.processOutputStream(
      makeStreamArgs([...parts, finishChunk()], state),
    );
    while (state[REPROCESS_KEY] !== undefined) {
      const stashed = state[REPROCESS_KEY] as ChunkType;
      delete state[REPROCESS_KEY];
      emitted += textOf(outcome);
      outcome = await engine.processOutputStream(
        makeStreamArgs([...parts, stashed], state),
      );
    }

    // #then — the finish part ultimately flows through and no text was lost
    expect((outcome as { type?: string })?.type).toBe('finish');
    expect(emitted).toBe('hello world!');
  });

  it('emits nothing before finish under a RegExp policy (Infinity window)', async () => {
    // #given — any RegExp hints Infinity: the whole stream stays buffered
    const engine = new PolicyEngine({
      policies: [denyPatterns([/secret-\d+/], { phases: ['output'] })],
      holdBack: true,
    });
    const state: Record<string, unknown> = {};

    // #when / #then — clean deltas are held, not emitted
    await expect(
      engine.processOutputStream(
        makeStreamArgs([textDelta('all clear ')], state),
      ),
    ).resolves.toBeNull();
    await expect(
      engine.processOutputStream(
        makeStreamArgs([textDelta('all clear '), textDelta('here')], state),
      ),
    ).resolves.toBeNull();

    // #then — the finish flush releases the full evaluated-clean text and
    // stashes finish for the runner to re-drive
    const flush = await engine.processOutputStream(
      makeStreamArgs([finishChunk()], state),
    );
    expect(textOf(flush)).toBe('all clear here');
    expect(state[REPROCESS_KEY]).toMatchObject({ type: 'finish' });
  });

  it('passes chunks through unmodified when every policy hints window 0', async () => {
    // #given — maxTextLength holds nothing back
    const engine = new PolicyEngine({
      policies: [maxTextLength(100)],
      holdBack: true,
    });
    const part = textDelta('streaming right through');

    // #when / #then — the SAME chunk object comes back (no coalescing)
    await expect(
      engine.processOutputStream(makeStreamArgs([part])),
    ).resolves.toBe(part);
  });

  it('suppresses intermediate object snapshots and emits the passing object-result', async () => {
    // #given
    const engine = new PolicyEngine({
      policies: [denyPatterns(['hunter2'], { phases: ['output'] })],
      holdBack: true,
    });
    const state: Record<string, unknown> = {};
    const partial = objectChunk({ a: 1 });
    const final = objectResult({ a: 1, b: 2 });

    // #when / #then — intermediates evaluated but never emitted; the final
    // object-result is emitted once it passes
    await expect(
      engine.processOutputStream(makeStreamArgs([partial], state)),
    ).resolves.toBeNull();
    await expect(
      engine.processOutputStream(makeStreamArgs([partial, final], state)),
    ).resolves.toBe(final);
  });

  it('drops pending text when the stream errors instead of emitting after the failure', async () => {
    // #given — held text (Infinity window), then an error chunk
    const engine = new PolicyEngine({
      policies: [denyPatterns([/anything-\d/], { phases: ['output'] })],
      holdBack: true,
    });
    const state: Record<string, unknown> = {};
    await engine.processOutputStream(
      makeStreamArgs([textDelta('held')], state),
    );

    // #when — the error passes through...
    const error = errorChunk();
    await expect(
      engine.processOutputStream(makeStreamArgs([error], state)),
    ).resolves.toBe(error);

    // #then — ...and a later finish has nothing to flush
    const finish = finishChunk();
    await expect(
      engine.processOutputStream(makeStreamArgs([finish], state)),
    ).resolves.toBe(finish);
  });

  it('drains the finish-flush through the REAL ProcessorRunner (reprocess-key tripwire)', async () => {
    // #given — a hold-back engine behind core's actual runner. This is the
    // guardrail for the private '__mastraReprocessPart' convention: if core
    // renames the key or changes its drain semantics, the stashed finish is
    // never found, the flush assertions below fail, and the drift surfaces
    // here instead of silently degrading zero-leak in production.
    const engine = new PolicyEngine({
      policies: [denyPatterns([/x-\d/], { phases: ['output'] })],
      holdBack: true,
    });
    const noopLogger = {
      debug() {},
      info() {},
      warn() {},
      error() {},
    } as unknown as ConstructorParameters<typeof ProcessorRunner>[0]['logger'];
    const runner = new ProcessorRunner({
      outputProcessors: [engine],
      logger: noopLogger,
      agentName: 'tripwire',
    });
    const states = new Map<string, ProcessorState>();
    const requestContext = new RequestContext();

    // #when — a clean delta is held (Infinity window: any RegExp policy)...
    const held = await runner.processPart(
      textDelta('all clear'),
      states,
      undefined,
      requestContext,
    );
    expect(held.part).toBeNull();

    // ...the finish pass returns the coalesced flush...
    const flush = await runner.processPart(
      finishChunk(),
      states,
      undefined,
      requestContext,
    );
    expect(textOf(flush.part)).toBe('all clear');

    // #then — the runner's own drain finds the stashed finish under its
    // private key, take-and-clears it, and re-drives it through the chain,
    // where it now flows through clean
    const drained = await runner.drainReprocessParts(
      states,
      undefined,
      requestContext,
    );
    expect(drained).toHaveLength(1);
    expect((drained[0]?.part as { type?: string } | null)?.type).toBe('finish');
  });

  it('drains multiple held channels over successive finish re-drives', async () => {
    // #given — pending text on both answer and reasoning (Infinity windows)
    const engine = new PolicyEngine({
      policies: [denyPatterns([/x-\d/], { phases: ['output'] })],
      holdBack: true,
    });
    const state: Record<string, unknown> = {};
    await engine.processOutputStream(
      makeStreamArgs([textDelta('final answer')], state),
    );
    await engine.processOutputStream(
      makeStreamArgs([reasoningDelta('the trace')], state),
    );

    // #when — the first finish pass flushes the answer channel (on the
    // channel's own chunk shape) and stashes finish
    const finish = finishChunk();
    const flushAnswer = await engine.processOutputStream(
      makeStreamArgs([finish], state),
    );
    expect(flushAnswer).toMatchObject({ type: 'text-delta' });
    expect(textOf(flushAnswer)).toBe('final answer');
    expect(state[REPROCESS_KEY]).toBe(finish);

    // #when — the runner re-drives the stashed finish: reasoning flushes
    delete state[REPROCESS_KEY];
    const flushReasoning = await engine.processOutputStream(
      makeStreamArgs([finish], state),
    );
    expect(flushReasoning).toMatchObject({ type: 'reasoning-delta' });
    expect(textOf(flushReasoning)).toBe('the trace');
    expect(state[REPROCESS_KEY]).toBe(finish);

    // #then — the third pass has nothing pending; finish flows through
    delete state[REPROCESS_KEY];
    await expect(
      engine.processOutputStream(makeStreamArgs([finish], state)),
    ).resolves.toBe(finish);
  });

  it('flushes the held tail before text-end and re-drives the end chunk', async () => {
    // #given — Infinity window: the whole segment is pending at its end
    const engine = new PolicyEngine({
      policies: [denyPatterns([/x-\d/], { phases: ['output'] })],
      holdBack: true,
    });
    const state: Record<string, unknown> = {};
    const first = textDelta('all clear ');
    const second = textDelta('here');
    const deltas = [first, second];
    await engine.processOutputStream(makeStreamArgs([first], state));
    await engine.processOutputStream(makeStreamArgs([...deltas], state));

    // #when — the end chunk arrives with text still pending
    const end = textEnd();
    const flush = await engine.processOutputStream(
      makeStreamArgs([...deltas, end], state),
    );

    // #then — the tail flushes FIRST; the end chunk is stashed for the
    // runner to re-drive, where it now flows through with nothing pending
    expect(flush).toMatchObject({ type: 'text-delta' });
    expect(textOf(flush)).toBe('all clear here');
    expect(state[REPROCESS_KEY]).toBe(end);
    delete state[REPROCESS_KEY];
    await expect(
      engine.processOutputStream(makeStreamArgs([...deltas, end], state)),
    ).resolves.toBe(end);
    // ...and the finish backstop has nothing left to flush
    const finish = finishChunk();
    await expect(
      engine.processOutputStream(makeStreamArgs([finish], state)),
    ).resolves.toBe(finish);
  });

  it('reasoning-end flushes only the reasoning channel', async () => {
    // #given — pending text on both channels (Infinity windows)
    const engine = new PolicyEngine({
      policies: [denyPatterns([/x-\d/], { phases: ['output'] })],
      holdBack: true,
    });
    const state: Record<string, unknown> = {};
    await engine.processOutputStream(
      makeStreamArgs([textDelta('final answer')], state),
    );
    await engine.processOutputStream(
      makeStreamArgs([reasoningDelta('the trace')], state),
    );

    // #when — reasoning ends
    const end = reasoningEnd();
    const flush = await engine.processOutputStream(
      makeStreamArgs([end], state),
    );

    // #then — only the reasoning tail flushes; the answer stays held for
    // its own flush point (finish here)
    expect(flush).toMatchObject({ type: 'reasoning-delta' });
    expect(textOf(flush)).toBe('the trace');
    expect(state[REPROCESS_KEY]).toBe(end);
    delete state[REPROCESS_KEY];
    await expect(
      engine.processOutputStream(makeStreamArgs([end], state)),
    ).resolves.toBe(end);
    const answerFlush = await engine.processOutputStream(
      makeStreamArgs([finishChunk()], state),
    );
    expect(answerFlush).toMatchObject({ type: 'text-delta' });
    expect(textOf(answerFlush)).toBe('final answer');
  });

  it('reassembles the input in order across a finite-window end flush', async () => {
    // #given — "secret" hints window 5; releases happen per delta, the
    // trailing window flushes at the end chunk
    const engine = new PolicyEngine({
      policies: [denyPatterns(['secret'], { phases: ['output'] })],
      holdBack: true,
    });
    const state: Record<string, unknown> = {};
    const first = textDelta('hello ');
    const second = textDelta('world');
    const deltas = [first, second];
    let emitted = '';

    // #when
    emitted += textOf(
      await engine.processOutputStream(makeStreamArgs([first], state)),
    );
    emitted += textOf(
      await engine.processOutputStream(makeStreamArgs([...deltas], state)),
    );
    const end = textEnd();
    emitted += textOf(
      await engine.processOutputStream(makeStreamArgs([...deltas, end], state)),
    );

    // #then — nothing lost, nothing reordered, end chunk stashed after the
    // tail it closes
    expect(emitted).toBe('hello world');
    expect(state[REPROCESS_KEY]).toBe(end);
  });

  it('restarts a fresh segment cleanly after an end-chunk flush', async () => {
    // #given — Infinity window; two text segments with distinct part ids
    const engine = new PolicyEngine({
      policies: [denyPatterns([/x-\d/], { phases: ['output'] })],
      holdBack: true,
    });
    const state: Record<string, unknown> = {};
    const first = textDelta('first segment ', 'seg1');
    await engine.processOutputStream(makeStreamArgs([first], state));
    const endFirst = textEnd('seg1');
    const flushFirst = await engine.processOutputStream(
      makeStreamArgs([first, endFirst], state),
    );
    delete state[REPROCESS_KEY];
    await engine.processOutputStream(makeStreamArgs([first, endFirst], state));

    // #when — the second segment accumulates under its own id and ends
    const second = textDelta('second segment', 'seg2');
    await engine.processOutputStream(
      makeStreamArgs([first, endFirst, second], state),
    );
    const endSecond = textEnd('seg2');
    const flushSecond = await engine.processOutputStream(
      makeStreamArgs([first, endFirst, second, endSecond], state),
    );

    // #then — each flush carries only its own segment's text, on its own
    // segment's shape: no cross-segment contamination
    expect(textOf(flushFirst)).toBe('first segment ');
    expect(idOf(flushFirst)).toBe('seg1');
    expect(textOf(flushSecond)).toBe('second segment');
    expect(idOf(flushSecond)).toBe('seg2');
  });

  it('flushes each channel at its own end chunk within one stream', async () => {
    // #given — pending text on both channels (Infinity windows)
    const engine = new PolicyEngine({
      policies: [denyPatterns([/x-\d/], { phases: ['output'] })],
      holdBack: true,
    });
    const state: Record<string, unknown> = {};
    await engine.processOutputStream(
      makeStreamArgs([textDelta('final answer')], state),
    );
    await engine.processOutputStream(
      makeStreamArgs([reasoningDelta('the trace')], state),
    );

    // #when / #then — reasoning ends first and flushes only its own tail
    const reasoningClose = reasoningEnd();
    expect(
      textOf(
        await engine.processOutputStream(
          makeStreamArgs([reasoningClose], state),
        ),
      ),
    ).toBe('the trace');
    delete state[REPROCESS_KEY];
    await expect(
      engine.processOutputStream(makeStreamArgs([reasoningClose], state)),
    ).resolves.toBe(reasoningClose);
    // ...then the answer flushes at its own end chunk, not at finish
    const answerClose = textEnd();
    expect(
      textOf(
        await engine.processOutputStream(makeStreamArgs([answerClose], state)),
      ),
    ).toBe('final answer');
    delete state[REPROCESS_KEY];
    await expect(
      engine.processOutputStream(makeStreamArgs([answerClose], state)),
    ).resolves.toBe(answerClose);
    // ...and finish has nothing left to flush
    const finish = finishChunk();
    await expect(
      engine.processOutputStream(makeStreamArgs([finish], state)),
    ).resolves.toBe(finish);
  });

  it('passes an end chunk through when nothing is pending', async () => {
    // #given — window 0: deltas flow through, nothing is ever held
    const engine = new PolicyEngine({
      policies: [maxTextLength(100)],
      holdBack: true,
    });
    const state: Record<string, unknown> = {};
    await engine.processOutputStream(
      makeStreamArgs([textDelta('all out')], state),
    );

    // #when / #then — the SAME chunk comes back, nothing stashed
    const end = textEnd();
    await expect(
      engine.processOutputStream(makeStreamArgs([end], state)),
    ).resolves.toBe(end);
    expect(state[REPROCESS_KEY]).toBeUndefined();
  });

  it('emits the flush before the end marker through the REAL ProcessorRunner', async () => {
    // #given — the end-chunk twin of the finish tripwire test: proves the
    // runner's per-part drain re-drives the stashed end chunk immediately,
    // so downstream order is flush-delta → text-end → finish
    const engine = new PolicyEngine({
      policies: [denyPatterns([/x-\d/], { phases: ['output'] })],
      holdBack: true,
    });
    const noopLogger = {
      debug() {},
      info() {},
      warn() {},
      error() {},
    } as unknown as ConstructorParameters<typeof ProcessorRunner>[0]['logger'];
    const runner = new ProcessorRunner({
      outputProcessors: [engine],
      logger: noopLogger,
      agentName: 'ordering',
    });
    const states = new Map<string, ProcessorState>();
    const requestContext = new RequestContext();

    // #when — a clean delta is held...
    const held = await runner.processPart(
      textDelta('all clear'),
      states,
      undefined,
      requestContext,
    );
    expect(held.part).toBeNull();

    // ...the end pass returns the coalesced flush...
    const flush = await runner.processPart(
      textEnd(),
      states,
      undefined,
      requestContext,
    );
    expect(textOf(flush.part)).toBe('all clear');

    // #then — the runner's own drain re-drives the stashed text-end right
    // after the flush, and the later finish has nothing left to flush
    const drained = await runner.drainReprocessParts(
      states,
      undefined,
      requestContext,
    );
    expect(drained).toHaveLength(1);
    expect((drained[0]?.part as { type?: string } | null)?.type).toBe(
      'text-end',
    );
    const finish = await runner.processPart(
      finishChunk(),
      states,
      undefined,
      requestContext,
    );
    expect((finish.part as { type?: string } | null)?.type).toBe('finish');
    await expect(
      runner.drainReprocessParts(states, undefined, requestContext),
    ).resolves.toHaveLength(0);
  });

  it('leaves the held tail unemitted when the stream ends without a flush trigger', async () => {
    // #given — an Infinity-window policy (any RegExp) holds everything until
    // text-end/reasoning-end/finish; this driver simply stops after two
    // clean deltas without ever sending one (the documented truncation case)
    const engine = new PolicyEngine({
      policies: [denyPatterns([/x-\d/], { phases: ['output'] })],
      holdBack: true,
    });
    const state: Record<string, unknown> = {};

    // #when
    const first = await engine.processOutputStream(
      makeStreamArgs([textDelta('all clear ')], state),
    );
    const second = await engine.processOutputStream(
      makeStreamArgs([textDelta('all clear '), textDelta('here')], state),
    );

    // #then — both deltas were held; nothing ever reached the caller. The
    // clean trailing text is silently dropped, not leaked (no leak; this
    // pins the documented loss, it does not fix it).
    expect(first).toBeNull();
    expect(second).toBeNull();
  });
});

describe('extractMessageText', () => {
  it('does not double-count content.content when text parts exist', () => {
    // #given — content.content mirrors the text part, as MessageList
    // commonly produces
    const mirrored: MastraDBMessage = {
      id: `msg-${++messageSeq}`,
      role: 'user',
      createdAt: new Date(),
      content: {
        format: 2,
        parts: [{ type: 'text', text: 'same text' }],
        content: 'same text',
      },
    };

    // #when / #then — counted once, so length policies see 9 chars, not 18
    expect(extractMessageText([mirrored])).toBe('same text');
  });

  it('falls back to content.content only when a message has no text parts', () => {
    // #given
    const legacyOnly: MastraDBMessage = {
      id: `msg-${++messageSeq}`,
      role: 'user',
      createdAt: new Date(),
      content: { format: 2, parts: [], content: 'legacy content' },
    };

    // #when / #then
    expect(extractMessageText([legacyOnly, makeMessage('second')])).toBe(
      'legacy content\nsecond',
    );
  });
});
