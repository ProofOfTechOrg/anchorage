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

import { AGENT_AUDIT_CONTEXT_KEY, AuditLogger } from '../audit/index.js';
import { ACTOR_CONTEXT_KEY, type Actor } from '../rbac/index.js';
import {
  type ContentPolicyGate,
  type ContentPolicyGateInput,
  type ContentPolicyGateOptions,
  createContentPolicyGate,
  denyPatterns,
  extractMessageText,
  maxTextLength,
  type OutputChannel,
  PolicyEngine,
  type PolicyEngineOptions,
  type PolicyEvaluator,
  type PolicyPhase,
  piiSecrets,
} from './index.js';

class Tripwire extends Error {}

class PrivateFieldPolicy implements PolicyEvaluator {
  readonly name = 'private-field-policy';
  readonly phases = ['output'] as const;
  readonly #blocked: string;

  constructor(blocked: string) {
    this.#blocked = blocked;
  }

  evaluate({ text }: Parameters<PolicyEvaluator['evaluate']>[0]) {
    return text.includes(this.#blocked)
      ? { allowed: false as const, reason: 'matched private field' }
      : { allowed: true as const };
  }
}

class MutableInputPolicy implements PolicyEvaluator {
  name = 'snapshotted-input';
  phases: PolicyPhase[] = ['input'];
  channels: OutputChannel[] = ['answer'];
  readonly #blocked: string;

  constructor(blocked: string) {
    this.#blocked = blocked;
  }

  evaluate({ text }: Parameters<PolicyEvaluator['evaluate']>[0]) {
    return text.includes(this.#blocked)
      ? { allowed: false as const, reason: 'matched private field' }
      : { allowed: true as const };
  }
}

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
      reason: 'policy denied',
      detail: { policy: 'deny-patterns' },
    });
  });

  it('never copies evaluator reasons or blocked patterns into audit events', async () => {
    const sentinel = 'sk_live_audit-must-not-contain-this';
    const evaluatorAudit = new AuditLogger();
    const evaluator = new PolicyEngine({
      policies: [
        {
          name: 'opaque-denial',
          evaluate: () => ({
            allowed: false,
            reason: `prompt contained ${sentinel}`,
          }),
        },
      ],
      audit: evaluatorAudit,
    });
    const patternAudit = new AuditLogger();
    const pattern = new PolicyEngine({
      policies: [denyPatterns([sentinel])],
      audit: patternAudit,
    });

    await expect(
      evaluator.processInput(makeInputArgs(sentinel)),
    ).rejects.toThrow(sentinel);
    await expect(pattern.processInput(makeInputArgs(sentinel))).rejects.toThrow(
      sentinel,
    );

    expect(evaluatorAudit.events()[0]?.reason).toBe('policy denied');
    expect(patternAudit.events()[0]?.reason).toBe('policy denied');
    expect(
      JSON.stringify([...evaluatorAudit.events(), ...patternAudit.events()]),
    ).not.toContain(sentinel);
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

  it('records an error audit event and aborts with a static reason when an evaluator throws', async () => {
    // #given
    const audit = new AuditLogger();
    const crashing: PolicyEvaluator = {
      name: 'crashy',
      evaluate: () => {
        throw new Error('evaluator internal failure');
      },
    };
    const engine = new PolicyEngine({ policies: [crashing], audit });

    // #when / #then — the crash aborts (fail closed) AND leaves an audit
    // record; an internal error must not leave less evidence than a denial.
    await expect(engine.processInput(makeInputArgs('x'))).rejects.toEqual(
      new Tripwire('policy evaluation failed'),
    );
    expect(audit.events()).toHaveLength(1);
    expect(audit.events()[0]).toMatchObject({
      decision: 'error',
      reason: 'policy evaluation failed',
      detail: { policy: 'crashy' },
    });
    expect(JSON.stringify(audit.events())).not.toContain(
      'evaluator internal failure',
    );
  });

  it('records an error audit event and aborts when an async evaluator rejects', async () => {
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
    await expect(engine.processInput(makeInputArgs('x'))).rejects.toEqual(
      new Tripwire('policy evaluation failed'),
    );
    expect(audit.events()).toMatchObject([{ decision: 'error' }]);
  });

  const NOT_DECISIONS: ReadonlyArray<[string, unknown]> = [
    ['undefined', undefined],
    ['null', null],
    ['a string', 'allowed'],
    ['a non-boolean allowed', { allowed: 'yes' }],
    ['a denial with a non-string reason', { allowed: false, reason: 42 }],
  ];

  it.each(
    NOT_DECISIONS,
  )('records an error event and aborts input for an evaluator that returns %s', async (_label, value) => {
    // #given
    const audit = new AuditLogger();
    const undecided: PolicyEvaluator = {
      name: 'undecided',
      evaluate: () => value as never,
    };
    const engine = new PolicyEngine({
      policies: [undecided, denyPatterns(['x'])],
      audit,
    });

    // #when / #then — the later policy never runs
    await expect(engine.processInput(makeInputArgs('x'))).rejects.toEqual(
      new Tripwire('policy evaluation failed'),
    );
    expect(audit.events()).toMatchObject([
      {
        action: 'agent.input.policy',
        decision: 'error',
        reason: 'policy evaluation failed',
        detail: { policy: 'undecided' },
      },
    ]);
  });

  it('answers a policy that returns no decision with the content gate error outcome', async () => {
    // #given
    const audit = new AuditLogger();
    const gate = createContentPolicyGate({
      policies: [{ name: 'undecided', evaluate: () => undefined as never }],
      audit,
    });

    // #when / #then
    await expect(gate({ text: 'hello' })).resolves.toEqual({
      allowed: false,
      outcome: 'error',
    });
    expect(audit.events()).toMatchObject([
      { decision: 'error', reason: 'policy evaluation failed' },
    ]);
  });

  it('rethrows a static TypeError at the final result for a policy that returns no decision', async () => {
    // #given
    const engine = new PolicyEngine({
      policies: [
        {
          name: 'undecided',
          phases: ['output'],
          evaluate: () => undefined as never,
        },
      ],
    });

    // #when / #then
    await expect(
      engine.processOutputResult(makeOutputArgs('answer')),
    ).rejects.toEqual(new TypeError('policy evaluator returned no decision'));
  });

  it('removes the call input from the message list before an input abort', async () => {
    // #given — the list holds the call's input, as Mastra's does
    const engine = new PolicyEngine({ policies: [denyPatterns(['blocked'])] });
    const args = makeInputArgs('blocked text');
    args.messageList.add(args.messages, 'input');

    // #when / #then
    await expect(engine.processInput(args)).rejects.toThrowError(Tripwire);
    expect(args.messageList.get.input.db()).toEqual([]);
  });

  it('keeps the call input in the message list when the input is allowed', async () => {
    // #given
    const engine = new PolicyEngine({ policies: [denyPatterns(['blocked'])] });
    const args = makeInputArgs('clean text');
    args.messageList.add(args.messages, 'input');

    // #when
    await engine.processInput(args);

    // #then
    expect(args.messageList.get.input.db()).toHaveLength(1);
  });
});

describe('createContentPolicyGate', () => {
  it('evaluates the exact input context and records the existing allowed audit vocabulary', async () => {
    const actor: Actor = { id: 'signal-sender', role: 'operator' };
    const requestContext = new RequestContext();
    requestContext.set(ACTOR_CONTEXT_KEY, actor);
    requestContext.set(AGENT_AUDIT_CONTEXT_KEY, {
      agentId: 'support-agent',
      threadId: 'thread-7',
      entryPath: 'signal.message',
    });
    const audit = new AuditLogger();
    let observed: Parameters<PolicyEvaluator['evaluate']>[0] | undefined;
    const gate = createContentPolicyGate({
      policies: [
        {
          name: 'observe-context',
          evaluate: async (context) => {
            observed = context;
            return { allowed: true };
          },
        },
      ],
      audit,
      resource: 'signal-content',
    });

    await expect(
      gate({ text: '<message>hello</message>', requestContext }),
    ).resolves.toEqual({ allowed: true });

    expect(Object.keys(observed ?? {}).sort()).toEqual([
      'channel',
      'messages',
      'phase',
      'requestContext',
      'text',
    ]);
    expect(observed).toMatchObject({
      phase: 'input',
      channel: 'answer',
      messages: [],
      text: '<message>hello</message>',
    });
    expect(observed?.requestContext).toBe(requestContext);
    expect(audit.events()).toHaveLength(1);
    expect(audit.events()[0]).toMatchObject({
      actor,
      action: 'agent.input.policy',
      resource: 'signal-content',
      decision: 'allowed',
      detail: {
        evaluated: ['observe-context'],
        agentId: 'support-agent',
        threadId: 'thread-7',
        entryPath: 'signal.message',
      },
    });
  });

  it('evaluates applicable policies in declaration order and stops on an opaque denial', async () => {
    const secret = 'sk_live_signal-content';
    const calls: string[] = [];
    const audit = new AuditLogger();
    const gate = createContentPolicyGate({
      policies: [
        {
          name: 'first',
          evaluate: async () => {
            calls.push('first');
            return { allowed: true };
          },
        },
        {
          name: 'blocker',
          evaluate: ({ text }) => {
            calls.push('blocker');
            return {
              allowed: false,
              reason: `blocked content ${text}`,
            };
          },
        },
        {
          name: 'never-called',
          evaluate: () => {
            calls.push('never-called');
            return { allowed: true };
          },
        },
      ],
      audit,
    });

    const result = await gate({ text: secret });

    expect(calls).toEqual(['first', 'blocker']);
    expect(result).toEqual({ allowed: false, outcome: 'denied' });
    expect(JSON.stringify(result)).not.toContain(secret);
    expect(audit.events()).toHaveLength(1);
    expect(audit.events()[0]).toMatchObject({
      action: 'agent.input.policy',
      resource: 'breakwater-content-policy-gate',
      decision: 'denied',
      reason: 'policy denied',
      detail: { policy: 'blocker', channel: 'answer' },
    });
    expect(JSON.stringify(audit.events())).not.toContain(secret);
  });

  it('maps an evaluator rejection to an opaque error and does not continue', async () => {
    const secret = 'provider-secret-in-exception';
    const failure = new Error(secret);
    const calls: string[] = [];
    const audit = new AuditLogger();
    const gate = createContentPolicyGate({
      policies: [
        {
          name: 'crashing-policy',
          evaluate: async () => {
            calls.push('crashing-policy');
            throw failure;
          },
        },
        {
          name: 'never-called',
          evaluate: () => {
            calls.push('never-called');
            return { allowed: true };
          },
        },
      ],
      audit,
    });

    const result = await gate({ text: 'model-visible text' });

    expect(calls).toEqual(['crashing-policy']);
    expect(result).toEqual({ allowed: false, outcome: 'error' });
    expect(JSON.stringify(result)).not.toContain(secret);
    expect(audit.events()).toHaveLength(1);
    expect(audit.events()[0]).toMatchObject({
      action: 'agent.input.policy',
      decision: 'error',
      reason: 'policy evaluation failed',
      detail: { policy: 'crashing-policy', channel: 'answer' },
    });
    expect(JSON.stringify(audit.events())).not.toContain(secret);
  });

  it('evaluates every registered policy, including phase- and channel-agnostic ones', async () => {
    const calls: string[] = [];
    const audit = new AuditLogger();
    const gate = createContentPolicyGate({
      policies: [
        {
          name: 'agnostic',
          evaluate: () => {
            calls.push('agnostic');
            return { allowed: true };
          },
        },
        {
          name: 'input-any-channel',
          phases: ['input'],
          evaluate: () => {
            calls.push('input-any-channel');
            return { allowed: true };
          },
        },
        {
          name: 'input-answer',
          phases: ['input'],
          channels: ['answer'],
          evaluate: () => {
            calls.push('input-answer');
            return { allowed: true };
          },
        },
      ],
      audit,
    });

    await expect(gate({ text: 'hello' })).resolves.toEqual({ allowed: true });
    expect(calls).toEqual(['agnostic', 'input-any-channel', 'input-answer']);
    expect(audit.events()[0]?.detail).toEqual({
      evaluated: ['agnostic', 'input-any-channel', 'input-answer'],
    });
  });

  // A policy this gate could never evaluate is a silent hole, not a harmless
  // no-op: the host wired it expecting inspection. Every unreachable selector
  // shape fails at construction rather than allowing everything at runtime.
  it.each([
    {
      case: 'output-only phases',
      policy: {
        name: 'output-only',
        phases: ['output'],
        evaluate: () => ({ allowed: true }),
      } satisfies PolicyEvaluator,
    },
    {
      case: 'non-answer channels',
      policy: {
        name: 'reasoning-only',
        channels: ['reasoning'],
        evaluate: () => ({ allowed: true }),
      } satisfies PolicyEvaluator,
    },
    {
      case: 'input phases with non-answer channels',
      policy: {
        name: 'input-reasoning-only',
        phases: ['input'],
        channels: ['reasoning'],
        evaluate: () => ({ allowed: true }),
      } satisfies PolicyEvaluator,
    },
  ])('rejects a policy that could never run here ($case)', ({ policy }) => {
    expect(() => createContentPolicyGate({ policies: [policy] })).toThrow(
      new RegExp(`${policy.name}.*would never run`, 'is'),
    );
  });

  it('snapshots policy declarations and preserves a class evaluator receiver', async () => {
    const phases: PolicyPhase[] = ['input'];
    const channels: OutputChannel[] = ['answer'];
    const policy = new MutableInputPolicy('blocked');
    policy.phases = phases;
    policy.channels = channels;
    const policies = [policy];
    const audit = new AuditLogger();
    const gate = createContentPolicyGate({ policies, audit });

    policies.length = 0;
    phases[0] = 'output';
    channels[0] = 'reasoning';
    policy.name = 'mutated';
    policy.evaluate = () => ({ allowed: true });

    await expect(gate({ text: 'blocked' })).resolves.toEqual({
      allowed: false,
      outcome: 'denied',
    });
    expect(audit.events()[0]?.detail).toMatchObject({
      policy: 'snapshotted-input',
    });
  });
});

describe('content gate call input', () => {
  const SECRET = 'contact john.doe@example.com now';
  const gates = (): Array<[string, ContentPolicyGate]> => [
    ['piiSecrets', createContentPolicyGate({ policies: [piiSecrets()] })],
    [
      'maxTextLength',
      createContentPolicyGate({
        policies: [maxTextLength(10, { phases: ['input'] })],
      }),
    ],
    [
      'denyPatterns',
      createContentPolicyGate({ policies: [denyPatterns(['john.doe'])] }),
    ],
  ];

  it.each<[string, unknown]>([
    ['the text passed bare', SECRET],
    ['text misspelled as txt', { txt: SECRET }],
    ['text misspelled as body', { body: SECRET }],
    ['text misspelled as contents', { contents: SECRET }],
    ['an undefined text', { text: undefined }],
    ['a null text', { text: null }],
    ['a text list', { text: [SECRET] }],
    [
      'message content as text',
      { text: { format: 2, parts: [{ type: 'text', text: SECRET }] } },
    ],
    ['a part list as text', { text: [{ type: 'text', text: SECRET }] }],
    ['a length-bearing object as text', { text: { length: 0 } }],
    ['the input inside a list', [{ text: SECRET }]],
    ['undefined', undefined],
    ['null', null],
    ['a plain-object requestContext', { text: SECRET, requestContext: {} }],
    ['a Map requestContext', { text: SECRET, requestContext: new Map() }],
    [
      'an extra field',
      { text: SECRET, requestContext: new RequestContext(), note: 'x' },
    ],
  ])('returns the error outcome for %s, whatever the policy', async (_label, input) => {
    for (const [, gate] of gates()) {
      // #when / #then
      await expect(gate(input as ContentPolicyGateInput)).resolves.toEqual({
        allowed: false,
        outcome: 'error',
      });
    }
  });

  it('records the static error for a malformed input and never evaluates it', async () => {
    // #given
    const audit = new AuditLogger();
    let evaluated = false;
    const gate = createContentPolicyGate({
      policies: [
        {
          name: 'observe',
          evaluate: () => {
            evaluated = true;
            return { allowed: true };
          },
        },
      ],
      audit,
    });

    // #when
    await gate({ txt: SECRET } as unknown as ContentPolicyGateInput);

    // #then
    expect(evaluated).toBe(false);
    expect(audit.events()).toEqual([
      expect.objectContaining({
        actor: null,
        action: 'agent.input.policy',
        resource: 'breakwater-content-policy-gate',
        decision: 'error',
        reason: 'content gate input is malformed',
      }),
    ]);
    expect(JSON.stringify(audit.events())).not.toContain('john.doe');
  });

  it('still denies the valid input under every policy, with or without a request context', async () => {
    for (const [, gate] of gates()) {
      for (const input of [
        { text: SECRET },
        { text: SECRET, requestContext: new RequestContext() },
      ]) {
        // #when / #then
        await expect(gate(input)).resolves.toEqual({
          allowed: false,
          outcome: 'denied',
        });
      }
    }
  });
});

describe('content evaluator text', () => {
  it.each<[string, () => PolicyEvaluator, unknown, string]>([
    ['denyPatterns', () => denyPatterns(['secret']), undefined, 'undefined'],
    ['denyPatterns', () => denyPatterns(['secret']), ['secret'], 'object'],
    ['maxTextLength', () => maxTextLength(10), ['a'.repeat(20)], 'object'],
    ['maxTextLength', () => maxTextLength(10), { length: 0 }, 'object'],
  ])('%s refuses a non-string text (%#)', async (subject, build, text, got) => {
    // #given
    const evaluator = build();

    // #when / #then
    await expect(async () =>
      evaluator.evaluate({
        phase: 'output',
        channel: 'answer',
        messages: [],
        text: text as string,
      }),
    ).rejects.toThrow(
      new TypeError(`${subject}: text must be a string (got ${got})`),
    );
  });

  it('turns the refusal into the engine error path', async () => {
    // #given
    const audit = new AuditLogger();
    const engine = new PolicyEngine({ policies: [maxTextLength(10)], audit });
    const args = makeOutputArgs('fine');
    (args.result as { text: unknown }).text = ['a'.repeat(20)];

    // #when / #then
    await expect(engine.processOutputResult(args)).rejects.toThrow(
      new TypeError('maxTextLength: text must be a string (got object)'),
    );
    expect(audit.events()[0]).toMatchObject({
      decision: 'error',
      reason: 'policy evaluation failed',
    });
  });
});

describe('class-based evaluators with instance state', () => {
  class ParameterPropertyPolicy implements PolicyEvaluator {
    readonly name = 'parameter-property';
    constructor(private readonly words: readonly string[]) {}
    evaluate({ text }: Parameters<PolicyEvaluator['evaluate']>[0]) {
      return this.words.some((word) => text.includes(word))
        ? { allowed: false as const, reason: 'matched a word' }
        : { allowed: true as const };
    }
  }

  class AssignedStatePolicy implements PolicyEvaluator {
    readonly name = 'assigned-state';
    readonly blocked: string;
    constructor(blocked: string) {
      this.blocked = blocked;
    }
    evaluate({ text }: Parameters<PolicyEvaluator['evaluate']>[0]) {
      return text.includes(this.blocked)
        ? { allowed: false as const, reason: 'matched assigned state' }
        : { allowed: true as const };
    }
  }

  class PublicFieldPolicy implements PolicyEvaluator {
    readonly name = 'public-field';
    minLength = 3;
    evaluate({ text }: Parameters<PolicyEvaluator['evaluate']>[0]) {
      return text.length >= this.minLength
        ? { allowed: false as const, reason: 'too long' }
        : { allowed: true as const };
    }
  }

  it.each<[string, () => PolicyEvaluator]>([
    [
      'a TypeScript parameter property',
      () => new ParameterPropertyPolicy(['blocked']),
    ],
    ['a constructor-assigned field', () => new AssignedStatePolicy('blocked')],
    ['a public class field', () => new PublicFieldPolicy()],
  ])('constructs and enforces an evaluator holding %s', async (_label, build) => {
    // #given
    const engine = new PolicyEngine({ policies: [build()] });
    const gate = createContentPolicyGate({ policies: [build()] });

    // #when / #then
    await expect(
      engine.processInput(makeInputArgs('blocked text')),
    ).rejects.toThrow(Tripwire);
    await expect(gate({ text: 'blocked text' })).resolves.toEqual({
      allowed: false,
      outcome: 'denied',
    });
    await expect(gate({ text: 'ok' })).resolves.toEqual({ allowed: true });
  });

  it('still refuses a misspelled field on a plain-object spread', () => {
    // #when / #then
    expect(
      () =>
        new PolicyEngine({
          policies: [
            {
              ...denyPatterns(['x']),
              holdbackChars: 5,
            } as unknown as PolicyEvaluator,
          ],
        }),
    ).toThrow(
      new TypeError(
        'PolicyEngine: policies entry 0 has unknown field "holdbackChars" (valid fields: name, phases, channels, holdBackChars, evaluate)',
      ),
    );
  });

  it('still refuses a misspelled field on a null-prototype object', () => {
    // #given
    const entry = Object.assign(Object.create(null), {
      ...denyPatterns(['x']),
      holdbackChars: 5,
    }) as PolicyEvaluator;

    // #when / #then
    expect(() => createContentPolicyGate({ policies: [entry] })).toThrow(
      new TypeError(
        'createContentPolicyGate: policies entry 0 has unknown field "holdbackChars" (valid fields: name, phases, channels, holdBackChars, evaluate)',
      ),
    );
  });
});

describe('malformed audit context at the engine and gate', () => {
  function contextWith(auditContext: unknown): RequestContext {
    const requestContext = new RequestContext();
    requestContext.set(ACTOR_CONTEXT_KEY, { id: 'u1', role: 'operator' });
    requestContext.set(AGENT_AUDIT_CONTEXT_KEY, auditContext);
    return requestContext;
  }

  it('records an error event on input and result, and still decides', async () => {
    // #given — a numeric tenant id is dropped from correlation
    const audit = new AuditLogger();
    const engine = new PolicyEngine({ policies: [], audit });
    const requestContext = contextWith({
      agentId: 'writer',
      entryPath: 'http-start',
      tenantId: 42,
    });
    const input = makeInputArgs('hello');
    const output = makeOutputArgs('fine');

    // #when
    await engine.processInput({ ...input, requestContext });
    await engine.processOutputResult({ ...output, requestContext });

    // #then
    expect(
      audit.events().map(({ action, decision }) => [action, decision]),
    ).toEqual([
      ['audit.context', 'error'],
      ['agent.input.policy', 'allowed'],
      ['audit.context', 'error'],
      ['agent.output.policy', 'allowed'],
    ]);
    expect(audit.events()[0]).toMatchObject({
      actor: { id: 'u1', role: 'operator' },
      resource: 'breakwater-policy-engine',
      reason: "request context 'breakwater.auditContext' is malformed",
      detail: { agentId: 'writer', entryPath: 'http-start' },
    });
  });

  it('records an error event at the gate, and still allows clean text', async () => {
    // #given
    const audit = new AuditLogger();
    const gate = createContentPolicyGate({
      policies: [denyPatterns(['secret'])],
      audit,
      resource: 'signal-content',
    });

    // #when
    const result = await gate({
      text: 'hello',
      requestContext: contextWith({ agentID: 'writer', entryPath: 'x' }),
    });

    // #then
    expect(result).toEqual({ allowed: true });
    expect(audit.events()).toMatchObject([
      {
        action: 'audit.context',
        resource: 'signal-content',
        decision: 'error',
      },
      { action: 'agent.input.policy', decision: 'allowed' },
    ]);
    expect(audit.events()[0]?.detail).toBeUndefined();
  });

  it('records nothing extra for a well-formed context with an undefined optional field', async () => {
    // #given
    const audit = new AuditLogger();
    const engine = new PolicyEngine({ policies: [], audit });
    const input = makeInputArgs('hello');

    // #when
    await engine.processInput({
      ...input,
      requestContext: contextWith({
        agentId: 'writer',
        entryPath: 'http-start',
        purpose: undefined,
      }),
    });

    // #then
    expect(audit.events().map(({ action }) => action)).toEqual([
      'agent.input.policy',
    ]);
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
    // #given — channels include 'object' but not 'answer', and no sink can
    // record the fail-closed coverage error if an invocation exposes no object
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
    // #given — the same policy, now with a sink for coverage errors
    const audit = new AuditLogger();
    const policy: PolicyEvaluator = {
      name: 'object-only',
      channels: ['object'],
      evaluate: () => ({ allowed: true }),
    };

    // #when / #then
    expect(() => new PolicyEngine({ policies: [policy], audit })).not.toThrow();
  });

  it('snapshots the policy list and decision-driving policy fields', async () => {
    const phases: Array<'input' | 'output'> = ['output'];
    const channels: Array<'answer' | 'object'> = ['answer'];
    const policy = denyPatterns(['blocked'], { phases, channels });
    const policies = [policy];
    const engine = new PolicyEngine({ policies });

    policies.length = 0;
    phases[0] = 'input';
    channels[0] = 'object';
    policy.name = 'mutated';
    policy.evaluate = () => ({ allowed: true });

    await expect(
      engine.processOutputResult(makeOutputArgs('blocked')),
    ).rejects.toThrowError(/deny-patterns: matched blocked pattern/);
  });

  it('snapshots hold-back hints before caller mutation', async () => {
    const policy = denyPatterns(['blocked'], { phases: ['output'] });
    const engine = new PolicyEngine({ policies: [policy], holdBack: true });
    policy.holdBackChars = 0;

    await expect(
      engine.processOutputStream(makeStreamArgs([textDelta('clean')], {})),
    ).resolves.toBeNull();
  });

  it('preserves a class evaluator receiver while capturing its method', async () => {
    const policy = new PrivateFieldPolicy('blocked');
    const engine = new PolicyEngine({ policies: [policy] });
    policy.evaluate = () => ({ allowed: true });

    await expect(
      engine.processOutputResult(makeOutputArgs('blocked')),
    ).rejects.toThrowError(/private-field-policy: matched private field/);
  });
});

// A real array whose own container methods and iterator answer with
// `answer`, never with its indexed entries.
function answeringList<T>(entries: readonly T[], answer: readonly unknown[]) {
  return Object.assign([...entries], {
    map: () => [...answer],
    some: () => answer.length > 0,
    every: () => true,
    includes: () => answer.length > 0,
    [Symbol.iterator]: function* () {
      yield* answer;
    },
  }) as T[];
}

function denySecret(overrides: Partial<PolicyEvaluator> = {}): PolicyEvaluator {
  return {
    name: 'deny-secret',
    evaluate: ({ text }) =>
      text.includes('secret')
        ? { allowed: false, reason: 'secret' }
        : { allowed: true },
    ...overrides,
  };
}

describe('policy selector validation', () => {
  it.each<[string, Partial<PolicyEvaluator>, string]>([
    [
      'a string phases selector',
      { phases: 'input' as unknown as PolicyPhase[] },
      "policy 'deny-secret' phases must be an array",
    ],
    [
      'a string channels selector',
      { channels: 'answer' as unknown as OutputChannel[] },
      "policy 'deny-secret' channels must be an array",
    ],
    [
      'an empty phases selector',
      { phases: [] },
      "policy 'deny-secret' phases must not be empty",
    ],
    [
      'an empty channels selector',
      { channels: [] },
      "policy 'deny-secret' channels must not be empty",
    ],
    [
      'an unknown phase',
      { phases: ['input', 'Output' as PolicyPhase] },
      `policy 'deny-secret' phases entry 1 must be 'input' or 'output' (got "Output")`,
    ],
    [
      'an unknown channel',
      { channels: ['answers' as OutputChannel] },
      `policy 'deny-secret' channels entry 0 must be 'answer', 'reasoning' or 'object' (got "answers")`,
    ],
    [
      'a non-string phase',
      { phases: [new String('input') as unknown as PolicyPhase] },
      `policy 'deny-secret' phases entry 0 must be 'input' or 'output' (got object)`,
    ],
  ])('refuses %s in PolicyEngine and createContentPolicyGate', (_label, overrides, message) => {
    // #given
    const policies = [denySecret(overrides)];
    // #when / #then — a policy whose selector selects nothing never runs
    expect(() => new PolicyEngine({ policies })).toThrow(
      new TypeError(`PolicyEngine: ${message}`),
    );
    expect(() => createContentPolicyGate({ policies })).toThrow(
      new TypeError(`createContentPolicyGate: ${message}`),
    );
  });

  it.each<[string, unknown]>([
    ['a single policy', denySecret()],
    ['an array-like object', { length: 1, 0: denySecret() }],
  ])('refuses %s as the policy list', (_label, policies) => {
    // #when / #then
    expect(
      () =>
        new PolicyEngine({ policies: policies as readonly PolicyEvaluator[] }),
    ).toThrow(new TypeError('PolicyEngine: policies must be an array'));
  });

  it('evaluates the policies it read by index, not the list its own map answers', async () => {
    // #given — own map answers with a policy that allows everything
    const policies = answeringList(
      [denySecret({ phases: ['input'] })],
      [{ name: 'allow-all', evaluate: () => ({ allowed: true }) }],
    );
    const engine = new PolicyEngine({ policies });
    // #when / #then
    await expect(
      engine.processInput(makeInputArgs('the secret')),
    ).rejects.toThrowError(Tripwire);
  });

  it('reads selectors by index, not through their own iterators', async () => {
    // #given — spreading these selectors produces ['output'] and []
    const phases = answeringList<PolicyPhase>(['input'], ['output']);
    const channels = answeringList<OutputChannel>(['answer'], []);
    const engine = new PolicyEngine({
      policies: [denySecret({ phases, channels })],
    });
    const gate = createContentPolicyGate({
      policies: [denySecret({ phases, channels })],
    });
    // #when / #then
    await expect(
      engine.processInput(makeInputArgs('the secret')),
    ).rejects.toThrowError(Tripwire);
    await expect(gate({ text: 'the secret' })).resolves.toEqual({
      allowed: false,
      outcome: 'denied',
    });
  });
});

const EVALUATOR_FIELDS = 'name, phases, channels, holdBackChars, evaluate';

describe('policy engine unknown fields', () => {
  it('refuses a misspelled PolicyEngine option, which would drop zero-leak buffering', () => {
    // #when / #then
    expect(
      () =>
        new PolicyEngine({
          policies: [denySecret()],
          holdback: true,
        } as PolicyEngineOptions),
    ).toThrow(
      new TypeError(
        'PolicyEngine: options has unknown field "holdback" (valid fields: policies, audit, resource, holdBack)',
      ),
    );
  });

  it('refuses a misspelled createContentPolicyGate option, which would drop its audit', () => {
    // #when / #then
    expect(() =>
      createContentPolicyGate({
        policies: [denySecret()],
        auditLogger: new AuditLogger(),
      } as ContentPolicyGateOptions),
    ).toThrow(
      new TypeError(
        'createContentPolicyGate: options has unknown field "auditLogger" (valid fields: policies, audit, resource)',
      ),
    );
  });

  it.each<[string, string, unknown]>([
    ['holdBackChars', 'holdbackChars', 5],
    ['channels', 'channel', ['reasoning']],
  ])('refuses a policy entry whose %s is misspelled as %s', (_field, key, value) => {
    // #given
    const policies = [{ ...denySecret(), [key]: value }];
    // #when / #then
    expect(() => new PolicyEngine({ policies })).toThrow(
      new TypeError(
        `PolicyEngine: policies entry 0 has unknown field ${JSON.stringify(key)} (valid fields: ${EVALUATOR_FIELDS})`,
      ),
    );
    expect(() => createContentPolicyGate({ policies })).toThrow(
      new TypeError(
        `createContentPolicyGate: policies entry 0 has unknown field ${JSON.stringify(key)} (valid fields: ${EVALUATOR_FIELDS})`,
      ),
    );
  });

  it('refuses a null policy entry', () => {
    // #when / #then
    expect(
      () =>
        new PolicyEngine({
          policies: [null] as unknown as PolicyEvaluator[],
        }),
    ).toThrow(
      new TypeError(
        'PolicyEngine: policies entry 0 must be an object (got null)',
      ),
    );
  });

  it('constructs with every declared option and evaluator field', () => {
    // #given — the Required types fail to compile while a declared field is
    // missing here
    const policy: Required<PolicyEvaluator> = {
      ...denySecret(),
      phases: ['input', 'output'],
      channels: ['answer'],
      holdBackChars: 5,
    };
    const engineOptions: Required<PolicyEngineOptions> = {
      policies: [policy],
      audit: new AuditLogger(),
      resource: 'engine',
      holdBack: true,
    };
    const gateOptions: Required<ContentPolicyGateOptions> = {
      policies: [policy],
      audit: new AuditLogger(),
      resource: 'gate',
    };
    // #when / #then
    expect(() => new PolicyEngine(engineOptions)).not.toThrow();
    expect(() => createContentPolicyGate(gateOptions)).not.toThrow();
  });
});

describe('policy engine values', () => {
  it.each<[string, unknown, string]>([
    ['an empty string', '', '""'],
    ['an empty list', [], 'object'],
    ['false', false, 'boolean'],
    ['null', null, 'null'],
    ['a negative number', -1, '-1'],
    ['NaN', Number.NaN, 'NaN'],
    ['a numeric string', '5', '"5"'],
  ])('refuses %s as a hand-built policy holdBackChars', (_label, holdBackChars, got) => {
    // #given — a window of 0 would release text before the policy saw it whole
    const policies = [denySecret({ holdBackChars: holdBackChars as number })];
    // #when / #then
    expect(() => new PolicyEngine({ policies, holdBack: true })).toThrow(
      new TypeError(
        `PolicyEngine: policy 'deny-secret' holdBackChars must be a number of at least 0, or Infinity (got ${got})`,
      ),
    );
  });

  it('holds back the window a valid hand-built hint asks for', async () => {
    // #given
    const engine = new PolicyEngine({
      policies: [denySecret({ phases: ['output'], holdBackChars: 5 })],
      holdBack: true,
    });
    // #when
    const released = await engine.processOutputStream(
      makeStreamArgs([textDelta('the se')], {}),
    );
    // #then — the five trailing chars stay held
    expect(
      (released as { payload?: { text?: string } } | null)?.payload?.text,
    ).toBe('t');
  });

  it('accepts Infinity as a hand-built policy holdBackChars', () => {
    // #when / #then
    expect(
      () =>
        new PolicyEngine({
          policies: [denySecret({ holdBackChars: Number.POSITIVE_INFINITY })],
          holdBack: true,
        }),
    ).not.toThrow();
  });

  it.each<[string, unknown, string]>([
    ['null, which would bypass the object-only fence', null, 'null'],
    ['a plain object without record', {}, 'object'],
    ['a string', 'audit', '"audit"'],
  ])('refuses %s as the PolicyEngine audit', (_label, audit, got) => {
    // #given
    const policy: PolicyEvaluator = {
      name: 'object-only',
      channels: ['object'],
      evaluate: () => ({ allowed: true }),
    };
    // #when / #then
    expect(
      () =>
        new PolicyEngine({
          policies: [policy],
          audit: audit as AuditLogger,
        }),
    ).toThrow(
      new TypeError(
        `PolicyEngine: audit must be an AuditLogger when provided (got ${got})`,
      ),
    );
  });

  it('refuses a content-policy gate with no policies, which allows every input', () => {
    // #when / #then
    expect(() => createContentPolicyGate({ policies: [] })).toThrow(
      new TypeError('createContentPolicyGate: policies must not be empty'),
    );
  });

  it('keeps a PolicyEngine with no policies, which a guarded agent runs for RBAC alone', () => {
    // #when / #then
    expect(() => new PolicyEngine({ policies: [] })).not.toThrow();
  });

  it('denies through a gate with one policy', async () => {
    // #given
    const gate = createContentPolicyGate({
      policies: [denyPatterns(['secret'])],
    });
    // #when / #then
    await expect(gate({ text: 'the secret' })).resolves.toEqual({
      allowed: false,
      outcome: 'denied',
    });
  });

  it.each<[string, unknown, string]>([
    ['Infinity, which never denies', Number.POSITIVE_INFINITY, 'Infinity'],
    ['a negative number', -1, '-1'],
    ['NaN', Number.NaN, 'NaN'],
    ['a numeric string', '10', '"10"'],
    ['null', null, 'null'],
  ])('refuses %s as maxTextLength maxChars', (_label, maxChars, got) => {
    // #when / #then
    expect(() => maxTextLength(maxChars as number)).toThrow(
      new TypeError(
        `maxTextLength: maxChars must be a finite number of at least 0 (got ${got})`,
      ),
    );
  });

  it('denies input over a finite maxChars', async () => {
    // #given
    const engine = new PolicyEngine({
      policies: [maxTextLength(10, { phases: ['input'] })],
    });
    // #when / #then
    await expect(
      engine.processInput(makeInputArgs('x'.repeat(50))),
    ).rejects.toThrowError(/max-text-length: text length 50 exceeds limit 10/);
  });

  it('refuses a misspelled maxTextLength option, which would narrow the phases it gates', () => {
    // #when / #then
    expect(() =>
      maxTextLength(10, { phase: ['input'] } as Parameters<
        typeof maxTextLength
      >[1]),
    ).toThrow(
      new TypeError(
        'maxTextLength: options has unknown field "phase" (valid fields: name, phases, channels)',
      ),
    );
  });

  it('constructs maxTextLength with every declared option', () => {
    // #given
    const options: Required<NonNullable<Parameters<typeof maxTextLength>[1]>> =
      { name: 'cap', phases: ['input'], channels: ['answer'] };
    // #when / #then
    expect(maxTextLength(10, options)).toMatchObject({ name: 'cap' });
  });

  it.each<[string, unknown, string]>([
    ['a negative number', -1, '-1'],
    ['an empty string', '', '""'],
    ['NaN', Number.NaN, 'NaN'],
  ])('refuses %s as denyPatterns holdBackChars', (_label, holdBackChars, got) => {
    // #when / #then
    expect(() =>
      denyPatterns(['secret'], { holdBackChars: holdBackChars as number }),
    ).toThrow(
      new TypeError(
        `denyPatterns: holdBackChars must be a number of at least 0, or Infinity (got ${got})`,
      ),
    );
  });

  it('keeps a valid denyPatterns holdBackChars override', () => {
    // #when / #then
    expect(denyPatterns([/secret/], { holdBackChars: 6 }).holdBackChars).toBe(
      6,
    );
  });
});

describe('denyPatterns entry validation', () => {
  const ownTest = /secret/;
  ownTest.test = () => false;
  const ownExec = /secret/;
  ownExec.exec = () => null;
  class NeverMatches extends RegExp {
    override test(): boolean {
      return false;
    }
  }

  it.each<[string, unknown, string]>([
    ['an object with its own test', { test: () => false }, 'object'],
    ['an object with its own exec', { exec: () => null }, 'object'],
    [
      'a RegExp-tagged object',
      {
        [Symbol.toStringTag]: 'RegExp',
        test: () => false,
        source: 'secret',
        flags: '',
      },
      'object',
    ],
    [
      'an object whose prototype is RegExp.prototype',
      Object.create(RegExp.prototype),
      'object',
    ],
    ['RegExp.prototype', RegExp.prototype, 'object'],
    ['a String object', new String('secret'), 'object'],
    ['a number', 42, 'number'],
    ['null', null, 'null'],
  ])('refuses %s as a pattern', (_label, entry, got) => {
    // #when / #then
    expect(() => denyPatterns(['blocked', entry as string])).toThrow(
      new TypeError(
        `denyPatterns: patterns entry 1 must be a string or a RegExp (got ${got})`,
      ),
    );
  });

  it.each<[string, unknown, string]>([
    ['an empty list', [], 'denyPatterns: patterns must not be empty'],
    ['a string', 'secret', 'denyPatterns: patterns must be an array'],
  ])('refuses %s as the pattern list', (_label, patterns, message) => {
    // #when / #then — a deny list with no entries never denies
    expect(() => denyPatterns(patterns as readonly string[])).toThrow(
      new TypeError(message),
    );
  });

  it.each<[string, () => RegExp]>([
    ['a RegExp with its own test', () => ownTest],
    ['a RegExp with its own exec', () => ownExec],
    ['a RegExp subclass overriding test', () => new NeverMatches('secret')],
  ])('matches %s by the RegExp it copied', (_label, pattern) => {
    // #given
    const policy = denyPatterns([pattern()]);
    // #when / #then
    expect(
      policy.evaluate({
        phase: 'input',
        channel: 'answer',
        messages: [],
        text: 'the secret',
      }),
    ).toMatchObject({ allowed: false });
  });

  it('keeps matching when test is assigned to the caller RegExp after construction', () => {
    // #given
    const pattern = /secret/;
    const policy = denyPatterns([pattern]);
    pattern.test = () => false;
    // #when / #then
    expect(
      policy.evaluate({
        phase: 'input',
        channel: 'answer',
        messages: [],
        text: 'the secret',
      }),
    ).toMatchObject({ allowed: false });
  });

  it('matches the patterns it read by index, not the list its own map answers', () => {
    // #given — own map answers with an entry whose test never matches
    const patterns = answeringList(['secret'], [{ test: () => false }]);
    const policy = denyPatterns(patterns);
    // #when / #then
    expect(
      policy.evaluate({
        phase: 'input',
        channel: 'answer',
        messages: [],
        text: 'the secret',
      }),
    ).toMatchObject({ allowed: false });
  });
});

describe('PolicyEngine object-channel result-phase fence (D1)', () => {
  it('fails closed when a policy requires an object the invocation never exposes', async () => {
    // #given — zero result-phase coverage: OutputResult has no object field,
    // and this policy never sees the answer channel either
    const audit = new AuditLogger();
    const engine = new PolicyEngine({
      policies: [denyPatterns(['x'], { channels: ['object'] })],
      audit,
    });

    // #when / #then — the result gate aborts instead of logging and allowing
    await expect(
      engine.processOutputResult(makeOutputArgs('clean')),
    ).rejects.toThrowError(/require the 'object' output channel/);

    expect(audit.events()).toHaveLength(1);
    expect(audit.events()[0]).toMatchObject({
      decision: 'error',
      reason: 'required object output channel was not observable',
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

  it('accepts a result after the processor actually evaluated an object chunk', async () => {
    const audit = new AuditLogger();
    const engine = new PolicyEngine({
      policies: [denyPatterns(['x'], { channels: ['object'] })],
      audit,
    });
    const state: Record<string, unknown> = {};
    const part = objectResult({ answer: 'clean' });

    await engine.processOutputStream(makeStreamArgs([part], state));
    const resultArgs = makeOutputArgs('clean');
    resultArgs.state = state;

    await expect(engine.processOutputResult(resultArgs)).resolves.toEqual(
      resultArgs.messages,
    );
    expect(audit.events().some((event) => event.decision === 'error')).toBe(
      false,
    );
  });

  it('exposes a frozen list of object-only policies', () => {
    const engine = new PolicyEngine({
      policies: [
        denyPatterns(['x'], { channels: ['object'] }),
        denyPatterns(['y'], { channels: ['answer', 'object'] }),
        maxTextLength(10),
      ],
      audit: new AuditLogger(),
    });

    expect(engine.objectOnlyPolicyNames).toEqual(['deny-patterns']);
    expect(Object.isFrozen(engine.objectOnlyPolicyNames)).toBe(true);
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

  it('bounds terminal audit metadata by policies and channels, not chunk count', async () => {
    const engine = new PolicyEngine({
      policies: [denyPatterns(['blocked'], { phases: ['output'] })],
    });
    const state: Record<string, unknown> = {};

    for (let index = 0; index < 100; index += 1) {
      const part = textDelta('clean');
      await engine.processOutputStream(makeStreamArgs([part], state));
    }

    expect(state['breakwater.streamEvaluatedPolicies']).toEqual({
      names: ['deny-patterns'],
      channels: ['answer'],
    });
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
      reason: 'policy evaluation failed',
      detail: { policy: 'crashy' },
    });
    expect(JSON.stringify(audit.events())).not.toContain(
      'evaluator internal failure',
    );
  });

  it('aborts mid-stream with an error event for a policy that returns no decision', async () => {
    // #given
    const audit = new AuditLogger();
    const engine = new PolicyEngine({
      policies: [
        {
          name: 'undecided',
          phases: ['output'],
          evaluate: () => undefined as never,
        },
      ],
      audit,
    });

    // #when / #then
    await expect(
      engine.processOutputStream(makeStreamArgs([textDelta('anything')])),
    ).rejects.toEqual(new Tripwire('policy evaluation failed'));
    expect(audit.events()).toMatchObject([
      { decision: 'error', reason: 'policy evaluation failed' },
    ]);
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

  it.each<[string, 'text-delta' | 'reasoning-delta', unknown]>([
    ['an undefined answer delta', 'text-delta', undefined],
    ['an answer delta list', 'text-delta', ['the SECRET']],
    ['an answer delta object', 'text-delta', { value: 'the SECRET' }],
    ['a numeric answer delta', 'text-delta', 42],
    ['a reasoning delta list', 'reasoning-delta', ['the SECRET']],
  ])('aborts on %s, which no policy could read, under hold-back off and on', async (_label, type, text) => {
    for (const holdBack of [false, true]) {
      // #given
      const audit = new AuditLogger();
      const engine = new PolicyEngine({
        policies: [denyPatterns(['SECRET'], { phases: ['output'] })],
        audit,
        holdBack,
      });
      const malformed = {
        runId: 'run',
        from: ChunkFrom.AGENT,
        type,
        payload: { id: 'out', text },
      } as unknown as ChunkType;

      // #when / #then — the chunk is never forwarded
      await expect(
        engine.processOutputStream(makeStreamArgs([malformed])),
      ).rejects.toThrow(new Tripwire('output text is not a string'));
      expect(audit.events()).toMatchObject([
        {
          action: 'agent.output.policy',
          decision: 'error',
          reason: 'output text is not a string',
          detail: {
            channel: type === 'text-delta' ? 'answer' : 'reasoning',
          },
        },
      ]);
    }
  });

  it('still forwards a string delta that passes', async () => {
    // #given
    const engine = new PolicyEngine({
      policies: [denyPatterns(['SECRET'], { phases: ['output'] })],
    });
    const chunk = textDelta('the plan');

    // #when / #then
    await expect(
      engine.processOutputStream(makeStreamArgs([chunk])),
    ).resolves.toBe(chunk);
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

  it('forwards the same canonical object snapshot that policy inspected', async () => {
    const engine = new PolicyEngine({ policies: [] });
    const transformed = Object.defineProperty({ answer: 'ok' }, 'secret', {
      value: 'hidden',
      enumerable: false,
    });
    const part = objectResult(transformed);

    const result = await engine.processOutputStream(makeStreamArgs([part]));

    expect(result).toMatchObject({
      type: 'object-result',
      object: { answer: 'ok' },
    });
    expect((result as { object?: unknown }).object).not.toBe(transformed);
    expect((result as { object?: unknown }).object).not.toHaveProperty(
      'secret',
    );
  });

  it('aborts before forwarding a structured chunk that is not JSON data', async () => {
    const audit = new AuditLogger();
    const engine = new PolicyEngine({ policies: [], audit });
    const part = objectResult({
      answer: 'ok',
      toJSON: () => ({ answer: 'ok' }),
    });

    await expect(
      engine.processOutputStream(makeStreamArgs([part])),
    ).rejects.toThrowError(/structured object is not JSON data/);
    expect(audit.events()).toHaveLength(1);
    expect(audit.events()[0]).toMatchObject({
      decision: 'error',
      reason: 'structured object is not JSON data',
      detail: { channel: 'object' },
    });
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
    ).resolves.toStrictEqual(partial);
    await expect(
      engine.processOutputStream(makeStreamArgs([partial, final], state)),
    ).resolves.toStrictEqual(final);
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

  it('aborts on a step whose reasoningText is present but not a string', async () => {
    // #given
    const audit = new AuditLogger();
    const engine = new PolicyEngine({
      policies: [denyPatterns(['secret'], { phases: ['output'] })],
      audit,
    });
    const args = makeOutputArgs(
      'clean answer',
      [],
      [
        reasoningStep('fine'),
        reasoningStep(['the secret plan'] as unknown as string),
      ],
    );

    // #when / #then
    await expect(engine.processOutputResult(args)).rejects.toThrow(
      new Tripwire('output text is not a string'),
    );
    expect(audit.events().at(-1)).toMatchObject({
      decision: 'error',
      reason: 'output text is not a string',
      detail: { channel: 'reasoning' },
    });
  });

  it('skips a step with no reasoningText', async () => {
    // #given
    const engine = new PolicyEngine({
      policies: [denyPatterns(['secret'], { phases: ['output'] })],
    });
    const args = makeOutputArgs(
      'clean answer',
      [],
      [{} as OutputResult['steps'][number], reasoningStep('fine')],
    );

    // #when / #then
    await expect(engine.processOutputResult(args)).resolves.toBe(args.messages);
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
    ).resolves.toStrictEqual(final);
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

  it("reads each tool call's name and input, then each result's name and output, in prompt order", () => {
    // #given — the format-2 shape Mastra builds from a replayed tool call and
    // its tool result
    const toolHistory: MastraDBMessage = {
      id: `msg-${++messageSeq}`,
      role: 'assistant',
      createdAt: new Date(),
      content: {
        format: 2,
        parts: [
          {
            type: 'tool-invocation',
            toolInvocation: {
              toolCallId: 'c1',
              toolName: 'lookup',
              args: { note: 'argument text' },
              state: 'result',
              result: 'result text',
            },
          },
          {
            type: 'tool-invocation',
            toolInvocation: {
              toolCallId: 'c2',
              toolName: 'lookup',
              args: { q: 'second' },
              state: 'result',
              result: { v: 'json result' },
            },
          },
        ],
      },
    };

    // #when / #then — a string stays as written; other values as JSON
    expect(extractMessageText([toolHistory, makeMessage('hi')])).toBe(
      'lookup\n{"note":"argument text"}\nlookup\n{"q":"second"}\nlookup\nresult text\nlookup\n{"v":"json result"}\nhi',
    );
  });

  it('reads the input of a tool call that has no result yet', () => {
    // #given
    const pendingCall: MastraDBMessage = {
      id: `msg-${++messageSeq}`,
      role: 'assistant',
      createdAt: new Date(),
      content: {
        format: 2,
        parts: [
          {
            type: 'tool-invocation',
            toolInvocation: {
              toolCallId: 'c1',
              toolName: 'lookup',
              args: { note: 'pending' },
              state: 'call',
            },
          },
        ],
      },
    };

    // #when / #then
    expect(extractMessageText([pendingCall])).toBe(
      'lookup\n{"note":"pending"}',
    );
  });

  function toolResultMessage(
    result: unknown,
    providerMetadata?: Record<string, unknown>,
  ): MastraDBMessage {
    return {
      id: `msg-${++messageSeq}`,
      role: 'assistant',
      createdAt: new Date(),
      content: {
        format: 2,
        parts: [
          {
            type: 'tool-invocation',
            toolInvocation: {
              toolCallId: 'call-id-text',
              toolName: 'lookup',
              args: {},
              state: 'result',
              result,
            },
            ...(providerMetadata !== undefined ? { providerMetadata } : {}),
          },
        ],
      },
    } as MastraDBMessage;
  }

  it("reads a stored model output in place of the raw result it replaces in the model's prompt", () => {
    // #given — a tool whose model output leaves out what its raw result holds
    const message = toolResultMessage('raw result for john.doe@example.com', {
      mastra: { modelOutput: { type: 'text', value: 'summary for the model' } },
    });

    // #when
    const text = extractMessageText([message]);

    // #then
    expect(text).toBe('lookup\n{}\nlookup\nsummary for the model');
  });

  it('leaves out tool-call ids and provider metadata while reading text file data', () => {
    // #given
    const message = toolResultMessage('result text', {
      openai: { itemId: 'provider-option-text' },
    });
    const file: MastraDBMessage = {
      id: `msg-${++messageSeq}`,
      role: 'user',
      createdAt: new Date(),
      content: {
        format: 2,
        parts: [
          { type: 'file', mimeType: 'text/plain', data: 'ZmlsZS1kYXRh' },
          { type: 'text', text: 'hi' },
        ],
      },
    };

    // #when
    const text = extractMessageText([message, file]);

    // #then
    expect(text).toBe(
      'lookup\n{}\nlookup\nresult text\ntext/plain\nfile-data\nhi',
    );
  });

  const UNCLASSIFIED: Array<[string, MastraDBMessage]> = [
    [
      'a part type',
      {
        ...makeMessage('hi'),
        content: {
          format: 2,
          parts: [{ type: 'mystery', text: 'hi' }],
        },
      } as unknown as MastraDBMessage,
    ],
    [
      'a tool-invocation state',
      {
        ...makeMessage('hi', 'assistant'),
        content: {
          format: 2,
          parts: [
            {
              type: 'tool-invocation',
              toolInvocation: {
                toolCallId: 'c1',
                toolName: 'lookup',
                args: {},
                state: 'mystery',
              },
            },
          ],
        },
      } as unknown as MastraDBMessage,
    ],
    [
      'a legacy tool-invocation state',
      {
        ...makeMessage('hi', 'assistant'),
        content: {
          format: 2,
          parts: [{ type: 'text', text: 'hi' }],
          toolInvocations: [
            {
              toolCallId: 'c1',
              toolName: 'lookup',
              args: {},
              state: 'mystery',
            },
          ],
        },
      } as unknown as MastraDBMessage,
    ],
    [
      'a role',
      { ...makeMessage('hi'), role: 'developer' } as unknown as MastraDBMessage,
    ],
    ['a system role', makeMessage('hi', 'system')],
    [
      'parts that are not a list',
      {
        ...makeMessage('hi'),
        content: { format: 2, parts: 'hi' },
      } as unknown as MastraDBMessage,
    ],
    [
      'a stored model output type',
      toolResultMessage('ok', {
        mastra: { modelOutput: { type: 'mystery', value: 'hi' } },
      }),
    ],
    [
      'a stored model output content item',
      toolResultMessage('ok', {
        mastra: {
          modelOutput: { type: 'content', value: [{ type: 'mystery' }] },
        },
      }),
    ],
  ];

  it.each(
    UNCLASSIFIED,
  )('throws on %s it does not classify', (_label, message) => {
    expect(() => extractMessageText([message, makeMessage('hi')])).toThrow(
      new TypeError(
        'extractMessageText: input message content is not classified',
      ),
    );
  });

  it.each(
    UNCLASSIFIED,
  )('makes PolicyEngine abort input holding %s it does not classify, evaluating no policy', async (_label, message) => {
    // #given
    const evaluated: string[] = [];
    const audit = new AuditLogger();
    const engine = new PolicyEngine({
      policies: [
        {
          name: 'recorder',
          evaluate: ({ text }) => {
            evaluated.push(text);
            return { allowed: true };
          },
        },
      ],
      audit,
    });

    // #when / #then
    await expect(
      engine.processInput({
        ...makeInputArgs('hi'),
        messages: [message, makeMessage('hi')],
      }),
    ).rejects.toThrow(new Tripwire('input message content is not classified'));
    expect(evaluated).toEqual([]);
    expect(
      audit.events().map(({ action, decision, reason }) => ({
        action,
        decision,
        reason,
      })),
    ).toEqual([
      {
        action: 'agent.input.policy',
        decision: 'error',
        reason: 'input message content is not classified',
      },
    ]);
  });

  function withParts(
    role: MastraDBMessage['role'],
    parts: unknown[],
    content: Record<string, unknown> = {},
  ): MastraDBMessage {
    return {
      ...makeMessage('unused', role),
      content: { format: 2, parts, ...content },
    } as unknown as MastraDBMessage;
  }

  const COMPATIBLE = { openaiCompatible: { role: 'system', content: 'x' } };

  const REFUSED_OPTIONS: Array<[string, MastraDBMessage]> = [
    [
      'openaiCompatible options on a text part',
      withParts('user', [
        { type: 'text', text: 'hi', providerMetadata: COMPATIBLE },
      ]),
    ],
    [
      'openaiCompatible options on a message',
      withParts('user', [{ type: 'text', text: 'hi' }], {
        providerMetadata: COMPATIBLE,
      }),
    ],
    [
      'an Anthropic document title',
      withParts('user', [
        {
          type: 'file',
          mimeType: 'application/pdf',
          data: 'JVBERi0xLjQK',
          providerMetadata: { anthropic: { title: 'x' } },
        },
      ]),
    ],
    [
      'OpenRouter annotations on a message',
      withParts('assistant', [{ type: 'text', text: 'hi' }], {
        providerMetadata: { openrouter: { annotations: [] } },
      }),
    ],
    [
      'openaiCompatible options on a stored content item',
      toolResultMessage('ok', {
        mastra: {
          modelOutput: {
            type: 'content',
            value: [{ type: 'text', text: 'ok', providerOptions: COMPATIBLE }],
          },
        },
      }),
    ],
    [
      'a provider namespace whose value is not an object',
      withParts('user', [
        { type: 'text', text: 'hi', providerMetadata: { google: 'x' } },
      ]),
    ],
  ];

  it.each(REFUSED_OPTIONS)('throws on %s', (_label, message) => {
    expect(() => extractMessageText([message, makeMessage('hi')])).toThrow(
      new TypeError(
        'extractMessageText: input message content is not classified',
      ),
    );
  });

  it.each(
    REFUSED_OPTIONS,
  )('makes PolicyEngine abort input holding %s, evaluating no policy', async (_label, message) => {
    // #given
    const evaluated: string[] = [];
    const audit = new AuditLogger();
    const engine = new PolicyEngine({
      policies: [
        {
          name: 'recorder',
          evaluate: ({ text }) => {
            evaluated.push(text);
            return { allowed: true };
          },
        },
      ],
      audit,
    });

    // #when / #then
    await expect(
      engine.processInput({
        ...makeInputArgs('hi'),
        messages: [message, makeMessage('hi')],
      }),
    ).rejects.toThrow(new Tripwire('input message content is not classified'));
    expect(evaluated).toEqual([]);
    expect(audit.events().map(({ decision }) => decision)).toEqual(['error']);
  });

  it('reads the text of replayed Anthropic citations and OpenRouter reasoning details, not their signatures, ids or encrypted values', () => {
    // #given — the provider metadata Anthropic and OpenRouter responses store
    const message = withParts('assistant', [
      {
        type: 'text',
        text: 'answer',
        providerMetadata: {
          anthropic: {
            citations: [
              {
                type: 'web_search_result_location',
                cited_text: 'cited passage',
                url: 'https://example.invalid/page',
                title: 'page title',
                encrypted_index: 'ENCRYPTEDINDEX',
              },
            ],
          },
        },
      },
      {
        type: 'reasoning',
        reasoning: 'thinking',
        details: [],
        providerMetadata: {
          openrouter: {
            reasoning_details: [
              {
                type: 'reasoning.text',
                text: 'detail text',
                signature: 'SIGNATURE',
              },
              { type: 'reasoning.summary', summary: 'detail summary' },
              {
                type: 'reasoning.encrypted',
                data: 'ENCRYPTEDDATA',
                id: 'DETAILID',
              },
            ],
          },
        },
      },
    ]);

    // #when
    const text = extractMessageText([message, makeMessage('hi')]);

    // #then
    for (const read of [
      'cited passage',
      'https://example.invalid/page',
      'page title',
      'detail text',
      'detail summary',
    ]) {
      expect(text).toContain(read);
    }
    for (const unread of [
      'ENCRYPTEDINDEX',
      'SIGNATURE',
      'ENCRYPTEDDATA',
      'DETAILID',
    ]) {
      expect(text).not.toContain(unread);
    }
  });

  it('reads every stored model output, on any role and state, whether or not a tool result receives it', () => {
    // #given — outputs no tool result in these messages receives
    const userCarrier = withParts('user', [
      { type: 'text', text: 'hi' },
      {
        type: 'tool-invocation',
        toolInvocation: {
          toolCallId: 'elsewhere',
          toolName: 'lookup',
          args: {},
          state: 'result',
          result: 'ok',
        },
        providerMetadata: {
          mastra: { modelOutput: { type: 'text', value: 'user-held output' } },
        },
      },
    ]);
    const pending = withParts('assistant', [
      {
        type: 'tool-invocation',
        toolInvocation: {
          toolCallId: 'pending',
          toolName: 'lookup',
          args: {},
          state: 'call',
        },
        providerMetadata: {
          mastra: { modelOutput: { type: 'text', value: 'pending output' } },
        },
      },
    ]);

    // #when
    const text = extractMessageText([userCarrier, pending]);

    // #then
    expect(text).toContain('user-held output');
    expect(text).toContain('pending output');
  });

  it('reads every field of a stored content item except its base64 data', () => {
    // #given
    const message = toolResultMessage('ok', {
      mastra: {
        modelOutput: {
          type: 'content',
          value: [
            { type: 'image-url', url: 'https://example.invalid/image' },
            { type: 'file-id', fileId: 'file-provider-id' },
            {
              type: 'image-data',
              data: 'BASE64DATA',
              mediaType: 'image/png',
              providerOptions: { host: { note: 'item note' } },
            },
          ],
        },
      },
    });

    // #when
    const text = extractMessageText([message]);

    // #then
    for (const read of [
      'https://example.invalid/image',
      'file-provider-id',
      'image/png',
      'item note',
    ]) {
      expect(text).toContain(read);
    }
    expect(text).not.toContain('BASE64DATA');
  });

  it('throws the classification TypeError with the conversion error as its cause', () => {
    // #given — Mastra's conversion throws on a text part whose text is a number
    const message = withParts('user', [{ type: 'text', text: 42 }]);

    // #when
    let thrown: unknown;
    try {
      extractMessageText([message]);
    } catch (error) {
      thrown = error;
    }

    // #then
    expect(thrown).toEqual(
      new TypeError(
        'extractMessageText: input message content is not classified',
      ),
    );
    expect((thrown as Error).cause).toBeInstanceOf(Error);
  });

  it.each<[string, MastraDBMessage]>([
    [
      'a text part whose text is a number',
      withParts('user', [{ type: 'text', text: 42 }]),
    ],
    [
      'a value structuredClone cannot copy',
      withParts('user', [
        {
          type: 'text',
          text: 'hi',
          providerMetadata: { host: { callback: () => 'hi' } },
        },
      ]),
    ],
  ])('makes PolicyEngine abort, not throw, on %s, with one error event', async (_label, message) => {
    // #given
    const audit = new AuditLogger();
    const engine = new PolicyEngine({ policies: [denySecret()], audit });

    // #when / #then
    await expect(
      engine.processInput({ ...makeInputArgs('hi'), messages: [message] }),
    ).rejects.toThrow(new Tripwire('input message content is not classified'));
    expect(
      audit.events().map(({ action, decision, reason }) => ({
        action,
        decision,
        reason,
      })),
    ).toEqual([
      {
        action: 'agent.input.policy',
        decision: 'error',
        reason: 'input message content is not classified',
      },
    ]);
  });

  it('evaluates the call messages and not the history memory adds to the message list', async () => {
    // #given
    const seen: { text: string; ids: string[] }[] = [];
    const engine = new PolicyEngine({
      policies: [
        {
          name: 'recorder',
          evaluate: ({ text, messages }) => {
            seen.push({ text, ids: messages.map(({ id }) => id) });
            return { allowed: true };
          },
        },
      ],
    });
    const remembered = makeMessage('remembered history');
    const caller = makeMessage('caller input');
    const messageList = new MessageList();
    messageList.add(remembered, 'memory');
    messageList.add(caller, 'input');

    // #when
    await engine.processInput({
      ...makeInputArgs('unused'),
      messages: messageList.get.all.db(),
      messageList,
    });

    // #then
    expect(seen).toEqual([{ text: 'caller input', ids: [caller.id] }]);
  });
});

// Opt-in perf evidence for hold-back cost (roadmap §13: "measure hold-back
// memory and latency under large streams"). Skipped unless
// BREAKWATER_PERF=1 so CI never carries timing variance; run with:
//   BREAKWATER_PERF=1 pnpm --filter @proofoftech/breakwater exec vitest run src/policy-engine/policy-engine.test.ts -t 'hold-back cost'
describe('PolicyEngine hold-back cost (opt-in perf evidence)', () => {
  const textOf = (chunk: ChunkType | null | undefined): string => {
    const text = (chunk as { payload?: { text?: unknown } } | null | undefined)
      ?.payload?.text;
    return typeof text === 'string' ? text : '';
  };

  it.skipIf(!process.env.BREAKWATER_PERF)(
    'measures latency and peak buffering on a 4 MB stream',
    async () => {
      // #given — a bounded string-pattern window (18-char pattern → 17-char
      // hold window) and 4 MB of clean text in 2 KB deltas
      const TOTAL_CHARS = 4 * 1024 * 1024;
      const CHUNK = 'x'.repeat(2048);
      const engine = new PolicyEngine({
        policies: [
          denyPatterns(['needle-not-present'], { phases: ['output'] }),
        ],
        holdBack: true,
      });
      const state: Record<string, unknown> = {};
      let emitted = 0;
      let peakPending = 0;
      const pendingOf = () => {
        const hold = state['breakwater.holdBack'] as
          | { answer?: { pending: string } }
          | undefined;
        return hold?.answer?.pending.length ?? 0;
      };

      // #when
      const started = performance.now();
      for (let sent = 0; sent < TOTAL_CHARS; sent += CHUNK.length) {
        const part = await engine.processOutputStream(
          makeStreamArgs([textDelta(CHUNK)], state),
        );
        emitted += textOf(part).length;
        peakPending = Math.max(peakPending, pendingOf());
      }
      const elapsedMs = performance.now() - started;

      // #then — bounded window held (pattern-bound string window + one
      // in-flight chunk), everything eventually released in aggregate, and
      // throughput stays far above interactive stream rates. Bounds are
      // generous regression fences, not benchmarks; the reported numbers are
      // recorded in docs/policy-engine-design.md.
      expect(peakPending).toBeLessThanOrEqual(
        'needle-not-present'.length - 1 + CHUNK.length,
      );
      const hold = state['breakwater.holdBack'] as {
        answer?: { pending: string };
      };
      expect(emitted + (hold.answer?.pending.length ?? 0)).toBe(TOTAL_CHARS);
      expect(elapsedMs).toBeLessThan(10_000);
      console.log(
        `hold-back perf: ${TOTAL_CHARS / 1024 / 1024} MB in ${Math.round(elapsedMs)} ms — ${Math.round(TOTAL_CHARS / 1024 / (elapsedMs / 1000))} KB/s, peak pending ${peakPending} chars`,
      );
    },
  );

  it.skipIf(!process.env.BREAKWATER_PERF)(
    'measures the RegExp (Infinity-window) buffer-all trade-off on the same stream',
    async () => {
      // #given — any RegExp forces an unbounded hold window: the entire
      // stream stays pending until finish. This MEASURES that documented
      // trade-off rather than asserting a bound that cannot hold.
      const TOTAL_CHARS = 1024 * 1024;
      const CHUNK = 'y'.repeat(2048);
      const engine = new PolicyEngine({
        policies: [denyPatterns([/z-\d+/], { phases: ['output'] })],
        holdBack: true,
      });
      const state: Record<string, unknown> = {};
      let emitted = 0;
      let peakPending = 0;
      const pendingOf = () => {
        const hold = state['breakwater.holdBack'] as
          | { answer?: { pending: string } }
          | undefined;
        return hold?.answer?.pending.length ?? 0;
      };

      // #when
      for (let sent = 0; sent < TOTAL_CHARS; sent += CHUNK.length) {
        const part = await engine.processOutputStream(
          makeStreamArgs([textDelta(CHUNK)], state),
        );
        emitted += textOf(part).length;
        peakPending = Math.max(peakPending, pendingOf());
      }

      // #then — nothing released mid-stream, everything buffered
      expect(emitted).toBe(0);
      expect(peakPending).toBe(TOTAL_CHARS);
    },
  );
});
