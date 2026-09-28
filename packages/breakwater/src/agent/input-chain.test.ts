// SPDX-License-Identifier: Apache-2.0

import { Agent, TripWire } from '@mastra/core/agent';
import { createDurableAgent } from '@mastra/core/agent/durable';
import {
  type MastraDBMessage,
  MessageList,
} from '@mastra/core/agent/message-list';
import type { MastraModelConfig } from '@mastra/core/llm';
import { Mastra } from '@mastra/core/mastra';
import { MockMemory } from '@mastra/core/memory';
import {
  BaseProcessor,
  type ProcessInputArgs,
  type ProcessorViolation,
  UnicodeNormalizer,
} from '@mastra/core/processors';
import { RequestContext } from '@mastra/core/request-context';
import { InMemoryStore } from '@mastra/core/storage';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { AuditLogger } from '../audit/index.js';
import {
  classifierPolicy,
  denyPatterns,
  PolicyEngine,
  type PolicyEvaluator,
  piiSecrets,
} from '../policy-engine/index.js';
import {
  ACTOR_CONTEXT_KEY,
  type Actor,
  RBACMiddleware,
} from '../rbac/index.js';
import {
  createGuardedAgent,
  type GuardedAgentHandle,
  type GuardedInputProcessor,
} from './index.js';

// Rows on Mastra's standard agent loop and its public durable agent for the
// input chain's failures and refusals: what reaches the model, what the audit
// records, and what the thread keeps.

const LOOPS = ['generate', 'stream', 'durable'] as const;
type Loop = (typeof LOOPS)[number];
const STANDARD = ['generate', 'stream'] as const;

const MARK = 'MKREFUSEDINPUT';
const POLICY_FAILED = 'policy evaluation failed';
const PROCESSOR_FAILED = 'input processor failed';

const usage = { inputTokens: 1, outputTokens: 1, totalTokens: 2 };

// A model that records each prompt and answers.
function recordingModel(prompts: unknown[]): MastraModelConfig {
  return {
    specificationVersion: 'v2',
    provider: 'breakwater-test',
    modelId: 'recording',
    supportedUrls: {},
    doGenerate: async (options) => {
      prompts.push(options.prompt);
      return {
        content: [{ type: 'text', text: 'answered' }],
        finishReason: 'stop',
        usage,
        warnings: [],
      };
    },
    doStream: async (options) => {
      prompts.push(options.prompt);
      const parts = [
        { type: 'stream-start', warnings: [] },
        { type: 'text-start', id: 'answer' },
        { type: 'text-delta', id: 'answer', delta: 'answered' },
        { type: 'text-end', id: 'answer' },
        { type: 'finish', finishReason: 'stop', usage },
      ];
      return {
        stream: new ReadableStream({
          start(controller) {
            for (const part of parts) controller.enqueue(part as never);
            controller.close();
          },
        }),
      };
    },
  } as MastraModelConfig;
}

function actorContext(role: Actor['role'] = 'operator'): RequestContext {
  const context = new RequestContext();
  context.set(ACTOR_CONTEXT_KEY, { id: 'actor-1', role });
  return context;
}

interface Driven {
  generate(messages: never, options: never): Promise<unknown>;
  stream(messages: never, options: never): Promise<unknown>;
}

// The call's tripwire reason, or undefined when it answered.
async function drive(
  agent: Driven,
  loop: Loop,
  messages: unknown,
  options: Record<string, unknown>,
): Promise<string | undefined> {
  if (loop === 'generate') {
    const result = (await agent.generate(
      messages as never,
      options as never,
    )) as { tripwire?: { reason?: string } };
    return result.tripwire?.reason;
  }
  if (loop === 'stream') {
    const output = (await agent.stream(
      messages as never,
      options as never,
    )) as {
      fullStream: AsyncIterable<unknown>;
      tripwire: Promise<{ reason?: string } | undefined>;
    };
    for await (const _chunk of output.fullStream) {
      // drain
    }
    return (await output.tripwire)?.reason;
  }
  const durable = createDurableAgent({
    agent: agent as unknown as Agent,
    cache: false,
  });
  const output = await durable.stream(messages as never, options as never);
  let tripwire: string | undefined;
  for await (const chunk of output.fullStream as AsyncIterable<{
    type: string;
    payload?: { reason?: string };
  }>) {
    if (chunk.type === 'tripwire') tripwire = chunk.payload?.reason;
  }
  return tripwire;
}

// The call's tripwire reason, or the message of what it threw, so a row
// reports every loop's outcome.
async function outcomeOf(
  call: Promise<string | undefined>,
): Promise<{ tripwire: string | undefined; failure: string | undefined }> {
  try {
    return { tripwire: await call, failure: undefined };
  } catch (error) {
    return {
      tripwire: undefined,
      failure: error instanceof Error ? error.message : String(error),
    };
  }
}

function eventsOf(
  audit: AuditLogger,
  action: string,
  decision?: 'allowed' | 'denied' | 'error',
) {
  return audit
    .events()
    .filter(
      (event) =>
        event.action === action &&
        (decision === undefined || event.decision === decision),
    );
}

function guardedAgent(settings: {
  prompts: unknown[];
  audit: AuditLogger;
  policies?: readonly PolicyEvaluator[];
  processors?: readonly GuardedInputProcessor[];
  memory?: MockMemory;
  instructions?: string;
  model?: MastraModelConfig;
}): GuardedAgentHandle {
  return createGuardedAgent({
    id: 'writer',
    name: 'Writer',
    instructions: settings.instructions ?? 'Answer the request.',
    // A stubbed provider answers every request with an error, which a retry
    // would only repeat.
    ...(settings.model
      ? { model: settings.model, maxRetries: 0 }
      : { model: recordingModel(settings.prompts) }),
    allowedRoles: ['operator'],
    policies: settings.policies ?? [denyPatterns([MARK])],
    audit: settings.audit,
    maxSteps: 1,
    toolChoice: 'auto',
    ...(settings.processors
      ? { applicationInputProcessors: settings.processors }
      : {}),
    ...(settings.memory ? { memory: settings.memory } : {}),
  });
}

const markedInput = () => [`${MARK} payload`];

function app(
  processInput: GuardedInputProcessor['processInput'],
  id = 'app-check',
): GuardedInputProcessor {
  return { id, processInput };
}

const textOf = (message: MastraDBMessage): string =>
  message.content.parts
    .flatMap((part) => (part.type === 'text' ? [part.text] : []))
    .join(' ');

// The call's own messages among a processor's `messages`, and the rest.
function splitInput(args: ProcessInputArgs): {
  own: MastraDBMessage[];
  other: MastraDBMessage[];
} {
  const input = args.messageList.makeMessageSourceChecker().input;
  return {
    own: args.messages.filter(({ id }) => input.has(id)),
    other: args.messages.filter(({ id }) => !input.has(id)),
  };
}

const callerText = (args: ProcessInputArgs): string =>
  args.messageList.get.input.db().map(textOf).join(' ');

function dropInput(args: ProcessInputArgs): void {
  args.messageList.removeByIds(
    args.messageList.get.input.db().map(({ id }) => id),
  );
}

function replaceText(
  messages: MastraDBMessage[],
  from: string,
  to: string,
): MastraDBMessage[] {
  return messages.map((message) => ({
    ...message,
    content: {
      ...message.content,
      parts: message.content.parts.map((part) =>
        part.type === 'text'
          ? { ...part, text: part.text.replaceAll(from, to) }
          : part,
      ),
    },
  }));
}

function storedSystemMessage(id: string, text: string): MastraDBMessage {
  return {
    id,
    role: 'system',
    createdAt: new Date(),
    content: { format: 2, parts: [{ type: 'text', text }] },
  };
}

function userMessage(id: string, text: string): MastraDBMessage {
  return {
    id,
    role: 'user',
    createdAt: new Date(),
    content: { format: 2, parts: [{ type: 'text', text }] },
  };
}

// The instructions entry, whose content Mastra stores as a string.
const instructionsOf = (args: ProcessInputArgs) =>
  args.systemMessages[0] as {
    content: string;
    providerOptions?: unknown;
    experimental_providerMetadata?: unknown;
  };

// Each message of a recorded prompt as `role: text`.
function promptLines(prompt: unknown): string[] {
  return (
    prompt as {
      role: string;
      content: string | { type: string; text?: string }[];
    }[]
  ).map(
    ({ role, content }) =>
      `${role}: ${
        typeof content === 'string'
          ? content
          : content.map((part) => part.text ?? `<${part.type}>`).join('|')
      }`,
  );
}

describe('input evaluator failures on both agent loops', () => {
  const FAILING: ReadonlyArray<[string, () => PolicyEvaluator]> = [
    [
      'an input evaluator that throws',
      () => ({
        name: 'crashing',
        evaluate: () => {
          throw new Error('private evaluator failure');
        },
      }),
    ],
    [
      'classifierPolicy whose classifier rejects',
      () =>
        classifierPolicy({
          name: 'moderation',
          classify: async () => {
            throw new Error('413 payload too large');
          },
        }),
    ],
    [
      'classifierPolicy past its timeoutMs',
      () =>
        classifierPolicy({
          name: 'moderation',
          timeoutMs: 20,
          classify: () => new Promise(() => {}),
        }),
    ],
    [
      'an input evaluator that returns no decision',
      () => ({ name: 'undecided', evaluate: () => undefined as never }),
    ],
    [
      'classifierPolicy whose classifier resolves null',
      () =>
        classifierPolicy({
          name: 'moderation',
          classify: async () => null as never,
        }),
    ],
  ];

  it.each(
    FAILING,
  )('%s records one error event and stops the call before the model, on generate, stream and the durable loop', async (_label, failing) => {
    for (const loop of LOOPS) {
      // #given — the later policy would deny the input; it must not be needed
      const prompts: unknown[] = [];
      const audit = new AuditLogger();
      const agent = guardedAgent({
        prompts,
        audit,
        policies: [failing(), denyPatterns([MARK])],
      });

      // #when
      const tripwire = await drive(agent, loop, markedInput(), {
        requestContext: actorContext(),
      });

      // #then
      expect({ loop, tripwire, prompts }).toEqual({
        loop,
        tripwire: POLICY_FAILED,
        prompts: [],
      });
      expect(eventsOf(audit, 'agent.input.policy', 'error')).toHaveLength(1);
      expect(eventsOf(audit, 'agent.input.policy', 'denied')).toEqual([]);
      expect(JSON.stringify(audit.events())).not.toContain('private');
    }
  });
});

// A message stored on another thread of the same resource.
const otherThreadNote = (): MastraDBMessage => ({
  id: 'note-1',
  role: 'user',
  createdAt: new Date(Date.now() - 1000),
  threadId: 'profile-thread',
  resourceId: 'r1',
  content: { format: 2, parts: [{ type: 'text', text: 'Profile note.' }] },
});

function failCleanup(): void {
  throw new Error('private cleanup failure');
}

describe('application input processors on both agent loops', () => {
  const FAILING: ReadonlyArray<
    [string, () => GuardedInputProcessor, unknown?]
  > = [
    [
      'throws',
      () => ({
        id: 'app-check',
        processInput: ({ messages }) => {
          if (JSON.stringify(messages).includes(MARK)) {
            throw new Error('private parse failure');
          }
          return messages;
        },
      }),
    ],
    [
      'returns a MessageList other than the one it received',
      () => ({ id: 'app-check', processInput: () => new MessageList() }),
    ],
    [
      'returns messages without system messages',
      () => ({
        id: 'app-check',
        processInput: ({ messages }) => ({ messages }) as never,
      }),
    ],
    [
      'returns a message Mastra cannot add',
      () =>
        app(
          ({ messages }) =>
            messages.map((message) => ({
              role: message.role,
              content: textOf(message) || undefined,
            })) as never,
        ),
      [
        `${MARK} payload`,
        {
          role: 'user',
          content: [
            { type: 'file', data: 'aGVsbG8=', mediaType: 'text/plain' },
          ],
        },
      ],
    ],
    [
      'appends a system-role entry without content',
      () =>
        app(({ messages }) => [
          ...messages,
          { role: 'system', content: undefined } as never,
        ]),
    ],
    [
      'returns a pair with an undefined system message',
      () =>
        app(({ messages, systemMessages }) => ({
          messages,
          systemMessages: [...systemMessages, undefined as never],
        })),
    ],
    [
      'appends a message stored on another thread',
      () => app(({ messages }) => [...messages, otherThreadNote()]),
    ],
    [
      'returns an array with a null entry',
      () => app(({ messages }) => [...messages, null as never]),
    ],
    [
      'returns a pair with a null message',
      () =>
        app(({ messages, systemMessages }) => ({
          messages: [...messages, null as never],
          systemMessages,
        })),
    ],
    [
      'wraps its own abort in another error',
      () =>
        app((args) => {
          try {
            args.abort('app refused');
          } catch (cause) {
            throw new Error('private sanitizer failure', { cause });
          }
          return args.messages;
        }),
    ],
    [
      'throws from cleanup after its own abort',
      () =>
        app((args) => {
          try {
            return args.abort('app refused');
          } finally {
            failCleanup();
          }
        }),
    ],
    [
      'moves its input into a system message with a file part',
      () =>
        app((args) => {
          const { own, other } = splitInput(args);
          return {
            messages: other,
            systemMessages: [
              ...args.systemMessages,
              {
                role: 'system',
                content: [
                  { type: 'text', text: own.map(textOf).join(' ') },
                  { type: 'file', data: 'aGVsbG8=', mediaType: 'text/plain' },
                ],
              } as never,
            ],
          };
        }),
    ],
    [
      "moves its input into a system message in Mastra's stored format",
      () =>
        app((args) => {
          const { own, other } = splitInput(args);
          return {
            messages: other,
            systemMessages: [
              ...args.systemMessages,
              storedSystemMessage('app-system', own.map(textOf).join(' ')),
            ] as never,
          };
        }),
    ],
    [
      'throws after adding its input as a system message',
      () =>
        app((args) => {
          args.messageList.addSystem(callerText(args));
          dropInput(args);
          throw new Error('private processor failure');
        }),
    ],
    [
      'throws after adding its input as a response message',
      () =>
        app((args) => {
          args.messageList.add(
            { role: 'assistant', content: callerText(args) },
            'response',
          );
          dropInput(args);
          throw new Error('private processor failure');
        }),
    ],
  ];

  it.each(
    FAILING,
  )('stops the call with one error event and saves none of its input when a processor %s, before the model, on generate, stream and the durable loop', async (_label, processor, input) => {
    const outcomes: unknown[] = [];
    for (const loop of LOOPS) {
      // #given
      const memory = await threadMemory();
      const prompts: unknown[] = [];
      const audit = new AuditLogger();
      const agent = guardedAgent({
        prompts,
        audit,
        processors: [processor()],
        memory,
      });

      // #when
      const outcome = await outcomeOf(
        drive(agent, loop, input ?? markedInput(), {
          requestContext: actorContext(),
          memory: THREAD,
        }),
      );

      outcomes.push({
        loop,
        ...outcome,
        prompts: prompts.length,
        sent: JSON.stringify(prompts).includes(MARK),
        saved: await savedMarker(memory, loop),
        processorEvents: eventsOf(audit, 'agent.input.processor').map(
          ({ actor, resource, decision, reason, detail }) => ({
            actor,
            resource,
            decision,
            reason,
            processor: detail?.processor,
          }),
        ),
        policyEvents: eventsOf(audit, 'agent.input.policy').length,
        leaked: JSON.stringify(audit.events()).includes('private'),
      });
    }

    // #then
    expect(outcomes).toEqual(
      LOOPS.map((loop) => ({
        loop,
        tripwire: PROCESSOR_FAILED,
        failure: undefined,
        prompts: 0,
        sent: false,
        saved: false,
        processorEvents: [
          {
            actor: { id: 'actor-1', role: 'operator' },
            resource: 'agent:writer',
            decision: 'error',
            reason: PROCESSOR_FAILED,
            processor: 'app-check',
          },
        ],
        policyEvents: 0,
        leaked: false,
      })),
    );
  });

  const OWN_REASONS: ReadonlyArray<[string, () => GuardedInputProcessor]> = [
    [
      'throws its own TripWire',
      () =>
        app(() => {
          throw new TripWire('app refused');
        }),
    ],
    [
      'aborts after adding its input as a system message',
      () =>
        app((args) => {
          args.messageList.addSystem(callerText(args));
          return args.abort('app refused');
        }),
    ],
    [
      'aborts after adding its input as a response message',
      () =>
        app((args) => {
          args.messageList.add(
            { role: 'assistant', content: callerText(args) },
            'response',
          );
          return args.abort('app refused');
        }),
    ],
  ];

  it.each(
    OWN_REASONS,
  )('stops the call with its own reason and saves none of its input when a processor %s, before the model, on generate, stream and the durable loop', async (_label, processor) => {
    const outcomes: unknown[] = [];
    for (const loop of LOOPS) {
      // #given
      const memory = await threadMemory();
      const prompts: unknown[] = [];
      const audit = new AuditLogger();
      const agent = guardedAgent({
        prompts,
        audit,
        processors: [processor()],
        memory,
      });

      // #when
      const outcome = await outcomeOf(
        drive(agent, loop, markedInput(), {
          requestContext: actorContext(),
          memory: THREAD,
        }),
      );

      outcomes.push({
        loop,
        ...outcome,
        prompts: prompts.length,
        saved: await savedMarker(memory, loop),
        processorEvents: eventsOf(audit, 'agent.input.processor').length,
      });
    }

    // #then
    expect(outcomes).toEqual(
      LOOPS.map((loop) => ({
        loop,
        tripwire: 'app refused',
        failure: undefined,
        prompts: 0,
        saved: false,
        processorEvents: 0,
      })),
    );
  });

  class ViolationRecorder implements GuardedInputProcessor {
    readonly id = 'app-refuser';
    readonly #violations: string[];

    constructor(violations: string[]) {
      this.#violations = violations;
    }

    processInput(args: ProcessInputArgs): never {
      return args.abort('app refused');
    }

    onViolation(violation: ProcessorViolation): void {
      this.#violations.push(`${violation.processorId}: ${violation.message}`);
    }
  }

  it("calls a class-based processor's onViolation with its own abort on the durable loop", async () => {
    // #given
    const violations: string[] = [];
    const prompts: unknown[] = [];
    const agent = guardedAgent({
      prompts,
      audit: new AuditLogger(),
      processors: [new ViolationRecorder(violations)],
    });

    // #when
    const tripwire = await drive(agent, 'durable', markedInput(), {
      requestContext: actorContext(),
    });

    // #then
    expect({ tripwire, prompts, violations }).toEqual({
      tripwire: 'app refused',
      prompts: [],
      violations: ['app-refuser: app refused'],
    });
  });

  class MarkerRefuser implements GuardedInputProcessor {
    readonly id = 'app-class';
    readonly #marker: string;

    constructor(marker: string) {
      this.#marker = marker;
    }

    processInput(args: ProcessInputArgs) {
      if (JSON.stringify(args.messages).includes(this.#marker)) {
        args.abort('app refused a marked message');
      }
      return args.messages;
    }
  }

  it('keeps a class-based processor its private state, so its own abort stops the call, on generate, stream and the durable loop', async () => {
    for (const loop of LOOPS) {
      // #given
      const prompts: unknown[] = [];
      const audit = new AuditLogger();
      const agent = guardedAgent({
        prompts,
        audit,
        processors: [new MarkerRefuser(MARK)],
      });

      // #when
      const tripwire = await drive(agent, loop, markedInput(), {
        requestContext: actorContext(),
      });

      // #then
      expect({ loop, tripwire, prompts }).toEqual({
        loop,
        tripwire: 'app refused a marked message',
        prompts: [],
      });
      expect(eventsOf(audit, 'agent.input.processor')).toEqual([]);
    }
  });
});

describe('RBACMiddleware actor lookups on both agent loops', () => {
  // A raw agent: a guarded agent takes no custom actor lookup.
  function rawAgent(
    prompts: unknown[],
    audit: AuditLogger,
    getActor: () => Actor | undefined,
  ): Agent {
    return new Agent({
      id: 'raw',
      name: 'Raw',
      instructions: 'Answer the request.',
      model: recordingModel(prompts),
      inputProcessors: [
        new RBACMiddleware({ allowedRoles: ['operator'], audit, getActor }),
        new PolicyEngine({ policies: [denyPatterns([MARK])], audit }),
      ],
    });
  }

  const LOOKUPS: ReadonlyArray<
    [string, () => Actor | undefined, string, 'error' | 'denied']
  > = [
    [
      'a lookup that throws',
      () => {
        throw new Error('private token decode failure');
      },
      'actor lookup failed',
      'error',
    ],
    [
      'an actor whose id getter throws',
      () =>
        ({
          get id(): string {
            throw new Error('private lazy id');
          },
          role: 'operator',
        }) as Actor,
      'actor lookup failed',
      'error',
    ],
    [
      'an actor of an undeclared kind',
      () => ({ id: 'u', role: 'operator', kind: 'robot' }) as unknown as Actor,
      'principal kind is not declared',
      'denied',
    ],
  ];

  it.each(
    LOOKUPS,
  )('denies %s with one audit record before the model, on generate, stream and the durable loop', async (_label, getActor, reason, decision) => {
    for (const loop of LOOPS) {
      // #given
      const prompts: unknown[] = [];
      const audit = new AuditLogger();
      const agent = rawAgent(prompts, audit, getActor);

      // #when
      const tripwire = await drive(
        agent as unknown as Driven,
        loop,
        markedInput(),
        {
          requestContext: new RequestContext(),
        },
      );

      // #then
      expect({ loop, tripwire, prompts }).toEqual({
        loop,
        tripwire: reason,
        prompts: [],
      });
      expect(eventsOf(audit, 'agent.input.authorize')).toMatchObject([
        { actor: null, decision, reason },
      ]);
      expect(JSON.stringify(audit.events())).not.toContain('private');
    }
  });
});

const THREAD = { thread: 't1', resource: 'r1' } as const;
const WAIT_MS = 1000;

async function threadMemory(
  options: {
    title?: boolean;
    generateTitle?: boolean;
    workingMemoryTemplate?: string;
  } = {},
): Promise<MockMemory> {
  const memory = new MockMemory({
    storage: new InMemoryStore(),
    ...(options.generateTitle ? { options: { generateTitle: true } } : {}),
    ...(options.workingMemoryTemplate !== undefined
      ? {
          options: {
            workingMemory: {
              enabled: true,
              template: options.workingMemoryTemplate,
            },
          },
        }
      : {}),
  });
  if (options.title !== false) {
    const created = new Date(Date.now() - 400_000);
    await memory.saveThread({
      thread: {
        id: 't1',
        resourceId: 'r1',
        title: 'Existing thread',
        createdAt: created,
        updatedAt: created,
        metadata: {},
      },
    });
  }
  return memory;
}

async function storedText(memory: MockMemory): Promise<string> {
  const { messages } = await memory.recall({
    threadId: THREAD.thread,
    resourceId: THREAD.resource,
  });
  return JSON.stringify(messages);
}

// Mastra's durable finish saves the thread after its stream drains, so the
// thread is read until `marker` appears or the wait ends.
async function storedWithin(
  memory: MockMemory,
  marker: string,
): Promise<string> {
  const deadline = Date.now() + WAIT_MS;
  let text = await storedText(memory);
  while (!text.includes(marker) && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 25));
    text = await storedText(memory);
  }
  return text;
}

// Whether the thread holds the marker once the call has finished.
async function savedMarker(memory: MockMemory, loop: Loop): Promise<boolean> {
  const text =
    loop === 'durable'
      ? await storedWithin(memory, MARK)
      : await storedText(memory);
  return text.includes(MARK);
}

async function historyMemory(
  question = 'earlier question',
): Promise<MockMemory> {
  const memory = await threadMemory();
  const at = Date.now() - 300_000;
  await memory.saveMessages({
    messages: [
      {
        id: 'mem-u',
        role: 'user',
        threadId: 't1',
        resourceId: 'r1',
        createdAt: new Date(at),
        content: {
          format: 2,
          parts: [{ type: 'text', text: question }],
        },
      },
      {
        id: 'mem-a',
        role: 'assistant',
        threadId: 't1',
        resourceId: 'r1',
        createdAt: new Date(at + 10_000),
        content: {
          format: 2,
          parts: [{ type: 'text', text: 'earlier answer' }],
        },
      },
    ],
  });
  return memory;
}

async function threadTitle(memory: MockMemory): Promise<string> {
  const thread = await memory.getThreadById({ threadId: THREAD.thread });
  return thread?.title ?? '';
}

// Mastra's durable finish titles a thread after its stream drains, so the
// thread is read until it has a title or the wait ends.
async function titleWithin(memory: MockMemory): Promise<string> {
  const deadline = Date.now() + WAIT_MS;
  let title = await threadTitle(memory);
  while (title === '' && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 25));
    title = await threadTitle(memory);
  }
  return title;
}

// A context message and a memory message, both user messages carrying the
// marker, which Mastra's durable finish would title a thread from.
function addMarkedMessages(args: ProcessInputArgs): void {
  args.messageList.add(
    userMessage('app-context', `${MARK} context`),
    'context',
  );
  args.messageList.add(userMessage('app-memory', `${MARK} memory`), 'memory');
}

// Application input processors that add those messages, and the refusal that
// follows them.
const ADDED_THEN_REFUSED: ReadonlyArray<
  [string, () => readonly GuardedInputProcessor[]]
> = [
  [
    'the processor that added them throws',
    () => [
      app((args) => {
        addMarkedMessages(args);
        throw new Error('processor failure');
      }),
    ],
  ],
  [
    'the processor that added them aborts',
    () => [
      app((args) => {
        addMarkedMessages(args);
        return args.abort('app refused');
      }),
    ],
  ],
  [
    'a later processor throws',
    () => [
      app((args) => {
        addMarkedMessages(args);
        return args.messageList;
      }, 'app-adder'),
      app(() => {
        throw new Error('processor failure');
      }, 'app-thrower'),
    ],
  ],
  [
    'an input policy refuses them',
    () => [
      app((args) => {
        addMarkedMessages(args);
        return args.messageList;
      }),
    ],
  ],
];

describe('a call refused on the input chain saves none of its input', () => {
  interface Refusal {
    readonly messages: unknown;
    readonly role?: Actor['role'];
    readonly withoutRequestContext?: true;
    readonly policies?: readonly PolicyEvaluator[];
    readonly processors?: readonly GuardedInputProcessor[];
  }

  const refusingProcessor = (
    refuse: (args: ProcessInputArgs) => never,
  ): GuardedInputProcessor => ({
    id: 'app-refuser',
    processInput: (args) =>
      JSON.stringify(args.messages).includes(MARK)
        ? refuse(args)
        : args.messages,
  });

  const REFUSALS: ReadonlyArray<[string, Refusal]> = [
    ['a policy denial', { messages: markedInput() }],
    [
      'a refused provider option',
      {
        messages: [
          {
            role: 'user',
            content: [
              {
                type: 'text',
                text: `${MARK} hi`,
                providerOptions: {
                  openaiCompatible: { role: 'system', content: MARK },
                },
              },
            ],
          },
        ],
      },
    ],
    ['an RBAC denial', { messages: markedInput(), role: 'viewer' }],
    [
      'an RBAC denial for a missing actor',
      { messages: markedInput(), withoutRequestContext: true },
    ],
    [
      'an input evaluator that throws',
      {
        messages: markedInput(),
        policies: [
          {
            name: 'crashing',
            evaluate: () => {
              throw new Error('evaluator failure');
            },
          },
        ],
      },
    ],
    [
      "an application processor's own abort",
      {
        messages: markedInput(),
        processors: [refusingProcessor((args) => args.abort('app refused'))],
      },
    ],
    [
      'an application processor that throws',
      {
        messages: markedInput(),
        processors: [
          refusingProcessor(() => {
            throw new Error('processor failure');
          }),
        ],
      },
    ],
  ];

  it('saves an allowed durable call within the wait, which the refused rows rely on', async () => {
    // #given
    const memory = await threadMemory();
    const agent = guardedAgent({
      prompts: [],
      audit: new AuditLogger(),
      policies: [denyPatterns(['never-present'])],
      memory,
    });

    // #when
    const tripwire = await drive(agent, 'durable', [`${MARK} allowed`], {
      requestContext: actorContext(),
      memory: THREAD,
    });

    // #then
    expect(tripwire).toBeUndefined();
    expect(await storedWithin(memory, MARK)).toContain(MARK);
  });

  it.each(
    REFUSALS,
  )('keeps a durable call refused by %s out of the thread and out of the next generate and stream', async (_label, refusal) => {
    // #given
    const memory = await threadMemory();
    const refusedPrompts: unknown[] = [];
    const refusing = guardedAgent({
      prompts: refusedPrompts,
      audit: new AuditLogger(),
      memory,
      ...(refusal.policies ? { policies: refusal.policies } : {}),
      ...(refusal.processors ? { processors: refusal.processors } : {}),
    });

    // #when
    const tripwire = await drive(refusing, 'durable', refusal.messages, {
      ...(refusal.withoutRequestContext
        ? {}
        : { requestContext: actorContext(refusal.role) }),
      memory: THREAD,
    });

    // #then
    expect(tripwire).toBeDefined();
    expect(refusedPrompts).toEqual([]);
    expect(await storedWithin(memory, MARK)).not.toContain(MARK);
    for (const loop of STANDARD) {
      const prompts: unknown[] = [];
      const next = guardedAgent({
        prompts,
        audit: new AuditLogger(),
        policies: [denyPatterns(['never-present'])],
        memory,
      });
      expect(
        await drive(next, loop, 'hi', {
          requestContext: actorContext(),
          memory: THREAD,
        }),
      ).toBeUndefined();
      expect(prompts).toHaveLength(1);
      expect(JSON.stringify(prompts)).not.toContain(MARK);
    }
  });

  it('generates no thread title from a refused durable call', async () => {
    // #given — the thread has no title, and memory titles new threads
    const memory = await threadMemory({ title: false, generateTitle: true });
    const prompts: unknown[] = [];
    const agent = guardedAgent({ prompts, audit: new AuditLogger(), memory });

    // #when
    const tripwire = await drive(agent, 'durable', markedInput(), {
      requestContext: actorContext(),
      memory: THREAD,
    });
    await storedWithin(memory, MARK);

    // #then — no model call at all, so no title was generated from the input
    expect(tripwire).toMatch(/^deny-patterns: /);
    expect(prompts).toEqual([]);
    const thread = await memory.getThreadById({ threadId: THREAD.thread });
    expect(thread?.title ?? '').toBe('');
  });

  it.each(
    ADDED_THEN_REFUSED,
  )('titles no thread from, and stores none of, the context and memory messages processors added to a durable call when %s', async (_label, processors) => {
    // #given — the thread has no title, and memory titles new threads
    const memory = await threadMemory({ title: false, generateTitle: true });
    const prompts: unknown[] = [];
    const agent = guardedAgent({
      prompts,
      audit: new AuditLogger(),
      memory,
      processors: processors(),
    });

    // #when
    const tripwire = await drive(agent, 'durable', ['hello'], {
      requestContext: actorContext(),
      memory: THREAD,
    });
    const title = await titleWithin(memory);

    // #then — the model, title calls included, never ran
    expect({
      refused: tripwire !== undefined,
      prompts: prompts.map(promptLines),
      title,
      stored: (await storedText(memory)).includes(MARK),
    }).toEqual({ refused: true, prompts: [], title: '', stored: false });
  });
});

describe('a refused standard-loop call', () => {
  interface Output {
    tripwire?: { reason?: string; processorId?: string };
    text: string;
    finishReason?: string;
    messages: { id: string }[];
    rememberedMessages: { id: string }[];
  }

  async function fullOutput(
    agent: Driven,
    loop: (typeof STANDARD)[number],
    options: Record<string, unknown>,
    messages: unknown = markedInput(),
  ): Promise<Output> {
    if (loop === 'generate') {
      return (await agent.generate(
        messages as never,
        options as never,
      )) as Output;
    }
    const output = (await agent.stream(
      messages as never,
      options as never,
    )) as {
      getFullOutput(): Promise<Output>;
    };
    return output.getFullOutput();
  }

  const summary = (output: Output) => ({
    reason: output.tripwire?.reason,
    processorId: output.tripwire?.processorId,
    text: output.text,
    finishReason: output.finishReason,
    messages: output.messages.map(({ id }) => id),
    remembered: output.rememberedMessages.map(({ id }) => id),
  });

  it('returns the tripwire, the remembered history and the audit it did before, without the refused input in messages, on generate and stream', async () => {
    for (const loop of STANDARD) {
      // #given
      const memory = await historyMemory();
      const audit = new AuditLogger();
      const prompts: unknown[] = [];
      const agent = guardedAgent({ prompts, audit, memory });

      // #when
      const output = await fullOutput(agent, loop, {
        requestContext: actorContext(),
        memory: THREAD,
      });

      // #then
      expect({ loop, ...summary(output) }).toEqual({
        loop,
        reason: expect.stringMatching(/^deny-patterns: /),
        processorId: 'breakwater-policy-engine',
        text: '',
        finishReason: 'other',
        messages: ['mem-u', 'mem-a'],
        remembered: ['mem-u', 'mem-a'],
      });
      expect(prompts).toEqual([]);
      expect(
        audit.events().map(({ action, decision }) => `${action}:${decision}`),
      ).toEqual(['agent.input.authorize:allowed', 'agent.input.policy:denied']);
      const stored = await storedText(memory);
      expect(stored).toContain('earlier answer');
      expect(stored).not.toContain(MARK);
    }
  });

  it('leaves the input out of messages when RBACMiddleware denies on a raw agent, on generate and stream', async () => {
    for (const loop of STANDARD) {
      // #given
      const prompts: unknown[] = [];
      const agent = new Agent({
        id: 'raw',
        name: 'Raw',
        instructions: 'Answer the request.',
        model: recordingModel(prompts),
        inputProcessors: [new RBACMiddleware({ allowedRoles: ['admin'] })],
      });

      // #when
      const output = await fullOutput(agent as unknown as Driven, loop, {
        requestContext: actorContext(),
      });

      // #then
      expect({ loop, ...summary(output) }).toEqual({
        loop,
        reason: "role 'operator' is not in allowed roles [admin]",
        processorId: 'breakwater-rbac',
        text: '',
        finishReason: 'other',
        messages: [],
        remembered: [],
      });
      expect(prompts).toEqual([]);
    }
  });

  it.each(
    ADDED_THEN_REFUSED,
  )('leaves the context and memory messages processors added out of messages and rememberedMessages when %s, on generate and stream', async (_label, processors) => {
    const outcomes: unknown[] = [];
    for (const loop of STANDARD) {
      // #given
      const agent = guardedAgent({
        prompts: [],
        audit: new AuditLogger(),
        processors: processors(),
      });

      // #when
      const output = await fullOutput(
        agent,
        loop,
        { requestContext: actorContext() },
        ['hello'],
      );

      const { reason, messages, remembered } = summary(output);
      outcomes.push({
        loop,
        refused: reason !== undefined,
        messages,
        remembered,
      });
    }

    // #then
    expect(outcomes).toEqual(
      STANDARD.map((loop) => ({
        loop,
        refused: true,
        messages: [],
        remembered: [],
      })),
    );
  });

  it('leaves out of rememberedMessages a history message a processor redacted before a later processor throws, and keeps the rest, on generate and stream', async () => {
    const outcomes: unknown[] = [];
    for (const loop of STANDARD) {
      // #given
      const agent = guardedAgent({
        prompts: [],
        audit: new AuditLogger(),
        memory: await historyMemory(),
        processors: [
          app(
            ({ messages }) =>
              replaceText(messages, 'earlier question', 'redacted question'),
            'app-redactor',
          ),
          app(() => {
            throw new Error('processor failure');
          }, 'app-thrower'),
        ],
      });

      // #when
      const output = await fullOutput(agent, loop, {
        requestContext: actorContext(),
        memory: THREAD,
      });

      const { reason, remembered } = summary(output);
      outcomes.push({ loop, reason, remembered });
    }

    // #then
    expect(outcomes).toEqual(
      STANDARD.map((loop) => ({
        loop,
        reason: PROCESSOR_FAILED,
        remembered: ['mem-a'],
      })),
    );
  });
});

describe('an application input processor built on BaseProcessor', () => {
  class ServiceReader
    extends BaseProcessor<'service-reader'>
    implements GuardedInputProcessor
  {
    readonly id = 'service-reader';
    readonly #registrations: unknown[];

    constructor(registrations: unknown[]) {
      super();
      this.#registrations = registrations;
    }

    processInput(args: ProcessInputArgs) {
      this.#registrations.push(this.mastra);
      // A Mastra service, read as BaseProcessor's own example reads one.
      (this.mastra as Mastra).getLogger();
      return args.messages;
    }
  }

  it('receives the Mastra it is registered on and reads its services, on generate, stream and the durable loop', async () => {
    // #given — its own Mastra: registration skips a processor id it has seen
    const registrations: unknown[] = [];
    const prompts: unknown[] = [];
    const agent = guardedAgent({
      prompts,
      audit: new AuditLogger(),
      processors: [new ServiceReader(registrations)],
    });
    const mastra = new Mastra({
      logger: false,
      agents: { writer: agent as unknown as Agent },
    });

    // #when
    const outcomes: unknown[] = [];
    for (const loop of LOOPS) {
      outcomes.push({
        loop,
        ...(await outcomeOf(
          drive(agent, loop, ['hello'], { requestContext: actorContext() }),
        )),
      });
    }

    // #then
    expect({
      outcomes,
      calls: prompts.length,
      registered: registrations.map((registration) => registration === mastra),
    }).toEqual({
      outcomes: LOOPS.map((loop) => ({
        loop,
        tripwire: undefined,
        failure: undefined,
      })),
      calls: 3,
      registered: [true, true, true],
    });
  });
});

// A call whose application processors change what the model receives and
// that answers: the text the change must put in the prompt, and the text it
// must leave out.
interface Answered {
  readonly processors: () => readonly GuardedInputProcessor[];
  readonly input?: readonly string[];
  readonly instructions?: string;
  readonly memory?: () => Promise<MockMemory>;
  readonly loops?: readonly Loop[];
  readonly present: readonly string[];
  readonly absent: readonly string[];
}

async function expectAnswered(row: Answered): Promise<void> {
  const loops = row.loops ?? LOOPS;
  const outcomes: unknown[] = [];
  for (const loop of loops) {
    // #given
    const memory = await (row.memory ?? historyMemory)();
    const prompts: unknown[] = [];
    const agent = guardedAgent({
      prompts,
      audit: new AuditLogger(),
      processors: row.processors(),
      memory,
      ...(row.instructions !== undefined
        ? { instructions: row.instructions }
        : {}),
    });

    // #when
    const outcome = await outcomeOf(
      drive(agent, loop, [...(row.input ?? ['plain request'])], {
        requestContext: actorContext(),
        memory: THREAD,
      }),
    );

    const sent = prompts.flatMap(promptLines).join('\n');
    outcomes.push({
      loop,
      ...outcome,
      calls: prompts.length,
      present: row.present.filter((text) => sent.includes(text)),
      absent: row.absent.filter((text) => sent.includes(text)),
    });
  }

  // #then
  expect(outcomes).toEqual(
    loops.map((loop) => ({
      loop,
      tripwire: undefined,
      failure: undefined,
      calls: 1,
      present: row.present,
      absent: [],
    })),
  );
}

const FRENCH = 'Reply in French.';

describe('the value an application input processor returns', () => {
  const ACCEPTED: ReadonlyArray<[string, Answered]> = [
    [
      'an edited array with the same ids',
      {
        processors: () => [
          app(({ messages }) => replaceText(messages, 'secret', 'REDACTED')),
        ],
        input: ['draft secret text'],
        present: ['user: draft REDACTED text'],
        absent: ['secret'],
      },
    ],
    [
      'an array without one of the input messages',
      {
        processors: () => [
          app(({ messages }) =>
            messages.filter((message) => textOf(message) !== 'drop this'),
          ),
        ],
        input: ['keep this', 'drop this'],
        present: ['user: keep this'],
        absent: ['drop this'],
      },
    ],
    [
      'an array with an appended system-role entry',
      {
        processors: () => [
          app(({ messages }) => [
            ...messages,
            storedSystemMessage('app-system', FRENCH),
          ]),
        ],
        present: [`system: ${FRENCH}`, 'user: plain request'],
        absent: [],
      },
    ],
    [
      'its input messages as system-role entries with their ids',
      {
        processors: () => [
          app(({ messages }) =>
            messages.map((message) => ({
              ...message,
              role: 'system' as const,
            })),
          ),
        ],
        present: ['system: plain request', 'user: plain request'],
        absent: [],
      },
    ],
    [
      'a pair that adds a system message',
      {
        processors: () => [
          app(({ messages, systemMessages }) => ({
            messages,
            systemMessages: [
              ...systemMessages,
              { role: 'system', content: FRENCH },
            ],
          })),
        ],
        present: [`system: ${FRENCH}`, 'user: plain request'],
        absent: [],
      },
    ],
    [
      'a pair of another prototype',
      {
        processors: () => [
          app(
            ({ messages, systemMessages }) =>
              new (class {
                readonly messages = messages;
                readonly systemMessages = [
                  ...systemMessages,
                  { role: 'system' as const, content: FRENCH },
                ];
              })(),
          ),
        ],
        present: [`system: ${FRENCH}`, 'user: plain request'],
        absent: [],
      },
    ],
    [
      'the message list it edited in place',
      {
        processors: () => [
          app(({ messageList }) => {
            messageList.addSystem(FRENCH);
            return messageList;
          }),
        ],
        present: [`system: ${FRENCH}`, 'user: plain request'],
        absent: [],
      },
    ],
    [
      'undefined',
      {
        processors: () => [app(() => undefined)],
        present: ['user: plain request'],
        absent: [],
      },
    ],
    [
      'null',
      {
        processors: () => [app(() => null)],
        present: ['user: plain request'],
        absent: [],
      },
    ],
    [
      'an edited array after an await',
      {
        processors: () => [
          app(async ({ messages }) => {
            await Promise.resolve();
            return replaceText(messages, 'secret', 'REDACTED');
          }),
        ],
        input: ['draft secret text'],
        present: ['user: draft REDACTED text'],
        absent: ['secret'],
      },
    ],
    [
      'its input messages alone, without the thread history',
      {
        processors: () => [app((args) => splitInput(args).own)],
        loops: STANDARD,
        present: ['user: plain request'],
        absent: ['earlier question', 'earlier answer'],
      },
    ],
  ];

  it.each(
    ACCEPTED,
  )('applies %s to the call, which answers', async (_label, row) => {
    await expectAnswered(row);
  });
});

// A call whose application processors move its input, or add text, where the
// input policies would not otherwise read it.
interface Moved {
  readonly processors: () => readonly GuardedInputProcessor[];
  readonly input?: readonly unknown[];
  readonly instructions?: string;
  readonly unsaved?: true;
  readonly loops?: readonly Loop[];
}

async function expectStoppedOnPolicy(row: Moved): Promise<void> {
  const loops = row.loops ?? LOOPS;
  const outcomes: unknown[] = [];
  for (const loop of loops) {
    // #given
    const memory = await historyMemory();
    const prompts: unknown[] = [];
    const agent = guardedAgent({
      prompts,
      audit: new AuditLogger(),
      processors: row.processors(),
      memory,
      ...(row.instructions !== undefined
        ? { instructions: row.instructions }
        : {}),
    });

    // #when
    const outcome = await outcomeOf(
      drive(agent, loop, [...(row.input ?? markedInput())], {
        requestContext: actorContext(),
        memory: THREAD,
      }),
    );

    outcomes.push({
      loop,
      ...outcome,
      prompts: prompts.length,
      sent: JSON.stringify(prompts).includes(MARK),
      ...(row.unsaved ? { saved: await savedMarker(memory, loop) } : {}),
    });
  }

  // #then
  expect(outcomes).toEqual(
    loops.map((loop) => ({
      loop,
      tripwire: expect.stringMatching(/^deny-patterns: /),
      failure: undefined,
      prompts: 0,
      sent: false,
      ...(row.unsaved ? { saved: false } : {}),
    })),
  );
}

// The input a processor re-adds with source `input` under the id of a message
// memory holds.
const asHistoryMessage = (returns: 'the list' | 'nothing') =>
  app((args) => {
    const text = callerText(args);
    dropInput(args);
    args.messageList.add(userMessage('mem-u', text), 'input');
    return returns === 'the list' ? args.messageList : undefined;
  });

describe('what application input processors add or change, read by the input policies', () => {
  const MOVED: ReadonlyArray<[string, Moved]> = [
    [
      'input messages returned as system-role entries with their ids',
      {
        processors: () => [
          app(({ messages }) =>
            messages.map((message) => ({
              ...message,
              role: 'system' as const,
            })),
          ),
        ],
      },
    ],
    [
      'input messages returned as system-role entries with new ids',
      {
        processors: () => [
          app(({ messages }) =>
            messages.map((message) => ({
              ...message,
              id: `${message.id}-system`,
              role: 'system' as const,
            })),
          ),
        ],
      },
    ],
    [
      'the input moved into the system messages of a pair',
      {
        processors: () => [
          app((args) => {
            const { own, other } = splitInput(args);
            return {
              messages: other,
              systemMessages: [
                ...args.systemMessages,
                { role: 'system', content: own.map(textOf).join(' ') },
              ],
            };
          }),
        ],
      },
    ],
    [
      'one of two input messages moved into a system-role entry',
      {
        input: ['hello there', `${MARK} payload`],
        processors: () => [
          app(({ messages }) => {
            const moved = messages.find((message) =>
              textOf(message).includes(MARK),
            ) as MastraDBMessage;
            return [
              ...messages.filter((message) => message !== moved),
              storedSystemMessage(`${moved.id}-system`, textOf(moved)),
            ];
          }),
        ],
      },
    ],
    [
      'the input appended to the instructions in place',
      {
        processors: () => [
          app((args) => {
            const instructions = instructionsOf(args);
            instructions.content = `${instructions.content} ${callerText(args)}`;
            return splitInput(args).other;
          }),
        ],
      },
    ],
    [
      'the input added as a system message by a processor that returns nothing',
      {
        processors: () => [
          app((args) => {
            args.messageList.addSystem(callerText(args));
            dropInput(args);
            return undefined;
          }),
        ],
      },
    ],
    [
      'the input added as a tagged system message',
      {
        processors: () => [
          app((args) => {
            args.messageList.addSystem(callerText(args), 'app-tag');
            dropInput(args);
            return args.messageList;
          }),
        ],
      },
    ],
    [
      'the input added as a system-role message through add',
      {
        processors: () => [
          app((args) => {
            args.messageList.add(
              { role: 'system', content: callerText(args) },
              'input',
            );
            dropInput(args);
            return args.messageList;
          }),
        ],
      },
    ],
    [
      'the input pushed onto the system messages it received',
      {
        processors: () => [
          app((args) => {
            args.systemMessages.push({
              role: 'system',
              content: callerText(args),
            });
            return splitInput(args).other;
          }),
        ],
      },
    ],
    [
      'the input split across the text parts of a system message in a pair',
      {
        processors: () => [
          app((args) => {
            const text = callerText(args);
            return {
              messages: splitInput(args).other,
              systemMessages: [
                ...args.systemMessages,
                {
                  role: 'system',
                  content: [
                    { type: 'text', text: text.slice(0, 2) },
                    { type: 'text', text: text.slice(2) },
                  ],
                } as never,
              ],
            };
          }),
        ],
      },
    ],
    [
      'the input added as a system message after an await',
      {
        processors: () => [
          app(async (args) => {
            await Promise.resolve();
            args.messageList.addSystem(callerText(args));
            dropInput(args);
            return args.messageList;
          }),
        ],
      },
    ],
    [
      'the input moved by a pair before a processor that passes it on',
      {
        processors: () => [
          app((args) => {
            const { own, other } = splitInput(args);
            return {
              messages: other,
              systemMessages: [
                ...args.systemMessages,
                { role: 'system', content: own.map(textOf).join(' ') },
              ],
            };
          }, 'app-mover'),
          app(() => undefined, 'app-passer'),
        ],
      },
    ],
    [
      'a system message that a later processor removes',
      {
        input: ['hello'],
        processors: () => [
          app(({ messageList }) => {
            messageList.addSystem(`${MARK} context from the first processor`);
            return messageList;
          }, 'app-adder'),
          app(({ messageList }) => {
            messageList.replaceAllSystemMessages(
              messageList
                .getSystemMessages()
                .filter((message) => !String(message.content).includes(MARK)),
            );
            return messageList;
          }, 'app-remover'),
        ],
      },
    ],
    [
      'a context message that a later processor rewrites under its id',
      {
        input: ['hello'],
        processors: () => [
          app(({ messageList }) => {
            messageList.add(
              userMessage(
                'app-context',
                `${MARK} context from the first processor`,
              ),
              'context',
            );
            return messageList;
          }, 'app-adder'),
          app(({ messageList }) => {
            messageList.removeByIds(['app-context']);
            messageList.add(
              userMessage('app-context', 'context from the first processor'),
              'context',
            );
            return messageList;
          }, 'app-redactor'),
        ],
      },
    ],
    [
      'instructions that carry the marker, edited by a processor',
      {
        input: ['hello'],
        instructions: `You are a writer. ${MARK} rule applies.`,
        processors: () => [
          app((args) => {
            instructionsOf(args).content += ' Today is Tuesday.';
            return undefined;
          }),
        ],
      },
    ],
    [
      'the input added as a memory message',
      {
        processors: () => [
          app((args) => {
            args.messageList.add(
              { role: 'user', content: callerText(args) },
              'memory',
            );
            dropInput(args);
            return args.messageList;
          }),
        ],
      },
    ],
    [
      'the input added as a context message',
      {
        processors: () => [
          app((args) => {
            args.messageList.add(
              { role: 'user', content: callerText(args) },
              'context',
            );
            dropInput(args);
            return args.messageList;
          }),
        ],
      },
    ],
    [
      'the input added as a response message',
      {
        unsaved: true,
        processors: () => [
          app((args) => {
            args.messageList.add(
              { role: 'assistant', content: callerText(args) },
              'response',
            );
            dropInput(args);
            return args.messageList;
          }),
        ],
      },
    ],
    [
      'a returned history message whose content is the input',
      {
        processors: () => [
          app((args) => {
            const { own, other } = splitInput(args);
            const target = other.find(({ role }) => role === 'user') ?? {
              id: 'mem-u',
              role: 'user' as const,
              createdAt: new Date(Date.now() - 300_000),
              content: { format: 2 as const, parts: [] },
            };
            return [
              ...other.filter(({ id }) => id !== target.id),
              {
                ...target,
                content: {
                  ...target.content,
                  parts: [{ type: 'text', text: own.map(textOf).join(' ') }],
                },
              },
            ];
          }),
        ],
      },
    ],
  ];

  it.each(
    MOVED,
  )('stops the call on the input policy for %s, before the model, on generate, stream and the durable loop', async (_label, row) => {
    await expectStoppedOnPolicy(row);
  });

  // Mastra keeps a message's id in its memory set when a processor replaces
  // that message, or merges into it, with source `input`. The guarded durable
  // loop loads no history, so only a memory message the processor adds itself
  // is there to replace.
  const UNDER_MEMORY_IDS: ReadonlyArray<[string, Moved]> = [
    [
      'the input re-added under a history id by a processor that returns the list, on generate and stream',
      {
        loops: STANDARD,
        unsaved: true,
        processors: () => [asHistoryMessage('the list')],
      },
    ],
    [
      'the input re-added under a history id by a processor that returns nothing, on generate and stream',
      {
        loops: STANDARD,
        unsaved: true,
        processors: () => [asHistoryMessage('nothing')],
      },
    ],
    [
      'an assistant input message merged into the last history message by a processor that re-adds the input, on generate and stream',
      {
        loops: STANDARD,
        unsaved: true,
        input: [
          { role: 'assistant', content: `${MARK} payload` },
          { role: 'user', content: 'hi' },
        ],
        processors: () => [
          app(({ messageList }) => {
            const input = messageList.get.input.db();
            messageList.removeByIds(input.map(({ id }) => id));
            for (const message of input) {
              messageList.add(
                {
                  ...message,
                  content: {
                    ...message.content,
                    parts: message.content.parts.map((part) => ({ ...part })),
                  },
                },
                'input',
              );
            }
            return messageList;
          }),
        ],
      },
    ],
    [
      'the input re-added under the id of a memory message the processor added, on generate, stream and the durable loop',
      {
        unsaved: true,
        processors: () => [
          app((args) => {
            const text = callerText(args);
            dropInput(args);
            args.messageList.add(userMessage('moved', 'decoy'), 'memory');
            args.messageList.add(userMessage('moved', text), 'input');
            return args.messageList;
          }),
        ],
      },
    ],
  ];

  it.each(
    UNDER_MEMORY_IDS,
  )('stops the call on the input policy, before the model and with nothing saved, for %s', async (_label, row) => {
    await expectStoppedOnPolicy(row);
  });

  const KEPT: ReadonlyArray<[string, Answered]> = [
    [
      'system context a processor adds',
      {
        input: ['hello'],
        processors: () => [
          app(({ messageList }) => {
            messageList.addSystem('Context: the user prefers metric units.');
            return messageList;
          }),
        ],
        present: ['system: Context: the user prefers metric units.'],
        absent: [],
      },
    ],
    [
      'instructions that carry the marker, passed on unchanged',
      {
        input: ['hello'],
        instructions: `You are a writer. ${MARK} rule applies.`,
        processors: () => [app(() => undefined)],
        present: [`system: You are a writer. ${MARK} rule applies.`],
        absent: [],
      },
    ],
    [
      'history that carries the marker, returned unchanged as copies',
      {
        input: ['hello'],
        loops: STANDARD,
        memory: () => historyMemory(`earlier ${MARK} question`),
        processors: () => [
          app(({ messages }) => messages.map((message) => ({ ...message }))),
        ],
        present: [`user: earlier ${MARK} question`],
        absent: [],
      },
    ],
    [
      'a history message a processor redacts',
      {
        input: ['hello'],
        memory: () => historyMemory(`earlier ${MARK} question`),
        processors: () => [
          app(({ messages }) => replaceText(messages, MARK, 'REDACTED')),
        ],
        present: ['user: hello'],
        absent: [MARK],
      },
    ],
    [
      'a working-memory template that carries the marker',
      {
        input: ['hello'],
        loops: STANDARD,
        memory: () =>
          threadMemory({
            workingMemoryTemplate: `# Notes\n- ${MARK} working memory`,
          }),
        processors: () => [app(() => undefined)],
        present: [`${MARK} working memory`],
        absent: [],
      },
    ],
  ];

  it.each(
    KEPT,
  )('answers a call with %s, which the processors leave for the model', async (_label, row) => {
    await expectAnswered(row);
  });

  const CONTEXT_MARK = 'MKCONTEXTNOTE';
  const MEMORY_MARK = 'MKMEMORYNOTE';

  const occurrences = (text: string, mark: string): number =>
    text.split(mark).length - 1;

  it('reads a context or memory message a processor adds once, and has the context message among messages on generate and stream only', async () => {
    const outcomes: unknown[] = [];
    for (const loop of LOOPS) {
      // #given
      const evaluated: { text: string; messages: string[] }[] = [];
      const agent = guardedAgent({
        prompts: [],
        audit: new AuditLogger(),
        policies: [
          {
            name: 'recording',
            phases: ['input'],
            evaluate: ({ text, messages }) => {
              evaluated.push({ text, messages: messages.map(textOf) });
              return { allowed: true };
            },
          },
        ],
        processors: [
          app(({ messageList }) => {
            messageList.add(
              userMessage('app-context', `${CONTEXT_MARK} note`),
              'context',
            );
            messageList.add(
              userMessage('app-memory', `${MEMORY_MARK} note`),
              'memory',
            );
            return messageList;
          }),
        ],
      });

      // #when
      const outcome = await outcomeOf(
        drive(agent, loop, ['hello'], { requestContext: actorContext() }),
      );

      outcomes.push({
        loop,
        ...outcome,
        contextReads: evaluated.map(({ text }) =>
          occurrences(text, CONTEXT_MARK),
        ),
        memoryReads: evaluated.map(({ text }) =>
          occurrences(text, MEMORY_MARK),
        ),
        messages: evaluated.map(({ messages }) => messages),
      });
    }

    // #then
    expect(outcomes).toEqual(
      LOOPS.map((loop) => ({
        loop,
        tripwire: undefined,
        failure: undefined,
        contextReads: [1],
        memoryReads: [1],
        messages: [
          loop === 'durable' ? ['hello'] : ['hello', `${CONTEXT_MARK} note`],
        ],
      })),
    );
  });

  it("answers a call whose history Mastra's UnicodeNormalizer returns as it was, without evaluating that history, on generate and stream", async () => {
    const outcomes: unknown[] = [];
    for (const loop of STANDARD) {
      // #given — history the normalizer leaves as it is, carrying an email
      const evaluated: string[] = [];
      const prompts: unknown[] = [];
      const agent = guardedAgent({
        prompts,
        audit: new AuditLogger(),
        memory: await historyMemory(
          'earlier question from jane.roe@acme.example',
        ),
        policies: [
          {
            name: 'recording',
            phases: ['input'],
            evaluate: ({ text }) => {
              evaluated.push(text);
              return { allowed: true };
            },
          },
          piiSecrets(),
        ],
        processors: [new UnicodeNormalizer()],
      });

      // #when
      const outcome = await outcomeOf(
        drive(agent, loop, ['hello'], {
          requestContext: actorContext(),
          memory: THREAD,
        }),
      );

      outcomes.push({ loop, ...outcome, calls: prompts.length, evaluated });
    }

    // #then
    expect(outcomes).toEqual(
      STANDARD.map((loop) => ({
        loop,
        tripwire: undefined,
        failure: undefined,
        calls: 1,
        evaluated: ['hello'],
      })),
    );
  });
});

describe('provider options on the system messages an application input processor adds or changes', () => {
  const UNCLASSIFIED = 'input message content is not classified';
  const OPENAI_COMPATIBLE: MastraModelConfig = {
    id: 'probe/probe-model',
    url: 'http://probe.invalid/v1',
    apiKey: 'test-key',
  };

  // The openai-compatible adapter spreads these over the system message it
  // sends, so `content` replaces the system text.
  const carrying = (args: ProcessInputArgs) => ({
    openaiCompatible: { content: callerText(args) },
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  const ROUTES: ReadonlyArray<[string, () => GuardedInputProcessor]> = [
    [
      'on a system message a pair adds',
      () =>
        app((args) => {
          const providerOptions = carrying(args);
          dropInput(args);
          return {
            messages: [],
            systemMessages: [
              ...args.systemMessages,
              { role: 'system', content: 'Context note.', providerOptions },
            ],
          };
        }),
    ],
    [
      'on a text part of a system message a pair adds',
      () =>
        app((args) => {
          const providerOptions = carrying(args);
          dropInput(args);
          return {
            messages: [],
            systemMessages: [
              ...args.systemMessages,
              {
                role: 'system',
                content: [
                  { type: 'text', text: 'Context note.', providerOptions },
                ],
              } as never,
            ],
          };
        }),
    ],
    [
      'on a system message added to the list',
      () =>
        app((args) => {
          const providerOptions = carrying(args);
          dropInput(args);
          args.messageList.addSystem({
            role: 'system',
            content: 'Context note.',
            providerOptions,
          });
          return args.messageList;
        }),
    ],
    [
      'set in place as the providerOptions of the instructions',
      () =>
        app((args) => {
          instructionsOf(args).providerOptions = carrying(args);
          dropInput(args);
          return args.messageList;
        }),
    ],
    [
      'set in place as the experimental_providerMetadata of the instructions',
      () =>
        app((args) => {
          instructionsOf(args).experimental_providerMetadata = carrying(args);
          dropInput(args);
          return args.messageList;
        }),
    ],
  ];

  it.each(
    ROUTES,
  )('stops the input carried in a refused provider option %s, before any request, on generate, stream and the durable loop', async (_label, processor) => {
    const outcomes: unknown[] = [];
    for (const loop of LOOPS) {
      // #given
      const bodies: string[] = [];
      vi.stubGlobal(
        'fetch',
        async (_url: unknown, init?: { body?: unknown }): Promise<Response> => {
          bodies.push(typeof init?.body === 'string' ? init.body : '');
          return new Response(
            JSON.stringify({ error: { message: 'recorded' } }),
            { status: 400, headers: { 'content-type': 'application/json' } },
          );
        },
      );
      const audit = new AuditLogger();
      const agent = guardedAgent({
        prompts: [],
        audit,
        model: OPENAI_COMPATIBLE,
        processors: [processor()],
      });

      // #when
      const outcome = await outcomeOf(
        drive(agent, loop, markedInput(), { requestContext: actorContext() }),
      );

      outcomes.push({
        loop,
        ...outcome,
        requests: bodies.length,
        sent: bodies.some((body) => body.includes(MARK)),
        policyErrors: eventsOf(audit, 'agent.input.policy', 'error').length,
      });
    }

    // #then
    expect(outcomes).toEqual(
      LOOPS.map((loop) => ({
        loop,
        tripwire: UNCLASSIFIED,
        failure: undefined,
        requests: 0,
        sent: false,
        policyErrors: 1,
      })),
    );
  });
});
