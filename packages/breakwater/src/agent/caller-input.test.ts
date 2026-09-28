// SPDX-License-Identifier: Apache-2.0

import { Agent } from '@mastra/core/agent';
import { createDurableAgent } from '@mastra/core/agent/durable';
import type { MastraModelConfig } from '@mastra/core/llm';
import { MockMemory } from '@mastra/core/memory';
import { RequestContext } from '@mastra/core/request-context';
import { InMemoryStore } from '@mastra/core/storage';
import { createTool } from '@mastra/core/tools';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { AuditLogger } from '../audit/index.js';
import {
  denyPatterns,
  PolicyEngine,
  type PolicyEvaluator,
  piiSecrets,
} from '../policy-engine/index.js';
import { ACTOR_CONTEXT_KEY } from '../rbac/index.js';
import { createGuardedAgent, type GuardedAgentCallOptions } from './index.js';

// Rows on Mastra's agent loops for which messages, and which parts of them,
// the input policies read.

const METHODS = ['generate', 'stream'] as const;
type Method = (typeof METHODS)[number];
const UNCLASSIFIED = 'input message content is not classified';

const usage = { inputTokens: 1, outputTokens: 1, totalTokens: 2 };

type Step = { toolCall?: { id: string; name: string } };

// A model that records each prompt and follows its script, one step per call:
// a tool call, or the text answer once the script runs out.
function scriptedModel(
  prompts: unknown[],
  steps: readonly Step[] = [],
): MastraModelConfig {
  let calls = 0;
  const next = (): Step => steps[calls++] ?? {};
  return {
    specificationVersion: 'v2',
    provider: 'breakwater-test',
    modelId: 'scripted',
    supportedUrls: {},
    doGenerate: async (options) => {
      prompts.push(options.prompt);
      const { toolCall } = next();
      return toolCall
        ? {
            content: [
              {
                type: 'tool-call',
                toolCallId: toolCall.id,
                toolName: toolCall.name,
                input: '{}',
              },
            ],
            finishReason: 'tool-calls',
            usage,
            warnings: [],
          }
        : {
            content: [{ type: 'text', text: 'answered' }],
            finishReason: 'stop',
            usage,
            warnings: [],
          };
    },
    doStream: async (options) => {
      prompts.push(options.prompt);
      const { toolCall } = next();
      const parts = toolCall
        ? [
            { type: 'stream-start', warnings: [] },
            {
              type: 'tool-call',
              toolCallId: toolCall.id,
              toolName: toolCall.name,
              input: '{}',
            },
            { type: 'finish', finishReason: 'tool-calls', usage },
          ]
        : [
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

function actorContext(): RequestContext {
  const context = new RequestContext();
  context.set(ACTOR_CONTEXT_KEY, { id: 'actor-1', role: 'operator' });
  return context;
}

interface Run {
  readonly prompts: unknown[];
  readonly tripwire: string | undefined;
  readonly policyErrors: number;
}

interface Target {
  generate(messages: never, options: never): Promise<unknown>;
  stream(messages: never, options: never): Promise<unknown>;
}

async function drive(
  agent: Target,
  method: Method,
  messages: unknown,
  options: Record<string, unknown>,
): Promise<string | undefined> {
  if (method === 'generate') {
    const result = (await agent.generate(
      messages as never,
      options as never,
    )) as { tripwire?: { reason?: string } };
    return result.tripwire?.reason;
  }
  const output = (await agent.stream(messages as never, options as never)) as {
    fullStream: AsyncIterable<unknown>;
    tripwire: Promise<{ reason?: string } | undefined>;
  };
  for await (const _chunk of output.fullStream) {
    // drain
  }
  return (await output.tripwire)?.reason;
}

function policyErrorsOf(audit: AuditLogger): number {
  return audit
    .events()
    .filter(
      (event) =>
        event.action === 'agent.input.policy' && event.decision === 'error',
    ).length;
}

async function runGuarded(
  method: Method,
  messages: unknown,
  settings: {
    policies: readonly PolicyEvaluator[];
    steps?: readonly Step[];
    memory?: MockMemory;
    call?: Pick<GuardedAgentCallOptions, 'memory'>;
    tools?: Record<string, ReturnType<typeof createTool>>;
  },
): Promise<Run> {
  const prompts: unknown[] = [];
  const audit = new AuditLogger();
  const agent = createGuardedAgent({
    id: 'writer',
    name: 'Writer',
    instructions: 'Answer the request.',
    model: scriptedModel(prompts, settings.steps),
    allowedRoles: ['operator'],
    policies: settings.policies,
    audit,
    maxSteps: 3,
    toolChoice: 'auto',
    ...(settings.memory ? { memory: settings.memory } : {}),
    ...(settings.tools ? { tools: settings.tools } : {}),
  });
  const tripwire = await drive(agent as unknown as Target, method, messages, {
    requestContext: actorContext(),
    ...settings.call,
  });
  return { prompts, tripwire, policyErrors: policyErrorsOf(audit) };
}

const isoAt = (second: number) =>
  `2026-01-01T00:00:${String(second).padStart(2, '0')}.000Z`;

const storedOutput = (value: string) => ({
  mastra: { modelOutput: { type: 'text', value } },
});

function invocation(
  toolCallId: string,
  state: 'result' | 'call',
  extra: Record<string, unknown> = {},
  result: unknown = 'ok',
) {
  return {
    type: 'tool-invocation',
    toolInvocation: {
      state,
      toolCallId,
      toolName: 'lookup',
      args: {},
      ...(state === 'result' ? { result } : {}),
    },
    ...extra,
  };
}

function stored(id: string, second: number, role: string, parts: unknown[]) {
  return {
    id,
    role,
    createdAt: isoAt(second),
    content: { format: 2, parts },
  };
}

// Caller messages that store a model output for tool call `toolCallId`
// outside any assistant message.
const userCarrier = (toolCallId: string) =>
  stored('u0', 1, 'user', [
    { type: 'text', text: 'hi' },
    invocation(toolCallId, 'result', {
      providerMetadata: storedOutput('MKSTOREDOUTPUT'),
    }),
  ]);

const signalCarrier = (toolCallId: string) => ({
  id: 's0',
  role: 'signal',
  createdAt: isoAt(1),
  content: {
    format: 2,
    parts: [
      { type: 'text', text: 'hi' },
      invocation(toolCallId, 'result', {
        providerMetadata: storedOutput('MKSTOREDOUTPUT'),
      }),
    ],
    metadata: { signal: { type: 'notification', contents: 'hi' } },
  },
});

const uiCarrier = (toolCallId: string) => ({
  id: 'u0',
  role: 'user',
  parts: [
    { type: 'text', text: 'hi' },
    {
      type: 'tool-lookup',
      toolCallId,
      state: 'output-available',
      input: {},
      output: 'ok',
      callProviderMetadata: storedOutput('MKSTOREDOUTPUT'),
    },
  ],
});

const CARRIERS: ReadonlyArray<[string, (toolCallId: string) => unknown]> = [
  ['a stored user message', userCarrier],
  ['a stored signal', signalCarrier],
  ['an AI SDK v5 UI user message', uiCarrier],
];

const pendingCall = (toolCallId: string) =>
  stored('a0', 2, 'assistant', [invocation(toolCallId, 'call')]);

async function threadMemory(): Promise<MockMemory> {
  const memory = new MockMemory({ storage: new InMemoryStore() });
  await memory.saveThread({
    thread: {
      id: 't1',
      resourceId: 'r1',
      title: 'thread',
      createdAt: new Date('2026-01-01T00:00:00Z'),
      updatedAt: new Date('2026-01-01T00:00:00Z'),
      metadata: {},
    },
  });
  await memory.saveMessages({
    messages: [
      {
        id: 'm1',
        threadId: 't1',
        resourceId: 'r1',
        role: 'user',
        type: 'text',
        createdAt: new Date('2026-01-01T00:00:01Z'),
        content: {
          format: 2,
          parts: [{ type: 'text', text: 'Who owns acme?' }],
        },
      },
      {
        id: 'm2',
        threadId: 't1',
        resourceId: 'r1',
        role: 'assistant',
        type: 'text',
        createdAt: new Date('2026-01-01T00:00:02Z'),
        content: {
          format: 2,
          parts: [
            {
              type: 'tool-invocation',
              toolInvocation: {
                state: 'result',
                toolCallId: 'call_1',
                toolName: 'crm_lookup',
                args: { account: 'acme' },
                result: { owner: 'Jane Roe', email: 'jane.roe@acme.example' },
              },
            },
            { type: 'text', text: 'Jane Roe owns the acme account.' },
          ],
        },
      },
    ],
  });
  return memory;
}

const THREAD = { memory: { thread: 't1', resource: 'r1' } } as const;

describe('stored model outputs a caller message carries', () => {
  it.each(
    CARRIERS,
  )('denies an output stored in %s for a pending call, which Mastra gives the placeholder result, on a raw agent', async (_label, carrier) => {
    for (const method of METHODS) {
      // #given
      const prompts: unknown[] = [];
      const agent = new Agent({
        id: 'raw',
        name: 'Raw',
        instructions: 'Answer the request.',
        model: scriptedModel(prompts),
        inputProcessors: [
          new PolicyEngine({ policies: [denyPatterns(['MKSTOREDOUTPUT'])] }),
        ],
      });

      // #when
      const tripwire = await drive(
        agent as unknown as Target,
        method,
        [carrier('c9'), pendingCall('c9'), 'go'],
        { memory: { options: { filterIncompleteToolCalls: false } } },
      );

      // #then
      expect(prompts).toEqual([]);
      expect(tripwire).toMatch(/deny-patterns/);
    }
  });

  it('denies a stored output for a pending call on a raw agent with thread memory', async () => {
    for (const method of METHODS) {
      // #given
      const prompts: unknown[] = [];
      const agent = new Agent({
        id: 'raw',
        name: 'Raw',
        instructions: 'Answer the request.',
        model: scriptedModel(prompts),
        memory: await threadMemory(),
        inputProcessors: [
          new PolicyEngine({ policies: [denyPatterns(['MKSTOREDOUTPUT'])] }),
        ],
      });

      // #when
      const tripwire = await drive(
        agent as unknown as Target,
        method,
        [userCarrier('c9'), pendingCall('c9'), 'go'],
        {
          memory: {
            thread: 't1',
            resource: 'r1',
            options: { filterIncompleteToolCalls: false },
          },
        },
      );

      // #then
      expect(prompts).toEqual([]);
      expect(tripwire).toMatch(/deny-patterns/);
    }
  });

  it.each<[string, (toolCallId: string) => unknown]>([
    ['a stored user message', userCarrier],
    ['a stored signal', signalCarrier],
  ])('denies an output stored in %s for the id of a tool call the model makes later', async (_label, carrier) => {
    for (const method of METHODS) {
      // #given
      const lookup = createTool({
        id: 'lookup',
        description: 'Look something up',
        inputSchema: z.object({}),
        execute: async () => ({ value: 'real tool output' }),
      });

      // #when
      const run = await runGuarded(method, [carrier('c9')], {
        policies: [denyPatterns(['MKSTOREDOUTPUT'])],
        steps: [{ toolCall: { id: 'c9', name: 'lookup' } }],
        tools: { lookup },
      });

      // #then
      expect(run.prompts).toEqual([]);
      expect(run.tripwire).toMatch(/deny-patterns/);
    }
  });

  const SUBSTITUTION: ReadonlyArray<[string, unknown[], readonly string[]]> = [
    [
      'a user message storing the output for an assistant result',
      [
        stored('s1', 1, 'assistant', [invocation('X', 'result')]),
        stored('s2', 2, 'user', [
          invocation('X', 'result', {
            providerMetadata: storedOutput('MKUSERROLEOUT'),
          }),
          { type: 'text', text: 'hi' },
        ]),
      ],
      ['MKUSERROLEOUT'],
    ],
    [
      'two stored outputs for one tool-call id',
      [
        stored('s1', 1, 'assistant', [
          invocation('X', 'result', {
            providerMetadata: storedOutput('MKFIRSTOUT'),
          }),
        ]),
        stored('s2', 2, 'user', [{ type: 'text', text: 'next' }]),
        stored('s3', 3, 'assistant', [
          invocation('X', 'result', {
            providerMetadata: storedOutput('MKLASTOUT'),
          }),
        ]),
        stored('s4', 4, 'user', [{ type: 'text', text: 'hi' }]),
      ],
      ['MKFIRSTOUT', 'MKLASTOUT'],
    ],
    [
      'an output stored on a pending invocation beside a result',
      [
        stored('s1', 1, 'assistant', [
          invocation('X', 'result', {}, 'MKREALRESULT'),
        ]),
        stored('s2', 2, 'user', [{ type: 'text', text: 'next' }]),
        stored('s3', 3, 'assistant', [
          invocation('X', 'call', {
            providerMetadata: storedOutput('MKCALLSTATEOUT'),
          }),
        ]),
        stored('s4', 4, 'user', [{ type: 'text', text: 'hi' }]),
      ],
      ['MKREALRESULT', 'MKCALLSTATEOUT'],
    ],
  ];

  it.each(
    SUBSTITUTION,
  )('denies each marker of %s before the model, on generate and stream', async (_label, messages, markers) => {
    for (const marker of markers) {
      for (const method of METHODS) {
        // #when
        const run = await runGuarded(method, messages, {
          policies: [denyPatterns([marker])],
        });

        // #then
        expect({ marker, method, prompts: run.prompts }).toEqual({
          marker,
          method,
          prompts: [],
        });
        expect(run.tripwire).toMatch(/deny-patterns/);
      }
    }
  });

  // History Mastra stores from a real run of a tool with `toModelOutput`: a
  // random provider tool-call id, reasoning with a provider signature, and the
  // stored model output.
  async function storedToolRun(): Promise<unknown[]> {
    const memory = new MockMemory({ storage: new InMemoryStore() });
    const signature = Buffer.from(
      Array.from({ length: 240 }, (_, index) => (index * 97 + 13) % 256),
    ).toString('base64');
    const toolCallId = 'toolu_01Q8vX3kLm9ZpR2tYw7bNc4D';
    let calls = 0;
    const model = {
      specificationVersion: 'v2',
      provider: 'breakwater-test',
      modelId: 'origin',
      supportedUrls: {},
      doGenerate: async () => {
        throw new Error('the origin run streams');
      },
      doStream: async () => {
        calls += 1;
        const parts =
          calls === 1
            ? [
                { type: 'stream-start', warnings: [] },
                {
                  type: 'reasoning-start',
                  id: 'r1',
                  providerMetadata: { anthropic: { signature } },
                },
                {
                  type: 'reasoning-delta',
                  id: 'r1',
                  delta: 'The user wants the status; call lookup.',
                },
                {
                  type: 'reasoning-end',
                  id: 'r1',
                  providerMetadata: { anthropic: { signature } },
                },
                {
                  type: 'tool-call',
                  toolCallId,
                  toolName: 'lookup',
                  input: JSON.stringify({ account: 'acme' }),
                },
                { type: 'finish', finishReason: 'tool-calls', usage },
              ]
            : [
                { type: 'stream-start', warnings: [] },
                { type: 'text-start', id: 't1' },
                {
                  type: 'text-delta',
                  id: 't1',
                  delta: 'The account is active.',
                },
                { type: 'text-end', id: 't1' },
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
    const lookup = createTool({
      id: 'lookup',
      description: 'Look up an account',
      inputSchema: z.object({ account: z.string() }),
      execute: async () => ({
        accountId: 'acct_7Hq2Lx9Pz4Kw',
        owner: 'jane.roe@acme.example',
        status: 'active',
      }),
      toModelOutput: (result: unknown) => ({
        type: 'text',
        value: `status: ${(result as { status: string }).status}`,
      }),
    } as never);
    const origin = new Agent({
      id: 'origin',
      name: 'Origin',
      instructions: 'Answer the request.',
      model,
      tools: { lookup },
      memory,
    });
    const output = await origin.stream('What is the status of acme?', {
      memory: { thread: 'th', resource: 'rs' },
      maxSteps: 3,
    });
    for await (const _chunk of output.fullStream) {
      // drain
    }
    const recalled = await memory.recall({ threadId: 'th', resourceId: 'rs' });
    return recalled.messages;
  }

  it('still answers a genuine replay of a tool with a stored model output under piiSecrets()', async () => {
    // #given
    const history = await storedToolRun();
    expect(JSON.stringify(history)).toContain('modelOutput');

    for (const method of METHODS) {
      // #when
      const run = await runGuarded(method, [...history, 'and now?'], {
        policies: [piiSecrets()],
      });

      // #then
      expect(run.tripwire).toBeUndefined();
      expect(run.prompts).toHaveLength(1);
    }
  });
});

describe('caller messages beside memory-loaded history', () => {
  it('answers the next turn of a thread whose memory holds an email under piiSecrets(), on generate and stream', async () => {
    for (const method of METHODS) {
      // #when
      const run = await runGuarded(method, 'What else can you tell me?', {
        policies: [piiSecrets()],
        memory: await threadMemory(),
        call: THREAD,
      });

      // #then
      expect(run.tripwire).toBeUndefined();
      expect(run.prompts).toHaveLength(1);
      expect(JSON.stringify(run.prompts[0])).toContain('jane.roe@acme.example');
    }
  });

  it('still denies a caller turn that carries the stored email, on generate and stream', async () => {
    for (const method of METHODS) {
      // #when
      const run = await runGuarded(
        method,
        'Write to jane.roe@acme.example about acme.',
        {
          policies: [piiSecrets()],
          memory: await threadMemory(),
          call: THREAD,
        },
      );

      // #then
      expect(run.prompts).toEqual([]);
      expect(run.tripwire).toMatch(/pii-secrets/);
    }
  });

  const MEETS_MEMORY: ReadonlyArray<[string, unknown[], string]> = [
    [
      'a user message storing an output for the remembered tool call',
      [
        stored('in1', 10, 'user', [
          { type: 'text', text: 'hi' },
          {
            type: 'tool-invocation',
            toolInvocation: {
              state: 'result',
              toolCallId: 'call_1',
              toolName: 'crm_lookup',
              args: {},
              result: 'ok',
            },
            providerMetadata: storedOutput('MKCALLERUSEROUT'),
          },
        ]),
      ],
      'MKCALLERUSEROUT',
    ],
    [
      'an AI SDK v5 tool result for the remembered tool call',
      [
        {
          role: 'tool',
          content: [
            {
              type: 'tool-result',
              toolCallId: 'call_1',
              toolName: 'crm_lookup',
              output: { type: 'text', value: 'MKCALLERORPHAN' },
            },
          ],
        },
        'hi',
      ],
      'MKCALLERORPHAN',
    ],
    [
      'a message reusing a remembered message id with a new timestamp',
      [stored('m1', 10, 'user', [{ type: 'text', text: 'MKCALLERIDCOLLIDE' }])],
      'MKCALLERIDCOLLIDE',
    ],
    [
      'a message reusing a remembered message id with its old timestamp',
      [
        {
          id: 'm1',
          role: 'user',
          createdAt: '2026-01-01T00:00:01.000Z',
          content: {
            format: 2,
            parts: [{ type: 'text', text: 'MKCALLERIDOLD' }],
          },
        },
      ],
      'MKCALLERIDOLD',
    ],
  ];

  it.each(
    MEETS_MEMORY,
  )('denies %s before the model, on generate and stream', async (_label, messages, marker) => {
    for (const method of METHODS) {
      // #when
      const run = await runGuarded(method, messages, {
        policies: [denyPatterns([marker])],
        memory: await threadMemory(),
        call: THREAD,
      });

      // #then
      expect(run.prompts).toEqual([]);
      expect(run.tripwire).toMatch(/deny-patterns/);
    }
  });
});

describe('caller messages the conversion cannot read', () => {
  // Mastra accepts both, and its own conversion throws on the first; the
  // second is a value `structuredClone` cannot copy.
  const UNREADABLE: ReadonlyArray<[string, () => unknown[]]> = [
    [
      'a text part whose text is a number',
      () => [{ role: 'user', content: [{ type: 'text', text: 42 }] }],
    ],
    [
      'a function in provider options',
      () => [
        {
          role: 'user',
          content: [
            {
              type: 'text',
              text: 'hi',
              providerOptions: { host: { callback: () => 'hi' } },
            },
          ],
        },
      ],
    ],
  ];

  it.each(
    UNREADABLE,
  )('aborts on %s with one error event before the model, on generate and stream', async (_label, build) => {
    for (const method of METHODS) {
      // #when
      const run = await runGuarded(method, build(), {
        policies: [denyPatterns(['never-present'])],
      });

      // #then
      expect(run.prompts).toEqual([]);
      expect(run.tripwire).toBe(UNCLASSIFIED);
      expect(run.policyErrors).toBe(1);
    }
  });

  it.each(
    UNREADABLE,
  )('aborts on %s with one error event before the model on the durable agent loop', async (_label, build) => {
    // #given
    const prompts: unknown[] = [];
    const audit = new AuditLogger();
    const handle = createGuardedAgent({
      id: 'writer',
      name: 'Writer',
      instructions: 'Answer the request.',
      model: scriptedModel(prompts),
      allowedRoles: ['operator'],
      policies: [denyPatterns(['never-present'])],
      audit,
      maxSteps: 1,
      toolChoice: 'auto',
    });
    const durable = createDurableAgent({
      agent: handle as unknown as Agent,
      cache: false,
    });

    // #when
    const result = await durable.stream(build() as never, {
      requestContext: actorContext(),
    });
    for await (const _chunk of result.fullStream) {
      // drain
    }

    // #then
    expect(prompts).toEqual([]);
    expect(policyErrorsOf(audit)).toBe(1);
  });
});
