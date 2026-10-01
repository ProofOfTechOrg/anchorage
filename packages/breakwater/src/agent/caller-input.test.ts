// SPDX-License-Identifier: Apache-2.0

import { Agent } from '@mastra/core/agent';
import { createDurableAgent } from '@mastra/core/agent/durable';
import type { MastraToolInvocation } from '@mastra/core/agent/message-list';
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

async function runRaw(
  method: Method,
  messages: unknown,
  settings: {
    markers: readonly string[];
    memory?: MockMemory;
    call?: Record<string, unknown>;
  },
): Promise<Pick<Run, 'prompts' | 'tripwire'>> {
  const prompts: unknown[] = [];
  const agent = new Agent({
    id: 'raw',
    name: 'Raw',
    instructions: 'Answer the request.',
    model: scriptedModel(prompts),
    ...(settings.memory ? { memory: settings.memory } : {}),
    inputProcessors: [
      new PolicyEngine({ policies: [denyPatterns(settings.markers)] }),
    ],
  });
  const tripwire = await drive(
    agent as unknown as Target,
    method,
    messages,
    settings.call ?? {},
  );
  return { prompts, tripwire };
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

function clientToolOutcome(
  state: MastraToolInvocation['state'],
  fields: Record<string, unknown> = {},
) {
  return {
    type: 'tool-invocation',
    toolInvocation: {
      toolCallId: 'call_1',
      toolName: 'crm_lookup',
      args: { account: 'acme' },
      state,
      ...fields,
    },
  };
}

function pendingClientCall(approval = false) {
  return clientToolOutcome(
    approval ? 'approval-requested' : 'call',
    approval ? { approval: APPROVAL } : {},
  );
}

function assistantMessage(parts: unknown[], role = 'assistant') {
  return stored('m2', 2, role, parts);
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

async function threadMemory(retainFullInput = false): Promise<MockMemory> {
  const memory = new MockMemory({
    storage: new InMemoryStore(),
    options: { retainFullInput },
  });
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

async function collectMemoryOutcomes(
  marker: string,
  run: (
    method: Method,
    memory: MockMemory,
  ) => Promise<Pick<Run, 'prompts' | 'tripwire'>>,
) {
  const outcomes = [];
  for (const method of METHODS) {
    const memory = await threadMemory();
    const { prompts, tripwire } = await run(method, memory);
    const recalled = await memory.recall({ threadId: 't1', resourceId: 'r1' });
    expect(prompts.length).toBeLessThanOrEqual(1);
    outcomes.push({
      method,
      tripwire,
      sent: JSON.stringify(prompts).includes(marker),
      saved: JSON.stringify(recalled.messages).includes(marker),
    });
  }
  return outcomes;
}

const THREAD = { memory: { thread: 't1', resource: 'r1' } } as const;

const OUTCOME_MARKER = 'MKCLIENTOUTCOME';
const CLEAN_OUTCOME = 'clean client result';
const APPROVAL = { id: 'approval_1' };

async function outcomeMemory({
  approval = false,
  storedResult,
  retainFullInput = false,
}: {
  approval?: boolean;
  storedResult?: ReturnType<typeof clientToolOutcome>;
  retainFullInput?: boolean;
} = {}) {
  const memory = await threadMemory(retainFullInput);
  await memory.saveMessages({
    messages: [
      {
        ...assistantMessage([
          ...(storedResult ? [storedResult] : []),
          pendingClientCall(approval),
        ]),
        createdAt: new Date(isoAt(2)),
        threadId: 't1',
        resourceId: 'r1',
        type: 'tool-call',
      } as never,
    ],
  });
  return memory;
}

async function savedClientCall(memory: MockMemory) {
  const { messages } = await memory.recall({
    threadId: 't1',
    resourceId: 'r1',
  });
  const savedPart = messages
    .find(({ id }) => id === 'm2')
    ?.content.parts.find(
      (part) =>
        part.type === 'tool-invocation' &&
        part.toolInvocation.toolCallId === 'call_1',
    );
  return savedPart?.type === 'tool-invocation'
    ? savedPart.toolInvocation
    : undefined;
}

function clientTools({ mapped = false }: { mapped?: boolean } = {}) {
  return {
    crm_lookup: createTool({
      id: 'crm_lookup',
      description: 'Look up an account on the client',
      inputSchema: z.object({ account: z.string() }),
      ...(mapped
        ? { toModelOutput: () => ({ type: 'text', value: OUTCOME_MARKER }) }
        : {}),
    } as never),
  };
}

describe('client tool outcomes merged into memory', () => {
  type OutcomeCase = {
    carries: string;
    parts?: unknown[];
    approval?: boolean;
    ui?: boolean;
    role?: string;
    tail?: boolean;
    fullHistory?: boolean;
    mapped?: boolean;
  };
  const DENIED: OutcomeCase[] = [
    {
      carries: 'a DB result',
      parts: [clientToolOutcome('result', { result: OUTCOME_MARKER })],
    },
    {
      carries: 'an output-error errorText',
      parts: [clientToolOutcome('output-error', { errorText: OUTCOME_MARKER })],
    },
    {
      carries: 'an output-denied approval reason',
      approval: true,
      parts: [
        clientToolOutcome('output-denied', {
          approval: { ...APPROVAL, approved: false, reason: OUTCOME_MARKER },
        }),
      ],
    },
    {
      carries: 'a result beside unrelated errorText and approval',
      parts: [
        clientToolOutcome('result', {
          result: OUTCOME_MARKER,
          errorText: 'unrelated',
          approval: { id: 'unrelated', approved: true },
        }),
      ],
    },
    {
      carries: 'a UI output-available with approval',
      approval: true,
      ui: true,
    },
    {
      carries: 'a user-role client result',
      role: 'user',
      parts: [clientToolOutcome('result', { result: OUTCOME_MARKER })],
    },
    {
      carries: 'a client result before a user tail',
      tail: true,
      parts: [clientToolOutcome('result', { result: OUTCOME_MARKER })],
    },
    {
      carries: 'a client result in retained full history',
      fullHistory: true,
      parts: [clientToolOutcome('result', { result: OUTCOME_MARKER })],
    },
    {
      carries: 'a clean result mapped to denied text',
      mapped: true,
      parts: [clientToolOutcome('result', { result: CLEAN_OUTCOME })],
    },
  ];

  it.each(
    DENIED.flatMap((row) => METHODS.map((method) => ({ ...row, method }))),
  )('denies $carries before the model and leaves the stored call pending on $method', async (row) => {
    const memory = await outcomeMemory({
      approval: row.approval,
      retainFullInput: row.fullHistory,
    });
    const message = row.ui
      ? {
          id: 'm2',
          role: 'assistant',
          parts: [
            {
              type: 'tool-crm_lookup',
              toolCallId: 'call_1',
              state: 'output-available',
              input: { account: 'acme' },
              output: OUTCOME_MARKER,
              approval: { ...APPROVAL, approved: true },
            },
          ],
        }
      : assistantMessage(row.parts ?? [], row.role);
    const history = row.fullHistory
      ? (
          await memory.recall({ threadId: 't1', resourceId: 'r1' })
        ).messages.filter(({ id }) => id !== 'm2')
      : [];
    const run = await runGuarded(
      row.method,
      [...history, message, ...(row.tail ? ['next'] : [])],
      {
        policies: [denyPatterns([OUTCOME_MARKER])],
        memory,
        tools: clientTools({ mapped: row.mapped }),
        call: THREAD,
      },
    );
    expect(run.tripwire).toMatch(/^deny-patterns:/);
    expect(run.prompts).toEqual([]);
    expect(await savedClientCall(memory)).toEqual(
      pendingClientCall(row.approval).toolInvocation,
    );
  });

  it.each(
    METHODS,
  )('refuses duplicate client tool outcomes before the model and leaves the stored call pending on %s', async (method) => {
    const memory = await outcomeMemory();
    const run = await runGuarded(
      method,
      [
        assistantMessage([
          clientToolOutcome('result', { result: CLEAN_OUTCOME }),
          clientToolOutcome('output-error', { errorText: 'failed' }),
        ]),
      ],
      {
        policies: [denyPatterns([OUTCOME_MARKER])],
        memory,
        tools: clientTools(),
        call: THREAD,
      },
    );
    expect(run.tripwire).toBe('input processor failed');
    expect(run.prompts).toEqual([]);
    expect(await savedClientCall(memory)).toEqual(
      pendingClientCall().toolInvocation,
    );
  });

  it.each(
    METHODS,
  )('allows a clean caller result beside an unsent stored denied result and saves the client tool outcome on %s', async (method) => {
    const memory = await outcomeMemory({
      storedResult: clientToolOutcome('result', {
        toolCallId: 'call_0',
        toolName: 'historical_lookup',
        args: {},
        result: OUTCOME_MARKER,
      }),
    });
    const run = await runGuarded(
      method,
      [
        assistantMessage([
          clientToolOutcome('result', { result: CLEAN_OUTCOME }),
        ]),
      ],
      {
        policies: [denyPatterns([OUTCOME_MARKER])],
        memory,
        tools: {
          ...clientTools(),
          historical_lookup: createTool({
            id: 'historical_lookup',
            description: 'Look up historical data on the client',
            inputSchema: z.object({}),
            toModelOutput: () => {
              throw new Error('unsent stored-result mapper called');
            },
          } as never),
        },
        call: THREAD,
      },
    );
    expect(
      run.tripwire,
      'unsent stored-result mapper throws if selected',
    ).toBeUndefined();
    expect(run.prompts).toHaveLength(1);
    expect(JSON.stringify(run.prompts[0])).toContain(CLEAN_OUTCOME);
    expect(JSON.stringify(run.prompts[0])).toContain(OUTCOME_MARKER);
    expect(await savedClientCall(memory)).toMatchObject({
      state: 'result',
      result: CLEAN_OUTCOME,
    });
  });
});

describe('stored model outputs a caller message carries', () => {
  it.each(
    CARRIERS,
  )('denies an output stored in %s for a pending call, which Mastra gives the placeholder result, on a raw agent', async (_label, carrier) => {
    for (const method of METHODS) {
      // #when
      const { prompts, tripwire } = await runRaw(
        method,
        [carrier('c9'), pendingCall('c9'), 'go'],
        {
          markers: ['MKSTOREDOUTPUT'],
          call: { memory: { options: { filterIncompleteToolCalls: false } } },
        },
      );

      // #then
      expect(prompts).toEqual([]);
      expect(tripwire).toMatch(/deny-patterns/);
    }
  });

  it('keeps a caller stored output out of prompts and saved memory without a tripwire on a raw agent with thread memory', async () => {
    const outcomes = await collectMemoryOutcomes(
      'MKSTOREDOUTPUT',
      (method, memory) =>
        runRaw(method, [userCarrier('c9'), pendingCall('c9'), 'go'], {
          markers: ['MKSTOREDOUTPUT'],
          memory,
          call: {
            memory: {
              thread: 't1',
              resource: 'r1',
              options: { filterIncompleteToolCalls: false },
            },
          },
        }),
    );
    expect(outcomes).toEqual(
      METHODS.map((method) => ({
        method,
        tripwire: undefined,
        sent: false,
        saved: false,
      })),
    );
  });

  it.each(
    METHODS,
  )('denies a client tool outcome merged into remembered input before the model on a raw agent with thread memory on %s', async (method) => {
    const memory = await outcomeMemory();
    const { prompts, tripwire } = await runRaw(
      method,
      [
        {
          id: 'm2',
          role: 'assistant',
          parts: [clientToolOutcome('result', { result: OUTCOME_MARKER })],
        },
        { role: 'user', content: 'go' },
      ],
      { markers: [OUTCOME_MARKER], memory, call: THREAD },
    );

    expect(tripwire).toMatch(/^deny-patterns:/);
    expect(prompts).toEqual([]);
    expect(await savedClientCall(memory)).toEqual(
      pendingClientCall().toolInvocation,
    );
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

  const DROPPED_MEMORY: ReadonlyArray<[string, unknown[], string]> = [
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
    DROPPED_MEMORY,
  )('keeps %s out of prompts and saved memory without a tripwire, on generate and stream', async (_label, messages, marker) => {
    const outcomes = await collectMemoryOutcomes(
      marker,
      async (method, memory) => {
        const run = await runGuarded(method, messages, {
          policies: [denyPatterns([marker])],
          memory,
          call: THREAD,
        });
        expect(run.policyErrors).toBe(0);
        return run;
      },
    );
    expect(outcomes).toEqual(
      METHODS.map((method) => ({
        method,
        tripwire: undefined,
        sent: false,
        saved: false,
      })),
    );
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
