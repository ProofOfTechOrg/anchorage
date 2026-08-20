// SPDX-License-Identifier: Apache-2.0

import { Agent } from '@mastra/core/agent';
import type { MastraModelConfig } from '@mastra/core/llm';
import type {
  ProcessInputArgs,
  ProcessOutputResultArgs,
  ProcessOutputStreamArgs,
} from '@mastra/core/processors';
import { RequestContext } from '@mastra/core/request-context';
import { describe, expect, it, vi } from 'vitest';

import { AGENT_AUDIT_CONTEXT_KEY, AuditLogger } from '../audit/index.js';
import { denyPatterns, type PolicyEvaluator } from '../policy-engine/index.js';
import { ACTOR_CONTEXT_KEY, type PrincipalKind } from '../rbac/index.js';
import {
  createGuardedAgent,
  GUARDED_AGENT_HOST_PROTOCOL,
  type GuardedAgentCallOptions,
  type GuardedAgentConfig,
  type GuardedAgentHandle,
  type GuardedInputProcessor,
  type GuardedOutputProcessor,
  isGuardedAgentHandle,
} from './index.js';

const usage = {
  inputTokens: 1,
  outputTokens: 1,
  totalTokens: 2,
};

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

function testModel(
  text = 'model answer',
  onCall: () => void = () => {},
): MastraModelConfig {
  return {
    specificationVersion: 'v2',
    provider: 'breakwater-test',
    modelId: 'guarded-agent',
    supportedUrls: {},
    doGenerate: async () => {
      onCall();
      return {
        content: [{ type: 'text', text }],
        finishReason: 'stop',
        usage,
        warnings: [],
      };
    },
    doStream: async () => {
      onCall();
      return {
        stream: new ReadableStream({
          start(controller) {
            controller.enqueue({ type: 'stream-start', warnings: [] });
            controller.enqueue({ type: 'text-start', id: 'answer' });
            controller.enqueue({
              type: 'text-delta',
              id: 'answer',
              delta: text,
            });
            controller.enqueue({ type: 'text-end', id: 'answer' });
            controller.enqueue({
              type: 'finish',
              finishReason: 'stop',
              usage,
            });
            controller.close();
          },
        }),
      };
    },
  };
}

function actorContext(
  role: 'admin' | 'builder' | 'operator' | 'reviewer' | 'viewer' = 'operator',
): RequestContext {
  const context = new RequestContext();
  context.set(ACTOR_CONTEXT_KEY, { id: 'actor-1', role });
  context.set(AGENT_AUDIT_CONTEXT_KEY, {
    agentId: 'writer',
    tenantId: 'tenant-1',
    runId: 'run-1',
    threadId: 'thread-1',
    resourceId: 'resource-1',
    entryPath: 'http-start',
    prompt: 'must-not-be-audited',
    channel: 'forged-channel',
  });
  return context;
}

function guarded(
  overrides: Partial<Parameters<typeof createGuardedAgent<'writer'>>[0]> = {},
): GuardedAgentHandle {
  return createGuardedAgent({
    id: 'writer',
    name: 'Writer',
    instructions: 'Answer the request.',
    model: testModel(),
    allowedRoles: ['operator', 'admin'],
    policies: [],
    audit: new AuditLogger(),
    maxSteps: 2,
    toolChoice: 'auto',
    ...overrides,
  });
}

describe('createGuardedAgent direct execution', () => {
  it('authorizes and generates an unstructured result', async () => {
    const modelCall = vi.fn();
    const agent = guarded({ model: testModel('generated', modelCall) });

    const result = await agent.generate('hello', {
      requestContext: actorContext(),
      runId: 'run-1',
    });

    expect(result.text).toBe('generated');
    expect(modelCall).toHaveBeenCalledTimes(1);
  });

  it('authorizes and streams an unstructured result', async () => {
    const modelCall = vi.fn();
    const agent = guarded({ model: testModel('streamed', modelCall) });

    const result = await agent.stream('hello', {
      requestContext: actorContext(),
    });

    await expect(result.text).resolves.toBe('streamed');
    expect(modelCall).toHaveBeenCalledTimes(1);
  });

  it.each([
    ['missing', undefined],
    ['malformed', 42],
    ['whitespace id', { id: '   ', role: 'operator' }],
    ['unknown role', { id: 'actor-1', role: 'owner' }],
    ['disallowed', { id: 'actor-1', role: 'viewer' }],
  ])('denies a %s actor before application processors and model execution', async (_label, actor) => {
    const inputCall = vi.fn();
    const modelCall = vi.fn();
    const input: GuardedInputProcessor = {
      id: 'application-input',
      processInput: (args) => {
        inputCall();
        return args.messages;
      },
    };
    const agent = guarded({
      model: testModel('unreachable', modelCall),
      applicationInputProcessors: [input],
    });
    const context = new RequestContext();
    if (actor !== undefined) context.set(ACTOR_CONTEXT_KEY, actor);

    await expect(
      agent.generate('hello', { requestContext: context }),
    ).rejects.toThrow(/authorization denied/);
    expect(inputCall).not.toHaveBeenCalled();
    expect(modelCall).not.toHaveBeenCalled();
  });

  it('runs direct processors in mandatory order', async () => {
    const order: string[] = [];
    let sawApplicationStream = false;
    const input: GuardedInputProcessor = {
      id: 'application-input',
      processInput: (args) => {
        order.push('application-input');
        return args.messages;
      },
    };
    const output: GuardedOutputProcessor = {
      id: 'application-output',
      processOutputStream: async (args) => {
        if (!sawApplicationStream && args.part.type === 'text-delta') {
          sawApplicationStream = true;
          order.push('application-output-stream');
        }
        return args.part;
      },
      processOutputResult: (args) => {
        order.push('application-output-result');
        return args.messages;
      },
    };
    const policy: PolicyEvaluator = {
      name: 'ordering-policy',
      evaluate: (context) => {
        if (context.phase === 'input') {
          order.push('policy-input');
        } else if (context.streamState) {
          if (!order.includes('policy-output-stream')) {
            order.push('policy-output-stream');
          }
        } else {
          order.push('policy-output-result');
        }
        return { allowed: true };
      },
    };
    const agent = guarded({
      model: testModel('ordered', () => order.push('model')),
      applicationInputProcessors: [input],
      applicationOutputProcessors: [output],
      policies: [policy],
    });

    await agent.generate('hello', {
      requestContext: actorContext(),
    });

    expect(order).toEqual([
      'application-input',
      'policy-input',
      'model',
      'application-output-stream',
      'policy-output-stream',
      'application-output-result',
      'policy-output-result',
    ]);
  });

  it('keeps guarded policy enforcement independent of caller mutations', async () => {
    const phases: Array<'input' | 'output'> = ['output'];
    const channels: Array<'answer' | 'object'> = ['answer'];
    const policy = denyPatterns(['blocked'], { phases, channels });
    const policies = [policy];
    const agent = guarded({
      model: testModel('blocked output'),
      policies,
    });

    policies.length = 0;
    phases[0] = 'input';
    channels[0] = 'object';
    policy.name = 'mutated';
    policy.evaluate = () => ({ allowed: true });

    const result = await agent.generate('hello', {
      requestContext: actorContext(),
    });

    expect(result.finishReason).toBe('other');
    expect(result.tripwire?.reason).toMatch(/deny-patterns/);
  });

  it('preserves a class policy receiver through guarded execution', async () => {
    const policy = new PrivateFieldPolicy('blocked');
    const agent = guarded({
      model: testModel('blocked output'),
      policies: [policy],
    });
    policy.evaluate = () => ({ allowed: true });

    const result = await agent.generate('hello', {
      requestContext: actorContext(),
    });

    expect(result.finishReason).toBe('other');
    expect(result.tripwire?.reason).toMatch(
      /private-field-policy: matched private field/,
    );
  });

  it('withholds denied streamed output before it reaches the consumer', async () => {
    const agent = guarded({
      model: testModel('blocked output'),
      policies: [denyPatterns(['blocked'], { phases: ['output'] })],
    });
    const output = await agent.stream('hello', {
      requestContext: actorContext(),
    });
    const reader = output.fullStream.getReader();
    const visibleText: string[] = [];
    let caught: unknown;

    try {
      for (;;) {
        const item = await reader.read();
        if (item.done) break;
        if (
          item.value.type === 'text-delta' &&
          typeof item.value.payload.text === 'string'
        ) {
          visibleText.push(item.value.payload.text);
        }
      }
    } catch (error) {
      caught = error;
    }

    await expect(output.finishReason).resolves.toBe('other');
    expect(visibleText.join('')).not.toContain('blocked');
    void caught;
  });
});

describe('guarded call-option boundary', () => {
  const unsafeKeys = [
    'inputProcessors',
    'outputProcessors',
    'errorProcessors',
    'toolsets',
    'clientTools',
    'prepareStep',
    'hooks',
    'model',
    'instructions',
    'system',
    'context',
    'onStepFinish',
    'onFinish',
    'onChunk',
    'onError',
    'onAbort',
    'structuredOutput',
    'maxSteps',
    'stopWhen',
    'maxProcessorRetries',
    'toolChoice',
    'activeTools',
    'modelSettings',
    'providerOptions',
    'scorers',
    'isTaskComplete',
    'requireToolApproval',
    'autoResumeSuspendedTools',
    'toolCallConcurrency',
    'delegation',
    'disableBackgroundTasks',
    'untilIdle',
  ] as const;

  it.each(
    unsafeKeys,
  )("rejects unsafe option '%s' even when its value is undefined", async (key) => {
    const modelCall = vi.fn();
    const agent = guarded({ model: testModel('unreachable', modelCall) });
    const options = {
      requestContext: actorContext(),
      [key]: undefined,
    };

    await expect(agent.generate('hello', options)).rejects.toThrow(
      new RegExp(`option '${key}'.*not allowed`),
    );
    expect(modelCall).not.toHaveBeenCalled();
  });

  it('rejects missing options, missing context, symbols, accessors, and non-plain objects', async () => {
    const agent = guarded();
    const symbolOptions = { requestContext: actorContext() };
    Object.defineProperty(symbolOptions, Symbol('unsafe'), { value: true });
    const accessorOptions = {
      get requestContext() {
        return actorContext();
      },
    };

    await expect(
      (
        agent.generate as (
          messages: string,
          options?: unknown,
        ) => Promise<unknown>
      )('hello'),
    ).rejects.toThrow(/options.*requestContext/);
    await expect(
      (
        agent.generate as (
          messages: string,
          options: unknown,
        ) => Promise<unknown>
      )('hello', {}),
    ).rejects.toThrow(/requestContext is required/);
    await expect(
      (
        agent.generate as (
          messages: string,
          options: unknown,
        ) => Promise<unknown>
      )('hello', symbolOptions),
    ).rejects.toThrow(/not allowed/);
    await expect(
      (
        agent.generate as (
          messages: string,
          options: unknown,
        ) => Promise<unknown>
      )('hello', accessorOptions),
    ).rejects.toThrow(/data property/);
    await expect(
      (
        agent.generate as (
          messages: string,
          options: unknown,
        ) => Promise<unknown>
      )('hello', new (class Options {})()),
    ).rejects.toThrow(/plain object/);
  });
});

describe('guarded construction and processor validation', () => {
  it.each([
    'agent',
    'inputProcessors',
    'outputProcessors',
    'errorProcessors',
    'maxProcessorRetries',
    'defaultOptions',
    'defaultGenerateOptionsLegacy',
    'defaultStreamOptionsLegacy',
    'defaultNetworkOptions',
    'backgroundTasks',
    'durable',
    'goal',
    'signals',
    'editor',
    'rawConfig',
  ])("rejects unsafe construction option '%s'", (key) => {
    expect(() => guarded({ [key]: undefined } as never)).toThrow(
      new RegExp(`option '${key}'.*not allowed`),
    );
  });

  it('validates roles, step budgets, and fixed tool choice', () => {
    expect(() => guarded({ allowedRoles: [] })).toThrow(/non-empty/);
    expect(() => guarded({ allowedRoles: ['operator', 'operator'] })).toThrow(
      /duplicate/,
    );
    expect(() => guarded({ allowedRoles: ['owner'] as never })).toThrow(
      /unknown allowed role/,
    );
    expect(() => guarded({ maxSteps: 0 })).toThrow(/positive/);
    expect(() =>
      guarded({ toolChoice: { type: 'tool', toolName: '' } }),
    ).toThrow(/toolChoice/);
  });

  it.each([
    'processInputStep',
    'computeStateSignal',
    'processLLMRequest',
    'processLLMResponse',
    'processOutputStream',
    'processOutputResult',
    'processOutputStep',
    'processAPIError',
  ])('rejects input processor hook %s', (hook) => {
    const processor = {
      id: `input-${hook}`,
      processInput: (args: ProcessInputArgs) => args.messages,
      [hook]: () => undefined,
    };

    expect(() =>
      guarded({
        applicationInputProcessors: [processor as never],
      }),
    ).toThrow(new RegExp(`must not implement ${hook}`));
  });

  it('requires both output enforcement hooks', () => {
    expect(() =>
      guarded({
        applicationOutputProcessors: [
          {
            id: 'stream-only',
            processOutputStream: (args: ProcessOutputStreamArgs) => args.part,
          } as never,
        ],
      }),
    ).toThrow(/must implement processOutputResult/);
    expect(() =>
      guarded({
        applicationOutputProcessors: [
          {
            id: 'result-only',
            processOutputResult: (args: ProcessOutputResultArgs) =>
              args.messages,
          } as never,
        ],
      }),
    ).toThrow(/must implement processOutputStream/);
  });

  it.each([
    'breakwater-rbac',
    'breakwater-policy-engine',
  ])("rejects reserved application processor id '%s'", (id) => {
    expect(() =>
      guarded({
        applicationInputProcessors: [
          {
            id,
            processInput: (args: ProcessInputArgs) => args.messages,
          },
        ],
      }),
    ).toThrow(/processor id.*reserved/);
  });

  it('rejects processor workflows instead of treating them as application processors', () => {
    expect(() =>
      guarded({
        applicationInputProcessors: [{ id: 'workflow', steps: [] } as never],
      }),
    ).toThrow(/must implement processInput/);
  });
});

describe('guarded durable interop and brand', () => {
  it('lists mandatory processors around application processors', async () => {
    const input: GuardedInputProcessor = {
      id: 'application-input',
      processInput: (args) => args.messages,
    };
    const output: GuardedOutputProcessor = {
      id: 'application-output',
      processOutputStream: async (args) => args.part,
      processOutputResult: (args) => args.messages,
    };
    const handle = guarded({
      applicationInputProcessors: [input],
      applicationOutputProcessors: [output],
    });
    const raw = handle as unknown as Agent;

    await expect(
      raw.listInputProcessors(actorContext()),
    ).resolves.toMatchObject([
      { id: 'breakwater-rbac' },
      { id: 'application-input' },
      { id: 'breakwater-policy-engine' },
    ]);
    await expect(
      raw.listOutputProcessors(actorContext()),
    ).resolves.toMatchObject([
      { id: 'application-output' },
      { id: 'breakwater-policy-engine' },
    ]);
  });

  it('uses an unforgeable package-local brand', () => {
    const handle = guarded();
    const forged = {
      id: handle.id,
      allowedRoles: handle.allowedRoles,
      maxSteps: handle.maxSteps,
      generate: handle.generate,
      stream: handle.stream,
    };

    expect(isGuardedAgentHandle(handle)).toBe(true);
    expect(isGuardedAgentHandle(forged)).toBe(false);
    expect(
      isGuardedAgentHandle(
        new Agent({
          id: 'raw',
          name: 'Raw',
          instructions: 'Raw',
          model: testModel(),
        }),
      ),
    ).toBe(false);
  });
});

describe('guarded audit behavior', () => {
  it('uses the agent resource and safe correlation on authorization denial', async () => {
    const audit = new AuditLogger();
    const agent = guarded({ audit });

    await expect(
      agent.generate('hello', {
        requestContext: actorContext('viewer'),
      }),
    ).rejects.toThrow(/authorization denied/);

    expect(audit.events()).toMatchObject([
      {
        resource: 'agent:writer',
        decision: 'denied',
        detail: {
          agentId: 'writer',
          tenantId: 'tenant-1',
          entryPath: 'http-start',
        },
      },
    ]);
  });

  it('uses the agent resource and safe correlation on allow and policy denial', async () => {
    const audit = new AuditLogger();
    const agent = guarded({
      policies: [denyPatterns(['deny me'])],
      audit,
    });

    const result = await agent.generate('deny me', {
      requestContext: actorContext(),
    });

    expect(result.finishReason).toBe('other');
    expect(result.tripwire?.reason).toMatch(/deny-patterns/);
    expect(audit.events()).toHaveLength(2);
    for (const event of audit.events()) {
      expect(event.resource).toBe('agent:writer');
      expect(event.detail).toMatchObject({
        agentId: 'writer',
        tenantId: 'tenant-1',
        runId: 'run-1',
        threadId: 'thread-1',
        resourceId: 'resource-1',
        entryPath: 'http-start',
      });
    }
    expect(audit.events()[1]?.detail).toMatchObject({
      policy: 'deny-patterns',
      channel: 'answer',
    });
    expect(JSON.stringify(audit.events())).not.toContain('must-not-be-audited');
    expect(JSON.stringify(audit.events())).not.toContain('forged-channel');
  });

  it('records safe correlation on gate error and contains audit sink failure', async () => {
    const sinkError = vi.fn();
    const audit = new AuditLogger({
      sink: () => {
        throw new Error('sink unavailable');
      },
      onSinkError: sinkError,
    });
    const agent = guarded({
      policies: [
        {
          name: 'crashing-policy',
          evaluate: () => {
            throw new Error('private evaluator failure');
          },
        },
      ],
      audit,
    });

    await expect(
      agent.generate('hello', {
        requestContext: actorContext(),
      }),
    ).rejects.toThrow(/Input processor error/);

    expect(sinkError).toHaveBeenCalledTimes(2);
    expect(audit.events()[1]).toMatchObject({
      resource: 'agent:writer',
      decision: 'error',
      reason: 'policy evaluation failed',
      detail: {
        agentId: 'writer',
        entryPath: 'http-start',
        policy: 'crashing-policy',
        channel: 'answer',
      },
    });
    expect(JSON.stringify(audit.events())).not.toContain(
      'private evaluator failure',
    );
  });
});

describe('createGuardedAgent principal kinds', () => {
  function automatedContext(
    kind: 'service' | 'agent' | 'system',
    role: 'admin' | 'operator' | 'viewer' = 'operator',
  ): RequestContext {
    const context = new RequestContext();
    context.set(ACTOR_CONTEXT_KEY, { id: 'scheduler-1', role, kind });
    context.set(AGENT_AUDIT_CONTEXT_KEY, {
      agentId: 'writer',
      entryPath: 'schedule.fire',
      principalKind: kind,
      principalId: 'scheduler-1',
      purpose: 'scheduled-agent-execution',
    });
    return context;
  }

  it('defaults to humans only, so an existing agent denies automation', async () => {
    // #given — `guarded()` names allowedRoles and nothing else, exactly as
    // every agent written before principal kinds existed.
    const modelCall = vi.fn();
    const agent = guarded({ model: testModel('generated', modelCall) });

    // #when / #then — 'operator' IS an allowed role; only the kind stops it.
    await expect(
      agent.generate('hello', { requestContext: automatedContext('system') }),
    ).rejects.toThrow(
      /principal kind 'system' is not in allowed kinds \[human\]/,
    );
    expect(modelCall).not.toHaveBeenCalled();
    expect(agent.allowedPrincipalKinds).toEqual(['human']);
  });

  it.each([
    'generate',
    'stream',
  ] as const)('denies an unnamed kind on the direct %s path, not just in the processor chain', async (method) => {
    // #given — the direct entries pre-authorize OUTSIDE the processor chain,
    // so a kind gate wired only into RBACMiddleware would leave them open.
    const modelCall = vi.fn();
    const agent = guarded({
      model: testModel('generated', modelCall),
      allowedPrincipalKinds: ['human', 'service'],
    });

    // #when / #then
    await expect(
      agent[method]('hello', { requestContext: automatedContext('agent') }),
    ).rejects.toThrow(/principal kind 'agent' is not in allowed kinds/);
    expect(modelCall).not.toHaveBeenCalled();
  });

  it('runs an automated principal whose kind is named, ignoring its role', async () => {
    // #given — 'viewer' is deliberately outside allowedRoles: an automated
    // principal must not need a human role to be admitted, because needing one
    // would also admit the humans who hold it.
    const modelCall = vi.fn();
    const agent = guarded({
      model: testModel('generated', modelCall),
      allowedRoles: ['admin'],
      allowedPrincipalKinds: ['system'],
    });

    // #when
    const result = await agent.generate('hello', {
      requestContext: automatedContext('system', 'viewer'),
    });

    // #then
    expect(result.text).toBe('generated');
    expect(modelCall).toHaveBeenCalledTimes(1);
  });

  it('keeps the human role gate intact once automation is enabled', async () => {
    // #given
    const modelCall = vi.fn();
    const agent = guarded({
      model: testModel('generated', modelCall),
      allowedRoles: ['admin'],
      allowedPrincipalKinds: ['human', 'system'],
    });

    // #when / #then — a real human operator is still refused.
    await expect(
      agent.generate('hello', { requestContext: actorContext('operator') }),
    ).rejects.toThrow(/role 'operator' is not in allowed roles \[admin\]/);
    expect(modelCall).not.toHaveBeenCalled();
  });

  it('carries principal correlation into the authorization audit event', async () => {
    // #given
    const audit = new AuditLogger();
    const agent = guarded({
      audit,
      allowedPrincipalKinds: ['system'],
    });

    // #when
    await agent.generate('hello', {
      requestContext: automatedContext('system'),
    });

    // #then — provenance a fabricated operator could never have carried.
    expect(audit.events()[0]).toMatchObject({
      decision: 'allowed',
      actor: { id: 'scheduler-1', kind: 'system' },
      detail: {
        entryPath: 'schedule.fire',
        principalKind: 'system',
        principalId: 'scheduler-1',
        purpose: 'scheduled-agent-execution',
      },
    });
  });

  it('rejects an invalid kind allowlist at construction', () => {
    // #when / #then
    expect(() => guarded({ allowedPrincipalKinds: [] })).toThrowError(
      /allowedPrincipalKinds must be a non-empty array/,
    );
    expect(() =>
      guarded({ allowedPrincipalKinds: ['root' as PrincipalKind] }),
    ).toThrowError(/unknown principal kind 'root'/);
  });
});

describe('Mastra Agent execution-entry inventory', () => {
  it('requires every own prototype property to remain classified', () => {
    const wrapped = ['generate', 'stream'];
    const intentionallyUnavailable = [
      '__runInputProcessors',
      '__runOutputProcessors',
      '__runProcessInputStep',
      'abortRunStream',
      'abortThreadStream',
      'approveNetworkToolCall',
      'approveToolCall',
      'approveToolCallGenerate',
      'declineNetworkToolCall',
      'declineToolCall',
      'declineToolCallGenerate',
      'genTitle',
      'generateLegacy',
      'generateTitleFromUserMessage',
      'network',
      'prepare',
      'queueMessage',
      'recover',
      'recoverActiveRuns',
      'resume',
      'resumeGenerate',
      'resumeNetwork',
      'resumeStream',
      'resumeStreamUntilIdle',
      'sendMessage',
      'sendNotificationSignal',
      'sendSignal',
      'sendStateSignal',
      'sendStreamResume',
      'sendToolApproval',
      'streamLegacy',
      'streamUntilIdle',
    ];
    const explicitlyNonExecution = [
      '__fork',
      '__getDrainPendingSignals',
      '__getEditorConfig',
      '__getGoalConfig',
      '__getOverridableFields',
      '__getStaticAgents',
      '__hasSubAgentsConfigured',
      '__listLLMRequestProcessors',
      '__markStoredVersionApplied',
      '__registerMastra',
      '__registerPrimitives',
      '__resetToOriginalModel',
      // Declarative schedule metadata accessors; neither starts a scheduled run.
      '__setDeclaredSchedules',
      '__setMemory',
      '__setPubSub',
      '__setTools',
      '__setWorkspace',
      '__updateInstructions',
      '__updateModel',
      'assertSupportsPreparedModels',
      'agent',
      'browser',
      'clearObjective',
      'combineProcessorsIntoWorkflow',
      'constructor',
      'convertTools',
      'deriveSubAgentBackgroundConfig',
      'disableBackgroundTasks',
      'durable',
      'enableBackgroundTasks',
      // Pure title-generation prefilter; it cannot initiate agent execution.
      'filterUiMessagesByThread',
      'formatMessagePartsForTitle',
      'formatMessagesForTitle',
      'formatTools',
      'getActiveThreadRunId',
      'getBackgroundTasksConfig',
      'getChannels',
      'getConfiguredProcessorIds',
      'getConfiguredProcessorWorkflows',
      'getConfiguredToolHooks',
      'getDeclaredSchedules',
      'getDefaultGenerateOptionsLegacy',
      'getDefaultNetworkOptions',
      'getDefaultOptions',
      'getDefaultStreamOptionsLegacy',
      'getDescription',
      'getInstructions',
      'getLegacyHandler',
      'getLLM',
      'getMastraInstance',
      'getMcpServerGuidance',
      'getMemory',
      'getMemoryMessages',
      'getMetadata',
      'getModel',
      'getModelList',
      'getMostRecentUserMessage',
      'getObjective',
      'getProcessorRunner',
      'getPubSub',
      'getSkill',
      'getSkillsProcessors',
      'getSubAgentToolSchemas',
      'getToolPayloadTransform',
      'getToolsForExecution',
      'getTracingPolicy',
      'getVoice',
      'getWorkspace',
      'getWorkspaceInstructionsProcessors',
      'hasOwnBrowser',
      'hasOwnMemory',
      'hasOwnPubSub',
      'hasOwnWorkspace',
      'isModelFallbacks',
      'listActiveRuns',
      'listActiveThreadRuns',
      'listAgents',
      'listAgentTools',
      'listAssignedTools',
      'listBrowserTools',
      'listChannelTools',
      'listClientTools',
      'listConfiguredInputProcessors',
      'listConfiguredOutputProcessors',
      'listErrorProcessors',
      'listInputProcessorLoadedTools',
      'listInputProcessors',
      'listMemoryTools',
      'listOutputProcessors',
      'listResolvedInputProcessors',
      'listResolvedLLMRequestProcessors',
      'listResolvedOutputProcessors',
      'listScorers',
      'listSkills',
      'listSkillTools',
      'listSuspendedRuns',
      'listTools',
      'listToolsets',
      'listWorkflowTools',
      'listWorkflows',
      'listWorkspaceTools',
      'normalizeModelFallbacks',
      'observe',
      'prepareModels',
      'reorderModels',
      'requestContextSchema',
      'requireAgentExecutionFGA',
      'resolveFallbackDynamic',
      'resolveInputProcessors',
      'resolveModelConfig',
      'resolveModelSelection',
      // May call the policy decider, but cannot send a signal or start execution.
      'resolveNotificationDeliveryDecision',
      'resolveOverrideScorerReferences',
      'resolveProcessorById',
      'resolveSkills',
      'resolveTitleGenerationConfig',
      'resolveTitleInstructions',
      'resolveToolHooks',
      'setBrowser',
      'setChannels',
      'setObjective',
      'stripParentToolParts',
      'subscribeToThread',
      'updateModelInModelList',
      'updateObjectiveOptions',
      'voice',
      'wrapToolsWithHooks',
      'wrapToolWithHooks',
    ];
    const classified = [
      ...wrapped,
      ...intentionallyUnavailable,
      ...explicitlyNonExecution,
    ];

    expect(new Set(classified).size).toBe(classified.length);
    const unclassified = Object.getOwnPropertyNames(Agent.prototype).filter(
      (property) => !classified.includes(property),
    );
    expect(unclassified).toEqual([]);
  });
});

describe('createGuardedAgent structured output boundary', () => {
  it.each([
    'generate',
    'stream',
  ] as const)('refuses structuredOutput on %s before model execution', async (method) => {
    const modelCall = vi.fn();
    const agent = guarded({ model: testModel('unreachable', modelCall) });
    const options = {
      requestContext: actorContext(),
      structuredOutput: { schema: {} },
    };

    await expect(
      (
        agent[method] as (
          messages: string,
          callOptions: unknown,
        ) => Promise<unknown>
      )('hello', options),
    ).rejects.toThrowError(/structuredOutput.*not allowed/);
    expect(modelCall).not.toHaveBeenCalled();
  });

  it('rejects object-only policies at construction', () => {
    expect(() =>
      guarded({
        policies: [denyPatterns(['blocked'], { channels: ['object'] })],
      }),
    ).toThrowError(/object-only policy.*cannot be enforced/is);
  });
});

function compileTimeSurface(
  handle: GuardedAgentHandle,
  requestContext: RequestContext,
): void {
  if (Date.now() < 0) {
    void handle.generate('hello', { requestContext });
    const options: GuardedAgentCallOptions = { requestContext };
    void handle.generate('hello', options);
    void handle.stream('hello', options);
    // @ts-expect-error Structured output is intentionally unavailable.
    void handle.generate('hello', { requestContext, structuredOutput: {} });
    // @ts-expect-error Structured output is intentionally unavailable.
    void handle.stream('hello', { requestContext, structuredOutput: {} });
    void handle[GUARDED_AGENT_HOST_PROTOCOL].supportsDurableStructuredOutput;
    // @ts-expect-error Raw resume is intentionally unavailable.
    void handle.resumeStream({}, { requestContext });
    // @ts-expect-error Standalone durable resume is intentionally unavailable.
    void handle.resume('run-1', {});
    // @ts-expect-error Legacy execution is intentionally unavailable.
    void handle.generateLegacy('hello');
    // @ts-expect-error Network execution is intentionally unavailable.
    void handle.network('hello');
  }
}

function compileTimeConstructionSurface(
  config: GuardedAgentConfig,
): GuardedAgentConfig {
  if (Date.now() < 0) {
    // @ts-expect-error Goal-driven continuation is intentionally unavailable.
    return { ...config, goal: undefined };
  }
  return config;
}

void compileTimeSurface;
void compileTimeConstructionSurface;
