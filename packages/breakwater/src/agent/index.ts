// SPDX-License-Identifier: Apache-2.0

import {
  Agent,
  type AgentConfig,
  type AgentExecutionOptionsBase,
  type AgentMemoryOption,
  type ToolsInput,
} from '@mastra/core/agent';
import type { MessageListInput } from '@mastra/core/agent/message-list';
import type {
  InputProcessorOrWorkflow,
  OutputProcessorOrWorkflow,
  Processor,
} from '@mastra/core/processors';
import { RequestContext } from '@mastra/core/request-context';
import type { FullOutput, MastraModelOutput } from '@mastra/core/stream';

import type { AuditLogger } from '../audit/index.js';
import { PolicyEngine, type PolicyEvaluator } from '../policy-engine/index.js';
import { authorizeActor } from '../rbac/authorize.js';
import {
  actorFromRequestContext,
  RBACMiddleware,
  ROLES,
  type Role,
} from '../rbac/index.js';
import { assertPrincipalKinds, type PrincipalKind } from '../rbac/principal.js';

const RESERVED_PROCESSOR_IDS = new Set([
  'breakwater-rbac',
  'breakwater-policy-engine',
]);

const GUARDED_CALL_OPTION_KEYS = new Set([
  'requestContext',
  'runId',
  'memory',
  'abortSignal',
]);

/** Well-known inter-package key for guarded host compatibility metadata. */
export const GUARDED_AGENT_HOST_PROTOCOL = Symbol.for(
  '@proofoftech/breakwater/guarded-agent-host/v1',
);

/** Runtime metadata consumed by hosts that cannot call the narrow handle. */
export interface GuardedAgentHostProtocol {
  readonly version: 1;
  readonly supportsDurableStructuredOutput: false;
}

const UNSAFE_CONSTRUCTION_KEYS = new Set([
  'agent',
  'inputProcessors',
  'outputProcessors',
  'errorProcessors',
  'maxProcessorRetries',
  'defaultGenerateOptionsLegacy',
  'defaultStreamOptionsLegacy',
  'defaultOptions',
  'defaultNetworkOptions',
  'backgroundTasks',
  'durable',
  'goal',
  'signals',
  'editor',
  'rawConfig',
]);

const INPUT_PROCESSOR_FORBIDDEN_HOOKS = [
  'processInputStep',
  'computeStateSignal',
  'processLLMRequest',
  'processLLMResponse',
  'processOutputStream',
  'processOutputResult',
  'processOutputStep',
  'processAPIError',
] as const;

const OUTPUT_PROCESSOR_FORBIDDEN_HOOKS = [
  'processInput',
  'processInputStep',
  'computeStateSignal',
  'processLLMRequest',
  'processLLMResponse',
  'processOutputStep',
  'processAPIError',
] as const;

type ProcessorHook = Extract<
  keyof Processor,
  | 'processInput'
  | 'processInputStep'
  | 'computeStateSignal'
  | 'processLLMRequest'
  | 'processLLMResponse'
  | 'processOutputStream'
  | 'processOutputResult'
  | 'processOutputStep'
  | 'processAPIError'
>;

/**
 * Application input processor accepted by {@link createGuardedAgent}.
 *
 * It can transform or reject the initial input only. Per-step, provider,
 * output, and error hooks are unavailable because they can mutate execution
 * after the mandatory input gates have run.
 */
export interface GuardedInputProcessor {
  readonly id: string;
  readonly name?: string;
  readonly description?: string;
  processDataParts?: boolean;
  onViolation?: Processor['onViolation'];
  processInput: NonNullable<Processor['processInput']>;
  processInputStep?: never;
  computeStateSignal?: never;
  processLLMRequest?: never;
  processLLMResponse?: never;
  processOutputStream?: never;
  processOutputResult?: never;
  processOutputStep?: never;
  processAPIError?: never;
}

/**
 * Application output processor accepted by {@link createGuardedAgent}.
 *
 * Both stream and final-result hooks are required so the processor enforces
 * the same rule on `stream()` and `generate()`.
 */
export interface GuardedOutputProcessor {
  readonly id: string;
  readonly name?: string;
  readonly description?: string;
  processDataParts?: boolean;
  onViolation?: Processor['onViolation'];
  processInput?: never;
  processInputStep?: never;
  computeStateSignal?: never;
  processLLMRequest?: never;
  processLLMResponse?: never;
  processOutputStream: NonNullable<Processor['processOutputStream']>;
  processOutputResult: NonNullable<Processor['processOutputResult']>;
  processOutputStep?: never;
  processAPIError?: never;
}

/** Fixed tool-selection behavior for every guarded execution. */
export type GuardedToolChoice = NonNullable<
  AgentExecutionOptionsBase<undefined>['toolChoice']
>;

/**
 * Construction-time configuration for a guarded Mastra agent.
 *
 * Processor and execution defaults are replaced by dedicated validated
 * fields. The factory never accepts an existing raw `Agent`.
 */
export type GuardedAgentConfig<
  TAgentId extends string = string,
  TTools extends ToolsInput = ToolsInput,
  TRequestContext extends Record<string, unknown> | unknown = unknown,
> = Omit<
  AgentConfig<TAgentId, TTools, undefined, TRequestContext, false>,
  | 'inputProcessors'
  | 'outputProcessors'
  | 'errorProcessors'
  | 'maxProcessorRetries'
  | 'defaultGenerateOptionsLegacy'
  | 'defaultStreamOptionsLegacy'
  | 'defaultOptions'
  | 'defaultNetworkOptions'
  | 'backgroundTasks'
  | 'durable'
  | 'goal'
  | 'signals'
  | 'editor'
  | 'rawConfig'
> & {
  /** Exact actor roles authorized for direct and durable execution. */
  allowedRoles: readonly Role[];
  /**
   * Exact principal kinds authorized for direct and durable execution.
   * Defaults to `['human']`: an agent that does not name its automation denies
   * every scheduled, signal, service, and agent-delegated entry.
   */
  allowedPrincipalKinds?: readonly PrincipalKind[];
  /** Mandatory input and output policies, evaluated in array order. */
  policies: readonly PolicyEvaluator[];
  /** Required failure-isolated audit logger for every mandatory gate. */
  audit: AuditLogger;
  /** Fixed positive step budget for every execution. */
  maxSteps: number;
  /** Fixed tool-selection behavior for every execution. */
  toolChoice: GuardedToolChoice;
  /** Initial-input-only application processors. */
  applicationInputProcessors?: readonly GuardedInputProcessor[];
  /** Stream-and-result application output processors. */
  applicationOutputProcessors?: readonly GuardedOutputProcessor[];
};

/** The only call options accepted by a guarded agent handle. */
export interface GuardedAgentCallOptions {
  /** Trusted context containing the authenticated actor and host correlation. */
  requestContext: RequestContext;
  /** Optional host-minted run identifier. */
  runId?: string;
  /** Optional memory thread and resource binding. */
  memory?: AgentMemoryOption;
  /** Optional caller cancellation signal. */
  abortSignal?: AbortSignal;
}

/**
 * Narrow in-process API for a guarded agent.
 *
 * This handle prevents accidental access to raw Mastra execution methods. It
 * is not a sandbox against hostile code running in the same JavaScript
 * process.
 */
export interface GuardedAgentHandle {
  /** Agent identifier used by catalogs and audit resources. */
  readonly id: string;
  /** Exact role allowlist enforced at every guarded entry. */
  readonly allowedRoles: readonly Role[];
  /** Exact principal-kind allowlist enforced at every guarded entry. */
  readonly allowedPrincipalKinds: readonly PrincipalKind[];
  /** Fixed maximum execution steps. */
  readonly maxSteps: number;
  /** Host compatibility metadata; not an alternate execution surface. */
  readonly [GUARDED_AGENT_HOST_PROTOCOL]: GuardedAgentHostProtocol;

  /** Generate one unstructured result through all mandatory gates. */
  generate(
    messages: MessageListInput,
    options: GuardedAgentCallOptions,
  ): Promise<FullOutput<undefined>>;

  /** Stream one unstructured result through all mandatory gates. */
  stream(
    messages: MessageListInput,
    options: GuardedAgentCallOptions,
  ): Promise<MastraModelOutput<undefined>>;
}

const guardedAgentHandles = new WeakSet<object>();

/** Return whether `value` was created by this package's guarded factory. */
export function isGuardedAgentHandle(
  value: unknown,
): value is GuardedAgentHandle {
  return (
    (typeof value === 'object' || typeof value === 'function') &&
    value !== null &&
    guardedAgentHandles.has(value)
  );
}

function assertConstructionOptions(options: object): void {
  for (const key of Reflect.ownKeys(options)) {
    if (typeof key === 'string' && UNSAFE_CONSTRUCTION_KEYS.has(key)) {
      throw new TypeError(
        `createGuardedAgent: construction option '${key}' is not allowed`,
      );
    }
  }
}

function assertRoles(roles: readonly Role[]): readonly Role[] {
  if (!Array.isArray(roles) || roles.length === 0) {
    throw new TypeError(
      'createGuardedAgent: allowedRoles must be a non-empty array',
    );
  }
  const seen = new Set<Role>();
  for (const role of roles) {
    if (!(ROLES as readonly unknown[]).includes(role)) {
      throw new TypeError(
        `createGuardedAgent: unknown allowed role '${String(role)}'`,
      );
    }
    if (seen.has(role)) {
      throw new TypeError(
        `createGuardedAgent: duplicate allowed role '${role}'`,
      );
    }
    seen.add(role);
  }
  return Object.freeze([...roles]);
}

function assertToolChoice(toolChoice: GuardedToolChoice): GuardedToolChoice {
  if (
    toolChoice === 'auto' ||
    toolChoice === 'none' ||
    toolChoice === 'required'
  ) {
    return toolChoice;
  }
  if (
    !toolChoice ||
    typeof toolChoice !== 'object' ||
    Object.getPrototypeOf(toolChoice) !== Object.prototype ||
    Reflect.ownKeys(toolChoice).some(
      (key) => key !== 'type' && key !== 'toolName',
    ) ||
    toolChoice.type !== 'tool' ||
    typeof toolChoice.toolName !== 'string' ||
    toolChoice.toolName.length === 0
  ) {
    throw new TypeError(
      'createGuardedAgent: toolChoice must be auto, none, required, or one named tool',
    );
  }
  return Object.freeze({
    type: 'tool' as const,
    toolName: toolChoice.toolName,
  });
}

function assertProcessorId(processor: Processor, kind: string): void {
  if (typeof processor.id !== 'string' || processor.id.length === 0) {
    throw new TypeError(
      `createGuardedAgent: ${kind} processor id must be non-empty`,
    );
  }
  if (RESERVED_PROCESSOR_IDS.has(processor.id)) {
    throw new TypeError(
      `createGuardedAgent: application processor id '${processor.id}' is reserved`,
    );
  }
}

function hasHook(processor: Processor, hook: ProcessorHook): boolean {
  return typeof processor[hook] === 'function';
}

function validateInputProcessors(
  processors: readonly GuardedInputProcessor[],
): readonly GuardedInputProcessor[] {
  if (!Array.isArray(processors)) {
    throw new TypeError(
      'createGuardedAgent: applicationInputProcessors must be an array',
    );
  }
  for (const processor of processors) {
    if (!processor || typeof processor !== 'object') {
      throw new TypeError(
        'createGuardedAgent: application input processors must be processor objects',
      );
    }
    assertProcessorId(processor, 'application input');
    if (!hasHook(processor, 'processInput')) {
      throw new TypeError(
        `createGuardedAgent: input processor '${processor.id}' must implement processInput`,
      );
    }
    for (const hook of INPUT_PROCESSOR_FORBIDDEN_HOOKS) {
      if (hasHook(processor, hook)) {
        throw new TypeError(
          `createGuardedAgent: input processor '${processor.id}' must not implement ${hook}`,
        );
      }
    }
  }
  return Object.freeze([...processors]);
}

function validateOutputProcessors(
  processors: readonly GuardedOutputProcessor[],
): readonly GuardedOutputProcessor[] {
  if (!Array.isArray(processors)) {
    throw new TypeError(
      'createGuardedAgent: applicationOutputProcessors must be an array',
    );
  }
  for (const processor of processors) {
    if (!processor || typeof processor !== 'object') {
      throw new TypeError(
        'createGuardedAgent: application output processors must be processor objects',
      );
    }
    assertProcessorId(processor, 'application output');
    for (const hook of [
      'processOutputStream',
      'processOutputResult',
    ] as const) {
      if (!hasHook(processor, hook)) {
        throw new TypeError(
          `createGuardedAgent: output processor '${processor.id}' must implement ${hook}`,
        );
      }
    }
    for (const hook of OUTPUT_PROCESSOR_FORBIDDEN_HOOKS) {
      if (hasHook(processor, hook)) {
        throw new TypeError(
          `createGuardedAgent: output processor '${processor.id}' must not implement ${hook}`,
        );
      }
    }
  }
  return Object.freeze([...processors]);
}

function guardedCallOptions(options: unknown): GuardedAgentCallOptions {
  if (!options || typeof options !== 'object' || Array.isArray(options)) {
    throw new TypeError(
      'GuardedAgent: options must be a plain object with requestContext',
    );
  }
  const prototype = Object.getPrototypeOf(options);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new TypeError('GuardedAgent: options must be a plain object');
  }
  for (const key of Reflect.ownKeys(options)) {
    if (typeof key !== 'string' || !GUARDED_CALL_OPTION_KEYS.has(key)) {
      throw new TypeError(
        `GuardedAgent: call option '${String(key)}' is not allowed`,
      );
    }
    const descriptor = Object.getOwnPropertyDescriptor(options, key);
    if (descriptor?.get || descriptor?.set) {
      throw new TypeError(
        `GuardedAgent: call option '${key}' must be a data property`,
      );
    }
  }
  const candidate = options as Partial<GuardedAgentCallOptions>;
  if (!(candidate.requestContext instanceof RequestContext)) {
    throw new TypeError(
      'GuardedAgent: requestContext is required and must be a RequestContext',
    );
  }
  if (candidate.runId !== undefined && typeof candidate.runId !== 'string') {
    throw new TypeError('GuardedAgent: runId must be a string');
  }
  return Object.freeze({
    requestContext: candidate.requestContext,
    ...(candidate.runId !== undefined ? { runId: candidate.runId } : {}),
    ...(candidate.memory !== undefined ? { memory: candidate.memory } : {}),
    ...(candidate.abortSignal !== undefined
      ? { abortSignal: candidate.abortSignal }
      : {}),
  });
}

function directAuthorizationError(reason: string): never {
  throw new Error(`GuardedAgent authorization denied: ${reason}`);
}

class GuardedAgent<
  TAgentId extends string,
  TTools extends ToolsInput,
  TRequestContext extends Record<string, unknown> | unknown,
> extends Agent<TAgentId, TTools, undefined, TRequestContext, false> {
  readonly allowedRoles: readonly Role[];
  readonly allowedPrincipalKinds: readonly PrincipalKind[];
  readonly maxSteps: number;
  readonly [GUARDED_AGENT_HOST_PROTOCOL]: GuardedAgentHostProtocol;
  readonly #audit: AuditLogger;
  readonly #applicationInputProcessors: readonly GuardedInputProcessor[];
  readonly #applicationOutputProcessors: readonly GuardedOutputProcessor[];
  readonly #policy: PolicyEngine;
  readonly #rbac: RBACMiddleware;
  readonly #toolChoice: GuardedToolChoice;

  constructor(options: GuardedAgentConfig<TAgentId, TTools, TRequestContext>) {
    assertConstructionOptions(options);
    if (!options.audit || typeof options.audit.record !== 'function') {
      throw new TypeError('createGuardedAgent: audit must be an AuditLogger');
    }
    if (!Number.isSafeInteger(options.maxSteps) || options.maxSteps < 1) {
      throw new TypeError(
        'createGuardedAgent: maxSteps must be a positive safe integer',
      );
    }
    if (!Array.isArray(options.policies)) {
      throw new TypeError('createGuardedAgent: policies must be an array');
    }
    const allowedRoles = assertRoles(options.allowedRoles);
    const allowedPrincipalKinds = assertPrincipalKinds(
      options.allowedPrincipalKinds,
      'createGuardedAgent',
    );
    const maxSteps = options.maxSteps;
    const toolChoice = assertToolChoice(options.toolChoice);
    const applicationInputProcessors = validateInputProcessors(
      options.applicationInputProcessors ?? [],
    );
    const applicationOutputProcessors = validateOutputProcessors(
      options.applicationOutputProcessors ?? [],
    );
    const {
      allowedRoles: _allowedRoles,
      allowedPrincipalKinds: _allowedPrincipalKinds,
      policies,
      audit,
      maxSteps: _maxSteps,
      toolChoice: _toolChoice,
      applicationInputProcessors: _applicationInputProcessors,
      applicationOutputProcessors: _applicationOutputProcessors,
      ...agentConfig
    } = options;
    const policy = new PolicyEngine({
      policies: Object.freeze([...policies]),
      audit,
      holdBack: true,
      resource: `agent:${options.id}`,
    });
    if (policy.objectOnlyPolicyNames.length > 0) {
      throw new TypeError(
        `createGuardedAgent: object-only polic${policy.objectOnlyPolicyNames.length === 1 ? 'y' : 'ies'} [${policy.objectOnlyPolicyNames.join(', ')}] cannot be enforced because guarded structured output is unavailable under the tested @mastra/core peer; include 'answer' in each affected policy's channels`,
      );
    }
    const rbac = new RBACMiddleware({
      allowedRoles,
      allowedPrincipalKinds,
      audit,
      resource: `agent:${options.id}`,
    });
    super({
      ...agentConfig,
      inputProcessors: [
        ...applicationInputProcessors,
      ] as InputProcessorOrWorkflow[],
      outputProcessors: [
        ...applicationOutputProcessors,
      ] as OutputProcessorOrWorkflow[],
      defaultOptions: {
        maxSteps,
        toolChoice,
        disableBackgroundTasks: true,
      },
    } as AgentConfig<TAgentId, TTools, undefined, TRequestContext, false>);
    this.allowedRoles = allowedRoles;
    this.allowedPrincipalKinds = allowedPrincipalKinds;
    this.maxSteps = maxSteps;
    this[GUARDED_AGENT_HOST_PROTOCOL] = Object.freeze({
      version: 1,
      supportsDurableStructuredOutput: false,
    });
    this.#audit = audit;
    this.#applicationInputProcessors = applicationInputProcessors;
    this.#applicationOutputProcessors = applicationOutputProcessors;
    this.#policy = policy;
    this.#rbac = rbac;
    this.#toolChoice = toolChoice;
    this.disableBackgroundTasks();
  }

  override async generate(
    messages: MessageListInput,
    rawOptions?: unknown,
    // biome-ignore lint/suspicious/noExplicitAny: the protected subclass must remain override-compatible with every inherited structured-output overload; runtime validation rejects structured output and the factory narrows the public handle to undefined.
  ): Promise<FullOutput<any>> {
    const options = guardedCallOptions(rawOptions);
    this.#preauthorize(options.requestContext);
    return super.generate(messages, this.#executionOptions(options));
  }

  override async stream(
    messages: MessageListInput,
    rawOptions?: unknown,
    // biome-ignore lint/suspicious/noExplicitAny: the protected subclass must remain override-compatible with every inherited structured-output overload; runtime validation rejects structured output and the factory narrows the public handle to undefined.
  ): Promise<MastraModelOutput<any>> {
    const options = guardedCallOptions(rawOptions);
    this.#preauthorize(options.requestContext);
    return super.stream(messages, this.#executionOptions(options));
  }

  override async listInputProcessors(
    _requestContext?: RequestContext,
  ): Promise<InputProcessorOrWorkflow[]> {
    return [this.#rbac, ...this.#applicationInputProcessors, this.#policy];
  }

  override async listOutputProcessors(
    _requestContext?: RequestContext,
  ): Promise<OutputProcessorOrWorkflow[]> {
    return [...this.#applicationOutputProcessors, this.#policy];
  }

  #preauthorize(requestContext: RequestContext): void {
    authorizeActor({
      allowedRoles: this.allowedRoles,
      // Direct calls bypass the processor chain entirely, so this gate must
      // carry the same kind allowlist or it is a hole around the middleware.
      allowedPrincipalKinds: this.allowedPrincipalKinds,
      audit: this.#audit,
      resource: `agent:${this.id}`,
      requestContext,
      resolveActor: () => actorFromRequestContext(requestContext),
      deny: directAuthorizationError,
    });
  }

  #executionOptions(
    options: GuardedAgentCallOptions,
  ): AgentExecutionOptionsBase<unknown> & { structuredOutput?: never } {
    return {
      requestContext: options.requestContext,
      ...(options.runId !== undefined ? { runId: options.runId } : {}),
      ...(options.memory !== undefined ? { memory: options.memory } : {}),
      ...(options.abortSignal !== undefined
        ? { abortSignal: options.abortSignal }
        : {}),
      maxSteps: this.maxSteps,
      toolChoice: this.#toolChoice,
      disableBackgroundTasks: true,
      inputProcessors: [...this.#applicationInputProcessors, this.#policy],
      outputProcessors: [...this.#applicationOutputProcessors, this.#policy],
    };
  }
}

/**
 * Construct a mandatory-policy agent and return only its narrow guarded
 * handle.
 */
export function createGuardedAgent<
  TAgentId extends string,
  TTools extends ToolsInput = ToolsInput,
  TRequestContext extends Record<string, unknown> | unknown = unknown,
>(
  options: GuardedAgentConfig<TAgentId, TTools, TRequestContext>,
): GuardedAgentHandle {
  const agent = new GuardedAgent(options);
  guardedAgentHandles.add(agent);
  return agent;
}
