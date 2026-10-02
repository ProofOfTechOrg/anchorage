// SPDX-License-Identifier: Apache-2.0
// FlowsafeDurableAgent drives Mastra's durable-agent loop through RunnerRuntime
// so agent legs inherit its execution and lifecycle invariants.
//
// DurableAgent.getWorkflow compiles the agent-agnostic durable-agentic-loop;
// each run resolves its agent from the input's agentId. DurableAgent.stream
// registers non-serializable dependencies in globalRunRegistry and calls the
// overridable executeWorkflow seam, which this class routes to runtime.start.
//
// createDurableToolCallStep passes the engine leg's requestContext to
// tool.execute, so #requestContextFor's connector grant reaches the write gate.
// The registry context serves the approval pre-check; a resume without a
// runtime-minted grant fails the connector gate.
//
// prepareForDurableExecution mints a UUID when stream, generate or prepare has
// no runId, and prepare also registers that id. The public overrides must
// reject an absent caller id before that UUID satisfies downstream path checks;
// streamUntilIdle delegates through the guarded stream override.
//
// BLOCKED_RUN_ENTRIES is the single source for which method is refused on
// which ground.
// Core's isDurableAgentLike checks that recover and recoverActiveRuns are
// functions, so throwing refusals preserve the durable-agent brand.
//
// Signal senders and queueMessage stay inherited because their starts reach
// executeWorkflow's refusal for ids absent from #startRequesters, preserving
// the input verdict and publishing terminal ERROR.
//
// AgentThreadStreamRuntime's idle wake, continuation and queued-signal drains
// can start runs with core-minted ids. registerRun's completion watcher removes
// completed thread records and releases or transfers their leases, while
// #drainPendingSignals also clears failed-start state and requeues the signal.
//
// The runner's terminal refusal lets core consume a failed stream result and
// complete thread cleanup. DurableAgent does not forward the wrapped agent's
// notifications configuration, and if terminal error publication fails the
// output can remain open until eviction or a host start.
//
// Mastra.restartAllActiveWorkflowRuns is a separate host capability that can
// reach core's processor-rebuild fallback. Agent.getLegacyHandler is
// TS-private but runtime-public and returns the legacy execution handler, so
// accessing it requires the same first-party trust as other live core objects.
//
// Live-isolate scope: the loop resolves the tool's execute closure from the
// in-process globalRunRegistry (populated by stream()). A DO holds one run in
// one isolate, so a resume decided before eviction finds it. A resume
// AFTER eviction must first rehydrate that registry without replaying
// application input processors. resumeViaRuntime() rebuilds the registry with
// complete runtime processor lists after invoking only Breakwater's reserved
// processInput steps during empty-message preparation, then drives
// runtime.resume().

import {
  type Agent,
  type AgentExecutionOptions,
  type CreatedAgentSignal,
  isCreatedAgentSignal,
  isDurableAgentLike,
  isMastraSignalMessage,
  isTransientSignalMessage,
  MessageList,
  mastraDBMessageToSignal,
  signalToMastraDBMessage,
  type ToolsInput,
} from '@mastra/core/agent';
import {
  DurableAgent,
  type DurableAgentConfig,
  type DurableAgenticWorkflowInput,
  type DurableAgentStreamOptions,
  globalRunRegistry,
  prepareForDurableExecution,
} from '@mastra/core/agent/durable';
import type { MastraDBMessage } from '@mastra/core/agent/message-list';
import type { Mastra } from '@mastra/core/mastra';
import type { RequestContext } from '@mastra/core/request-context';
import type { AnyWorkflow } from '@mastra/core/workflows';

import {
  type ExecutionPrincipalKind,
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from '../approval-api/principal.js';
import {
  type D1RunExecutionIdentity,
  normalizeMutationEpoch,
  normalizeStartIdentity,
  type RunExecutionIdentity,
  type StartExecutionIdentity,
  type StartIdentity,
} from '../do-runner/execution-admission.js';
import {
  InvalidRunRequestError,
  isPathSafeId,
  RunStateUnreadableError,
} from '../do-runner/index.js';
import { resourceIdFromKey } from '../do-runner/memory-id.js';
import type {
  AuthoritativeStartState,
  LegacyRunState,
  RunnerRuntime,
  RunSummary,
  StartRunOptions,
} from '../do-runner/runtime.js';
import {
  captureReservation,
  type StartReservationReading,
} from '../do-runner/start-reservation-contract.js';

/** @internal One owned snapshot observation; pending never carries a summary. */
export type AuthoritativeAgentStartState = AuthoritativeStartState & {
  readonly execution: StartExecutionIdentity;
  readonly threaded: boolean;
};

/** @internal Selected ordinary agent data without generation authority. */
export type LegacyAgentRunState = LegacyRunState & {
  readonly threaded: boolean;
};

/** @internal A coherent snapshot belongs to another agent or thread. */
export class AgentRunSelectorMismatchError extends RunStateUnreadableError {
  constructor(workflowId: string, runId: string) {
    super(workflowId, runId);
    this.name = 'AgentRunSelectorMismatchError';
  }
}

/** @internal Host-owned start authority captured before streaming. */
export interface AgentStartAuthority {
  readonly startReservation?: StartReservationReading;
  readonly mutationEpoch?: number;
  readonly startIdentity: StartIdentity & {
    readonly target: {
      readonly kind: 'agent';
      readonly id: string;
      readonly threadId: string;
    };
  };
  readonly agentStart: { readonly threaded: boolean };
  readonly onPreparedStartIdentity:
    | ((execution: RunExecutionIdentity) => void | Promise<void>)
    | undefined;
  readonly runOwnerGuard?: StartRunOptions['runOwnerGuard'];
}

/**
 * The shared workflow id every durable-agent loop compiles to (core's
 * DurableAgentDefaults.AGENTIC_LOOP). Exposed so hosts and tests can reference
 * the registered id without a magic string.
 */
export const DURABLE_AGENTIC_LOOP_WORKFLOW_ID = 'durable-agentic-loop';

const UNOWNED_INPUT_PERSIST_TIMEOUT_MS = 5_000;
const UNREGISTERED_RUN_REFUSAL_PREFIX =
  'Flowsafe durable-agent runner refused unregistered run: ';

/**
 * @internal Metadata key that prevents a route delivery from being terminally
 * persisted. Kept off `./index.js`.
 */
export const FLOWSAFE_PERSISTENCE_FORBIDDEN = 'flowsafe.persistence-forbidden';

const BREAKWATER_GUARDED_AGENT_HOST_PROTOCOL = Symbol.for(
  '@proofoftech/breakwater/guarded-agent-host/v1',
);
const BREAKWATER_RBAC_PROCESSOR_ID = 'breakwater-rbac';
// Replaying the reserved failure step for unresolved memory processors refuses
// rehydration before the resumed step.
const BREAKWATER_MEMORY_PROCESSOR_ID = 'breakwater-memory';

interface BreakwaterGuardedAgentHostProtocol {
  readonly version: 1;
  readonly supportsDurableStructuredOutput: false;
}

function captureAgentStartAuthority(
  source: AgentStartAuthority,
  requestedBy: string,
  requestedByKind: ExecutionPrincipalKind,
): AgentStartAuthority {
  if (source === null || typeof source !== 'object' || Array.isArray(source)) {
    throw new InvalidRunRequestError('agent start authority is required');
  }
  const {
    mutationEpoch: rawEpoch,
    startIdentity: rawIdentity,
    agentStart: rawAgentStart,
    onPreparedStartIdentity,
    runOwnerGuard: rawGuard,
    startReservation: rawReservation,
  } = source;
  if (
    rawIdentity === undefined ||
    rawAgentStart === undefined ||
    !Object.hasOwn(source, 'onPreparedStartIdentity')
  ) {
    throw new InvalidRunRequestError('agent start authority is incomplete');
  }
  const startReservation =
    rawReservation === undefined
      ? undefined
      : captureReservation(rawReservation, 'started');
  const mutationEpoch = normalizeMutationEpoch(rawEpoch);
  const identity = normalizeStartIdentity(rawIdentity);
  if (identity.target.kind !== 'agent') {
    throw new InvalidRunRequestError(
      'agent start authority requires an agent target',
    );
  }
  if (
    identity.owner.id !== requestedBy ||
    identity.owner.kind !== requestedByKind
  ) {
    throw new InvalidRunRequestError(
      'startIdentity owner does not match requester',
    );
  }
  if (
    rawAgentStart === null ||
    typeof rawAgentStart !== 'object' ||
    Array.isArray(rawAgentStart)
  ) {
    throw new InvalidRunRequestError('agentStart is malformed');
  }
  const { threaded } = rawAgentStart;
  if (typeof threaded !== 'boolean') {
    throw new InvalidRunRequestError('agentStart is malformed');
  }
  if (
    onPreparedStartIdentity !== undefined &&
    typeof onPreparedStartIdentity !== 'function'
  ) {
    throw new InvalidRunRequestError('onPreparedStartIdentity is malformed');
  }
  let runOwnerGuard: AgentStartAuthority['runOwnerGuard'];
  if (rawGuard !== undefined) {
    if (
      rawGuard === null ||
      typeof rawGuard !== 'object' ||
      Array.isArray(rawGuard)
    ) {
      throw new InvalidRunRequestError('runOwnerGuard is malformed');
    }
    const { owner: rawOwner, reservationToken } = rawGuard;
    if (
      rawOwner === null ||
      typeof rawOwner !== 'object' ||
      Array.isArray(rawOwner)
    ) {
      throw new InvalidRunRequestError('runOwnerGuard is malformed');
    }
    const { kind, id } = rawOwner;
    if (
      !isExecutionPrincipalKind(kind) ||
      !isExecutionPrincipalId(id) ||
      !isPathSafeId(reservationToken)
    ) {
      throw new InvalidRunRequestError('runOwnerGuard is malformed');
    }
    runOwnerGuard = Object.freeze({
      owner: Object.freeze({ kind, id }),
      reservationToken,
    });
  }
  return Object.freeze({
    mutationEpoch,
    startIdentity: Object.freeze({
      owner: identity.owner,
      target: identity.target,
    }),
    agentStart: Object.freeze({ threaded }),
    onPreparedStartIdentity,
    runOwnerGuard,
    startReservation,
  });
}

type DurableCallOptionMapper = (key: PropertyKey, value: unknown) => unknown;

function ownDataDescriptor(target: object, key: PropertyKey, path: string) {
  const descriptor = Object.getOwnPropertyDescriptor(target, key);
  if (descriptor && !('value' in descriptor)) {
    throw new TypeError(
      `FlowsafeDurableAgent: call option '${path}' must be a data property`,
    );
  }
  return descriptor;
}

function snapshotDurableCallOptions<T extends object>(
  options: T | undefined,
  mapper?: DurableCallOptionMapper,
): T | undefined {
  if (options === undefined) return undefined;
  if (
    options === null ||
    typeof options !== 'object' ||
    Array.isArray(options)
  ) {
    throw new TypeError('FlowsafeDurableAgent: call options must be an object');
  }
  const snapshot: Record<PropertyKey, unknown> = {};
  for (const key of Reflect.ownKeys(options)) {
    const descriptor = ownDataDescriptor(options, key, String(key));
    if (!descriptor) continue;
    Object.defineProperty(snapshot, key, {
      configurable: false,
      enumerable: descriptor.enumerable,
      value: mapper ? mapper(key, descriptor.value) : descriptor.value,
      writable: false,
    });
  }
  return Object.freeze(snapshot) as T;
}

/** @internal */
export function breakwaterGuardedAgentHostProtocol(
  agent: unknown,
): BreakwaterGuardedAgentHostProtocol | undefined {
  if ((typeof agent !== 'object' && typeof agent !== 'function') || !agent) {
    return undefined;
  }
  const protocol = (agent as Record<symbol, unknown>)[
    BREAKWATER_GUARDED_AGENT_HOST_PROTOCOL
  ];
  if (protocol === undefined) return undefined;
  if (
    !protocol ||
    typeof protocol !== 'object' ||
    (protocol as { version?: unknown }).version !== 1 ||
    (protocol as { supportsDurableStructuredOutput?: unknown })
      .supportsDurableStructuredOutput !== false
  ) {
    throw new TypeError(
      'FlowsafeDurableAgent: malformed Breakwater guarded-agent host protocol',
    );
  }
  return protocol as BreakwaterGuardedAgentHostProtocol;
}

/**
 * @internal Why the runner refuses to wrap `agent`, or `undefined` when it
 * accepts it.
 */
export function unwrappableAgentReason(
  agent: Pick<Agent, 'durable' | 'getChannels' | 'getDeclaredSchedules'>,
): string | undefined {
  if (agent.durable) {
    return "sets the 'durable' option: a Mastra that registers it wraps it in its own DurableAgent, whose loop, recovery and run listing run outside RunnerRuntime";
  }
  if (isDurableAgentLike(agent)) {
    return 'is already a durable agent: the runtime registers the agent it wraps on its own Mastra, where a Mastra DurableAgent brings recovery and run listing outside RunnerRuntime and a FlowsafeDurableAgent refuses to register; wrap the plain Agent instead';
  }
  if (agent.getChannels() != null) {
    return 'has channels configured: Mastra dispatches their inbound messages and tool approvals to that agent outside RunnerRuntime';
  }
  if (agent.getDeclaredSchedules().length > 0) {
    return 'declares schedules: the schedule worker of a Mastra that registers that agent fires them on it outside RunnerRuntime';
  }
  return undefined;
}

/**
 * Why every member of the resume family is refused. Homed once: the family's
 * entry points are one code path (resumeStream/resumeGenerate/the approve and
 * decline pairs all funnel into resume()), so one sentence must not drift into
 * a copy per entry point.
 */
const RESUME_FAMILY_REASON =
  "on a run-registry miss the inherited path rehydrates from persisted snapshot storage and re-drives with createRun + run.resume outside RunnerRuntime, bypassing the approval-decision path's grant derivation and the fail-closed registry rehydration";

/**
 * Why the network entry points are refused. Homed once: `network()` starts and
 * `resumeNetwork()` resumes THE SAME networkLoop, and the approve/decline pair
 * are one-line forwards to `resumeNetwork()`, so one sentence must not drift
 * into a copy per entry point.
 */
const NETWORK_FAMILY_REASON =
  'the multi-agent network loop compiles its own workflow and drives it with createRun plus run.stream/run.resumeStream on the default engine, outside RunnerRuntime, so no leg is run-owned, grant-derived or snapshot-provenanced — and under autoResumeSuspendedTools it additionally recovers a suspended run id from thread memory and re-drives that run';

/**
 * Why the legacy execution entry points are refused. Homed once: each is a
 * one-line forward into the SAME AgentLegacyHandler, so they are one code path.
 */
const LEGACY_FAMILY_REASON =
  "the AI SDK v4 legacy handler is a second execution surface that converts and runs the agent's tools outside RunnerRuntime, mints its own run id when the caller omits one, and skips the authorization gate every supported entry calls (requireAgentExecutionFGA)";

/**
 * Why the thread-level tool approval is refused. Its name promises a resume,
 * but its continuation branch STARTS a run under a core-minted id — a mint on
 * the far side of a method that never asks the caller for one.
 */
const THREAD_TOOL_APPROVAL_REASON =
  'the messages-plus-approved branch does not resume at all: it hands the thread runtime a continuation whose run id falls back to randomUUID() when the caller names none, then starts a run under that id — path-safe, so the host-owned run-id guard cannot tell it from a caller-minted one — and when neither an explicit run id nor an active thread run is available, it reaches the equally unscoped suspended-run discovery';

/**
 * Why binding the wrapper to a Mastra is refused. Homed once: `__setMastra()`
 * forwards to `__registerMastra()`, so the two are one code path.
 */
const MASTRA_BINDING_REASON =
  "it binds the runner and its wrapped agent to a Mastra the runtime did not build, so every later leg is prepared against that Mastra; registered through Mastra.addAgent once the runtime has built its own Mastra, it also repoints the runtime's loop workflow, and with it run state, to that Mastra's storage";
/**
 * Why service setters are refused. Homed once: both install after construction
 * and forward their service to the wrapped agent.
 */
const INSTALLED_SERVICE_REASON =
  "it installs memory or pub/sub after construction and forwards it to the wrapped agent; the wrapper's pub/sub is fixed at construction because thread state, signal delivery and the run's drain share it";
/**
 * Why direct aborts are refused. Homed once: both bypass the terminate route's
 * ownership and disputed-settlement checks.
 */
const DIRECT_ABORT_REASON =
  "a direct abort bypasses the terminate route's ownership check and disputed-settlement refusal, stops running tools, and can reach another thread's run in the isolate through core's global run registry; cancel through the terminate route";

type GuardedDurableCallOptionRule =
  | { readonly kind: 'refused'; readonly why: string }
  | {
      readonly kind: 'compared';
      readonly comparison:
        | 'guardedMaxSteps'
        | 'guardedToolChoice'
        | 'backgroundTasksDisabled';
    }
  | { readonly kind: 'memoryBinding' }
  | { readonly kind: 'allowed'; readonly why: string };

type GuardedDurableDefaults = Readonly<{
  maxSteps: NonNullable<AgentExecutionOptions<unknown>['maxSteps']>;
  toolChoice: NonNullable<AgentExecutionOptions<unknown>['toolChoice']>;
}>;

const GUARDED_CALLBACK_REASON =
  "it receives what the call's stream delivers; on the durable loop the saved message and the returned result are not filtered by the output policies";
const GUARDED_OBSERVABILITY_REASON = 'observability only';
const GUARDED_REENTRY_REASON =
  "Mastra's own re-entry options for its idle loop and background-task wait";

// The single classification of a guarded agent's durable call options; the
// host-facing list in docs/durable-agents.md#durable-call-options follows it.
// Keys the table does not name, and symbol keys, pass unchanged: core re-enters
// stream() with keys of its own. Absent compared keys pass because core merges
// the guarded defaults into every run it registers.
const GUARDED_DURABLE_CALL_OPTIONS = {
  structuredOutput: {
    kind: 'refused',
    why: 'Mastra parses and releases the structured object outside the output policies',
  },
  errorProcessors: {
    kind: 'refused',
    why: 'error processors can change model requests after the guarded input chain',
  },
  clientTools: {
    kind: 'refused',
    why: "Mastra maps a per-call client tool's result after the guarded input policies have read it",
  },
  toolsets: {
    kind: 'refused',
    why: "Mastra adds the call's tools and maps a client tool's result after the guarded input policies have read it",
  },
  outputProcessors: {
    kind: 'refused',
    why: "Mastra replaces the guarded output processors, output policies included, with the call's list",
  },
  inputProcessors: {
    kind: 'refused',
    why: 'Breakwater fixes the guarded input processors at construction',
  },
  instructions: {
    kind: 'refused',
    why: "Mastra replaces the guarded agent's instructions with the call's",
  },
  system: {
    kind: 'refused',
    why: "Mastra adds the call's system messages, which the input policies do not read",
  },
  context: {
    kind: 'refused',
    why: "Mastra adds the call's context messages, which the input policies do not read",
  },
  prepareStep: {
    kind: 'refused',
    why: "Mastra runs the call's step preparation after the guarded input processors on every step",
  },
  hooks: {
    kind: 'refused',
    why: "Mastra runs the call's tool hooks over the guarded agent's own, and a hook can replace a tool call's result",
  },
  scorers: {
    kind: 'refused',
    why: "Mastra replaces the guarded agent's scorers with the call's",
  },
  savePerStep: {
    kind: 'refused',
    why: "Mastra saves each step before the output processors' result phase and before a later refusal",
  },
  versions: {
    kind: 'refused',
    why: "Mastra resolves the guarded agent's sub-agents to the call's versions",
  },
  autoResumeSuspendedTools: {
    kind: 'refused',
    why: 'Mastra adds a system instruction after the guarded input chain and lets model input resume suspended tools',
  },
  includeRawChunks: {
    kind: 'refused',
    why: "the provider's raw chunks reach the stream without passing the output policies",
  },
  onChunk: {
    kind: 'refused',
    why: 'Mastra calls it with each chunk before the output processors run',
  },
  delegation: {
    kind: 'refused',
    why: "Mastra runs the call's delegation hooks, which can change what sub-agents receive and return",
  },
  onIterationComplete: {
    kind: 'refused',
    why: 'its feedback reaches the model and the saved thread without passing the guarded policies',
  },
  isTaskComplete: {
    kind: 'refused',
    why: "Mastra runs the call's completion scorers, and their feedback reaches the model and the saved thread without passing the guarded policies",
  },
  transform: {
    kind: 'refused',
    why: "Mastra replaces the guarded agent's tool-payload transform with the call's",
  },
  experimentalTransform: {
    kind: 'refused',
    why: "it rewrites the thread's shared stream after the output processors",
  },
  requireToolApproval: {
    kind: 'refused',
    why: "Mastra runs a call-level approval function with the tool's arguments, and a tool's own approval rule replaces the call's setting",
  },
  backgroundTaskPolicy: {
    kind: 'refused',
    why: 'it changes the delegated tools Mastra assembles, and Breakwater disables background tasks at construction',
  },
  maxProcessorRetries: {
    kind: 'refused',
    why: 'Breakwater fixes processor retries at construction',
  },
  eagerToolExecution: {
    kind: 'refused',
    why: "the guarded agent runs no tool eagerly, and Mastra's durable execution does not support eager tool execution",
  },
  maxSteps: { kind: 'compared', comparison: 'guardedMaxSteps' },
  toolChoice: { kind: 'compared', comparison: 'guardedToolChoice' },
  disableBackgroundTasks: {
    kind: 'compared',
    comparison: 'backgroundTasksDisabled',
  },
  memory: { kind: 'memoryBinding' },
  runId: {
    kind: 'allowed',
    why: 'the host mints the run id, and each entry requires a path-safe one',
  },
  requestContext: {
    kind: 'allowed',
    why: 'the host builds it, and Breakwater authorizes the call from it',
  },
  abortSignal: { kind: 'allowed', why: 'it can only stop the run' },
  actor: {
    kind: 'allowed',
    why: "Mastra's authorization signal, which only the host sets; Breakwater authorizes from the request context",
  },
  mcp: {
    kind: 'allowed',
    why: 'an execution context for tools the agent already has; it adds none',
  },
  serverless: {
    kind: 'allowed',
    why: 'it keeps the platform alive for finish work and changes nothing the call does',
  },
  hideSignals: {
    kind: 'allowed',
    why: "it hides signals from the caller's stream only; model context and storage are unchanged",
  },
  closeOnSuspend: {
    kind: 'allowed',
    why: "it closes the caller's stream when the run suspends",
  },
  stopWhen: {
    kind: 'allowed',
    why: 'it can only stop the run earlier, within maxSteps',
  },
  activeTools: {
    kind: 'allowed',
    why: "it selects among the guarded agent's tools and adds none",
  },
  toolCallConcurrency: {
    kind: 'allowed',
    why: "it schedules the guarded agent's tool calls and adds none",
  },
  returnScorerData: {
    kind: 'allowed',
    why: "it returns the guarded agent's own scorers' data",
  },
  modelSettings: {
    kind: 'allowed',
    why: 'host-owned generation settings: headers, stop sequences and timeouts',
  },
  providerOptions: {
    kind: 'allowed',
    why: "the thread host checks it against Breakwater's accepted provider options, and a direct caller must",
  },
  onStepFinish: { kind: 'allowed', why: GUARDED_CALLBACK_REASON },
  onFinish: { kind: 'allowed', why: GUARDED_CALLBACK_REASON },
  onError: { kind: 'allowed', why: GUARDED_CALLBACK_REASON },
  onSuspended: { kind: 'allowed', why: GUARDED_CALLBACK_REASON },
  onAbort: { kind: 'allowed', why: GUARDED_CALLBACK_REASON },
  tracingOptions: { kind: 'allowed', why: GUARDED_OBSERVABILITY_REASON },
  tracing: { kind: 'allowed', why: GUARDED_OBSERVABILITY_REASON },
  loggerVNext: { kind: 'allowed', why: GUARDED_OBSERVABILITY_REASON },
  metrics: { kind: 'allowed', why: GUARDED_OBSERVABILITY_REASON },
  tracingContext: { kind: 'allowed', why: GUARDED_OBSERVABILITY_REASON },
  untilIdle: { kind: 'allowed', why: GUARDED_REENTRY_REASON },
  _skipBgTaskWait: { kind: 'allowed', why: GUARDED_REENTRY_REASON },
} as const satisfies Record<
  | keyof DurableAgentStreamOptions<unknown>
  | keyof AgentExecutionOptions<unknown>,
  GuardedDurableCallOptionRule
>;

const GUARDED_MEMORY_REASON =
  "Mastra applies call-level memory configuration and thread fields outside the input policies; configure them on the agent's Memory";

function guardedMemoryBinding<M>(memory: M): M {
  if (memory === undefined) return memory;
  if (memory === null || typeof memory !== 'object' || Array.isArray(memory)) {
    throw new TypeError(
      'FlowsafeDurableAgent: memory must be an object for a Breakwater guarded agent',
    );
  }
  const binding: Record<string, unknown> = {};
  for (const key of Reflect.ownKeys(memory)) {
    if (key !== 'thread' && key !== 'resource') {
      throw new TypeError(
        `FlowsafeDurableAgent: memory.${String(key)} is not supported for a Breakwater guarded agent because ${GUARDED_MEMORY_REASON}`,
      );
    }
    const descriptor = ownDataDescriptor(memory, key, `memory.${key}`);
    if (!descriptor) continue;
    let value = descriptor.value;
    if (
      key === 'thread' &&
      value !== null &&
      (typeof value === 'object' || typeof value === 'function')
    ) {
      let idDescriptor: PropertyDescriptor | undefined;
      for (const threadKey of Reflect.ownKeys(value)) {
        if (threadKey !== 'id') {
          throw new TypeError(
            `FlowsafeDurableAgent: memory.thread.${String(threadKey)} is not supported for a Breakwater guarded agent because ${GUARDED_MEMORY_REASON}`,
          );
        }
        idDescriptor = ownDataDescriptor(value, threadKey, 'memory.thread.id');
      }
      value = Object.freeze({ id: idDescriptor?.value });
    }
    binding[key] = value;
  }
  return Object.freeze(binding) as M;
}

function guardedToolChoiceMatches(
  value: unknown,
  guarded: GuardedDurableDefaults['toolChoice'],
): boolean {
  if (typeof guarded === 'string') return value === guarded;
  if (!guarded || value === null || typeof value !== 'object') return false;
  if (
    Reflect.ownKeys(value).some((key) => key !== 'type' && key !== 'toolName')
  ) {
    return false;
  }
  const typeDescriptor = Object.getOwnPropertyDescriptor(value, 'type');
  const toolNameDescriptor = Object.getOwnPropertyDescriptor(value, 'toolName');
  return (
    typeDescriptor !== undefined &&
    'value' in typeDescriptor &&
    typeDescriptor.value === guarded.type &&
    toolNameDescriptor !== undefined &&
    'value' in toolNameDescriptor &&
    toolNameDescriptor.value === guarded.toolName
  );
}

function guardedCallOptionMapper(
  defaults: GuardedDurableDefaults,
): DurableCallOptionMapper {
  const { maxSteps, toolChoice } = defaults;
  return (key, value) => {
    if (key === '__proto__') {
      const why =
        "Mastra's option merge makes its value the prototype of the options it resolves";
      throw new TypeError(
        `FlowsafeDurableAgent: __proto__ is not supported for a Breakwater guarded agent because ${why}`,
      );
    }
    if (
      typeof key !== 'string' ||
      !Object.hasOwn(GUARDED_DURABLE_CALL_OPTIONS, key)
    ) {
      return value;
    }
    const rule =
      GUARDED_DURABLE_CALL_OPTIONS[
        key as keyof typeof GUARDED_DURABLE_CALL_OPTIONS
      ];
    switch (rule.kind) {
      case 'refused':
        throw new TypeError(
          `FlowsafeDurableAgent: ${key} is not supported for a Breakwater guarded agent because ${rule.why}`,
        );
      case 'memoryBinding':
        return guardedMemoryBinding(value);
      case 'allowed':
        return value;
      case 'compared': {
        let expected: unknown;
        let matches: boolean;
        switch (rule.comparison) {
          case 'guardedMaxSteps':
            expected = maxSteps;
            matches = value === expected;
            break;
          case 'guardedToolChoice':
            expected = toolChoice;
            matches = guardedToolChoiceMatches(value, toolChoice);
            break;
          case 'backgroundTasksDisabled':
            expected = true;
            matches = value === expected;
            break;
        }
        if (!matches) {
          throw new TypeError(
            `FlowsafeDurableAgent: ${key} must equal the guarded agent's own value`,
          );
        }
        return expected;
      }
    }
  };
}

/**
 * Why each blocked entry point is refused, keyed by method name: the SINGLE
 * source of those reasons.
 *
 * The grounds:
 *  1. re-drives a persisted run below `executeWorkflow`, where there is no
 *     RunnerRuntime — no run ownership, no per-leg grant, no snapshot
 *     provenance;
 *  2. unscoped run or thread identity discovery that bypasses the host
 *     topology's per-principal ownership checks and returns identities the
 *     caller does not own;
 *  3. snapshot deletion owned by deployment-scoped retention;
 *  4. a SECOND execution surface outside RunnerRuntime, whether the call runs
 *     it or installs something that later does, or that mints a run id below
 *     the caller;
 *  5. installs a runtime service — a Mastra, memory or pub/sub — after
 *     construction;
 *  6. cancels a run outside RunnerRuntime's terminal lifecycle.
 *
 * NOT re-exported from `./index.js` (a named-exports-only barrel), so this
 * stays off the public `@proofoftech/flowsafe/agent-runner` subpath.
 */
export const BLOCKED_RUN_ENTRIES = {
  recover:
    're-driving a persisted run bypasses run ownership, per-leg grant minting and the fail-closed registry rehydration',
  recoverActiveRuns:
    'bulk re-driving persisted runs bypasses run ownership and per-leg grant minting',
  resume: RESUME_FAMILY_REASON,
  resumeStream: RESUME_FAMILY_REASON,
  resumeGenerate: RESUME_FAMILY_REASON,
  approveToolCall: RESUME_FAMILY_REASON,
  declineToolCall: RESUME_FAMILY_REASON,
  approveToolCallGenerate: RESUME_FAMILY_REASON,
  declineToolCallGenerate: RESUME_FAMILY_REASON,
  listActiveRuns:
    "core scopes the running-run listing by agentId plus the caller's own optional thread and resource ids, never by per-principal ownership, so it bypasses the host topology's run-ownership checks and returns run, thread and resource ids the caller does not own",
  listSuspendedRuns:
    "core scopes the suspended-run listing by agentId plus the caller's own optional thread and resource ids, never by per-principal ownership, so it bypasses the host topology's run-ownership checks and returns run, thread and resource ids the caller does not own",
  listActiveThreadRuns:
    'core scopes the active thread-run listing by nothing at all: it takes no arguments and returns the run, thread and resource ids of every thread on the pubsub instance with a run in flight, which core keys by pubsub instance rather than by agent, so it enumerates ids across every principal AND every agent that shares the instance',
  discoverThreadPeers:
    'it takes no caller-named thread or principal and returns the agent, resource, thread and source identities every peer on the pub/sub advertises',
  deleteRunSnapshots:
    'durable-agent snapshot rows are retained until deployment-scoped retention purge removes them',
  network: `${NETWORK_FAMILY_REASON}, and it mints an unowned run id when the caller omits one`,
  resumeNetwork: NETWORK_FAMILY_REASON,
  approveNetworkToolCall: NETWORK_FAMILY_REASON,
  declineNetworkToolCall: NETWORK_FAMILY_REASON,
  generateLegacy: LEGACY_FAMILY_REASON,
  streamLegacy: LEGACY_FAMILY_REASON,
  sendToolApproval: THREAD_TOOL_APPROVAL_REASON,
  __setThreadRuntimeAgent:
    "it installs the target subscribeToThread, claimThreadOwnership, sendMessage, queueMessage, sendStateSignal, sendNotificationSignal and sendSignal resolve through the thread-runtime agent field; discoverThreadPeers also resolves that target and listSuspendedRuns reads its durable loop workflow name — redirecting execution and subscription replay onto an agent that carries none of this class's overrides: no caller-minted run id, no executeWorkflow, and no terminal refusal for a run the host start seam never registered",
  setChannels:
    "it forwards the channels it is given to the wrapped agent, which becomes their dispatch target, so a plain AgentChannels delivers inbound messages to the wrapped agent's sendMessage, and channel approvals and declines to its approveToolCall and declineToolCall, outside RunnerRuntime; an AgentControllerChannels dispatches through its controller instead, and is refused as well because a controller over this class cannot install its runtime services or register this wrapper on its Mastra",
  __setDeclaredSchedules:
    "it forwards the schedules it is given to the wrapped agent, and the schedule worker of a Mastra that registers that agent and runs startWorkers() fires each of them through the wrapped agent's sendSignal or generate, outside RunnerRuntime",
  __setMastra: MASTRA_BINDING_REASON,
  __registerMastra: MASTRA_BINDING_REASON,
  __setMemory: INSTALLED_SERVICE_REASON,
  __setPubSub: INSTALLED_SERVICE_REASON,
  abortRunStream: DIRECT_ABORT_REASON,
  abortThreadStream: DIRECT_ABORT_REASON,
} as const;

/**
 * The refusal for a blocked run entry point. A plain `Error`, not
 * {@link InvalidRunRequestError}: that class means "the client's run request is
 * malformed", and calling one of these is neither a run request nor
 * recoverable by fixing an argument. The message names WHAT is refused and WHY, and carries no run data — the discovery
 * entries have none to carry, and the others must not echo an id the caller may
 * not own into its log.
 */
function unavailableRunEntry(method: string, why: string): Error {
  return new Error(
    `FlowsafeDurableAgent.${method}() is unavailable: ${why} — this wrapper starts runs through RunnerRuntime from the host start seam (streamUntilPersisted) and resumes them through the approval-decision path (resumeViaRuntime)`,
  );
}

function bindThreadCompletion<T extends object>(
  output: T,
  completion: Promise<void>,
): T {
  return new Proxy(output, {
    get(target, property) {
      // Core's output getters read private fields, which a proxy receiver lacks.
      const value = Reflect.get(target, property, target);
      if (property === '_waitUntilFinished') {
        return () =>
          typeof value === 'function'
            ? Promise.race([
                Promise.resolve(value.call(target) as unknown),
                completion,
              ])
            : completion;
      }
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

/**
 * Brand marking an agent as runtime-driven. The thread Durable Object requires
 * it before a wake seam may start a run.
 *
 * Direct core sender calls that mint a run still reach the durable runner,
 * which fails them terminally without executing. Structural so a test double
 * can opt in without constructing a real durable agent.
 */
export const RUNTIME_DRIVEN_AGENT: unique symbol = Symbol(
  'flowsafe.runtimeDrivenAgent',
);

/**
 * Does this value carry the {@link RUNTIME_DRIVEN_AGENT} brand — i.e. is its
 * signal wake driven through RunnerRuntime rather than the default engine?
 * Takes `unknown` so a caller holding a core `Agent` (whose type has no index
 * for the brand symbol) can check without a cast.
 */
export function isRuntimeDrivenAgent(agent: unknown): boolean {
  return (
    typeof agent === 'object' &&
    agent !== null &&
    (agent as Record<symbol, unknown>)[RUNTIME_DRIVEN_AGENT] === true
  );
}

/** Options for {@link createFlowsafeDurableAgent}. */
export interface FlowsafeDurableAgentOptions<
  TAgentId extends string = string,
  TTools extends ToolsInput = ToolsInput,
  TOutput = undefined,
> {
  /** The Agent to wrap with durable, runtime-driven execution. */
  agent: Agent<TAgentId, TTools, TOutput>;
  /**
   * The RunnerRuntime through which the loop is driven. Required because
   * this is the whole point of the flowsafe wrapper: executeWorkflow() calls
   * `runtime.start('durable-agentic-loop', ...)` instead of the base
   * `createRun + start`, so host-owned run IDs, the per-leg grant context, and
   * persisted snapshot provenance apply to agent legs. The loop workflow is registered on this runtime
   * by the factory (idempotently — one shared id serves every durable agent).
   */
  runtime: RunnerRuntime;
  /** Optional id override (defaults to agent.id). */
  id?: TAgentId;
  /** Optional name override (defaults to agent.name). */
  name?: string;
  /** Resumable-stream cache — see createDurableAgent's `cache`. */
  cache?: DurableAgentConfig<TAgentId, TTools, TOutput>['cache'];
  /**
   * Stream and agent-level pub/sub. Uses `pubsub ?? runtime.pubsub`, or the
   * wrapper's stream bus when both are absent. Thread state, signal delivery,
   * and the run's drain share this identity. Construction refuses a wrapped
   * agent with a different pub/sub of its own. The wrapped agent's pub/sub
   * follows the last wrapper constructed over it.
   */
  pubsub?: DurableAgentConfig<TAgentId, TTools, TOutput>['pubsub'];
  /**
   * Public Mastra thread runtime (`mastra.agentThreadStreamRuntime`). Core
   * registers started runs itself. When present, a run resumed through
   * `resumeViaRuntime()` registers on the agent's pub/sub so active-thread
   * signals join it. Without it, resumed runs are absent from thread state and
   * signals sent while they run are not delivered into them.
   */
  threadRuntime?: Mastra['agentThreadStreamRuntime'];
  /** Max steps for the agentic loop (bakes into the shared loop's isTaskComplete step). */
  maxSteps?: number;
}

/**
 * A DurableAgent whose loop runs through {@link RunnerRuntime} rather than the
 * base `createRun + run.start`. Construct via {@link createFlowsafeDurableAgent}
 * so the loop workflow is registered on the runtime.
 */
export class FlowsafeDurableAgent<
  TAgentId extends string = string,
  TTools extends ToolsInput = ToolsInput,
  TOutput = undefined,
> extends DurableAgent<TAgentId, TTools, TOutput> {
  /**
   * The runtime-driven brand the thread-Durable-Object signal wake requires (see
   * {@link RUNTIME_DRIVEN_AGENT}). A `unique symbol` field, so it cannot collide
   * with an inherited property and a plain `Agent` never carries it.
   */
  readonly [RUNTIME_DRIVEN_AGENT] = true;
  readonly #runtime: RunnerRuntime;
  readonly #wrappedAgent: Agent<TAgentId, TTools, TOutput>;
  readonly #rehydrationInputProcessorIds: ReadonlySet<string>;
  readonly #guardedCallOptionMapper?: DurableCallOptionMapper;
  readonly #threadRuntime?: Mastra['agentThreadStreamRuntime'];
  readonly #persistenceWaiters = new Map<
    string,
    {
      resolve: () => void;
      reject: (error: unknown) => void;
    }
  >();
  readonly #startRequesters = new Map<string, string>();
  readonly #startRequesterKinds = new Map<string, ExecutionPrincipalKind>();
  readonly #startAttemptTokens = new Map<string, string>();
  /**
   * runId -> the reservation key the thread topology took for this start, so
   * `executeWorkflow` can hand it to `RunnerRuntime.start` for the execution
   * fence's proof-only match.
   */
  readonly #startIdempotencyKeys = new Map<string, string>();
  readonly #startAuthorities = new Map<string, AgentStartAuthority>();
  readonly #startScheduleDispatches = new Map<
    string,
    { scheduleId: string; dispatchId: string }
  >();
  // The host's exact options object receives one private, single-use re-entry ticket.
  readonly #hostStreamTickets = new WeakSet<object>();
  // The created signal of a start with neither a host ticket nor a request
  // context, the options a drain inherits from a run resumeViaRuntime()
  // registered. An RBAC refusal removes that signal from the prepared message
  // list, so executeWorkflow preserves it from this capture.
  readonly #replayedSignals = new Map<string, CreatedAgentSignal>();

  constructor(options: FlowsafeDurableAgentOptions<TAgentId, TTools, TOutput>) {
    const refusal = unwrappableAgentReason(options.agent);
    if (refusal !== undefined) {
      throw new TypeError(`FlowsafeDurableAgent: the wrapped agent ${refusal}`);
    }
    const pubsub = options.pubsub ?? options.runtime.pubsub;
    // The run's drain reads the wrapped agent's pub/sub, where an own pub/sub
    // wins over the one this wrapper installs.
    if (options.agent.hasOwnPubSub() && options.agent.getPubSub() !== pubsub) {
      throw new TypeError(
        "FlowsafeDurableAgent: the wrapped agent has its own pub/sub, which differs from the one the wrapper uses; signal delivery and the run's drain must read the same pub/sub",
      );
    }
    const guardedProtocol = breakwaterGuardedAgentHostProtocol(options.agent);
    let callOptionMapper: DurableCallOptionMapper | undefined;
    if (guardedProtocol !== undefined) {
      const defaults = options.agent.getDefaultOptions();
      if (
        defaults === null ||
        typeof defaults !== 'object' ||
        typeof (defaults as { then?: unknown }).then === 'function'
      ) {
        throw new TypeError(
          'FlowsafeDurableAgent: a Breakwater guarded agent must have static default options',
        );
      }
      for (const [key, rule] of Object.entries(GUARDED_DURABLE_CALL_OPTIONS)) {
        if (
          rule.kind === 'compared' &&
          (!Object.hasOwn(defaults, key) ||
            ownDataDescriptor(defaults, key, key)?.value === undefined)
        ) {
          throw new TypeError(
            `FlowsafeDurableAgent: a Breakwater guarded agent's default options must carry ${key}`,
          );
        }
      }
      const { maxSteps, toolChoice } = defaults as GuardedDurableDefaults;
      callOptionMapper = guardedCallOptionMapper(
        Object.freeze({
          maxSteps,
          toolChoice:
            toolChoice !== null && typeof toolChoice === 'object'
              ? Object.freeze({
                  type: toolChoice.type,
                  toolName: toolChoice.toolName,
                })
              : toolChoice,
        }),
      );
      // Signal drains replay these defaults, so an unusable one fails before registering a run.
      snapshotDurableCallOptions(defaults, callOptionMapper);
    }
    super({
      agent: options.agent,
      id: options.id,
      name: options.name,
      cache: options.cache,
      pubsub,
      maxSteps: options.maxSteps,
    });
    this.#runtime = options.runtime;
    this.#wrappedAgent = options.agent;
    this.#rehydrationInputProcessorIds = new Set(
      guardedProtocol !== undefined
        ? [BREAKWATER_RBAC_PROCESSOR_ID, BREAKWATER_MEMORY_PROCESSOR_ID]
        : [BREAKWATER_RBAC_PROCESSOR_ID],
    );
    this.#guardedCallOptionMapper = callOptionMapper;
    this.#threadRuntime = options.threadRuntime;
    // Core keys thread state and signal delivery on the agent-level pub/sub;
    // its stream pub/sub option leaves that identity unset for the run's drain.
    super.__setPubSub(pubsub ?? this.pubsub);
  }

  /**
   * Reject an absent caller runId before prepareForDurableExecution replaces
   * it with a UUID that downstream path checks accept. The string check also
   * prevents RegExp coercion from accepting a numeric registry key.
   */
  #assertCallerRunId(runId: unknown): asserts runId is string {
    if (!isPathSafeId(runId)) {
      throw new InvalidRunRequestError(
        'a caller-minted runId is required and must be URL-path-safe (the host owns run ids) — the durable-agent runner never generates one',
      );
    }
  }

  /**
   * Refuse every run id that is still registered on any start or core seam.
   * `#startRequesters` alone is insufficient because `streamUntilPersisted()`
   * removes it after the first summary while a suspended stream stays live.
   * The internal registry has no TTL, so it also covers long suspensions after
   * the global registry's TTL expires.
   * It exempts the host's own first start: `streamUntilPersisted()`
   * stamps its exact options object with a single-use ticket that `stream()`
   * consumes. A private `WeakSet`, a locally constructed object, and first-use
   * consumption make that exemption unavailable to callers.
   */
  #assertRunIdNotLive(runId: string): void {
    if (this.isRunLive(runId)) {
      throw new InvalidRunRequestError(
        'run id is live in the run registry — a registered run cannot be re-entered',
      );
    }
  }

  /** @internal */
  isRunLive(runId: string): boolean {
    return (
      this.#startRequesters.has(runId) ||
      this.#persistenceWaiters.has(runId) ||
      globalRunRegistry.has(runId) ||
      this.runRegistryInternal.has(runId)
    );
  }

  #snapshotCallOptions<T extends object>(
    options: T | undefined,
  ): T | undefined {
    return snapshotDurableCallOptions(options, this.#guardedCallOptionMapper);
  }

  /**
   * Enforce a caller-minted run ID before the inherited durable
   * `stream()` runs: without this the
   * optional `options.runId` would let core mint an unowned
   * `crypto.randomUUID()` upstream. The private run-ID guard rejects a missing or
   * non-path-safe value before delegating to core.
   * A host mints an opaque path-safe id and passes it as `options.runId`.
   */
  override async stream(
    messages: Parameters<DurableAgent<TAgentId, TTools, TOutput>['stream']>[0],
    options?: Parameters<DurableAgent<TAgentId, TTools, TOutput>['stream']>[1],
  ): Promise<
    Awaited<ReturnType<DurableAgent<TAgentId, TTools, TOutput>['stream']>>
  > {
    const hostStreamTicket =
      options !== undefined && this.#hostStreamTickets.has(options);
    if (hostStreamTicket) this.#hostStreamTickets.delete(options);
    const callOptions = this.#snapshotCallOptions(options);
    this.#assertCallerRunId(callOptions?.runId);
    if (!hostStreamTicket) this.#assertRunIdNotLive(callOptions.runId);
    let capturedSignal = false;
    if (
      !hostStreamTicket &&
      callOptions.requestContext === undefined &&
      isCreatedAgentSignal(messages)
    ) {
      this.#replayedSignals.set(callOptions.runId, messages);
      capturedSignal = true;
    }
    try {
      return await super.stream(messages, callOptions);
    } catch (error) {
      if (capturedSignal) this.#replayedSignals.delete(callOptions.runId);
      throw error;
    }
  }

  /**
   * Start a durable stream and wait until RunnerRuntime has persisted the
   * first suspended or terminal summary.
   *
   * The regular durable `stream()` returns after subscription setup while its
   * workflow starts asynchronously. Agent hosts need an authoritative summary
   * before answering a start request, but must keep the stream subscription
   * and replay cache intact for later HTTP observation.
   * It stamps the locally constructed stream options with the private,
   * single-use host ticket that permits only this first call through `stream()`.
   *
   * @internal
   */
  async streamUntilPersisted(
    messages: Parameters<DurableAgent<TAgentId, TTools, TOutput>['stream']>[0],
    options: NonNullable<
      Parameters<DurableAgent<TAgentId, TTools, TOutput>['stream']>[1]
    >,
    requestedBy: string,
    requestedByKind: ExecutionPrincipalKind,
    attemptToken = crypto.randomUUID(),
    scheduleDispatch: { scheduleId: string; dispatchId: string } | undefined,
    /**
     * The idempotency key the thread topology already RESERVED for this run.
     *
     * It travels down here because the execution fence's proof-only state
     * admits exactly the start whose key matches its nominated proof key, and
     * `RunnerRuntime.start` is where that comparison happens. It buys no
     * exactly-once property at this layer — the reservation above already did —
     * so nothing here validates it beyond passing it on.
     *
     * Parked per-runId beside the attempt token rather than threaded through
     * core, because core owns the call between `stream()` and
     * `executeWorkflow()` and carries no field this could ride in.
     */
    idempotencyKey: string | undefined,
    authority: AgentStartAuthority,
  ): Promise<
    Awaited<ReturnType<DurableAgent<TAgentId, TTools, TOutput>['stream']>>
  > {
    const callOptions = this.#snapshotCallOptions(options);
    this.#assertCallerRunId(callOptions?.runId);
    if (callOptions.untilIdle) {
      throw new InvalidRunRequestError(
        'streamUntilPersisted does not support untilIdle',
      );
    }
    if (!isExecutionPrincipalId(requestedBy)) {
      throw new InvalidRunRequestError('requestedBy is malformed');
    }
    if (!isExecutionPrincipalKind(requestedByKind)) {
      throw new InvalidRunRequestError('requestedByKind is malformed');
    }
    const capturedAuthority = captureAgentStartAuthority(
      authority,
      requestedBy,
      requestedByKind,
    );
    let capturedScheduleDispatch: typeof scheduleDispatch;
    if (scheduleDispatch !== undefined) {
      if (
        scheduleDispatch === null ||
        typeof scheduleDispatch !== 'object' ||
        Array.isArray(scheduleDispatch)
      ) {
        throw new Error('stored run lifecycle is malformed');
      }
      const { scheduleId, dispatchId } = scheduleDispatch;
      if (!isPathSafeId(scheduleId) || !isPathSafeId(dispatchId)) {
        throw new Error('stored run lifecycle is malformed');
      }
      capturedScheduleDispatch = Object.freeze({ scheduleId, dispatchId });
    }
    const runId = callOptions.runId;
    this.#assertRunIdNotLive(runId);
    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    const persisted = new Promise<void>((resolvePromise, rejectPromise) => {
      resolve = resolvePromise;
      reject = rejectPromise;
    });
    void persisted.catch(() => undefined);
    this.#persistenceWaiters.set(runId, { resolve, reject });
    this.#startRequesters.set(runId, requestedBy);
    this.#startRequesterKinds.set(runId, requestedByKind);
    this.#startAttemptTokens.set(runId, attemptToken);
    this.#startAuthorities.set(runId, capturedAuthority);
    if (capturedScheduleDispatch) {
      this.#startScheduleDispatches.set(runId, capturedScheduleDispatch);
    }
    if (idempotencyKey !== undefined) {
      this.#startIdempotencyKeys.set(runId, idempotencyKey);
    }
    const onError = callOptions.onError;
    try {
      const hostCallOptions: typeof callOptions = {
        ...callOptions,
        onError: async (data) => {
          reject(
            data.error instanceof Error
              ? data.error
              : new Error(String(data.error)),
          );
          await onError?.(data);
        },
      };
      this.#hostStreamTickets.add(hostCallOptions);
      const result = await this.stream(messages, hostCallOptions);
      await persisted;
      return result;
    } finally {
      this.#persistenceWaiters.delete(runId);
      this.#startRequesters.delete(runId);
      this.#startRequesterKinds.delete(runId);
      this.#startAttemptTokens.delete(runId);
      this.#startScheduleDispatches.delete(runId);
      this.#startIdempotencyKeys.delete(runId);
      this.#startAuthorities.delete(runId);
    }
  }

  /**
   * The same host-owned run-ID guard as {@link FlowsafeDurableAgent.stream} —
   * `generate()`
   * re-implements the durable setup and mints its own runId the same way when
   * one is not supplied.
   */
  override async generate(
    messages: Parameters<
      DurableAgent<TAgentId, TTools, TOutput>['generate']
    >[0],
    options?: Parameters<
      DurableAgent<TAgentId, TTools, TOutput>['generate']
    >[1],
  ): Promise<
    Awaited<ReturnType<DurableAgent<TAgentId, TTools, TOutput>['generate']>>
  > {
    const callOptions = this.#snapshotCallOptions(options);
    this.#assertCallerRunId(callOptions?.runId);
    this.#assertRunIdNotLive(callOptions.runId);
    try {
      return await super.generate(messages, callOptions);
    } catch (error) {
      // Core reconstructs an ERROR chunk with `new Error(message)` plus `name`,
      // which loses the InvalidRunRequestError prototype before generate sees it.
      if (
        error instanceof Error &&
        error.name === 'InvalidRunRequestError' &&
        error.message.startsWith(UNREGISTERED_RUN_REFUSAL_PREFIX) &&
        !(error instanceof InvalidRunRequestError)
      ) {
        const wrapped = new InvalidRunRequestError(error.message);
        Object.defineProperty(wrapped, 'cause', { value: error });
        throw wrapped;
      }
      throw error;
    }
  }

  /**
   * Enforce the caller-owned id before DurableAgent.prepare forwards it to
   * prepareForDurableExecution, which mints an absent id and registers the run.
   * That UUID satisfies downstream path checks and stays live until cleanup.
   */
  override async prepare(
    messages: Parameters<DurableAgent<TAgentId, TTools, TOutput>['prepare']>[0],
    options?: Parameters<DurableAgent<TAgentId, TTools, TOutput>['prepare']>[1],
  ): Promise<
    Awaited<ReturnType<DurableAgent<TAgentId, TTools, TOutput>['prepare']>>
  > {
    const callOptions = this.#snapshotCallOptions(options);
    this.#assertCallerRunId(callOptions?.runId);
    this.#assertRunIdNotLive(callOptions.runId);
    return super.prepare(messages, callOptions);
  }

  /**
   * Refuse core's single-run recovery. `DurableAgent.recover()` loads the
   * persisted `durable-agentic-loop` snapshot, rebuilds model/memory/processors
   * from it and re-drives the run with `createRun + run.restart()` — a second
   * execution path that never enters `executeWorkflow`, so no leg of it is
   * grant-derived, run-owned or snapshot-provenanced. Throw BEFORE the storage
   * read, so a mistaken call cannot even enumerate a run.
   */
  override async recover(
    _runId: Parameters<DurableAgent<TAgentId, TTools, TOutput>['recover']>[0],
    _options?: Parameters<
      DurableAgent<TAgentId, TTools, TOutput>['recover']
    >[1],
  ): Promise<never> {
    throw unavailableRunEntry('recover', BLOCKED_RUN_ENTRIES.recover);
  }

  /**
   * Refuse core's bulk recovery. `recoverActiveRuns()` is
   * {@link FlowsafeDurableAgent.listActiveRuns} plus a `recover()` per row, and
   * it is what `Mastra.recoverAllDurableAgents()` calls on every registered
   * durable agent — so a host reaches this blocked entry point without a
   * FlowSafe call site by opting into `recovery: { durableAgents: 'auto' }`.
   * That loop isolates each agent in its own try/catch, so this
   * refusal is logged there rather than failing boot. Refuse with an explicit
   * `runId` too: a single target is still a re-drive off RunnerRuntime.
   */
  override async recoverActiveRuns(
    _options?: Parameters<
      DurableAgent<TAgentId, TTools, TOutput>['recoverActiveRuns']
    >[0],
  ): Promise<never> {
    throw unavailableRunEntry(
      'recoverActiveRuns',
      BLOCKED_RUN_ENTRIES.recoverActiveRuns,
    );
  }

  /**
   * Refuse core's recovery discovery API. `listActiveRuns()` enumerates
   * `listWorkflowRuns({ workflowName: 'durable-agentic-loop', status:
   * 'running' })` narrowed by `agentId` plus the optional
   * `threadId`/`resourceId` filters the CALLER supplies, so it never consults
   * the host topology's per-principal run-ownership checks
   * (`resourceAccess().owner('run', …)`) and hands the caller run ids, thread
   * ids and resource ids for runs it does not own. Host run listing is the
   * topology's job, where ownership is checked.
   */
  override async listActiveRuns(
    _options?: Parameters<
      DurableAgent<TAgentId, TTools, TOutput>['listActiveRuns']
    >[0],
  ): Promise<never> {
    throw unavailableRunEntry(
      'listActiveRuns',
      BLOCKED_RUN_ENTRIES.listActiveRuns,
    );
  }

  /**
   * Refuse the Agent-level analogue of
   * {@link FlowsafeDurableAgent.listActiveRuns}. `listSuspendedRuns()` reads
   * the workflows store directly —
   * `listWorkflowRuns({ workflowName: 'agentic-loop', status: 'suspended' })`
   * — and narrows by `agentId` plus the optional `threadId`/`resourceId`
   * filters the CALLER supplies, the same scoping `listActiveRuns()` applies.
   * Same ground too: an unfiltered call returns run, thread and resource ids
   * across every principal that shares the agent.
   */
  override async listSuspendedRuns(
    _options?: Parameters<
      Agent<TAgentId, TTools, TOutput>['listSuspendedRuns']
    >[0],
  ): Promise<never> {
    throw unavailableRunEntry(
      'listSuspendedRuns',
      BLOCKED_RUN_ENTRIES.listSuspendedRuns,
    );
  }

  /**
   * Refuse the thread-level run enumerator.
   * `listActiveThreadRuns()` takes no arguments and returns a runId plus the
   * resourceId and threadId parsed out of the key for every thread on the pubsub
   * instance with a run in flight, and core keys that state by pubsub instance
   * rather than by agent — so it is the same discovery ground as
   * {@link FlowsafeDurableAgent.listActiveRuns} with the last scoping gone.
   * The in-process sibling `getActiveThreadRunId()` stays inherited because it
   * makes the caller name the (resourceId, threadId) pair it confirms.
   *
   * Signature caveat, and the reason this one is not `async`: the base declares
   * it SYNCHRONOUS (`listActiveThreadRuns(): ActiveThreadRun[]`), so an
   * `async … Promise<never>` refusal would not be assignable to it. A re-check
   * for a peer bump.
   */
  override listActiveThreadRuns(): never {
    throw unavailableRunEntry(
      'listActiveThreadRuns',
      BLOCKED_RUN_ENTRIES.listActiveThreadRuns,
    );
  }

  /** Refuse unscoped thread identity discovery under the second ground. */
  override async discoverThreadPeers(_options?: unknown): Promise<never> {
    throw unavailableRunEntry(
      'discoverThreadPeers',
      BLOCKED_RUN_ENTRIES.discoverThreadPeers,
    );
  }

  /** Refuse direct run cancellation under the sixth ground. */
  override abortRunStream(_runId: string): never {
    throw unavailableRunEntry(
      'abortRunStream',
      BLOCKED_RUN_ENTRIES.abortRunStream,
    );
  }

  /** Refuse direct thread cancellation under the sixth ground. */
  override abortThreadStream(_options: unknown): never {
    throw unavailableRunEntry(
      'abortThreadStream',
      BLOCKED_RUN_ENTRIES.abortThreadStream,
    );
  }

  /**
   * Refuse the multi-agent network start. `network()` does not touch the
   * durable-agentic-loop at all: it compiles a SEPARATE workflow and drives it
   * with `createRun + run.stream` on the default engine, so the whole
   * collaboration — every sub-agent leg and every tool call inside it — runs
   * with no per-leg grant context, no snapshot provenance and no RunSummary.
   * It also mints `options.runId || mastra.generateId() || randomUUID()`, the
   * exact unowned fallback this runner forbids: the host mints every run id.
   *
   * Signature caveat: the base method is OVERLOADED and generic in OUTPUT, so
   * `Parameters<>` sees only the LAST overload and is too narrow to satisfy the
   * first. The options parameter is therefore widened to `unknown` — the one
   * supertype that satisfies every overload at once. Re-check on every peer
   * bump; nothing here fails if core changes the shape.
   */
  override async network(
    _messages: Parameters<Agent<TAgentId, TTools, TOutput>['network']>[0],
    _options?: unknown,
  ): Promise<never> {
    throw unavailableRunEntry('network', BLOCKED_RUN_ENTRIES.network);
  }

  /**
   * Refuse the network resume — same loop and same ground as
   * {@link FlowsafeDurableAgent.network}, plus one fact of its own: under
   * `autoResumeSuspendedTools` it RECOVERS a suspended run id out of thread
   * memory, so a caller need not even name the run it re-drives.
   */
  override async resumeNetwork(
    _resumeData: Parameters<
      Agent<TAgentId, TTools, TOutput>['resumeNetwork']
    >[0],
    _options: Parameters<Agent<TAgentId, TTools, TOutput>['resumeNetwork']>[1],
  ): Promise<never> {
    throw unavailableRunEntry(
      'resumeNetwork',
      BLOCKED_RUN_ENTRIES.resumeNetwork,
    );
  }

  /**
   * Refuse the network tool-approval resume. It is a one-line forward to
   * {@link FlowsafeDurableAgent.resumeNetwork}; blocking it here closes the
   * same door from the side a tool-approval caller reaches for.
   */
  override async approveNetworkToolCall(
    _options: Parameters<
      Agent<TAgentId, TTools, TOutput>['approveNetworkToolCall']
    >[0],
  ): Promise<never> {
    throw unavailableRunEntry(
      'approveNetworkToolCall',
      BLOCKED_RUN_ENTRIES.approveNetworkToolCall,
    );
  }

  /** The decline half of {@link FlowsafeDurableAgent.approveNetworkToolCall}. */
  override async declineNetworkToolCall(
    _options: Parameters<
      Agent<TAgentId, TTools, TOutput>['declineNetworkToolCall']
    >[0],
  ): Promise<never> {
    throw unavailableRunEntry(
      'declineNetworkToolCall',
      BLOCKED_RUN_ENTRIES.declineNetworkToolCall,
    );
  }

  /**
   * Refuse the AI SDK v4 legacy execution path. `generateLegacy()` forwards
   * into AgentLegacyHandler, which converts and RUNS the agent's tools while
   * bypassing RunnerRuntime entirely, mints its own run id when the caller
   * omits one, and skips `requireAgentExecutionFGA` — the authorization gate
   * every SUPPORTED entry point calls, so this would run the agent without it.
   * (The network family skips that gate too; neither is unique in doing so.) It
   * persists no workflow run state, which is why it is refused on those
   * grounds rather than as a re-drive.
   *
   * Signature caveat: overloaded and generic in OUTPUT on the base, so this
   * signature is hand-derived and must be re-checked on every peer bump.
   */
  override async generateLegacy(
    _messages: Parameters<
      Agent<TAgentId, TTools, TOutput>['generateLegacy']
    >[0],
    _args?: Parameters<Agent<TAgentId, TTools, TOutput>['generateLegacy']>[1],
  ): Promise<never> {
    throw unavailableRunEntry(
      'generateLegacy',
      BLOCKED_RUN_ENTRIES.generateLegacy,
    );
  }

  /** The streaming half of {@link FlowsafeDurableAgent.generateLegacy}. */
  override async streamLegacy(
    _messages: Parameters<Agent<TAgentId, TTools, TOutput>['streamLegacy']>[0],
    _args?: Parameters<Agent<TAgentId, TTools, TOutput>['streamLegacy']>[1],
  ): Promise<never> {
    throw unavailableRunEntry('streamLegacy', BLOCKED_RUN_ENTRIES.streamLegacy);
  }

  /**
   * Refuse the thread-level tool approval. The name reads like a resume, but
   * only its tail is one. Called with `messages` and `approved`, it routes to
   * the thread runtime's continuation, which falls back to `randomUUID()` when
   * the caller named no run id and then STARTS a run under it — an unowned id
   * the host-owned run-id guard cannot distinguish from a real one, because it
   * is path-safe. Called with no active thread run, it reaches the blocked
   * suspended-run discovery instead. FlowSafe's own tool approval is a decided
   * ApprovalRecord resumed through the approval-decision path, which mints the
   * leg's grant; this mints nothing and owns nothing.
   *
   * Signature caveat: the base method is generic in OUTPUT, which
   * `Parameters<>` instantiates to its `undefined` default and so types too
   * narrowly to satisfy the base. The options parameter is widened to
   * `unknown` so it fits each instantiation. Re-check on a peer bump.
   */
  override async sendToolApproval(_options: unknown): Promise<never> {
    throw unavailableRunEntry(
      'sendToolApproval',
      BLOCKED_RUN_ENTRIES.sendToolApproval,
    );
  }

  /**
   * Refuse the thread-runtime target swap. It sets one private field that the
   * thread-runtime paths resolve their target through, falling back to `this`;
   * the refusal in `BLOCKED_RUN_ENTRIES` names those paths, for the core it is
   * written against. So the containment those inherited members rely on is
   * virtual dispatch on `this`, and one call to this setter moves every run they
   * start, and their replay targets with them, onto an agent with none of these
   * overrides in the chain. That is the fourth ground reached by installing a
   * second execution surface rather than by calling one.
   *
   * Signature caveat: the parameter is `unknown` rather than core's
   * `Agent<any, any, any, any>`. `unknown` satisfies that base without
   * depending on method bivariance.
   */
  override __setThreadRuntimeAgent(_agent: unknown): never {
    throw unavailableRunEntry(
      '__setThreadRuntimeAgent',
      BLOCKED_RUN_ENTRIES.__setThreadRuntimeAgent,
    );
  }

  /**
   * Refuse binding channels. The base forwards them to the wrapped agent,
   * which becomes their dispatch target outside RunnerRuntime: the fourth
   * ground reached by installation.
   */
  override setChannels(_agentChannels: unknown): never {
    throw unavailableRunEntry('setChannels', BLOCKED_RUN_ENTRIES.setChannels);
  }

  /**
   * Refuse declaring schedules. The base forwards them to the wrapped agent,
   * which a Mastra schedule worker fires outside RunnerRuntime: the fourth
   * ground reached by installation.
   *
   * Signature caveat: the parameter is `unknown` rather than core's
   * `DeclaredAgentSchedule[]`, which it satisfies without depending on method
   * bivariance.
   */
  override __setDeclaredSchedules(_schedules: unknown): never {
    throw unavailableRunEntry(
      '__setDeclaredSchedules',
      BLOCKED_RUN_ENTRIES.__setDeclaredSchedules,
    );
  }

  /**
   * Refuse memory installation after construction under the fifth ground.
   *
   * Signature caveat: `unknown` accepts core's memory parameter without
   * depending on method bivariance.
   */
  override __setMemory(_memory: unknown): never {
    throw unavailableRunEntry('__setMemory', BLOCKED_RUN_ENTRIES.__setMemory);
  }

  /**
   * Refuse pub/sub installation after construction under the fifth ground.
   *
   * Signature caveat: `unknown` accepts core's pub/sub parameter without
   * depending on method bivariance.
   */
  override __setPubSub(_pubsub: unknown): never {
    throw unavailableRunEntry('__setPubSub', BLOCKED_RUN_ENTRIES.__setPubSub);
  }

  /**
   * Refuse Mastra registration. `Mastra.addAgent` calls this first, so the
   * refusal throws before the registry write and before the loop workflow is
   * added to that Mastra.
   *
   * Signature caveat: the parameter is `unknown` rather than core's `Mastra`,
   * which it satisfies without depending on method bivariance.
   */
  override __setMastra(_mastra: unknown): never {
    throw unavailableRunEntry('__setMastra', BLOCKED_RUN_ENTRIES.__setMastra);
  }

  /**
   * The member {@link FlowsafeDurableAgent.__setMastra} forwards to, which
   * `Agent.listAgents` calls directly on a static sub-agent.
   */
  override __registerMastra(_mastra: unknown): never {
    throw unavailableRunEntry(
      '__registerMastra',
      BLOCKED_RUN_ENTRIES.__registerMastra,
    );
  }

  /**
   * Refuse core's terminal snapshot cleanup. Its only call sites are the base
   * `executeWorkflow` (overridden here), the blocked `resume()` and the blocked
   * `recover()`, so nothing this class drives reaches it; blocking keeps the
   * snapshot rows — which deployment-scoped retention purge owns — from being
   * dropped out from under that owner by a future internal caller.
   *
   * The override keeps core's best-effort deletion from taking ownership of
   * these rows if an internal execution path reaches the cleanup method.
   */
  protected override async deleteRunSnapshots(_runId: string): Promise<never> {
    throw unavailableRunEntry(
      'deleteRunSnapshots',
      BLOCKED_RUN_ENTRIES.deleteRunSnapshots,
    );
  }

  /**
   * Refuse core's durable resume. From 1.53.0 a registry MISS makes it load the
   * persisted `durable-agentic-loop` snapshot, rehydrate through `prepare()`
   * with the full application processor chain, and re-drive the run with
   * `createRun + run.resume` — below `executeWorkflow`, so the leg carries no
   * minted grant and no snapshot provenance. The runner's resume path is
   * {@link FlowsafeDurableAgent.resumeViaRuntime}.
   */
  override async resume(
    _runId: Parameters<DurableAgent<TAgentId, TTools, TOutput>['resume']>[0],
    _resumeData: Parameters<
      DurableAgent<TAgentId, TTools, TOutput>['resume']
    >[1],
    _options?: Parameters<DurableAgent<TAgentId, TTools, TOutput>['resume']>[2],
  ): Promise<never> {
    throw unavailableRunEntry('resume', BLOCKED_RUN_ENTRIES.resume);
  }

  /**
   * Refuse the base-`Agent`-shaped resume. Core overrides `resumeStream()` on
   * DurableAgent precisely so an `Agent`-API caller lands on the durable
   * `resume()`; blocking it here closes that same door from the other side.
   */
  override async resumeStream(
    _resumeData: Parameters<
      DurableAgent<TAgentId, TTools, TOutput>['resumeStream']
    >[0],
    _streamOptions?: Parameters<
      DurableAgent<TAgentId, TTools, TOutput>['resumeStream']
    >[1],
  ): Promise<never> {
    throw unavailableRunEntry('resumeStream', BLOCKED_RUN_ENTRIES.resumeStream);
  }

  /**
   * Refuse the drain-to-completion resume. `resumeGenerate()` forwards straight
   * to `resume()`, so it inherits the same below-the-seam re-drive.
   */
  override async resumeGenerate(
    _runId: Parameters<
      DurableAgent<TAgentId, TTools, TOutput>['resumeGenerate']
    >[0],
    _resumeData: Parameters<
      DurableAgent<TAgentId, TTools, TOutput>['resumeGenerate']
    >[1],
    _options?: Parameters<
      DurableAgent<TAgentId, TTools, TOutput>['resumeGenerate']
    >[2],
  ): Promise<never> {
    throw unavailableRunEntry(
      'resumeGenerate',
      BLOCKED_RUN_ENTRIES.resumeGenerate,
    );
  }

  /**
   * Refuse Mastra's own tool-approval resume. Tool approval in FlowSafe is a
   * decided ApprovalRecord resumed through the approval-decision path, which
   * mints the leg's connector grant; `approveToolCall()` funnels into
   * `resumeStream()` -> `resume()` and mints nothing.
   */
  override async approveToolCall(
    _options: Parameters<
      DurableAgent<TAgentId, TTools, TOutput>['approveToolCall']
    >[0],
  ): Promise<never> {
    throw unavailableRunEntry(
      'approveToolCall',
      BLOCKED_RUN_ENTRIES.approveToolCall,
    );
  }

  /** The decline half of {@link FlowsafeDurableAgent.approveToolCall}. */
  override async declineToolCall(
    _options: Parameters<
      DurableAgent<TAgentId, TTools, TOutput>['declineToolCall']
    >[0],
  ): Promise<never> {
    throw unavailableRunEntry(
      'declineToolCall',
      BLOCKED_RUN_ENTRIES.declineToolCall,
    );
  }

  /**
   * The 1.53.0 generate-shaped tool-approval entries. They funnel into
   * `resumeGenerate()` -> `resume()`, so they are the same entry point with a
   * different return type.
   *
   * Signature caveat for each: the base method is GENERIC in its OUTPUT
   * type, which `Parameters<>` cannot carry, so the parameter type below is
   * hand-written rather than derived. Re-check it against the base on every
   * peer bump — nothing here fails if core changes the shape.
   */
  override async approveToolCallGenerate<OUTPUT = undefined>(
    _options: AgentExecutionOptions<OUTPUT> & {
      runId: string;
      toolCallId?: string;
    },
  ): Promise<never> {
    throw unavailableRunEntry(
      'approveToolCallGenerate',
      BLOCKED_RUN_ENTRIES.approveToolCallGenerate,
    );
  }

  /**
   * The decline half of {@link FlowsafeDurableAgent.approveToolCallGenerate},
   * including its hand-written-signature caveat.
   */
  override async declineToolCallGenerate<OUTPUT = undefined>(
    _options: AgentExecutionOptions<OUTPUT> & {
      runId: string;
      toolCallId?: string;
    },
  ): Promise<never> {
    throw unavailableRunEntry(
      'declineToolCallGenerate',
      BLOCKED_RUN_ENTRIES.declineToolCallGenerate,
    );
  }

  async #rehydrateRegistry(options: {
    runId: string;
    requestContext: RequestContext;
    memory?: DurableAgentStreamOptions<TOutput>['memory'];
  }): Promise<void> {
    const wrappedAgent = this.#wrappedAgent;
    let inputProcessors: Awaited<
      ReturnType<Agent<TAgentId, TTools, TOutput>['listInputProcessors']>
    > = [];
    let llmRequestInputProcessors: Awaited<
      ReturnType<Agent<TAgentId, TTools, TOutput>['__listLLMRequestProcessors']>
    > = [];
    const rehydrationAgent = new Proxy(wrappedAgent, {
      get: (target, property) => {
        if (property === 'listInputProcessors') {
          return async (requestContext?: RequestContext) => {
            inputProcessors = await target.listInputProcessors(requestContext);
            return inputProcessors.filter((processor) =>
              this.#rehydrationInputProcessorIds.has(processor.id),
            );
          };
        }
        if (property === '__listLLMRequestProcessors') {
          return async (
            ...args: Parameters<
              Agent<TAgentId, TTools, TOutput>['__listLLMRequestProcessors']
            >
          ) => {
            llmRequestInputProcessors = await target.__listLLMRequestProcessors(
              ...args,
            );
            return [];
          };
        }
        const value = Reflect.get(target, property, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
    const preparationOptions =
      options.memory !== undefined
        ? ({
            memory: options.memory,
          } as NonNullable<
            Parameters<DurableAgent<TAgentId, TTools, TOutput>['prepare']>[1]
          >)
        : undefined;
    const preparation = await prepareForDurableExecution({
      agent: rehydrationAgent,
      messages: [],
      ...(preparationOptions !== undefined
        ? { options: preparationOptions }
        : {}),
      runId: options.runId,
      requestContext: options.requestContext,
      mastra: this.getMastraInstance(),
    });
    const tripwire = preparation.registryEntry.tripwire;
    if (tripwire) {
      preparation.registryEntry.cleanup?.();
      throw new Error(
        `Durable agent registry rehydration denied: ${tripwire.reason}`,
      );
    }
    preparation.registryEntry.inputProcessors = inputProcessors;
    preparation.registryEntry.llmRequestInputProcessors =
      llmRequestInputProcessors;
    let cleaned = false;
    const cleanup = () => {
      if (cleaned) return;
      cleaned = true;
      preparation.registryEntry.cleanup?.();
    };
    const registryEntry = {
      ...preparation.registryEntry,
      cleanup,
    };
    this.runRegistryInternal.registerWithMessageList(
      options.runId,
      registryEntry,
      preparation.messageList,
      {
        threadId: preparation.threadId,
        resourceId: preparation.resourceId,
      },
    );
    globalRunRegistry.set(options.runId, {
      ...registryEntry,
      messageList: preparation.messageList,
    });
  }

  /**
   * Rehydrate a suspended durable-agent run after isolate eviction, restore its
   * active thread registration, then resume through RunnerRuntime so approval
   * grant derivation and snapshot provenance remain authoritative.
   * Hosts expose this only from their trusted approval-decision topology.
   */
  async resumeViaRuntime(options: {
    runId: string;
    requestedBy: string;
    step?: string | string[];
    resumeData?: unknown;
    memory?: DurableAgentStreamOptions<TOutput>['memory'];
  }): Promise<RunSummary> {
    this.#assertCallerRunId(options.runId);
    const memory = this.#guardedCallOptionMapper
      ? (this.#guardedCallOptionMapper(
          'memory',
          options.memory,
        ) as typeof options.memory)
      : options.memory;
    let rehydrated = false;
    let finishThreadRegistration: (() => void) | undefined;
    try {
      const summary = await this.#runtime.resume(
        this.getWorkflow().id,
        options.runId,
        {
          ...(options.step !== undefined ? { step: options.step } : {}),
          ...(options.resumeData !== undefined
            ? { resumeData: options.resumeData }
            : {}),
          requestedBy: options.requestedBy,
          requestedByKind: 'human',
          prepareExecution: async (requestContext) => {
            await this.#rehydrateRegistry({
              runId: options.runId,
              requestContext,
              ...(memory !== undefined ? { memory } : {}),
            });
            rehydrated = true;
            const observed = await this.observe(options.runId);
            const completion = new Promise<void>((resolve) => {
              finishThreadRegistration = resolve;
            });
            await this.#threadRuntime?.registerRun(
              this as unknown as Parameters<
                Mastra['agentThreadStreamRuntime']['registerRun']
              >[0],
              bindThreadCompletion(observed.output, completion),
              {
                runId: options.runId,
                ...(memory !== undefined ? { memory } : {}),
              } as Parameters<
                Mastra['agentThreadStreamRuntime']['registerRun']
              >[2],
              this.getPubSub(),
            );
          },
        },
      );
      if (summary.status === 'failed') {
        await this.#publishTerminalError(
          options.runId,
          new Error(summary.error ?? 'Durable agent workflow resume failed'),
        );
        this.runRegistryInternal.cleanup(options.runId);
        globalRunRegistry.delete(options.runId);
      }
      return summary;
    } catch (error) {
      if (rehydrated) {
        await this.#publishTerminalError(options.runId, error);
        this.runRegistryInternal.cleanup(options.runId);
        globalRunRegistry.delete(options.runId);
      }
      throw error;
    } finally {
      finishThreadRegistration?.();
    }
  }

  /**
   * Publish one terminal ERROR attempt without throwing. Call sites publish
   * before cleanup because `emitError()` closes spans through the live global
   * registry entry.
   */
  async #publishTerminalError(runId: string, error: unknown): Promise<boolean> {
    const terminalError =
      error instanceof Error ? error : new Error(String(error));
    try {
      await this.emitError(runId, terminalError);
      return true;
    } catch {
      console.error(
        JSON.stringify({
          type: 'durable-agent-terminal-error-publication-failed',
          runId,
        }),
      );
      return false;
    }
  }

  /**
   * Preserve unowned input before terminal refusal, by the guarded input
   * chain's verdict on the call:
   * - no tripwire: the prepared input;
   * - a tripwire from Breakwater's RBAC gate on a call whose created signal
   *   `stream()` captured: that signal;
   * - any other tripwire, an RBAC one on a call with no capture included:
   *   nothing.
   *
   * `stream()` captures a created signal only on a start that carries neither
   * a host ticket nor a request context: a drain after a run
   * `resumeViaRuntime()` registered, which the gate refuses for its missing
   * actor without reading content. On a guarded agent such a call has no
   * actor, so the gate refuses it before any application input processor
   * runs. Host code that calls `stream()` the same way, with a created signal
   * or an object carrying the signal brand as input, has that input kept when
   * the gate refuses it.
   *
   * The verdict comes from the tripwire rather than from the prepared list, so
   * it holds whether or not the guard removes refused input from the list. A
   * bounded best-effort write prevents hung memory from blocking the ERROR
   * that heals thread state.
   */
  async #persistUnownedInput(
    runId: string,
    workflowInput: DurableAgenticWorkflowInput,
    replayedSignal: CreatedAgentSignal | undefined,
  ): Promise<void> {
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        (async () => {
          const state = workflowInput.state;
          const threadId = state?.threadId;
          if (!threadId || state?.threadExists !== true) return;
          // Explicit read-only memory forbids preserving denied input.
          if (state.memoryConfig?.readOnly === true) return;
          const registryEntry = globalRunRegistry.get(runId);
          const tripwire = registryEntry?.tripwire;
          const memory = await this.getMemory({
            requestContext: registryEntry?.requestContext,
          });
          if (!memory) return;
          let candidates: MastraDBMessage[];
          if (tripwire === undefined) {
            const list = new MessageList({
              threadId,
              resourceId: state.resourceId,
            }).deserialize(workflowInput.messageListState);
            const inputIds = list.makeMessageSourceChecker().input;
            candidates = list.get.all
              .db()
              .filter((message) => inputIds.has(message.id));
          } else if (
            tripwire.processorId === BREAKWATER_RBAC_PROCESSOR_ID &&
            replayedSignal !== undefined
          ) {
            candidates = [
              signalToMastraDBMessage(replayedSignal, {
                threadId,
                resourceId: state.resourceId,
              }),
            ];
          } else {
            return;
          }
          const messages = candidates
            // Mastra's own persist lane stores no transient signal either.
            .filter((message) => !isTransientSignalMessage(message))
            // Honor markPersistenceForbidden's route-to-runner metadata contract.
            .filter(
              (message) =>
                !isMastraSignalMessage(message) ||
                mastraDBMessageToSignal(message).metadata?.[
                  FLOWSAFE_PERSISTENCE_FORBIDDEN
                ] !== true,
            );
          if (messages.length) await memory.saveMessages({ messages });
        })(),
        new Promise<never>((_resolve, reject) => {
          timeout = setTimeout(
            () =>
              reject(
                new Error(
                  `unowned input persistence timed out after ${UNOWNED_INPUT_PERSIST_TIMEOUT_MS} ms`,
                ),
              ),
            UNOWNED_INPUT_PERSIST_TIMEOUT_MS,
          );
        }),
      ]);
    } catch {
      console.error(
        JSON.stringify({
          type: 'durable-agent-unowned-input-persistence-failed',
          runId,
        }),
      );
    } finally {
      if (timeout !== undefined) clearTimeout(timeout);
    }
  }

  /** @internal Include validated legacy agent data for ordinary host operations. */
  authoritativeAgentStartState(
    expectedRuntime: RunnerRuntime,
    threadId: string,
    runId: string,
    options: { readonly includeLegacy: true },
  ): Promise<AuthoritativeAgentStartState | LegacyAgentRunState | null>;
  /** @internal Select the actual private Runtime/workflow and immutable agent owner once. */
  authoritativeAgentStartState(
    expectedRuntime: RunnerRuntime,
    threadId: string,
    runId: string,
  ): Promise<AuthoritativeAgentStartState | null>;
  async authoritativeAgentStartState(
    expectedRuntime: RunnerRuntime,
    threadId: string,
    runId: string,
    options?: { readonly includeLegacy: true },
  ): Promise<AuthoritativeAgentStartState | LegacyAgentRunState | null> {
    const includeLegacy = options?.includeLegacy === true;
    const runtime = this.#runtime;
    const workflowId = this.getWorkflow().id;
    const agentId = this.#wrappedAgent.id;
    try {
      if (
        expectedRuntime !== runtime ||
        !isPathSafeId(threadId) ||
        !isPathSafeId(runId)
      )
        throw new Error('agent observation selector is invalid');
      const state = includeLegacy
        ? await runtime.authoritativeStartState(workflowId, runId, {
            includeLegacy: true,
          })
        : await runtime.authoritativeStartState(workflowId, runId);
      if (state === null) return null;
      if (state.kind === 'legacy') {
        if (!includeLegacy)
          throw new Error('legacy agent observation requires explicit opt-in');
        const context = state.snapshot.requestContext;
        const input = state.snapshot.context?.input as
          | {
              agentId?: unknown;
              runId?: unknown;
              messageListState?: {
                memoryInfo?: {
                  threadId?: unknown;
                  resourceId?: unknown;
                } | null;
              };
            }
          | undefined;
        const correlation = context?.['breakwater.auditContext'] as
          | Record<string, unknown>
          | undefined;
        const memory = input?.messageListState?.memoryInfo;
        const observedAgentId = input?.agentId;
        const observedThreadId = context?.threadId;
        if (!isPathSafeId(observedAgentId) || !isPathSafeId(observedThreadId))
          throw new Error('legacy agent observation identity is malformed');
        const observedResourceId = resourceIdFromKey(observedThreadId);
        if (
          state.address.workflowId !== workflowId ||
          state.address.runId !== runId ||
          (input?.runId !== undefined && input.runId !== runId) ||
          context?.runId !== runId ||
          context.resourceId !== observedResourceId ||
          correlation?.agentId !== observedAgentId ||
          correlation.threadId !== observedThreadId ||
          correlation.resourceId !== observedResourceId ||
          (memory !== null &&
            (memory?.threadId !== observedThreadId ||
              memory.resourceId !== observedResourceId))
        )
          throw new Error(
            'legacy agent observation context contradicts identity',
          );
        if (observedAgentId !== agentId || observedThreadId !== threadId)
          throw new AgentRunSelectorMismatchError(workflowId, runId);
        return { ...state, threaded: memory !== null };
      }
      const identity = state.provenance.startIdentity;
      const threaded = state.provenance.agentStart?.threaded;
      if (
        identity?.target.kind !== 'agent' ||
        typeof threaded !== 'boolean' ||
        state.execution.workflowId !== workflowId ||
        state.execution.runId !== runId
      )
        throw new Error('agent observation identity is malformed');
      const observedAgentId = identity.target.id;
      const observedThreadId = identity.target.threadId;
      const observedResourceId = resourceIdFromKey(observedThreadId);
      const context = state.snapshot.requestContext;
      const record = (value: unknown): Record<string, unknown> => {
        if (value === null || typeof value !== 'object' || Array.isArray(value))
          throw new Error('agent observation context is malformed');
        return value as Record<string, unknown>;
      };
      const check = (value: unknown, selectors: Record<string, string>) => {
        if (value === undefined) return;
        const values = record(value);
        for (const [key, expected] of Object.entries(selectors))
          if (Object.hasOwn(values, key) && values[key] !== expected)
            throw new Error('agent observation context contradicts identity');
      };
      check(context, {
        runId,
        threadId: observedThreadId,
        resourceId: observedResourceId,
      });
      check(context?.['breakwater.auditContext'], {
        agentId: observedAgentId,
        threadId: observedThreadId,
        resourceId: observedResourceId,
      });
      const input = state.snapshot.context?.input;
      if (input !== undefined) {
        check(input, { agentId: observedAgentId, runId });
        const messageList = record(input).messageListState;
        if (messageList !== undefined) {
          const values = record(messageList);
          if (Object.hasOwn(values, 'memoryInfo')) {
            const memory = values.memoryInfo;
            if (memory === null) {
              if (threaded) throw new Error('agent mode contradicts memory');
            } else {
              const selected = record(memory);
              if (
                !threaded ||
                selected.threadId !== observedThreadId ||
                selected.resourceId !== observedResourceId
              )
                throw new Error('agent mode contradicts memory');
            }
          }
        }
      }
      if (observedAgentId !== agentId || observedThreadId !== threadId)
        throw new AgentRunSelectorMismatchError(workflowId, runId);
      return {
        ...state,
        execution: { ...state.execution, ...identity },
        threaded,
      } as AuthoritativeAgentStartState;
    } catch (cause) {
      if (cause instanceof RunStateUnreadableError) throw cause;
      throw new RunStateUnreadableError(workflowId, runId, { cause });
    }
  }

  /** @internal Initial identity can correlate proof without granting replay success. */
  async proofExecutionFor(
    expectedRuntime: RunnerRuntime,
    threadId: string,
    runId: string,
  ): Promise<D1RunExecutionIdentity | undefined> {
    const state = await this.authoritativeAgentStartState(
      expectedRuntime,
      threadId,
      runId,
    );
    if (state?.storage !== 'd1') return undefined;
    return {
      tablePrefix: state.execution.tablePrefix,
      workflowId: state.execution.workflowId,
      runId: state.execution.runId,
      startToken: state.execution.startToken,
    };
  }

  /**
   * Drive the durable-agentic-loop through RunnerRuntime instead of the base
   * `createRun + run.start`. stream()/generate() have already parked the
   * non-serializables (model/tools/messageList) on the in-process run registry
   * keyed by this runId, so the loop the runtime starts resolves them in-isolate
   * while the runtime mints the per-leg grant context. The runId guard here is
   * defense in depth — the public boundary (stream/generate) already enforced
   * the host-owned run-ID rule; this catches any future internal caller.
   */
  protected override async executeWorkflow(
    runId: string,
    workflowInput: DurableAgenticWorkflowInput,
  ): Promise<void> {
    this.#assertCallerRunId(runId);
    const waiter = this.#persistenceWaiters.get(runId);
    let summary: RunSummary;
    try {
      // getWorkflow() is memoized and its id is the shared loop id the factory
      // registered; driving that exact id keeps the started run and the
      // registered workflow in lockstep.
      const requestedBy = this.#startRequesters.get(runId);
      const requestedByKind = this.#startRequesterKinds.get(runId);
      const attemptToken = this.#startAttemptTokens.get(runId);
      const scheduleDispatch = this.#startScheduleDispatches.get(runId);
      const idempotencyKey = this.#startIdempotencyKeys.get(runId);
      const authority = this.#startAuthorities.get(runId);
      if (requestedBy === undefined || requestedByKind === undefined) {
        if (requestedBy !== undefined || requestedByKind !== undefined) {
          throw new InvalidRunRequestError(
            'requestedBy and requestedByKind must be provided together',
          );
        }
        // No #startRequesters entry means the host start seam never registered
        // this id, so core minted it below our boundary. Such a run has no
        // ownership record or trusted engine-leg context. Preserve what the
        // input chain's verdict allows, including a signal stream() captured
        // for this id, then close the stream so core can clean up its maps and
        // release the lease transferred to this id.
        const replayedSignal = this.#replayedSignals.get(runId);
        this.#replayedSignals.delete(runId);
        await this.#persistUnownedInput(runId, workflowInput, replayedSignal);
        const refusal = new InvalidRunRequestError(
          `${UNREGISTERED_RUN_REFUSAL_PREFIX}run '${runId}' was not registered by the host start seam — the durable-agent runner never executes a run it does not own`,
        );
        let published = false;
        try {
          published =
            (await this.#publishTerminalError(runId, refusal)) ||
            (await this.#publishTerminalError(runId, refusal));
        } finally {
          this.#replayedSignals.delete(runId);
          this.runRegistryInternal.cleanup(runId);
          globalRunRegistry.delete(runId);
        }
        if (!published) throw refusal;
        return;
      }
      const {
        runId: coreRunId,
        agentId: coreAgentId,
        ...payload
      } = workflowInput;
      if (!authority) {
        throw new InvalidRunRequestError(
          'registered run is missing agent start authority',
        );
      }
      if (
        coreRunId !== runId ||
        authority.startIdentity.target.id !== this.#wrappedAgent.id ||
        coreAgentId !== authority.startIdentity.target.id
      ) {
        throw new InvalidRunRequestError(
          'Core input does not match agent start authority',
        );
      }
      const workflow = this.getWorkflow();
      summary = await this.#runtime.start(workflow.id, {
        runId,
        inputData: { ...payload, runId: coreRunId, agentId: coreAgentId },
        ...(attemptToken === undefined ? {} : { attemptToken }),
        ...(scheduleDispatch === undefined ? {} : { scheduleDispatch }),
        ...(idempotencyKey === undefined ? {} : { idempotencyKey }),
        requestedBy,
        requestedByKind,
        mutationEpoch: authority.mutationEpoch,
        startIdentity: authority.startIdentity,
        agentStart: authority.agentStart,
        onPreparedStartIdentity: authority.onPreparedStartIdentity,
        runOwnerGuard: authority.runOwnerGuard,
        startReservation: authority.startReservation,
      });
      waiter?.resolve();
    } catch (error) {
      waiter?.reject(error);
      throw error;
    }
    // Mirror the base: a FAILED run emits an error onto the agent's stream so
    // observe()/onError see it. A SUSPENDED run is the approval-gate path — it
    // returns normally and the host bridges the suspension to the approval
    // queue. emitError publishes on this.pubsub, which the constructor defaults
    // to the runtime's identity so the event reaches the run's observers.
    if (summary.status === 'failed') {
      await this.emitError(
        runId,
        new Error(summary.error ?? 'Durable agent workflow execution failed'),
      );
    }
  }
}

/**
 * Wrap an Agent as a {@link FlowsafeDurableAgent} and register its loop workflow
 * and raw agent on the runtime. The workflow uses the same `runtime.register`
 * path init()'s boundCreateWorkflow uses; the raw agent lets Mastra resolve the
 * durable input's `agentId` after isolate eviction.
 *
 * Workflow registration is IDEMPOTENT by its shared id: every durable agent
 * compiles to the ONE 'durable-agentic-loop' workflow, while the `agentId` in
 * each run's input selects one of the uniquely registered raw agents. A second
 * createFlowsafeDurableAgent on a shared runtime therefore registers its agent
 * but must not throw 'duplicate workflow id'. Like init()'s createWorkflow,
 * registration also throws once runs have started (the Mastra instance is
 * frozen) — call this at host setup, before any run.
 *
 * Multi-agent caveat: the shared loop bakes in the FIRST registrant's `maxSteps`
 * (it is compiled into the isTaskComplete step) as the DEFAULT — a per-call
 * `stream(msg, { maxSteps })` still overrides it, so this only bites a
 * second-plus agent that relies on its CONSTRUCTOR budget. Agents needing
 * distinct default step budgets take separate runtimes.
 */
export function createFlowsafeDurableAgent<
  TAgentId extends string = string,
  TTools extends ToolsInput = ToolsInput,
  TOutput = undefined,
>(
  options: FlowsafeDurableAgentOptions<TAgentId, TTools, TOutput>,
): FlowsafeDurableAgent<TAgentId, TTools, TOutput> {
  const durableAgent = new FlowsafeDurableAgent(options);
  options.runtime.registerAgent(options.agent);
  const workflow = durableAgent.getWorkflow();
  if (!options.runtime.workflowIds().includes(workflow.id)) {
    // getWorkflow()'s concrete engine generics are not single-cast-assignable to
    // AnyWorkflow, so the double cast through `unknown` is required.
    options.runtime.register(workflow as unknown as AnyWorkflow);
  }
  return durableAgent;
}
