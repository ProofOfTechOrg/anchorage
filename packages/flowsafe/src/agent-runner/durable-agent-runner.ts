// SPDX-License-Identifier: Apache-2.0
// FlowsafeDurableAgent — drive Mastra's durable-agent loop through the ONE
// RunnerRuntime chokepoint (DL-001/DL-010) so every agent leg inherits the
// substrate's invariants: INV-1 server-minted runIds, per-leg
// requestContextForRun grant derivation, snapshot provenance, RunSummary, and
// retention purge — with no second execution path to audit.
//
// The mechanic (validated against @mastra/core 1.50.0 dist, spike S1):
// DurableAgent compiles the agent loop to the default-engine workflow
// 'durable-agentic-loop' (agent/durable index.js: AGENTIC_LOOP :62,
// getWorkflow() :5936, agent-agnostic — the agent is resolved per run by the
// `agentId` in the input, so ONE registered loop serves every durable agent).
// `DurableAgent.stream()` resolves the non-serializable model/tools onto the
// in-process globalRunRegistry (keyed by runId), builds a serializable
// DurableAgenticWorkflowInput, then calls the OVERRIDABLE
// `executeWorkflow(runId, workflowInput)` — the documented subclass seam
// (durable-agent.d.ts: "Subclasses override this method to customize how the
// workflow is executed"; EventedAgent/InngestAgent override it the same way).
// The base runs it via `workflow.createRun() + run.start()`; we route it
// through `runtime.start('durable-agentic-loop', { runId, inputData })` instead.
//
// Why this composes the grant-only doctrine for free: the durable tool-call
// step hands `tool.execute` the ENGINE-LEG requestContext from its step params
// (index.js: `const { ..., requestContext } = params` :3138 ->
// `toolOptions = { ..., requestContext }` :3339 ->
// `tool.execute(cleanedArgs, toolOptions)` :3642), NOT the stream()-time
// registry copy. Under runtime drive that engine-leg context is exactly what
// #requestContextFor mints per leg — so approvalGrantProvider's
// `breakwater.connectorGrants` grant reaches the connector write gate with
// zero extra wiring, and a forged/self resume that mints no grant fails closed
// there (the registry copy is read only for the fail-closed, over-require-safe
// approval PRE-check, S5).
//
// INV-1 at the boundary: stream()/generate()/prepare() are the THREE inherited
// minting entry points — each takes an OPTIONAL runId and, when it is absent,
// lets core mint an unowned crypto.randomUUID() upstream
// (prepareForDurableExecution, agent/durable index.js:589) that
// PATH_SAFE_ID_PATTERN then accepts, slipping past executeWorkflow's guard AND
// RunnerRuntime.start's (the exact fallback INV-1 forbids). All three are
// overridden ONLY to REQUIRE a caller-minted runId before delegating to super —
// and prepare() also REGISTERS the run under that id (index.js:5984), so an
// unguarded prepare() strands an unowned run in the registry. streamUntilIdle()
// needs no override: it drives agent.stream() (index.js:368), so the stream()
// guard already covers it. resume() is NOT overridden to be client-facing
// (A-D2/P8): resume flows ONLY through the approval-decision path
// (ApprovalService.decide -> resumeViaRuntime -> runtime.resume), which derives
// grants on the (suspendedAt, resumeCount) fingerprint. The inherited
// resume()/approveToolCall()/declineToolCall() still exist and never mint (they
// read the run-registry entry), but no route wires them.
//
// Live-isolate scope: the loop resolves the tool's execute closure from the
// in-process globalRunRegistry (populated by stream()). A DO holds one run in
// one isolate (P1), so a resume decided before eviction finds it. A resume
// AFTER eviction must first rehydrate that registry without replaying
// application input processors. resumeViaRuntime() rebuilds the registry with
// complete runtime processor lists after invoking only reserved RBAC during
// empty-message preparation, then drives runtime.resume().

import type { Agent, ToolsInput } from '@mastra/core/agent';
import {
  DurableAgent,
  type DurableAgentConfig,
  type DurableAgenticWorkflowInput,
  type DurableAgentStreamOptions,
  globalRunRegistry,
  prepareForDurableExecution,
} from '@mastra/core/agent/durable';
import type { Mastra } from '@mastra/core/mastra';
import type { RequestContext } from '@mastra/core/request-context';
import type { AnyWorkflow } from '@mastra/core/workflows';

import {
  type ExecutionPrincipalKind,
  isExecutionPrincipalId,
  isExecutionPrincipalKind,
} from '../approval-api/principal.js';
import {
  InvalidRunRequestError,
  isPathSafeId,
  type RunnerRuntime,
  type RunSummary,
} from '../do-runner/index.js';

/**
 * The shared workflow id every durable-agent loop compiles to (core's
 * DurableAgentDefaults.AGENTIC_LOOP). Exposed so hosts and tests can reference
 * the registered id without a magic string.
 */
export const DURABLE_AGENTIC_LOOP_WORKFLOW_ID = 'durable-agentic-loop';

function bindThreadCompletion<T extends object>(
  output: T,
  completion: Promise<void>,
): T {
  return new Proxy(output, {
    get(target, property, receiver) {
      if (property === '_waitUntilFinished') {
        const wait = Reflect.get(target, property, target);
        return () =>
          typeof wait === 'function'
            ? Promise.race([
                Promise.resolve(wait.call(target) as unknown),
                completion,
              ])
            : completion;
      }
      const value = Reflect.get(target, property, receiver);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

/**
 * Brand marking an agent as RUNTIME-DRIVEN: its inherited signal wake
 * (`agent.stream` under `ifIdle:'wake'`) re-enters the host-owned-run-ID guard in
 * {@link FlowsafeDurableAgent.executeWorkflow}, which drives
 * `runtime.start('durable-agentic-loop', …)` rather than the base
 * `createRun + run.start` on the default engine. The thread-Durable-Object signal
 * routes require this brand before honoring an idle wake, because the wake is the
 * one signal path that starts a run: a plain core `Agent` (or a STOCK
 * `DurableAgent`, whose `stream()` mints an unowned UUID and whose
 * `executeWorkflow` runs on the default engine) would start a run OUTSIDE
 * RunnerRuntime — an unsafe second execution path that is unscoped and
 * grant-underivable. Structural (a `unique symbol`-keyed truthy field) so a test
 * double can opt in without constructing a real durable agent.
 *
 * The brand is paired with the host-owned idle-start seam: the thread Durable Object
 * mints the run id and invokes the wrapper directly, avoiding core's unowned
 * idle-wake id generation.
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
   * PubSub for the agent's own stream events (observe()/onChunk). DEFAULTS to
   * `runtime.pubsub` (the host Durable Object's single identity) when omitted, so the
   * run's events and the agent's observe()/emitError feed agree without the host
   * wiring it twice — a mismatched pubsub would leave observe() replaying an
   * empty feed. Pass an explicit instance only to override that default.
   */
  pubsub?: DurableAgentConfig<TAgentId, TTools, TOutput>['pubsub'];
  /**
   * Public Mastra thread runtime (`mastra.agentThreadStreamRuntime`). When
   * present, started and rehydrated outputs are registered on the same pubsub
   * identity so active-thread signals join the durable loop.
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
  readonly #startScheduleDispatches = new Map<
    string,
    { scheduleId: string; dispatchId: string }
  >();

  constructor(options: FlowsafeDurableAgentOptions<TAgentId, TTools, TOutput>) {
    super({
      agent: options.agent,
      id: options.id,
      name: options.name,
      cache: options.cache,
      // Default the agent's own stream pubsub to the runtime's identity (DL-001:
      // ONE feed per DO), so a host that configures only init()'s pubsub still
      // gets observe()/emitError aligned with the run's events (the run is driven
      // through the runtime, which publishes on THAT identity). An explicit
      // pubsub wins; both-absent falls to core's per-agent default (poll-only,
      // byte-identical to before this seam existed).
      pubsub: options.pubsub ?? options.runtime.pubsub,
      maxSteps: options.maxSteps,
    });
    this.#runtime = options.runtime;
    this.#wrappedAgent = options.agent;
    this.#threadRuntime = options.threadRuntime;
  }

  /**
   * Host-owned run-ID enforcement at the public boundary. The durable-agent entry points (stream /
   * generate / prepare) take an OPTIONAL runId, and when it is omitted core's
   * `prepareForDurableExecution` mints `crypto.randomUUID()`
   * (agent/durable/index.js:589) — the exact unowned fallback this wrapper forbids —
   * and hands it to `executeWorkflow` BELOW this class's own guard, where a bare
   * UUID is already indistinguishable from a legitimately caller-minted one. So
   * the guard must ALSO fire HERE, before `super.stream()/generate()/prepare()`,
   * while "absent" is still visible. Same posture as `RunnerRuntime.start`
   * (typeof + PATH_SAFE_ID_PATTERN, NO generation fallback); the `typeof` check
   * is load-bearing because `RegExp.test` coerces its argument to a string, so a
   * numeric runId would pass the pattern yet key a run by the number. Homed once
   * and shared by the four call sites below so the rule cannot drift within this
   * class.
   */
  #assertCallerRunId(runId: unknown): asserts runId is string {
    if (!isPathSafeId(runId)) {
      throw new InvalidRunRequestError(
        'a caller-minted runId is required and must be URL-path-safe (INV-1: the host owns run ids) — the durable-agent runner never generates one',
      );
    }
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
    this.#assertCallerRunId(options?.runId);
    const result = await super.stream(messages, options);
    if (!options?.untilIdle) {
      await this.#threadRuntime?.registerRun(
        this as unknown as Parameters<
          Mastra['agentThreadStreamRuntime']['registerRun']
        >[0],
        result.output,
        (options ?? {}) as Parameters<
          Mastra['agentThreadStreamRuntime']['registerRun']
        >[2],
        this.pubsub,
      );
    }
    return result;
  }

  /**
   * Start a durable stream and wait until RunnerRuntime has persisted the
   * first suspended or terminal summary.
   *
   * The regular durable `stream()` returns after subscription setup while its
   * workflow starts asynchronously. Agent hosts need an authoritative summary
   * before answering a start request, but must keep the stream subscription
   * and replay cache intact for later HTTP observation.
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
    scheduleDispatch?: { scheduleId: string; dispatchId: string },
  ): Promise<
    Awaited<ReturnType<DurableAgent<TAgentId, TTools, TOutput>['stream']>>
  > {
    this.#assertCallerRunId(options.runId);
    if (!isExecutionPrincipalId(requestedBy)) {
      throw new InvalidRunRequestError('requestedBy is malformed');
    }
    if (!isExecutionPrincipalKind(requestedByKind)) {
      throw new InvalidRunRequestError('requestedByKind is malformed');
    }
    const runId = options.runId;
    if (this.#persistenceWaiters.has(runId)) {
      throw new InvalidRunRequestError(
        `run '${runId}' already has a pending durable start`,
      );
    }
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
    if (scheduleDispatch) {
      this.#startScheduleDispatches.set(runId, scheduleDispatch);
    }
    const onError = options.onError;
    try {
      const result = await this.stream(messages, {
        ...options,
        onError: async (data) => {
          reject(
            data.error instanceof Error
              ? data.error
              : new Error(String(data.error)),
          );
          await onError?.(data);
        },
      });
      await persisted;
      return result;
    } finally {
      this.#persistenceWaiters.delete(runId);
      this.#startRequesters.delete(runId);
      this.#startRequesterKinds.delete(runId);
      this.#startAttemptTokens.delete(runId);
      this.#startScheduleDispatches.delete(runId);
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
    this.#assertCallerRunId(options?.runId);
    return super.generate(messages, options);
  }

  /**
   * The same host-owned run-ID guard as {@link FlowsafeDurableAgent.stream}.
   * `prepare()`
   * is the third inherited minting entry point: it forwards `options?.runId` into
   * core's `prepareForDurableExecution` (agent/durable/index.js:5980), which mints
   * an unowned `crypto.randomUUID()` when it is absent (index.js:589) AND
   * REGISTERS a run under that id (index.js:5984) — so a later
   * `resume(runId)`/`executeWorkflow` sees a bare UUID `PATH_SAFE_ID_PATTERN`
   * already accepts, past every downstream guard. Enforce the caller-minted ID here, while
   * "absent" is still visible.
   */
  override async prepare(
    messages: Parameters<DurableAgent<TAgentId, TTools, TOutput>['prepare']>[0],
    options?: Parameters<DurableAgent<TAgentId, TTools, TOutput>['prepare']>[1],
  ): Promise<
    Awaited<ReturnType<DurableAgent<TAgentId, TTools, TOutput>['prepare']>>
  > {
    this.#assertCallerRunId(options?.runId);
    return super.prepare(messages, options);
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
      get(target, property) {
        if (property === 'listInputProcessors') {
          return async (requestContext?: RequestContext) => {
            inputProcessors = await target.listInputProcessors(requestContext);
            return inputProcessors.filter(
              (processor) => processor.id === 'breakwater-rbac',
            );
          };
        }
        if (property === '__listLLMRequestProcessors') {
          return async (requestContext?: RequestContext) => {
            llmRequestInputProcessors =
              await target.__listLLMRequestProcessors(requestContext);
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
    let rehydrated = false;
    let finishThreadRegistration: (() => void) | undefined;
    const emitTerminalError = async (error: unknown): Promise<void> => {
      try {
        await this.emitError(
          options.runId,
          error instanceof Error ? error : new Error(String(error)),
        );
      } catch (publicationError) {
        console.error(
          JSON.stringify({
            type: 'durable-agent-resume-error-publication-failed',
            runId: options.runId,
            error:
              publicationError instanceof Error
                ? publicationError.message
                : String(publicationError),
          }),
        );
      }
    };
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
              ...(options.memory !== undefined
                ? { memory: options.memory }
                : {}),
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
                ...(options.memory !== undefined
                  ? { memory: options.memory }
                  : {}),
              } as Parameters<
                Mastra['agentThreadStreamRuntime']['registerRun']
              >[2],
              this.pubsub,
            );
          },
        },
      );
      if (summary.status === 'failed') {
        await emitTerminalError(
          new Error(summary.error ?? 'Durable agent workflow resume failed'),
        );
      }
      return summary;
    } catch (error) {
      if (rehydrated) {
        await emitTerminalError(error);
        this.runRegistryInternal.cleanup(options.runId);
        globalRunRegistry.delete(options.runId);
      }
      throw error;
    } finally {
      finishThreadRegistration?.();
    }
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
      const startOptions = {
        runId,
        inputData: workflowInput,
        ...(attemptToken === undefined ? {} : { attemptToken }),
        ...(scheduleDispatch === undefined ? {} : { scheduleDispatch }),
      };
      if (requestedBy === undefined || requestedByKind === undefined) {
        if (requestedBy !== undefined || requestedByKind !== undefined) {
          throw new InvalidRunRequestError(
            'requestedBy and requestedByKind must be provided together',
          );
        }
        summary = await this.#runtime.start(
          this.getWorkflow().id,
          startOptions,
        );
      } else {
        summary = await this.#runtime.start(this.getWorkflow().id, {
          ...startOptions,
          requestedBy,
          requestedByKind,
        });
      }
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
    // AnyWorkflow, so the double cast through `unknown` is required (init.ts gets
    // away with a single cast because coreCreateWorkflow's return already lines
    // up). The runtime only ever reads id / getWorkflowRunById / createRun off it.
    options.runtime.register(workflow as unknown as AnyWorkflow);
  }
  return durableAgent;
}
