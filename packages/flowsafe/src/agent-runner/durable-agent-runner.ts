// SPDX-License-Identifier: Apache-2.0
// FlowsafeDurableAgent — drive Mastra's durable-agent loop through the ONE
// RunnerRuntime chokepoint (DL-001/DL-010) so every agent leg inherits the
// substrate's invariants: INV-1 server-minted runIds, per-leg
// requestContextForRun grant derivation, the resume ledger, RunSummary, and
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
// `breakwater.approvedConnectors` grant reaches the connector write gate with
// zero extra wiring, and a forged/self resume that mints no grant fails closed
// there (the registry copy is read only for the fail-closed, over-require-safe
// approval PRE-check, S5).
//
// INV-1 at the boundary: stream()/generate()/prepare() are the THREE inherited
// minting entry points — each takes an OPTIONAL runId and, when it is absent,
// lets core mint a tenant-less crypto.randomUUID() upstream
// (prepareForDurableExecution, agent/durable index.js:589) that
// PATH_SAFE_ID_PATTERN then accepts, slipping past executeWorkflow's guard AND
// RunnerRuntime.start's (the exact fallback INV-1 forbids). All three are
// overridden ONLY to REQUIRE a caller-minted runId before delegating to super —
// and prepare() also REGISTERS the run under that id (index.js:5984), so an
// unguarded prepare() strands a tenant-less run in the registry. streamUntilIdle()
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
// AFTER eviction must first rehydrate that registry with DurableAgent.prepare()
// (the snapshot's messageListState wins) before runtime.resume — the S3 seam,
// wired by the host that owns the resume topology (see the CLAUDE.md note).

import type { Agent, ToolsInput } from '@mastra/core/agent';
import {
  DurableAgent,
  type DurableAgentConfig,
  type DurableAgenticWorkflowInput,
} from '@mastra/core/agent/durable';
import type { AnyWorkflow } from '@mastra/core/workflows';

import {
  InvalidRunRequestError,
  PATH_SAFE_ID_PATTERN,
  type RunnerRuntime,
} from '../do-runner/index.js';

/**
 * The shared workflow id every durable-agent loop compiles to (core's
 * DurableAgentDefaults.AGENTIC_LOOP). Exposed so hosts and tests can reference
 * the registered id without a magic string.
 */
export const DURABLE_AGENTIC_LOOP_WORKFLOW_ID = 'durable-agentic-loop';

/** Options for {@link createFlowsafeDurableAgent}. */
export interface FlowsafeDurableAgentOptions<
  TAgentId extends string = string,
  TTools extends ToolsInput = ToolsInput,
  TOutput = undefined,
> {
  /** The Agent to wrap with durable, runtime-driven execution. */
  agent: Agent<TAgentId, TTools, TOutput>;
  /**
   * The RunnerRuntime the loop is driven through (DL-001/DL-010). REQUIRED —
   * this is the whole point of the flowsafe wrapper: executeWorkflow() calls
   * `runtime.start('durable-agentic-loop', ...)` instead of the base
   * `createRun + start`, so INV-1, the per-leg grant context, and the resume
   * ledger apply to agent legs. The loop workflow is registered on this runtime
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
   * `runtime.pubsub` (the host DO's ONE identity, DL-001) when omitted, so the
   * run's events and the agent's observe()/emitError feed agree without the host
   * wiring it twice — a mismatched pubsub would leave observe() replaying an
   * empty feed. Pass an explicit instance only to override that default.
   */
  pubsub?: DurableAgentConfig<TAgentId, TTools, TOutput>['pubsub'];
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
  readonly #runtime: RunnerRuntime;

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
  }

  /**
   * INV-1 at the public boundary. The durable-agent entry points (stream /
   * generate / prepare) take an OPTIONAL runId, and when it is omitted core's
   * `prepareForDurableExecution` mints `crypto.randomUUID()`
   * (agent/durable/index.js:589) — the exact tenant-less fallback INV-1 forbids —
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
    if (typeof runId !== 'string' || !PATH_SAFE_ID_PATTERN.test(runId)) {
      throw new InvalidRunRequestError(
        'a caller-minted runId is required and must be URL-path-safe (INV-1: hosts mint `<tenantId>_<uuid>`) — the durable-agent runner never generates one',
      );
    }
  }

  /**
   * Enforce INV-1 before the inherited durable `stream()` runs: without this the
   * optional `options.runId` would let core mint a tenant-less
   * `crypto.randomUUID()` upstream (see {@link FlowsafeDurableAgent.#assertCallerRunId}).
   * A host mints `${tenantId}_${uuid}` and passes it as `options.runId`.
   */
  override async stream(
    messages: Parameters<DurableAgent<TAgentId, TTools, TOutput>['stream']>[0],
    options?: Parameters<DurableAgent<TAgentId, TTools, TOutput>['stream']>[1],
  ): Promise<
    Awaited<ReturnType<DurableAgent<TAgentId, TTools, TOutput>['stream']>>
  > {
    this.#assertCallerRunId(options?.runId);
    return super.stream(messages, options);
  }

  /**
   * Same INV-1 boundary guard as {@link FlowsafeDurableAgent.stream} — `generate()`
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
   * Same INV-1 boundary guard as {@link FlowsafeDurableAgent.stream}. `prepare()`
   * is the third inherited minting entry point: it forwards `options?.runId` into
   * core's `prepareForDurableExecution` (agent/durable/index.js:5980), which mints
   * a tenant-less `crypto.randomUUID()` when it is absent (index.js:589) AND
   * REGISTERS a run under that id (index.js:5984) — so a later
   * `resume(runId)`/`executeWorkflow` sees a bare UUID `PATH_SAFE_ID_PATTERN`
   * already accepts, past every downstream guard. Enforce INV-1 HERE, while
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

  /**
   * Drive the durable-agentic-loop through RunnerRuntime instead of the base
   * `createRun + run.start` (DL-010). stream()/generate() have already parked the
   * non-serializables (model/tools/messageList) on the in-process run registry
   * keyed by this runId, so the loop the runtime starts resolves them in-isolate
   * while the runtime mints the per-leg grant context. The runId guard here is
   * defense in depth — the public boundary (stream/generate) already enforced
   * INV-1; this catches any future internal caller.
   */
  protected override async executeWorkflow(
    runId: string,
    workflowInput: DurableAgenticWorkflowInput,
  ): Promise<void> {
    this.#assertCallerRunId(runId);
    // getWorkflow() is memoized and its id is the shared loop id the factory
    // registered; driving that exact id keeps the started run and the
    // registered workflow in lockstep.
    const summary = await this.#runtime.start(this.getWorkflow().id, {
      runId,
      inputData: workflowInput,
    });
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
 * on the runtime — the same `runtime.register` path init()'s boundCreateWorkflow
 * uses.
 *
 * Registration is IDEMPOTENT by id: every durable agent compiles to the ONE
 * shared 'durable-agentic-loop' workflow (the `agentId` in each run's input
 * routes to the right agent via the per-run registry), so a second
 * createFlowsafeDurableAgent on a shared runtime must not throw 'duplicate
 * workflow id'. Like init()'s createWorkflow, register() also throws once runs
 * have started (the Mastra instance is frozen) — call this at host setup, before
 * any run.
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
