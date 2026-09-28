// SPDX-License-Identifier: Apache-2.0
// FlowsafeDurableAgent — drive Mastra's durable-agent loop through the ONE
// RunnerRuntime chokepoint so every agent leg inherits the substrate's
// invariants.
//
// The mechanic was validated against @mastra/core 1.50.0 dist. Every offset in
// THIS section is 1.50.0-vintage and kept as the provenance of the original
// validation; later sections carry their own stamp:
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
// there. The registry copy is read only for the fail-closed, over-require-safe
// approval pre-check.
//
// Host-owned run ids at the boundary: stream()/generate()/prepare() are
// inherited minting entry points. Each takes an OPTIONAL runId and, when it is
// absent, lets core mint an unowned crypto.randomUUID() upstream
// (prepareForDurableExecution, agent/durable index.js:589) that
// PATH_SAFE_ID_PATTERN then accepts, slipping past executeWorkflow's guard AND
// RunnerRuntime.start's exact fallback, which the host-owned run-id rule
// forbids. They are overridden ONLY to require a caller-minted runId before
// delegating to super. prepare() also REGISTERS the run under that id
// (index.js:5984), so an unguarded prepare() strands an unowned run in the
// registry. streamUntilIdle() needs no override: it drives agent.stream()
// (index.js:368), so the stream() guard already covers it.
//
// EVERY OTHER inherited entry point whose call can drive a run that would not
// land on those guarded overrides, or whose call itself discovers run or thread
// identities without a scope the caller names, is BLOCKED, overridden to throw
// before it touches storage or the registry. A call that returns the run id of
// a thread or run the caller names discovers nothing. A call that installs a
// second execution surface is refused on the fourth ground too. Installing a
// runtime service after construction is refused on the fifth ground.
// Direct cancellation outside RunnerRuntime's terminal lifecycle is refused
// on the sixth.
// BLOCKED_RUN_ENTRIES below is the single source for which method is refused on
// which ground; the grounds are enumerated there.
//
// Provenance for this section: read from the @mastra/core 1.53.0 dist. Chunk
// file names are CONTENT-HASHED and move every release, so each offset below is
// qualified by the chunk that carried it at 1.53.0 — re-read, never re-trust,
// on a peer bump.
//
//   (1) Recovery (new in 1.53.0), in chunk-XMEACVLS.js. recover() (:6385) reads
//       the persisted AGENTIC_LOOP snapshot (:6405) and re-drives it with
//       `workflow.createRun({ runId }) + run.restart()` (:6587-6588); it also
//       rebuilds the registry from the FULL application processor list (:6471)
//       rather than #rehydrateRegistry's RBAC-only, fail-closed preparation.
//       recoverActiveRuns() (:7027) is listActiveRuns() plus a recover() per
//       row. The protected deleteRunSnapshots() (:5877) joins them: its only
//       core call sites are the base executeWorkflow (:5834, overridden here),
//       resume() (:6308) and recover() (:6593), so no path this class drives
//       reaches it, and the snapshot rows belong to deployment-scoped retention
//       purge.
//
//   (2) Unscoped run or thread identity DISCOVERY returns identities rather
//       than driving anything. listActiveRuns() (chunk-XMEACVLS.js:6927)
//       enumerates `listWorkflowRuns({ workflowName: 'durable-agentic-loop',
//       status: 'running' })`, and the Agent-level listSuspendedRuns()
//       (chunk-3S5BFAEP.js:49532) enumerates
//       `listWorkflowRuns({ workflowName: 'agentic-loop', status:
//       'suspended' })`; both then narrow the rows identically, by the
//       snapshot's agentId plus the caller's OWN optional threadId/resourceId
//       (:6962-6981 and :49563-49586). Neither consults the host topology's
//       per-principal run-ownership checks (resourceAccess().owner('run', …)),
//       so either one hands a caller ids for runs it does not own. Run listing
//       is the topology's job. discoverThreadPeers()
//       (agent-Dk0N0Nlg.js:38208-38210) likewise enumerates advertised thread
//       identities on the pub/sub without a caller-named thread or principal.
//
//   (3) The resume family, in chunk-XMEACVLS.js: resume() (:6072) and its
//       funnels resumeStream() (:6650), resumeGenerate() (:6878),
//       approveToolCall() (:6669), declineToolCall() (:6676) and 1.53.0's
//       approveToolCallGenerate() (:6679) / declineToolCallGenerate() (:6683),
//       plus the new resume(..., { toolCallId }) which only forwards toolCallId
//       as run.resume's `label` (:6293). None of them MINTS — each takes the
//       runId from its caller. 1.53.0 changed what they reach: resume() no longer requires a live registry entry,
//       and on a miss it loads the persisted snapshot (:6075-6076), rehydrates
//       via prepare() (:6115) with the full application processor chain, then
//       re-drives with createRun + run.resume (:6281/:6293) below
//       executeWorkflow. That is the same second execution path recovery opens,
//       so it earns the same refusal rather than a comment asking hosts not to
//       call it.
//
// Agent-level surface (offsets in chunk-3S5BFAEP.js at 1.53.0 unless another
// chunk or file is named). The base `Agent` also carries own members
// DurableAgent does not shadow; durable-agent-surface.test.ts pins their
// partition by name.
//
//   BLOCKED outright, because each is its own execution path:
//     - listSuspendedRuns() (:49532) — the discovery ground.
//     - the network family: network() (:49263) and resumeNetwork() (:49326)
//       start and resume THE SAME networkLoop (:32496), which compiles its own
//       workflow and drives it with `mainWorkflow.createRun` (:32968) +
//       `run.stream` (:32990) / `run.resumeStream` (:32985) on the default
//       engine. network() also MINTS: `runId = mergedOptions?.runId ||
//       this.#mastra?.generateId() || randomUUID()` (:49272) — the exact
//       unowned fallback the host-owned run-id rule forbids.
//       approveNetworkToolCall() (:49382) and declineNetworkToolCall()
//       (:49400) are one-line forwards to resumeNetwork(). None of them
//       dispatches through a blocked method, so each needs its own override.
//     - the legacy family: generateLegacy() (:50414) and streamLegacy()
//       (:50417) forward into AgentLegacyHandler (:41721-42751), which converts
//       and RUNS the agent's tools, mints `runId = args.runId ||
//       mastra.generateId() || randomUUID()`, and skips the authorization gate
//       every supported entry calls (requireAgentExecutionFGA, defined :48798,
//       called from generate :49420, stream :49858, resumeStream :50002,
//       resumeGenerate :50105, and durable stream/resume/generate
//       chunk-XMEACVLS.js:5918/6158/6718 — none of them on the network or
//       legacy path). It touches no persisted workflow run state, so it is
//       convicted on minting plus the skipped gate, not on re-drive.
//     - sendToolApproval() (:50254), whose name suggests a funnel. With
//       `messages && approved` it calls
//       agentThreadStreamRuntime.continueWithMessages() (:50266), where
//       `const runId = target.runId ?? randomUUID()` (chunk-P4Y2BJL7.js:6752)
//       mints and `agent.stream(..., { runId })` (:6720) then starts a run
//       under that core-minted id — path-safe, so #assertCallerRunId cannot
//       tell it from a host-minted one. With no active thread run id it calls
//       this.listSuspendedRuns() (:50287), which is blocked. Only its tail
//       reaches this.resumeStream() (:50338) / this.sendStreamResume()
//       (:50345). The mint is the conviction.
//
//   LEFT INHERITED as funnellers, because virtual dispatch already lands them
//   on an override: resumeStreamUntilIdle() (:49954 -> agent.resumeStream) and
//   sendStreamResume() (:50212 -> this.resumeStream) reach the BLOCKED
//   resumeStream. The signal senders stay inherited because every outcome they
//   can produce lands on this runner's terminal path; queueMessage keeps the
//   same containment property.
//
//     Direct sender calls can reach core's minting sites: the idle wake
//     (:7441), continuation (:6720), completion drain (:6665), and queued-id
//     drain (:6808), all in chunk-P4Y2BJL7.js.
//     executeWorkflow therefore treats a missing #startRequesters entry as a
//     terminal refusal: it preserves input by the guarded input chain's
//     verdict when memory permits, publishes ERROR, and never calls
//     RunnerRuntime. #persistUnownedInput states the verdict rule.
//
//     Core registers thread state only when stream options carry a memory
//     thread (chunk-P4Y2BJL7.js:6557-6594). For those runs its completion
//     watcher consumes the failed output, removes the run maps, publishes
//     run-completed, and releases or transfers the lease (:6596-6624).
//     getThreadState heals a non-blocking record (:6235-6246), but neither it
//     nor getActiveThreadRunId heals a record-less active id. This is why the
//     refusal is terminal instead of synchronous: the idle-wake site is
//     wrapped (:7440-7462), while #drainPendingSignals' call at :6665 has no
//     failure cleanup. Upstream note: #drainPendingSignals needs failure
//     cleanup before a synchronous refusal would be safe there. Upstream note:
//     DurableAgent does not forward the wrapped Agent's `notifications`
//     configuration, so the wrapper cannot inject a delivery policy.
//     If the runner's publication attempts and core's own fire-and-forget
//     attempt fail, the terminal output never closes and the thread remains
//     active until eviction or a new host start.
//
//   LEFT INHERITED as non-execution, on a read of each: the base delegators
//   resume/recover/listActiveRuns/recoverActiveRuns/prepare (:50497 onward) are
//   shadowed by DurableAgent and overridden here, so they are unreachable on
//   this instance; `observe` is likewise shadowed by DurableAgent but is NOT
//   overridden and must not be — it only reattaches to a run's pubsub replay,
//   and resumeViaRuntime() calls this.observe() itself after rehydration;
//   `durable` (:44568) is a field accessor; getActiveThreadRunId() (:49507)
//   reads only the in-process pubsub registry, never storage;
//   genTitle() (:46377) and generateTitleFromUserMessage() (:46296) call
//   `llm.stream`/`llm.__text` with no tools.
//
// Residuals outside this class's reach include:
// Mastra.restartAllActiveWorkflowRuns() belongs to Mastra, not an agent —
// nothing here calls it, and it would hit core's processor-rebuild fallback if
// a host did. getLegacyHandler() (:45595) is TS-private but runtime-public and
// returns the very handler the legacy entries refuse; that is the same class
// of caveat as getWorkflow() returning a startable object, and reaching it
// takes a deliberate private cast, which is a first-party act.
//
// Corroboration: breakwater's guarded handle inventory
// (packages/breakwater/src/agent/agent.test.ts) classifies the same Agent
// members for a narrowed HANDLE. A handle can only omit, so a data-returning
// member or a setter is harmless there, while an INSTANCE Mastra calls
// in-process must throw; the inventories can file a member differently for
// that reason.
//
// The Agent-level members below are blocked as well. Read from the
// @mastra/core 1.67.0 dist, the declared peer; offsets here are 1.67.0-vintage,
// in agent-Dk0N0Nlg.js unless another file is named, and the 1.53.0 offsets
// above are left as the provenance of that read. The peer carries each of
// them, so each refusal shadows a real implementation.
//
//   - listActiveThreadRuns() (:38214) is the discovery ground one scope wider
//     than listActiveRuns. It takes no arguments and returns `{ runId,
//     resourceId, threadId }` for every thread on the pubsub instance with a run
//     in flight (storage-MbGlKLkB.js:1011-1023), and that state is keyed by
//     pubsub instance rather than by agent (`#statesByPubSub`, :150, read through
//     #getState :294), so it narrows by neither principal nor agent where the
//     listings above at least narrow by agentId. The in-process sibling
//     getActiveThreadRunId() stays non-execution because it makes the caller name
//     the (resourceId, threadId) pair: it confirms where this enumerates.
//     Cost, stated rather than left to be rediscovered: core's AgentController
//     aggregates this member across its backing agents
//     (agent-controller-CKgKFyMR.js:5722-5725), so that aggregation throws. The
//     controller installs configured memory or pub/sub when the agent lacks its
//     own (agent-controller-CKgKFyMR.js:5618-5622), reaching the refused
//     __setMemory() or __setPubSub(). When it installs neither, init() still
//     reaches the refused __setMastra() through Mastra.addAgent(). It also calls
//     the blocked sendToolApproval() at :4089 and :4118.
//   - __setThreadRuntimeAgent() (:33609) installs another agent as the target
//     the thread-runtime paths resolve through #getThreadRuntimeAgent()
//     (:33612, `this.#threadRuntimeAgent ?? this`); the refusal in
//     BLOCKED_RUN_ENTRIES names those paths, for the core it is written
//     against. That is the fourth ground by installation rather than by call:
//     the containment those inherited members rely on IS virtual dispatch on
//     `this`, so one call moves every run those paths start onto an agent
//     carrying none of these overrides — no caller-minted runId assertion, no
//     executeWorkflow, no #startRequesters backstop. The field also moves
//     subscribeToThread's replay target. It is public in the type surface
//     (agent.d.ts:229 declares it with no modifier), so unlike getLegacyHandler
//     it takes no private cast.
// The DurableAgent-level members below are blocked too. Read from the
// @mastra/core 1.67.0 dist; offsets here are 1.67.0-vintage, in
// create-durable-agent-DFHwqN2K.js unless another file is named.
//
//   - setChannels() (:6408) and __setDeclaredSchedules() (:6240) forward what
//     they are given to the wrapped agent. Channels make that agent their
//     dispatch target (agent-Dk0N0Nlg.js:33791-33796), so a plain AgentChannels
//     delivers inbound messages to its sendMessage, and channel approvals and
//     declines to its approveToolCall and declineToolCall (agent-Dk0N0Nlg.js
//     :21002, :21031, :21045). The schedule worker of a Mastra that registers
//     the wrapped agent and runs startWorkers() (mastra-CCeMcPkn.js:4525-4589)
//     resolves each declared schedule's agent by id and fires it through that
//     agent's sendSignal or generate (worker-CemmWBp9.js:167, :319, :448). Both
//     run the wrapped agent outside RunnerRuntime, so each call is the fourth
//     ground by installation, as __setThreadRuntimeAgent() is.
//   - __setMemory() and __setPubSub() (:6414-6421) install and forward services
//     to the wrapped agent. AgentController reaches them under the conditions
//     above (agent-controller-CKgKFyMR.js:5618-5622); a parent with pub/sub
//     also reaches __setPubSub() when the wrapped agent has no pub/sub of its
//     own (agent-Dk0N0Nlg.js:34105).
//   - abortRunStream() and abortThreadStream() (:6565-6601) cancel outside the
//     terminate route's ownership and settlement checks.
//   - __setMastra() (:7922) forwards to __registerMastra() (:7934), which sets
//     this wrapper's Mastra and the wrapped agent's (:7934-7939). Every later
//     leg is prepared against that Mastra: core hands it to
//     prepareForDurableExecution (:6693, :7362, :7882), and so does
//     #rehydrateRegistry. Mastra.addAgent calls __setMastra()
//     (mastra-CCeMcPkn.js:1674) and then adds getDurableWorkflows() to its own
//     registry (:1689-1692); once the runtime has built its own Mastra, that
//     repoints the runtime's loop workflow, and with it run state, to the
//     other Mastra's storage. Agent.listAgents calls
//     __registerMastra() on each static sub-agent (agent-Dk0N0Nlg.js:34104).
//     A parent with pub/sub also calls __setPubSub() on a static sub-agent
//     without its own pub/sub (:34105), even when it has no Mastra. These are
//     the fifth ground. The refusal throws from addAgent before its registry
//     write and before it repoints the loop.
//
// Each carries the `override` keyword: the declared peer declares the member,
// so the keyword holds the refusal to a base that exists and fails the
// typecheck if a future core drops it. Each signature satisfies the base it
// shadows on that base's own terms.
//
// The runner's resume path is resumeViaRuntime(). Blocking does not un-brand
// the agent — DurableAgentLike duck-types on `recover`/`recoverActiveRuns`
// merely BEING functions (agent-Dk0N0Nlg.js:272), which the overrides still
// are.
//
// Live-isolate scope: the loop resolves the tool's execute closure from the
// in-process globalRunRegistry (populated by stream()). A DO holds one run in
// one isolate, so a resume decided before eviction finds it. A resume
// AFTER eviction must first rehydrate that registry without replaying
// application input processors. resumeViaRuntime() rebuilds the registry with
// complete runtime processor lists after invoking only reserved RBAC during
// empty-message preparation, then drives runtime.resume().

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

function snapshotDurableCallOptions<T extends object>(options: T): T;
function snapshotDurableCallOptions(options: undefined): undefined;
function snapshotDurableCallOptions<T extends object>(
  options: T | undefined,
): T | undefined;
function snapshotDurableCallOptions<T extends object>(
  options: T | undefined,
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
    const descriptor = Object.getOwnPropertyDescriptor(options, key);
    if (!descriptor) continue;
    if (descriptor.get || descriptor.set) {
      throw new TypeError(
        `FlowsafeDurableAgent: call option '${String(key)}' must be a data property`,
      );
    }
    Object.defineProperty(snapshot, key, {
      configurable: false,
      enumerable: descriptor.enumerable,
      value: descriptor.value,
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
  'the messages-plus-approved branch does not resume at all: it hands the thread runtime a continuation whose run id falls back to randomUUID() when the caller names none, then starts a run under that id — path-safe, so the host-owned run-id guard cannot tell it from a caller-minted one — and its no-active-run branch reaches the equally unscoped suspended-run discovery';

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
    "it installs the agent the thread-runtime paths resolve their target through — on @mastra/core 1.67.0 these are subscribeToThread, claimThreadOwnership, sendMessage, queueMessage, sendStateSignal, sendNotificationSignal and sendSignal, which read the field it writes — so one call moves every run those paths start, and subscribeToThread's replay target with them, onto an agent that carries none of this class's overrides: no caller-minted run id, no executeWorkflow, and no terminal refusal for a run the host start seam never registered",
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
  readonly #isBreakwaterGuardedAgent: boolean;
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
    this.#isBreakwaterGuardedAgent = guardedProtocol !== undefined;
    this.#threadRuntime = options.threadRuntime;
    // Core keys thread state and signal delivery on the agent-level pub/sub;
    // its stream pub/sub option leaves that identity unset for the run's drain.
    super.__setPubSub(pubsub ?? this.pubsub);
  }

  /**
   * Host-owned run-ID enforcement at the public boundary. The durable-agent entry points (stream /
   * generate / prepare) take an OPTIONAL runId, and when it is omitted core's
   * `prepareForDurableExecution` mints `crypto.randomUUID()`
   * (create-durable-agent-DFHwqN2K.js:1211) — the exact unowned fallback this
   * wrapper forbids — and hands it to `executeWorkflow` BELOW this class's own
   * guard, where a bare UUID is already indistinguishable from a legitimately
   * caller-minted one. So the guard must ALSO fire HERE, before
   * `super.stream()/generate()/prepare()`, while "absent" is still visible. The
   * `typeof` check is load-bearing because `RegExp.test` coerces its argument to a string, so a
   * numeric runId would pass the pattern yet key a run by the number. Homed once
   * and shared by the call sites below so the rule cannot drift within this
   * class.
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

  #assertGuardedStructuredOutput(options: unknown): void {
    if (
      this.#isBreakwaterGuardedAgent &&
      options !== null &&
      typeof options === 'object' &&
      Object.hasOwn(options, 'structuredOutput')
    ) {
      throw new TypeError(
        'FlowsafeDurableAgent: structuredOutput is not supported for a Breakwater guarded agent because Mastra durable execution bypasses the narrow guarded handle',
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
    const hostStreamTicket =
      options !== undefined && this.#hostStreamTickets.has(options);
    if (hostStreamTicket) this.#hostStreamTickets.delete(options);
    const callOptions = snapshotDurableCallOptions(options);
    this.#assertCallerRunId(callOptions?.runId);
    if (!hostStreamTicket) this.#assertRunIdNotLive(callOptions.runId);
    this.#assertGuardedStructuredOutput(callOptions);
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
    const callOptions = snapshotDurableCallOptions(options);
    this.#assertCallerRunId(callOptions?.runId);
    this.#assertGuardedStructuredOutput(callOptions);
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
    const callOptions = snapshotDurableCallOptions(options);
    this.#assertCallerRunId(callOptions?.runId);
    this.#assertRunIdNotLive(callOptions.runId);
    this.#assertGuardedStructuredOutput(callOptions);
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
   * The same host-owned run-ID guard as {@link FlowsafeDurableAgent.stream}.
   * `prepare()`
   * is an inherited minting entry point: it forwards `options?.runId` into
   * core's `prepareForDurableExecution` (create-durable-agent-DFHwqN2K.js:7882),
   * which mints an unowned `crypto.randomUUID()` when it is absent
   * (create-durable-agent-DFHwqN2K.js:1211) AND REGISTERS a run under that id
   * (create-durable-agent-DFHwqN2K.js:7890) — so a later
   * `resume(runId)`/`executeWorkflow` sees a bare UUID `PATH_SAFE_ID_PATTERN`
   * already accepts, past every downstream guard. Enforce the caller-minted ID
   * here, while "absent" is still visible. A prepared id remains live until core
   * cleans up the run.
   */
  override async prepare(
    messages: Parameters<DurableAgent<TAgentId, TTools, TOutput>['prepare']>[0],
    options?: Parameters<DurableAgent<TAgentId, TTools, TOutput>['prepare']>[1],
  ): Promise<
    Awaited<ReturnType<DurableAgent<TAgentId, TTools, TOutput>['prepare']>>
  > {
    const callOptions = snapshotDurableCallOptions(options);
    this.#assertCallerRunId(callOptions?.runId);
    this.#assertRunIdNotLive(callOptions.runId);
    this.#assertGuardedStructuredOutput(callOptions);
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
   * Note what this override buys beyond that call-site audit: the override is
   * the standing guard —
   * it converts core's best-effort cleanup into a throw the moment a future
   * release calls it on a path FlowSafe drives. None does at 1.67.0.
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
      get(target, property) {
        if (property === 'listInputProcessors') {
          return async (requestContext?: RequestContext) => {
            inputProcessors = await target.listInputProcessors(requestContext);
            return inputProcessors.filter(
              (processor) => processor.id === BREAKWATER_RBAC_PROCESSOR_ID,
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
