// SPDX-License-Identifier: Apache-2.0
// FlowsafeDurableAgent — drive Mastra's durable-agent loop through the ONE
// RunnerRuntime chokepoint (DL-001/DL-010) so every agent leg inherits the
// substrate's invariants: INV-1 server-minted runIds, per-leg
// requestContextForRun grant derivation, snapshot provenance, RunSummary, and
// retention purge — with no second execution path to audit.
//
// The mechanic (validated against @mastra/core 1.50.0 dist, spike S1 — every
// offset in THIS section is 1.50.0-vintage and deliberately kept as the
// provenance of the original validation; later sections carry their own stamp):
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
// guard already covers it.
//
// EVERY OTHER inherited entry point that can drive a run — or that hands back
// runs the caller may not own — is BLOCKED, overridden to throw before it
// touches storage or the registry. BLOCKED_RUN_ENTRIES below is the single
// source for which method is refused on which ground; the four grounds are
// enumerated there.
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
//   (2) Unscoped run DISCOVERY, which returns run/thread/resource ids rather
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
//       is the topology's job.
//
//   (3) The resume family, in chunk-XMEACVLS.js: resume() (:6072) and its
//       funnels resumeStream() (:6650), resumeGenerate() (:6878),
//       approveToolCall() (:6669), declineToolCall() (:6676) and 1.53.0's
//       approveToolCallGenerate() (:6679) / declineToolCallGenerate() (:6683),
//       plus the new resume(..., { toolCallId }) which only forwards toolCallId
//       as run.resume's `label` (:6293). None of them MINTS — each takes the
//       runId from its caller — which is why they were merely unwired before.
//       1.53.0 changed that: resume() no longer requires a live registry entry,
//       and on a miss it loads the persisted snapshot (:6075-6076), rehydrates
//       via prepare() (:6115) with the full application processor chain, then
//       re-drives with createRun + run.resume (:6281/:6293) below
//       executeWorkflow. That is the same second execution path recovery opens,
//       so it earns the same refusal rather than a comment asking hosts not to
//       call it.
//
// Agent-level surface (offsets in chunk-3S5BFAEP.js at 1.53.0 unless another
// chunk is named). The base `Agent` carries 82 own members DurableAgent does
// not shadow. Every one of them has been read and dispositioned into exactly
// one of the three buckets below, and durable-agent-surface.test.ts pins that
// partition by name the same way it pins the 87-member durable one — so the
// invariant holds over the WHOLE inherited surface, not just the durable half,
// and stays holding on a bump.
//
//   BLOCKED outright, because each is its own execution path:
//     - listSuspendedRuns() (:49532), the one direct data-returning discovery
//       member — the discovery ground.
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
//       chunk-XMEACVLS.js:5918/6158/6718 — seven sites, none of them on the
//       network or legacy path). It touches no persisted workflow run state, so
//       it is convicted on minting plus the skipped gate, not on re-drive.
//     - sendToolApproval() (:50254), which has four exits and only one of them
//       is the funnel its name suggests. With `messages && approved` it calls
//       agentThreadStreamRuntime.continueWithMessages() (:50266), where
//       `const runId = target.runId ?? randomUUID()` (chunk-P4Y2BJL7.js:6752)
//       mints and `agent.stream(..., { runId })` (:6720) then starts a run
//       under that core-minted id — path-safe, so #assertCallerRunId cannot
//       tell it from a host-minted one. With no active thread run id it calls
//       this.listSuspendedRuns() (:50287), already blocked. Only its tail
//       reaches this.resumeStream() (:50338) / this.sendStreamResume()
//       (:50345). The mint is the conviction.
//
//   LEFT INHERITED as funnellers, because virtual dispatch already lands them
//   on an override: resumeStreamUntilIdle() (:49954 -> agent.resumeStream) and
//   sendStreamResume() (:50212 -> this.resumeStream) reach the BLOCKED
//   resumeStream; and the five signal senders sendMessage() (:49609),
//   queueMessage() (:49620), sendStateSignal() (:49631),
//   sendNotificationSignal() (:49639) and sendSignal() (:49820) reach
//   agentThreadStreamRuntime, whose idle-start path calls `agent.stream(...)`
//   — the GUARDED override. Those five stay inherited because FlowSafe's
//   thread Durable Object USES them and blocking one would take a route with
//   it; sendToolApproval is blocked because no route needs it. The difference
//   is NECESSITY, not who mints — and who mints is a residual, recorded here:
//
//     All offsets below are chunk-P4Y2BJL7.js unless another chunk or file is
//     named — NOT the chunk-3S5BFAEP.js default this section header sets.
//
//     Only sendMessage/sendSignal wake through the host's own startIdleRun
//     seam (signals/thread-do-routes.ts:1235), where the host mints the id
//     (signals/thread-do-routes.ts:1228) and reserves ownership before calling
//     streamUntilPersisted
//     (agent-host/thread-host.ts:1541). Core still starts runs of its own
//     through the guarded stream(): on an IDLE thread queueMessage forces a
//     wake (:7192-7196), while sendStateSignal (:7235) and the DELIVER branch
//     of sendNotificationSignal (chunk-3S5BFAEP.js:49765-49770, which forwards
//     the target unmodified) fall to sendSignal's default ifIdle behavior of
//     'wake' (:7255); that branch's sibling SUMMARY path
//     (chunk-3S5BFAEP.js:49716-49721) sets
//     the behavior explicitly instead, persist for high priority and wake
//     otherwise. Either way the wake mints `runId = randomUUID()` (:7362),
//     overwriting any caller id, and then calls
//     `agent.stream(signal, { untilIdle: true, runId })` (:7441). So do the
//     completion drains after ANY of the five: #drainPendingSignals replays a
//     leftover active-delivery signal under a fresh randomUUID() with the
//     PREVIOUS run's stream options (:6641, :6665), and
//     #drainPendingIdleSignals replays a queued message under the id minted
//     when it was queued (:7180, :6808). #assertCallerRunId is path-safety
//     only, so it cannot tell those ids from host-minted ones — the host's own
//     wake uses crypto.randomUUID() too. Such runs carry no requestedBy (no
//     #startRequesters entry, so executeWorkflow starts without one), no
//     ownership reservation, no agent-run record (thread-host.ts:1493-1513
//     never runs) and no trusted engine-leg context (requestContextForRun,
//     thread-host.ts:1276-1286, misses on the runId). With an EMPTY request
//     context — the idle wake — breakwater's RBAC input processor aborts on
//     the missing actor, prepareForDurableExecution catches the TripWire
//     (chunk-XMEACVLS.js:535-548), and the first llm step returns
//     reason:'tripwire' (chunk-XMEACVLS.js:2026-2060): a run row, no model
//     call. With the
//     PREVIOUS run's request context — a completion drain — RBAC passes, the
//     model IS called, and tools see no actor, permissions or grants. Either
//     way the run is unobservable and unresumable through the host routes.
//     Pre-existing rather than introduced by this bump: all five core
//     functions are byte-identical at 1.50.0. Tracked as a residual alongside
//     getLegacyHandler() and Mastra.restartAllActiveWorkflowRuns(); the
//     reachable routes are thread-do-routes.ts /signal/queue, /signal/state
//     and /signal/notification. RUNTIME_DRIVEN_AGENT does not cover it: its
//     brand gates the host's own startIdleRun wake
//     (signals/thread-do-routes.ts:1194-1197), which those
//     three routes never reach.
//
//   LEFT INHERITED as non-execution, on a read of each: the base delegators
//   resume/recover/listActiveRuns/recoverActiveRuns/prepare (:50497 onward) are
//   shadowed by DurableAgent and overridden here, so they are unreachable on
//   this instance; `observe` is likewise shadowed by DurableAgent but is NOT
//   overridden and must not be — it only reattaches to a run's pubsub replay,
//   and resumeViaRuntime() calls this.observe() itself after rehydration;
//   `durable` (:44568) is a field accessor; subscribeToThread() (:49500) and
//   getActiveThreadRunId() (:49507) read only the in-process pubsub registry,
//   never storage; abortThreadStream() (:49600) and abortRunStream() (:49603)
//   can stop a stream but not drive one; and genTitle() (:46377) /
//   generateTitleFromUserMessage() (:46296) call `llm.stream`/`llm.__text` with
//   no tools, no run id and no run state.
//
// Two further residuals, both outside this class's reach rather than
// overlooked. A third, the only one INSIDE this class's reach, is recorded
// above under the five signal senders.
// Mastra.restartAllActiveWorkflowRuns() is Mastra-level, not an agent member —
// nothing here calls it, and it would hit core's processor-rebuild fallback if
// a host did. getLegacyHandler() (:45595) is TS-private but runtime-public and
// returns the very handler the legacy pair refuses; that is the same class of
// caveat as getWorkflow() returning a startable object, and reaching it takes a
// deliberate private cast, which is a first-party act.
//
// Corroboration: breakwater's guarded handle reaches the same verdict on seven
// of the eight members blocked here that live on Agent.prototype — network,
// resumeNetwork, approveNetworkToolCall, declineNetworkToolCall,
// generateLegacy, streamLegacy and sendToolApproval are all
// `intentionallyUnavailable` (packages/breakwater/src/agent/agent.test.ts:824).
// It diverges on listSuspendedRuns (:955), which it files under
// `explicitlyNonExecution` — the same divergence as listActiveRuns (:935), and
// for the same reason: a narrowed HANDLE can only omit, so a data-returning
// member is harmless there, while an INSTANCE Mastra calls in-process must
// throw.
//
// Blocking them keeps A-D2/P8 true by construction: resumeViaRuntime() is the
// ONLY way a run resumes. ApprovalService.decide -> the host's ResumeRunFn ->
// createAgentApprovalResumer, which hands every 'durable-agentic-loop' record to
// the thread topology's resume (-> resumeViaRuntime -> runtime.resume) and
// refuses one carrying no agent-thread target rather than falling through to the
// generic resumer; grants are derived on the (suspendedAt, resumeCount)
// fingerprint there. Blocking does not un-brand the agent — DurableAgentLike
// duck-types on `recover`/`recoverActiveRuns` merely BEING functions
// (chunk-3S5BFAEP.js:204), which the overrides still are. The surface tripwire in
// durable-agent-surface.test.ts requires every DurableAgent prototype member to
// stay classified, so a future peer bump surfaces whatever it adds.
//
// Live-isolate scope: the loop resolves the tool's execute closure from the
// in-process globalRunRegistry (populated by stream()). A DO holds one run in
// one isolate (P1), so a resume decided before eviction finds it. A resume
// AFTER eviction must first rehydrate that registry without replaying
// application input processors. resumeViaRuntime() rebuilds the registry with
// complete runtime processor lists after invoking only reserved RBAC during
// empty-message preparation, then drives runtime.resume().

import type {
  Agent,
  AgentExecutionOptions,
  ToolsInput,
} from '@mastra/core/agent';
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

const BREAKWATER_GUARDED_AGENT_HOST_PROTOCOL = Symbol.for(
  '@proofoftech/breakwater/guarded-agent-host/v1',
);

interface BreakwaterGuardedAgentHostProtocol {
  readonly version: 1;
  readonly supportsDurableStructuredOutput: false;
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

/** @internal Shared with the guarded agent catalog's compatibility check. */
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
 * Why every member of the resume family is refused. Homed once: the seven
 * entry points are one code path (resumeStream/resumeGenerate/the approve and
 * decline pairs all funnel into resume()), so one sentence must not drift into
 * seven.
 */
const RESUME_FAMILY_REASON =
  "on a run-registry miss the inherited path rehydrates from persisted snapshot storage and re-drives with createRun + run.resume outside RunnerRuntime, bypassing the approval-decision path's grant derivation and the fail-closed registry rehydration";

/**
 * Why the four network entry points are refused. Homed once: `network()` starts
 * and `resumeNetwork()` resumes THE SAME networkLoop, and the approve/decline
 * pair are one-line forwards to `resumeNetwork()`, so one sentence must not
 * drift into four.
 */
const NETWORK_FAMILY_REASON =
  'the multi-agent network loop compiles its own workflow and drives it with createRun plus run.stream/run.resumeStream on the default engine, outside RunnerRuntime, so no leg is run-owned, grant-derived or snapshot-provenanced — and under autoResumeSuspendedTools it additionally recovers a suspended run id from thread memory and re-drives that run';

/**
 * Why both legacy execution entry points are refused. Homed once: each is a
 * one-line forward into the SAME AgentLegacyHandler, so they are one code path
 * wearing two names.
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
 * Why each blocked entry point is refused, keyed by method name. The SINGLE
 * source: every override throws `unavailableRunEntry(name,
 * BLOCKED_RUN_ENTRIES[name])`, and durable-agent-surface.test.ts derives both
 * its blocked-member partition and its per-method message assertions from these
 * keys — so a new blocked entry cannot ship with an unexercised refusal, and a
 * reason cannot drift between the throw and the test.
 *
 * Four grounds. The module comment walks the same members by PROVENANCE
 * section (the durable surface, then the Agent surface) rather than by ground,
 * so its numbering is not this list's:
 *  1. re-drives a persisted run below `executeWorkflow`, where there is no
 *     RunnerRuntime — no run ownership, no per-leg grant, no snapshot
 *     provenance;
 *  2. unscoped run discovery that bypasses the host topology's per-principal
 *     run-ownership checks and returns ids the caller does not own;
 *  3. snapshot deletion owned by deployment-scoped retention;
 *  4. a SECOND execution surface that runs the agent outside RunnerRuntime
 *     entirely, or that mints a run id below the caller — the network loop's
 *     own workflow, the legacy handler, and the thread runtime's tool-approval
 *     continuation.
 *
 * Deliberately NOT re-exported from `./index.js` (a named-exports-only barrel),
 * so this stays off the public `@proofoftech/flowsafe/agent-runner` subpath.
 */
export const BLOCKED_RUN_ENTRIES = {
  recover:
    're-driving a persisted run bypasses run ownership (INV-1), per-leg grant minting and the fail-closed registry rehydration',
  recoverActiveRuns:
    'bulk re-driving persisted runs bypasses run ownership (INV-1) and per-leg grant minting',
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
  deleteRunSnapshots:
    'durable-agent snapshot rows are retained until deployment-scoped retention purge removes them',
  network: `${NETWORK_FAMILY_REASON}, and it mints an unowned run id when the caller omits one (INV-1)`,
  resumeNetwork: NETWORK_FAMILY_REASON,
  approveNetworkToolCall: NETWORK_FAMILY_REASON,
  declineNetworkToolCall: NETWORK_FAMILY_REASON,
  generateLegacy: LEGACY_FAMILY_REASON,
  streamLegacy: LEGACY_FAMILY_REASON,
  sendToolApproval: THREAD_TOOL_APPROVAL_REASON,
} as const;

/**
 * The refusal every blocked run entry point throws. A plain `Error`, not
 * {@link InvalidRunRequestError}: that class means "the client's run request is
 * malformed" (runtime.ts homes the convention — client input is an
 * InvalidRunRequestError, a developer-controlled mistake is a plain Error), and
 * calling one of these is neither a run request nor recoverable by fixing an
 * argument. Same voice as #rehydrateRegistry's fail-closed throw. The message
 * names WHAT is refused and WHY, and carries no run data — the discovery
 * entries have none to carry, and the others must not echo an id the caller may
 * not own into its log.
 */
function unavailableRunEntry(method: string, why: string): Error {
  return new Error(
    `FlowsafeDurableAgent.${method}() is unavailable: ${why} — the durable-agentic-loop runs only through RunnerRuntime (executeWorkflow), and resume flows only through the approval-decision path (resumeViaRuntime)`,
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
 * Brand marking an agent as RUNTIME-DRIVEN: its inherited signal wake
 * (`agent.stream` under `ifIdle:'wake'`) re-enters the host-owned-run-ID guard in
 * {@link FlowsafeDurableAgent.executeWorkflow}, which drives
 * `runtime.start('durable-agentic-loop', …)` rather than the base
 * `createRun + run.start` on the default engine. The thread Durable Object
 * requires this brand before honoring an idle wake through its OWN start seam
 * — the message, signal, schedule, and notification-dispatch routes: a plain
 * core `Agent` (or a STOCK `DurableAgent`, whose `stream()` mints an unowned
 * UUID and whose `executeWorkflow` runs on the default engine) would start a
 * run OUTSIDE RunnerRuntime — an unsafe second execution path that is unscoped
 * and grant-underivable. It gates that seam only, and is not a claim that
 * nothing else starts a run: the queue, state, and notification routes reach
 * an idle-start inside Mastra, as do Mastra's completion drains. See the
 * residual paragraph in this module's header comment for what those runs lack.
 * Structural (a `unique symbol`-keyed truthy field) so a test double can opt in
 * without constructing a real durable agent.
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
  readonly #startScheduleDispatches = new Map<
    string,
    { scheduleId: string; dispatchId: string }
  >();

  constructor(options: FlowsafeDurableAgentOptions<TAgentId, TTools, TOutput>) {
    const guardedProtocol = breakwaterGuardedAgentHostProtocol(options.agent);
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
    this.#isBreakwaterGuardedAgent = guardedProtocol !== undefined;
    this.#threadRuntime = options.threadRuntime;
  }

  /**
   * Host-owned run-ID enforcement at the public boundary. The durable-agent entry points (stream /
   * generate / prepare) take an OPTIONAL runId, and when it is omitted core's
   * `prepareForDurableExecution` mints `crypto.randomUUID()`
   * (agent/durable/index.js:589 — a 1.50.0-vintage offset, since that file is a
   * re-export shim from 1.53.0) — the exact unowned fallback this wrapper forbids —
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
    const callOptions = snapshotDurableCallOptions(options);
    this.#assertCallerRunId(callOptions?.runId);
    this.#assertGuardedStructuredOutput(callOptions);
    const result = await super.stream(messages, callOptions);
    if (!callOptions?.untilIdle) {
      await this.#threadRuntime?.registerRun(
        this as unknown as Parameters<
          Mastra['agentThreadStreamRuntime']['registerRun']
        >[0],
        result.output,
        (callOptions ?? {}) as Parameters<
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
    const callOptions = snapshotDurableCallOptions(options);
    this.#assertCallerRunId(callOptions?.runId);
    this.#assertGuardedStructuredOutput(callOptions);
    if (!isExecutionPrincipalId(requestedBy)) {
      throw new InvalidRunRequestError('requestedBy is malformed');
    }
    if (!isExecutionPrincipalKind(requestedByKind)) {
      throw new InvalidRunRequestError('requestedByKind is malformed');
    }
    const runId = callOptions.runId;
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
    const onError = callOptions.onError;
    try {
      const result = await this.stream(messages, {
        ...callOptions,
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
    const callOptions = snapshotDurableCallOptions(options);
    this.#assertCallerRunId(callOptions?.runId);
    this.#assertGuardedStructuredOutput(callOptions);
    return super.generate(messages, callOptions);
  }

  /**
   * The same host-owned run-ID guard as {@link FlowsafeDurableAgent.stream}.
   * `prepare()`
   * is the third inherited minting entry point: it forwards `options?.runId` into
   * core's `prepareForDurableExecution` (agent/durable/index.js:5980), which mints
   * an unowned `crypto.randomUUID()` when it is absent
   * (agent/durable/index.js:589) AND REGISTERS a run under that id
   * (agent/durable/index.js:5984 — all three are 1.50.0-vintage offsets, since
   * that file is a re-export shim from 1.53.0) — so a later
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
    const callOptions = snapshotDurableCallOptions(options);
    this.#assertCallerRunId(callOptions?.runId);
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
   * durable agent — so this is the one blocked entry point a host can reach
   * without a FlowSafe call site, by opting into `recovery: { durableAgents:
   * 'auto' }`. That loop isolates each agent in its own try/catch, so this
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
   * across every principal that shares the agent. It is the one direct
   * data-returning discovery member the base `Agent` surface adds; the other
   * additions funnel through `this.resumeStream()` and so fail closed on that
   * override.
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
   * persists no workflow run state, which is why it is refused on those two
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
   * `unknown` — the one supertype that fits every instantiation. Re-check on
   * every peer bump.
   */
  override async sendToolApproval(_options: unknown): Promise<never> {
    throw unavailableRunEntry(
      'sendToolApproval',
      BLOCKED_RUN_ENTRIES.sendToolApproval,
    );
  }

  /**
   * Refuse core's terminal snapshot cleanup. Its only call sites are the base
   * `executeWorkflow` (overridden here), the blocked `resume()` and the blocked
   * `recover()`, so nothing this class drives reaches it; blocking keeps the
   * snapshot rows — which deployment-scoped retention purge owns — from being
   * dropped out from under that owner by a future internal caller.
   *
   * Note what this override buys beyond that call-site audit: the member
   * inventory in durable-agent-surface.test.ts sees a NEW member, never a new
   * core call site on an EXISTING one. So the override is the standing guard —
   * it converts core's best-effort cleanup into a throw the moment a future
   * release calls it on a path FlowSafe drives. None does at 1.53.0.
   */
  protected override async deleteRunSnapshots(_runId: string): Promise<never> {
    throw unavailableRunEntry(
      'deleteRunSnapshots',
      BLOCKED_RUN_ENTRIES.deleteRunSnapshots,
    );
  }

  /**
   * Refuse core's durable resume. Until 1.53.0 this merely read the in-process
   * run registry, which is why it was left inherited-but-unwired; now a registry
   * MISS makes it load the persisted `durable-agentic-loop` snapshot, rehydrate
   * through `prepare()` with the full application processor chain, and re-drive
   * the run with `createRun + run.resume` — below `executeWorkflow`, so the leg
   * carries no minted grant and no snapshot provenance.
   * {@link FlowsafeDurableAgent.resumeViaRuntime} is the only resume path, and
   * it is reached from the approval decision, never from a client.
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
   * The 1.53.0 generate-shaped tool-approval pair. They funnel into
   * `resumeGenerate()` -> `resume()`, so they are the same entry point wearing a
   * different return type.
   *
   * Signature caveat for both halves: the base method is GENERIC in its OUTPUT
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
