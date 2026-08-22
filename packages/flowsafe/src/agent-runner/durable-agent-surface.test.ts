// SPDX-License-Identifier: Apache-2.0
// Surface tripwire for FlowsafeDurableAgent's ONE execution seam.
//
// The wrapper's contract is that the durable-agentic-loop runs only through
// RunnerRuntime, via the overridden executeWorkflow, entered only by a
// caller-minted runId (INV-1). That contract is stated over the surface core's
// DurableAgent exposes — so it silently decays whenever a peer bump ADDS a
// method. 1.53.0 is the live example: it added recover() / recoverActiveRuns()
// / listActiveRuns() / deleteRunSnapshots(), which read persisted snapshot
// storage and re-drive a run with `createRun + run.restart()` BELOW
// executeWorkflow, and no existing test could see them because no test calls a
// method that did not exist.
//
// So the tripwire is the inventory itself: every own property of
// DurableAgent.prototype must appear in exactly one classified list here, and
// the lists must stay honest in both directions (nothing unclassified, nothing
// stale). A future bump that adds an entry point fails this test until someone
// reads the new implementation and classifies it.
//
// Not the same inventory as breakwater's 'Mastra Agent execution-entry
// inventory' (packages/breakwater/src/agent/agent.test.ts), despite the shape.
// That one classifies Agent.prototype for what a narrowed guarded HANDLE may
// expose — a handle cannot throw, it can only omit. This one classifies
// DurableAgent.prototype for what a runner-driven INSTANCE must refuse, because
// Mastra calls the instance in-process and reaches whatever it inherits. Hence
// listActiveRuns is merely `explicitlyNonExecution` there and blocked here.

import { Agent } from '@mastra/core/agent';
import {
  DurableAgent,
  type ExtendedRunRegistry,
  globalRunRegistry,
} from '@mastra/core/agent/durable';
import type { MastraModelConfig } from '@mastra/core/llm';
import { Mastra } from '@mastra/core/mastra';
import { MockMemory } from '@mastra/core/memory';
import { InMemoryStore } from '@mastra/core/storage';
import { afterEach, describe, expect, it, vi } from 'vitest';

import type { RunnerRuntime } from '../do-runner/index.js';
import {
  BLOCKED_RUN_ENTRIES,
  createFlowsafeDurableAgent,
  FlowsafeDurableAgent,
} from './durable-agent-runner.js';
import * as barrel from './index.js';

/**
 * Own overrides that keep the loop on RunnerRuntime: each REQUIRES a
 * caller-minted, path-safe runId. Host-seam starts drive
 * `runtime.start('durable-agentic-loop', ...)`; a direct generate without that
 * registration rejects after the runner terminally closes its output.
 */
const guardedByRunner = [
  'executeWorkflow',
  'generate',
  'prepare',
  'stream',
] as const;

/**
 * Reaches a guarded entry through `this.` rather than its own override, so
 * virtual dispatch applies the terminal guard: `streamUntilIdle()` drives
 * `agent.stream()`. Not an own property of FlowsafeDurableAgent.prototype.
 */
const guardedByDelegation = ['streamUntilIdle'] as const;

/**
 * Every entry point FlowsafeDurableAgent refuses, taken from the runner's own
 * reason table rather than restated here — so a new blocked entry cannot be
 * added on one side only. The four grounds (re-drive below executeWorkflow,
 * unscoped run discovery, retention-owned snapshot deletion, and a second
 * execution surface outside RunnerRuntime, or a mint below the caller) are
 * documented on BLOCKED_RUN_ENTRIES.
 */
const blockedEntries = Object.keys(BLOCKED_RUN_ENTRIES) as ReadonlyArray<
  keyof typeof BLOCKED_RUN_ENTRIES
>;

/**
 * Blocked entries that live on `Agent.prototype`, NOT on DurableAgent's own —
 * so the durable-surface partition below cannot contain them. Pinned by name
 * rather than filtered out by a `surface.includes()` test: filtering would let
 * core DROPPING a durable blocked member pass silently, whereas an exact-match
 * assertion turns that into a failure that demands re-reading the member.
 *
 * Three families beyond the discovery member: the NETWORK four, which drive
 * the multi-agent loop's own workflow with `createRun + run.stream/resumeStream`
 * outside RunnerRuntime (and `network()` mints its own run id); the LEGACY
 * pair, which run the agent's tools through AgentLegacyHandler while minting a
 * run id and skipping `requireAgentExecutionFGA`; and `sendToolApproval`,
 * whose continuation branch starts a run under a `randomUUID()` fallback.
 */
const blockedOnAgentPrototype = [
  'approveNetworkToolCall',
  'declineNetworkToolCall',
  'generateLegacy',
  'listSuspendedRuns',
  'network',
  'resumeNetwork',
  'sendToolApproval',
  'streamLegacy',
] as const;

/**
 * The blocked entries that ARE own members of DurableAgent.prototype — this
 * inventory's share of the partition. Own overrides that THROW; three families:
 *
 *  - recovery (new in 1.53.0): recover/recoverActiveRuns re-drive via
 *    `createRun + run.restart()`, and deleteRunSnapshots is called only from
 *    the base executeWorkflow (overridden here), the blocked resume() and the
 *    blocked recover() — recoverActiveRuns reaches it through recover() alone.
 *  - unscoped discovery: listActiveRuns enumerates running runs narrowed by
 *    agentId plus the caller's own optional thread/resource ids, returning
 *    run/thread/resource ids without consulting the host topology's
 *    per-principal ownership checks.
 *  - the resume family: resumeStream/resumeGenerate and the approve/decline
 *    pairs all funnel into resume(), which since 1.53.0 rehydrates from
 *    snapshot storage on a run-registry miss and re-drives via
 *    `createRun + run.resume()`. resumeViaRuntime() is the only resume path.
 */
const blockedByRunner = blockedEntries.filter(
  (method) => !(blockedOnAgentPrototype as readonly string[]).includes(method),
);

/**
 * Constructors, accessors, registration hooks and delegators. None of them can
 * start, resume or re-drive a run, and none reads snapshot storage.
 *
 * Honest caveat: `getWorkflow`, `getDurableWorkflows` and `listWorkflows`
 * return objects that themselves expose `createRun().start()`, and they are
 * load-bearing rather than removable — `executeWorkflow` and `resumeViaRuntime`
 * both read `this.getWorkflow()`. So this partition classifies ENTRY POINTS ON
 * THE AGENT, not the capabilities of the objects they hand back.
 */
const nonExecution = [
  '__fork',
  '__getEditorConfig',
  '__getGoalConfig',
  '__getOverridableFields',
  '__getStaticAgents',
  '__hasSubAgentsConfigured',
  '__registerMastra',
  '__setMastra',
  '__setMemory',
  '__setPubSub',
  '__setTools',
  '__setWorkspace',
  '__updateInstructions',
  '__updateModel',
  'agent',
  'browser',
  'cache',
  'cleanupTimeoutMs',
  'constructor',
  // Compiles the loop workflow object; compiling starts no run.
  'createWorkflow',
  'disableBackgroundTasks',
  // Publishes an error event onto the run's feed; executeWorkflow and
  // resumeViaRuntime both use it after a terminal summary.
  'emitError',
  'enableBackgroundTasks',
  'getBackgroundTasksConfig',
  'getChannels',
  'getConfiguredProcessorIds',
  'getConfiguredProcessorWorkflows',
  'getConfiguredToolHooks',
  'getDefaultGenerateOptionsLegacy',
  'getDefaultNetworkOptions',
  'getDefaultOptions',
  'getDefaultStreamOptionsLegacy',
  'getDescription',
  'getDurableWorkflows',
  'getInstructions',
  'getLLM',
  'getMemory',
  'getMetadata',
  'getModel',
  'getModelList',
  'getSkill',
  'getToolPayloadTransform',
  'getTracingPolicy',
  'getVoice',
  // Memoized accessor for the compiled loop; the factory registers its id.
  'getWorkflow',
  'getWorkspace',
  'hasOwnBrowser',
  'hasOwnMemory',
  'hasOwnPubSub',
  'hasOwnWorkspace',
  'listAgents',
  'listConfiguredInputProcessors',
  'listConfiguredOutputProcessors',
  'listErrorProcessors',
  'listInputProcessors',
  'listOutputProcessors',
  'listScorers',
  'listSkills',
  'listTools',
  'listWorkflows',
  'maxSteps',
  // Reattaches to an existing run's pubsub replay; it cannot drive one.
  'observe',
  'pubsub',
  'pubsubInternal',
  'requestContextSchema',
  'resolveProcessorById',
  'runRegistry',
  'runRegistryInternal',
  'setBrowser',
  'setChannels',
  'voice',
] as const;

const classified: readonly string[] = [
  ...guardedByRunner,
  ...guardedByDelegation,
  ...blockedByRunner,
  ...nonExecution,
];

/**
 * ---------------------------------------------------------------------------
 * The AGENT-level partition.
 *
 * The inventory above covers DurableAgent.prototype. But Mastra calls the
 * INSTANCE, and the instance also inherits every `Agent.prototype` member
 * DurableAgent does not shadow — 82 of them, including the network family, the
 * legacy pair and sendToolApproval, all of which drive execution. Classifying
 * only the durable half would leave that surface unpinned, so it gets the same
 * treatment: every name in exactly one list, nothing unclassified, nothing
 * stale.
 * ---------------------------------------------------------------------------
 */
const agentSurface = Object.getOwnPropertyNames(Agent.prototype).filter(
  (property) => !Object.hasOwn(DurableAgent.prototype, property),
);

/**
 * Agent-level members that reach a guarded or blocked entry through `this.`,
 * so virtual dispatch already applies the rule and a second override would only
 * be somewhere for the two to drift apart.
 *
 * `resumeStreamUntilIdle` and `sendStreamResume` land on the BLOCKED
 * `resumeStream()`. The five signal senders stay inherited because every run
 * outcome they can produce lands on the runner's terminal path, not because all
 * five still have route callers. Any run core mints through them reaches a
 * terminal output without entering RunnerRuntime; see the runner module comment
 * for the cleanup mechanism.
 */
const delegatingToGuard = [
  'queueMessage',
  'resumeStreamUntilIdle',
  'sendMessage',
  'sendNotificationSignal',
  'sendSignal',
  'sendStateSignal',
  'sendStreamResume',
] as const;

/**
 * Agent-level members that cannot start, resume or re-drive a run, and return
 * no run ids. Read individually; the ones that LOOK execution-shaped carry
 * their reason inline.
 *
 * Not here, and deliberately: the base delegators resume / recover /
 * listActiveRuns / recoverActiveRuns / observe / prepare. DurableAgent shadows
 * all six, so they are not in this surface at all — the test below asserts
 * that rather than trusting it.
 */
const agentNonExecution = [
  '__getDrainPendingSignals',
  '__listLLMRequestProcessors',
  '__registerPrimitives',
  '__resetToOriginalModel',
  // Run the processor chain, not a run. No run id, no storage.
  '__runInputProcessors',
  '__runOutputProcessors',
  '__runProcessInputStep',
  // Stop an in-flight stream; there is no path from either to starting one.
  'abortRunStream',
  'abortThreadStream',
  'assertSupportsPreparedModels',
  // Objective read/write over thread state; drives nothing.
  'clearObjective',
  'combineProcessorsIntoWorkflow',
  'convertTools',
  'deriveSubAgentBackgroundConfig',
  // Field accessor for the durable flag.
  'durable',
  'formatMessagePartsForTitle',
  'formatMessagesForTitle',
  'formatTools',
  // Title summarization: llm.stream / llm.__text with no tools, no run id and
  // no run state. Not an agent run.
  'genTitle',
  'generateTitleFromUserMessage',
  // In-process pubsub registry only — core's own docs contrast it with the
  // storage-backed listSuspendedRuns, which is blocked.
  'getActiveThreadRunId',
  // TS-private but runtime-public: returns the handler generateLegacy and
  // streamLegacy refuse. Same class of caveat as getWorkflow() returning a
  // startable object — reaching it takes a deliberate private cast.
  'getLegacyHandler',
  'getMastraInstance',
  'getMcpServerGuidance',
  'getMemoryMessages',
  'getMostRecentUserMessage',
  'getObjective',
  'getProcessorRunner',
  'getPubSub',
  'getSkillsProcessors',
  'getSubAgentToolSchemas',
  'getToolsForExecution',
  'getWorkspaceInstructionsProcessors',
  'isModelFallbacks',
  'listAgentTools',
  'listAssignedTools',
  'listBrowserTools',
  'listClientTools',
  'listInputProcessorLoadedTools',
  'listMemoryTools',
  'listResolvedInputProcessors',
  'listResolvedLLMRequestProcessors',
  'listResolvedOutputProcessors',
  'listSkillTools',
  'listToolsets',
  'listWorkflowTools',
  'listWorkspaceTools',
  'normalizeModelFallbacks',
  'prepareModels',
  'reorderModels',
  // The authorization gate itself, not an entry point through it.
  'requireAgentExecutionFGA',
  'resolveFallbackDynamic',
  'resolveInputProcessors',
  'resolveModelConfig',
  'resolveModelSelection',
  'resolveOverrideScorerReferences',
  'resolveSkills',
  'resolveTitleGenerationConfig',
  'resolveTitleInstructions',
  'resolveToolHooks',
  'setObjective',
  'stripParentToolParts',
  // Reattaches to a thread's pubsub replay; cannot drive a run.
  'subscribeToThread',
  'updateModelInModelList',
  'updateObjectiveOptions',
  'wrapToolWithHooks',
  'wrapToolsWithHooks',
] as const;

const agentClassified: readonly string[] = [
  ...blockedOnAgentPrototype,
  ...delegatingToGuard,
  ...agentNonExecution,
];

const RUN_ID = 'run-1';
const APPROVED = { approved: true };

function fakeRuntime(): RunnerRuntime {
  const registered: string[] = [];
  return {
    registerAgent: vi.fn(),
    register: vi.fn((workflow: { id: string }) => {
      registered.push(workflow.id);
    }),
    workflowIds: vi.fn(() => [...registered]),
    start: vi.fn(),
    resume: vi.fn(),
  } as unknown as RunnerRuntime;
}

/**
 * A v2 model that can never reach a provider. A model STRING here would resolve
 * a real provider, and the base-reach control below deliberately lets core's
 * network loop run past the storage read — which, with credentials in the
 * environment, sent a live request to the provider's API after the test body
 * had returned. Both entry points reject instead, so the furthest any row gets
 * is this rejection. v2 is load-bearing too: the legacy pair refuses a non-v1
 * model before touching storage, which is what makes their rows vacuous by
 * construction (see registeredAgent()).
 */
function unreachableModel(): MastraModelConfig {
  const unreachable = () =>
    Promise.reject(
      new Error('the surface tripwire must never reach a language model'),
    );
  return {
    specificationVersion: 'v2',
    provider: 'flowsafe-test',
    modelId: 'unreachable',
    supportedUrls: {},
    doGenerate: unreachable,
    doStream: unreachable,
  };
}

function testAgent(id = 'writer'): Agent {
  return new Agent({
    id,
    name: id,
    instructions: 'You are a test agent.',
    model: unreachableModel(),
    // Load-bearing for non-vacuity, not decoration. core's networkLoop throws
    // AGENT_NETWORK_MEMORY_REQUIRED before touching storage when the agent has
    // no memory, which would make the "read nothing on the way out" assertion
    // on the four network rows pass for the wrong reason. With memory the
    // unmodified base reaches the workflows store, so those assertions bite.
    memory: new MockMemory(),
  });
}

/**
 * A durable agent registered on a real Mastra with real storage, so the base
 * implementations WOULD reach `storage.getStore('workflows')` — that is what
 * makes the "storage untouched" assertions below meaningful rather than
 * vacuous.
 *
 * The read spies sit on the WORKFLOWS STORE object, not on the compiled
 * workflow: core reads runs through the store it resolves from `getStore`, so a
 * spy on `workflow.getWorkflowRunById` never fires — not even on the unmodified
 * base. Resolving the store once up front is safe: `getStore` memoizes, so the
 * spied object is the one core gets.
 *
 * How much each row's "read nothing" assertion is worth, against base
 * @mastra/core 1.53.0 — this is NOT uniform, and pretending it is would be the
 * same vacuity trap the workflow-level spy was. The non-vacuous rows are no
 * longer merely PROBED: the companion control below drives each of them through
 * a stock DurableAgent on this same spied storage and ASSERTS that the
 * workflows store was reached, so a core that stops touching storage on one of
 * them fails loudly rather than turning its row quietly vacuous.
 *
 *  - Non-vacuous, one store method each: `getWorkflowRunById` for recover and
 *    the whole resume family; `listWorkflowRuns` for recoverActiveRuns,
 *    listActiveRuns, listSuspendedRuns and sendToolApproval;
 *    `deleteWorkflowRunById` for deleteRunSnapshots (which also calls the
 *    workflow-level delete). All touch `getStore` at least once.
 *  - Network rows: non-vacuous, but SMALLER than a drained count suggests.
 *    `network()` resolves as soon as it has a stream object, before its loop
 *    settles, so at assertion time the base has touched `getStore` >= 1 rather
 *    than the larger figure a fully drained run reaches. Do not pin a number.
 *  - generateLegacy / streamLegacy: VACUOUS by construction, and therefore the
 *    two rows the control excludes. `testAgent()` uses a v2 model, and the
 *    legacy handler rejects a non-v1 model before touching storage at all, so
 *    the base reaches no store either. Their non-vacuous evidence is the
 *    refusal MESSAGE assertion — the base throws core's model-support error,
 *    the override throws FlowSafe's tabled reason, and only the latter
 *    satisfies the row.
 *
 * All spies are installed after construction AND after that resolution, so
 * neither Mastra's own setup nor the resolution itself can be mistaken for a
 * call from the refused entry point.
 */
async function registeredAgent(): Promise<SpiedAgent<FlowsafeDurableAgent>> {
  return registerWithSpies(
    createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime: fakeRuntime(),
    }),
  );
}

interface SpiedAgent<TAgent extends DurableAgent> {
  agent: TAgent;
  getStore: ReturnType<typeof vi.spyOn>;
  workflowDeleteRunById: ReturnType<typeof vi.spyOn>;
  store: {
    getWorkflowRunById: ReturnType<typeof vi.spyOn>;
    listWorkflowRuns: ReturnType<typeof vi.spyOn>;
    deleteWorkflowRunById: ReturnType<typeof vi.spyOn>;
  };
}

/**
 * The wiring {@link registeredAgent} describes, applied to whichever durable
 * agent it is handed — the guarded subclass for the refusal rows, and the stock
 * base for the vacuity control. Shared so the two cannot drift into comparing
 * differently spied storage.
 */
async function registerWithSpies<TAgent extends DurableAgent>(
  agent: TAgent,
): Promise<SpiedAgent<TAgent>> {
  const storage = new InMemoryStore();
  new Mastra({
    storage,
    agents: { writer: agent as unknown as Agent },
    // The control below lets the base network loop run as far as the model,
    // where unreachableModel() rejects and core logs the stack. That rejection
    // is the design and is irrelevant to what is asserted, so keep most of it
    // out of the suite's output rather than reading as a failure.
    logger: false,
  });
  const workflowStore = await storage.getStore('workflows');
  if (!workflowStore) {
    throw new Error('the workflows store must resolve for this test to bite');
  }
  const workflow = agent.getWorkflow() as unknown as {
    deleteWorkflowRunById: (runId: string) => Promise<unknown>;
  };
  return {
    agent,
    getStore: vi.spyOn(storage, 'getStore'),
    workflowDeleteRunById: vi.spyOn(workflow, 'deleteWorkflowRunById'),
    store: {
      getWorkflowRunById: vi.spyOn(workflowStore, 'getWorkflowRunById'),
      listWorkflowRuns: vi.spyOn(workflowStore, 'listWorkflowRuns'),
      deleteWorkflowRunById: vi.spyOn(workflowStore, 'deleteWorkflowRunById'),
    },
  };
}

function registryFor(agent: FlowsafeDurableAgent): ExtendedRunRegistry {
  return (
    agent as unknown as {
      readonly runRegistryInternal: ExtendedRunRegistry;
    }
  ).runRegistryInternal;
}

/** `deleteRunSnapshots` is protected on the base; reach it the way core does. */
function protectedEntry(
  agent: FlowsafeDurableAgent,
  method: string,
): (...args: unknown[]) => Promise<unknown> {
  return (
    agent as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>
  )[method] as (...args: unknown[]) => Promise<unknown>;
}

const blockedCalls: ReadonlyArray<{
  method: keyof typeof BLOCKED_RUN_ENTRIES;
  invoke: (agent: FlowsafeDurableAgent) => Promise<unknown>;
}> = [
  { method: 'recover', invoke: (agent) => agent.recover(RUN_ID) },
  {
    method: 'recoverActiveRuns',
    invoke: (agent) => agent.recoverActiveRuns(),
  },
  { method: 'listActiveRuns', invoke: (agent) => agent.listActiveRuns() },
  {
    method: 'listSuspendedRuns',
    invoke: (agent) => agent.listSuspendedRuns(),
  },
  {
    method: 'deleteRunSnapshots',
    invoke: (agent) =>
      protectedEntry(agent, 'deleteRunSnapshots').call(agent, RUN_ID),
  },
  {
    method: 'resume',
    invoke: (agent) => agent.resume(RUN_ID, APPROVED),
  },
  {
    method: 'resumeStream',
    invoke: (agent) => agent.resumeStream(APPROVED, { runId: RUN_ID }),
  },
  {
    method: 'resumeGenerate',
    invoke: (agent) => agent.resumeGenerate(RUN_ID, APPROVED),
  },
  {
    method: 'approveToolCall',
    invoke: (agent) => agent.approveToolCall({ runId: RUN_ID }),
  },
  {
    method: 'declineToolCall',
    invoke: (agent) => agent.declineToolCall({ runId: RUN_ID }),
  },
  {
    method: 'approveToolCallGenerate',
    invoke: (agent) => agent.approveToolCallGenerate({ runId: RUN_ID }),
  },
  {
    method: 'declineToolCallGenerate',
    invoke: (agent) => agent.declineToolCallGenerate({ runId: RUN_ID }),
  },
  { method: 'network', invoke: (agent) => agent.network('hello') },
  {
    method: 'resumeNetwork',
    invoke: (agent) => agent.resumeNetwork(APPROVED, { runId: RUN_ID }),
  },
  {
    method: 'approveNetworkToolCall',
    invoke: (agent) => agent.approveNetworkToolCall({ runId: RUN_ID }),
  },
  {
    method: 'declineNetworkToolCall',
    invoke: (agent) => agent.declineNetworkToolCall({ runId: RUN_ID }),
  },
  {
    method: 'generateLegacy',
    invoke: (agent) => agent.generateLegacy('hello'),
  },
  { method: 'streamLegacy', invoke: (agent) => agent.streamLegacy('hello') },
  {
    method: 'sendToolApproval',
    // Deliberately the NO-messages shape. The continuation shape
    // (messages + approved) returns before touching storage on the base, so it
    // would make the "read nothing" assertion vacuous; this shape reaches
    // listSuspendedRuns and therefore the workflows store.
    invoke: (agent) =>
      agent.sendToolApproval({
        threadId: 'thread-1',
        resourceId: 'resource-1',
        approved: true,
      }),
  },
];

describe('FlowsafeDurableAgent prototype surface inventory', () => {
  it('requires every DurableAgent prototype member to stay classified', () => {
    // #given the classification is a partition: no name in two lists
    const duplicates = [
      ...new Set(
        classified.filter((name, index) => classified.indexOf(name) !== index),
      ),
    ];
    expect(
      new Set(classified).size,
      `the lists must partition the surface, but [${duplicates.join(', ')}] appear in more than one of them`,
    ).toBe(classified.length);

    // #when the installed core's durable surface is enumerated
    const surface = Object.getOwnPropertyNames(DurableAgent.prototype);

    // #then nothing on it is unclassified
    const unclassified = surface.filter(
      (property) => !classified.includes(property),
    );
    expect(
      unclassified,
      `@mastra/core exposes new DurableAgent member(s) [${unclassified.join(', ')}]. Read each implementation in the installed dist, then add it to exactly one list in this file: guardedByRunner (needs an INV-1 override), guardedByDelegation (reaches a guarded entry through 'this.'), nonExecution (cannot start, resume or re-drive a run, and returns no run ids), or — if it matches ANY of the four blocked grounds: (1) re-drives a persisted run below executeWorkflow, (2) discovers runs without the host topology's per-principal ownership checks, (3) deletes snapshot rows retention owns, or (4) is a second execution surface outside RunnerRuntime, or mints a run id below the caller — add it to BLOCKED_RUN_ENTRIES in durable-agent-runner.ts with its reason, an override that throws, and a row in blockedCalls below.`,
    ).toEqual([]);

    // #then and nothing classified has since been removed from it
    const stale = classified.filter((property) => !surface.includes(property));
    expect(
      stale,
      `this file classifies DurableAgent member(s) [${stale.join(', ')}] that the installed core no longer exposes — drop them, and drop any override that exists only for them.`,
    ).toEqual([]);

    // #then and the partition covers the surface exactly, name for name
    expect(
      classified,
      'the classified lists must partition the durable surface exactly — same length means no member is counted twice or missed',
    ).toHaveLength(surface.length);

    // #then and it is entirely string-keyed: this partition enumerates own
    // STRING names, so a symbol-keyed execution member would sail past every
    // assertion above rather than land unclassified.
    expect(
      Object.getOwnPropertySymbols(DurableAgent.prototype),
      'DurableAgent.prototype now carries symbol-keyed member(s), which the name-based partition above cannot see — read each one and either classify it or block it',
    ).toEqual([]);
  });

  it('pins the third prototype level the two partitions do not reach', () => {
    // #given the instance's chain is DurableAgent -> Agent -> MastraBase ->
    // Object. The two partitions above cover the first two levels; MastraBase
    // is where an unclassified member could still hide, so pin it too — the
    // invariant is stated over the WHOLE inherited surface.
    const mastraBase = Object.getPrototypeOf(Agent.prototype);

    // #then its four members are logger and raw-config plumbing, none of which
    // can start, resume or re-drive a run. Exact match, not a subset: a new
    // member at this level must be read before it is inherited silently.
    expect(
      Object.getOwnPropertyNames(mastraBase).sort(),
      'MastraBase.prototype has changed — read each new member in the installed dist and classify it here before it reaches the instance unexamined',
    ).toEqual(['__setLogger', '__setRawConfig', 'constructor', 'toRawConfig']);

    // #then and the chain ends there, so those three levels are all of it
    expect(
      Object.getPrototypeOf(mastraBase),
      'the prototype chain gained a level above MastraBase — pin it the same way',
    ).toBe(Object.prototype);
  });

  it('pins which blocked entries live outside the DurableAgent surface', () => {
    // #given the inventory above enumerates DurableAgent.prototype only, so a
    // blocked entry inherited from Agent.prototype cannot appear in it
    const surface = Object.getOwnPropertyNames(DurableAgent.prototype);

    // #when the reason table is split against that surface (sorted: this
    // compares SET membership, and the table's key order is authoring order)
    const outside = blockedEntries
      .filter((method) => !surface.includes(method))
      .sort();

    // #then the split is exactly the pinned exception — a blocked member
    // joining or leaving DurableAgent.prototype must be noticed, not absorbed
    expect(
      outside,
      'a blocked entry moved between Agent.prototype and DurableAgent.prototype: re-read it in the installed dist, then move it between blockedOnAgentPrototype and the durable partition.',
    ).toEqual([...blockedOnAgentPrototype].sort());
  });

  it('overrides every entry in the reason table on its own prototype', () => {
    // #then BLOCKED_RUN_ENTRIES is the contract, so every key it lists must be
    // refused by an OWN override — including the Agent-level ones the durable
    // inventory above structurally cannot see.
    for (const method of blockedEntries) {
      expect(
        Object.hasOwn(FlowsafeDurableAgent.prototype, method),
        `BLOCKED_RUN_ENTRIES names ${method}(), so FlowsafeDurableAgent must override it — an inherited one is an unrefused one`,
      ).toBe(true);
    }
  });

  it('overrides every guarded entry point on its own prototype', () => {
    // #then an inherited entry point is an unguarded one, so the guard must be
    // an OWN property — not merely a name the class happens to expose. The
    // BLOCKED half is covered by the reason-table assertion above, which is
    // strictly wider (it reaches the Agent-level entries too).
    for (const method of guardedByRunner) {
      expect(
        Object.hasOwn(FlowsafeDurableAgent.prototype, method),
        `FlowsafeDurableAgent must override ${method}()`,
      ).toBe(true);
    }
  });

  it('requires every inherited Agent member to stay classified', () => {
    // #given the Agent-level classification is a partition too
    const duplicates = [
      ...new Set(
        agentClassified.filter(
          (name, index) => agentClassified.indexOf(name) !== index,
        ),
      ),
    ];
    expect(
      new Set(agentClassified).size,
      `the Agent-level lists must partition that surface, but [${duplicates.join(', ')}] appear in more than one of them`,
    ).toBe(agentClassified.length);

    // #then nothing on it is unclassified
    const unclassified = agentSurface.filter(
      (property) => !agentClassified.includes(property),
    );
    expect(
      unclassified,
      `@mastra/core exposes new inherited Agent member(s) [${unclassified.join(', ')}] that FlowsafeDurableAgent also inherits. Read each implementation in the installed dist, then add it to exactly one list: delegatingToGuard (reaches a guarded or blocked member through 'this.'), agentNonExecution (cannot start, resume or re-drive a run, and returns no run ids), or — if it matches any of the four blocked grounds: (1) re-drives a persisted run below executeWorkflow, (2) discovers runs without the host topology's per-principal ownership checks, (3) deletes snapshot rows retention owns, or (4) is a second execution surface outside RunnerRuntime, or mints a run id below the caller — add it to BLOCKED_RUN_ENTRIES with a reason, an override that throws, and a row in blockedCalls.`,
    ).toEqual([]);

    // #then and nothing classified has since been removed from it
    const stale = agentClassified.filter(
      (property) => !agentSurface.includes(property),
    );
    expect(
      stale,
      `this file classifies inherited Agent member(s) [${stale.join(', ')}] the installed core no longer exposes there — drop them, and drop any override that exists only for them.`,
    ).toEqual([]);

    // #then and the surface is the size this file was written against
    expect(
      agentSurface,
      'the count of Agent.prototype members DurableAgent does not shadow has moved; re-derive the three Agent-level lists from the installed dist',
    ).toHaveLength(82);

    // #then and it is entirely string-keyed: this partition enumerates own
    // STRING names, so a symbol-keyed execution member would sail past every
    // assertion above rather than land unclassified.
    expect(
      Object.getOwnPropertySymbols(Agent.prototype),
      'Agent.prototype now carries symbol-keyed member(s), which the name-based partition above cannot see — read each one and either classify it or block it',
    ).toEqual([]);
  });

  it('keeps the base durable delegators out of the inherited surface', () => {
    // #given core also defines resume/recover/... on Agent as standalone
    // delegators. DurableAgent shadows all six, so they never reach this
    // instance through the base — assert that rather than trusting it, because
    // if core ever stopped shadowing one, the base delegator would become a
    // live unguarded path.
    for (const method of [
      'listActiveRuns',
      'observe',
      'prepare',
      'recover',
      'recoverActiveRuns',
      'resume',
    ]) {
      expect(
        agentSurface,
        `${method}() is no longer shadowed by DurableAgent, so the base Agent delegator is now reachable on this instance — read it and classify it`,
      ).not.toContain(method);
    }
  });

  it('overrides every blocked Agent-level member and no delegator', () => {
    // #then the blocked ones must be OWN properties here (they are inherited
    // from Agent, so nothing else would refuse them)
    for (const method of blockedOnAgentPrototype) {
      expect(
        Object.hasOwn(FlowsafeDurableAgent.prototype, method),
        `${method}() is blocked, so FlowsafeDurableAgent must override it`,
      ).toBe(true);
    }

    // #then and the delegators must NOT be, or the guard has two copies
    for (const method of delegatingToGuard) {
      expect(
        Object.hasOwn(FlowsafeDurableAgent.prototype, method),
        `${method}() must stay inherited: it reaches a guarded or blocked member through 'this.', and a second override is only somewhere for the two to drift apart`,
      ).toBe(false);
    }
  });

  it('exposes no override that is not in the reason table', () => {
    // #given the reverse tie. Every other assertion checks that a NAMED member
    // is overridden; this one checks the converse — that no override exists
    // which no table entry explains. An untabled throwing override would refuse
    // a caller with a reason nothing documents and no behavioral row exercises.
    const expected = [
      ...guardedByRunner,
      ...blockedEntries,
      // FlowSafe's own members, which core has no say in.
      'constructor',
      'resumeViaRuntime',
      'streamUntilPersisted',
    ].sort();

    // #when
    const own = Object.getOwnPropertyNames(
      FlowsafeDurableAgent.prototype,
    ).sort();

    // #then
    expect(
      own,
      'FlowsafeDurableAgent.prototype carries an override the reason table does not explain (or is missing one it does). Every override must either guard (guardedByRunner) or refuse with a tabled reason.',
    ).toEqual(expected);
  });

  it('keeps internal protocol constants off the public subpath', () => {
    // #then BLOCKED_RUN_ENTRIES is a reason table and
    // FLOWSAFE_PERSISTENCE_FORBIDDEN is a wire-format metadata key; exporting
    // either from the barrel would put it into the package's semver surface.
    expect(
      barrel,
      'BLOCKED_RUN_ENTRIES is re-exported from ./index.js — it must stay off the @proofoftech/flowsafe/agent-runner subpath',
    ).not.toHaveProperty('BLOCKED_RUN_ENTRIES');
    expect(
      barrel,
      'FLOWSAFE_PERSISTENCE_FORBIDDEN is re-exported from ./index.js — it must stay off the @proofoftech/flowsafe/agent-runner subpath',
    ).not.toHaveProperty('FLOWSAFE_PERSISTENCE_FORBIDDEN');
  });

  it('leaves the delegating entry points inherited', () => {
    // #then a second copy of the stream guard is a place for the two to drift
    // apart; virtual dispatch already routes these through the override.
    for (const method of guardedByDelegation) {
      expect(
        Object.hasOwn(FlowsafeDurableAgent.prototype, method),
        `${method}() must stay inherited`,
      ).toBe(false);
    }
  });

  it('keeps resumeViaRuntime as the only resume path', () => {
    // #given resumeViaRuntime is FlowSafe's own, not part of core's surface
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime: fakeRuntime(),
    });
    // #then the sanctioned resume path — reached from the approval decision,
    // never from a client — exists here and nowhere on the base. That the
    // INHERITED resume entry points are refused is proven by the reason-table
    // rows above, not by this test.
    expect(typeof agent.resumeViaRuntime).toBe('function');
    expect(Object.getOwnPropertyNames(DurableAgent.prototype)).not.toContain(
      'resumeViaRuntime',
    );
  });

  it('still satisfies the DurableAgentLike duck-type after blocking recovery', () => {
    // #given core detects durable agents by probing recover/recoverActiveRuns
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime: fakeRuntime(),
    });
    // #then blocking replaces the BEHAVIOR, not the shape — Mastra must still
    // recognize the agent (it just gets refused when it asks for recovery).
    expect(typeof agent.recover).toBe('function');
    expect(typeof agent.recoverActiveRuns).toBe('function');
  });
});

describe('FlowsafeDurableAgent blocked recovery entry points', () => {
  afterEach(() => {
    globalRunRegistry.clear();
    vi.restoreAllMocks();
  });

  it('gives every entry in the reason table a behavioral row', () => {
    // #then a blocked entry with no row here would have its refusal asserted
    // nowhere — the table would be a claim rather than a tested contract
    expect(
      blockedCalls.map((call) => call.method).sort(),
      'every blocked entry needs a behavioral row so its refusal is actually exercised',
    ).toEqual([...blockedEntries].sort());
  });

  for (const { method, invoke } of blockedCalls) {
    it(`${method}() refuses before reading storage or the registry`, async () => {
      // #given a durable agent on a Mastra whose storage the base WOULD read
      const { agent, getStore, workflowDeleteRunById, store } =
        await registeredAgent();

      // #when the blocked entry point is called
      const error = await invoke(agent).catch((caught: unknown) => caught);

      // #then it refuses, naming what is unavailable and why — the reason is
      // read from the same table the override throws, so the message asserted
      // here cannot drift from the message shipped
      expect(error).toBeInstanceOf(Error);
      const { message } = error as Error;
      expect(message).toMatch(
        new RegExp(`^FlowsafeDurableAgent\\.${method}\\(\\) is unavailable: `),
      );
      expect(message).toContain(BLOCKED_RUN_ENTRIES[method]);
      expect(message).toContain(
        'runs only through RunnerRuntime (executeWorkflow)',
      );

      // #then and it read nothing on the way out
      expect(getStore).not.toHaveBeenCalled();
      expect(store.getWorkflowRunById).not.toHaveBeenCalled();
      expect(store.listWorkflowRuns).not.toHaveBeenCalled();
      expect(store.deleteWorkflowRunById).not.toHaveBeenCalled();
      expect(workflowDeleteRunById).not.toHaveBeenCalled();
      expect(globalRunRegistry.has(RUN_ID)).toBe(false);
      expect(registryFor(agent).has(RUN_ID)).toBe(false);
    });
  }

  /**
   * The control for the loop above. Every "read nothing on the way out" row is
   * only worth something if the UNMODIFIED base WOULD have read something, so
   * drive the same rows through a stock DurableAgent on the same spied storage
   * and require the workflows store to be reached.
   *
   * `toHaveBeenCalled()`, never a count: the network family resolves as soon as
   * it has a stream object, before its loop settles, so the figure at assertion
   * time is whatever that run happened to reach. The base fails these calls for
   * its own reasons (the unreachable model rejects, and there is no persisted
   * run) — swallowed, because the claim is only that storage was reached BEFORE
   * they did.
   *
   * generateLegacy/streamLegacy are excluded: they are vacuous by construction,
   * for the reason registeredAgent() records.
   */
  const vacuousByConstruction: readonly string[] = [
    'generateLegacy',
    'streamLegacy',
  ];

  for (const { method, invoke } of blockedCalls.filter(
    (call) => !vacuousByConstruction.includes(call.method),
  )) {
    it(`${method}() reaches storage on the unmodified base`, async () => {
      // #given the same spied storage, but the stock DurableAgent
      const { agent, getStore } = await registerWithSpies(
        new DurableAgent({ agent: testAgent() }),
      );

      // #when the base implementation runs
      await invoke(agent as unknown as FlowsafeDurableAgent).catch(
        () => undefined,
      );

      // #then it got as far as the workflows store
      expect(
        getStore,
        `${method}() no longer reaches storage on the unmodified base, so the refusal row's "read nothing on the way out" assertion now passes vacuously — re-read the base implementation and re-grade the row in registeredAgent()'s notes`,
      ).toHaveBeenCalled();
    });
  }

  it('refuses recoverActiveRuns() with an explicit runId too', async () => {
    // #given the single-target form, which skips listActiveRuns() discovery
    const { agent, getStore, store } = await registeredAgent();

    // #when / #then one targeted run is still a re-drive off RunnerRuntime
    await expect(agent.recoverActiveRuns({ runId: RUN_ID })).rejects.toThrow(
      /^FlowsafeDurableAgent\.recoverActiveRuns\(\) is unavailable: /,
    );
    expect(getStore).not.toHaveBeenCalled();
    expect(store.getWorkflowRunById).not.toHaveBeenCalled();
    expect(store.listWorkflowRuns).not.toHaveBeenCalled();
    expect(globalRunRegistry.has(RUN_ID)).toBe(false);
  });

  it('refuses with a plain Error carrying no run identifiers', async () => {
    // #given a runId the caller may have no claim to
    const { agent } = await registeredAgent();

    // #when
    const error = await agent
      .recover(RUN_ID)
      .catch((caught: unknown) => caught);

    // #then the refusal is a programming-error Error (not an
    // InvalidRunRequestError, which means "this run request is malformed"),
    // and it echoes no id back to the caller.
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).name).toBe('Error');
    expect((error as Error).message).not.toContain(RUN_ID);
  });
});
