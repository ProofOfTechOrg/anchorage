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
// executeWorkflow, and no behavioral test can see a method it does not call.
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
// Mastra calls the instance in-process and reaches whatever it inherits.

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
 * The entry points FlowsafeDurableAgent refuses, taken from the runner's own
 * reason table rather than restated here — so a new blocked entry cannot be
 * added on one side only. The grounds are documented on BLOCKED_RUN_ENTRIES.
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
 * Each member's reason is its BLOCKED_RUN_ENTRIES entry.
 *
 * The exact-match assertion counts a member absent from both prototypes as
 * outside DurableAgent.prototype.
 */
const blockedOnAgentPrototype = [
  '__setThreadRuntimeAgent',
  'approveNetworkToolCall',
  'declineNetworkToolCall',
  'discoverThreadPeers',
  'generateLegacy',
  'listActiveThreadRuns',
  'listSuspendedRuns',
  'network',
  'resumeNetwork',
  'sendToolApproval',
  'streamLegacy',
] as const;

/**
 * The blocked entries that ARE own members of DurableAgent.prototype — this
 * inventory's share of the partition: own overrides that THROW, each for the
 * reason its BLOCKED_RUN_ENTRIES entry gives. Core calls deleteRunSnapshots
 * only from the base executeWorkflow, resume() and recover(), which this class
 * overrides or refuses, and recoverActiveRuns reaches it through recover()
 * alone.
 */
const blockedByRunner = blockedEntries.filter(
  (method) => !(blockedOnAgentPrototype as readonly string[]).includes(method),
);

/**
 * Entry points whose call cannot start, resume or re-drive a run, reads no
 * snapshot storage, and matches none of the grounds documented on
 * BLOCKED_RUN_ENTRIES.
 *
 * The rule classifies the call, not the object it returns. An entry point
 * whose call returns a live object that reaches a capability a ground refuses
 * stays in this list, and carries an inline reason naming what that object
 * reaches.
 *
 * Offsets in the reasons below are @mastra/core 1.67.0-vintage, in
 * dist/create-durable-agent-DFHwqN2K.js, unless the reason names another
 * version or file. A member whose prototype level differs between the pinned
 * peer and a newer core gets a VERSION_SKEW row.
 */
const nonExecution = [
  // Returns a fork constructed with no runtime (:6453-6470). The fork's agent is
  // a new plain Agent (agent-Dk0N0Nlg.js:35342), not a Breakwater guarded one,
  // carrying the wrapped agent's Mastra (:35346), so it reaches what the `agent`
  // reason below names.
  '__fork',
  '__getEditorConfig',
  '__getGoalConfig',
  '__getOverridableFields',
  // Returns the wrapped agent's configured sub-agents
  // (agent-Dk0N0Nlg.js:33698-33701): plain Agents whose own stream() and
  // generate() run outside RunnerRuntime and carry none of this class's
  // refusals.
  '__getStaticAgents',
  '__hasSubAgentsConfigured',
  '__setTools',
  '__setWorkspace',
  '__updateInstructions',
  '__updateModel',
  // Returns the wrapped agent (:6223-6225). Its own stream() and generate() run
  // the agent loop in-process through createRun().start()
  // (agent-Dk0N0Nlg.js:37816), outside RunnerRuntime, and it carries none of
  // this class's refusals, so every blocked Agent-level member is callable on
  // it. Core reads this getter (isDurableAgentLike, agent-Dk0N0Nlg.js:272;
  // Mastra.addAgent, mastra-CCeMcPkn.js:1667), so it cannot throw.
  'agent',
  'browser',
  'cache',
  'cleanupTimeoutMs',
  'constructor',
  // Compiles a new, unregistered loop workflow (:6529-6531); compiling starts
  // no run. The workflow's createRun() mints a run id when none is given and
  // starts the run on request (agent-Dk0N0Nlg.js:5546-5554), outside
  // RunnerRuntime.
  'createWorkflow',
  'disableBackgroundTasks',
  // Publishes an error event onto a caller-named run's feed (:6539-6542); the
  // runner publishes its terminal errors through it.
  'emitError',
  // The fire-and-forget half of emitError (:6548), which core calls from its
  // own failure paths; it publishes onto a run's feed and starts nothing.
  'emitErrorInBackground',
  'enableBackgroundTasks',
  'getBackgroundTasksConfig',
  // Returns the wrapped agent's channels (1.67.0,
  // create-durable-agent-DFHwqN2K.js:6405-6407). A plain AgentChannels there
  // has the wrapped agent as its dispatch target (agent-Dk0N0Nlg.js:33470,
  // :33477, :33794): an inbound channel message reaches its sendMessage, and a
  // channel approval its approveToolCall or declineToolCall
  // (agent-Dk0N0Nlg.js:21002, :21031, :21045), outside RunnerRuntime.
  'getChannels',
  'getConfiguredProcessorIds',
  // Returns the wrapped agent's processor workflows, registered on its Mastra
  // (:6336-6338; agent-Dk0N0Nlg.js:34186); a workflow's `mastra`
  // (agent-Dk0N0Nlg.js:5025-5027) reaches what the getMastraInstance reason in
  // agentNonExecution names.
  'getConfiguredProcessorWorkflows',
  'getConfiguredToolHooks',
  // Reads the wrapped agent's declared schedules (:6233); it returns no run ids
  // and touches no storage.
  'getDeclaredSchedules',
  'getDefaultGenerateOptionsLegacy',
  'getDefaultNetworkOptions',
  'getDefaultOptions',
  'getDefaultStreamOptionsLegacy',
  'getDescription',
  // Returns [this.getWorkflow()] (:7912-7914); what that workflow reaches is the
  // getWorkflow reason below.
  'getDurableWorkflows',
  'getInstructions',
  // Returns the wrapped agent's LLM wrapper (:6265-6267). Its stream() runs
  // core's multi-step, tool-executing model loop (agent-Dk0N0Nlg.js:28533,
  // :28581) over the tools and run id the caller passes, outside RunnerRuntime
  // and without the agent's instructions, memory or processors.
  'getLLM',
  // Returns the wrapped agent's memory (:6315). Memory without storage of its
  // own takes its Mastra's (agent-Dk0N0Nlg.js:34419-34424), which on the
  // runtime's Mastra is the store the runtime reads snapshots from; the public
  // `storage` getter (dist/memory/memory.d.ts:77) reaches that store's
  // workflows domain, which lists and deletes workflow runs
  // (dist/storage/domains/workflows/base.d.ts).
  'getMemory',
  'getMetadata',
  'getModel',
  'getModelList',
  'getSkill',
  'getToolPayloadTransform',
  'getTracingPolicy',
  // Returns the agent's voice, which carries its tools and instructions
  // (agent-Dk0N0Nlg.js:34646-34664, :33460-33465); a composite voice hands
  // them to its realtime provider (voice-Dc6kCQAB.js:296-306), which runs
  // outside RunnerRuntime.
  'getVoice',
  // Memoized accessor for the compiled loop (:7852-7864); the factory registers
  // its id. Once the runtime's Mastra registers it (mastra-CCeMcPkn.js:3514-3522),
  // the workflow reaches createRun().start() (agent-Dk0N0Nlg.js:5546),
  // listWorkflowRuns (:5793), restartAllActiveWorkflowRuns (:5822) and
  // deleteWorkflowRunById (:5843) over that Mastra's storage. Adding it to
  // another Mastra repoints its `mastra` there, and RunnerRuntime then refuses
  // it. The runner reads this.getWorkflow(), so it cannot throw.
  'getWorkflow',
  'getWorkspace',
  'guardrailLogger',
  'hasOwnBrowser',
  'hasOwnMemory',
  'hasOwnPubSub',
  'hasOwnWorkspace',
  // Returns the wrapped agent's sub-agents with its Mastra registered on each
  // (:6360-6362; agent-Dk0N0Nlg.js:34089-34109): plain Agents whose own
  // stream() and generate() run outside RunnerRuntime.
  'listAgents',
  'listConfiguredInputProcessors',
  'listConfiguredOutputProcessors',
  'listErrorProcessors',
  // listInputProcessors and listOutputProcessors return the wrapped agent's
  // combined processor workflows, registered on its Mastra (:6339-6344;
  // agent-Dk0N0Nlg.js:34186); a workflow's `mastra` reaches what the
  // getMastraInstance reason in agentNonExecution names.
  'listInputProcessors',
  'listOutputProcessors',
  'listScorers',
  'listSkills',
  'listTools',
  // Returns the wrapped agent's workflows with its Mastra registered on each
  // (:6369-6371; agent-Dk0N0Nlg.js:34580-34591). Each reaches createRun(),
  // start() and restart outside RunnerRuntime, and lists and deletes its own
  // runs.
  'listWorkflows',
  'maxSteps',
  // Reattaches to an existing run's pubsub replay; it cannot drive one.
  'observe',
  // Thread state is keyed by getPubSub(). These accessors return the stream
  // bus, under the default cache a CachingPubSub over the agent-level pub/sub.
  // Publishing on that bus reaches the agent-level pub/sub, including abort
  // and event topics addressed by run id (:272). Core's
  // agentThreadStreamRuntime (mastra-CCeMcPkn.js:650-652) lists state keyed by
  // getPubSub(), which the blocked listActiveThreadRuns exposes
  // (storage-MbGlKLkB.js:1011-1023).
  'pubsub',
  'pubsubInternal',
  'requestContextSchema',
  // The abort primitive reached by the `abort` closure core returns with each
  // durable stream result (:6814, :7095, :7265, :7796).
  'requestRemoteAbort',
  'resolveProcessorById',
  // Returns the snapshot-persistence predicate createWorkflow compiles into the
  // loop (1.69.0, create-durable-agent-B36Fu53G.js:6734, read at :6723); it
  // writes and deletes no snapshot row.
  'resolveShouldPersistSnapshot',
  // runRegistry and runRegistryInternal return this instance's live
  // ExtendedRunRegistry (:6246, :6483; class at :138-262): the ids of its runs,
  // with no principal parameter; each run's live state by reference; and
  // cleanup, which flips the runner's isRunLive(). No core code reads either
  // getter at 1.67.0, and the same runs sit isolate-wide in the public
  // globalRunRegistry export, so refusing the getters closes nothing that
  // in-process code lacks. The runner reads runRegistryInternal.
  'runRegistry',
  'runRegistryInternal',
  'setBrowser',
  // Returns the voice getVoice returns, which reaches what that reason names.
  'voice',
  // Calls the configured shouldPersistSnapshot predicate with synthetic
  // statuses and logs warnings (1.69.0, create-durable-agent-B36Fu53G.js:8090);
  // it persists nothing and starts no run.
  'warnOnRiskyPersistencePolicy',
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
 * DurableAgent does not shadow — a surface whose size differs between the two
 * supported cores, and which holds members that drive execution. Classifying
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
 * A member without an inline reason takes its reason from the runner module
 * comment's LEFT INHERITED paragraphs, which also give the cleanup mechanism.
 *
 * For the members that resolve their target through the thread runtime's agent
 * field, the containment holds only while `__setThreadRuntimeAgent` is blocked,
 * which is what keeps that target this instance.
 */
const delegatingToGuard = [
  // Opts the agent in as a thread's remote wake target: the thread runtime
  // keeps the claim and, on an idle signal published by another process, drives
  // `owner.agent.stream(...)` (storage-MbGlKLkB.js:672) with a run id from that
  // message. `owner.agent` is `this`, so that lands on the guarded stream
  // override — but the runId assertion is not what contains it, since a
  // pubsub-supplied id is path-safe like any other; the terminal refusal
  // `executeWorkflow` raises for a runId with no `#startRequesters` entry is.
  'claimThreadOwnership',
  'queueMessage',
  'resumeStreamUntilIdle',
  'sendMessage',
  'sendNotificationSignal',
  'sendSignal',
  'sendStateSignal',
  'sendStreamResume',
  // Subscribes to the thread the caller names (:38196-38198). Its listener
  // drains the thread's queued idle signals into the queuing agent's stream()
  // under the run id core minted when the signal was queued
  // (storage-MbGlKLkB.js:1671-1675, reached from :2040 and :2117), so a signal
  // this instance queued lands on the guarded stream override, where
  // executeWorkflow refuses a run the host start seam never registered.
  'subscribeToThread',
] as const;

/**
 * Agent-level entry points whose call cannot start, resume or re-drive a run,
 * and matches none of the grounds documented on BLOCKED_RUN_ENTRIES.
 * nonExecution's rule for an entry point whose call returns a live object
 * applies here too. Read individually; the ones that LOOK execution-shaped
 * carry their reason inline.
 *
 * Not here: the base durable delegators DurableAgent shadows, which are not in
 * this surface at all — 'keeps the base durable delegators out of the
 * inherited surface' asserts that rather than trusting it.
 *
 * Offsets in the reasons below are @mastra/core 1.67.0-vintage, in
 * dist/agent-Dk0N0Nlg.js, unless the reason names another version or file. A
 * member whose prototype level differs between the pinned peer and a newer
 * core gets a VERSION_SKEW row.
 */
const agentNonExecution = [
  '__getDrainPendingSignals',
  '__getLogger',
  '__listLLMRequestProcessors',
  // Sets a flag (:35357); it starts nothing.
  '__markStoredVersionApplied',
  '__registerPrimitives',
  '__resetToOriginalModel',
  // Run the processor chain, not an agent run.
  '__runInputProcessors',
  '__runOutputProcessors',
  '__runProcessInputStep',
  'assertSupportsPreparedModels',
  // Removes pending idle signals from the in-process queue, matched on the
  // agent core passes — `this`, not the thread-runtime target (:38342) — and on
  // ids the caller already holds. It cancels work; it starts none.
  'cancelQueuedMessages',
  // Objective read/write over thread state; drives nothing.
  'clearObjective',
  // Returns a processor workflow registered on this instance's Mastra when a
  // host registers it (:34186); the workflow's `mastra` (:5025-5027) then
  // reaches what the getMastraInstance reason below names.
  'combineProcessorsIntoWorkflow',
  // Includes the tools listAgentTools and listWorkflowTools build
  // (:37055-37077), which reach what their reasons below name.
  'convertTools',
  'deriveSubAgentBackgroundConfig',
  // Field accessor for the durable flag.
  'durable',
  // Pure title-generation prefilter over a message list (:35450).
  'filterUiMessagesByThread',
  'formatMessagePartsForTitle',
  'formatMessagesForTitle',
  'formatTools',
  // Title summarization: llm.stream / llm.__text with no tools (:35421-35440).
  // Core's LLM wrapper runs that model call as an internal workflow under a run
  // id it mints and does not return (:28245-28270, :28347-28354): a model
  // call, not an agent run.
  'genTitle',
  'generateTitleFromUserMessage',
  // Returns the run id of the thread the caller names by (resourceId,
  // threadId), from the in-process thread-stream state
  // (storage-MbGlKLkB.js:1001-1009); it reads no storage. A lookup the caller
  // keys discovers nothing, so it is not the discovery ground.
  'getActiveThreadRunId',
  // TS-private but runtime-public: returns the handler generateLegacy and
  // streamLegacy refuse. Same class of caveat as getWorkflow() returning a
  // startable object — reaching it takes a deliberate private cast.
  'getLegacyHandler',
  // Returns the Mastra this instance is registered on (:33557-33559), which is
  // undefined: createFlowsafeDurableAgent registers the wrapped agent, not this
  // one, and the runner refuses __setMastra and __registerMastra, through which
  // a Mastra registers this one. A Mastra reaches the thread runtime's
  // listActiveThreadRuns (mastra-CCeMcPkn.js:650-652;
  // storage-MbGlKLkB.js:1011-1023) and, through getStorage()
  // (mastra-CCeMcPkn.js:4123-4125), the workflows domain that lists and
  // deletes workflow runs.
  'getMastraInstance',
  'getMcpServerGuidance',
  'getMemoryMessages',
  'getMostRecentUserMessage',
  'getObjective',
  // Returns a ProcessorRunner holding the processor workflows
  // combineProcessorsIntoWorkflow builds, which reach what its reason names.
  'getProcessorRunner',
  // Returns this agent's pubsub (:33560-33562). The thread runtime keys a
  // thread's state by pubsub (storage-MbGlKLkB.js:150, :294), and its public
  // listActiveThreadRuns(pubsub) (:1011-1023) lists, for the runs keyed by it,
  // what the blocked listActiveThreadRuns refuses.
  'getPubSub',
  'getSkillsProcessors',
  'getSubAgentToolSchemas',
  // Calls convertTools (:36940-36974), so it returns the tools convertTools'
  // reason names.
  'getToolsForExecution',
  'getWorkspaceInstructionsProcessors',
  'isModelFallbacks',
  // Builds agent tools over the wrapped agent's sub-agents (:36094-36739).
  // Their execute runs a sub-agent's generate() or stream() under a run id the
  // sub-agent's core mints, or resumes one under the run id the caller passes
  // (:36390-36391, :36404, :36469-36470, :36483), outside RunnerRuntime for a
  // plain sub-agent.
  'listAgentTools',
  'listAssignedTools',
  'listBrowserTools',
  'listClientTools',
  'listInputProcessorLoadedTools',
  'listMemoryTools',
  // listResolvedInputProcessors and listResolvedOutputProcessors return the
  // processor workflow combineProcessorsIntoWorkflow builds, which reaches what
  // its reason names.
  'listResolvedInputProcessors',
  'listResolvedLLMRequestProcessors',
  'listResolvedOutputProcessors',
  'listSkillTools',
  'listToolsets',
  // Builds workflow tools over the wrapped agent's workflows (:36740-36927).
  // Their execute creates a run under the run id the caller passes or a minted
  // one, then starts or resumes it (:36801-36842), outside RunnerRuntime.
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
  // Runs the configured delivery policy for one notification record and returns
  // a decision. Core reads that decision's `streamOptions` when it wakes an
  // idle thread (storage-MbGlKLkB.js:2814-2816), inside a try/catch that
  // degrades to a bare wake — so it feeds a start core makes, and cannot make
  // one.
  'resolveNotificationDeliveryDecision',
  'resolveOverrideScorerReferences',
  'resolveSkills',
  'resolveTitleGenerationConfig',
  'resolveTitleInstructions',
  'resolveToolHooks',
  'setObjective',
  'stripParentToolParts',
  // Registers a listener that receives queued-message counts for the thread the
  // caller names (:38348); it discloses a count and drives nothing.
  'subscribeThreadEvents',
  'updateModelInModelList',
  'updateObjectiveOptions',
  // Edits the label, title and metadata of the advertisement an existing claim
  // holds (1.69.0, storage-BkPsrBDT.js:645); it cannot change the
  // advertisement's id or sourceId, so it cannot re-address the claim, and no
  // run path reads the fields it edits.
  'updateThreadPeerAdvertisement',
  'wrapToolWithHooks',
  'wrapToolsWithHooks',
] as const;

const agentClassified: readonly string[] = [
  ...blockedOnAgentPrototype,
  ...delegatingToGuard,
  ...agentNonExecution,
];

/**
 * ---------------------------------------------------------------------------
 * The VERSION SKEW between the two supported cores.
 *
 * The partitions above demand exact equality with the installed surface:
 * nothing unclassified, nothing stale. But two cores are supported, and one
 * partition cannot equal two surfaces. So every name whose PRESENCE differs between them
 * is recorded here, with the prototype level it sits on at each version;
 * `null` means it is on neither prototype there. A member that MOVED level is
 * ONE row naming both levels, not two list edits in opposite directions.
 *
 * A row excuses its name from the stale check at the level the installed core
 * does not carry it on. It never excuses it from being classified: every level
 * a row names must hold that name in one of its lists, so a moved member is
 * classified on both sides, and the `unclassified` assertions stay exact in
 * both directions.
 *
 * A row leaves the table when both cores agree. The expiry assertion below says
 * when, by checking the table's claim about the INSTALLED core rather than
 * trusting it.
 * ---------------------------------------------------------------------------
 */
type SkewLevel = 'durable' | 'agent' | null;
interface SkewRow {
  pin: SkewLevel;
  newest: SkewLevel;
}

const VERSION_SKEW: Readonly<Record<string, Readonly<SkewRow>>> = {
  __getLogger: { pin: null, newest: 'agent' },
  guardrailLogger: { pin: null, newest: 'durable' },
  resolveShouldPersistSnapshot: { pin: null, newest: 'durable' },
  updateThreadPeerAdvertisement: { pin: null, newest: 'agent' },
  warnOnRiskyPersistencePolicy: { pin: null, newest: 'durable' },
};

// Rows are read as entries, never indexed by name: the name and its row travel
// together, so every read below is total whatever the table holds.
const skewRows = Object.entries(VERSION_SKEW);
const skewNames = skewRows.map(([name]) => name);

interface FsModule {
  readFileSync(path: URL, encoding: 'utf8'): string;
}
interface ModuleModule {
  createRequire(url: string): (id: string) => unknown;
}

/**
 * Node builtins load through process.getBuiltinModule (the spdx.test.ts
 * pattern): this file compiles in the workers-typed package test pass, and
 * this package's src forbids importing a `node:` specifier statically.
 */
function builtin<T>(id: string): T {
  const getBuiltin = (
    globalThis as {
      process?: { getBuiltinModule?: (id: string) => unknown };
    }
  ).process?.getBuiltinModule;
  if (!getBuiltin) {
    throw new Error(`${id} unavailable — tests require node >= 22`);
  }
  return getBuiltin(id) as T;
}

// The workers-typed ImportMeta carries no `url`; at runtime (vitest on node) it
// is always present.
const HERE = (import.meta as unknown as { url: string }).url;
const installedCore = (
  builtin<ModuleModule>('node:module').createRequire(HERE)(
    '@mastra/core/package.json',
  ) as { version: string }
).version;
const declaredPeer = (
  JSON.parse(
    builtin<FsModule>('node:fs').readFileSync(
      new URL('../../package.json', HERE),
      'utf8',
    ),
  ) as { peerDependencies: Record<string, string> }
).peerDependencies['@mastra/core'];

/** Which core is installed decides which half of every row applies. */
const onPin = installedCore === declaredPeer;
const levelHere = (row: SkewRow): SkewLevel => (onPin ? row.pin : row.newest);

/**
 * The names one level's stale check must excuse: recorded as skewed, not on
 * that level in the installed core, and classified at that level. The last
 * clause is load-bearing — it keeps the other level's rows out of this level's
 * arithmetic, so each length assertion below still counts its own partition.
 */
const exemptFor = (
  level: Exclude<SkewLevel, null>,
  lists: readonly string[],
): string[] =>
  skewRows
    .filter(([name, row]) => levelHere(row) !== level && lists.includes(name))
    .map(([name]) => name);
const exemptDurable = exemptFor('durable', classified);
const exemptAgent = exemptFor('agent', agentClassified);

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
 * a real provider, and the base-reach control below lets core's network loop
 * run past the storage read — which, with credentials in the environment,
 * would send a live request to the provider's API after the test body
 * returns. Its doGenerate and doStream reject instead, so the furthest any row
 * gets is this rejection. v2 is load-bearing too: the legacy pair refuses a
 * non-v1 model before touching storage, which is what makes their rows vacuous
 * by construction (see registeredAgent()).
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
    // on the network rows pass for the wrong reason. With memory the
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
 * How much each row's "read nothing" assertion is worth, against the pinned
 * core — this is NOT uniform, and pretending it is would be the same vacuity
 * trap as a workflow-level spy. The non-vacuous rows are not merely PROBED:
 * the companion control below drives each of them through a stock DurableAgent
 * on this same spied storage and ASSERTS that the workflows store was reached,
 * so a core that stops touching storage on one of them fails loudly rather than
 * turning its row quietly vacuous. The vacuous rows take the inverted half of
 * that control, which drives them the same way and asserts the store was NOT
 * reached — so each grading below is checked rather than claimed, in whichever
 * direction it goes, on a core that exposes the member.
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
 *  - generateLegacy / streamLegacy: VACUOUS by construction.
 *    `testAgent()` uses a v2 model, and the legacy handler rejects a non-v1
 *    model before touching storage at all, so the base reaches no store
 *    either. Their non-vacuous evidence is the refusal MESSAGE assertion —
 *    the base throws core's model-support error, the override throws
 *    FlowSafe's tabled reason, and only the latter satisfies the row.
 *  - listActiveThreadRuns / __setThreadRuntimeAgent: VACUOUS by construction
 *    as well, not by any version skew. At @mastra/core 1.67.0,
 *    `__setThreadRuntimeAgent` writes a private field
 *    (dist/agent-Dk0N0Nlg.js:33609-33611), and `listActiveThreadRuns` hands
 *    `getPubSub()` (:33560-33562) to the thread-stream runtime, which reads
 *    its state from a WeakMap keyed by that pubsub instance
 *    (dist/storage-MbGlKLkB.js:150, :294-298) and then reads the in-memory maps
 *    and sets that state holds (:1011-1023). Neither path resolves a store, so
 *    the base reaches none here either — which is what the inverted control
 *    asserts. Their non-vacuous evidence is the refusal MESSAGE assertion too
 *    — the base returns where the override throws FlowSafe's tabled reason.
 *  - setChannels / __setDeclaredSchedules / __setMastra / __registerMastra:
 *    VACUOUS by construction on the same terms. At @mastra/core 1.67.0 the
 *    first two forward what they are given to the wrapped agent, which stores
 *    it (dist/create-durable-agent-DFHwqN2K.js:6240-6242, :6408-6410;
 *    dist/agent-Dk0N0Nlg.js:33791-33796, :34740-34742), and the other two set
 *    Mastra references, register the agent's tools and processors on that
 *    Mastra, and rewire the inner pubsub (create-durable-agent-DFHwqN2K.js
 *    :7922-7939; agent-Dk0N0Nlg.js:35294-35326). None of them resolves a store.
 *  - __setMemory / __setPubSub / abortRunStream / abortThreadStream /
 *    discoverThreadPeers: VACUOUS by construction. The setters store the
 *    service and forward it to the wrapped agent
 *    (create-durable-agent-DFHwqN2K.js:6414-6421); the abort pair flips a
 *    local controller and publishes an abort request (:6565-6601);
 *    discoverThreadPeers reads in-memory thread-runtime state
 *    (agent-Dk0N0Nlg.js:38208-38210). None reaches storage.
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
 *
 * The guarded subclass refuses `__setMastra` and `__registerMastra`, which
 * `Mastra.addAgent` calls. For the one registration below, both are shadowed
 * on the instance with DurableAgent.prototype's members, and the shadows are
 * deleted as soon as it returns. The subclass then takes the same `addAgent`
 * path as the stock base, its rows run against a wrapper whose base would
 * reach storage, and every row still calls the subclass's own overrides.
 */
async function registerWithSpies<TAgent extends DurableAgent>(
  agent: TAgent,
): Promise<SpiedAgent<TAgent>> {
  const storage = new InMemoryStore();
  const shadowed = agent instanceof FlowsafeDurableAgent;
  if (shadowed) {
    for (const member of ['__setMastra', '__registerMastra'] as const) {
      Object.defineProperty(agent, member, {
        value: DurableAgent.prototype[member],
        configurable: true,
        writable: true,
      });
    }
  }
  try {
    new Mastra({
      storage,
      agents: { writer: agent as unknown as Agent },
      // The control below lets the base network loop run as far as the model,
      // where unreachableModel() rejects and core logs the stack. That
      // rejection is the design and is irrelevant to what is asserted, so keep
      // most of it out of the suite's output rather than reading as a failure.
      logger: false,
    });
  } finally {
    if (shadowed) {
      delete (agent as unknown as Record<string, unknown>).__setMastra;
      delete (agent as unknown as Record<string, unknown>).__registerMastra;
    }
  }
  if (agent.getMastraInstance() === undefined) {
    throw new Error('the agent must be registered for this test to bite');
  }
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
    method: 'listActiveThreadRuns',
    // An ASYNC arrow although the member is synchronous, and that is not
    // decoration: the loop below evaluates invoke(agent) before attaching its
    // .catch, so a synchronous throw would escape the row that exists to
    // capture it and fail the test with the refusal it is asserting.
    invoke: async (agent) => agent.listActiveThreadRuns(),
  },
  {
    method: 'discoverThreadPeers',
    invoke: (agent) => agent.discoverThreadPeers(),
  },
  {
    method: '__setMemory',
    invoke: async (agent) => agent.__setMemory(new MockMemory()),
  },
  {
    method: '__setPubSub',
    invoke: async (agent) => agent.__setPubSub(agent.pubsub),
  },
  {
    method: 'abortRunStream',
    invoke: async (agent) => agent.abortRunStream(RUN_ID),
  },
  {
    method: 'abortThreadStream',
    invoke: async (agent) =>
      agent.abortThreadStream({
        threadId: 'thread-1',
        resourceId: 'resource-1',
      }),
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
    // The NO-messages shape. The continuation shape
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
  {
    method: '__setThreadRuntimeAgent',
    // Async for the same reason as listActiveThreadRuns. The argument is a
    // plain Agent because that is exactly what installing one would hand it.
    invoke: async (agent) => agent.__setThreadRuntimeAgent(testAgent()),
  },
  {
    method: 'setChannels',
    // Async for the same reason as listActiveThreadRuns. The argument carries
    // the two members core calls on the channels it binds.
    invoke: async (agent) =>
      agent.setChannels({ __setAgent() {}, __setLogger() {} }),
  },
  {
    method: '__setDeclaredSchedules',
    invoke: async (agent) => agent.__setDeclaredSchedules([]),
  },
  {
    method: '__setMastra',
    invoke: async (agent) =>
      agent.__setMastra(
        new Mastra({ storage: new InMemoryStore(), logger: false }),
      ),
  },
  {
    method: '__registerMastra',
    invoke: async (agent) =>
      agent.__registerMastra(
        new Mastra({ storage: new InMemoryStore(), logger: false }),
      ),
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
      `@mastra/core exposes new DurableAgent member(s) [${unclassified.join(', ')}]. Read each implementation in the installed dist and classify what calling it does. If that call falls under a ground documented on BLOCKED_RUN_ENTRIES in durable-agent-runner.ts, add the member there with its reason, an override that throws, and a row in blockedCalls below. Otherwise it meets the rule of one of the other lists, stated in that list's comment: add it to that list. If the OTHER supported core does not carry it here, give it a VERSION_SKEW row too, naming the prototype level it sits on at each version.`,
    ).toEqual([]);

    // #then and nothing classified has since been removed from it, beyond the
    // members a VERSION_SKEW row says this core does not carry here
    const stale = classified.filter(
      (property) =>
        !surface.includes(property) && !exemptDurable.includes(property),
    );
    expect(
      stale,
      `this file classifies DurableAgent member(s) [${stale.join(', ')}] that @mastra/core ${installedCore} does not expose there — drop them, and drop any override that exists only for them, or, if the other supported core carries them, give each a VERSION_SKEW row naming the level it sits on at each version.`,
    ).toEqual([]);

    // #then and the partition covers the surface exactly, name for name, plus
    // exactly the skewed members this core does not carry here
    expect(
      classified.length,
      `the classified lists must partition the durable surface exactly — every member once, plus exactly the ${exemptDurable.length} skewed member(s) @mastra/core ${installedCore} does not expose on DurableAgent.prototype`,
    ).toBe(surface.length + exemptDurable.length);

    // #then and it is entirely string-keyed: this partition enumerates own
    // STRING names, so a symbol-keyed execution member would sail past every
    // assertion above rather than land unclassified.
    expect(
      Object.getOwnPropertySymbols(DurableAgent.prototype),
      'DurableAgent.prototype now carries symbol-keyed member(s), which the name-based partition above cannot see — read each one and either classify it or block it',
    ).toEqual([]);
  });

  it('pins the MastraBase prototype level the partitions do not reach', () => {
    // #given the instance's chain is DurableAgent -> Agent -> MastraBase ->
    // Object. The partitions above cover DurableAgent and Agent; MastraBase
    // is where an unclassified member could still hide, so pin it too — the
    // invariant is stated over the WHOLE inherited surface.
    const mastraBase = Object.getPrototypeOf(Agent.prototype);

    // #then its members are logger and raw-config plumbing, none of which can
    // start, resume or re-drive a run. Exact match, not a subset: a new member
    // at this level must be read before it is inherited silently.
    expect(
      Object.getOwnPropertyNames(mastraBase).sort(),
      'MastraBase.prototype has changed — read each new member in the installed dist and classify it here before it reaches the instance unexamined',
    ).toEqual(['__setLogger', '__setRawConfig', 'constructor', 'toRawConfig']);

    // #then and the chain ends there
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
    // BLOCKED half is the reason-table assertion above.
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
      `@mastra/core exposes new inherited Agent member(s) [${unclassified.join(', ')}] that FlowsafeDurableAgent also inherits. Read each implementation in the installed dist and classify what calling it does. If that call falls under a ground documented on BLOCKED_RUN_ENTRIES in durable-agent-runner.ts, add the member there with its reason, an override that throws, and a row in blockedCalls, and add it to blockedOnAgentPrototype. Otherwise it meets the rule of one of the other lists, stated in that list's comment: add it to that list. If the OTHER supported core does not carry it here — because it is newer than the pin, or because DurableAgent shadows it there — give it a VERSION_SKEW row naming the level it sits on at each version, and classify it on every level that row names.`,
    ).toEqual([]);

    // #then and nothing classified has since been removed from it, beyond the
    // members a VERSION_SKEW row says this core does not carry here
    const stale = agentClassified.filter(
      (property) =>
        !agentSurface.includes(property) && !exemptAgent.includes(property),
    );
    expect(
      stale,
      `this file classifies inherited Agent member(s) [${stale.join(', ')}] that @mastra/core ${installedCore} does not expose there — drop them, and drop any override that exists only for them, or, if DurableAgent has merely started shadowing them, give each a VERSION_SKEW row naming both levels and classify it on the durable side as well.`,
    ).toEqual([]);

    // #then and the partition covers that surface exactly, plus exactly the
    // skewed members this core does not carry at this level
    expect(
      agentClassified.length,
      `the classified lists must partition the inherited Agent surface exactly — every member once, plus exactly the ${exemptAgent.length} skewed member(s) @mastra/core ${installedCore} does not expose there`,
    ).toBe(agentSurface.length + exemptAgent.length);

    // #then and it is entirely string-keyed: this partition enumerates own
    // STRING names, so a symbol-keyed execution member would sail past every
    // assertion above rather than land unclassified.
    expect(
      Object.getOwnPropertySymbols(Agent.prototype),
      'Agent.prototype now carries symbol-keyed member(s), which the name-based partition above cannot see — read each one and either classify it or block it',
    ).toEqual([]);
  });

  it('keeps the version-skew table honest against the installed core', () => {
    // #given the table's two halves are selected by comparing the installed
    // core with the declared peer, which only means "the pinned run" while that
    // peer is an EXACT version. Against a range, a pinned install would compare
    // unequal, take the canary half, and excuse the wrong names on both levels.
    expect(
      declaredPeer,
      'the version-skew halves key on @mastra/core being an exact peer pin; widen the peer and this mechanism must be redesigned',
    ).toMatch(/^\d+\.\d+\.\d+$/);

    // #when the installed core's two surfaces are enumerated
    const surface = Object.getOwnPropertyNames(DurableAgent.prototype);

    // #then every level a row names still holds that name in one of the lists
    // for that level: a row excuses a name from the stale check, it never
    // classifies it, and a member that moved level is classified on both sides
    for (const [name, row] of skewRows) {
      for (const level of [row.pin, row.newest]) {
        if (!level) continue;
        expect(
          level === 'durable' ? classified : agentClassified,
          `VERSION_SKEW says ${name} sits on the ${level} prototype at one of the two supported cores, so it must appear in one of that level's partition lists — the row excuses it from the stale check, it does not classify it`,
        ).toContain(name);
      }
    }

    // #then and the table's claim about the INSTALLED core holds, checked as a
    // claim rather than in one direction; the message says what to re-read
    // when it does not.
    for (const [name, row] of skewRows) {
      const claimed = levelHere(row);
      expect(
        { durable: surface.includes(name), agent: agentSurface.includes(name) },
        `VERSION_SKEW says ${name} sits on ${claimed ?? 'neither prototype'} at @mastra/core ${installedCore} (declared peer ${declaredPeer}). Re-read the installed dist: if both supported cores now agree, drop the row and let the stale check cover it again; if it is on neither core, delete it from the classification lists too; if it moved level again, update the row and classify it on the level it moved to.`,
      ).toEqual({ durable: claimed === 'durable', agent: claimed === 'agent' });
    }
  });

  it('keeps the base durable delegators out of the inherited surface', () => {
    // #given core also defines the delegators below on Agent as standalone
    // members. DurableAgent shadows them, so they never reach this instance
    // through the base — assert that rather than trusting it, because if core
    // ever stopped shadowing one, the base delegator would become a live
    // unguarded path.
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
    // #then the blocked ones must be OWN properties here: inherited from
    // Agent, they run core's implementation unrefused
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
    // #given the reverse of the named-member override checks: no override
    // exists which no table entry explains. An untabled throwing override
    // would refuse a caller with a reason nothing documents and no behavioral
    // row exercises.
    const expected = [
      ...guardedByRunner,
      ...blockedEntries,
      // FlowSafe's own members, which core has no say in.
      'constructor',
      'resumeViaRuntime',
      'authoritativeAgentStartState',
      'isRunLive',
      'proofExecutionFor',
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

  it('keeps requestRemoteAbort untabled and inherited', () => {
    // #given the abort primitive core reaches on its own. Blocking it would be
    // a well-formed block — a tabled reason, an override and a behavioral row
    // — so the structural assertions above hold either way and its exemption
    // needs an assertion of its own.
    const why =
      'core calls it from the `abort` closure it returns with each durable stream result, so refusing it rejects the abort handle of every run this class itself started';

    // #then no tabled reason, so nothing here demands an override for it
    expect(
      Object.hasOwn(BLOCKED_RUN_ENTRIES, 'requestRemoteAbort'),
      `requestRemoteAbort() must stay out of BLOCKED_RUN_ENTRIES: ${why}`,
    ).toBe(false);

    // #then and no own override, which is where a refusal would live
    expect(
      Object.hasOwn(FlowsafeDurableAgent.prototype, 'requestRemoteAbort'),
      `requestRemoteAbort() must stay inherited: ${why}`,
    ).toBe(false);
  });

  it('keeps resumeViaRuntime, the runner resume path, on the wrapper and off the DurableAgent base', () => {
    // #given resumeViaRuntime is FlowSafe's own, not part of core's surface
    const agent = createFlowsafeDurableAgent({
      agent: testAgent(),
      runtime: fakeRuntime(),
    });
    // #then the sanctioned resume path exists here and nowhere on the base.
    // That the INHERITED resume entry points are refused is proven by the
    // reason-table rows above, not by this test.
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
        'this wrapper starts runs through RunnerRuntime from the host start seam (streamUntilPersisted) and resumes them through the approval-decision path (resumeViaRuntime)',
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
   * `toHaveBeenCalled()`, never a count, for the reason registeredAgent()'s
   * notes give on the network rows. The base fails these calls for
   * its own reasons (the unreachable model rejects, and there is no persisted
   * run) — swallowed, because the claim is only that storage was reached BEFORE
   * they did.
   *
   * The rows in `vacuousByConstruction` take the inverted control below: their
   * base path reaches no store to spy on, for the reasons registeredAgent()'s
   * notes give, so what is worth asserting about them is that absence.
   */
  const vacuousByConstruction: readonly (keyof typeof BLOCKED_RUN_ENTRIES)[] = [
    '__registerMastra',
    '__setDeclaredSchedules',
    '__setMastra',
    '__setMemory',
    '__setPubSub',
    '__setThreadRuntimeAgent',
    'abortRunStream',
    'abortThreadStream',
    'discoverThreadPeers',
    'generateLegacy',
    'listActiveThreadRuns',
    'setChannels',
    'streamLegacy',
  ];

  /**
   * A member the INSTALLED core does not expose on the base has no base path to
   * observe in either direction, so neither control can bite on it. Require
   * that absence to be a RECORDED skew rather than skipping on it: an absence
   * no VERSION_SKEW row names means the override refuses a member neither
   * supported core has, and belongs nowhere.
   */
  const baseCarries = (agent: DurableAgent, method: string): boolean => {
    if (
      typeof (agent as unknown as Record<string, unknown>)[method] ===
      'function'
    ) {
      return true;
    }
    expect(
      skewNames,
      `${method}() is blocked and has a behavioral row, but @mastra/core ${installedCore} does not expose it on the base at all. Either record it in VERSION_SKEW with the level it holds on the other supported core, or drop the override and its row.`,
    ).toContain(method);
    return false;
  };

  for (const { method, invoke } of blockedCalls.filter(
    (call) => !vacuousByConstruction.includes(call.method),
  )) {
    it(`${method}() reaches storage on the unmodified base`, async () => {
      // #given the same spied storage, but the stock DurableAgent
      const { agent, getStore } = await registerWithSpies(
        new DurableAgent({ agent: testAgent() }),
      );
      if (!baseCarries(agent, method)) return;

      // #when the base implementation runs
      await invoke(agent as unknown as FlowsafeDurableAgent).catch(
        () => undefined,
      );

      // #then it got as far as the workflows store
      expect(
        getStore,
        `${method}() does not reach storage on the unmodified base, so the refusal row's "read nothing on the way out" assertion passes vacuously. Re-read the base implementation: a member whose base never reaches storage belongs in vacuousByConstruction.`,
      ).toHaveBeenCalled();
    });
  }

  /**
   * The inverted control, for the rows the loop above excludes. Their grading —
   * the base reaches no store either, so the refusal row's "read nothing on the
   * way out" proves nothing about storage for them — is a claim about core, so
   * drive them the same way and require the store to stay untouched. A core
   * that starts reading storage on one of them fails here and the row moves
   * into the loop above, which is what makes the exclusion expire on its own
   * rather than rest on the notes.
   */
  for (const { method, invoke } of blockedCalls.filter((call) =>
    vacuousByConstruction.includes(call.method),
  )) {
    it(`${method}() reaches no storage on the unmodified base`, async () => {
      // #given the same spied storage and the same stock DurableAgent
      const { agent, getStore } = await registerWithSpies(
        new DurableAgent({ agent: testAgent() }),
      );
      if (!baseCarries(agent, method)) return;

      // #when the base implementation runs
      await invoke(agent as unknown as FlowsafeDurableAgent).catch(
        () => undefined,
      );

      // #then it never resolved a store, which is the grading that keeps it out
      // of the control above
      expect(
        getStore,
        `${method}() now reaches storage on the unmodified base, so its refusal row is no longer vacuous — re-read the base implementation, then drop it from vacuousByConstruction, which moves it into the control above`,
      ).not.toHaveBeenCalled();
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
