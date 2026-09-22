// SPDX-License-Identifier: Apache-2.0

import type { Agent } from '@mastra/core/agent';
import {
  AGENT_STREAM_TOPIC,
  globalRunRegistry,
} from '@mastra/core/agent/durable';
import { Mastra } from '@mastra/core/mastra';
import type { MastraCompositeStore } from '@mastra/core/storage';
import { isPrincipalPermissions } from '@proofoftech/breakwater/rbac';
import {
  AgentRunSelectorMismatchError,
  type AuthoritativeAgentStartState,
  type LegacyAgentRunState,
} from '../agent-runner/durable-agent-runner.js';
import {
  AGENT_ENTRY_PATHS,
  AGENT_RUN_STORAGE_KEY_PREFIX,
  type AgentEntryPath,
  type AgentRunRecord,
  type AgentStartAuthority,
  type AgentThreadBinding,
  bindAgentThread,
  createFlowsafeDurableAgent,
  deleteAgentRunRecord,
  deleteAgentThreadBinding,
  type FlowsafeDurableAgent,
  type FlowsafeDurableAgentOptions,
  readAgentRunRecord,
  readAgentThreadBinding,
  writeAgentRunRecord,
} from '../agent-runner/index.js';
import {
  type ApprovalAuditSink,
  type ApprovalRecord,
  type ApprovalService,
  canonicalResourceOwner,
  type ExecutionPrincipal,
  principalActor,
  principalAuditFields,
  principalOwner,
  type RecoverableResourceOwnershipStore,
  type ResourceOwner,
  RUN_START_ROLES,
  samePrincipal,
} from '../approval-api/index.js';
import {
  type AutomatedExecutionPrincipal,
  assertExecutionPrincipal,
  isExecutionPrincipalId,
} from '../approval-api/principal.js';
import {
  type D1RunExecutionIdentity,
  ExecutionFenceUnreadableError,
  normalizeD1RunExecutionIdentity,
  normalizeMutationEpoch,
  normalizeRunExecutionIdentity,
  type RunExecutionIdentity,
  RunStartPendingError,
} from '../do-runner/execution-admission.js';
import {
  DoStatusError,
  isPathSafeId,
  type RequestContextProvider,
  type RunSummary,
  resolveScheduleStartOwner,
  resourceIdFromKey,
  type ScheduleSourceAgentTarget,
  type ScheduleSourceStore,
  SUSPENSION_TIMEOUT_RESUME_KEY,
  type ThreadScope,
} from '../do-runner/index.js';
import { isDefinitiveInitialAdmissionRefusal } from '../do-runner/initial-admission-refusal.js';
import { mastraRegistryEntries } from '../do-runner/mastra-registry.js';
import {
  lifecycleFromRequestContext,
  terminalCleanupFor,
} from '../do-runner/run-lifecycle.js';
import { isTerminalRunStatus } from '../do-runner/run-terminal-state.js';
import type {
  RecoveredStart,
  RunLifecycleTransitionResult,
} from '../do-runner/runtime.js';
import {
  captureReservation,
  type StartReservationReading,
  sameReservationIdentity,
} from '../do-runner/start-reservation-contract.js';
import { persistedStartRecord } from '../host-kit/do-response.js';
import {
  abandonApprovalsForRun,
  reconcileApprovalsForSummary,
} from '../host-kit/index.js';
import { createAgentModuleCatalog } from './catalog.js';
import {
  AGENT_HOST_ROUTE_PREFIX,
  publicAgentRunEnvelope,
} from './thread-topology.js';
import {
  createTrustedAgentRequestContext,
  deriveTrustedAgentContext,
  sanitizeStoredAgentContext,
} from './trusted-context.js';
import type {
  AgentModule,
  AgentModuleCatalog,
  AgentRunEnvelope,
  PrincipalPermissionResolution,
  PrincipalPermissionResolver,
  TrustedAgentExecution,
} from './types.js';

export interface AgentThreadStateStorage {
  get<T = unknown>(key: string): Promise<T | undefined>;
  put<T>(key: string, value: T): Promise<void>;
  delete(key: string): Promise<boolean>;
  list<T = unknown>(options: { prefix: string }): Promise<Map<string, T>>;
  getAlarm(): Promise<number | null>;
  setAlarm(scheduledTime: number | Date): Promise<void>;
  deleteAlarm(): Promise<void>;
}

export interface AgentThreadInstanceScope {
  readonly threadId: string;
  readonly deploymentTag?: string;
  readonly init: ThreadScope['init'];
}

/** What the host is asked to authorize. Never a human — those go by role. */
export interface AutomatedEntryRequest {
  principal: AutomatedExecutionPrincipal;
  agentId: string;
  entryPath: AgentEntryPath;
  deploymentTag?: string;
  threadId: string;
}

/**
 * Optional host policy over automated entry, AND-composed with the agent's own
 * `allowedAutomation`. It can only DENY: returning true where the agent did not
 * declare the entry changes nothing, so a host cannot widen automation from its
 * wiring. Absent means "no additional restriction", not "allow".
 */
export type AutomatedEntryAuthorizer = (
  request: AutomatedEntryRequest,
) => boolean | Promise<boolean>;

// Moved to types.ts beside the execution shape that now carries a resolution;
// re-exported so '@proofoftech/flowsafe/agent-host' keeps its surface.
export type {
  PrincipalPermissionResolution,
  PrincipalPermissionResolver,
} from './types.js';

export interface ThreadAgentHostOptions {
  /** Narrows automated entry beyond what each agent's metadata declares. */
  authorizeAutomatedEntry?: AutomatedEntryAuthorizer;
  /**
   * Server-owned principal-to-permission resolver. Required by any catalog
   * agent that declares `requiredPermissions` — such an agent fails closed
   * when this seam is absent. When configured, it runs on EVERY authorized
   * entry and its resolution is projected into the run's derived request
   * context as `breakwater.principalPermissions`, which is what a connector
   * declaring `requiredPermissions` enforces against.
   */
  resolvePrincipalPermissions?: PrincipalPermissionResolver;
  buildModules:
    | ((scope: AgentThreadInstanceScope) => readonly AgentModule[])
    | ((scope: AgentThreadInstanceScope) => Promise<readonly AgentModule[]>);
  storage: (scope: AgentThreadInstanceScope) => MastraCompositeStore;
  stateStorage: () => AgentThreadStateStorage;
  /** Deployment registry used to atomically bind thread/resource/run ids. */
  resourceAccess: () => RecoverableResourceOwnershipStore;
  /** Existing schedules domain used to verify schedule.fire target provenance. */
  scheduleSource?: () => ScheduleSourceStore;
  /** Authoritatively discard an executing or same-run settled dispatch. */
  discardScheduleDispatch?: (
    scheduleId: string,
    dispatchId: string,
    runId: string,
  ) => Promise<void>;
  approvalService: (scope: AgentThreadInstanceScope) => ApprovalService;
  systemPrincipalId?: string;
  audit?: ApprovalAuditSink;
  cache?: FlowsafeDurableAgentOptions['cache'];
}

export interface ThreadAgentStartInput {
  readonly startReservation?: StartReservationReading;
  agentId: string;
  threadId: string;
  resourceId: string;
  runId: string;
  prompt?: string;
  messages?: Parameters<FlowsafeDurableAgent['stream']>[0];
  entryPath: AgentEntryPath;
  threaded?: boolean;
  /** Required for schedule.fire; never accepted from a public agent request. */
  scheduleId?: string;
  /** Prepared trigger authorizing this exact schedule fire and run id. */
  dispatchId?: string;
  /**
   * Trusted signal-dispatch marker set only after beginAgentScheduleDispatch
   * returned the executing lease. Direct scheduled starts leave it absent.
   */
  scheduleDispatchLease?: 'executing';
  /**
   * Non-reserved context accepted from trusted internal topology calls only.
   * It is never part of the public agent-start request contract.
   */
  safeContext?: Record<string, unknown>;
  /** Trusted, JSON-safe model provider options from schedule dispatch only. */
  providerOptions?: Record<string, unknown>;
  /**
   * The idempotency key the thread topology already RESERVED for this run.
   *
   * Nothing at this layer reserves, claims, or replays on it: the reservation
   * lives on the Worker side, where a retry that minted a fresh thread can
   * still be redirected to the original one. It travels down here only so
   * `RunnerRuntime.start` can compare it against the execution fence's
   * nominated proof key.
   * @internal
   */
  idempotencyKey?: string;
}

export interface BoundThreadAgent {
  agentId: string;
  resourceId: string;
  durableAgent: FlowsafeDurableAgent;
}

export interface BlockingAgentRun {
  runId: string;
  principal: ExecutionPrincipal;
}

export interface ThreadAgentHost {
  requestContextForRun(base?: RequestContextProvider): RequestContextProvider;
  /** Serialize one target-thread dispatch decision with public start/resume routes. */
  serializeDispatch<T>(operation: () => Promise<T>): Promise<T>;
  start(
    scope: ThreadScope,
    input: ThreadAgentStartInput,
  ): Promise<AgentRunEnvelope>;
  resolveBoundAgent(
    scope: ThreadScope,
    input: {
      agentId?: string;
      entryPath: AgentEntryPath;
    },
  ): Promise<BoundThreadAgent>;
  /**
   * Storage-backed nonterminal run occupying this thread, including after
   * eviction. Call through `serializeDispatch` when the result governs a
   * signal delivery or start decision.
   */
  blockingRun(scope: ThreadScope): Promise<BlockingAgentRun | undefined>;
  /** Recover/read the stable run id used by a threaded schedule wake. */
  scheduleDispatchStatus(
    scope: ThreadScope,
    input: { agentId: string; resourceId: string; runId: string },
  ): Promise<RunSummary | undefined>;
  recoverOwnership(scope: AgentThreadInstanceScope): Promise<void>;
  route(request: Request, scope: ThreadScope): Promise<Response | null>;
}

class AgentHostRequestError extends DoStatusError {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'AgentHostRequestError';
    this.status = status;
  }
}

function json(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json',
      'cache-control': 'no-store',
      'x-content-type-options': 'nosniff',
    },
  });
}

function decode(value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  try {
    return decodeURIComponent(value);
  } catch {
    return undefined;
  }
}

async function objectBody(request: Request): Promise<Record<string, unknown>> {
  let value: unknown;
  try {
    value = await request.json();
  } catch {
    throw new AgentHostRequestError(400, 'a JSON object body is required');
  }
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new AgentHostRequestError(400, 'a JSON object body is required');
  }
  return value as Record<string, unknown>;
}

const AGENT_OWNER_RECOVERY_PREFIX = 'flowsafe:agent-owner-recovery:v1:';
const AGENT_OWNER_RECOVERY_DELAY_MS = 60_000;

type AgentOwnerRecovery = {
  version: 2;
  agentId: string;
  threadId: string;
  resourceId: string;
  runId: string;
  owner: ResourceOwner;
  token: string;
  threaded: boolean;
  bindingPreexisting: boolean;
  runRecord: AgentRunRecord;
  startReservation?: StartReservationReading;
} & (
  | { phase: 'preparing'; execution?: never }
  | { phase: 'prepared'; execution: D1RunExecutionIdentity }
  | { phase: 'prepared-unfenced'; execution: RunExecutionIdentity }
);

function sameRunRecord(
  actual: AgentRunRecord | undefined,
  expected: AgentRunRecord,
): boolean {
  return (
    actual !== undefined &&
    actual.version === expected.version &&
    actual.agentId === expected.agentId &&
    actual.originEntryPath === expected.originEntryPath &&
    samePrincipal(actual.principal, expected.principal)
  );
}

function sameOwnerRecovery(
  actual: AgentOwnerRecovery,
  expected: AgentOwnerRecovery,
): boolean {
  const claim = actual.startReservation,
    wanted = expected.startReservation;
  return (
    actual.version === expected.version &&
    actual.phase === expected.phase &&
    actual.token === expected.token &&
    actual.agentId === expected.agentId &&
    actual.threadId === expected.threadId &&
    actual.resourceId === expected.resourceId &&
    actual.runId === expected.runId &&
    actual.owner.kind === expected.owner.kind &&
    actual.owner.id === expected.owner.id &&
    actual.threaded === expected.threaded &&
    actual.bindingPreexisting === expected.bindingPreexisting &&
    sameRunRecord(actual.runRecord, expected.runRecord) &&
    actual.execution?.tablePrefix === expected.execution?.tablePrefix &&
    actual.execution?.workflowId === expected.execution?.workflowId &&
    actual.execution?.runId === expected.execution?.runId &&
    actual.execution?.startToken === expected.execution?.startToken &&
    (claim === undefined
      ? wanted === undefined
      : wanted !== undefined &&
        sameReservationIdentity(claim, wanted) &&
        claim.state === wanted.state &&
        claim.updatedAt === wanted.updatedAt &&
        claim.binding.kind === wanted.binding.kind)
  );
}

function entryPath(value: unknown): AgentEntryPath {
  if (
    typeof value !== 'string' ||
    !(AGENT_ENTRY_PATHS as readonly string[]).includes(value)
  ) {
    throw new AgentHostRequestError(400, 'entryPath is required');
  }
  return value as AgentEntryPath;
}

function safeContext(value: unknown): Record<string, unknown> {
  if (
    value === undefined ||
    (value !== null && typeof value === 'object' && !Array.isArray(value))
  ) {
    return sanitizeStoredAgentContext(
      value as Record<string, unknown> | undefined,
    );
  }
  throw new AgentHostRequestError(400, 'safeContext must be an object');
}

function providerOptions(value: unknown): Record<string, unknown> | undefined {
  if (value === undefined) return undefined;
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new AgentHostRequestError(400, 'providerOptions must be an object');
  }
  return structuredClone(value as Record<string, unknown>);
}

function record(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function scheduleAgentContext(
  target: ScheduleSourceAgentTarget,
): Record<string, unknown> {
  const idle = record(target.ifIdle);
  const streamOptions = record(idle?.streamOptions);
  const streamContext = record(streamOptions?.requestContext);
  return {
    ...target.requestContext,
    ...streamContext,
  };
}

function resourceOwner(value: unknown): ResourceOwner {
  try {
    return canonicalResourceOwner(value);
  } catch {
    throw new AgentHostRequestError(400, 'resourceOwner is required');
  }
}

function requestedBy(value: unknown): string {
  if (!isExecutionPrincipalId(value)) {
    throw new AgentHostRequestError(400, 'requestedBy is malformed');
  }
  return value;
}

/**
 * The reserved suspension-timeout envelope is minted by a run object's alarm and
 * by nothing else. Agent runs never arm a suspension deadline, so a forged
 * envelope here could only mislead a step — but this route forwards client
 * resume data verbatim under `requestedByKind: 'human'`, and the guarantee the
 * feature sells is that no caller can present itself to a step as an expired
 * deadline. The KEY is refused, exactly as the workflow resume route refuses it,
 * so a step that reads the key directly cannot be fooled either.
 */
function resumeData(value: unknown): unknown {
  if (
    value !== null &&
    typeof value === 'object' &&
    SUSPENSION_TIMEOUT_RESUME_KEY in value
  ) {
    throw new AgentHostRequestError(
      400,
      `resume data must not carry the reserved '${SUSPENSION_TIMEOUT_RESUME_KEY}' key`,
    );
  }
  return value;
}

function createFifoLock(): <T>(operation: () => Promise<T>) => Promise<T> {
  let tail = Promise.resolve();
  return async <T>(operation: () => Promise<T>): Promise<T> => {
    const previous = tail;
    let release: () => void = () => undefined;
    tail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await operation();
    } finally {
      release();
    }
  };
}

function runRef(
  scope: ThreadScope,
  input: Record<string, unknown>,
): {
  agentId: string;
  threadId: string;
  resourceId: string;
  runId: string;
} {
  const { agentId, threadId, resourceId, runId } = input;
  if (
    !isPathSafeId(agentId) ||
    threadId !== scope.threadId ||
    !isPathSafeId(resourceId) ||
    !isPathSafeId(runId)
  ) {
    throw new AgentHostRequestError(404, 'run not found');
  }
  return { agentId, threadId: scope.threadId, resourceId, runId };
}

function audit(
  sink: ApprovalAuditSink | undefined,
  event: Parameters<ApprovalAuditSink>[0],
): void {
  try {
    const result = sink?.(event);
    if (result instanceof Promise) void result.catch(() => undefined);
  } catch {
    // Authorization is never made unavailable by an audit export failure.
  }
}

function normalizedPermissionResolution(
  value: unknown,
): PrincipalPermissionResolution {
  // Delegate the shape check to breakwater's guard — the SAME predicate its
  // connector required-permissions gate applies to the projection — so this
  // host can never mint a resolution breakwater would reject. Duplicates are
  // tolerated rather than treated as malformed: the all-of check has set
  // semantics, so a host that unions role bundles must not take an
  // availability hit for a repeat that cannot change any decision.
  if (!isPrincipalPermissions(value)) {
    throw new Error('permission resolution is malformed');
  }
  return Object.freeze({
    permissions: Object.freeze([...new Set(value.permissions)]),
    policyVersion: value.policyVersion,
  });
}

function ndjson(
  stream: ReadableStream<unknown>,
  initialOffset: number,
): Response {
  const reader = stream.getReader();
  const encoder = new TextEncoder();
  let offset = initialOffset;
  return new Response(
    new ReadableStream<Uint8Array>({
      async pull(controller) {
        const next = await reader.read();
        if (next.done) {
          controller.close();
          return;
        }
        offset += 1;
        controller.enqueue(
          encoder.encode(`${JSON.stringify({ offset, event: next.value })}\n`),
        );
      },
      async cancel(reason) {
        await reader.cancel(reason);
      },
    }),
    {
      headers: {
        'content-type': 'application/x-ndjson; charset=utf-8',
        'cache-control': 'no-store',
        'x-content-type-options': 'nosniff',
      },
    },
  );
}

export function createThreadAgentHost(
  options: ThreadAgentHostOptions,
): ThreadAgentHost {
  let catalogPromise: Promise<AgentModuleCatalog> | undefined;
  let runtime:
    | {
        scopeRuntime: ThreadScope['init']['runtime'];
        catalog: AgentModuleCatalog;
        agents: Map<string, FlowsafeDurableAgent>;
      }
    | undefined;
  const executions = new Map<string, TrustedAgentExecution>();
  let stableScope: AgentThreadInstanceScope | undefined;
  const withBindingLock = createFifoLock();
  const withDispatchLock = createFifoLock();
  const withRecoveryLock = createFifoLock();
  /** Claim durability does not establish current run liveness. */
  const startsInFlight = new Set<string>();
  const unwoundExecutions = new WeakSet<TrustedAgentExecution>();

  const instanceScopeFor = (
    scope: AgentThreadInstanceScope,
  ): AgentThreadInstanceScope => {
    if (stableScope) {
      if (
        stableScope.threadId !== scope.threadId ||
        stableScope.deploymentTag !== scope.deploymentTag ||
        stableScope.init !== scope.init
      ) {
        throw new Error(
          'thread agent host cannot be shared across DO instances',
        );
      }
      return stableScope;
    }
    stableScope = Object.freeze({
      threadId: scope.threadId,
      deploymentTag: scope.deploymentTag,
      init: scope.init,
    });
    return stableScope;
  };

  const catalogFor = async (
    scope: AgentThreadInstanceScope,
  ): Promise<AgentModuleCatalog> => {
    catalogPromise ??= Promise.resolve(
      options.buildModules(instanceScopeFor(scope)),
    )
      .then(createAgentModuleCatalog)
      .catch((error: unknown) => {
        catalogPromise = undefined;
        throw error;
      });
    return catalogPromise;
  };

  const runtimeFor = async (scope: AgentThreadInstanceScope) => {
    instanceScopeFor(scope);
    const catalog = await catalogFor(scope);
    if (runtime) {
      if (runtime.scopeRuntime !== scope.init.runtime) {
        throw new Error(
          'thread agent host cannot be shared across DO instances',
        );
      }
      return runtime;
    }
    const mastra = new Mastra({
      storage: options.storage(instanceScopeFor(scope)),
      // Preserve ordinary key lookup, but remap Object.prototype collisions in
      // Mastra's plain-object registry. Resolve those agents by intrinsic id.
      agents: Object.fromEntries(
        mastraRegistryEntries(
          catalog.modules.map(
            (module) =>
              [module.meta.id, module.agent as unknown as Agent] as const,
          ),
          'catalog-agent',
        ),
      ),
      ...(scope.init.pubsub ? { pubsub: scope.init.pubsub } : {}),
    });
    const agents = new Map<string, FlowsafeDurableAgent>();
    for (const module of catalog.modules) {
      agents.set(
        module.meta.id,
        createFlowsafeDurableAgent({
          agent: mastra.getAgentById(module.meta.id),
          runtime: scope.init.runtime,
          pubsub: scope.init.pubsub,
          threadRuntime: mastra.agentThreadStreamRuntime,
          maxSteps: module.agent.maxSteps,
          cache: options.cache,
        }),
      );
    }
    runtime = { scopeRuntime: scope.init.runtime, catalog, agents };
    return runtime;
  };

  /**
   * The one entry gate, split by principal kind because the two kinds are
   * authorized by different things and must not fall through to each other.
   *
   * A human passes the route-level start roles intersected with the agent's own
   * allowedRoles, exactly as before. An automated principal never consults
   * roles at all: it must be declared in the agent's `allowedAutomation` for
   * this precise entry path, AND survive the host's optional authorizer. Absent
   * declaration denies — which is why a scheduled start of an agent that has
   * not opted in fails here rather than executing through a role-bearing
   * human identity.
   *
   * After that kind-specific gate, a configured server-owned resolver runs for
   * EVERY granted entry: an agent that declares required permissions also
   * requires every identifier from the resolution, and the resolution itself
   * is returned so the execution projects it into derived request context —
   * the input to breakwater's connector required-permissions gate. A failed
   * resolution fails closed at the matching scope: it denies a
   * permission-requiring agent, and it costs any other run its projection
   * (audited as `agent.permissions.resolve`), so a permission-declaring
   * connector inside that run denies rather than executing unauthorized.
   */
  const authorize = async (
    scope: ThreadScope,
    agentId: string,
    entry: AgentEntryPath,
    principal: ExecutionPrincipal,
  ) => {
    const current = await runtimeFor(scope);
    const module = current.catalog.get(agentId);
    const requiredPermissions = module?.meta.requiredPermissions;
    let permissionPolicyVersion: string | null = null;
    let principalPermissions: PrincipalPermissionResolution | null = null;
    let decision: 'allowed' | 'denied' | 'error' = 'denied';
    let reason: string | undefined;
    let granted = false;
    if (module === undefined) {
      reason = 'agent is not registered';
    } else if (principal.kind === 'human') {
      const allowed = current.catalog.allowedRoles(agentId);
      granted =
        RUN_START_ROLES.includes(principal.role) &&
        allowed?.includes(principal.role) === true;
      if (!granted) reason = 'role is not allowed to mutate this agent';
    } else if (!current.catalog.automationAllowed(agentId, principal, entry)) {
      reason = `agent does not accept '${principal.kind}' principals on entry path '${entry}'`;
    } else {
      // AND-composed: the injected authorizer can only narrow what the agent
      // already declared. A host cannot widen automation from wiring.
      const hostAllows =
        (await options.authorizeAutomatedEntry?.({
          principal,
          agentId,
          entryPath: entry,
          deploymentTag: scope.deploymentTag,
          threadId: scope.threadId,
        })) ?? true;
      granted = hostAllows;
      if (!granted) reason = 'host denied this automated entry';
    }
    const resolver = options.resolvePrincipalPermissions;
    if (granted && resolver) {
      try {
        principalPermissions = normalizedPermissionResolution(
          await resolver(principal),
        );
        permissionPolicyVersion = principalPermissions.policyVersion;
      } catch {
        if (requiredPermissions !== undefined) {
          decision = 'error';
          granted = false;
          reason = 'permission resolution failed';
        } else {
          // The entry stays granted — this agent requires no permissions —
          // but the run loses its projection, so a permission-declaring
          // connector inside it fails closed. A dedicated event says so,
          // because the entry event below reports this entry as allowed.
          audit(options.audit, {
            actor: principalActor(principal),
            action: 'agent.permissions.resolve',
            resource: `agent:${agentId}`,
            decision: 'error',
            reason: 'permission resolution failed',
            detail: {
              agentId,
              ...(scope.deploymentTag !== undefined
                ? { tenantId: scope.deploymentTag }
                : {}),
              threadId: scope.threadId,
              entryPath: entry,
              ...principalAuditFields(principal),
              permissionPolicyVersion: null,
            },
          });
        }
      }
    }
    if (granted && requiredPermissions !== undefined) {
      if (!resolver) {
        granted = false;
        reason = 'permission resolver is not configured';
      } else if (principalPermissions) {
        const effective = new Set(principalPermissions.permissions);
        granted = requiredPermissions.every((permission) =>
          effective.has(permission),
        );
        if (!granted) {
          reason = 'required permissions are not satisfied';
        }
      }
    }
    if (granted) decision = 'allowed';
    audit(options.audit, {
      actor: principalActor(principal),
      action: 'agent.entry.authorize',
      resource: `agent:${agentId}`,
      decision,
      ...(reason !== undefined ? { reason } : {}),
      detail: {
        agentId,
        ...(scope.deploymentTag !== undefined
          ? { tenantId: scope.deploymentTag }
          : {}),
        threadId: scope.threadId,
        entryPath: entry,
        ...principalAuditFields(principal),
        ...(requiredPermissions !== undefined
          ? { requiredPermissions, permissionPolicyVersion }
          : {}),
      },
    });
    if (!module) throw new AgentHostRequestError(404, 'agent not found');
    if (!granted) throw new AgentHostRequestError(403, 'forbidden');
    return { current, module, principalPermissions };
  };

  const readBinding = (): Promise<AgentThreadBinding | undefined> =>
    readAgentThreadBinding(options.stateStorage());

  const readRun = (runId: string): Promise<AgentRunRecord | undefined> =>
    readAgentRunRecord(options.stateStorage(), runId);

  const findBlockingRun = async (
    scope: AgentThreadInstanceScope,
  ): Promise<BlockingAgentRun | undefined> => {
    const storage = options.stateStorage();
    const records = await storage.list<unknown>({
      prefix: AGENT_RUN_STORAGE_KEY_PREFIX,
    });
    if (records.size === 0) return undefined;
    for (const key of records.keys()) {
      const runId = key.slice(AGENT_RUN_STORAGE_KEY_PREFIX.length);
      const runRecord = await readRun(runId);
      if (!runRecord) continue;
      if (executions.has(runId))
        return { runId, principal: runRecord.principal };
      const state = await selectedAgentState(
        scope,
        {
          agentId: runRecord.agentId,
          resourceId: resourceIdFromKey(scope.threadId),
          runId,
        },
        { includeLegacy: true },
      );
      if (
        !state ||
        state.kind === 'initial' ||
        !isTerminalRunStatus(state.summary.status) ||
        (state.kind === 'legacy' && !isTerminalRunStatus(state.snapshot.status))
      )
        return { runId, principal: runRecord.principal };
      const recovery = await storage.get(ownerRecoveryKey(runId));
      if (recovery !== undefined)
        return { runId, principal: runRecord.principal };
      await withRecoveryLock(() =>
        finalizeTerminalRecord(scope, runId, runRecord, state),
      );
    }
    return undefined;
  };

  const ownerRecoveryKey = (runId: string): string =>
    AGENT_OWNER_RECOVERY_PREFIX + runId;

  const resolveStartSource = async (
    scope: ThreadScope,
    ref: {
      agentId: string;
      threadId: string;
      resourceId: string;
      runId: string;
    },
    entry: AgentEntryPath,
    threaded: boolean,
    scheduleId: string | undefined,
    dispatchId: string | undefined,
  ): Promise<{
    owner: ResourceOwner;
    target?: ScheduleSourceAgentTarget;
  }> => {
    const ownership = options.resourceAccess();
    if (
      entry !== 'schedule.fire' &&
      (scheduleId !== undefined || dispatchId !== undefined)
    ) {
      throw new AgentHostRequestError(404, 'run not found');
    }
    if (entry === 'schedule.fire') {
      if (!isPathSafeId(scheduleId) || !isPathSafeId(dispatchId)) {
        throw new AgentHostRequestError(404, 'run not found');
      }
      const schedules = options.scheduleSource?.();
      const source = schedules
        ? await resolveScheduleStartOwner(
            schedules,
            ownership,
            scheduleId,
            dispatchId,
            ref.runId,
            threaded
              ? {
                  type: 'agent',
                  mode: 'threaded-wake',
                  agentId: ref.agentId,
                  threadId: ref.threadId,
                  resourceId: ref.resourceId,
                }
              : {
                  type: 'agent',
                  mode: 'threadless-start',
                  agentId: ref.agentId,
                },
          )
        : undefined;
      if (!source) throw new AgentHostRequestError(404, 'run not found');
      return source;
    }
    if (entry === 'http.start') {
      return { owner: principalOwner(scope.principal) };
    }
    if (!threaded) throw new AgentHostRequestError(404, 'run not found');
    const [threadOwner, resolvedResourceOwner] = await Promise.all([
      ownership.owner('thread', ref.threadId),
      ownership.owner('resource', ref.resourceId),
    ]);
    if (
      !threadOwner ||
      !resolvedResourceOwner ||
      threadOwner.kind !== resolvedResourceOwner.kind ||
      threadOwner.id !== resolvedResourceOwner.id
    ) {
      throw new AgentHostRequestError(404, 'run not found');
    }
    return { owner: threadOwner };
  };

  const ensureOwnerRecoveryAlarm = async (
    storage: AgentThreadStateStorage,
  ): Promise<void> => {
    const scheduled = Date.now() + AGENT_OWNER_RECOVERY_DELAY_MS;
    const existing = await storage.getAlarm();
    if (existing === null || existing > scheduled) {
      await storage.setAlarm(scheduled);
    }
  };

  const armOwnerRecovery = async (
    recovery: AgentOwnerRecovery,
  ): Promise<void> =>
    withRecoveryLock(async () => {
      const storage = options.stateStorage();
      await ensureOwnerRecoveryAlarm(storage);
      await storage.put(ownerRecoveryKey(recovery.runId), recovery);
      await ensureOwnerRecoveryAlarm(storage);
    });

  const clearOwnerRecovery = async (
    recovery: AgentOwnerRecovery,
  ): Promise<void> =>
    withRecoveryLock(async () => {
      const storage = options.stateStorage();
      const key = ownerRecoveryKey(recovery.runId);
      const current = await storage.get(key);
      if (
        current === undefined ||
        !sameOwnerRecovery(
          validateOwnerRecovery(recovery.threadId, key, current),
          recovery,
        )
      )
        throw new Error('agent owner recovery changed');
      await storage.delete(key);
    });

  const releaseEphemeralOwnerClaims = async (
    recovery: AgentOwnerRecovery,
  ): Promise<void> => {
    const ownership = options.resourceAccess();
    const release = async (
      kind: 'resource' | 'thread',
      resourceId: string,
    ): Promise<void> => {
      if (await ownership.release(kind, resourceId, recovery.owner)) return;
      if (await ownership.owner(kind, resourceId)) {
        throw new Error(
          `${kind} '${resourceId}' ownership could not be released`,
        );
      }
    };
    await Promise.all([
      release('resource', recovery.resourceId),
      release('thread', recovery.threadId),
    ]);
  };

  const validateOwnerRecovery = (
    threadId: string,
    key: string,
    value: unknown,
  ): AgentOwnerRecovery => {
    try {
      const stored = persistedStartRecord(value);
      if (
        stored.version !== 2 ||
        stored.threadId !== threadId ||
        !isPathSafeId(stored.agentId) ||
        !isPathSafeId(stored.threadId) ||
        !isPathSafeId(stored.resourceId) ||
        stored.resourceId !== resourceIdFromKey(threadId) ||
        !isPathSafeId(stored.runId) ||
        !isPathSafeId(stored.token) ||
        ownerRecoveryKey(stored.runId) !== key ||
        typeof stored.threaded !== 'boolean' ||
        typeof stored.bindingPreexisting !== 'boolean'
      )
        throw new Error('invalid journal');
      const owner = resourceOwner(persistedStartRecord(stored.owner));
      const rawRecord = persistedStartRecord(stored.runRecord);
      if (rawRecord.version !== 2 || rawRecord.agentId !== stored.agentId)
        throw new Error('invalid run record');
      const runRecord: AgentRunRecord = {
        version: 2,
        agentId: stored.agentId,
        principal: assertExecutionPrincipal(
          persistedStartRecord(rawRecord.principal),
          'stored agent run',
        ),
        originEntryPath: entryPath(rawRecord.originEntryPath),
      };
      const rawClaim =
        stored.startReservation === undefined
          ? undefined
          : persistedStartRecord(stored.startReservation);
      const claim =
        rawClaim === undefined
          ? undefined
          : captureReservation(
              {
                ...rawClaim,
                owner: persistedStartRecord(rawClaim.owner),
                binding: persistedStartRecord(rawClaim.binding),
              } as unknown as StartReservationReading,
              'started',
            );
      if (
        claim &&
        (claim.targetKind !== 'agent' ||
          claim.targetId !== stored.agentId ||
          claim.threadId !== threadId ||
          claim.runId !== stored.runId ||
          claim.owner.kind !== runRecord.principal.kind ||
          claim.owner.id !== runRecord.principal.id)
      )
        throw new Error('invalid start claim');
      const base = {
        version: 2 as const,
        agentId: stored.agentId,
        threadId,
        resourceId: stored.resourceId,
        runId: stored.runId,
        token: stored.token,
        owner,
        threaded: stored.threaded,
        bindingPreexisting: stored.bindingPreexisting,
        runRecord,
        ...(claim ? { startReservation: claim } : {}),
      };
      if (stored.phase === 'preparing' && !Object.hasOwn(stored, 'execution'))
        return { ...base, phase: 'preparing' };
      if (stored.phase !== 'prepared' && stored.phase !== 'prepared-unfenced')
        throw new Error('invalid phase');
      const raw = persistedStartRecord(stored.execution);
      const execution = normalizeRunExecutionIdentity(raw);
      if (
        execution.tablePrefix !== raw.tablePrefix ||
        execution.runId !== stored.runId
      )
        throw new Error('invalid execution');
      return stored.phase === 'prepared'
        ? {
            ...base,
            phase: 'prepared',
            execution: normalizeD1RunExecutionIdentity(execution),
          }
        : { ...base, phase: 'prepared-unfenced', execution };
    } catch {
      throw new Error('stored agent owner recovery is malformed');
    }
  };

  const prepareOwnerRecovery = async (
    scope: AgentThreadInstanceScope,
    recovery: AgentOwnerRecovery,
    supplied: RunExecutionIdentity,
  ): Promise<AgentOwnerRecovery> => {
    const identity = normalizeRunExecutionIdentity(supplied);
    const currentRuntime = await runtimeFor(scope);
    const durable = currentRuntime.agents.get(recovery.agentId);
    if (
      !durable ||
      identity.workflowId !== durable.getWorkflow().id ||
      identity.runId !== recovery.runId
    )
      throw new Error('prepared agent identity mismatch');
    const prepared: AgentOwnerRecovery = scope.init.runtime.executionFence
      ? {
          ...recovery,
          phase: 'prepared',
          execution: normalizeD1RunExecutionIdentity(identity),
        }
      : { ...recovery, phase: 'prepared-unfenced', execution: identity };
    return withRecoveryLock(async () => {
      const storage = options.stateStorage(),
        key = ownerRecoveryKey(recovery.runId);
      const current = validateOwnerRecovery(
        scope.threadId,
        key,
        await storage.get(key),
      );
      if (sameOwnerRecovery(current, prepared)) return prepared;
      if (
        current.phase !== 'preparing' ||
        !sameOwnerRecovery(current, recovery)
      )
        throw new Error('agent owner recovery changed');
      try {
        await storage.put(key, prepared);
      } catch (error) {
        const reread = await storage.get(key);
        if (
          reread === undefined ||
          !sameOwnerRecovery(
            validateOwnerRecovery(scope.threadId, key, reread),
            prepared,
          )
        )
          throw error;
      }
      return prepared;
    });
  };

  const workflowIdFor = async (
    scope: AgentThreadInstanceScope,
    agentId: string,
  ): Promise<string> => {
    const current = await runtimeFor(scope);
    const durable = current.agents.get(agentId);
    if (!durable) throw new AgentHostRequestError(404, 'agent not found');
    return durable.getWorkflow().id;
  };

  type NormalAgentRunState = AuthoritativeAgentStartState | LegacyAgentRunState;

  async function selectedAgentState(
    scope: AgentThreadInstanceScope,
    ref: { agentId: string; resourceId: string; runId: string },
    readOptions: { readonly includeLegacy: true },
  ): Promise<NormalAgentRunState | null>;
  async function selectedAgentState(
    scope: AgentThreadInstanceScope,
    ref: { agentId: string; resourceId: string; runId: string },
  ): Promise<AuthoritativeAgentStartState | null>;
  async function selectedAgentState(
    scope: AgentThreadInstanceScope,
    ref: { agentId: string; resourceId: string; runId: string },
    readOptions?: { readonly includeLegacy: true },
  ): Promise<NormalAgentRunState | null> {
    const includeLegacy = readOptions?.includeLegacy === true;
    if (ref.resourceId !== resourceIdFromKey(scope.threadId))
      throw new AgentHostRequestError(404, 'run not found');
    const current = await runtimeFor(scope);
    const durable = current.agents.get(ref.agentId);
    if (!current.catalog.get(ref.agentId) || !durable)
      throw new AgentHostRequestError(404, 'agent not found');
    if (includeLegacy)
      return durable.authoritativeAgentStartState(
        scope.init.runtime,
        scope.threadId,
        ref.runId,
        { includeLegacy: true },
      );
    return durable.authoritativeAgentStartState(
      scope.init.runtime,
      scope.threadId,
      ref.runId,
    );
  }

  const publicAgentState = async (
    scope: AgentThreadInstanceScope,
    ref: { agentId: string; resourceId: string; runId: string },
  ): Promise<NormalAgentRunState | null> => {
    try {
      return await selectedAgentState(scope, ref, { includeLegacy: true });
    } catch (error) {
      if (error instanceof AgentRunSelectorMismatchError)
        throw new AgentHostRequestError(404, 'run not found');
      throw error;
    }
  };

  const matchRecoveryState = (
    recovery: AgentOwnerRecovery,
    state: AuthoritativeAgentStartState | null,
  ): AuthoritativeAgentStartState => {
    const expected = recovery.execution;
    if (
      !state ||
      !expected ||
      state.execution.tablePrefix !== expected.tablePrefix ||
      state.execution.workflowId !== expected.workflowId ||
      state.execution.runId !== expected.runId ||
      state.execution.startToken !== expected.startToken ||
      state.threaded !== recovery.threaded ||
      state.execution.target.kind !== 'agent' ||
      state.execution.target.id !== recovery.agentId ||
      state.execution.target.threadId !== recovery.threadId ||
      state.execution.owner.kind !== recovery.runRecord.principal.kind ||
      state.execution.owner.id !== recovery.runRecord.principal.id
    )
      throw new Error('agent owner recovery does not match the execution');
    return state;
  };

  const assertRecoveryCurrent = async (
    recovery: AgentOwnerRecovery,
  ): Promise<void> => {
    const key = ownerRecoveryKey(recovery.runId),
      current = await options.stateStorage().get(key);
    if (
      current === undefined ||
      !sameOwnerRecovery(
        validateOwnerRecovery(recovery.threadId, key, current),
        recovery,
      )
    )
      throw new Error('agent owner recovery changed');
  };

  const assertLegacyTerminalCurrent = async (
    scope: AgentThreadInstanceScope,
    ref: { agentId: string; resourceId: string; runId: string },
    state: LegacyAgentRunState,
    expectedRecord: AgentRunRecord | undefined,
  ): Promise<void> => {
    if (
      !isTerminalRunStatus(state.snapshot.status) ||
      !isTerminalRunStatus(state.summary.status) ||
      state.address.runId !== ref.runId ||
      record(state.snapshot.context?.input)?.agentId !== ref.agentId
    )
      throw new ExecutionFenceUnreadableError(
        'legacy run cleanup is unresolved',
      );
    const quiescent = () =>
      !executions.has(ref.runId) &&
      !scope.init.runtime.isRunActive(state.address.workflowId, ref.runId);
    if (!quiescent()) throw new RunStartPendingError();
    const [binding, current, journal] = await Promise.all([
      readBinding(),
      readRun(ref.runId),
      options.stateStorage().get(ownerRecoveryKey(ref.runId)),
    ]);
    if (journal !== undefined)
      throw new ExecutionFenceUnreadableError(
        'legacy run cleanup is unresolved',
      );
    const bindingMatches =
      binding?.agentId === ref.agentId && binding.resourceId === ref.resourceId;
    if ((state.threaded && !bindingMatches) || (!state.threaded && binding))
      throw new AgentHostRequestError(404, 'run not found');
    if (
      current !== undefined &&
      (expectedRecord === undefined || !sameRunRecord(current, expectedRecord))
    )
      throw new Error('agent run record changed');
    if (!quiescent()) throw new RunStartPendingError();
  };

  const finalizeTerminalRecord = async (
    scope: AgentThreadInstanceScope,
    runId: string,
    expected: AgentRunRecord,
    state: NormalAgentRunState,
    ownFrame?: TrustedAgentExecution,
  ): Promise<void> => {
    if (state.kind === 'initial' || !isTerminalRunStatus(state.summary.status))
      throw new RunStartPendingError();
    if (state.kind === 'legacy') {
      const ref = {
        agentId: expected.agentId,
        resourceId: resourceIdFromKey(scope.threadId),
        runId,
      };
      await assertLegacyTerminalCurrent(scope, ref, state, expected);
      const current = await readRun(runId);
      if (current !== undefined && !sameRunRecord(current, expected))
        throw new Error('agent run record changed');
      await assertLegacyTerminalCurrent(scope, ref, state, expected);
      if (current) await deleteAgentRunRecord(options.stateStorage(), runId);
      return;
    }
    if (
      state.execution.target.kind !== 'agent' ||
      state.execution.target.id !== expected.agentId ||
      state.execution.target.threadId !== scope.threadId ||
      state.execution.owner.kind !== expected.principal.kind ||
      state.execution.owner.id !== expected.principal.id
    )
      throw new ExecutionFenceUnreadableError(
        'run start recovery is unresolved',
      );
    const quiescent = (): boolean => {
      const active = executions.get(runId);
      return (
        (active === undefined ||
          (active === ownFrame && unwoundExecutions.has(active))) &&
        !scope.init.runtime.isRunActive(state.execution.workflowId, runId)
      );
    };
    if (!quiescent()) throw new RunStartPendingError();
    await scope.init.runtime.settleStartExecution(state);
    const current = await readRun(runId);
    if (current !== undefined && !sameRunRecord(current, expected))
      throw new Error('agent run record changed');
    if (!quiescent()) throw new RunStartPendingError();
    if (current) await deleteAgentRunRecord(options.stateStorage(), runId);
  };

  const finalizeJournalBookkeeping = async (
    recovery: AgentOwnerRecovery,
    summary: RunSummary,
  ): Promise<boolean> => {
    await assertRecoveryCurrent(recovery);
    if (!recovery.threaded && !isTerminalRunStatus(summary.status)) {
      await ensureOwnerRecoveryAlarm(options.stateStorage());
      return false;
    }
    if (!recovery.threaded) await releaseEphemeralOwnerClaims(recovery);
    if (isTerminalRunStatus(summary.status)) {
      const current = await readRun(recovery.runId);
      if (current !== undefined && !sameRunRecord(current, recovery.runRecord))
        throw new Error('agent run record changed');
      if (current)
        await deleteAgentRunRecord(options.stateStorage(), recovery.runId);
    }
    return true;
  };

  const finishLifecycle = async (
    scope: AgentThreadInstanceScope,
    recovery: AgentOwnerRecovery,
    transition: RunLifecycleTransitionResult,
  ): Promise<RunSummary> => {
    await assertRecoveryCurrent(recovery);
    const workflowId = recovery.execution?.workflowId;
    if (!workflowId) throw new Error('agent recovery has no execution');
    await options.resourceAccess().settleReservation(recovery.token, []);
    await assertRecoveryCurrent(recovery);
    if (!transition.cleanup.cleanupCompleted) {
      await abandonApprovalsForRun(
        options.approvalService(scope),
        workflowId,
        recovery.runId,
        transition.cleanup.status,
        systemPrincipalId,
      );
      const dispatch = transition.cleanup.scheduleDispatch;
      if (dispatch) {
        if (!options.discardScheduleDispatch)
          throw new Error(
            'scheduled agent termination requires a dispatch-discard hook',
          );
        await options.discardScheduleDispatch(
          dispatch.scheduleId,
          dispatch.dispatchId,
          recovery.runId,
        );
      }
      const ownership = options.resourceAccess();
      if (
        !(await ownership.release('run', recovery.runId, recovery.owner)) &&
        (await ownership.owner('run', recovery.runId))
      )
        throw new Error('run ownership could not be released');
    }
    await finalizeJournalBookkeeping(recovery, transition.summary);
    return transition.cleanup.cleanupCompleted
      ? transition.summary
      : scope.init.runtime.completeTerminalCleanup(
          workflowId,
          recovery.runId,
          transition.cleanup.revision,
        );
  };

  const finalizeOwnerRecovery = async (
    scope: AgentThreadInstanceScope,
    recovery: AgentOwnerRecovery,
    state: AuthoritativeAgentStartState,
    ownFrame?: TrustedAgentExecution,
  ): Promise<RunSummary> =>
    withBindingLock(async () => {
      if (recovery.startReservation && !scope.init.runtime.startIdempotency)
        throw new ExecutionFenceUnreadableError(
          'run start recovery is unresolved',
        );
      const active = executions.get(recovery.runId);
      if (
        (active !== undefined &&
          (active !== ownFrame || !unwoundExecutions.has(active))) ||
        scope.init.runtime.isRunActive(
          state.execution.workflowId,
          recovery.runId,
        )
      )
        throw new RunStartPendingError();
      await assertRecoveryCurrent(recovery);
      const selected = matchRecoveryState(recovery, state);
      if (selected.kind === 'initial') throw new RunStartPendingError();
      if (isTerminalRunStatus(selected.summary.status))
        await scope.init.runtime.settleStartExecution(
          selected,
          recovery.startReservation,
        );
      const cleanup = terminalCleanupFor(
        lifecycleFromRequestContext(selected.snapshot.requestContext),
      );
      let summary = selected.summary;
      const clear = await withRecoveryLock(async () => {
        await assertRecoveryCurrent(recovery);
        if (cleanup) {
          summary = await finishLifecycle(scope, recovery, {
            summary,
            transitioned: false,
            casMatched: true,
            cleanup,
          });
          return true;
        }
        await options.resourceAccess().settleReservation(recovery.token, []);
        return finalizeJournalBookkeeping(recovery, summary);
      });
      if (clear) await clearOwnerRecovery(recovery);
      return summary;
    });

  const finalizeTerminalAgentState = async (
    scope: AgentThreadInstanceScope,
    ref: { agentId: string; resourceId: string; runId: string },
    state: NormalAgentRunState | undefined,
    expectedRecord: AgentRunRecord | undefined,
  ): Promise<void> => {
    const selected =
      state ?? (await selectedAgentState(scope, ref, { includeLegacy: true }));
    if (
      !selected ||
      selected.kind === 'initial' ||
      !isTerminalRunStatus(selected.summary.status)
    )
      throw new RunStartPendingError();
    const stored = await options
      .stateStorage()
      .get(ownerRecoveryKey(ref.runId));
    if (stored !== undefined) {
      if (selected.kind === 'legacy')
        throw new ExecutionFenceUnreadableError(
          'legacy run cleanup is unresolved',
        );
      const recovery = validateOwnerRecovery(
        scope.threadId,
        ownerRecoveryKey(ref.runId),
        stored,
      );
      await finalizeOwnerRecovery(scope, recovery, selected);
      return;
    }
    await withBindingLock(() =>
      withRecoveryLock(async () => {
        if (expectedRecord)
          await finalizeTerminalRecord(
            scope,
            ref.runId,
            expectedRecord,
            selected,
          );
        else if (selected.kind === 'legacy')
          await assertLegacyTerminalCurrent(scope, ref, selected, undefined);
        else {
          if ((await readRun(ref.runId)) !== undefined)
            throw new Error('agent run record changed');
          await scope.init.runtime.settleStartExecution(selected);
        }
      }),
    );
  };

  const withExecution = async <T>(
    execution: TrustedAgentExecution,
    operation: () => Promise<T>,
  ): Promise<T> => {
    if (executions.has(execution.runId)) {
      throw new AgentHostRequestError(
        409,
        `run '${execution.runId}' already has an active operation`,
      );
    }
    executions.set(execution.runId, execution);
    try {
      return await operation();
    } finally {
      if (executions.get(execution.runId) === execution)
        executions.delete(execution.runId);
    }
  };

  // Reconciling approvals is trusted platform work with no person behind it.
  // The bridge mints its own principal from this id, so the audit trail shows
  // an automated principal rather than a human operator.
  const systemPrincipalId = options.systemPrincipalId ?? 'flowsafe-system';
  // Deliberately NOT vouched. Its only consumer projects it to an ApprovalActor
  // for a role-gated READ, which grants nothing an automated principal does not
  // already have — so calling the trust assertion here would assert trust that
  // nothing consumes, and `trustAutomationPrincipal` has to stay greppable as
  // "this is where authority is conferred" to be worth anything.
  //
  // `purpose` is likewise inert here: principalActor drops it, and a successful
  // list() emits no audit event, so this string reaches nothing. It is not
  // shared with the bridge's RECONCILE_PURPOSE for that reason — there is no
  // provenance here to drift.
  const systemPrincipal = (): ExecutionPrincipal => ({
    kind: 'system',
    id: systemPrincipalId,
    purpose: 'approval-suspension-reconcile',
  });

  const currentApprovals = async (
    scope: ThreadScope,
    summary: RunSummary,
    principal: ExecutionPrincipal,
    agentId: string,
    resourceId: string,
  ): Promise<ApprovalRecord[]> => {
    if (summary.status !== 'suspended') return [];
    if (!summary.requestedBy) {
      throw new Error(
        'suspended agent run has no durable requester provenance',
      );
    }
    const service = options.approvalService(instanceScopeFor(scope));
    await reconcileApprovalsForSummary(
      service,
      await workflowIdFor(scope, agentId),
      summary,
      systemPrincipalId,
      {
        kind: 'agent-thread',
        agentId,
        threadId: scope.threadId,
        resourceId,
        principal,
      },
      summary.requestedBy,
    );
    const records = await service.list(
      {
        workflowId: await workflowIdFor(scope, agentId),
        runId: summary.runId,
      },
      principalActor(systemPrincipal()),
    );
    const keys = new Set(
      (summary.suspended ?? []).map((path) => path.join('.')),
    );
    return records.filter((runRecord) => {
      const key = runRecord.stepPath?.join('.');
      return (
        key !== undefined &&
        keys.has(key) &&
        runRecord.suspendedAt === summary.suspendedAt?.[key] &&
        runRecord.resumeCount === summary.resumeCount?.[key]
      );
    });
  };

  const envelopeFor = async (
    scope: ThreadScope,
    ref: {
      agentId: string;
      resourceId: string;
      runId: string;
    },
    principal: ExecutionPrincipal,
    summary: RunSummary,
  ): Promise<AgentRunEnvelope> => {
    const base: AgentRunEnvelope = {
      agentId: ref.agentId,
      threadId: scope.threadId,
      resourceId: ref.resourceId,
      runId: ref.runId,
      summary,
    };
    if (summary.status !== 'suspended') return base;
    let approvals: ApprovalRecord[] = [];
    try {
      approvals = await currentApprovals(
        scope,
        summary,
        principal,
        ref.agentId,
        ref.resourceId,
      );
    } catch (error) {
      console.error(
        JSON.stringify({
          type: 'agent-approval-filing-error',
          agentId: ref.agentId,
          runId: ref.runId,
          error: error instanceof Error ? error.message : String(error),
        }),
      );
    }
    return {
      ...base,
      ...(approvals[0] ? { approval: approvals[0] } : {}),
      approvals,
    };
  };

  const snapshotExecutionFor = async (
    scope: ThreadScope,
    ref: { agentId: string; resourceId: string; runId: string },
    knownState?: NormalAgentRunState | null,
  ) => {
    const state =
      knownState === undefined
        ? await selectedAgentState(scope, ref, { includeLegacy: true })
        : knownState;
    if (!state) throw new AgentHostRequestError(404, 'run not found');
    if (state.kind === 'initial') throw new RunStartPendingError();
    return {
      state,
      threaded: state.threaded,
      safeContext: sanitizeStoredAgentContext(state.snapshot.requestContext),
    };
  };

  const statusFor = async (
    scope: ThreadScope,
    ref: { agentId: string; resourceId: string; runId: string },
    knownState?: NormalAgentRunState | null,
  ): Promise<AgentRunEnvelope> => {
    const stored = await readRun(ref.runId);
    const selected =
      knownState === undefined
        ? await selectedAgentState(scope, ref, { includeLegacy: true })
        : knownState;
    if (!selected) throw new AgentHostRequestError(404, 'run not found');
    if (selected.kind === 'initial') throw new RunStartPendingError();
    const summary = selected.summary;
    const binding = await readBinding();
    const bindingMatches =
      binding?.agentId === ref.agentId && binding.resourceId === ref.resourceId;
    if (
      (selected.threaded && !bindingMatches) ||
      (!selected.threaded && binding)
    )
      throw new AgentHostRequestError(404, 'run not found');
    if (stored && stored.agentId !== ref.agentId)
      throw new AgentHostRequestError(404, 'run not found');
    if (summary.status === 'suspended' && !stored)
      throw new AgentHostRequestError(
        409,
        'suspended agent run has no recoverable execution principal',
      );
    if (isTerminalRunStatus(summary.status))
      await finalizeTerminalAgentState(scope, ref, selected, stored);
    return envelopeFor(
      scope,
      ref,
      stored?.principal ?? scope.principal,
      summary,
    );
  };

  const recoverOwner = async (
    scope: AgentThreadInstanceScope,
    key: string,
    value: unknown,
    ownFrame?: TrustedAgentExecution,
    ownFailure?: unknown,
  ): Promise<RunSummary | null> =>
    withBindingLock(() =>
      withRecoveryLock(async () => {
        const stored = validateOwnerRecovery(scope.threadId, key, value);
        if (stored.startReservation && !scope.init.runtime.startIdempotency)
          throw new ExecutionFenceUnreadableError(
            'run start recovery is unresolved',
          );
        await assertRecoveryCurrent(stored);
        const quiescent = (): boolean => {
          const active = executions.get(stored.runId);
          return (
            active === undefined ||
            (active === ownFrame && unwoundExecutions.has(active))
          );
        };
        if (
          !quiescent() ||
          scope.init.runtime
            .workflowIds()
            .some((workflowId) =>
              scope.init.runtime.isRunActive(workflowId, stored.runId),
            )
        )
          throw new RunStartPendingError();
        let recovered: RecoveredStart | null = null;
        if (stored.phase !== 'preparing') {
          const current = await runtimeFor(scope);
          const durable = current.agents.get(stored.agentId);
          if (
            !durable ||
            stored.execution.workflowId !== durable.getWorkflow().id
          )
            throw new Error(
              'stored agent owner recovery does not match the wrapper',
            );
          if (
            scope.init.runtime.isRunActive(
              stored.execution.workflowId,
              stored.runId,
            )
          )
            throw new RunStartPendingError();
          if (stored.phase === 'prepared') {
            recovered = await scope.init.runtime.recoverStartAttempt(
              stored.execution,
              {
                attemptToken: stored.token,
                isOwnerQuiescent: quiescent,
                startReservation: stored.startReservation,
                expectedTarget: {
                  kind: 'agent',
                  id: stored.agentId,
                  threadId: stored.threadId,
                  owner: principalOwner(stored.runRecord.principal),
                  threaded: stored.threaded,
                },
              },
            );
          } else {
            const selected = matchRecoveryState(
              stored,
              await selectedAgentState(scope, stored),
            );
            if (selected.kind === 'initial') throw new RunStartPendingError();
            if (isTerminalRunStatus(selected.summary.status))
              await scope.init.runtime.settleStartExecution(
                selected,
                stored.startReservation,
              );
            const cleanup = terminalCleanupFor(
              lifecycleFromRequestContext(selected.snapshot.requestContext),
            );
            recovered = cleanup
              ? {
                  kind: 'lifecycle',
                  transition: {
                    summary: selected.summary,
                    transitioned: false,
                    casMatched: true,
                    cleanup,
                  },
                }
              : { kind: 'ordinary', summary: selected.summary };
          }
        }
        if (recovered) {
          let summary =
            recovered.kind === 'ordinary'
              ? recovered.summary
              : recovered.transition.summary;
          let clear: boolean;
          if (recovered.kind === 'lifecycle') {
            summary = await finishLifecycle(
              scope,
              stored,
              recovered.transition,
            );
            clear = true;
          } else {
            await assertRecoveryCurrent(stored);
            await options.resourceAccess().settleReservation(stored.token, []);
            clear = await finalizeJournalBookkeeping(stored, summary);
          }
          if (clear) {
            await assertRecoveryCurrent(stored);
            await options.stateStorage().delete(key);
          }
          return summary;
        }
        await assertRecoveryCurrent(stored);
        const localZero = (): boolean =>
          stored.phase === 'prepared' &&
          ownFrame !== undefined &&
          executions.get(stored.runId) === ownFrame &&
          unwoundExecutions.has(ownFrame) &&
          isDefinitiveInitialAdmissionRefusal(ownFailure, stored.execution);
        if (stored.phase === 'prepared' && !localZero()) {
          await options.resourceAccess().settleReservation(stored.token, [
            { kind: 'run', resourceId: stored.runId },
            { kind: 'thread', resourceId: stored.threadId },
            { kind: 'resource', resourceId: stored.resourceId },
          ]);
          await ensureOwnerRecoveryAlarm(options.stateStorage());
          throw new ExecutionFenceUnreadableError(
            'run start recovery is unresolved',
          );
        }
        const storage = options.stateStorage();
        const binding = await readBinding(),
          runRecord = await readRun(stored.runId);
        await assertRecoveryCurrent(stored);
        const matches =
          binding?.agentId === stored.agentId &&
          binding.resourceId === stored.resourceId;
        if (runRecord && !sameRunRecord(runRecord, stored.runRecord))
          throw new Error('agent run record changed');
        if (runRecord) await deleteAgentRunRecord(storage, stored.runId);
        if (!stored.bindingPreexisting && matches)
          await deleteAgentThreadBinding(storage, {
            agentId: stored.agentId,
            resourceId: stored.resourceId,
          });
        const release: Array<{
          kind: 'run' | 'thread' | 'resource';
          resourceId: string;
        }> = [{ kind: 'run', resourceId: stored.runId }];
        if (!(stored.threaded && stored.bindingPreexisting && matches))
          release.push(
            { kind: 'resource', resourceId: stored.resourceId },
            { kind: 'thread', resourceId: stored.threadId },
          );
        await options.resourceAccess().settleReservation(stored.token, release);
        await assertRecoveryCurrent(stored);
        if (stored.phase === 'prepared' && !localZero())
          throw new ExecutionFenceUnreadableError(
            'run start recovery is unresolved',
          );
        await storage.delete(key);
        return null;
      }),
    );

  const host: ThreadAgentHost = {
    requestContextForRun: (base) => async (workflowId, runId, leg) => {
      const values = base ? await base(workflowId, runId, leg) : undefined;
      const execution = executions.get(runId);
      return execution &&
        workflowId === runtime?.agents.get(execution.agentId)?.getWorkflow().id
        ? {
            ...execution.safeContext,
            ...values,
            ...deriveTrustedAgentContext(execution, {}),
          }
        : values;
    },
    serializeDispatch: withDispatchLock,
    blockingRun: (scope) =>
      withBindingLock(() => findBlockingRun(instanceScopeFor(scope))),
    scheduleDispatchStatus: async (scope, input) => {
      const ref = runRef(scope, {
        ...input,
        threadId: scope.threadId,
      });
      const key = ownerRecoveryKey(ref.runId);
      const recovery = await options.stateStorage().get<unknown>(key);
      if (recovery !== undefined) {
        await recoverOwner(scope, key, recovery);
      }
      const selected = await selectedAgentState(scope, ref, {
        includeLegacy: true,
      });
      if (!selected) return undefined;
      if (selected.kind === 'initial') throw new RunStartPendingError();
      return (await statusFor(scope, ref, selected)).summary;
    },
    recoverOwnership: (inputScope) => {
      const scope = instanceScopeFor(inputScope);
      return withDispatchLock(async () => {
        const storage = options.stateStorage();
        try {
          const pending = await storage.list<AgentOwnerRecovery>({
            prefix: AGENT_OWNER_RECOVERY_PREFIX,
          });
          for (const [key, stored] of pending) {
            await recoverOwner(scope, key, stored);
          }
          await withRecoveryLock(async () => {
            const remaining = await storage.list({
              prefix: AGENT_OWNER_RECOVERY_PREFIX,
            });
            if (remaining.size > 0) {
              await ensureOwnerRecoveryAlarm(storage);
            } else {
              await storage.deleteAlarm();
            }
          });
        } catch (error) {
          await withRecoveryLock(() => ensureOwnerRecoveryAlarm(storage));
          throw error;
        }
      });
    },
    start: async (sourceScope, sourceInput) => {
      const principal = assertExecutionPrincipal(
        sourceScope.principal,
        'thread start principal',
      );
      const mutationEpoch = normalizeMutationEpoch(sourceScope.mutationEpoch);
      const { threadId, deploymentTag, init } = sourceScope;
      const scope: ThreadScope = Object.freeze({
        principal,
        mutationEpoch,
        threadId,
        deploymentTag,
        init,
      });
      const {
        agentId,
        threadId: inputThreadId,
        resourceId,
        runId,
        prompt,
        messages: inputMessages,
        entryPath: inputEntryPath,
        threaded: inputThreaded,
        scheduleId,
        dispatchId,
        scheduleDispatchLease,
        safeContext: inputSafeContext,
        providerOptions: inputProviderOptions,
        idempotencyKey,
        startReservation: suppliedReservation,
      } = sourceInput;
      const startReservation =
        suppliedReservation === undefined
          ? undefined
          : captureReservation(suppliedReservation, 'started');
      if (
        startReservation &&
        (startReservation.key !== idempotencyKey ||
          startReservation.runId !== runId ||
          startReservation.threadId !== threadId ||
          startReservation.targetKind !== 'agent' ||
          startReservation.targetId !== agentId ||
          startReservation.owner.kind !== principal.kind ||
          startReservation.owner.id !== principal.id ||
          !init.runtime.startIdempotency)
      )
        throw new AgentHostRequestError(
          400,
          'start reservation does not match the trusted start',
        );
      const input: ThreadAgentStartInput = {
        agentId,
        threadId: inputThreadId,
        resourceId,
        runId,
        prompt,
        messages: inputMessages,
        entryPath: inputEntryPath,
        threaded: inputThreaded,
        scheduleId,
        dispatchId,
        scheduleDispatchLease,
        safeContext: inputSafeContext,
        providerOptions: inputProviderOptions,
        idempotencyKey,
      };
      const ref = Object.freeze(
        runRef(scope, input as unknown as Record<string, unknown>),
      );
      const entry = entryPath(input.entryPath);
      const threaded = input.threaded !== false;
      const source = await resolveStartSource(
        scope,
        ref,
        entry,
        threaded,
        input.scheduleId,
        input.dispatchId,
      );
      const rawOwner = source.owner;
      const owner = canonicalResourceOwner({
        kind: rawOwner.kind,
        id: rawOwner.id,
      });
      const startIdentity: AgentStartAuthority['startIdentity'] = Object.freeze(
        {
          owner: principalOwner(principal),
          target: Object.freeze({
            kind: 'agent',
            id: ref.agentId,
            threadId: ref.threadId,
          }),
        },
      );
      const hasPrompt =
        source.target === undefined && input.prompt !== undefined;
      const hasMessages =
        source.target === undefined && input.messages !== undefined;
      if (
        (source.target === undefined && hasPrompt === hasMessages) ||
        (hasPrompt &&
          (typeof input.prompt !== 'string' ||
            input.prompt.trim() === '' ||
            input.prompt.length > 10_000))
      ) {
        throw new AgentHostRequestError(
          400,
          'exactly one valid prompt or messages input is required',
        );
      }
      const messages = source.target
        ? threaded && input.messages !== undefined
          ? input.messages
          : source.target.prompt
        : (input.messages ?? input.prompt);
      if (messages === undefined) {
        throw new AgentHostRequestError(400, 'agent input is required');
      }
      const resolvedProviderOptions = source.target
        ? source.target.providerOptions
        : input.providerOptions;
      const { current, module, principalPermissions } = await authorize(
        scope,
        ref.agentId,
        entry,
        scope.principal,
      );
      if (ref.resourceId !== resourceIdFromKey(scope.threadId)) {
        throw new AgentHostRequestError(404, 'run not found');
      }
      const execution: TrustedAgentExecution = {
        agentId: ref.agentId,
        deploymentTag: scope.deploymentTag,
        principal: scope.principal,
        threadId: scope.threadId,
        resourceId: ref.resourceId,
        runId: ref.runId,
        entryPath: entry,
        principalPermissions,
        safeContext: safeContext(
          source.target
            ? scheduleAgentContext(source.target)
            : input.safeContext,
        ),
      };
      const durable = current.agents.get(module.meta.id);
      if (!durable) throw new Error('guarded agent was not registered');
      const recoveryKey = ownerRecoveryKey(ref.runId);
      const pending = await options.stateStorage().get<unknown>(recoveryKey);
      if (pending !== undefined) {
        await recoverOwner(scope, recoveryKey, pending);
      }
      return withExecution(execution, async () => {
        const existingRecord = await readRun(ref.runId);
        const existingSummary = await scope.init.runtime.status(
          await workflowIdFor(scope, ref.agentId),
          ref.runId,
        );
        if (existingRecord || existingSummary) {
          throw new AgentHostRequestError(
            409,
            `run '${ref.runId}' already exists`,
          );
        }
        const claims = [
          { kind: 'thread' as const, resourceId: scope.threadId },
          { kind: 'resource' as const, resourceId: ref.resourceId },
          { kind: 'run' as const, resourceId: ref.runId },
        ];
        const stored: AgentRunRecord = {
          version: 2,
          agentId: ref.agentId,
          principal: scope.principal,
          originEntryPath: entry,
        };
        let recovery: AgentOwnerRecovery = await withBindingLock(
          async (): Promise<AgentOwnerRecovery> => {
            const blocking = await findBlockingRun(scope);
            if (blocking && blocking.runId !== ref.runId)
              throw new AgentHostRequestError(
                409,
                `thread is blocked by run '${blocking.runId}'`,
              );
            const existing = await readBinding();
            if (!threaded && existing) {
              throw new AgentHostRequestError(
                409,
                'unthreaded starts require an unbound object',
              );
            }
            if (threaded) {
              const bindingMatches =
                existing?.agentId === ref.agentId &&
                existing.resourceId === ref.resourceId;
              if (entry === 'schedule.fire') {
                if (!bindingMatches) {
                  throw new AgentHostRequestError(404, 'run not found');
                }
              } else if (existing && !bindingMatches) {
                throw new AgentHostRequestError(
                  409,
                  'thread is bound to another agent',
                );
              }
            }
            const recoveryState: AgentOwnerRecovery = {
              version: 2,
              phase: 'preparing',
              runRecord: stored,
              ...(startReservation ? { startReservation } : {}),
              agentId: ref.agentId,
              threadId: scope.threadId,
              resourceId: ref.resourceId,
              runId: ref.runId,
              owner,
              token: crypto.randomUUID(),
              threaded,
              bindingPreexisting: existing !== undefined,
            };
            if (
              !threaded &&
              (
                await Promise.all(
                  claims.map((claim) =>
                    options
                      .resourceAccess()
                      .owner(claim.kind, claim.resourceId),
                  ),
                )
              ).some((registered) => registered !== undefined)
            ) {
              throw new AgentHostRequestError(404, 'run not found');
            }
            await armOwnerRecovery(recoveryState);
            if (
              !(await options
                .resourceAccess()
                .reserveAll(claims, owner, recoveryState.token))
            ) {
              await options
                .resourceAccess()
                .settleReservation(recoveryState.token, claims);
              await clearOwnerRecovery(recoveryState);
              throw new AgentHostRequestError(404, 'run not found');
            }
            if (threaded && !existing) {
              await bindAgentThread(options.stateStorage(), {
                version: 1,
                agentId: ref.agentId,
                resourceId: ref.resourceId,
              });
            }
            await writeAgentRunRecord(
              options.stateStorage(),
              ref.runId,
              stored,
            );
            return recoveryState;
          },
        );
        // From here to the finally below, this object IS the run's execution.
        // Registered BEFORE the stream so the window a replaying start asks
        // about — the one before core has persisted anything — is covered too.
        startsInFlight.add(ref.runId);
        try {
          const streamOptions = {
            runId: ref.runId,
            requestContext: createTrustedAgentRequestContext(execution),
            ...(threaded
              ? {
                  memory: {
                    thread: scope.threadId,
                    resource: ref.resourceId,
                  },
                }
              : {}),
            maxSteps: module.agent.maxSteps,
            disableBackgroundTasks: true,
            ...(resolvedProviderOptions !== undefined
              ? { providerOptions: providerOptions(resolvedProviderOptions) }
              : {}),
          };
          const scheduleDispatch =
            source.target &&
            input.scheduleId &&
            input.dispatchId &&
            input.scheduleDispatchLease === 'executing'
              ? { scheduleId: input.scheduleId, dispatchId: input.dispatchId }
              : undefined;
          try {
            await durable.streamUntilPersisted(
              messages,
              streamOptions,
              principal.id,
              principal.kind,
              recovery.token,
              scheduleDispatch,
              idempotencyKey,
              {
                ...(mutationEpoch === undefined ? {} : { mutationEpoch }),
                startIdentity,
                agentStart: { threaded },
                ...(startReservation ? { startReservation } : {}),
                onPreparedStartIdentity: async (identity) => {
                  recovery = await prepareOwnerRecovery(
                    scope,
                    recovery,
                    identity,
                  );
                },
                runOwnerGuard: { owner, reservationToken: recovery.token },
              },
            );
          } finally {
            unwoundExecutions.add(execution);
          }
          const selected = matchRecoveryState(
            recovery,
            await selectedAgentState(scope, ref),
          );
          if (selected.kind === 'initial') throw new RunStartPendingError();
          const summary = await finalizeOwnerRecovery(
            scope,
            recovery,
            selected,
            execution,
          );
          const result = await envelopeFor(
            scope,
            ref,
            scope.principal,
            summary,
          );
          return result;
        } catch (error) {
          unwoundExecutions.add(execution);
          try {
            const storedRecovery = await options
              .stateStorage()
              .get(recoveryKey);
            if (storedRecovery !== undefined) {
              const latest = validateOwnerRecovery(
                scope.threadId,
                recoveryKey,
                storedRecovery,
              );
              if (!sameOwnerRecovery(latest, recovery))
                throw new Error('agent owner recovery changed');
              const summary = await recoverOwner(
                scope,
                recoveryKey,
                latest,
                execution,
                error,
              );
              if (summary)
                return envelopeFor(scope, ref, scope.principal, summary);
            }
          } catch (recoveryError) {
            console.error('agent owner recovery failed', recoveryError);
            await withRecoveryLock(() =>
              ensureOwnerRecoveryAlarm(options.stateStorage()),
            );
          }
          throw error;
        } finally {
          // Unconditional: an entry left behind would answer every later probe
          // "live" for the lifetime of the isolate, turning a crashed start's
          // honest UNRESOLVABLE into an endless PENDING.
          startsInFlight.delete(ref.runId);
        }
      });
    },
    resolveBoundAgent: async (scope, input) => {
      const binding = await readBinding();
      if (!binding) throw new AgentHostRequestError(404, 'agent not found');
      if (binding.resourceId !== resourceIdFromKey(scope.threadId)) {
        throw new AgentHostRequestError(404, 'agent not found');
      }
      if (input.agentId && input.agentId !== binding.agentId) {
        throw new AgentHostRequestError(404, 'agent not found');
      }
      const { current } = await authorize(
        scope,
        binding.agentId,
        entryPath(input.entryPath),
        scope.principal,
      );
      const durableAgent = current.agents.get(binding.agentId);
      if (!durableAgent) throw new Error('guarded agent was not registered');
      return {
        agentId: binding.agentId,
        resourceId: binding.resourceId,
        durableAgent,
      };
    },
    route: async (request, scope) => {
      let preflightedTermination = false;
      const preflightUrl = new URL(request.url);
      const preflightSuffix = preflightUrl.pathname.startsWith(
        AGENT_HOST_ROUTE_PREFIX,
      )
        ? preflightUrl.pathname.slice(AGENT_HOST_ROUTE_PREFIX.length)
        : '';
      const preflightSegments = preflightSuffix.split('/').filter(Boolean);
      // The start holds the dispatch lock while its liveness probe must remain responsive.
      if (
        request.method === 'GET' &&
        preflightSegments.length === 4 &&
        preflightSegments[0] === 'runs' &&
        preflightSegments[3] === 'start-liveness'
      ) {
        instanceScopeFor(scope);
        const agentId = decode(preflightSegments[1]);
        const runId = decode(preflightSegments[2]);
        return json({
          live:
            agentId !== undefined &&
            runId !== undefined &&
            (startsInFlight.has(runId) ||
              executions.has(runId) ||
              (runtime?.agents.get(agentId)?.isRunLive(runId) ??
                globalRunRegistry.has(runId))),
        });
      }
      if (
        request.method === 'GET' &&
        preflightSegments.length === 3 &&
        preflightSegments[0] === 'runs' &&
        preflightUrl.searchParams.get('replay') === '1'
      ) {
        const ref = runRef(scope, {
          agentId: decode(preflightSegments[1]),
          runId: decode(preflightSegments[2]),
          threadId: scope.threadId,
          resourceId: preflightUrl.searchParams.get('resourceId'),
        });
        const state = await selectedAgentState(scope, ref);
        if (
          state &&
          (state.execution.owner.kind !== scope.principal.kind ||
            state.execution.owner.id !== scope.principal.id)
        )
          throw new AgentHostRequestError(404, 'run not found');
        if (state?.kind === 'initial')
          return json({ kind: 'initial', execution: state.execution });
      }
      if (
        request.method === 'POST' &&
        preflightSegments.length === 4 &&
        preflightSegments[0] === 'runs' &&
        preflightSegments[3] === 'terminate'
      ) {
        const agentId = decode(preflightSegments[1]);
        const runId = decode(preflightSegments[2]);
        const resourceId = preflightUrl.searchParams.get('resourceId');
        if (!agentId || !runId || !resourceId) {
          throw new AgentHostRequestError(404, 'run not found');
        }
        const ref = runRef(scope, {
          agentId,
          threadId: scope.threadId,
          resourceId,
          runId,
        });
        const storedRun = await readRun(ref.runId);
        if (storedRun && storedRun.agentId !== ref.agentId) {
          throw new AgentHostRequestError(404, 'run not found');
        }
        await snapshotExecutionFor(
          scope,
          ref,
          preflightUrl.searchParams.get('replay') === '1'
            ? undefined
            : await publicAgentState(scope, ref),
        );
        const owner = await options.resourceAccess().owner('run', ref.runId);
        if (preflightUrl.searchParams.get('replay') !== '1') {
          await scope.init.runtime.cancelActiveExecution(
            await workflowIdFor(scope, ref.agentId),
            ref.runId,
            'cancelled',
            [scope.principal, owner ?? scope.principal],
          );
          preflightedTermination = true;
        }
      }
      return withDispatchLock(async () => {
        const url = new URL(request.url);
        if (!url.pathname.startsWith(AGENT_HOST_ROUTE_PREFIX)) return null;

        if (
          request.method === 'GET' &&
          url.pathname === `${AGENT_HOST_ROUTE_PREFIX}/binding`
        ) {
          const resourceId = url.searchParams.get('resourceId');
          const agentId = url.searchParams.get('agentId');
          const binding = await readBinding();
          if (
            !resourceId ||
            resourceId !== resourceIdFromKey(scope.threadId) ||
            binding?.resourceId !== resourceId ||
            (agentId !== null && binding.agentId !== agentId)
          ) {
            throw new AgentHostRequestError(404, 'agent not found');
          }
          return json({ bound: true });
        }

        if (
          request.method === 'POST' &&
          url.pathname === `${AGENT_HOST_ROUTE_PREFIX}/start`
        ) {
          const body = await objectBody(request);
          if (
            'resourceOwner' in body ||
            'requestedBy' in body ||
            [
              'mutationEpoch',
              'startIdentity',
              'agentStart',
              'execution',
              'tablePrefix',
              'startToken',
              'attemptToken',
              'runOwnerGuard',
              'onPreparedStartIdentity',
            ].some((key) => Object.hasOwn(body, key))
          ) {
            throw new AgentHostRequestError(
              400,
              'start owner and requester are derived from trusted provenance',
            );
          }
          const startReservation =
            body.startReservation === undefined
              ? undefined
              : captureReservation(
                  body.startReservation as StartReservationReading,
                  'started',
                );
          const ref = runRef(scope, body);
          const requestedEntry = entryPath(body.entryPath);
          if (
            requestedEntry !== 'schedule.fire' &&
            typeof body.prompt !== 'string'
          ) {
            throw new AgentHostRequestError(400, 'prompt is required');
          }
          return json(
            await host.start(scope, {
              ...ref,
              startReservation,
              ...(typeof body.prompt === 'string'
                ? { prompt: body.prompt }
                : {}),
              entryPath: requestedEntry,
              threaded: body.threaded !== false,
              safeContext: safeContext(body.safeContext),
              providerOptions: providerOptions(body.providerOptions),
              // Validated, not trusted: this body is JSON, and the same string
              // reaches the execution fence's proof-only comparison.
              ...(body.idempotencyKey === undefined
                ? {}
                : {
                    idempotencyKey: isPathSafeId(body.idempotencyKey)
                      ? body.idempotencyKey
                      : (() => {
                          throw new AgentHostRequestError(
                            400,
                            'idempotencyKey must be a URL-path-safe identifier',
                          );
                        })(),
                  }),
              ...(body.scheduleId !== undefined
                ? {
                    scheduleId: isPathSafeId(body.scheduleId)
                      ? body.scheduleId
                      : (() => {
                          throw new AgentHostRequestError(404, 'run not found');
                        })(),
                  }
                : {}),
              ...(body.dispatchId !== undefined
                ? {
                    dispatchId: isPathSafeId(body.dispatchId)
                      ? body.dispatchId
                      : (() => {
                          throw new AgentHostRequestError(404, 'run not found');
                        })(),
                  }
                : {}),
            }),
          );
        }

        if (
          request.method === 'POST' &&
          url.pathname === `${AGENT_HOST_ROUTE_PREFIX}/resume`
        ) {
          const body = await objectBody(request);
          const ref = runRef(scope, body);
          const snapshotExecution = await snapshotExecutionFor(scope, ref);
          await statusFor(scope, ref, snapshotExecution.state);
          const stored = await readRun(ref.runId);
          if (
            !stored ||
            stored.agentId !== ref.agentId ||
            !samePrincipal(stored.principal, scope.principal)
          ) {
            throw new AgentHostRequestError(404, 'run not found');
          }
          const { current, module, principalPermissions } = await authorize(
            scope,
            ref.agentId,
            entryPath(body.entryPath),
            stored.principal,
          );
          const durable = current.agents.get(module.meta.id);
          if (!durable) throw new Error('guarded agent was not registered');
          const requesterId = requestedBy(body.requestedBy);
          const resumeFields =
            'resumeData' in body
              ? { resumeData: resumeData(body.resumeData) }
              : {};
          const step =
            typeof body.step === 'string' ||
            (Array.isArray(body.step) &&
              body.step.every((part) => typeof part === 'string'))
              ? body.step
              : undefined;
          const execution: TrustedAgentExecution = {
            agentId: ref.agentId,
            deploymentTag: scope.deploymentTag,
            principal: stored.principal,
            threadId: scope.threadId,
            resourceId: ref.resourceId,
            runId: ref.runId,
            entryPath: 'approval.resume',
            // The re-derived resolution, not the start leg's: the resume merges
            // over the persisted context, so this leg's projection retires a
            // stale one minted under an older policy snapshot.
            principalPermissions,
            safeContext: snapshotExecution.safeContext,
          };
          const summary = await withExecution(execution, async () => {
            return durable.resumeViaRuntime({
              runId: ref.runId,
              requestedBy: requesterId,
              ...(step !== undefined ? { step } : {}),
              ...resumeFields,
              ...(snapshotExecution.threaded
                ? {
                    memory: {
                      thread: scope.threadId,
                      resource: ref.resourceId,
                    },
                  }
                : {}),
            });
          });
          const result = await envelopeFor(
            scope,
            ref,
            stored.principal,
            summary,
          );
          if (isTerminalRunStatus(summary.status))
            await finalizeTerminalAgentState(scope, ref, undefined, stored);
          return json(result);
        }

        const suffix = url.pathname.slice(AGENT_HOST_ROUTE_PREFIX.length);
        const segments = suffix.split('/').filter(Boolean);
        if (segments[0] !== 'runs') return json({ error: 'not found' }, 404);
        const agentId = decode(segments[1]);
        const runId = decode(segments[2]);
        const resourceId = url.searchParams.get('resourceId');
        if (!agentId || !runId || !resourceId) {
          throw new AgentHostRequestError(404, 'run not found');
        }
        const ref = runRef(scope, {
          agentId,
          threadId: scope.threadId,
          resourceId,
          runId,
        });

        if (segments.length === 3 && request.method === 'GET') {
          if (url.searchParams.get('replay') === '1') {
            const selected = await selectedAgentState(scope, ref);
            if (!selected) return json({ error: 'run not found' }, 404);
            if (
              selected.execution.owner.kind !== scope.principal.kind ||
              selected.execution.owner.id !== scope.principal.id
            )
              throw new AgentHostRequestError(404, 'run not found');
            if (selected.kind === 'initial')
              return json({ kind: 'initial', execution: selected.execution });
            const stored = await readRun(ref.runId);
            const value = await envelopeFor(
              scope,
              ref,
              stored?.principal ?? scope.principal,
              selected.summary,
            );
            return json({
              kind: 'result',
              execution: selected.execution,
              value: publicAgentRunEnvelope(value, selected.execution, {
                ...ref,
                threadId: scope.threadId,
              }),
            });
          }
          if (url.searchParams.get('dispatch') === '1') {
            const key = ownerRecoveryKey(ref.runId);
            const pending = await options.stateStorage().get<unknown>(key);
            if (pending !== undefined) {
              await recoverOwner(scope, key, pending);
            }
            return json(await statusFor(scope, ref));
          }
          return json(
            await statusFor(scope, ref, await publicAgentState(scope, ref)),
          );
        }

        if (
          segments.length === 4 &&
          segments[3] === 'terminate' &&
          request.method === 'POST'
        ) {
          const scopedRuntime = scope.init.runtime;
          const replayOnly = url.searchParams.get('replay') === '1';
          const storedRun = await readRun(ref.runId);
          if (storedRun && storedRun.agentId !== ref.agentId) {
            throw new AgentHostRequestError(404, 'run not found');
          }
          await snapshotExecutionFor(
            scope,
            ref,
            replayOnly ? undefined : await publicAgentState(scope, ref),
          );
          const preflightOwner = await options
            .resourceAccess()
            .owner('run', ref.runId);
          if (!replayOnly && !preflightedTermination) {
            await scopedRuntime.cancelActiveExecution(
              await workflowIdFor(scope, ref.agentId),
              ref.runId,
              'cancelled',
              [scope.principal, preflightOwner ?? scope.principal],
            );
          }
          const owner = await options.resourceAccess().owner('run', ref.runId);
          if (replayOnly) {
            const existing = await scopedRuntime.status(
              await workflowIdFor(scope, ref.agentId),
              ref.runId,
            );
            if (
              existing?.status !== 'cancelled' &&
              existing?.status !== 'timed_out'
            ) {
              throw new AgentHostRequestError(404, 'run not found');
            }
          }
          const transition = await scopedRuntime.terminateAsPrincipal(
            await workflowIdFor(scope, ref.agentId),
            ref.runId,
            scope.principal,
            owner ?? scope.principal,
          );
          const selected = await selectedAgentState(scope, ref, {
            includeLegacy: true,
          });
          if (!selected || selected.kind === 'initial')
            throw new RunStartPendingError();
          const journal = await options
            .stateStorage()
            .get(ownerRecoveryKey(ref.runId));
          if (journal !== undefined) {
            if (selected.kind === 'legacy')
              throw new ExecutionFenceUnreadableError(
                'legacy run cleanup is unresolved',
              );
            const recovery = validateOwnerRecovery(
              scope.threadId,
              ownerRecoveryKey(ref.runId),
              journal,
            );
            const summary = await finalizeOwnerRecovery(
              scope,
              recovery,
              selected,
            );
            return json(
              await envelopeFor(
                scope,
                ref,
                storedRun?.principal ?? scope.principal,
                summary,
              ),
            );
          }
          const legacy = selected.kind === 'legacy' ? selected : undefined;
          const workflowId =
            selected.kind === 'legacy'
              ? selected.address.workflowId
              : selected.execution.workflowId;
          const cleanup = legacy
            ? terminalCleanupFor(
                lifecycleFromRequestContext(legacy.snapshot.requestContext),
              )
            : transition.cleanup;
          if (
            !cleanup ||
            (legacy &&
              (legacy.summary.status !== transition.summary.status ||
                cleanup.revision !== transition.cleanup.revision ||
                cleanup.status !== transition.cleanup.status ||
                cleanup.scheduleDispatch?.scheduleId !==
                  transition.cleanup.scheduleDispatch?.scheduleId ||
                cleanup.scheduleDispatch?.dispatchId !==
                  transition.cleanup.scheduleDispatch?.dispatchId ||
                (transition.cleanup.cleanupCompleted &&
                  !cleanup.cleanupCompleted)))
          )
            throw new ExecutionFenceUnreadableError(
              'legacy run cleanup is unresolved',
            );
          const finish = async (): Promise<Response> => {
            const guard = legacy
              ? () => assertLegacyTerminalCurrent(scope, ref, legacy, storedRun)
              : undefined;
            if (guard) await guard();
            if (selected.kind !== 'legacy')
              await scopedRuntime.settleStartExecution(selected);
            let summary = legacy?.summary ?? transition.summary;
            if (!cleanup.cleanupCompleted) {
              if (guard) await guard();
              await abandonApprovalsForRun(
                options.approvalService(scope),
                workflowId,
                ref.runId,
                cleanup.status,
                options.systemPrincipalId ?? 'flowsafe-system',
              );
              const dispatch = cleanup.scheduleDispatch;
              if (dispatch) {
                if (!options.discardScheduleDispatch) {
                  throw new Error(
                    'scheduled agent termination requires a dispatch-discard hook',
                  );
                }
                if (guard) await guard();
                await options.discardScheduleDispatch(
                  dispatch.scheduleId,
                  dispatch.dispatchId,
                  ref.runId,
                );
              }
              if (guard) await guard();
              const released = await options
                .resourceAccess()
                .release('run', ref.runId, owner ?? scope.principal);
              if (!released) {
                const current = await options
                  .resourceAccess()
                  .owner('run', ref.runId);
                if (current) {
                  throw new Error(
                    `run '${ref.runId}' ownership could not be released`,
                  );
                }
              }
              if (guard) await guard();
              summary = await scopedRuntime.completeTerminalCleanup(
                workflowId,
                ref.runId,
                cleanup.revision,
              );
            }
            if (legacy) {
              if (guard) await guard();
              if (storedRun)
                await finalizeTerminalRecord(
                  scope,
                  ref.runId,
                  storedRun,
                  legacy,
                );
            } else {
              await finalizeTerminalAgentState(scope, ref, selected, storedRun);
            }
            return json(
              await envelopeFor(
                scope,
                ref,
                storedRun?.principal ?? scope.principal,
                summary,
              ),
            );
          };
          return legacy
            ? withBindingLock(() => withRecoveryLock(finish))
            : finish();
        }

        if (
          segments.length === 4 &&
          segments[3] === 'stream' &&
          request.method === 'GET'
        ) {
          const run = await statusFor(
            scope,
            ref,
            await publicAgentState(scope, ref),
          );
          const offset = Number(url.searchParams.get('offset') ?? '0');
          if (!Number.isSafeInteger(offset) || offset < 0) {
            throw new AgentHostRequestError(400, 'invalid stream offset');
          }
          const current = await runtimeFor(scope);
          const durable = current.agents.get(ref.agentId);
          if (!durable) throw new AgentHostRequestError(404, 'agent not found');
          const live = durable.runRegistry.has(ref.runId);
          let historyLength = 0;
          try {
            const history = await durable.pubsub.getHistory(
              AGENT_STREAM_TOPIC(ref.runId),
            );
            historyLength = Array.isArray(history) ? history.length : 0;
          } catch {
            // A live registry can still serve future events in this isolate.
          }
          if (!live && historyLength === 0) {
            throw new AgentHostRequestError(
              409,
              'stream replay is unavailable; inspect the authoritative status endpoint',
            );
          }
          if (isTerminalRunStatus(run.summary.status)) {
            if (historyLength === 0) {
              throw new AgentHostRequestError(
                409,
                'stream replay is unavailable; inspect the authoritative status endpoint',
              );
            }
            if (offset >= historyLength) {
              return ndjson(
                new ReadableStream<unknown>({
                  start(controller) {
                    controller.close();
                  },
                }),
                offset,
              );
            }
          }
          const observed = await durable.observe(ref.runId, { offset });
          return ndjson(observed.fullStream, offset);
        }

        const known =
          segments.length === 3 ||
          (segments.length === 4 &&
            (segments[3] === 'stream' || segments[3] === 'terminate'));
        return known
          ? json({ error: 'method not allowed' }, 405)
          : json({ error: 'not found' }, 404);
      });
    },
  };
  return host;
}
