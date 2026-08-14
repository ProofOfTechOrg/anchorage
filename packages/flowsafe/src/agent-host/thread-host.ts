// SPDX-License-Identifier: Apache-2.0

import type { Agent } from '@mastra/core/agent';
import { AGENT_STREAM_TOPIC } from '@mastra/core/agent/durable';
import { Mastra } from '@mastra/core/mastra';
import type { MastraCompositeStore } from '@mastra/core/storage';
import { isPrincipalPermissions } from '@proofoftech/breakwater/rbac';

import {
  AGENT_ENTRY_PATHS,
  AGENT_RUN_STORAGE_KEY_PREFIX,
  type AgentEntryPath,
  type AgentRunRecord,
  type AgentThreadBinding,
  bindAgentThread,
  createFlowsafeDurableAgent,
  DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
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
  isExecutionPrincipalId,
} from '../approval-api/principal.js';
import {
  DoStatusError,
  isPathSafeId,
  type RequestContextProvider,
  type RunSummary,
  resolveScheduleStartOwner,
  resourceIdFromKey,
  type ScheduleSourceAgentTarget,
  type ScheduleSourceStore,
  type ThreadScope,
} from '../do-runner/index.js';
import { mastraRegistryEntries } from '../do-runner/mastra-registry.js';
import {
  abandonApprovalsForRun,
  reconcileApprovalsForSummary,
} from '../host-kit/index.js';
import { createAgentModuleCatalog } from './catalog.js';
import { AGENT_HOST_ROUTE_PREFIX } from './thread-topology.js';
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
  recoverOwnership(
    runtime: ThreadScope['init']['runtime'],
    threadId: string,
  ): Promise<void>;
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

const TERMINAL_RUN_STATUSES: readonly RunSummary['status'][] = [
  'success',
  'failed',
  'tripwire',
  'canceled',
  'bailed',
  'skipped',
  'cancelled',
  'timed_out',
];

const AGENT_OWNER_RECOVERY_PREFIX = 'flowsafe:agent-owner-recovery:v1:';
const AGENT_OWNER_RECOVERY_DELAY_MS = 60_000;

interface AgentOwnerRecovery {
  version: 1;
  agentId: string;
  threadId: string;
  resourceId: string;
  runId: string;
  owner: ResourceOwner;
  token: string;
  threaded: boolean;
  bindingPreexisting: boolean;
}

function isTerminalRunStatus(status: RunSummary['status']): boolean {
  return TERMINAL_RUN_STATUSES.includes(status);
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

  const instanceScopeFor = (scope: ThreadScope): AgentThreadInstanceScope => {
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
    scope: ThreadScope,
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

  const runtimeFor = async (scope: ThreadScope) => {
    if (runtime) {
      if (runtime.scopeRuntime !== scope.init.runtime) {
        throw new Error(
          'thread agent host cannot be shared across DO instances',
        );
      }
      return runtime;
    }
    const catalog = await catalogFor(scope);
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
    scopeRuntime: ThreadScope['init']['runtime'],
  ): Promise<BlockingAgentRun | undefined> => {
    const storage = options.stateStorage();
    const records = await storage.list<unknown>({
      prefix: AGENT_RUN_STORAGE_KEY_PREFIX,
    });
    for (const key of records.keys()) {
      const runId = key.slice(AGENT_RUN_STORAGE_KEY_PREFIX.length);
      const record = await readRun(runId);
      if (!record) continue;
      const summary = await scopeRuntime.status(
        DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
        runId,
      );
      if (
        executions.has(runId) ||
        (summary !== null && !isTerminalRunStatus(summary.status))
      ) {
        return { runId, principal: record.principal };
      }
      const recovery = await storage.get<AgentOwnerRecovery>(
        AGENT_OWNER_RECOVERY_PREFIX + runId,
      );
      if (recovery) return { runId, principal: record.principal };
      await deleteAgentRunRecord(storage, runId);
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
    const [threadOwner, resourceOwner] = await Promise.all([
      ownership.owner('thread', ref.threadId),
      ownership.owner('resource', ref.resourceId),
    ]);
    if (
      !threadOwner ||
      !resourceOwner ||
      threadOwner.kind !== resourceOwner.kind ||
      threadOwner.id !== resourceOwner.id
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
      const current = await storage.get<AgentOwnerRecovery>(key);
      if (current?.token === recovery.token) await storage.delete(key);
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

  const finalizeOwnerRecovery = (
    recovery: AgentOwnerRecovery,
    summary: RunSummary,
  ): Promise<void> =>
    withRecoveryLock(async () => {
      const storage = options.stateStorage();
      await options.resourceAccess().settleReservation(recovery.token, []);
      if (!recovery.threaded && !isTerminalRunStatus(summary.status)) {
        await ensureOwnerRecoveryAlarm(storage);
        return;
      }
      if (!recovery.threaded) {
        await releaseEphemeralOwnerClaims(recovery);
      }
      const key = ownerRecoveryKey(recovery.runId);
      const current = await storage.get<AgentOwnerRecovery>(key);
      if (current?.token === recovery.token) await storage.delete(key);
    });

  const finalizeOwnerRecoveryBestEffort = async (
    recovery: AgentOwnerRecovery,
    summary: RunSummary,
  ): Promise<void> => {
    try {
      await finalizeOwnerRecovery(recovery, summary);
    } catch (error) {
      console.error('agent owner recovery cleanup failed', error);
      try {
        await withRecoveryLock(() =>
          ensureOwnerRecoveryAlarm(options.stateStorage()),
        );
      } catch (alarmError) {
        console.error('agent owner recovery rearm failed', alarmError);
      }
    }
  };

  const validateOwnerRecovery = (
    threadId: string,
    key: string,
    stored: AgentOwnerRecovery,
  ): void => {
    if (
      stored?.version !== 1 ||
      stored.threadId !== threadId ||
      !isPathSafeId(stored.agentId) ||
      !isPathSafeId(stored.resourceId) ||
      !isPathSafeId(stored.runId) ||
      !isPathSafeId(stored.token) ||
      ownerRecoveryKey(stored.runId) !== key ||
      typeof stored.threaded !== 'boolean' ||
      typeof stored.bindingPreexisting !== 'boolean'
    ) {
      throw new Error('stored agent owner recovery is malformed');
    }
    resourceOwner(stored.owner);
  };

  const finalizeTerminalAgentState = async (
    threadId: string,
    runId: string,
    summary: RunSummary,
  ): Promise<void> => {
    const storage = options.stateStorage();
    const key = ownerRecoveryKey(runId);
    const recovery = await storage.get<AgentOwnerRecovery>(key);
    if (recovery) {
      validateOwnerRecovery(threadId, key, recovery);
      await finalizeOwnerRecovery(recovery, summary);
    }
    await deleteAgentRunRecord(storage, runId);
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
      executions.delete(execution.runId);
    }
  };

  // Reconciling approvals is trusted platform work with no person behind it.
  // It used to mint role:'operator', which is why an approval bridge looked
  // indistinguishable from a human operator in the audit trail. The bridge
  // mints its own principal from this id.
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
      DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
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
        workflowId: DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
        runId: summary.runId,
      },
      principalActor(systemPrincipal()),
    );
    const keys = new Set(
      (summary.suspended ?? []).map((path) => path.join('.')),
    );
    return records.filter((record) => {
      const key = record.stepPath?.join('.');
      return (
        key !== undefined &&
        keys.has(key) &&
        record.suspendedAt === summary.suspendedAt?.[key] &&
        record.resumeCount === summary.resumeCount?.[key]
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
    ref: {
      agentId: string;
      resourceId: string;
      runId: string;
    },
  ): Promise<{
    threaded: boolean;
    safeContext: Record<string, unknown>;
  }> => {
    const workflows = await options
      .storage(instanceScopeFor(scope))
      .getStore('workflows');
    const snapshot = await workflows?.loadWorkflowSnapshot({
      workflowName: DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
      runId: ref.runId,
    });
    const input = snapshot?.context.input as
      | {
          agentId?: unknown;
          messageListState?: {
            memoryInfo?: {
              threadId?: unknown;
              resourceId?: unknown;
            } | null;
          };
        }
      | undefined;
    const requestContext = snapshot?.requestContext as
      | Record<string, unknown>
      | undefined;
    const correlation = requestContext?.['breakwater.auditContext'] as
      | Record<string, unknown>
      | undefined;
    const memory = input?.messageListState?.memoryInfo;
    const memoryMatches =
      memory === null ||
      (memory?.threadId === scope.threadId &&
        memory.resourceId === ref.resourceId);
    if (
      input?.agentId !== ref.agentId ||
      requestContext?.runId !== ref.runId ||
      requestContext.threadId !== scope.threadId ||
      requestContext.resourceId !== ref.resourceId ||
      correlation?.agentId !== ref.agentId ||
      correlation.threadId !== scope.threadId ||
      correlation.resourceId !== ref.resourceId ||
      !memoryMatches
    ) {
      throw new AgentHostRequestError(404, 'run not found');
    }
    return {
      threaded: memory !== null,
      safeContext: sanitizeStoredAgentContext(requestContext),
    };
  };

  let recoverOwner: (
    scopeRuntime: ThreadScope['init']['runtime'],
    threadId: string,
    key: string,
    stored: AgentOwnerRecovery,
    ignoreActive?: boolean,
  ) => Promise<'cleared' | 'pending'>;

  const statusFor = async (
    scope: ThreadScope,
    ref: {
      agentId: string;
      resourceId: string;
      runId: string;
    },
    knownThreaded?: boolean,
  ): Promise<AgentRunEnvelope> => {
    const binding = await readBinding();
    const current = await runtimeFor(scope);
    if (!current.catalog.get(ref.agentId)) {
      throw new AgentHostRequestError(404, 'agent not found');
    }
    const summary = await scope.init.runtime.status(
      DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
      ref.runId,
    );
    if (!summary) throw new AgentHostRequestError(404, 'run not found');
    const threaded =
      knownThreaded ?? (await snapshotExecutionFor(scope, ref)).threaded;
    const bindingMatches =
      binding?.agentId === ref.agentId && binding.resourceId === ref.resourceId;
    if ((threaded && !bindingMatches) || (!threaded && binding)) {
      throw new AgentHostRequestError(404, 'run not found');
    }
    const stored = await readRun(ref.runId);
    if (stored && stored.agentId !== ref.agentId) {
      throw new AgentHostRequestError(404, 'run not found');
    }
    if (summary.status === 'suspended' && !stored) {
      throw new AgentHostRequestError(
        409,
        'suspended agent run has no recoverable execution principal',
      );
    }
    const principal = stored?.principal ?? scope.principal;
    const result = await envelopeFor(scope, ref, principal, summary);
    if (isTerminalRunStatus(summary.status) && stored) {
      await deleteAgentRunRecord(options.stateStorage(), ref.runId);
    }
    if (isTerminalRunStatus(summary.status)) {
      const key = ownerRecoveryKey(ref.runId);
      const recovery = await options
        .stateStorage()
        .get<AgentOwnerRecovery>(key);
      if (recovery) {
        await recoverOwner(
          scope.init.runtime,
          scope.threadId,
          key,
          recovery,
          true,
        );
      }
    }
    return result;
  };

  recoverOwner = async (
    scopeRuntime: ThreadScope['init']['runtime'],
    threadId: string,
    key: string,
    stored: AgentOwnerRecovery,
    ignoreActive = false,
  ): Promise<'cleared' | 'pending'> =>
    withBindingLock(() =>
      withRecoveryLock(async () => {
        validateOwnerRecovery(threadId, key, stored);
        const storage = options.stateStorage();
        const currentRecovery = await storage.get<AgentOwnerRecovery>(key);
        if (currentRecovery?.token !== stored.token) return 'pending';
        if (!ignoreActive && executions.has(stored.runId)) return 'pending';

        const summary = await scopeRuntime.recoverStartAttempt(
          DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
          stored.runId,
          stored.token,
        );
        const [binding, record] = await Promise.all([
          readBinding(),
          readRun(stored.runId),
        ]);
        const bindingMatches =
          binding?.agentId === stored.agentId &&
          binding.resourceId === stored.resourceId;
        if (record && (!summary || isTerminalRunStatus(summary.status))) {
          await deleteAgentRunRecord(storage, stored.runId);
        }
        if (!summary && !stored.bindingPreexisting && bindingMatches) {
          await deleteAgentThreadBinding(storage, {
            agentId: stored.agentId,
            resourceId: stored.resourceId,
          });
        }

        if (summary) {
          await options.resourceAccess().settleReservation(stored.token, []);
          if (!stored.threaded && !isTerminalRunStatus(summary.status)) {
            await ensureOwnerRecoveryAlarm(storage);
            return 'pending';
          }
          if (!stored.threaded) await releaseEphemeralOwnerClaims(stored);
          const current = await storage.get<AgentOwnerRecovery>(key);
          if (current?.token === stored.token) await storage.delete(key);
          return 'cleared';
        }

        const release: Array<{
          kind: 'run' | 'thread' | 'resource';
          resourceId: string;
        }> = [{ kind: 'run', resourceId: stored.runId }];
        const retainThread =
          stored.threaded && stored.bindingPreexisting && bindingMatches;
        if (!retainThread) {
          release.push(
            { kind: 'resource', resourceId: stored.resourceId },
            { kind: 'thread', resourceId: stored.threadId },
          );
        }
        await options.resourceAccess().settleReservation(stored.token, release);
        const current = await storage.get<AgentOwnerRecovery>(key);
        if (current?.token === stored.token) await storage.delete(key);
        return 'cleared';
      }),
    );

  const host: ThreadAgentHost = {
    requestContextForRun: (base) => async (workflowId, runId, leg) => {
      const values = base ? await base(workflowId, runId, leg) : undefined;
      const execution = executions.get(runId);
      return execution && workflowId === DURABLE_AGENTIC_LOOP_WORKFLOW_ID
        ? {
            ...execution.safeContext,
            ...values,
            ...deriveTrustedAgentContext(execution, {}),
          }
        : values;
    },
    serializeDispatch: withDispatchLock,
    blockingRun: (scope) => findBlockingRun(scope.init.runtime),
    scheduleDispatchStatus: async (scope, input) => {
      const ref = runRef(scope, {
        ...input,
        threadId: scope.threadId,
      });
      const key = ownerRecoveryKey(ref.runId);
      const recovery = await options
        .stateStorage()
        .get<AgentOwnerRecovery>(key);
      if (recovery) {
        await recoverOwner(
          scope.init.runtime,
          scope.threadId,
          key,
          recovery,
          true,
        );
      }
      const summary = await scope.init.runtime.status(
        DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
        ref.runId,
      );
      if (!summary) return undefined;
      return (await statusFor(scope, ref)).summary;
    },
    recoverOwnership: (scopeRuntime, threadId) =>
      withDispatchLock(async () => {
        const storage = options.stateStorage();
        try {
          const pending = await storage.list<AgentOwnerRecovery>({
            prefix: AGENT_OWNER_RECOVERY_PREFIX,
          });
          for (const [key, stored] of pending) {
            await recoverOwner(scopeRuntime, threadId, key, stored);
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
      }),
    start: async (scope, input) => {
      const ref = runRef(scope, input as unknown as Record<string, unknown>);
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
      const owner = source.owner;
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
      return withExecution(execution, async () => {
        const recoveryKey = ownerRecoveryKey(ref.runId);
        const pending = await options
          .stateStorage()
          .get<AgentOwnerRecovery>(recoveryKey);
        if (pending) {
          await recoverOwner(
            scope.init.runtime,
            scope.threadId,
            recoveryKey,
            pending,
            true,
          );
        }
        const existingRecord = await readRun(ref.runId);
        const existingSummary = await scope.init.runtime.status(
          DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
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
        const recovery = await withBindingLock(async () => {
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
          const blocking = await findBlockingRun(scope.init.runtime);
          if (blocking && blocking.runId !== ref.runId) {
            throw new AgentHostRequestError(
              409,
              `thread is blocked by run '${blocking.runId}'`,
            );
          }
          const recovery: AgentOwnerRecovery = {
            version: 1,
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
                  options.resourceAccess().owner(claim.kind, claim.resourceId),
                ),
              )
            ).some((registered) => registered !== undefined)
          ) {
            throw new AgentHostRequestError(404, 'run not found');
          }
          await armOwnerRecovery(recovery);
          if (
            !(await options
              .resourceAccess()
              .reserveAll(claims, owner, recovery.token))
          ) {
            await options
              .resourceAccess()
              .settleReservation(recovery.token, claims);
            await clearOwnerRecovery(recovery);
            throw new AgentHostRequestError(404, 'run not found');
          }
          if (threaded && !existing) {
            await bindAgentThread(options.stateStorage(), {
              version: 1,
              agentId: ref.agentId,
              resourceId: ref.resourceId,
            });
          }
          await writeAgentRunRecord(options.stateStorage(), ref.runId, stored);
          return recovery;
        });
        try {
          const streamOptions = {
            runId: ref.runId,
            requestContext: createTrustedAgentRequestContext(execution),
            ...(input.threaded !== false
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
          await durable.streamUntilPersisted(
            messages,
            streamOptions,
            scope.principal.id,
            scope.principal.kind,
            recovery.token,
            ...(scheduleDispatch ? ([scheduleDispatch] as const) : []),
          );
          const summary = await scope.init.runtime.status(
            DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
            ref.runId,
          );
          if (!summary) throw new Error('agent run did not persist a summary');
          const result = await envelopeFor(
            scope,
            ref,
            scope.principal,
            summary,
          );
          if (isTerminalRunStatus(result.summary.status)) {
            await deleteAgentRunRecord(options.stateStorage(), ref.runId);
          }
          await finalizeOwnerRecoveryBestEffort(recovery, summary);
          return result;
        } catch (error) {
          let summary: RunSummary | null | undefined;
          try {
            summary = await scope.init.runtime.recoverStartAttempt(
              DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
              ref.runId,
              recovery.token,
            );
          } catch {
            // Unknown authoritative state: retain metadata and fail closed.
          }
          if (
            summary === null ||
            (summary && isTerminalRunStatus(summary.status))
          ) {
            await deleteAgentRunRecord(options.stateStorage(), ref.runId);
          }
          if (summary) {
            await finalizeOwnerRecoveryBestEffort(recovery, summary);
            return envelopeFor(scope, ref, scope.principal, summary);
          } else if (summary === null) {
            try {
              await recoverOwner(
                scope.init.runtime,
                scope.threadId,
                recoveryKey,
                recovery,
                true,
              );
            } catch (recoveryError) {
              console.error('agent owner recovery failed', recoveryError);
            }
          } else {
            try {
              await withRecoveryLock(() =>
                ensureOwnerRecoveryAlarm(options.stateStorage()),
              );
            } catch (alarmError) {
              console.error('agent owner recovery rearm failed', alarmError);
            }
          }
          throw error;
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
        await snapshotExecutionFor(scope, ref);
        const owner = await options.resourceAccess().owner('run', ref.runId);
        if (preflightUrl.searchParams.get('replay') !== '1') {
          await scope.init.runtime.cancelActiveExecution(
            DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
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
          if ('resourceOwner' in body || 'requestedBy' in body) {
            throw new AgentHostRequestError(
              400,
              'start owner and requester are derived from trusted provenance',
            );
          }
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
              ...(typeof body.prompt === 'string'
                ? { prompt: body.prompt }
                : {}),
              entryPath: requestedEntry,
              threaded: body.threaded !== false,
              safeContext: safeContext(body.safeContext),
              providerOptions: providerOptions(body.providerOptions),
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
          await statusFor(scope, ref, snapshotExecution.threaded);
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
              ...('resumeData' in body ? { resumeData: body.resumeData } : {}),
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
          if (isTerminalRunStatus(summary.status)) {
            await deleteAgentRunRecord(options.stateStorage(), ref.runId);
            const recovery = await options
              .stateStorage()
              .get<AgentOwnerRecovery>(ownerRecoveryKey(ref.runId));
            if (recovery) {
              await finalizeOwnerRecoveryBestEffort(recovery, summary);
            }
          }
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
          if (url.searchParams.get('dispatch') === '1') {
            const key = ownerRecoveryKey(ref.runId);
            const pending = await options
              .stateStorage()
              .get<AgentOwnerRecovery>(key);
            if (pending) {
              await recoverOwner(
                scope.init.runtime,
                scope.threadId,
                key,
                pending,
                true,
              );
            }
            return json(await statusFor(scope, ref));
          }
          return json(await statusFor(scope, ref));
        }

        if (
          segments.length === 4 &&
          segments[3] === 'terminate' &&
          request.method === 'POST'
        ) {
          const runtime = scope.init.runtime;
          const replayOnly = url.searchParams.get('replay') === '1';
          const storedRun = await readRun(ref.runId);
          if (storedRun && storedRun.agentId !== ref.agentId) {
            throw new AgentHostRequestError(404, 'run not found');
          }
          await snapshotExecutionFor(scope, ref);
          const preflightOwner = await options
            .resourceAccess()
            .owner('run', ref.runId);
          if (!replayOnly && !preflightedTermination) {
            await runtime.cancelActiveExecution(
              DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
              ref.runId,
              'cancelled',
              [scope.principal, preflightOwner ?? scope.principal],
            );
          }
          const owner = await options.resourceAccess().owner('run', ref.runId);
          if (replayOnly) {
            const existing = await runtime.status(
              DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
              ref.runId,
            );
            if (
              existing?.status !== 'cancelled' &&
              existing?.status !== 'timed_out'
            ) {
              throw new AgentHostRequestError(404, 'run not found');
            }
          }
          const transition = await runtime.terminateAsPrincipal(
            DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
            ref.runId,
            scope.principal,
            owner ?? scope.principal,
          );
          let summary = transition.summary;
          if (!transition.cleanup.cleanupCompleted) {
            await abandonApprovalsForRun(
              options.approvalService(scope),
              DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
              ref.runId,
              transition.cleanup.status,
              options.systemPrincipalId ?? 'flowsafe-system',
            );
            const dispatch = transition.cleanup.scheduleDispatch;
            if (dispatch) {
              if (!options.discardScheduleDispatch) {
                throw new Error(
                  'scheduled agent termination requires a dispatch-discard hook',
                );
              }
              await options.discardScheduleDispatch(
                dispatch.scheduleId,
                dispatch.dispatchId,
                ref.runId,
              );
            }
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
          }
          await finalizeTerminalAgentState(scope.threadId, ref.runId, summary);
          if (!transition.cleanup.cleanupCompleted) {
            summary = await runtime.completeTerminalCleanup(
              DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
              ref.runId,
              transition.cleanup.revision,
            );
          }
          return json(
            await envelopeFor(
              scope,
              ref,
              storedRun?.principal ?? scope.principal,
              summary,
            ),
          );
        }

        if (
          segments.length === 4 &&
          segments[3] === 'stream' &&
          request.method === 'GET'
        ) {
          const run = await statusFor(scope, ref);
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
