// SPDX-License-Identifier: Apache-2.0

import type { Agent } from '@mastra/core/agent';
import { AGENT_STREAM_TOPIC } from '@mastra/core/agent/durable';
import { Mastra } from '@mastra/core/mastra';
import type { MastraCompositeStore } from '@mastra/core/storage';

import {
  type AgentEntryPath,
  type AgentRunRecord,
  type AgentThreadBinding,
  bindAgentThread,
  createFlowsafeDurableAgent,
  DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
  deleteAgentRunRecord,
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
  type ExecutionPrincipal,
  principalActor,
  principalAuditFields,
  RUN_START_ROLES,
  samePrincipal,
} from '../approval-api/index.js';
import {
  DoStatusError,
  mintResourceId,
  type RequestContextProvider,
  type RunSummary,
  type ThreadScope,
  tenantOwnsMemoryId,
  tenantOwnsSaltedId,
} from '../do-runner/index.js';
import { reconcileApprovalsForSummary } from '../host-kit/index.js';
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
  TrustedAgentExecution,
} from './types.js';

export interface AgentThreadStateStorage {
  get<T = unknown>(key: string): Promise<T | undefined>;
  put<T>(key: string, value: T): Promise<void>;
  delete(key: string): Promise<boolean>;
}

export interface AgentThreadInstanceScope {
  readonly threadId: string;
  readonly tenantId: string;
  readonly init: ThreadScope['init'];
}

/** What the host is asked to authorize. Never a human — those go by role. */
export interface AutomatedEntryRequest {
  principal: Extract<
    ExecutionPrincipal,
    { kind: 'service' | 'agent' | 'system' }
  >;
  agentId: string;
  entryPath: AgentEntryPath;
  tenantId: string;
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

export interface ThreadAgentHostOptions {
  /** Narrows automated entry beyond what each agent's metadata declares. */
  authorizeAutomatedEntry?: AutomatedEntryAuthorizer;
  buildModules:
    | ((scope: AgentThreadInstanceScope) => readonly AgentModule[])
    | ((scope: AgentThreadInstanceScope) => Promise<readonly AgentModule[]>);
  storage: (scope: AgentThreadInstanceScope) => MastraCompositeStore;
  stateStorage: () => AgentThreadStateStorage;
  approvalService: (scope: AgentThreadInstanceScope) => ApprovalService;
  systemActorId?: string;
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
  /**
   * Non-reserved context accepted from trusted internal topology calls only.
   * It is never part of the public agent-start request contract.
   */
  safeContext?: Record<string, unknown>;
}

export interface BoundThreadAgent {
  agentId: string;
  resourceId: string;
  durableAgent: FlowsafeDurableAgent;
}

export interface ThreadAgentHost {
  requestContextForRun(base?: RequestContextProvider): RequestContextProvider;
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
];

function isTerminalRunStatus(status: RunSummary['status']): boolean {
  return TERMINAL_RUN_STATUSES.includes(status);
}

function entryPath(value: unknown): AgentEntryPath {
  const allowed = [
    'http.start',
    'approval.resume',
    'signal.message',
    'signal.queue',
    'signal.reactive',
    'signal.state',
    'signal.notification',
    'signal.wake',
    'notification.dispatch',
    'schedule.fire',
  ] as const;
  if (
    typeof value !== 'string' ||
    !(allowed as readonly string[]).includes(value)
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
    typeof agentId !== 'string' ||
    threadId !== scope.threadId ||
    typeof resourceId !== 'string' ||
    !tenantOwnsMemoryId(scope.tenantId, resourceId) ||
    typeof runId !== 'string' ||
    !tenantOwnsSaltedId(scope.tenantId, runId)
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
  let bindingTail = Promise.resolve();

  const instanceScopeFor = (scope: ThreadScope): AgentThreadInstanceScope => {
    if (stableScope) {
      if (
        stableScope.threadId !== scope.threadId ||
        stableScope.tenantId !== scope.tenantId ||
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
      tenantId: scope.tenantId,
      init: scope.init,
    });
    return stableScope;
  };

  const withBindingLock = async <T>(
    operation: () => Promise<T>,
  ): Promise<T> => {
    const previous = bindingTail;
    let release: () => void = () => undefined;
    bindingTail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await operation();
    } finally {
      release();
    }
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
      agents: Object.fromEntries(
        catalog.modules.map((module) => [
          module.meta.id,
          module.agent as unknown as Agent,
        ]),
      ),
      ...(scope.init.pubsub ? { pubsub: scope.init.pubsub } : {}),
    });
    const agents = new Map<string, FlowsafeDurableAgent>();
    for (const module of catalog.modules) {
      agents.set(
        module.meta.id,
        createFlowsafeDurableAgent({
          agent: mastra.getAgent(module.meta.id),
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
   * not opted in fails here rather than executing as a synthetic operator.
   */
  const authorize = async (
    scope: ThreadScope,
    agentId: string,
    entry: AgentEntryPath,
    principal: ExecutionPrincipal,
  ) => {
    const current = await runtimeFor(scope);
    const module = current.catalog.get(agentId);
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
          tenantId: scope.tenantId,
          threadId: scope.threadId,
        })) ?? true;
      granted = hostAllows;
      if (!granted) reason = 'host denied this automated entry';
    }
    audit(options.audit, {
      actor: principalActor(principal),
      action: 'agent.entry.authorize',
      resource: `agent:${agentId}`,
      decision: granted ? 'allowed' : 'denied',
      ...(reason !== undefined ? { reason } : {}),
      detail: {
        agentId,
        tenantId: scope.tenantId,
        threadId: scope.threadId,
        entryPath: entry,
        ...principalAuditFields(principal),
      },
    });
    if (!module) throw new AgentHostRequestError(404, 'agent not found');
    if (!granted) throw new AgentHostRequestError(403, 'forbidden');
    return { current, module };
  };

  const readBinding = (): Promise<AgentThreadBinding | undefined> =>
    readAgentThreadBinding(options.stateStorage());

  const readRun = (runId: string): Promise<AgentRunRecord | undefined> =>
    readAgentRunRecord(options.stateStorage(), runId);

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
  // indistinguishable from a human operator in the audit trail.
  const systemPrincipal = (scope: ThreadScope): ExecutionPrincipal => ({
    kind: 'system',
    id: options.systemActorId ?? 'flowsafe-system',
    tenantId: scope.tenantId,
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
    const service = options.approvalService(instanceScopeFor(scope));
    await reconcileApprovalsForSummary(
      service,
      DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
      summary,
      systemPrincipal(scope),
      {
        kind: 'agent-thread',
        agentId,
        threadId: scope.threadId,
        resourceId,
        principal,
      },
      principal.id,
    );
    const records = await service.list(
      {
        workflowId: DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
        runId: summary.runId,
      },
      principalActor(systemPrincipal(scope)),
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
    const approvals = await currentApprovals(
      scope,
      summary,
      principal,
      ref.agentId,
      ref.resourceId,
    );
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
    // Inert today — a suspended run without a stored record already threw 409
    // above, and only a suspended run consults this. Kept as the scope's
    // PRINCIPAL rather than its actor so that if those two conditions are ever
    // decoupled, the fallback still cannot relabel an automated run as whoever
    // happened to poll its status.
    const principal = stored?.principal ?? scope.principal;
    const result = await envelopeFor(scope, ref, principal, summary);
    if (isTerminalRunStatus(summary.status) && stored) {
      await deleteAgentRunRecord(options.stateStorage(), ref.runId);
    }
    return result;
  };

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
    start: async (scope, input) => {
      const ref = runRef(scope, input as unknown as Record<string, unknown>);
      const hasPrompt = input.prompt !== undefined;
      const hasMessages = input.messages !== undefined;
      if (
        hasPrompt === hasMessages ||
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
      const messages = input.messages ?? input.prompt;
      if (messages === undefined) {
        throw new AgentHostRequestError(400, 'agent input is required');
      }
      const entry = entryPath(input.entryPath);
      const { current, module } = await authorize(
        scope,
        ref.agentId,
        entry,
        scope.principal,
      );
      if (ref.resourceId !== mintResourceId(scope.tenantId, scope.threadId)) {
        throw new AgentHostRequestError(404, 'run not found');
      }
      const execution: TrustedAgentExecution = {
        agentId: ref.agentId,
        principal: scope.principal,
        threadId: scope.threadId,
        resourceId: ref.resourceId,
        runId: ref.runId,
        entryPath: entry,
        safeContext: safeContext(input.safeContext),
      };
      const durable = current.agents.get(module.meta.id);
      if (!durable) throw new Error('guarded agent was not registered');
      return withExecution(execution, async () => {
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
        if (input.threaded !== false) {
          await withBindingLock(async () => {
            const existing = await readBinding();
            const bindingMatches =
              existing?.agentId === ref.agentId &&
              existing.resourceId === ref.resourceId;
            if (entry === 'schedule.fire') {
              if (!bindingMatches) {
                throw new AgentHostRequestError(404, 'run not found');
              }
              return;
            }
            if (existing && !bindingMatches) {
              throw new AgentHostRequestError(
                409,
                'thread is bound to another agent',
              );
            }
            if (!existing) {
              await bindAgentThread(options.stateStorage(), {
                version: 1,
                agentId: ref.agentId,
                resourceId: ref.resourceId,
              });
            }
          });
        }
        const stored: AgentRunRecord = {
          version: 2,
          agentId: ref.agentId,
          principal: scope.principal,
          originEntryPath: entry,
        };
        await writeAgentRunRecord(options.stateStorage(), ref.runId, stored);
        try {
          await durable.streamUntilPersisted(messages, {
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
          });
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
          return result;
        } catch (error) {
          let summary: RunSummary | null | undefined;
          try {
            summary = await scope.init.runtime.status(
              DURABLE_AGENTIC_LOOP_WORKFLOW_ID,
              ref.runId,
            );
          } catch {
            // Unknown authoritative state: retain metadata and fail closed.
          }
          if (!summary || isTerminalRunStatus(summary.status)) {
            await deleteAgentRunRecord(options.stateStorage(), ref.runId);
          }
          throw error;
        }
      });
    },
    resolveBoundAgent: async (scope, input) => {
      const binding = await readBinding();
      if (!binding) throw new AgentHostRequestError(404, 'agent not found');
      if (
        binding.resourceId !== mintResourceId(scope.tenantId, scope.threadId)
      ) {
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
      const url = new URL(request.url);
      if (!url.pathname.startsWith(AGENT_HOST_ROUTE_PREFIX)) return null;

      if (
        request.method === 'POST' &&
        url.pathname === `${AGENT_HOST_ROUTE_PREFIX}/start`
      ) {
        const body = await objectBody(request);
        const ref = runRef(scope, body);
        if (typeof body.prompt !== 'string') {
          throw new AgentHostRequestError(400, 'prompt is required');
        }
        return json(
          await host.start(scope, {
            ...ref,
            prompt: body.prompt,
            entryPath: entryPath(body.entryPath),
            threaded: body.threaded !== false,
            safeContext: safeContext(body.safeContext),
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
        const { current, module } = await authorize(
          scope,
          ref.agentId,
          entryPath(body.entryPath),
          stored.principal,
        );
        const durable = current.agents.get(module.meta.id);
        if (!durable) throw new Error('guarded agent was not registered');
        const step =
          typeof body.step === 'string' ||
          (Array.isArray(body.step) &&
            body.step.every((part) => typeof part === 'string'))
            ? body.step
            : undefined;
        const execution: TrustedAgentExecution = {
          agentId: ref.agentId,
          principal: stored.principal,
          threadId: scope.threadId,
          resourceId: ref.resourceId,
          runId: ref.runId,
          entryPath: 'approval.resume',
          safeContext: snapshotExecution.safeContext,
        };
        const summary = await withExecution(execution, () =>
          durable.resumeViaRuntime({
            runId: ref.runId,
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
          }),
        );
        const result = await envelopeFor(scope, ref, stored.principal, summary);
        if (isTerminalRunStatus(summary.status)) {
          await deleteAgentRunRecord(options.stateStorage(), ref.runId);
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
        return json(await statusFor(scope, ref));
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
        (segments.length === 4 && segments[3] === 'stream');
      return known
        ? json({ error: 'method not allowed' }, 405)
        : json({ error: 'not found' }, 404);
    },
  };
  return host;
}
