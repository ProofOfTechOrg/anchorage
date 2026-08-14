// SPDX-License-Identifier: Apache-2.0

import type { DurableObjectState } from '@cloudflare/workers-types';
import {
  ApprovalConflictError,
  type ApprovalRecord,
  type ApprovalService,
} from '@proofoftech/flowsafe/approval-api';
import {
  type DurableObjectRunLifecycleHooks,
  DurableObjectRunner,
  doErrorResponse,
  verifyDurableObjectDeploymentRequest,
} from '@proofoftech/flowsafe/do-runner';
import {
  approvalStoreFactoryFor,
  buildHostApprovalService,
  createDoRunTopology,
  createFlowsafeRunnerLifecycle,
  createStateEgressFetch,
  type FlowsafeRunnerLifecycleConfig,
  queueApprovalForSuspension,
  RunRouteError,
} from '@proofoftech/flowsafe/host-kit';

import {
  CONFORMANCE_CONTRACT,
  CONFORMANCE_SYSTEM_PRINCIPAL_ID,
  CONFORMANCE_WORKFLOW_ID,
  requiredText,
} from './contract.js';
import type { ConformanceStateEnv } from './env.js';
import { countEffects, defineConformanceWorkflows } from './workflow.js';

/**
 * The trusted state script's conformance surface. The candidate in front of it
 * is a stateless dispatcher, because the gate starts a FlowSafe run on release
 * one, replaces the candidate with release two, and only then approves —
 * nothing durable may live in candidate isolate memory.
 *
 * Each method answers one action from `docs/fleet-control.md` under "Implement
 * the artifact contract" and returns only that action's fields. The candidate
 * adds `contractVersion` and `action`, so the gate's exact-key discipline has
 * exactly one owner.
 */

const MARKER_KEY = 'conformance:state-marker:v1';
const runKey = (runId: string) => `conformance:run:${runId}`;

/**
 * Two identities on purpose: `ApprovalService.decide` enforces separation of
 * duties, so the principal that starts the run can never approve it.
 */
const REQUESTER = { id: 'conformance-requester', role: 'operator' } as const;
const DECIDER = { id: 'conformance-approver', role: 'admin' } as const;
export const conformanceRunnerLifecycleConfig = {
  systemPrincipalId: CONFORMANCE_SYSTEM_PRINCIPAL_ID,
} satisfies FlowsafeRunnerLifecycleConfig<ConformanceStateEnv>;

export class ConformanceRunner extends DurableObjectRunner<ConformanceStateEnv> {
  protected build(env: ConformanceStateEnv) {
    return defineConformanceWorkflows(env);
  }

  protected runOwnership(env: ConformanceStateEnv) {
    return approvalStoreFactoryFor(env.DB).resources();
  }

  protected runLifecycle(
    env: ConformanceStateEnv,
  ): DurableObjectRunLifecycleHooks {
    return createFlowsafeRunnerLifecycle(
      conformanceRunnerLifecycleConfig,
      env,
      { waitUntil: this.state?.waitUntil?.bind(this.state) },
    );
  }
}

/** The class the v2 migration appends; absent from release one by design. */
export class ConformanceV2 {
  readonly #state: DurableObjectState;
  readonly #env: ConformanceStateEnv;

  constructor(state: DurableObjectState, env: ConformanceStateEnv) {
    this.#state = state;
    this.#env = env;
  }

  async fetch(request: Request): Promise<Response> {
    try {
      await verifyDurableObjectDeploymentRequest(
        request,
        this.#state,
        this.#env,
      );
      if (this.#state.id.name !== CONFORMANCE_CONTRACT.v2InstanceName) {
        throw new Error(
          `conformance v2 must be addressed as '${CONFORMANCE_CONTRACT.v2InstanceName}'`,
        );
      }
      const { nonce } = (await request.json()) as { nonce?: unknown };
      if (typeof nonce !== 'string' || nonce.length === 0) {
        return Response.json({ error: 'nonce is required' }, { status: 400 });
      }
      await this.#state.storage.put(`conformance:v2:${nonce}`, nonce);
      const stored = await this.#state.storage.get<string>(
        `conformance:v2:${nonce}`,
      );
      return Response.json({ nonce, stored: stored === nonce });
    } catch (error) {
      return doErrorResponse(error);
    }
  }
}

interface StateActionRequest {
  readonly action?: unknown;
  readonly marker?: unknown;
  readonly url?: unknown;
  readonly effectNonce?: unknown;
  readonly runId?: unknown;
  readonly approvalId?: unknown;
  readonly revision?: unknown;
}

/**
 * The optimistic-concurrency token the contract calls `revision`. A grant binds
 * to the exact (suspendedAt, resumeCount) suspension; `suspendedAt` is the
 * integer half, so echoing it makes a stale approve detectable instead of
 * making the field decorative.
 */
function revisionOf(record: ApprovalRecord): number {
  return record.suspendedAt ?? 0;
}

/**
 * The gate only ever sees `doErrorResponse`'s masked body, so the state side
 * has to say what actually broke. Structured like the rest of the host's
 * Durable Object logging.
 */
function logStateFailure(action: unknown, error: unknown): void {
  console.error(
    JSON.stringify({
      type: 'conformance-state-error',
      action: typeof action === 'string' ? action : 'unknown',
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : undefined,
    }),
  );
}

export type ReplayAction =
  | 'flowsafe-replay-decision'
  | 'flowsafe-replay-resume';

/**
 * Recognize ONLY the exact refusal each replay action exists to prove.
 *
 * A blanket `catch { rejected = true }` would answer 409 for any failure at all
 * — a transport fault, a typo, a deleted guard — so the gate would report
 * FlowSafe's anti-replay defenses as working while they were entirely bypassed.
 * The claim is that a specific bar held, so the bar is what gets identified.
 */
export function isExpectedReplayRefusal(
  action: ReplayAction,
  error: unknown,
): boolean {
  if (action === 'flowsafe-replay-decision') {
    // The decision store's compare-and-swap found a record that is no longer
    // pending. Anything else is not this proof.
    return (
      error instanceof ApprovalConflictError &&
      error.currentStatus === 'approved'
    );
  }
  // The raw resume reached the runner and the runner refused it BECAUSE the run
  // is no longer suspended: RunNotSuspendedError, which the Durable Object maps
  // to 409 (`do-error-response.ts`). Not a status class — a 404, 400 or 413 from
  // the same topology means the request never reached the bar, and reporting
  // those as a refusal would attest a defense that was never exercised.
  return error instanceof RunRouteError && error.status === 409;
}

export class ConformanceState {
  readonly #state: DurableObjectState;
  readonly #env: ConformanceStateEnv;

  constructor(state: DurableObjectState, env: ConformanceStateEnv) {
    this.#state = state;
    this.#env = env;
  }

  #topology() {
    return createDoRunTopology(
      this.#env.RUNNER,
      this.#env.DEPLOYMENT_IDENTITY_SECRET,
    );
  }

  #approvals(): ApprovalService {
    return buildHostApprovalService(
      approvalStoreFactoryFor(this.#env.DB).store(),
      {
        systemPrincipalId: CONFORMANCE_SYSTEM_PRINCIPAL_ID,
        deploymentTag: this.#env.DEPLOYMENT_TENANT,
        resumeRun: (record, decision) =>
          this.#topology().resumeRecord(record, decision),
      },
    );
  }

  #record(id: string): Promise<ApprovalRecord | null> {
    return approvalStoreFactoryFor(this.#env.DB).store().get(id);
  }

  /**
   * This object owns the run/nonce binding rather than re-deriving it from the
   * workflow result, because `effectCount` must be readable BEFORE the effect
   * runs (`flowsafe-start` reports zero) and AFTER the state artifact is
   * replaced (the namespace is retained, so this survives).
   */
  async #effectNonceFor(runId: string): Promise<string> {
    const nonce = await this.#state.storage.get<string>(runKey(runId));
    if (!nonce) throw new Error(`run '${runId}' is not a conformance run`);
    return nonce;
  }

  async #countFor(runId: string): Promise<number> {
    return countEffects(this.#env.DB, await this.#effectNonceFor(runId));
  }

  async #marker(
    action: 'state-marker-put' | 'state-marker-get',
    input: string,
  ) {
    if (action === 'state-marker-put') {
      await this.#state.storage.put(MARKER_KEY, input);
    }
    // Always read back from storage. The gate replays `state-marker-get` after
    // replacing the whole state artifact, so echoing the request would prove
    // nothing about Durable Object namespace retention.
    const stored = await this.#state.storage.get<string>(MARKER_KEY);
    return { marker: stored ?? '' };
  }

  async #stateEgress(url: string, expectAllowed: boolean) {
    const response = await createStateEgressFetch(this.#env)(url, {
      redirect: 'manual',
    });
    return expectAllowed
      ? { allowed: true, upstreamStatus: response.status }
      : { denied: true, upstreamStatus: response.status };
  }

  async #start(effectNonce: string) {
    const summary = await this.#topology().start({
      workflowId: CONFORMANCE_WORKFLOW_ID,
      runId: crypto.randomUUID(),
      inputData: { effectNonce },
      principal: { kind: 'human', ...REQUESTER },
    });
    if (summary.status !== 'suspended') {
      throw new Error(
        `conformance run reached '${summary.status}' without suspending`,
      );
    }
    const [record] = await queueApprovalForSuspension(
      this.#approvals(),
      CONFORMANCE_WORKFLOW_ID,
      summary,
      REQUESTER.id,
      CONFORMANCE_SYSTEM_PRINCIPAL_ID,
    );
    if (!record) throw new Error('the suspended gate filed no approval record');
    await this.#state.storage.put(runKey(summary.runId), effectNonce);
    return {
      runId: summary.runId,
      approvalId: record.id,
      revision: revisionOf(record),
      status: 'pending',
      effectCount: await countEffects(this.#env.DB, effectNonce),
    };
  }

  async #pendingApproval(runId: string, approvalId: string, revision: number) {
    const record = await this.#record(approvalId);
    if (!record || record.runId !== runId) {
      throw new Error(`approval '${approvalId}' does not belong to '${runId}'`);
    }
    if (revisionOf(record) !== revision) {
      throw new Error(
        `approval '${approvalId}' moved past revision ${revision}`,
      );
    }
    return record;
  }

  async #approvalUpdate(runId: string, approvalId: string, revision: number) {
    const record = await this.#pendingApproval(runId, approvalId, revision);
    return {
      runId: record.runId,
      approvalId: record.id,
      revision: revisionOf(record),
      status: record.status,
    };
  }

  async #approve(runId: string, approvalId: string, revision: number) {
    await this.#pendingApproval(runId, approvalId, revision);
    await this.#approvals().decide(
      approvalId,
      { decision: 'approve' },
      { ...DECIDER },
    );
    const decided = await this.#record(approvalId);
    return {
      runId,
      approvalId,
      status: decided?.status ?? 'unknown',
      resumed: true,
      effectCount: await this.#countFor(runId),
    };
  }

  async #status(runId: string) {
    // Read through a runtime this object builds over D1, not through the run
    // Durable Object: the claim is that the TERMINAL state is durable, which a
    // stub read could satisfy from the runner's own memory.
    const summary = await defineConformanceWorkflows(this.#env).status(
      CONFORMANCE_WORKFLOW_ID,
      runId,
    );
    if (!summary) throw new Error(`run '${runId}' has no durable D1 state`);
    return {
      runId,
      terminalD1: summary.status === 'success' || summary.status === 'failed',
      effectCount: await this.#countFor(runId),
    };
  }

  async #replay(action: ReplayAction, runId: string, approvalId: string) {
    // Resolve the run first, outside the guarded region: an unknown run is a
    // caller error, not a security refusal, and must not be reported as one.
    const effectCountBefore = await this.#countFor(runId);
    let rejected = false;
    try {
      if (action === 'flowsafe-replay-decision') {
        await this.#approvals().decide(
          approvalId,
          { decision: 'approve' },
          { ...DECIDER },
        );
      } else {
        // A raw resume carrying no derived grant. The run is already terminal
        // and the write gate has no capability to mint, so both bars must hold.
        const record = await this.#record(approvalId);
        await this.#topology().resume(
          CONFORMANCE_WORKFLOW_ID,
          runId,
          {
            step: record?.stepPath ?? ['approval'],
            resumeData: { approved: true },
          },
          DECIDER.id,
          'human',
        );
      }
    } catch (error) {
      if (!isExpectedReplayRefusal(action, error)) {
        logStateFailure(action, error);
        throw error;
      }
      rejected = true;
    }
    // Re-read rather than reuse: a replay that slipped through would have run
    // the effect again, and that is the number the gate compares.
    const effectCount = await this.#countFor(runId);
    if (effectCount !== effectCountBefore) {
      throw new Error(
        `${action} changed the effect count from ${effectCountBefore} to ${effectCount}`,
      );
    }
    return { effectCount, rejected, runId };
  }

  async fetch(request: Request): Promise<Response> {
    try {
      await verifyDurableObjectDeploymentRequest(
        request,
        this.#state,
        this.#env,
      );
      if (this.#state.id.name !== CONFORMANCE_CONTRACT.stateInstanceName) {
        throw new Error(
          `conformance state must be addressed as '${CONFORMANCE_CONTRACT.stateInstanceName}'`,
        );
      }
      const body = (await request.json()) as StateActionRequest;
      try {
        return await this.#dispatch(body);
      } catch (error) {
        logStateFailure(body.action, error);
        throw error;
      }
    } catch (error) {
      return doErrorResponse(error);
    }
  }

  async #dispatch(body: StateActionRequest): Promise<Response> {
    switch (body.action) {
      case 'state-marker-put':
      case 'state-marker-get':
        return Response.json(
          await this.#marker(body.action, requiredText(body.marker, 'marker')),
        );
      case 'state-egress-allowed':
        return Response.json(
          await this.#stateEgress(requiredText(body.url, 'url'), true),
        );
      case 'state-egress-denied':
        return Response.json(
          await this.#stateEgress(requiredText(body.url, 'url'), false),
        );
      case 'flowsafe-start':
        return Response.json(
          await this.#start(requiredText(body.effectNonce, 'effectNonce')),
        );
      case 'flowsafe-approval-update':
        return Response.json(
          await this.#approvalUpdate(
            requiredText(body.runId, 'runId'),
            requiredText(body.approvalId, 'approvalId'),
            Number(body.revision),
          ),
        );
      case 'flowsafe-approve':
        return Response.json(
          await this.#approve(
            requiredText(body.runId, 'runId'),
            requiredText(body.approvalId, 'approvalId'),
            Number(body.revision),
          ),
        );
      case 'flowsafe-status':
        return Response.json(
          await this.#status(requiredText(body.runId, 'runId')),
        );
      case 'flowsafe-replay-decision':
      case 'flowsafe-replay-resume': {
        const replay = await this.#replay(
          body.action,
          requiredText(body.runId, 'runId'),
          requiredText(body.approvalId, 'approvalId'),
        );
        return Response.json(replay, { status: replay.rejected ? 409 : 200 });
      }
      default:
        return Response.json(
          { error: `unknown state action '${String(body.action)}'` },
          { status: 400 },
        );
    }
  }
}
