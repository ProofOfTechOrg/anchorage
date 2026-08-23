// SPDX-License-Identifier: Apache-2.0
// The DO-stub run topology every DO-routing host repeated verbatim: one
// Durable Object instance per (workflowId, runId), addressed by idFromName,
// with run reads and lifecycle mutations travelling as HTTP through the stub
// and read back via doSummary. Hosts pass their `env.RUNNER` binding directly;
// the namespace/stub types are structural subsets so this module (like the rest
// of host-kit) never imports @cloudflare/workers-types.

import type {
  ApprovalDecision,
  ApprovalRecord,
  ExecutionPrincipal,
  ExecutionPrincipalKind,
} from '../approval-api/index.js';
import {
  defaultResumeData,
  encodeExecutionPrincipal,
} from '../approval-api/index.js';
import {
  deploymentIdentityHeaders,
  EXECUTION_PRINCIPAL_HEADER,
  type RunLifecycleCas,
  type RunSummary,
} from '../do-runner/index.js';
import { type DoResponseLike, doSummary } from './do-response.js';
import type { RunStartInput } from './run-router.js';

/** The subset of a DurableObjectStub the topology uses. */
export interface RunnerStubLike {
  fetch(
    url: string,
    init?: {
      method?: string;
      headers?: Record<string, string>;
      body?: string;
    },
  ): Promise<DoResponseLike>;
}

/** The subset of a DurableObjectNamespace the topology uses. */
export interface RunnerNamespaceLike<Id = unknown> {
  idFromName(name: string): Id;
  get(id: Id): RunnerStubLike;
}

export interface DoRunTopology {
  /** createRunRouter's `start` thunk. */
  start(input: DoRunStartInput): Promise<RunSummary>;
  /** createRunRouter's `status` thunk (a DO 404 reads as undefined). */
  status(workflowId: string, runId: string): Promise<RunSummary | undefined>;
  /** Waits behind start/recovery before declaring a dispatch absent. */
  dispatchStatus(
    workflowId: string,
    runId: string,
  ): Promise<RunSummary | undefined>;
  /**
   * Is a start for this run executing in its Durable Object right now?
   *
   * createRunRouter's liveness probe, and the reason an idempotent replay can
   * tell "still working" from "died holding the claim" without a timer. The run
   * object is the only place that can answer: it is addressed by
   * `idFromName(workflowId:runId)`, so there is exactly one instance that could
   * be running this run, and if that instance says no then nothing is.
   *
   * An unreachable object reads as NOT live. That is the fail-closed direction
   * here — it produces the refusal that asks a human to investigate, whereas a
   * default of "live" would answer a permanently broken run with a permanently
   * retryable 503.
   */
  startLiveness(workflowId: string, runId: string): Promise<boolean>;
  /** createRunRouter's `resume` thunk. */
  resume(
    workflowId: string,
    runId: string,
    body: unknown,
    requestedBy: string,
    requestedByKind: ExecutionPrincipalKind,
  ): Promise<RunSummary>;
  /**
   * ApprovalServiceOptions.resumeRun base: resume a DECIDED approval's run
   * through its DO stub with the standard resumeData contract. Hand this to
   * buildHostApprovalService, which wraps it in the SoD-guarded re-queue.
   */
  resumeRecord(
    record: ApprovalRecord,
    decision: ApprovalDecision,
  ): Promise<RunSummary>;
}

/** Additive run-lifecycle operations returned by createDoRunTopology. */
export interface DoRunLifecycleTopology extends DoRunTopology {
  terminate(
    workflowId: string,
    runId: string,
    principal: ExecutionPrincipal,
    replayOnly?: boolean,
  ): Promise<RunSummary>;
  timeOut(
    workflowId: string,
    runId: string,
    cas: RunLifecycleCas,
    principal: ExecutionPrincipal,
  ): Promise<RunSummary>;
}

export type DoRunStartInput = RunStartInput & {
  /** Prepared trigger paired with scheduleId for a one-shot schedule fire. */
  dispatchId?: string;
  /** Ordinary trusted starts may seed core workflow state. */
  initialState?: unknown;
};

/** The run object's answer to the liveness probe. */
interface StartLivenessBody {
  live?: unknown;
}

export function createDoRunTopology<Id>(
  namespace: RunnerNamespaceLike<Id>,
  deploymentIdentitySecret: string,
): DoRunLifecycleTopology {
  // One DO per (workflowId, runId): the name join is unambiguous because
  // PATH_SAFE_ID_PATTERN excludes ':' from both ids.
  const stub = (workflowId: string, runId: string): RunnerStubLike =>
    namespace.get(namespace.idFromName(`${workflowId}:${runId}`));

  const resume = async (
    workflowId: string,
    runId: string,
    body: unknown,
    requestedBy: string,
    requestedByKind: ExecutionPrincipalKind,
  ): Promise<RunSummary> =>
    doSummary(
      await stub(workflowId, runId).fetch(
        `http://do/runs/${workflowId}/${runId}/resume`,
        {
          method: 'POST',
          headers: deploymentIdentityHeaders(deploymentIdentitySecret, {
            'content-type': 'application/json',
          }),
          body: JSON.stringify({
            ...(body && typeof body === 'object' ? body : {}),
            requestedBy,
            requestedByKind,
          }),
        },
      ),
    );

  return {
    start: async ({
      workflowId,
      runId,
      inputData,
      initialState,
      principal,
      scheduleId,
      dispatchId,
      deadlineMs,
      idempotencyKey,
    }) => {
      if ((scheduleId === undefined) !== (dispatchId === undefined)) {
        throw new Error(
          'scheduled run starts require both scheduleId and dispatchId',
        );
      }
      return doSummary(
        await stub(workflowId, runId).fetch('http://do/runs', {
          method: 'POST',
          headers: deploymentIdentityHeaders(deploymentIdentitySecret, {
            'content-type': 'application/json',
            [EXECUTION_PRINCIPAL_HEADER]: encodeExecutionPrincipal(principal),
          }),
          body: JSON.stringify({
            workflowId,
            runId,
            // The key travels on THIS channel and no other: the request
            // carries the deployment-identity header, which a tenant request
            // bearing one is refused for, so the DO can treat what arrives
            // here as the router's own reserved key.
            ...(idempotencyKey === undefined ? {} : { idempotencyKey }),
            ...(scheduleId === undefined
              ? { inputData, initialState, deadlineMs }
              : { scheduleId, dispatchId, deadlineMs }),
          }),
        }),
      );
    },
    startLiveness: async (workflowId, runId) => {
      // Only an explicit `true` from the run's own object counts as live.
      // Everything else — a non-200, an unparseable body, a field of the wrong
      // shape — is "this did not tell me the run is running", which is not the
      // same as "it is", and treating it as such would turn a broken probe into
      // an indefinitely retryable PENDING for a run nobody is executing.
      const response = await stub(workflowId, runId).fetch(
        `http://do/runs/${workflowId}/${runId}/start-liveness`,
        { headers: deploymentIdentityHeaders(deploymentIdentitySecret) },
      );
      if (response.status !== 200) return false;
      try {
        return ((await response.json()) as StartLivenessBody).live === true;
      } catch {
        return false;
      }
    },
    // The DO answers 404 for a run it has never seen; the router turns the
    // undefined into its own 404 rather than leaking the DO's body.
    status: async (workflowId, runId) => {
      const response = await stub(workflowId, runId).fetch(
        `http://do/runs/${workflowId}/${runId}`,
        { headers: deploymentIdentityHeaders(deploymentIdentitySecret) },
      );
      if (response.status === 404) return undefined;
      return doSummary(response);
    },
    dispatchStatus: async (workflowId, runId) => {
      const response = await stub(workflowId, runId).fetch(
        `http://do/runs/${workflowId}/${runId}/dispatch-status`,
        { headers: deploymentIdentityHeaders(deploymentIdentitySecret) },
      );
      if (response.status === 404) return undefined;
      return doSummary(response);
    },
    resume,
    terminate: async (workflowId, runId, principal, replayOnly = false) =>
      doSummary(
        await stub(workflowId, runId).fetch(
          `http://do/runs/${workflowId}/${runId}/${
            replayOnly ? 'terminate-replay' : 'terminate'
          }`,
          {
            method: 'POST',
            headers: deploymentIdentityHeaders(deploymentIdentitySecret, {
              [EXECUTION_PRINCIPAL_HEADER]: encodeExecutionPrincipal(principal),
            }),
          },
        ),
      ),
    timeOut: async (workflowId, runId, cas, principal) =>
      doSummary(
        await stub(workflowId, runId).fetch(
          `http://do/runs/${workflowId}/${runId}/deadline`,
          {
            method: 'POST',
            headers: deploymentIdentityHeaders(deploymentIdentitySecret, {
              'content-type': 'application/json',
              [EXECUTION_PRINCIPAL_HEADER]: encodeExecutionPrincipal(principal),
            }),
            body: JSON.stringify(cas),
          },
        ),
      ),
    resumeRecord: (record, decision) => {
      if (!record.decidedBy) {
        throw new Error(`approval '${record.id}' has no decision actor`);
      }
      return resume(
        record.workflowId,
        record.runId,
        {
          step: record.stepPath,
          resumeData: defaultResumeData(record, decision),
        },
        record.decidedBy,
        'human',
      );
    },
  };
}
