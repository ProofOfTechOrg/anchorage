// SPDX-License-Identifier: Apache-2.0
// The DO-stub run topology every DO-routing host repeated verbatim: one
// Durable Object instance per (workflowId, runId), addressed by idFromName,
// with start/status/resume travelling as HTTP through the stub and read back
// via doSummary. Hosts pass their `env.RUNNER` binding directly — the
// namespace/stub types are structural subsets so this module (like the rest
// of host-kit) never imports @cloudflare/workers-types.

import type {
  ApprovalDecision,
  ApprovalRecord,
} from '../approval-api/index.js';
import { defaultResumeData } from '../approval-api/index.js';
import type { RunSummary } from '../do-runner/index.js';
import { type DoResponseLike, doSummary } from './do-response.js';

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
  start(
    workflowId: string,
    runId: string,
    inputData: unknown,
  ): Promise<RunSummary>;
  /** createRunRouter's `status` thunk (a DO 404 reads as undefined). */
  status(workflowId: string, runId: string): Promise<RunSummary | undefined>;
  /** createRunRouter's `resume` thunk. */
  resume(workflowId: string, runId: string, body: unknown): Promise<RunSummary>;
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

export function createDoRunTopology<Id>(
  namespace: RunnerNamespaceLike<Id>,
): DoRunTopology {
  // One DO per (workflowId, runId): the name join is unambiguous because
  // PATH_SAFE_ID_PATTERN excludes ':' from both ids.
  const stub = (workflowId: string, runId: string): RunnerStubLike =>
    namespace.get(namespace.idFromName(`${workflowId}:${runId}`));

  const resume = async (
    workflowId: string,
    runId: string,
    body: unknown,
  ): Promise<RunSummary> =>
    doSummary(
      await stub(workflowId, runId).fetch(
        `http://do/runs/${workflowId}/${runId}/resume`,
        {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(body),
        },
      ),
    );

  return {
    start: async (workflowId, runId, inputData) =>
      doSummary(
        await stub(workflowId, runId).fetch('http://do/runs', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ workflowId, runId, inputData }),
        }),
      ),
    // The DO answers 404 for a run it has never seen; the router turns the
    // undefined into its own 404 rather than leaking the DO's body.
    status: async (workflowId, runId) => {
      const response = await stub(workflowId, runId).fetch(
        `http://do/runs/${workflowId}/${runId}`,
      );
      if (response.status === 404) return undefined;
      return doSummary(response);
    },
    resume,
    resumeRecord: (record, decision) =>
      resume(record.workflowId, record.runId, {
        step: record.stepPath,
        resumeData: defaultResumeData(record, decision),
      }),
  };
}
