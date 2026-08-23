// Regression guard for the gtm-app pipeline: the same real Anchorage seams the
// worker runs (DO runner + approval queue + breakwater outreach connector),
// driven in-process with in-memory stores and NO Email Service binding — so the
// send is simulated. Proves the binding-gated simulation STILL enforces the
// approval grant (approve => allowed + outcome 'simulated'; forged resume =>
// denied), and that a rejected review never invokes the connector — the
// properties that make Phase A a real safety demo rather than a bypass.

import { InMemoryStore } from '@mastra/core/storage';
import { AGENT_AUDIT_CONTEXT_KEY, AuditLogger } from '@proofoftech/breakwater';
import {
  type ApprovalActor,
  ApprovalService,
  approvalGrantProvider,
  InMemoryApprovalStore,
  resumeViaRuntime,
} from '@proofoftech/flowsafe/approval-api';
import { describe, expect, it } from 'vitest';
import { buildShowcaseRuntime } from '#worker/runtime';
import { OUTREACH_CONNECTOR } from '#worker/workflows/gtm-outbound';

const OPERATOR: ApprovalActor = {
  id: 'opal',
  role: 'operator',
};
const REVIEWER: ApprovalActor = {
  id: 'ray',
  role: 'reviewer',
};

function buildHarness() {
  const store = new InMemoryApprovalStore();
  const audit = new AuditLogger();
  const grants = approvalGrantProvider(store);
  const runtime = buildShowcaseRuntime({
    initInput: { storage: new InMemoryStore() },
    executionFence: 'none',
    startIdempotency: 'none',
    grantProvider: async (workflowId, runId, leg) => ({
      ...(await grants(workflowId, runId, leg)),
      [AGENT_AUDIT_CONTEXT_KEY]: {
        agentId: workflowId,
        tenantId: 'showcase-test',
        runId,
        entryPath: leg.kind === 'start' ? 'workflow.start' : 'workflow.resume',
      },
    }),
    audit,
    // no `email` binding => the connector simulates the send
  });
  const service = new ApprovalService({
    store,
    // In-memory store, no database to fence against: the opt-out is written down
    // rather than defaulted — see ExecutionFenceWiring.
    executionFence: 'none',
    resumeRun: resumeViaRuntime(runtime),
  });
  return { runtime, service, audit };
}

// Queue the approval bound to a run's current suspension, mirroring the worker
// bridge (queueApprovalForSuspension).
async function queueApproval(
  service: ApprovalService,
  runId: string,
  suspended: readonly (readonly string[])[] | undefined,
  suspendedAtMap: Record<string, number> | undefined,
  suspendPayload: unknown,
) {
  const stepPath = suspended?.[0];
  if (!stepPath) throw new Error('expected a suspended step');
  // Bind the approval to THIS suspension by its exact timestamp. Fail loudly
  // if it goes missing because an unbound capability record is inert.
  const suspendedAt = suspendedAtMap?.[stepPath.join('.')];
  if (typeof suspendedAt !== 'number') {
    throw new Error('expected a numeric suspendedAt for exact-match binding');
  }
  const { record } = await service.create(
    {
      workflowId: 'gtm-outbound',
      runId,
      stepPath: [...stepPath],
      suspendedAt,
      title: 'Approve GTM outreach send',
      payload: suspendPayload,
      connectors: [OUTREACH_CONNECTOR],
    },
    OPERATOR,
  );
  return record;
}

describe('gtm-app: outreach pipeline on real Anchorage seams (simulated send)', () => {
  it('suspends, mints the grant on approve, and the connector runs (simulated)', async () => {
    // #given — a run suspended at the approval gate, queued as an approval
    const { runtime, service, audit } = buildHarness();
    const started = await runtime.start('gtm-outbound', {
      runId: crypto.randomUUID(),
      inputData: { industry: 'fintech', targetCount: 50 },
    });
    expect(started.status).toBe('suspended');
    expect(started.suspended).toEqual([['reviewAndApprove']]);
    const record = await queueApproval(
      service,
      started.runId,
      started.suspended,
      started.suspendedAt,
      started.suspendPayload,
    );

    // #when — a different actor approves; decide() resumes via the runtime,
    // whose provider derives the grant from the now-approved record
    const decided = await service.decide(
      record.id,
      { decision: 'approve', comment: 'lgtm' },
      REVIEWER,
    );

    // #then — the grant admitted the (simulated) send; the run completed
    expect(decided.resume).toMatchObject({ attempted: true, ok: true });
    expect(decided.resume.summary).toMatchObject({
      status: 'success',
      result: { outcome: 'simulated', delivered: 0 },
    });
    expect(audit.events()).toContainEqual(
      expect.objectContaining({
        resource: OUTREACH_CONNECTOR,
        decision: 'allowed',
      }),
    );
  });

  it('fails closed: a resume that bypasses approval finds no grant', async () => {
    // #given — a suspended run, nothing approved
    const { runtime, audit } = buildHarness();
    const started = await runtime.start('gtm-outbound', {
      runId: crypto.randomUUID(),
      inputData: { industry: 'fintech', targetCount: 50 },
    });
    expect(started.status).toBe('suspended');

    // #when — an "approved" resume forged straight at the runtime
    const forged = await runtime.resume('gtm-outbound', started.runId, {
      step: 'reviewAndApprove',
      resumeData: { approved: true },
    });

    // #then — the connector write gate denies (no grant); the run failed
    expect(forged.status).toBe('failed');
    expect(forged.error).toContain(
      'approval required and no matching structured grant was found',
    );
    expect(audit.events()).toContainEqual(
      expect.objectContaining({
        resource: OUTREACH_CONNECTOR,
        decision: 'denied',
      }),
    );
  });

  it('rejected review completes without ever invoking the connector', async () => {
    // #given — a suspended run queued as an approval
    const { runtime, service, audit } = buildHarness();
    const started = await runtime.start('gtm-outbound', {
      runId: crypto.randomUUID(),
      inputData: { industry: 'fintech', targetCount: 50 },
    });
    const record = await queueApproval(
      service,
      started.runId,
      started.suspended,
      started.suspendedAt,
      started.suspendPayload,
    );

    // #when — the reviewer rejects; the run resumes with approved:false
    const decided = await service.decide(
      record.id,
      { decision: 'reject', comment: 'off-brand' },
      REVIEWER,
    );

    // #then — the run completes cleanly (not failed) with outcome 'declined',
    // and the connector was never called (no audit event at all for it)
    expect(decided.resume).toMatchObject({ attempted: true, ok: true });
    expect(decided.resume.summary).toMatchObject({
      status: 'success',
      result: { outcome: 'declined', delivered: 0 },
    });
    expect(
      audit.events().some((event) => event.resource === OUTREACH_CONNECTOR),
    ).toBe(false);
  });
});
