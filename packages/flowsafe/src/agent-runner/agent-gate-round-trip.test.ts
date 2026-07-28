// SPDX-License-Identifier: Apache-2.0
// Track A acceptance criteria #2 + #3, proven against the REAL runtime +
// breakwater connector + grant provider + host-kit bridge (no LLM needed):
//
//   - S1/S2: the ENGINE-LEG requestContext reaches the connector, so an approved
//     agent gate mints the grant the write gate demands and the run completes;
//     a FORGED resume that mints no grant fails closed at that same gate.
//   - R-003: BOTH durable approval-suspend shapes (flat + nested) round-trip
//     through the grant-only path — the bridge derives connectors:[toolName]
//     from the agent shape (not an explicit `connectors` array), and the
//     decision resumes on the (suspendedAt, resumeCount) fingerprint.
//
// This is the composition S1 verified in dist made executable: the workflow
// here MIMICS the durable-agentic-loop's tool-call gate (suspend with the agent
// payload shape, then call a write-gated connector using the step-param
// requestContext), which is exactly the mechanic the real loop uses
// (agent/durable index.js: params.requestContext -> toolOptions -> tool.execute).

import { InMemoryStore } from '@mastra/core/storage';
import type { ToolExecutionContext } from '@mastra/core/tools';
import { AuditLogger, createConnector } from '@proofoftech/breakwater';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';
import type { ApprovalActor } from '../approval-api/contract.js';
import {
  approvalGrantProvider,
  resumeViaRuntime,
} from '../approval-api/grants.js';
import type { ExecutionPrincipal } from '../approval-api/principal.js';
import { ApprovalService } from '../approval-api/service.js';
import { InMemoryApprovalStore } from '../approval-api/store.js';
import { init } from '../do-runner/init.js';
import { queueApprovalForSuspension } from '../host-kit/approval-bridge.js';

const SYSTEM: ExecutionPrincipal = {
  kind: 'system',
  id: 'sys',
  tenantId: 'acme',
  purpose: 'test-reconcile',
};
const REVIEWER: ApprovalActor = {
  id: 'rev',
  role: 'reviewer',
  tenantId: 'acme',
};
const STARTER = 'starter'; // the human who advanced the run (requestedBy)

const CONNECTOR_ID = 'blog-publisher';

// The two durable approval-suspend shapes (verbatim from @mastra/core dist).
const flatShape = (topic: string) => ({
  type: 'approval' as const,
  toolCallId: 'call-1',
  toolName: CONNECTOR_ID,
  args: { topic },
});
const nestedShape = (topic: string) => ({
  type: 'approval' as const,
  requireToolApproval: {
    toolCallId: 'call-1',
    toolName: CONNECTOR_ID,
    args: { topic },
  },
});

function buildHarness() {
  const storage = new InMemoryStore();
  const store = new InMemoryApprovalStore('acme');
  const connectorAudit = new AuditLogger();
  let publishes = 0;

  const publisher = createConnector<{ topic: string }, { published: boolean }>({
    id: CONNECTOR_ID,
    description: 'Publishes the launch post',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ published: z.boolean() }),
    permissions: { sideEffect: 'write', requiresApproval: true },
    policies: { audit: connectorAudit },
    execute: async () => {
      publishes += 1;
      return { published: true };
    },
  });

  // Build a runtime over the SHARED storage + approval store + connector. Called
  // once below; the eviction test calls it AGAIN to model a fresh post-eviction
  // isolate (empty in-process resume ledger, gone Mastra/registry) reattaching to
  // the SAME durable snapshot already persisted in `storage`.
  const makeRuntime = () => {
    const { createWorkflow, createStep, runtime } = init(
      { storage },
      { requestContextForRun: approvalGrantProvider(store) },
    );

    // The agent tool-call gate: suspends with the AGENT payload shape (chosen per
    // run via inputData.shape), then — on an approved resume — calls the connector
    // using the ENGINE-LEG requestContext from step params.
    const gate = createStep({
      id: 'gate',
      inputSchema: z.object({
        topic: z.string(),
        shape: z.enum(['flat', 'nested']),
      }),
      outputSchema: z.object({ topic: z.string(), approved: z.boolean() }),
      suspendSchema: z.object({
        type: z.literal('approval'),
        toolCallId: z.string().optional(),
        toolName: z.string().optional(),
        args: z.record(z.string(), z.unknown()).optional(),
        requireToolApproval: z
          .object({
            toolCallId: z.string().optional(),
            toolName: z.string().optional(),
            args: z.record(z.string(), z.unknown()).optional(),
          })
          .optional(),
      }),
      resumeSchema: z.object({
        approved: z.boolean(),
        comment: z.string().optional(),
        decidedBy: z.string().optional(),
      }),
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) {
          const payload =
            inputData.shape === 'nested'
              ? nestedShape(inputData.topic)
              : flatShape(inputData.topic);
          return suspend(payload);
        }
        return { topic: inputData.topic, approved: resumeData.approved };
      },
    });

    const use = createStep({
      id: 'use',
      inputSchema: z.object({ topic: z.string(), approved: z.boolean() }),
      outputSchema: z.object({ published: z.boolean() }),
      execute: async ({ inputData, requestContext }) => {
        // A rejected gate resumes the run but skips the side effect — no grant is
        // needed or minted (the run completes cleanly, nothing published).
        if (!inputData.approved) return { published: false };
        if (!publisher.execute) throw new Error('connector has no execute');
        return (await publisher.execute({ topic: inputData.topic }, {
          requestContext,
        } as unknown as ToolExecutionContext)) as { published: boolean };
      },
    });

    createWorkflow({
      id: 'agent-launch',
      inputSchema: z.object({
        topic: z.string(),
        shape: z.enum(['flat', 'nested']),
      }),
      outputSchema: z.object({ published: z.boolean() }),
    })
      .then(gate)
      .then(use)
      .commit();

    return runtime;
  };

  const runtime = makeRuntime();
  const service = new ApprovalService({
    store,
    resumeRun: resumeViaRuntime(runtime),
  });

  return {
    runtime,
    makeRuntime,
    service,
    store,
    publishes: () => publishes,
    connectorAudit,
  };
}

describe('agent gate grant round-trip (R-003, both shapes)', () => {
  for (const shape of ['flat', 'nested'] as const) {
    it(`${shape} shape: an approved agent gate mints the grant and the connector runs`, async () => {
      // #given — a run suspended at the agent tool-call gate
      const h = buildHarness();
      const started = await h.runtime.start('agent-launch', {
        runId: 'acme_run1',
        inputData: { topic: 'ship-it', shape },
      });
      expect(started.status).toBe('suspended');

      // #when — the suspension is bridged to an approval record. The bridge must
      // derive the connector to grant FROM the agent suspend shape (R-003),
      // since the payload carries no explicit `connectors` array.
      const [record] = await queueApprovalForSuspension(
        h.service,
        'agent-launch',
        started,
        STARTER,
        SYSTEM,
      );
      // #then — connectors:[toolName] was parsed out of the agent shape
      expect(record?.connectors).toEqual([CONNECTOR_ID]);

      // #when — the reviewer approves; decide() resumes via the runtime, whose
      // provider derives the grant from the now-approved record on the resumed
      // step's (suspendedAt, resumeCount) fingerprint
      const decided = await h.service.decide(
        record?.id ?? '',
        { decision: 'approve' },
        REVIEWER,
      );

      // #then — the engine-leg grant reached the connector; the run completed
      expect(decided.resume).toMatchObject({ attempted: true, ok: true });
      expect(decided.resume.summary).toMatchObject({
        status: 'success',
        result: { published: true },
      });
      expect(h.publishes()).toBe(1);
      expect(h.connectorAudit.events()).toContainEqual(
        expect.objectContaining({
          resource: CONNECTOR_ID,
          decision: 'allowed',
        }),
      );
    });
  }

  it('fails closed: a forged resume of an agent gate mints no grant and the connector denies', async () => {
    // #given — a suspended agent gate, nothing approved
    const h = buildHarness();
    const started = await h.runtime.start('agent-launch', {
      runId: 'acme_run2',
      inputData: { topic: 'ship-it', shape: 'flat' },
    });
    expect(started.status).toBe('suspended');

    // #when — an "approved" resume forged straight at the runtime, bypassing decide()
    const resumed = await h.runtime.resume('agent-launch', started.runId, {
      step: 'gate',
      resumeData: { approved: true },
    });

    // #then — no grant, the write gate denies, nothing published
    expect(resumed.status).toBe('failed');
    expect(resumed.error).toContain('approval required and not granted');
    expect(h.publishes()).toBe(0);
    expect(h.connectorAudit.events()).toContainEqual(
      expect.objectContaining({ resource: CONNECTOR_ID, decision: 'denied' }),
    );
  });

  it('self-decision is denied at the bridge (SoD): the requester cannot approve their own gate', async () => {
    // #given — the human who advanced the run is a reviewer
    const h = buildHarness();
    const started = await h.runtime.start('agent-launch', {
      runId: 'acme_run3',
      inputData: { topic: 'ship-it', shape: 'flat' },
    });
    const [record] = await queueApprovalForSuspension(
      h.service,
      'agent-launch',
      started,
      REVIEWER.id, // requestedBy === the would-be decider
      SYSTEM,
    );

    // #when / #then — separation of duties refuses the self-decision, nothing runs
    await expect(
      h.service.decide(record?.id ?? '', { decision: 'approve' }, REVIEWER),
    ).rejects.toThrow(/separation of duties/i);
    expect(h.publishes()).toBe(0);
  });

  it('reject: the decision resumes the run, mints no grant, and the connector is skipped', async () => {
    // #given — a suspended agent gate with a queued approval
    const h = buildHarness();
    const started = await h.runtime.start('agent-launch', {
      runId: 'acme_run4',
      inputData: { topic: 'ship-it', shape: 'flat' },
    });
    const [record] = await queueApprovalForSuspension(
      h.service,
      'agent-launch',
      started,
      STARTER,
      SYSTEM,
    );

    // #when — the reviewer rejects
    const decided = await h.service.decide(
      record?.id ?? '',
      { decision: 'reject' },
      REVIEWER,
    );

    // #then — the run resumes to completion (learns the outcome), the gated
    // connector is skipped, and no grant was ever minted (rejected record)
    expect(decided.record.status).toBe('rejected');
    expect(decided.resume).toMatchObject({ attempted: true, ok: true });
    expect(decided.resume.summary).toMatchObject({
      status: 'success',
      result: { published: false },
    });
    expect(h.publishes()).toBe(0);
    expect(h.connectorAudit.events()).toEqual([]);
  });

  it('eviction fail-closed: a resume on a FRESH runtime re-derives the grant from durable state (barrier 1)', async () => {
    // The DO-eviction signature made no-LLM-testable. A run suspends on
    // runtime1 (its (suspendedAt, resumeCount) fingerprint + snapshot persisted
    // to the SHARED durable storage), then the isolate is REPLACED: `evicted` is
    // a fresh RunnerRuntime over the SAME storage + approval store, with an empty
    // in-process resume ledger and no in-process run registry — exactly what a
    // reattaching DO sees after eviction (runtime.resume's "designed
    // fresh-process pattern"). The grant is RE-DERIVED from durable state (the
    // snapshot's suspendedAt read by suspendedAtOf + the APPROVED store record),
    // never any in-process registry, so the approved connector still runs.
    //
    // This pins BARRIER 1: the capability survives eviction because it is
    // durable-state derived. BARRIER 2 — the REAL durable-agentic-loop
    // additionally cannot resolve the tool's execute closure from the emptied
    // globalRunRegistry on a post-eviction resume (ToolNotFoundError against the
    // runtime's workflow-only Mastra — runtime.ts #ensureMastra registers NO
    // tools/agents — fail-closing BEFORE tool.execute) — needs the LLM-driven
    // loop to place a real tool call, so it stays the deferred S3 spike (the
    // scope note in durable-agent-runner.ts). This harness's `use` step closes
    // over the connector instead of resolving it from the registry, so it cannot
    // exercise barrier 2 without an LLM; it proves the grant-survives-eviction
    // half that IS testable.
    const h = buildHarness();

    // #given — runtime1 suspends the agent gate and durably persists the snapshot
    const started = await h.runtime.start('agent-launch', {
      runId: 'acme_evict1',
      inputData: { topic: 'ship-it', shape: 'flat' },
    });
    expect(started.status).toBe('suspended');

    // #when — the isolate is EVICTED: a fresh runtime over the SAME durable
    // storage + store handles the approval-driven resume
    const evicted = h.makeRuntime();
    const service = new ApprovalService({
      store: h.store,
      resumeRun: resumeViaRuntime(evicted),
    });
    const [record] = await queueApprovalForSuspension(
      service,
      'agent-launch',
      started,
      STARTER,
      SYSTEM,
    );
    expect(record?.connectors).toEqual([CONNECTOR_ID]);
    const decided = await service.decide(
      record?.id ?? '',
      { decision: 'approve' },
      REVIEWER,
    );

    // #then — the fresh isolate re-derived the grant from durable state; the
    // connector ran (grant survived eviction) and the run completed
    expect(decided.resume).toMatchObject({ attempted: true, ok: true });
    expect(decided.resume.summary).toMatchObject({
      status: 'success',
      result: { published: true },
    });
    expect(h.publishes()).toBe(1);
    expect(h.connectorAudit.events()).toContainEqual(
      expect.objectContaining({ resource: CONNECTOR_ID, decision: 'allowed' }),
    );
  });
});
