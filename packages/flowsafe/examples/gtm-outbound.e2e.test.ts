// Runnable example: the docs/examples/gtm-outbound.ts SKETCH, made to actually
// execute end-to-end on Anchorage's real seams — no Cloudflare, no wrangler.
//
// Chain: research -> enrich -> generate -> reviewAndApprove (SUSPENDS) ->
// sendOutreach, where sendOutreach calls a REAL breakwater createConnector whose
// write gate (permissions.requiresApproval) admits the call ONLY when flowsafe's
// approval grant is present. The grant is minted server-side by
// approvalGrantProvider from the APPROVED approval record on resume — it never
// crosses a boundary.
//
// Two deltas vs the sketch (both mirror src/approval-api/end-to-end.test.ts):
//   1. reviewAndApprove becomes a real suspend gate (suspend/resume schemas).
//   2. a new sendOutreach step performs the gated write via the connector.
//
// Run:  pnpm --filter @proofoftech/flowsafe example:gtm
// Also runs under `pnpm --filter @proofoftech/flowsafe test` as a regression guard.

import { InMemoryStore } from '@mastra/core/storage';
import type { ToolExecutionContext } from '@mastra/core/tools';
import { AuditLogger, createConnector } from '@proofoftech/breakwater';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

// Barrels (not deep paths): these map 1:1 to the published subpaths a real
// consumer imports, and mirror spike/worker.ts's wiring.
import {
  type ApprovalActor,
  ApprovalService,
  approvalGrantProvider,
  InMemoryApprovalStore,
  resumeViaRuntime,
} from '../src/approval-api/index.js';
import {
  init,
  type RunnerRuntime,
  type RunSummary,
} from '../src/do-runner/index.js';

const OUTREACH_CONNECTOR = 'outreach-sender';
// Separation of duties: the operator who requests is NOT the reviewer who
// decides (ApprovalService denies self-decision by default).
const OPERATOR: ApprovalActor = {
  id: 'opal',
  role: 'operator',
  tenantId: 'acme',
};
const REVIEWER: ApprovalActor = {
  id: 'ray',
  role: 'reviewer',
  tenantId: 'acme',
};

interface Harness {
  runtime: RunnerRuntime;
  service: ApprovalService;
  sends: () => number;
  audit: AuditLogger;
}

function buildHarness(): Harness {
  const store = new InMemoryApprovalStore('acme');
  const audit = new AuditLogger();
  let sends = 0;

  // The gated write. requiresApproval => the minted grant is the only token that
  // admits execution; no grant => the write gate throws (fail closed).
  const outreachSender = createConnector<{ count: number }, { sent: boolean }>({
    id: OUTREACH_CONNECTOR,
    description: 'Sends the approved outreach batch',
    inputSchema: z.object({ count: z.number() }),
    outputSchema: z.object({ sent: z.boolean() }),
    permissions: { sideEffect: 'write', requiresApproval: true },
    policies: { audit },
    execute: async () => {
      sends += 1;
      return { sent: true };
    },
  });

  // init() import-swap: createWorkflow/createStep are backend-bound; the runtime
  // mints requestContext from approved records on every start/resume.
  const { createWorkflow, createStep, runtime } = init(
    { storage: new InMemoryStore() },
    { requestContextForRun: approvalGrantProvider(store) },
  );

  const researchAccounts = createStep({
    id: 'researchAccounts',
    inputSchema: z.object({
      industry: z.string(),
      targetCount: z.number().default(50),
    }),
    outputSchema: z.object({ accounts: z.array(z.string()) }),
    execute: async ({ inputData }) => {
      console.log(
        `  research: scanning ${inputData.industry} for ~${inputData.targetCount} accounts`,
      );
      return { accounts: ['Acme Corp', 'Globex Inc'] };
    },
  });

  const enrichContacts = createStep({
    id: 'enrichContacts',
    inputSchema: z.object({ accounts: z.array(z.string()) }),
    outputSchema: z.object({ contacts: z.array(z.string()) }),
    execute: async ({ inputData }) => ({
      contacts: inputData.accounts.map(
        (account) =>
          `head-of-growth@${account.toLowerCase().replace(/\s+/g, '')}.com`,
      ),
    }),
  });

  const generateOutreach = createStep({
    id: 'generateOutreach',
    inputSchema: z.object({ contacts: z.array(z.string()) }),
    outputSchema: z.object({ drafts: z.array(z.string()) }),
    execute: async ({ inputData }) => ({
      drafts: inputData.contacts.map((contact) => `Hi ${contact}, ...`),
    }),
  });

  // EDIT 1 vs sketch: a real suspend gate. resumeSchema MUST match
  // defaultResumeData() = { approved, comment?, decidedBy? } (grants.ts).
  const reviewAndApprove = createStep({
    id: 'reviewAndApprove',
    inputSchema: z.object({ drafts: z.array(z.string()) }),
    outputSchema: z.object({
      drafts: z.array(z.string()),
      approved: z.boolean(),
    }),
    suspendSchema: z.object({ reason: z.string(), draftCount: z.number() }),
    resumeSchema: z.object({
      approved: z.boolean(),
      comment: z.string().optional(),
      decidedBy: z.string().optional(),
    }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        return suspend({
          reason: 'human approval required before send',
          draftCount: inputData.drafts.length,
        });
      }
      return { drafts: inputData.drafts, approved: resumeData.approved };
    },
  });

  // EDIT 2 vs sketch: the gated write. Forward the runtime-supplied
  // requestContext into the connector; never set the grant key by hand — the
  // provider mints it, which is the boundary this example demonstrates.
  const sendOutreach = createStep({
    id: 'sendOutreach',
    inputSchema: z.object({
      drafts: z.array(z.string()),
      approved: z.boolean(),
    }),
    outputSchema: z.object({ sent: z.boolean(), count: z.number() }),
    execute: async ({ inputData, requestContext }) => {
      if (!inputData.approved) return { sent: false, count: 0 };
      if (!outreachSender.execute) throw new Error('connector has no execute');
      const { sent } = (await outreachSender.execute(
        { count: inputData.drafts.length },
        {
          requestContext,
        } as unknown as ToolExecutionContext,
      )) as { sent: boolean };
      return { sent, count: inputData.drafts.length };
    },
  });

  createWorkflow({
    id: 'gtm-outbound',
    inputSchema: z.object({
      industry: z.string(),
      targetCount: z.number().default(50),
    }),
    outputSchema: z.object({ sent: z.boolean(), count: z.number() }),
  })
    .then(researchAccounts)
    .then(enrichContacts)
    .then(generateOutreach)
    .then(reviewAndApprove)
    .then(sendOutreach)
    .commit();

  const service = new ApprovalService({
    store,
    resumeRun: resumeViaRuntime(runtime),
  });
  return { runtime, service, sends: () => sends, audit };
}

describe('example: gtm-outbound runs end to end on real Anchorage seams', () => {
  it('suspends at approval, mints the grant on approve, and sends', async () => {
    // #given — a started run that suspends at the approval gate
    const harness = buildHarness();
    const started = await harness.runtime.start('gtm-outbound', {
      runId: `acme_${crypto.randomUUID()}`,
      inputData: { industry: 'fintech', targetCount: 50 },
    });
    console.log(`[1] started run ${started.runId} -> status=${started.status}`);
    expect(started.status).toBe('suspended');
    expect(started.suspended).toEqual([['reviewAndApprove']]);

    const stepPath = started.suspended?.[0];
    if (!stepPath) throw new Error('expected a suspended step');

    // Bind the approval to THIS suspension by its exact timestamp — the
    // clock-free grant binding (grants.ts). Guard it: were the summary ever to
    // stop carrying suspendedAt, minting would silently fall back to the legacy
    // decidedAt-after path, and this example (which uses no clock settling)
    // would start flaking. Fail loudly instead.
    const suspendedAt = started.suspendedAt?.[stepPath.join('.')];
    if (typeof suspendedAt !== 'number') {
      throw new Error('expected a numeric suspendedAt for exact-match binding');
    }

    // #given — the suspension is queued as an approval carrying the connector
    // id it grants; the requester defaults to the creating actor (opal).
    const { record } = await harness.service.create(
      {
        workflowId: 'gtm-outbound',
        runId: started.runId,
        stepPath,
        suspendedAt,
        title: 'Approve GTM outreach send',
        payload: started.suspendPayload,
        connectors: [OUTREACH_CONNECTOR],
      },
      OPERATOR,
    );
    console.log(
      `[2] queued approval ${record.id} (requested by ${OPERATOR.id})`,
    );

    // #when — a different actor approves; decide() resumes via the runtime, whose
    // provider derives the grant from the now-approved record
    const decided = await harness.service.decide(
      record.id,
      { decision: 'approve', comment: 'lgtm' },
      REVIEWER,
    );
    const summary = decided.resume.summary as RunSummary | undefined;
    console.log(
      `[3] ${REVIEWER.id} approved -> resume ok=${decided.resume.ok}, result=`,
      summary?.result,
    );

    // #then — the grant admitted the gated send; the run completed
    expect(decided.resume).toMatchObject({ attempted: true, ok: true });
    expect(decided.resume.summary).toMatchObject({
      status: 'success',
      result: { sent: true, count: 2 },
    });
    expect(harness.sends()).toBe(1);
    expect(harness.audit.events()).toContainEqual(
      expect.objectContaining({
        resource: OUTREACH_CONNECTOR,
        decision: 'allowed',
      }),
    );
  });

  it('fails closed: a resume that bypasses approval finds no grant and the connector denies', async () => {
    // #given — a suspended run, nothing approved
    const harness = buildHarness();
    const started = await harness.runtime.start('gtm-outbound', {
      runId: `acme_${crypto.randomUUID()}`,
      inputData: { industry: 'fintech', targetCount: 50 },
    });
    expect(started.status).toBe('suspended');

    // #when — an "approved" resume forged straight at the runtime
    const forged = await harness.runtime.resume('gtm-outbound', started.runId, {
      step: 'reviewAndApprove',
      resumeData: { approved: true },
    });

    // #then — the write gate denies (no grant); nothing sent
    expect(forged.status).toBe('failed');
    expect(forged.error).toContain('approval required and not granted');
    expect(harness.sends()).toBe(0);
    expect(harness.audit.events()).toContainEqual(
      expect.objectContaining({
        resource: OUTREACH_CONNECTOR,
        decision: 'denied',
      }),
    );
  });
});
