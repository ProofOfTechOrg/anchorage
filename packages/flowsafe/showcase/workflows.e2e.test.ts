// Per-workflow regression guards for the four non-gtm showcase modules, driven
// in-process on the real Anchorage seams (DO runner + approval queue + host-kit
// bridge + breakwater connectors) with in-memory stores. Proves each module's
// distinctive capability end to end: content-pipeline parallel fan-in + R2
// write, lead-generation branch routing, product-launch's two gates re-queued
// through host-kit, access-request's grant + cross-workflow isolation denial.
// gtm-outbound keeps its own guard in worker.e2e.test.ts.

import { InMemoryStore } from '@mastra/core/storage';
import { AuditLogger } from '@proofoftech/breakwater';
import { describe, expect, it } from 'vitest';

import {
  type ApprovalActor,
  ApprovalService,
  InMemoryApprovalStore,
  resumeViaRuntime,
} from '../src/approval-api/index.js';
import { InMemoryArtifactBucket } from '../src/artifacts/index.js';
import type { RunSummary } from '../src/do-runner/index.js';
import {
  queueApprovalForSuspension,
  resumeRunWithRequeue,
} from '../src/host-kit/index.js';
import { buildShowcaseRuntime } from './runtime.js';
import { ACCESS_CONNECTOR } from './workflows/access-request.js';
import { PUBLISH_CONNECTOR } from './workflows/content-pipeline.js';
import { CRM_ASSIGN_CONNECTOR } from './workflows/lead-generation.js';
import { DEPLOY_CONNECTOR } from './workflows/product-launch.js';

const SYSTEM: ApprovalActor = { id: 'sys', role: 'operator' };
const REVIEWER: ApprovalActor = { id: 'ray', role: 'reviewer' };
const REVIEWER2: ApprovalActor = { id: 'rhea', role: 'reviewer' };

function buildHarness() {
  const bucket = new InMemoryArtifactBucket();
  const store = new InMemoryApprovalStore();
  const audit = new AuditLogger();
  const runtime = buildShowcaseRuntime({
    initInput: { storage: new InMemoryStore() },
    approvalStore: store,
    audit,
    artifactBucket: bucket,
    // crm/deploy egress left unset => those connectors simulate
  });
  // resumeRunWithRequeue so a multi-gate run auto-queues its next gate (the
  // product-launch flow); single-gate runs simply resume to success.
  const service: ApprovalService = new ApprovalService({
    store,
    resumeRun: resumeRunWithRequeue(
      resumeViaRuntime(runtime),
      () => service,
      SYSTEM,
    ),
  });
  return { runtime, service, store, audit, bucket };
}

/** Queue the approval bound to a run's current suspension, then decide it. */
async function decideCurrent(
  harness: ReturnType<typeof buildHarness>,
  workflowId: string,
  summary: RunSummary,
  actor: ApprovalActor,
  decision: 'approve' | 'reject' = 'approve',
) {
  const record = await queueApprovalForSuspension(
    harness.service,
    workflowId,
    summary,
    'starter',
    SYSTEM,
  );
  return harness.service.decide(record.id, { decision }, actor);
}

describe('content-pipeline: parallel fan-in, gate, idempotent R2 publish', () => {
  it('approve mints the grant, the parallel article is assembled and written to R2', async () => {
    const harness = buildHarness();
    const started = await harness.runtime.start('content-pipeline', {
      inputData: { topic: 'durable workflows' },
    });
    // #then — the gate suspends AFTER the parallel fan-in, as a bare id
    expect(started.status).toBe('suspended');
    expect(started.suspended).toEqual([['reviewContent']]);

    const decided = await decideCurrent(
      harness,
      'content-pipeline',
      started,
      REVIEWER,
    );
    const summary = decided.resume.summary as RunSummary;
    expect(summary.status).toBe('success');
    const result = summary.result as { published: boolean; key: string };
    expect(result.published).toBe(true);
    // #then — the article landed in R2 under workflowId/runId/name
    expect(result.key).toBe(`content-pipeline/${started.runId}/article.md`);
    expect(await harness.bucket.get(result.key)).not.toBeNull();
    expect(harness.audit.events()).toContainEqual(
      expect.objectContaining({
        resource: PUBLISH_CONNECTOR,
        decision: 'allowed',
      }),
    );
  });

  it('two runs with identical content each publish to their OWN R2 key (no cross-run idempotency collision)', async () => {
    // #given — the same topic launched twice produces byte-identical content;
    // the idempotency key must still be scoped per-run so run 2 is not silently
    // replayed as run 1's write (the debugger's HIGH-impact finding).
    const harness = buildHarness();
    const run1 = await harness.runtime.start('content-pipeline', {
      inputData: { topic: 'durable workflows' },
    });
    const d1 = await decideCurrent(harness, 'content-pipeline', run1, REVIEWER);
    const key1 = ((d1.resume.summary as RunSummary).result as { key: string })
      .key;

    const run2 = await harness.runtime.start('content-pipeline', {
      inputData: { topic: 'durable workflows' },
    });
    const d2 = await decideCurrent(harness, 'content-pipeline', run2, REVIEWER);
    const key2 = ((d2.resume.summary as RunSummary).result as { key: string })
      .key;

    // #then — distinct per-run keys, and BOTH are actually written (run 2 is not
    // a replay reporting run 1's key while writing nothing)
    expect(key1).toBe(`content-pipeline/${run1.runId}/article.md`);
    expect(key2).toBe(`content-pipeline/${run2.runId}/article.md`);
    expect(key1).not.toBe(key2);
    expect(await harness.bucket.get(key1)).not.toBeNull();
    expect(await harness.bucket.get(key2)).not.toBeNull();
  });

  it('fails closed: a forged resume finds no grant and never publishes', async () => {
    const harness = buildHarness();
    const started = await harness.runtime.start('content-pipeline', {
      inputData: { topic: 'durable workflows' },
    });
    const forged = await harness.runtime.resume(
      'content-pipeline',
      started.runId,
      {
        step: 'reviewContent',
        resumeData: { approved: true },
      },
    );
    expect(forged.status).toBe('failed');
    expect(harness.audit.events()).toContainEqual(
      expect.objectContaining({
        resource: PUBLISH_CONNECTOR,
        decision: 'denied',
      }),
    );
    // nothing was written
    expect(
      await harness.bucket.get(`content-pipeline/${started.runId}/article.md`),
    ).toBeNull();
  });
});

describe('lead-generation: branch routing then gated CRM assign', () => {
  it('routes hot and cold, gates on the hot count, assigns on approve', async () => {
    const harness = buildHarness();
    const started = await harness.runtime.start('lead-generation', {
      inputData: {
        leads: [
          {
            name: 'Dana Ito',
            title: 'VP Engineering',
            company: 'Acme',
            companySize: 400,
          },
          {
            name: 'Lee Poe',
            title: 'Engineer',
            company: 'Globex',
            companySize: 40,
          },
        ],
      },
    });
    expect(started.status).toBe('suspended');
    expect(started.suspended).toEqual([['reviewHotLeads']]);
    // #then — branch routed exactly one lead to fastTrack (the hot one)
    const payload = started.suspendPayload as {
      reviewHotLeads: { hotCount: number };
    };
    expect(payload.reviewHotLeads.hotCount).toBe(1);

    const decided = await decideCurrent(
      harness,
      'lead-generation',
      started,
      REVIEWER,
    );
    const summary = decided.resume.summary as RunSummary;
    expect(summary.status).toBe('success');
    // simulated (no CRM binding) but the assign connector DID run under the grant
    expect(summary.result).toMatchObject({ outcome: 'simulated', assigned: 0 });
  });

  it('an all-cold batch completes without opening an approval or calling the connector', async () => {
    // #given — no hot leads: nothing to assign
    const harness = buildHarness();
    const started = await harness.runtime.start('lead-generation', {
      inputData: {
        leads: [
          {
            name: 'Lee Poe',
            title: 'Engineer',
            company: 'Globex',
            companySize: 40,
          },
        ],
      },
    });
    // #then — the gate short-circuits: the run completes directly (no suspend),
    // no approval is queued, and the rate-limited CRM connector is never called
    // (which would otherwise burn budget on an empty batch).
    expect(started.status).toBe('success');
    expect(started.result).toMatchObject({ outcome: 'declined', assigned: 0 });
    expect(await harness.store.list({ status: 'pending' })).toHaveLength(0);
    expect(
      harness.audit.events().some((e) => e.resource === CRM_ASSIGN_CONNECTOR),
    ).toBe(false);
  });
});

describe('product-launch: two approval gates re-queued through host-kit', () => {
  it('clears both gates (SoD across gates) and completes, with a dry-run pre-flight', async () => {
    const harness = buildHarness();
    const started = await harness.runtime.start('product-launch', {
      inputData: { productName: 'anchorage', version: '1.0.0' },
    });
    expect(started.suspended).toEqual([['approveLaunch']]);
    // #then — validateReadiness dry-ran the deploy connector before the gate
    expect(harness.audit.events()).toContainEqual(
      expect.objectContaining({
        resource: DEPLOY_CONNECTOR,
        decision: 'allowed',
        detail: expect.objectContaining({ dryRun: true }),
      }),
    );

    // gate 1 — the reviewer approves; the run re-suspends at gate 2, which the
    // host-kit re-queue auto-enqueues attributed to that reviewer.
    const decided1 = await decideCurrent(
      harness,
      'product-launch',
      started,
      REVIEWER,
    );
    const g2 = decided1.resume.summary as RunSummary;
    expect(g2.status).toBe('suspended');
    expect(g2.suspended).toEqual([['confirmRollout']]);

    // gate 2 was auto-queued, attributed to REVIEWER (who advanced the run), so a
    // DIFFERENT reviewer must decide it (SoD across gates).
    const [gate2] = await harness.store.list({ status: 'pending' });
    if (!gate2) throw new Error('expected gate 2 approval to be queued');
    expect(gate2.requestedBy).toBe(REVIEWER.id);
    await expect(
      harness.service.decide(gate2.id, { decision: 'approve' }, REVIEWER),
    ).rejects.toThrow(/separation of duties|requester cannot decide/i);

    const decided2 = await harness.service.decide(
      gate2.id,
      { decision: 'approve' },
      REVIEWER2,
    );
    const final = decided2.resume.summary as RunSummary;
    expect(final.status).toBe('success');
    expect(final.result).toMatchObject({ healthy: true, outcome: 'simulated' });
  });
});

describe('access-request: gated grant with cross-workflow isolation', () => {
  it('grants access on approve', async () => {
    const harness = buildHarness();
    const started = await harness.runtime.start('access-request', {
      inputData: {
        resource: 'prod-database',
        role: 'reader',
        justification: 'on-call debugging',
      },
    });
    expect(started.suspended).toEqual([['approveAccess']]);
    const decided = await decideCurrent(
      harness,
      'access-request',
      started,
      REVIEWER,
    );
    const summary = decided.resume.summary as RunSummary;
    expect(summary.status).toBe('success');
    expect(summary.result).toMatchObject({
      granted: true,
      resource: 'prod-database',
    });
    expect(harness.audit.events()).toContainEqual(
      expect.objectContaining({
        resource: ACCESS_CONNECTOR,
        decision: 'allowed',
      }),
    );
  });

  it('cross-workflow isolation denies a request scoped to another workflow', async () => {
    const harness = buildHarness();
    const started = await harness.runtime.start('access-request', {
      inputData: {
        resource: 'prod-database',
        role: 'reader',
        justification: 'j',
        targetScope: 'some-other-workflow',
      },
    });
    // even after approval, the isolation gate denies at grantAccess (the target
    // scope is not this workflow's) — fail closed.
    const decided = await decideCurrent(
      harness,
      'access-request',
      started,
      REVIEWER,
    );
    const summary = decided.resume.summary as RunSummary;
    expect(summary.status).toBe('failed');
    expect(harness.audit.events()).toContainEqual(
      expect.objectContaining({
        resource: ACCESS_CONNECTOR,
        decision: 'denied',
      }),
    );
  });
});
