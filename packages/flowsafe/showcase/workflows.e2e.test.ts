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
  approvalGrantProviderFromFactory,
  ApprovalService,
  createTenantResolver,
  type InMemoryApprovalStore,
  InMemoryApprovalStoreFactory,
  resumeViaRuntime,
} from '../src/approval-api/index.js';
import { InMemoryArtifactBucket } from '../src/artifacts/index.js';
import type { RunSummary } from '../src/do-runner/index.js';
import {
  createRunRouter,
  queueApprovalForSuspension,
  resumeRunWithRequeue,
} from '../src/host-kit/index.js';
import { buildShowcaseRuntime, SHOWCASE_MODULES } from './runtime.js';
import { ACCESS_CONNECTOR } from './workflows/access-request.js';
import { PUBLISH_CONNECTOR } from './workflows/content-pipeline.js';
import { CRM_ASSIGN_CONNECTOR } from './workflows/lead-generation.js';
import { DEPLOY_CONNECTOR } from './workflows/product-launch.js';

const SYSTEM: ApprovalActor = { id: 'sys', role: 'operator', tenantId: 'demo' };
const REVIEWER: ApprovalActor = {
  id: 'ray',
  role: 'reviewer',
  tenantId: 'demo',
};
const REVIEWER2: ApprovalActor = {
  id: 'rhea',
  role: 'reviewer',
  tenantId: 'demo',
};

function buildHarness() {
  const bucket = new InMemoryArtifactBucket();
  // ONE shared backend: the run router binds per request tenant, while the
  // in-process grant provider recovers each leg's tenant from its runId.
  const storeFactory = new InMemoryApprovalStoreFactory();
  const store = storeFactory.forTenant('demo') as InMemoryApprovalStore;
  const audit = new AuditLogger();
  const runtime = buildShowcaseRuntime({
    initInput: { storage: new InMemoryStore() },
    grantProvider: approvalGrantProviderFromFactory(storeFactory),
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
  return { runtime, service, store, storeFactory, audit, bucket };
}

/** Queue the approval bound to a run's current suspension, then decide it. */
async function decideCurrent(
  harness: ReturnType<typeof buildHarness>,
  workflowId: string,
  summary: RunSummary,
  actor: ApprovalActor,
  decision: 'approve' | 'reject' = 'approve',
) {
  const [record] = await queueApprovalForSuspension(
    harness.service,
    workflowId,
    summary,
    'starter',
    SYSTEM,
  );
  if (!record) throw new Error('expected the suspension to queue an approval');
  return harness.service.decide(record.id, { decision }, actor);
}

describe('content-pipeline: parallel fan-in, gate, idempotent R2 publish', () => {
  it('approve mints the grant, the parallel article is assembled and written to R2', async () => {
    const harness = buildHarness();
    const started = await harness.runtime.start('content-pipeline', {
      runId: `demo_${crypto.randomUUID()}`,
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
      runId: `demo_${crypto.randomUUID()}`,
      inputData: { topic: 'durable workflows' },
    });
    const d1 = await decideCurrent(harness, 'content-pipeline', run1, REVIEWER);
    const key1 = ((d1.resume.summary as RunSummary).result as { key: string })
      .key;

    const run2 = await harness.runtime.start('content-pipeline', {
      runId: `demo_${crypto.randomUUID()}`,
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
      runId: `demo_${crypto.randomUUID()}`,
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
      runId: `demo_${crypto.randomUUID()}`,
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
      runId: `demo_${crypto.randomUUID()}`,
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
      runId: `demo_${crypto.randomUUID()}`,
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

  it('a rejected deploy declines without a second gate, approval, or promote', async () => {
    // #given — the run suspends at gate 1 (deploy approval)
    const harness = buildHarness();
    const started = await harness.runtime.start('product-launch', {
      runId: `demo_${crypto.randomUUID()}`,
      inputData: { productName: 'anchorage', version: '1.0.0' },
    });
    expect(started.suspended).toEqual([['approveLaunch']]);

    // #when — the reviewer REJECTS gate 1
    const decided = await decideCurrent(
      harness,
      'product-launch',
      started,
      REVIEWER,
      'reject',
    );
    const summary = decided.resume.summary as RunSummary;

    // #then — the run completes as declined immediately: confirmRollout did not
    // suspend (status is success, not suspended — this alone fails pre-fix), no
    // second approval was queued, and the promote (phase:'promote') never ran.
    expect(summary.status).toBe('success');
    expect(summary.result).toMatchObject({
      healthy: false,
      outcome: 'declined',
    });
    expect(await harness.store.list({ status: 'pending' })).toHaveLength(0);
    // the only deploy-connector activity is the dry-run pre-flight — no real
    // (grant-gated) deploy and no promote executed
    expect(
      harness.audit
        .events()
        .some(
          (e) => e.resource === DEPLOY_CONNECTOR && e.detail?.dryRun !== true,
        ),
    ).toBe(false);
  });
});

describe('access-request: gated grant with cross-workflow isolation', () => {
  it('grants access on approve', async () => {
    const harness = buildHarness();
    const started = await harness.runtime.start('access-request', {
      runId: `demo_${crypto.randomUUID()}`,
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
      runId: `demo_${crypto.randomUUID()}`,
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

describe('tenant isolation: scope-less connector calls are denied, not silently shared', () => {
  // The runtime mints breakwater's isolation scope from the INV-1 runId
  // prefix; a runId without one (path-safe, but no `${tenant}_` shape) mints
  // NO scope. Without the tenantIsolation evaluator that call would fall back
  // to UNSEGMENTED idempotency/rate-limit keys — tenant B replaying tenant
  // A's cached result. Every showcase connector registers the evaluator, so
  // the fallback is a denial instead.

  it('denies the dry-run pre-flight of a scope-less run (the evaluator binds simulations too)', async () => {
    // #given — a run whose runId carries no tenant prefix
    const harness = buildHarness();

    // #when — product-launch's FIRST step dry-runs the deploy connector
    const started = await harness.runtime.start('product-launch', {
      runId: 'noscope-run',
      inputData: { productName: 'anchorage', version: '1.0.0' },
    });

    // #then — denied at the pre-execute gate, before any simulation
    expect(started.status).toBe('failed');
    expect(started.error).toContain('tenant-isolation');
    expect(harness.audit.events()).toContainEqual(
      expect.objectContaining({
        resource: DEPLOY_CONNECTOR,
        decision: 'denied',
      }),
    );
  });

  it('denies a scope-less gated write even when the resume forges approval', async () => {
    // #given — a suspended scope-less run (the gate itself calls no connector)
    const harness = buildHarness();
    const started = await harness.runtime.start('gtm-outbound', {
      runId: 'noscope-run2',
      inputData: { industry: 'fintech', targetCount: 5 },
    });
    expect(started.status).toBe('suspended');

    // #when — a forged approved:true resume reaches the send step
    const forged = await harness.runtime.resume(
      'gtm-outbound',
      'noscope-run2',
      {
        step: 'reviewAndApprove',
        resumeData: { approved: true },
      },
    );

    // #then — tenant-isolation denies ahead of the grant gate; nothing sent
    expect(forged.status).toBe('failed');
    expect(forged.error).toContain('tenant-isolation');
  });
});

// createRunRouter's gates are unit-tested against fixture metas in
// src/host-kit/run-router.test.ts. These drive it over the REAL showcase metas
// and the real runtime, so a module whose allowedRoles regresses (or whose id
// drifts from its committed workflow) fails here.
describe('showcase run routes', () => {
  function routerFor(
    harness: ReturnType<typeof buildHarness>,
    actor: ApprovalActor,
  ) {
    return createRunRouter({
      workflows: SHOWCASE_MODULES.map((entry) => entry.meta),
      resolve: createTenantResolver({
        authenticate: () => actor,
        storeFactory: harness.storeFactory,
        buildService: (store) =>
          new ApprovalService({
            store,
            resumeRun: resumeRunWithRequeue(
              resumeViaRuntime(harness.runtime),
              () => harness.service,
              SYSTEM,
            ),
          }),
      }),
      systemActorId: SYSTEM.id,
      start: (workflowId, runId, inputData) =>
        harness.runtime.start(workflowId, { runId, inputData }),
      status: async (workflowId, runId) =>
        (await harness.runtime.status(workflowId, runId)) ?? undefined,
      resume: (workflowId, runId, body) => {
        const { step, resumeData } = (body ?? {}) as {
          step?: string | string[];
          resumeData?: unknown;
        };
        return harness.runtime.resume(workflowId, runId, { step, resumeData });
      },
    });
  }

  function startRequest(workflowId: string, inputData: unknown): Request {
    return new Request('http://showcase.test/runs', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ workflowId, inputData }),
    });
  }

  const ACCESS_INPUT = {
    resource: 'prod-database',
    role: 'reader',
    justification: 'oncall',
    targetScope: 'access-request',
  };

  it('serves all five workflow metas at GET /workflows', async () => {
    // #given
    const harness = buildHarness();
    const handle = routerFor(harness, {
      id: 'vic',
      role: 'viewer',
      tenantId: 'demo',
    });

    // #when
    const response = await handle(
      new Request('http://showcase.test/workflows'),
    );

    // #then
    expect(response?.status).toBe(200);
    const { workflows } = (await response?.json()) as {
      workflows: Array<{ id: string }>;
    };
    expect(workflows.map((meta) => meta.id)).toEqual([
      'gtm-outbound',
      'content-pipeline',
      'lead-generation',
      'product-launch',
      'access-request',
    ]);
  });

  it('403s an operator starting access-request, whose meta allows admin/builder only', async () => {
    // #given — the operator clears the coarse start gate; the module's own
    // allowedRoles is the finer one
    const harness = buildHarness();
    const handle = routerFor(harness, {
      id: 'opal',
      role: 'operator',
      tenantId: 'demo',
    });

    // #when
    const response = await handle(startRequest('access-request', ACCESS_INPUT));

    // #then
    expect(response?.status).toBe(403);
    expect(await harness.store.list()).toEqual([]);
  });

  it('lets a builder start access-request, queuing an approval attributed to them', async () => {
    // #given
    const harness = buildHarness();
    const handle = routerFor(harness, {
      id: 'bo',
      role: 'builder',
      tenantId: 'demo',
    });

    // #when
    const response = await handle(startRequest('access-request', ACCESS_INPUT));

    // #then — the suspension became an approval the STARTER cannot decide
    expect(response?.status).toBe(200);
    expect(await response?.json()).toMatchObject({
      status: 'suspended',
      approval: { requestedBy: 'bo', connectors: [ACCESS_CONNECTOR] },
    });
  });

  it('403s a reviewer starting any workflow (coarse gate)', async () => {
    // #given
    const harness = buildHarness();
    const handle = routerFor(harness, REVIEWER);

    // #when / #then
    expect((await handle(startRequest('gtm-outbound', {})))?.status).toBe(403);
  });

  it('two tenants starting the same workflow get disjoint tenant-salted runs; neither can read the other (INV-1)', async () => {
    // #given — one shared runtime + store (worst case: everything colocated),
    // two tenants
    const harness = buildHarness();
    const alfa = routerFor(harness, {
      id: 'a1',
      role: 'operator',
      tenantId: 'alfa',
    });
    const bravo = routerFor(harness, {
      id: 'b1',
      role: 'operator',
      tenantId: 'bravo',
    });
    const input = { industry: 'fintech', targetCount: 2 };

    // #when — both start gtm-outbound
    const startedA = (await (
      await alfa(startRequest('gtm-outbound', input))
    )?.json()) as { runId: string };
    const startedB = (await (
      await bravo(startRequest('gtm-outbound', input))
    )?.json()) as { runId: string };

    // #then — each runId carries its tenant, so the DO name join
    // (`${workflowId}:${runId}`) and the snapshot key (workflow_name, run_id)
    // are tenant-disjoint BY CONSTRUCTION, with no schema change
    expect(startedA.runId.startsWith('alfa_')).toBe(true);
    expect(startedB.runId.startsWith('bravo_')).toBe(true);
    expect(startedA.runId).not.toBe(startedB.runId);

    // #then — each tenant reads its own run; the other tenant's probe 404s
    // on status AND resume (no existence oracle)
    expect(
      (await alfa(new Request(`http://x/runs/gtm-outbound/${startedA.runId}`)))
        ?.status,
    ).toBe(200);
    expect(
      (await bravo(new Request(`http://x/runs/gtm-outbound/${startedA.runId}`)))
        ?.status,
    ).toBe(404);
    expect(
      (
        await bravo(
          new Request(`http://x/runs/gtm-outbound/${startedA.runId}/resume`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({}),
          }),
        )
      )?.status,
    ).toBe(404);
  });
});
