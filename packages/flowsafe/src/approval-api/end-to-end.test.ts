// SPDX-License-Identifier: Apache-2.0
// Cross-package proof of the Phase 3 loop. breakwater is a devDependency
// resolved FROM SOURCE (vitest alias + tsconfig.test paths), so `pnpm -r
// test` never needs a built breakwater dist.
//
// Two duties:
// 1. Contract tripwires — flowsafe mirrors breakwater's requestContext keys
//    and role set by value (contract.ts explains why there is no runtime
//    dependency); these tests fail the suite if the packages drift.
// 2. End-to-end: a workflow suspends at an approval step, the queue decides,
//    resumeViaRuntime resumes, and the grant provider mints the breakwater
//    grant the write-gated connector demands — including the fail-closed
//    path where a resume that bypasses decide() finds no grant.

import { InMemoryStore } from '@mastra/core/storage';
import type { ToolExecutionContext } from '@mastra/core/tools';
import {
  ACTOR_CONTEXT_KEY,
  type Actor,
  AuditLogger,
  type ConnectorApprovalGrant as BreakwaterConnectorApprovalGrant,
  CONNECTOR_EXECUTION_CONTEXT_KEY,
  CONNECTOR_GRANTS_CONTEXT_KEY,
  createConnector,
  ISOLATION_SCOPE_CONTEXT_KEY,
  PRINCIPAL_KINDS,
  PRINCIPAL_PERMISSIONS_CONTEXT_KEY,
  ROLES,
  WORKFLOW_SCOPE_CONTEXT_KEY,
} from '@proofoftech/breakwater';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import {
  BREAKWATER_CONNECTOR_EXECUTION_KEY,
  BREAKWATER_ISOLATION_SCOPE_KEY,
} from '../do-runner/breakwater-keys.js';
import { init } from '../do-runner/init.js';
import type {
  RunLeg,
  RunnerRuntime,
  RunSummary,
} from '../do-runner/runtime.js';
import { createActorResolver } from './actor-context.js';
import type { ApprovalActor, ApprovalAuditSink } from './contract.js';
import {
  APPROVAL_ROLES,
  BREAKWATER_ACTOR_KEY,
  BREAKWATER_CONNECTOR_GRANTS_KEY,
  BREAKWATER_PRINCIPAL_PERMISSIONS_KEY,
  BREAKWATER_WORKFLOW_SCOPE_KEY,
  DECIDER_ROLES,
  RUN_START_ROLES,
} from './contract.js';
import {
  approvalGrantProvider,
  connectorGrantsForLeg,
  resumeViaRuntime,
} from './grants.js';
import {
  AUTOMATED_PRINCIPAL_KINDS,
  AUTOMATED_PROJECTED_ROLE,
  EXECUTION_PRINCIPAL_KINDS,
  trustAutomationPrincipal,
} from './principal.js';
import { InMemoryResourceOwnershipStore } from './resource-ownership.js';
import { createApprovalRouter } from './router.js';
import { ApprovalService, type ApprovalServiceOptions } from './service.js';
import { InMemoryApprovalStore } from './store.js';
import type { ConnectorApprovalGrant } from './types.js';

const OPERATOR: ApprovalActor = {
  id: 'opal',
  role: 'operator',
};
const REVIEWER: ApprovalActor = {
  id: 'ray',
  role: 'reviewer',
};
// admin holds BOTH CAN_CREATE and CAN_REVIEW — the single principal the
// create-route capability chain needed.
const ADMIN: ApprovalActor = { id: 'ada', role: 'admin' };

// The bridge's capture: the summary's suspension timestamp for a step.
// Accepts unknown because ResumeOutcome.summary is untyped wire data; the
// runtime check below is the safety.
function suspendedAtFor(summary: unknown, stepPath: readonly string[]): number {
  const at = (summary as RunSummary | undefined)?.suspendedAt?.[
    stepPath.join('.')
  ];
  if (typeof at !== 'number') {
    throw new Error(`no suspendedAt for step '${stepPath.join('.')}'`);
  }
  return at;
}

// The bridge's resumedAt capture (INFORMATIONAL). No throw: a step's FIRST
// suspension legitimately carries no resumedAt (undefined), and Mastra also
// omits it on a no-payload re-suspension — which is exactly why resumedAt is
// NOT the binding signal.
function resumedAtFor(
  summary: unknown,
  stepPath: readonly string[],
): number | undefined {
  const at = (summary as RunSummary | undefined)?.resumedAt?.[
    stepPath.join('.')
  ];
  return typeof at === 'number' ? at : undefined;
}

// The bridge's resumeCount capture — the grant-binding tie-breaker. Undefined
// for a step's FIRST suspension, 1,2,… on re-suspensions; unlike resumedAt the
// runtime sets it on every resume, so it is present even for a no-payload
// re-suspension.
function resumeCountFor(
  summary: unknown,
  stepPath: readonly string[],
): number | undefined {
  const count = (summary as RunSummary | undefined)?.resumeCount?.[
    stepPath.join('.')
  ];
  return typeof count === 'number' ? count : undefined;
}

describe('breakwater contract tripwires', () => {
  it('mirrors the structured connector-grants key literally', () => {
    expect(BREAKWATER_CONNECTOR_GRANTS_KEY).toBe(CONNECTOR_GRANTS_CONTEXT_KEY);
  });

  it('mirrors the runtime execution-identity key literally', () => {
    expect(BREAKWATER_CONNECTOR_EXECUTION_KEY).toBe(
      CONNECTOR_EXECUTION_CONTEXT_KEY,
    );
  });

  it('keeps the structured grant types mutually assignable', () => {
    const flowsafeGrant: ConnectorApprovalGrant = {
      scope: 'tool-call',
      connectorId: 'send-email',
      workflowId: 'workflow-1',
      runId: 'acme_run-1',
      suspension: {
        stepPath: ['agent-gate'],
        suspendedAt: 1_000,
        resumeCount: 2,
      },
      toolCallId: 'call-1',
    };
    const breakwaterGrant: BreakwaterConnectorApprovalGrant = flowsafeGrant;
    const roundTrip: ConnectorApprovalGrant = breakwaterGrant;

    expect(roundTrip).toBe(flowsafeGrant);
  });

  it('mirrors the actor key literally', () => {
    expect(BREAKWATER_ACTOR_KEY).toBe(ACTOR_CONTEXT_KEY);
  });

  it('mirrors the principal-permissions key literally', () => {
    expect(BREAKWATER_PRINCIPAL_PERMISSIONS_KEY).toBe(
      PRINCIPAL_PERMISSIONS_CONTEXT_KEY,
    );
  });

  it('mirrors the workflow-scope key literally', () => {
    expect(BREAKWATER_WORKFLOW_SCOPE_KEY).toBe(WORKFLOW_SCOPE_CONTEXT_KEY);
  });

  it('mirrors the isolation-scope key literally', () => {
    expect(BREAKWATER_ISOLATION_SCOPE_KEY).toBe(ISOLATION_SCOPE_CONTEXT_KEY);
  });

  it('mirrors the role set', () => {
    expect([...APPROVAL_ROLES]).toEqual([...ROLES]);
  });

  it('mirrors the tenant-agnostic breakwater Actor shape', () => {
    const flowsafeActor: ApprovalActor = {
      id: 'x',
      role: 'admin',
    };
    const widened: Actor = flowsafeActor;
    expect(widened).toMatchObject({ id: 'x', role: 'admin' });
  });

  it('mirrors breakwater PrincipalKind by value', () => {
    // #given / #when / #then — flowsafe does not import breakwater at runtime,
    // so this union is mirrored. Drift makes the catalog reject a kind
    // breakwater accepts, at host construction, with no test going red.
    expect([...EXECUTION_PRINCIPAL_KINDS]).toEqual([...PRINCIPAL_KINDS]);
    expect([...AUTOMATED_PRINCIPAL_KINDS]).toEqual(
      PRINCIPAL_KINDS.filter((kind) => kind !== 'human'),
    );
  });

  it('keeps the automated projected role out of every start gate', () => {
    // #given / #when / #then — the load-bearing invariant behind projecting an
    // inert role: nothing that gates on a HUMAN role may admit it. If this ever
    // fails, an automated principal reaching a role check is admitted without
    // its agent ever declaring allowedAutomation.
    expect(RUN_START_ROLES).not.toContain(AUTOMATED_PROJECTED_ROLE);
    expect(DECIDER_ROLES).not.toContain(AUTOMATED_PROJECTED_ROLE);
  });

  it('pins RUN_START_ROLES to the start-capable subset', () => {
    // The coarse start-role gate: the three start-capable roles, excluding the
    // review-only roles (reviewer/viewer). A host-level concept that mirrors no
    // breakwater constant, so the exact membership is pinned here.
    expect([...RUN_START_ROLES]).toEqual(['admin', 'operator', 'builder']);
  });

  it('adapts approval audit events onto AuditLogger.record', () => {
    // #given — the one-line adapter the docs promise; assignment is the
    // compile-time structural check
    const logger = new AuditLogger();
    const sink: ApprovalAuditSink = (event) => {
      logger.record(event);
    };

    // #when
    sink({
      actor: REVIEWER,
      action: 'approval.decide',
      resource: 'approval:1',
      decision: 'allowed',
      detail: { decision: 'approve' },
    });

    // #then
    expect(logger.events()[0]).toMatchObject({
      action: 'approval.decide',
      decision: 'allowed',
      actor: REVIEWER,
    });
    expect(logger.events()[0]?.timestamp).toBeTruthy();
  });
});

// Workflow under test: research -> approval (suspends) -> publish, where
// publish calls a write-gated breakwater connector with the step's
// requestContext. The grant can only appear via the runtime's provider.
interface Harness {
  runtime: RunnerRuntime;
  service: ApprovalService;
  store: InMemoryApprovalStore;
  connectorAudit: AuditLogger;
  publishes: () => number;
}

function buildHarness(): Harness {
  const store = new InMemoryApprovalStore();
  const connectorAudit = new AuditLogger();
  let publishes = 0;

  const publisher = createConnector<{ topic: string }, { published: boolean }>({
    id: 'blog-publisher',
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

  const { createWorkflow, createStep, runtime } = init(
    { storage: new InMemoryStore() },
    { requestContextForRun: approvalGrantProvider(store) },
  );

  const research = createStep({
    id: 'research',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string() }),
    execute: async ({ inputData }) => ({ topic: inputData.topic }),
  });

  const approval = createStep({
    id: 'approval',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string(), approved: z.boolean() }),
    suspendSchema: z.object({ reason: z.string() }),
    // The defaultResumeData contract: { approved, comment?, decidedBy? }.
    resumeSchema: z.object({
      approved: z.boolean(),
      comment: z.string().optional(),
      decidedBy: z.string().optional(),
    }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) {
        return suspend({ reason: 'human approval required before publish' });
      }
      return { topic: inputData.topic, approved: resumeData.approved };
    },
  });

  const publish = createStep({
    id: 'publish',
    inputSchema: z.object({ topic: z.string(), approved: z.boolean() }),
    outputSchema: z.object({ published: z.boolean() }),
    execute: async ({ inputData, requestContext }) => {
      if (!inputData.approved) return { published: false };
      if (!publisher.execute) throw new Error('connector has no execute');
      const result = await publisher.execute({ topic: inputData.topic }, {
        requestContext,
      } as unknown as ToolExecutionContext);
      return result as { published: boolean };
    },
  });

  createWorkflow({
    id: 'launch',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ published: z.boolean() }),
  })
    .then(research)
    .then(approval)
    .then(publish)
    .commit();

  // double-launch: two approval points, BOTH followed by a call to the SAME
  // write-gated connector — the leg-scoping regression fixture. Approving
  // gateA must not unlock the connector for a forged resume of gateB.
  const makeGate = (id: string) =>
    createStep({
      id,
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string() }),
      suspendSchema: z.object({ reason: z.string() }),
      resumeSchema: z.object({
        approved: z.boolean(),
        comment: z.string().optional(),
        decidedBy: z.string().optional(),
      }),
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) return suspend({ reason: `${id} awaits approval` });
        return { topic: inputData.topic };
      },
    });
  const makeUse = (id: string) =>
    createStep({
      id,
      inputSchema: z.object({ topic: z.string() }),
      outputSchema: z.object({ topic: z.string() }),
      execute: async ({ inputData, requestContext }) => {
        if (!publisher.execute) throw new Error('connector has no execute');
        await publisher.execute({ topic: inputData.topic }, {
          requestContext,
        } as unknown as ToolExecutionContext);
        return { topic: inputData.topic };
      },
    });
  createWorkflow({
    id: 'double-launch',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string() }),
  })
    .then(makeGate('gateA'))
    .then(makeUse('useA'))
    .then(makeGate('gateB'))
    .then(makeUse('useB'))
    .commit();

  // relaunch: ONE gate step that suspends twice (two rounds of confirmation)
  // before the gated call — the spent-approval regression fixture. The
  // approval decided for suspension #1 must not mint into suspension #2 of
  // the same step.
  let gate2xRounds = 0;
  const gate2x = createStep({
    id: 'gate2x',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string() }),
    suspendSchema: z.object({ reason: z.string() }),
    resumeSchema: z.object({
      approved: z.boolean(),
      comment: z.string().optional(),
      decidedBy: z.string().optional(),
    }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) return suspend({ reason: 'gate2x round 1' });
      gate2xRounds += 1;
      if (gate2xRounds < 2) return suspend({ reason: 'gate2x round 2' });
      return { topic: inputData.topic };
    },
  });
  createWorkflow({
    id: 'relaunch',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string() }),
  })
    .then(gate2x)
    .then(makeUse('relaunch-use'))
    .commit();

  // relaunch-falsy: a gate with NO resumeSchema, so a no-payload resume is not
  // schema-rejected and reaches execute, where it re-suspends. This is the
  // configuration that exposes the bug the resumeCount binding closes — the
  // re-suspension carries no resumedAt (Mastra stamps it only on a payload
  // resume), so the old (suspendedAt, resumedAt) pair could not tell it from
  // the first suspension.
  const gateFalsy = createStep({
    id: 'gateFalsy',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string() }),
    suspendSchema: z.object({ reason: z.string() }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) return suspend({ reason: 'gateFalsy awaits approval' });
      return { topic: inputData.topic };
    },
  });
  createWorkflow({
    id: 'relaunch-falsy',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string() }),
  })
    .then(gateFalsy)
    .then(makeUse('falsy-use'))
    .commit();

  // relaunch-hole2: a schema-less gate that re-suspends on BOTH a truthy resume
  // (round 2) and a falsy resume (round 1). This reproduces the deterministic
  // depth-3 "truthy -> falsy" residual: Mastra stamps resumedAt on the truthy
  // resume (susp #2) and PRESERVES that same value across the falsy resume
  // (susp #3), so #2 and #3 share resumedAt — the old binding could not tell
  // them apart. resumeCount (1 vs 2) still can. gate completes on the 2nd
  // truthy resume.
  let hole2Rounds = 0;
  const gate2xNoSchema = createStep({
    id: 'gate2xNoSchema',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string() }),
    suspendSchema: z.object({ reason: z.string() }),
    execute: async ({ inputData, resumeData, suspend }) => {
      if (!resumeData) return suspend({ reason: 'hole2 round 1' });
      hole2Rounds += 1;
      if (hole2Rounds < 2) return suspend({ reason: 'hole2 round 2' });
      return { topic: inputData.topic };
    },
  });
  createWorkflow({
    id: 'relaunch-hole2',
    inputSchema: z.object({ topic: z.string() }),
    outputSchema: z.object({ topic: z.string() }),
  })
    .then(gate2xNoSchema)
    .then(makeUse('hole2-use'))
    .commit();

  const service = new ApprovalService({
    store,
    resumeRun: resumeViaRuntime(runtime),
  });

  return {
    runtime,
    service,
    store,
    connectorAudit,
    publishes: () => publishes,
  };
}

// Regression suite for the create-route capability chain. Before the fix, one
// admin (holding both CAN_CREATE and CAN_REVIEW) could POST a body carrying
// `connectors` + a spoofed `requestedBy`, approve it themselves (the
// separation-of-duties check compared the spoofed field), and — because a
// step-less record was implicitly a run-scoped standing grant — mint an
// arbitrary connector capability on EVERY leg of an arbitrary run, with no
// second party. Three independent barriers now stand in that chain's way, and
// each is asserted below.
describe('fail closed: the HTTP create route cannot mint a run-scoped standing grant', () => {
  const CAPABILITY_BODY = {
    workflowId: 'launch',
    runId: 'acme_run-poc',
    title: 'innocuous-looking request',
    connectors: ['release-deploy'],
    requestedBy: 'nobody',
  };

  function post(body: unknown): Request {
    return new Request('http://queue.test/api/approvals', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    });
  }

  function routerOver(
    store: InMemoryApprovalStore,
    options: {
      allowCreate?: boolean;
      actor?: ApprovalActor;
      resumeRun?: ApprovalServiceOptions['resumeRun'];
    } = {},
  ) {
    const service = new ApprovalService({
      store,
      ...(options.resumeRun === undefined
        ? {}
        : { resumeRun: options.resumeRun }),
    });
    const resources = new InMemoryResourceOwnershipStore();
    const handle = createApprovalRouter({
      resolve: createActorResolver({
        authenticate: () => options.actor ?? ADMIN,
        storeFactory: { store: () => store, resources: () => resources },
        buildService: () => service,
      }),
      allowCreate: options.allowCreate,
    });
    return { service, handle, resources };
  }

  it('barrier 1: the route is off by default, so nothing is written', async () => {
    // #given
    const store = new InMemoryApprovalStore();
    const { handle } = routerOver(store);

    // #when — PoC step 1
    const response = await handle(post(CAPABILITY_BODY));

    // #then
    expect(response?.status).toBe(404);
    expect(await store.list()).toEqual([]);
  });

  it('barrier 1: a deliberately mounted route still rejects every capability-bearing field', async () => {
    // #given — the host opted in to the "file a request" affordance
    const store = new InMemoryApprovalStore();
    const { handle } = routerOver(store, { allowCreate: true });

    // #when / #then — connectors and scope select authority; stepPath,
    // toolCallId, and the binding pair select identity; requestedBy drives SoD.
    for (const field of [
      'connectors',
      'grantScope',
      'toolCallId',
      'stepPath',
      'suspendedAt',
      'resumedAt',
      'resumeCount',
      'runScoped',
      'requestedBy',
      'requestedByKind',
      'resumeTarget',
    ]) {
      const response = await handle(
        post({
          workflowId: 'launch',
          runId: 'acme_run-poc',
          title: 'x',
          [field]: 'anything',
        }),
      );
      expect(response?.status).toBe(400);
    }
    expect(await store.list()).toEqual([]);
  });

  it('barrier 1: foreign and missing run ids cannot file a record or reach the resume target', async () => {
    // #given — the authenticated operator does not own the named run. resumeRun
    // is the target-DO seam and would expose any approval that slipped through.
    const store = new InMemoryApprovalStore();
    const resumeRun = vi.fn().mockResolvedValue({ status: 'success' });
    const { handle, resources } = routerOver(store, {
      allowCreate: true,
      actor: OPERATOR,
      resumeRun,
    });
    await resources.claim('run', 'acme_run-foreign', {
      kind: 'human',
      id: 'other',
    });

    // #when
    const foreign = await handle(
      post({
        workflowId: 'launch',
        runId: 'acme_run-foreign',
        title: 'foreign',
      }),
    );
    const missing = await handle(
      post({
        workflowId: 'launch',
        runId: 'acme_run-missing',
        title: 'missing',
      }),
    );

    // #then — existence is indistinguishable and the write/target seams are
    // untouched.
    expect(foreign?.status).toBe(404);
    expect(missing?.status).toBe(404);
    expect(await foreign?.json()).toEqual({ error: 'run not found' });
    expect(await missing?.json()).toEqual({ error: 'run not found' });
    expect(await store.list()).toEqual([]);
    expect(resumeRun).not.toHaveBeenCalled();
  });

  it('barrier 2: forced self-attribution makes the filer unable to decide their own request', async () => {
    // #given — the route force-sets requestedBy = the authenticated actor, so
    // the spoof that disarmed separation of duties is gone
    const store = new InMemoryApprovalStore();
    const { service, handle, resources } = routerOver(store, {
      allowCreate: true,
    });
    await resources.claim('run', 'acme_run-poc', {
      kind: 'human',
      id: ADMIN.id,
    });
    const created = await handle(
      post({ workflowId: 'launch', runId: 'acme_run-poc', title: 'inert' }),
    );
    expect(created?.status).toBe(201);
    const record = (await created?.json()) as { id: string };

    // #when / #then — PoC step 2: the same admin decides. SoD now fires.
    await expect(
      service.decide(record.id, { decision: 'approve' }, ADMIN),
    ).rejects.toThrow(/cannot decide their own approval/);
  });

  it('barrier 3: an APPROVED step-less record with connectors mints NOTHING without runScoped', async () => {
    // #given — the last barrier, isolated. Inject the record straight into the
    // store (the HTTP route cannot author `connectors` at all, so driving this
    // through the router would assert an empty union against an empty list and
    // pass even with the runScoped gate reverted). This is the exact record
    // shape the PoC produced: approved, step-less, capability-bearing.
    const store = new InMemoryApprovalStore();
    const decidedAt = new Date().toISOString();
    await store.create({
      id: crypto.randomUUID(),
      workflowId: 'launch',
      runId: 'acme_run-poc',
      title: 'standing grant by omission',
      connectors: ['release-deploy'],
      priority: 'normal',
      status: 'approved',
      createdAt: decidedAt,
      updatedAt: decidedAt,
      decidedAt,
      // stepPath omitted, runScoped omitted — the inverted default.
    });

    // #when / #then — PoC step 3: inert on every leg shape
    const legs: RunLeg[] = [
      { kind: 'start' },
      { kind: 'resume', step: ['approval'], suspendedAt: 1 },
      { kind: 'resume' },
    ];
    for (const leg of legs) {
      expect(
        await connectorGrantsForLeg(store, 'launch', 'acme_run-poc', leg),
      ).toEqual([]);
    }
  });

  it('barrier 3, other direction: the same record WITH runScoped does mint (the gate is load-bearing)', async () => {
    // #given — identical to the record above but with the explicit opt-in. If
    // this did not mint, the test above would pass for the wrong reason.
    const store = new InMemoryApprovalStore();
    const decidedAt = new Date().toISOString();
    await store.create({
      id: crypto.randomUUID(),
      workflowId: 'launch',
      runId: 'acme_run-ok',
      title: 'deliberate standing grant',
      connectors: ['release-deploy'],
      priority: 'normal',
      status: 'approved',
      createdAt: decidedAt,
      updatedAt: decidedAt,
      decidedAt,
      runScoped: true,
      grantScope: 'run',
    });

    // #when / #then
    expect(
      await connectorGrantsForLeg(store, 'launch', 'acme_run-ok', {
        kind: 'start',
      }),
    ).toEqual([
      {
        scope: 'run',
        connectorId: 'release-deploy',
        workflowId: 'launch',
        runId: 'acme_run-ok',
      },
    ]);
  });

  it('barrier 3: an APPROVED http-authored record carries no capability to mint', async () => {
    // #given — a second party (the reviewer) approves the inert HTTP record.
    // resumeRun stands in for the target DO boundary: even an approved plain
    // queue record must not reach it without trusted resumability provenance.
    const store = new InMemoryApprovalStore();
    const resumeRun = vi.fn().mockResolvedValue({ status: 'success' });
    const { service, handle, resources } = routerOver(store, {
      allowCreate: true,
      resumeRun,
    });
    await resources.claim('run', 'acme_run-poc', {
      kind: 'human',
      id: ADMIN.id,
    });
    const created = await handle(
      post({ workflowId: 'launch', runId: 'acme_run-poc', title: 'inert' }),
    );
    const record = (await created?.json()) as { id: string };
    const decided = await service.decide(
      record.id,
      { decision: 'approve' },
      REVIEWER,
    );

    // #then — approved, yet there is nothing in it for a grant to derive from
    expect(decided.record.status).toBe('approved');
    expect(decided.record.connectors).toEqual([]);
    expect(decided.record.runScoped).toBeUndefined();
    expect(decided.record.stepPath).toBeUndefined();
    expect(decided.resume).toEqual({ attempted: false });
    expect(resumeRun).not.toHaveBeenCalled();
  });

  it('contrast: the intended in-process bridge path still mints on its own leg', async () => {
    // #given — a step-keyed record created by TRUSTED in-process code, bound to
    // the leg's exact suspension. The fix tightens the HTTP boundary only.
    const store = new InMemoryApprovalStore();
    const service = new ApprovalService({ store });
    const suspendedAt = Date.parse('2026-07-09T00:00:00.000Z');
    const { record } = await service.create(
      {
        workflowId: 'launch',
        runId: 'acme_run-ok',
        stepPath: ['approval'],
        suspendedAt,
        title: 'Approve launch',
        connectors: ['release-deploy'],
        requestedBy: 'starter',
        requestedByKind: 'human',
      },
      OPERATOR,
    );
    await service.decide(record.id, { decision: 'approve' }, REVIEWER);

    // #when / #then — mints on its bound leg, and nowhere else
    expect(
      await connectorGrantsForLeg(store, 'launch', 'acme_run-ok', {
        kind: 'resume',
        step: ['approval'],
        suspendedAt,
      }),
    ).toEqual([
      {
        scope: 'suspension',
        connectorId: 'release-deploy',
        workflowId: 'launch',
        runId: 'acme_run-ok',
        suspension: {
          stepPath: ['approval'],
          suspendedAt,
        },
      },
    ]);
    expect(
      await connectorGrantsForLeg(store, 'launch', 'acme_run-ok', {
        kind: 'start',
      }),
    ).toEqual([]);
  });
});

describe('approval queue end to end', () => {
  it('fails closed: a resume that bypasses decide() finds no grant and the connector denies', async () => {
    // #given — a suspended run, nothing approved in the store
    const harness = buildHarness();
    const started = await harness.runtime.start('launch', {
      runId: `acme_${crypto.randomUUID()}`,
      inputData: { topic: 'ship-it' },
    });
    expect(started.status).toBe('suspended');

    // #when — an "approved" resume forged straight at the runtime
    const resumed = await harness.runtime.resume('launch', started.runId, {
      step: 'approval',
      resumeData: { approved: true },
    });

    // #then — the write gate denies (no grant), the run fails, nothing published
    expect(resumed.status).toBe('failed');
    expect(resumed.error).toContain(
      'approval required and no matching structured grant was found',
    );
    expect(harness.publishes()).toBe(0);
    expect(harness.connectorAudit.events()).toContainEqual(
      expect.objectContaining({
        resource: 'blog-publisher',
        decision: 'denied',
      }),
    );
  });

  it('approve loop: decide() mints the grant the connector demands and the run completes', async () => {
    // #given — a suspended run with a queued approval carrying the connector
    // id and the runtime-observed exact suspension identity
    const harness = buildHarness();
    const started = await harness.runtime.start('launch', {
      runId: `acme_${crypto.randomUUID()}`,
      inputData: { topic: 'ship-it' },
    });
    expect(started.status).toBe('suspended');
    const { record } = await harness.service.create(
      {
        workflowId: 'launch',
        runId: started.runId,
        stepPath: ['approval'],
        title: 'Publish launch post',
        payload: started.suspendPayload,
        connectors: ['blog-publisher'],
        suspendedAt: suspendedAtFor(started, ['approval']),
      },
      OPERATOR,
    );

    // #when — the reviewer approves; decide() resumes via the runtime, whose
    // provider derives the grant from the now-approved record
    const decided = await harness.service.decide(
      record.id,
      { decision: 'approve', comment: 'lgtm' },
      REVIEWER,
    );

    // #then
    expect(decided.resume).toMatchObject({ attempted: true, ok: true });
    expect(decided.resume.summary).toMatchObject({
      status: 'success',
      result: { published: true },
    });
    expect(harness.publishes()).toBe(1);
    expect(harness.connectorAudit.events()).toContainEqual(
      expect.objectContaining({
        resource: 'blog-publisher',
        decision: 'allowed',
      }),
    );
  });

  it('reject loop: the workflow resumes, learns the outcome, and skips the gated call', async () => {
    // #given
    const harness = buildHarness();
    const started = await harness.runtime.start('launch', {
      runId: `acme_${crypto.randomUUID()}`,
      inputData: { topic: 'hold-it' },
    });
    const { record } = await harness.service.create(
      {
        workflowId: 'launch',
        runId: started.runId,
        stepPath: ['approval'],
        title: 'Publish launch post',
        connectors: ['blog-publisher'],
        suspendedAt: suspendedAtFor(started, ['approval']),
      },
      OPERATOR,
    );

    // #when
    const decided = await harness.service.decide(
      record.id,
      { decision: 'reject', comment: 'not yet' },
      REVIEWER,
    );

    // #then — run completes without publishing; no grant was ever minted
    expect(decided.resume.summary).toMatchObject({
      status: 'success',
      result: { published: false },
    });
    expect(harness.publishes()).toBe(0);
  });

  it('leg-scopes grants: approving one gate does not unlock the same connector at a later gate', async () => {
    // #given — gateA approved and its leg published once; the run is now
    // suspended at gateB, whose own approval is still pending
    const harness = buildHarness();
    const started = await harness.runtime.start('double-launch', {
      runId: `acme_${crypto.randomUUID()}`,
      inputData: { topic: 'twice-gated' },
    });
    expect(started.suspended).toEqual([['gateA']]);
    const { record: recordA } = await harness.service.create(
      {
        workflowId: 'double-launch',
        runId: started.runId,
        stepPath: ['gateA'],
        suspendedAt: suspendedAtFor(started, ['gateA']),
        resumedAt: resumedAtFor(started, ['gateA']),
        title: 'Gate A',
        connectors: ['blog-publisher'],
      },
      OPERATOR,
    );
    const decidedA = await harness.service.decide(
      recordA.id,
      { decision: 'approve' },
      REVIEWER,
    );
    expect(decidedA.resume.summary).toMatchObject({ status: 'suspended' });
    expect(harness.publishes()).toBe(1);

    // #when — a forged resume of gateB, with gateA's approval on the books
    const forged = await harness.runtime.resume(
      'double-launch',
      started.runId,
      {
        step: 'gateB',
        resumeData: { approved: true },
      },
    );

    // #then — gateA's approval does not travel: the gateB leg mints nothing,
    // the connector denies, and the second publish never happens
    expect(forged.status).toBe('failed');
    expect(forged.error).toContain(
      'approval required and no matching structured grant was found',
    );
    expect(harness.publishes()).toBe(1);
  });

  it('completes a two-gate run when each gate is decided on its own record', async () => {
    // #given
    const harness = buildHarness();
    const started = await harness.runtime.start('double-launch', {
      runId: `acme_${crypto.randomUUID()}`,
      inputData: { topic: 'twice-approved' },
    });
    const { record: recordA } = await harness.service.create(
      {
        workflowId: 'double-launch',
        runId: started.runId,
        stepPath: ['gateA'],
        suspendedAt: suspendedAtFor(started, ['gateA']),
        resumedAt: resumedAtFor(started, ['gateA']),
        title: 'Gate A',
        connectors: ['blog-publisher'],
      },
      OPERATOR,
    );
    const afterA = await harness.service.decide(
      recordA.id,
      { decision: 'approve' },
      REVIEWER,
    );
    expect(afterA.resume.summary).toMatchObject({
      status: 'suspended',
      suspended: [['gateB']],
    });

    // #when — gateB gets its own approval, bound to gateB's suspension as
    // observed on the resume summary
    const { record: recordB } = await harness.service.create(
      {
        workflowId: 'double-launch',
        runId: started.runId,
        stepPath: ['gateB'],
        suspendedAt: suspendedAtFor(afterA.resume.summary, ['gateB']),
        resumedAt: resumedAtFor(afterA.resume.summary, ['gateB']),
        title: 'Gate B',
        connectors: ['blog-publisher'],
      },
      OPERATOR,
    );
    // gate B must be decided by a DIFFERENT reviewer: ADMIN did not advance the
    // run at gate A, so the cross-gate SoD bar does not apply. REVIEWER, who
    // approved gate A causally-before gate B was filed, would now be refused here.
    const afterB = await harness.service.decide(
      recordB.id,
      { decision: 'approve' },
      ADMIN,
    );

    // #then — both gated calls executed, one per approved leg
    expect(afterB.resume.summary).toMatchObject({ status: 'success' });
    expect(harness.publishes()).toBe(2);
  });

  it('refuses the gate-A approver at gate B even when gate B was reconcile-filed by the system principal', async () => {
    // The reconcile hole: resumeRunWithRequeue attributes gate B to the gate-A
    // decider (so the requestedBy self-check already bars them), but
    // reconcileApprovalsForSummary files gate B as the SYSTEM principal — which the
    // requestedBy check never blocks. The cross-gate bar catches it anyway,
    // derived from the run's own APPROVED history.
    const harness = buildHarness();
    const started = await harness.runtime.start('double-launch', {
      runId: `acme_${crypto.randomUUID()}`,
      inputData: { topic: 'twice-gated-sod' },
    });
    const { record: recordA } = await harness.service.create(
      {
        workflowId: 'double-launch',
        runId: started.runId,
        stepPath: ['gateA'],
        suspendedAt: suspendedAtFor(started, ['gateA']),
        title: 'Gate A',
        connectors: ['blog-publisher'],
      },
      OPERATOR,
    );
    // #given — REVIEWER approves gate A; the run advances and re-suspends at gate B
    const afterA = await harness.service.decide(
      recordA.id,
      { decision: 'approve' },
      REVIEWER,
    );
    expect(afterA.resume.summary).toMatchObject({
      status: 'suspended',
      suspended: [['gateB']],
    });
    expect(harness.publishes()).toBe(1);

    // gate B is RECONCILE-filed: attributed to the system principal, not the reviewer
    const { record: recordB } = await harness.service.createAsPrincipal(
      {
        workflowId: 'double-launch',
        runId: started.runId,
        stepPath: ['gateB'],
        suspendedAt: suspendedAtFor(afterA.resume.summary, ['gateB']),
        title: 'Gate B (reconcile-filed)',
        connectors: ['blog-publisher'],
        requestedBy: 'flowsafe-system',
        requestedByKind: 'system',
      },
      trustAutomationPrincipal({
        kind: 'system',
        id: 'flowsafe-system',
        purpose: 'approval-reconciliation',
      }),
    );

    // #when / #then — REVIEWER (who advanced the run at gate A) is refused gate B,
    // despite requestedBy being the system principal rather than the reviewer, and
    // the second publish never happens
    await expect(
      harness.service.decide(recordB.id, { decision: 'approve' }, REVIEWER),
    ).rejects.toThrow(/earlier gate|separation of duties/i);
    expect(harness.publishes()).toBe(1);
  });

  it('a spent approval does not mint into a re-suspension of the same step', async () => {
    // #given — gate2x approved for suspension #1; the resumed leg suspends
    // the SAME step again, and no request for suspension #2 has been decided
    // (or even created — the window before a bridge reacts)
    const harness = buildHarness();
    const started = await harness.runtime.start('relaunch', {
      runId: `acme_${crypto.randomUUID()}`,
      inputData: { topic: 'respun' },
    });
    expect(started.suspended).toEqual([['gate2x']]);
    // Tripwire for the pair-binding: a FIRST suspension carries no resumeCount.
    // The forged re-suspension below WILL carry one (the runtime increments it
    // on the intervening resume), and that categorical undefined-vs-defined gap
    // is what denies deterministically — even if the two suspensions'
    // suspendedAt stamps collide within a millisecond.
    expect(resumeCountFor(started, ['gate2x'])).toBeUndefined();
    const { record: first } = await harness.service.create(
      {
        workflowId: 'relaunch',
        runId: started.runId,
        stepPath: ['gate2x'],
        suspendedAt: suspendedAtFor(started, ['gate2x']),
        resumeCount: resumeCountFor(started, ['gate2x']),
        title: 'Gate 2x — round 1',
        connectors: ['blog-publisher'],
      },
      OPERATOR,
    );
    const decided = await harness.service.decide(
      first.id,
      { decision: 'approve' },
      REVIEWER,
    );
    expect(decided.resume.summary).toMatchObject({
      status: 'suspended',
      suspended: [['gate2x']],
    });
    expect(harness.publishes()).toBe(0);

    // #when — a forged resume of suspension #2 rides on round 1's approval
    const forged = await harness.runtime.resume('relaunch', started.runId, {
      step: 'gate2x',
      resumeData: { approved: true },
    });

    // #then — round-1's approval captured resumeCount=undefined (a first
    // suspension); suspension #2's leg carries resumeCount=1, so the
    // (suspendedAt, resumeCount) pair cannot match and the approval never mints
    // into #2 — deterministically, with no reliance on the suspendedAt stamps
    // differing (they can collide in-process) or on decidedAt ordering (without
    // the pair binding, this fail-closed path relied on settleClock ordering).
    expect(forged.status).toBe('failed');
    expect(forged.error).toContain(
      'approval required and no matching structured grant was found',
    );
    expect(harness.publishes()).toBe(0);
  });

  it('a spent approval does not mint into a NO-PAYLOAD re-suspension (falsy-resume regression)', async () => {
    // The reported leak: Mastra stamps resumedAt only on a payload-bearing
    // resume, so a re-suspension reached via a falsy resume left the old
    // (suspendedAt, resumedAt) binding unable to tell it from a first
    // suspension. Pin Date.now so both suspensions share suspendedAt — the
    // in-process same-ms collision the leak needs (Mastra stamps suspendedAt
    // from Date.now; the service clock uses new Date(), unaffected).
    const fixed = Date.parse('2026-07-08T00:00:00.000Z');
    const nowSpy = vi.spyOn(Date, 'now').mockReturnValue(fixed);
    try {
      const harness = buildHarness();
      const started = await harness.runtime.start('relaunch-falsy', {
        runId: `acme_${crypto.randomUUID()}`,
        inputData: { topic: 'respun-falsy' },
      });
      // #given — suspended at the schema-less gate (first suspension:
      // resumeCount undefined), with an approval bound to THAT suspension.
      expect(started.suspended).toEqual([['gateFalsy']]);
      expect(resumeCountFor(started, ['gateFalsy'])).toBeUndefined();
      const decidedAt = new Date().toISOString();
      await harness.store.create({
        id: crypto.randomUUID(),
        workflowId: 'relaunch-falsy',
        runId: started.runId,
        stepPath: ['gateFalsy'],
        title: 'Gate — round 1',
        connectors: ['blog-publisher'],
        grantScope: 'suspension',
        priority: 'normal',
        status: 'approved',
        createdAt: decidedAt,
        updatedAt: decidedAt,
        decidedAt,
        suspendedAt: suspendedAtFor(started, ['gateFalsy']),
        // resumeCount omitted → undefined: bound to the FIRST suspension.
      });

      // #when — a NO-PAYLOAD resume re-suspends the SAME step. Mastra leaves
      // resumedAt undefined; the runtime still increments resumeCount to 1.
      const reSuspended = await harness.runtime.resume(
        'relaunch-falsy',
        started.runId,
        { step: 'gateFalsy' },
      );
      expect(reSuspended.suspended).toEqual([['gateFalsy']]);
      expect(resumedAtFor(reSuspended, ['gateFalsy'])).toBeUndefined();
      expect(resumeCountFor(reSuspended, ['gateFalsy'])).toBe(1);

      // A truthy resume of the re-suspension drives the gate through to the
      // write-gated connector.
      const forged = await harness.runtime.resume(
        'relaunch-falsy',
        started.runId,
        { step: 'gateFalsy', resumeData: { approved: true } },
      );

      // #then — suspension #1's approval (resumeCount undefined) cannot mint
      // into suspension #2's leg (resumeCount 1) even though their suspendedAt
      // collide, so the connector denies. On the superseded resumedAt binding
      // (undefined on both sides) this leaked and published.
      expect(forged.status).toBe('failed');
      expect(forged.error).toContain(
        'approval required and no matching structured grant was found',
      );
      expect(harness.publishes()).toBe(0);
    } finally {
      nowSpy.mockRestore();
    }
  });

  it('depth-3 truthy->falsy: a spent depth-2 approval does not mint into depth-3 despite a shared resumedAt (Hole-2)', async () => {
    // Pin Date.now so all three suspensions share suspendedAt (the in-process
    // same-ms collision), isolating resumeCount as the sole distinguisher.
    const fixed = Date.parse('2026-07-08T01:00:00.000Z');
    const nowSpy = vi.spyOn(Date, 'now').mockReturnValue(fixed);
    try {
      const harness = buildHarness();
      const started = await harness.runtime.start('relaunch-hole2', {
        runId: `acme_${crypto.randomUUID()}`,
        inputData: { topic: 'hole2' },
      });
      expect(started.suspended).toEqual([['gate2xNoSchema']]);

      // #when — a TRUTHY resume re-suspends (round 2 -> susp #2); Mastra stamps
      // resumedAt here.
      const susp2 = await harness.runtime.resume(
        'relaunch-hole2',
        started.runId,
        { step: 'gate2xNoSchema', resumeData: { go: true } },
      );
      expect(susp2.suspended).toEqual([['gate2xNoSchema']]);
      expect(resumeCountFor(susp2, ['gate2xNoSchema'])).toBe(1);
      const sharedResumedAt = resumedAtFor(susp2, ['gate2xNoSchema']);
      expect(sharedResumedAt).toBeTypeOf('number');

      // A depth-2 approval bound to susp #2 (resumeCount 1) is approved.
      const decidedAt = new Date().toISOString();
      await harness.store.create({
        id: crypto.randomUUID(),
        workflowId: 'relaunch-hole2',
        runId: started.runId,
        stepPath: ['gate2xNoSchema'],
        title: 'Depth-2 approval',
        connectors: ['blog-publisher'],
        grantScope: 'suspension',
        priority: 'normal',
        status: 'approved',
        createdAt: decidedAt,
        updatedAt: decidedAt,
        decidedAt,
        suspendedAt: suspendedAtFor(susp2, ['gate2xNoSchema']),
        resumeCount: resumeCountFor(susp2, ['gate2xNoSchema']),
      });

      // #when — a FALSY resume re-suspends again (round 1 -> susp #3). Mastra
      // PRESERVES the prior resumedAt, so #2 and #3 share it — the Hole-2 trap.
      const susp3 = await harness.runtime.resume(
        'relaunch-hole2',
        started.runId,
        { step: 'gate2xNoSchema' },
      );
      expect(susp3.suspended).toEqual([['gate2xNoSchema']]);
      expect(resumeCountFor(susp3, ['gate2xNoSchema'])).toBe(2);
      // The trap, proven: #3 carries the SAME resumedAt as #2.
      expect(resumedAtFor(susp3, ['gate2xNoSchema'])).toBe(sharedResumedAt);

      // A TRUTHY resume of susp #3 completes the gate and reaches the connector.
      const forged = await harness.runtime.resume(
        'relaunch-hole2',
        started.runId,
        { step: 'gate2xNoSchema', resumeData: { go: true } },
      );

      // #then — the depth-2 approval (resumeCount 1) cannot mint into susp #3's
      // leg (resumeCount 2) even though suspendedAt AND resumedAt both collide,
      // so the connector denies. On the old (suspendedAt, resumedAt) binding
      // both pairs were equal, so this leaked and published.
      expect(forged.status).toBe('failed');
      expect(forged.error).toContain(
        'approval required and no matching structured grant was found',
      );
      expect(harness.publishes()).toBe(0);
    } finally {
      nowSpy.mockRestore();
    }
  });

  it('a re-suspension completes with its own fresh decision', async () => {
    // #given — round 1 approved and resumed; the step suspended again
    const harness = buildHarness();
    const started = await harness.runtime.start('relaunch', {
      runId: `acme_${crypto.randomUUID()}`,
      inputData: { topic: 'respun-legit' },
    });
    const { record: first } = await harness.service.create(
      {
        workflowId: 'relaunch',
        runId: started.runId,
        stepPath: ['gate2x'],
        suspendedAt: suspendedAtFor(started, ['gate2x']),
        resumeCount: resumeCountFor(started, ['gate2x']),
        title: 'Gate 2x — round 1',
        connectors: ['blog-publisher'],
      },
      OPERATOR,
    );
    const afterFirst = await harness.service.decide(
      first.id,
      { decision: 'approve' },
      REVIEWER,
    );

    // The re-suspension carries resumeCount=1 (round 1 had it undefined) —
    // proof the summary plumbs the binding ordinal end to end; round 2's
    // approval binds to this new (suspendedAt, resumeCount) pair.
    expect(resumeCountFor(afterFirst.resume.summary, ['gate2x'])).toBe(1);

    // #when — suspension #2 gets its own request (fresh, not collapsed into
    // the decided round-1 record), bound to the NEW suspension's
    // (suspendedAt, resumeCount) pair, and its own approval
    const second = await harness.service.create(
      {
        workflowId: 'relaunch',
        runId: started.runId,
        stepPath: ['gate2x'],
        suspendedAt: suspendedAtFor(afterFirst.resume.summary, ['gate2x']),
        resumeCount: resumeCountFor(afterFirst.resume.summary, ['gate2x']),
        title: 'Gate 2x — round 2',
        connectors: ['blog-publisher'],
      },
      OPERATOR,
    );
    expect(second.created).toBe(true);
    // Round 2 is decided by a DIFFERENT reviewer: REVIEWER approved round 1, so
    // the cross-gate SoD bar refuses them at round 2 (in production the re-queue
    // attributes round 2 to REVIEWER, and the requestedBy self-check bars them
    // the same way). ADMIN did not advance the run, so it may decide round 2.
    const decided = await harness.service.decide(
      second.record.id,
      { decision: 'approve' },
      ADMIN,
    );

    // #then — round 2's decision mints for round 2's suspension
    expect(decided.resume.summary).toMatchObject({ status: 'success' });
    expect(harness.publishes()).toBe(1);
  });
});
