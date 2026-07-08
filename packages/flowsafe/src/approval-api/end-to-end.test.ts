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
  APPROVED_CONNECTORS_CONTEXT_KEY,
  AuditLogger,
  createConnector,
  ROLES,
  WORKFLOW_SCOPE_CONTEXT_KEY,
} from '@proofoftech/breakwater';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { init } from '../do-runner/init.js';
import type { RunnerRuntime, RunSummary } from '../do-runner/runtime.js';
import type { ApprovalActor, ApprovalAuditSink } from './contract.js';
import {
  APPROVAL_ROLES,
  BREAKWATER_ACTOR_KEY,
  BREAKWATER_APPROVED_CONNECTORS_KEY,
  BREAKWATER_WORKFLOW_SCOPE_KEY,
} from './contract.js';
import { approvalGrantProvider, resumeViaRuntime } from './grants.js';
import { ApprovalService } from './service.js';
import { InMemoryApprovalStore } from './store.js';

const OPERATOR: ApprovalActor = { id: 'opal', role: 'operator' };
const REVIEWER: ApprovalActor = { id: 'ray', role: 'reviewer' };

// LEGACY-fallback timing nudge: a step-keyed record created WITHOUT
// suspendedAt mints only when decidedAt lands STRICTLY after the
// suspension's timestamp, and these in-process tests can decide within the
// same millisecond. Suites whose creates capture suspendedAt (the preferred
// bridge behavior) bind by exact match and need no clock choreography.
const settleClock = (): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, 5));

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

// The bridge's resumedAt capture, paired with suspendedAtFor. No throw: a
// step's FIRST suspension legitimately carries no resumedAt (undefined), which
// is exactly the signal that distinguishes it from a re-suspension.
function resumedAtFor(
  summary: unknown,
  stepPath: readonly string[],
): number | undefined {
  const at = (summary as RunSummary | undefined)?.resumedAt?.[
    stepPath.join('.')
  ];
  return typeof at === 'number' ? at : undefined;
}

describe('breakwater contract tripwires', () => {
  it('mirrors the approved-connectors key literally', () => {
    expect(BREAKWATER_APPROVED_CONNECTORS_KEY).toBe(
      APPROVED_CONNECTORS_CONTEXT_KEY,
    );
  });

  it('mirrors the actor key literally', () => {
    expect(BREAKWATER_ACTOR_KEY).toBe(ACTOR_CONTEXT_KEY);
  });

  it('mirrors the workflow-scope key literally', () => {
    expect(BREAKWATER_WORKFLOW_SCOPE_KEY).toBe(WORKFLOW_SCOPE_CONTEXT_KEY);
  });

  it('mirrors the role set', () => {
    expect([...APPROVAL_ROLES]).toEqual([...ROLES]);
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

describe('approval queue end to end', () => {
  it('fails closed: a resume that bypasses decide() finds no grant and the connector denies', async () => {
    // #given — a suspended run, nothing approved in the store
    const harness = buildHarness();
    const started = await harness.runtime.start('launch', {
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
    expect(resumed.error).toContain('approval required and not granted');
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
    // id. Deliberately created WITHOUT suspendedAt: this pins the LEGACY
    // decidedAt-after-suspension fallback (hence the settleClock below).
    const harness = buildHarness();
    const started = await harness.runtime.start('launch', {
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
      },
      OPERATOR,
    );

    // #when — the reviewer approves; decide() resumes via the runtime, whose
    // provider derives the grant from the now-approved record
    await settleClock();
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
      inputData: { topic: 'hold-it' },
    });
    const { record } = await harness.service.create(
      {
        workflowId: 'launch',
        runId: started.runId,
        stepPath: ['approval'],
        title: 'Publish launch post',
        connectors: ['blog-publisher'],
      },
      OPERATOR,
    );

    // #when
    await settleClock();
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
    expect(forged.error).toContain('approval required and not granted');
    expect(harness.publishes()).toBe(1);
  });

  it('completes a two-gate run when each gate is decided on its own record', async () => {
    // #given
    const harness = buildHarness();
    const started = await harness.runtime.start('double-launch', {
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
    const afterB = await harness.service.decide(
      recordB.id,
      { decision: 'approve' },
      REVIEWER,
    );

    // #then — both gated calls executed, one per approved leg
    expect(afterB.resume.summary).toMatchObject({ status: 'success' });
    expect(harness.publishes()).toBe(2);
  });

  it('a spent approval does not mint into a re-suspension of the same step', async () => {
    // #given — gate2x approved for suspension #1; the resumed leg suspends
    // the SAME step again, and no request for suspension #2 has been decided
    // (or even created — the window before a bridge reacts)
    const harness = buildHarness();
    const started = await harness.runtime.start('relaunch', {
      inputData: { topic: 'respun' },
    });
    expect(started.suspended).toEqual([['gate2x']]);
    // Tripwire for the pair-binding: a FIRST suspension carries no resumedAt.
    // The forged re-suspension below WILL carry one, and that categorical
    // undefined-vs-defined gap is what denies deterministically — even if the
    // two suspensions' suspendedAt stamps collide within a millisecond.
    expect(resumedAtFor(started, ['gate2x'])).toBeUndefined();
    const { record: first } = await harness.service.create(
      {
        workflowId: 'relaunch',
        runId: started.runId,
        stepPath: ['gate2x'],
        suspendedAt: suspendedAtFor(started, ['gate2x']),
        resumedAt: resumedAtFor(started, ['gate2x']),
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

    // #then — round-1's approval captured resumedAt=undefined (a first
    // suspension); suspension #2's leg carries a defined resumedAt, so the
    // (suspendedAt, resumedAt) pair cannot match and the approval never mints
    // into #2 — deterministically, with no reliance on the suspendedAt stamps
    // differing (they can collide in-process) or on decidedAt ordering (without
    // the pair binding, this fail-closed path relied on settleClock ordering).
    expect(forged.status).toBe('failed');
    expect(forged.error).toContain('approval required and not granted');
    expect(harness.publishes()).toBe(0);
  });

  it('a re-suspension completes with its own fresh decision', async () => {
    // #given — round 1 approved and resumed; the step suspended again
    const harness = buildHarness();
    const started = await harness.runtime.start('relaunch', {
      inputData: { topic: 'respun-legit' },
    });
    const { record: first } = await harness.service.create(
      {
        workflowId: 'relaunch',
        runId: started.runId,
        stepPath: ['gate2x'],
        suspendedAt: suspendedAtFor(started, ['gate2x']),
        resumedAt: resumedAtFor(started, ['gate2x']),
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

    // The re-suspension carries a DEFINED resumedAt (unlike round 1) — proof
    // the summary plumbs it end to end; round 2's approval binds to this new
    // (suspendedAt, resumedAt) pair.
    expect(resumedAtFor(afterFirst.resume.summary, ['gate2x'])).toBeTypeOf(
      'number',
    );

    // #when — suspension #2 gets its own request (fresh, not collapsed into
    // the decided round-1 record), bound to the NEW suspension's
    // (suspendedAt, resumedAt) pair, and its own approval
    const second = await harness.service.create(
      {
        workflowId: 'relaunch',
        runId: started.runId,
        stepPath: ['gate2x'],
        suspendedAt: suspendedAtFor(afterFirst.resume.summary, ['gate2x']),
        resumedAt: resumedAtFor(afterFirst.resume.summary, ['gate2x']),
        title: 'Gate 2x — round 2',
        connectors: ['blog-publisher'],
      },
      OPERATOR,
    );
    expect(second.created).toBe(true);
    const decided = await harness.service.decide(
      second.record.id,
      { decision: 'approve' },
      REVIEWER,
    );

    // #then — round 2's decision mints for round 2's suspension
    expect(decided.resume.summary).toMatchObject({ status: 'success' });
    expect(harness.publishes()).toBe(1);
  });
});
