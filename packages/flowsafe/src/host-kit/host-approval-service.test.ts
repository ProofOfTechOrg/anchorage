// SPDX-License-Identifier: Apache-2.0
import { afterEach, describe, expect, it, vi } from 'vitest';

import type {
  ApprovalActor,
  SelfDecisionPolicy,
} from '../approval-api/index.js';
import {
  ApprovalAuthzError,
  InMemoryApprovalStoreFactory,
} from '../approval-api/index.js';
import {
  buildHostApprovalService,
  runApprovalRetentionPurge,
} from './host-approval-service.js';

const OPERATOR: ApprovalActor = {
  id: 'opal',
  role: 'operator',
  tenantId: 'acme',
};
const ADMIN: ApprovalActor = { id: 'ada', role: 'admin', tenantId: 'acme' };

describe('runApprovalRetentionPurge', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('contains the 1e303 env overflow: numberVar accepts it, the ms multiply overflows to Infinity, and the purge TypeError is logged — never thrown (QA audit 2026-07-11)', async () => {
    // #given — "1e303" passes numberVar's own validation (finite, positive)
    // but 1e303 * 86_400_000 overflows to Infinity before reaching
    // purgeExpiredApprovals, whose finiteness guard throws
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const store = new InMemoryApprovalStoreFactory().system();

    // #when
    const purged = await runApprovalRetentionPurge({
      store,
      retentionDays: '1e303',
      cron: '7 * * * *',
    });

    // #then — contained: resolves undefined, one maintenance-error line
    expect(purged).toBeUndefined();
    const logged = errorSpy.mock.calls
      .map(([line]) => String(line))
      .filter((line) => line.includes('maintenance-error'));
    expect(logged).toHaveLength(1);
    expect(JSON.parse(logged[0] ?? '{}')).toMatchObject({
      type: 'maintenance-error',
      surface: 'approval-retention-purge',
      cron: '7 * * * *',
      error: expect.stringContaining('TypeError'),
    });
  });

  it('purges through the real store on a sane retentionDays value', async () => {
    // #given — one decided record older than a 0-day retention window
    const factory = new InMemoryApprovalStoreFactory();
    const store = factory.forTenant('acme');
    await store.create({
      id: 'apr-retention-1',
      tenantId: 'acme',
      workflowId: 'wf',
      runId: 'acme_run-1',
      title: 'old decided approval',
      connectors: [],
      priority: 'normal',
      status: 'approved',
      createdAt: new Date(0).toISOString(),
      decidedAt: new Date(0).toISOString(),
      updatedAt: new Date(0).toISOString(),
    });

    // #when — APPROVAL_RETENTION_DAYS=0: purge decided approvals now
    const purged = await runApprovalRetentionPurge({
      store: factory.system(),
      retentionDays: '0',
      cron: '7 * * * *',
    });

    // #then
    expect(purged).toBe(1);
  });
});

describe('buildHostApprovalService allowSelfDecision passthrough', () => {
  function buildService(allowSelfDecision?: SelfDecisionPolicy) {
    const store = new InMemoryApprovalStoreFactory().forTenant('acme');
    return buildHostApprovalService(store, {
      systemActorId: 'flowsafe-system',
      // A benign resume topology: decide() calls #resume on approve, and a
      // non-'suspended' summary means resumeRunWithRequeue queues nothing.
      resumeRun: async (record) => ({ runId: record.runId, status: 'success' }),
      allowSelfDecision,
    });
  }

  async function adminRequestedRecordId(
    service: ReturnType<typeof buildService>,
  ): Promise<string> {
    const { record } = await service.create(
      {
        workflowId: 'wf',
        runId: 'acme_run-1',
        title: 'self-request',
        requestedBy: ADMIN.id,
      },
      OPERATOR,
    );
    return record.id;
  }

  it('forwards a role-scoped exemption so admin can self-decide', async () => {
    // #given
    const service = buildService({ roles: ['admin'] });
    const id = await adminRequestedRecordId(service);

    // #when
    const result = await service.decide(id, { decision: 'approve' }, ADMIN);

    // #then
    expect(result.record.status).toBe('approved');
  });

  it('defaults to SoD ON when allowSelfDecision is unset', async () => {
    // #given
    const service = buildService();
    const id = await adminRequestedRecordId(service);

    // #when / #then — the requester (admin) is refused their own request
    await expect(
      service.decide(id, { decision: 'approve' }, ADMIN),
    ).rejects.toBeInstanceOf(ApprovalAuthzError);
  });
});
