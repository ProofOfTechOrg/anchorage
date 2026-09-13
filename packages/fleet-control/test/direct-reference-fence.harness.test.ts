// SPDX-License-Identifier: Apache-2.0

import {
  EXECUTION_FENCE_ROW_ID,
  EXECUTION_FENCE_TABLE,
} from '@proofoftech/flowsafe/deployment-identity-protocol';
import { INVENTORY_CATEGORIES } from '@proofoftech/flowsafe/do-runner';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import {
  createDirectReferenceHarness,
  type DirectReferenceHarness,
} from './fixtures/direct-reference-harness.js';
import type { D1State } from './fixtures/provider-world.js';

type Reading = {
  state: string;
  mutationEpoch: number;
  requireMutationEpoch: boolean;
  transitionRevision: number;
};
type Sweep = {
  fence: Reading;
  categories: { category: string; class: string; empty: boolean }[];
  observedAt: number;
};

describe.sequential('reference fence composition at release 1', {
  timeout: 180_000,
}, () => {
  let fixture: DirectReferenceHarness;
  let initial: Reading;
  let drained: Reading;
  let first: Sweep;
  beforeAll(async () => {
    fixture = await createDirectReferenceHarness({
      applicationProbes: true,
      maintenanceNow: Date.now,
    });
  }, 60_000);
  afterAll(async () => {
    await fixture?.close();
  }, 30_000);

  const read = (role: 'a' | 'b' = 'a') =>
    fixture.success<Reading>({ kind: 'tenant-fence', role, operation: 'read' });
  const inventory = () =>
    fixture.success<Sweep>({
      kind: 'tenant-fence',
      role: 'a',
      operation: 'inventory',
    });
  const probe = (operation: 'probe-missing' | 'probe-stale' | 'probe-future') =>
    fixture.success<{ epoch: string; classification: string }>({
      kind: 'tenant-fence',
      role: 'a',
      operation,
    });

  it('provisions both roles and reads open epoch-zero fences at release 1', async () => {
    for (const role of ['a', 'b'] as const) {
      expect(
        await fixture.success({ kind: 'provision', role, release: 'initial' }),
      ).toMatchObject({ status: 'ready' });
      const value = await read(role);
      expect(value).toMatchObject({
        state: 'open',
        mutationEpoch: 0,
        requireMutationEpoch: false,
      });
      expect(Number.isSafeInteger(value.transitionRevision)).toBe(true);
      expect(value.transitionRevision).toBeGreaterThanOrEqual(0);
      if (role === 'a') initial = value;
    }
  });

  it('accepts current, stale, missing and future before epoch activation', async () => {
    expect(await read('b')).toMatchObject({
      state: 'open',
      mutationEpoch: 0,
      requireMutationEpoch: false,
    });
    // assertMutationEpoch returns before comparison while the requirement is false.
    expect(
      await fixture.success({
        kind: 'tenant-fence',
        role: 'a',
        operation: 'mutate-current',
      }),
    ).toEqual({ accepted: true });
    for (const operation of [
      'probe-stale',
      'probe-missing',
      'probe-future',
    ] as const)
      expect(await probe(operation)).toEqual({
        epoch: operation.slice('probe-'.length),
        classification: 'accepted',
      });
  });

  it('sweeps open role a with empty work categories', async () => {
    const { response, value } = await fixture.call({
      kind: 'tenant-fence',
      role: 'a',
      operation: 'inventory',
    });
    expect(value.ok).toBe(true);
    expect(response.headers.get('X-Direct-Maintenance-Attempts')).toBe('11');
    first = value.result as Sweep;
    expect(first.fence.state).toBe('open');
    expect(
      first.categories.every(
        (entry) => entry.class === 'standing' || entry.empty,
      ),
    ).toBe(true);
  });

  it('drains role a once and classifies release-1 epochs against the activated fence', async () => {
    const result = await fixture.success<{ ok: boolean; after: Reading }>({
      kind: 'tenant-fence',
      role: 'a',
      operation: 'drain',
      expectedMutationEpoch: initial.mutationEpoch,
      expectedRevision: initial.transitionRevision,
    });
    expect(result).toEqual({
      ok: true,
      after: {
        state: 'draining',
        mutationEpoch: 1,
        requireMutationEpoch: true,
        transitionRevision: initial.transitionRevision + 1,
      },
    });
    drained = result.after;
    const current = await fixture.success<{
      accepted: boolean;
      classification: string;
    }>({
      kind: 'tenant-fence',
      role: 'a',
      operation: 'mutate-current',
    });
    expect(current).toEqual({
      accepted: false,
      code: 'MUTATION_EPOCH_MISMATCH',
      classification: 'stale',
      status: 409,
    });
    const stale = await probe('probe-stale');
    const future = await probe('probe-future');
    const missing = await probe('probe-missing');
    expect(stale).toEqual({ epoch: 'stale', classification: 'stale' });
    expect(future).toEqual({ epoch: 'future', classification: 'accepted' });
    expect(missing).toEqual({ epoch: 'missing', classification: 'missing' });
    // This fixture checks epochs without a router or schedule mutation. The
    // scenario's post-migration probes use the active release's advanced epoch.
    process.stdout.write(
      `A1_POST_DRAIN_CLASSIFICATIONS ${JSON.stringify({
        current: current.classification,
        stale: stale.classification,
        future: future.classification,
        missing: missing.classification,
      })}\n`,
    );
  });

  it('sweeps draining role a and reopens with the epoch requirement preserved', async () => {
    const second = await inventory();
    expect(second.fence.state).toBe('draining');
    expect(
      second.categories.every(
        (entry) => entry.class === 'standing' || entry.empty,
      ),
    ).toBe(true);
    for (const observedAt of [first.observedAt, second.observedAt]) {
      expect(Number.isFinite(observedAt)).toBe(true);
      expect(Number.isSafeInteger(observedAt)).toBe(true);
    }
    expect(second.observedAt).toBeGreaterThanOrEqual(first.observedAt);
    expect(
      await fixture.success({
        kind: 'tenant-fence',
        role: 'a',
        operation: 'reopen',
        expectedMutationEpoch: drained.mutationEpoch,
        expectedRevision: drained.transitionRevision,
      }),
    ).toEqual({
      ok: true,
      after: {
        state: 'open',
        mutationEpoch: 1,
        requireMutationEpoch: true,
        transitionRevision: drained.transitionRevision + 1,
      },
    });
  });

  it('separates application and maintenance credentials on role a ingress', async () => {
    for (const [path, token, status] of [
      ['/admin/execution-fence', fixture.secrets.a.maintenanceAdmin, 200],
      [
        '/admin/execution-fence',
        fixture.secrets.a.application?.APP_PROBE_TOKEN,
        401,
      ],
      ['/admin/inventory', fixture.secrets.a.maintenanceAdmin, 200],
      ['/__direct/health', fixture.secrets.a.maintenanceAdmin, 401],
    ] as const) {
      const response = await fixture.projection.fetch(
        `https://${fixture.manifest.names.roles.a.routeHostname}${path}`,
        { headers: { authorization: `Bearer ${token}` } },
      );
      expect(response.status).toBe(status);
      if (status === 200 && path === '/admin/execution-fence')
        expect(await response.json()).toMatchObject(await read());
      else await response.body?.cancel();
    }
  });

  it('returns a flattened CAS conflict without retrying the drain', async () => {
    const current = await read();
    const posts = () =>
      fixture.projection.requests.filter(
        (request) =>
          request.method === 'POST' &&
          request.url ===
            `https://${fixture.manifest.names.roles.a.routeHostname}/admin/execution-fence`,
      ).length;
    const before = posts();
    const { response, value } = await fixture.call({
      kind: 'tenant-fence',
      role: 'a',
      operation: 'drain',
      expectedMutationEpoch: initial.mutationEpoch,
      expectedRevision: initial.transitionRevision,
    });
    expect(response.status).toBe(200);
    expect(value).toMatchObject({
      ok: true,
      result: {
        ok: false,
        reason: {
          code: 'FENCE_CAS_CONFLICT',
          ...current,
          conflict: 'expectation-mismatch',
        },
      },
    });
    expect(posts()).toBe(before + 1);
    expect(await read()).toEqual(current);
  });

  it('projects reading fields while the proof-only store also carries a proof key', async () => {
    const current = await read();
    const response = await fixture.projection.fetch(
      `https://${fixture.manifest.names.roles.a.routeHostname}/admin/execution-fence`,
      {
        method: 'POST',
        headers: {
          authorization: `Bearer ${fixture.secrets.a.maintenanceAdmin}`,
        },
        body: JSON.stringify({
          expected: 'open',
          next: 'proof-only',
          proofKey: 'a1-fence-proof',
          expectedMutationEpoch: current.mutationEpoch,
          expectedRevision: current.transitionRevision,
        }),
      },
    );
    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ proofKey: 'a1-fence-proof' });
    expect(await read()).toEqual({
      state: 'proof-only',
      mutationEpoch: current.mutationEpoch,
      requireMutationEpoch: true,
      transitionRevision: current.transitionRevision + 1,
    });
  });

  it('counts the fence, index and category reads as eleven maintenance subrequests', async () => {
    const { response, value } = await fixture.call({
      kind: 'tenant-fence',
      role: 'a',
      operation: 'inventory',
    });
    expect(value.ok).toBe(true);
    expect(2 + INVENTORY_CATEGORIES.length).toBe(11);
    expect(response.headers.get('X-Direct-Maintenance-Attempts')).toBe('11');
  });

  it('replays numeric and null fence bindings through the database clone', async () => {
    const record = await fixture.fleetStore.get(
      fixture.manifest.names.roles.a.tenantTag,
      fixture.manifest.environment,
    );
    const original = fixture.world.databases.find(
      ({ databaseId }) => databaseId === record?.databaseId,
    )?.d1;
    if (!original) throw new Error('missing fixture database for role a');
    const fenceRow = (state: D1State) =>
      state.queryDatabase(
        `SELECT state, mutation_epoch, require_mutation_epoch, transition_revision
           FROM ${EXECUTION_FENCE_TABLE} WHERE id = ?`,
        [EXECUTION_FENCE_ROW_ID],
      );
    expect(fenceRow(original.clone())).toEqual(fenceRow(original));
  });

  it('refuses malformed tenant responses and accepts extra reading fields', async () => {
    const valid = {
      state: 'open',
      mutationEpoch: 0,
      requireMutationEpoch: false,
      transitionRevision: 7,
    };
    let answer = () => Response.json(valid);
    const isolated = await createDirectReferenceHarness({
      applicationProbes: false,
      maintenanceNow: Date.now,
      applicationFetch: async () => answer(),
    });
    try {
      expect(
        await isolated.success({
          kind: 'provision',
          role: 'a',
          release: 'initial',
        }),
      ).toMatchObject({ status: 'ready' });
      const cases = [
        [
          'media',
          () =>
            new Response(JSON.stringify(valid), {
              headers: { 'content-type': 'text/plain' },
            }),
        ],
        [
          'body-limit',
          () => Response.json({ ...valid, extra: 'x'.repeat(4096) }),
        ],
        [
          'parse',
          () =>
            new Response('{', {
              headers: { 'content-type': 'application/json' },
            }),
        ],
        ['state', () => Response.json({ ...valid, state: 'sealed' })],
        ['counter', () => Response.json({ ...valid, transitionRevision: '7' })],
      ] as const;
      for (const [name, response] of cases) {
        answer = response;
        const { response: failed, value } = await isolated.call({
          kind: 'tenant-fence',
          role: 'a',
          operation: 'read',
        });
        expect(failed.status).toBe(409);
        expect(value).toEqual({
          contractVersion: 1,
          ok: false,
          error: { code: 'operation-refused' },
        });
        process.stdout.write(
          `A1_MALFORMED_RESPONSE ${name} ${JSON.stringify(value)}\n`,
        );
      }
      answer = () =>
        Response.json({ ...valid, proofKey: 'extra', proofRunId: 'extra-run' });
      expect(
        await isolated.success({
          kind: 'tenant-fence',
          role: 'a',
          operation: 'read',
        }),
      ).toEqual(valid);
      process.stdout.write(
        `A1_EXTRA_FIELDS_ACCEPTED ${JSON.stringify(valid)}\n`,
      );
    } finally {
      await isolated.close();
    }
  });
});
