// SPDX-License-Identifier: Apache-2.0

import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { afterEach, expect, it } from 'vitest';
import { z } from 'zod';
import {
  DIRECT_SCENARIO_INVOCATION_BUDGET,
  DIRECT_SCENARIO_PHASES,
} from '../scripts/direct-credentialed-scenario-budget.mjs';
import { checkInvocationHeadroom } from '../scripts/direct-credentialed-scenario-checks.mjs';
import { continuationInputSchema } from '../scripts/direct-credentialed-tenant-schemas.mjs';
import {
  cleanupDirectRunState,
  completeScenario,
  present,
  scenarioJournal,
} from './fixtures/direct-run-state-builder.js';

const sha256 = (value: string) =>
  createHash('sha256').update(value).digest('hex');

async function source(path: string) {
  return readFile(new URL(path, import.meta.url), 'utf8');
}

function mutation(name: string, before: string, after: string) {
  const hashes = { before: sha256(before), after: sha256(after) };
  expect(hashes.after).not.toBe(hashes.before);
  console.log(`LV2_NEGATIVE ${name} ${JSON.stringify(hashes)}`);
}

afterEach(cleanupDirectRunState);

it('pins provider-promotion coupling to the native feasibility control', async () => {
  // `LV2 singleton continuation native feasibility` compares the promoted
  // generation and provider observation after this source tripwire reds.
  const before = await source('./fixtures/direct-scenario-harness.ts');
  const after = before.replace(
    'if (response.ok) await syncNativeTenant();',
    'if (response.ok) return response;',
  );
  mutation('promotion-coupling', before, after);
  expect(after).not.toContain('await syncNativeTenant()');
  expect(before).toContain('await syncNativeTenant()');
});

it('pins the three-member namespace tuple used by native feasibility', async () => {
  // `LV2 singleton continuation native feasibility` compares the fresh
  // database and both namespace identifiers independently.
  const before = await source('./fixtures/direct-native-tenant.ts');
  const after = before.replace(
    'JSON.stringify([databaseId, maintenanceNamespaceId, runnerNamespaceId])',
    'JSON.stringify([maintenanceNamespaceId, runnerNamespaceId])',
  );
  mutation('namespace-tuple', before, after);
  expect(after).not.toContain(
    'JSON.stringify([databaseId, maintenanceNamespaceId, runnerNamespaceId])',
  );
  expect(before).toContain(
    'JSON.stringify([databaseId, maintenanceNamespaceId, runnerNamespaceId])',
  );
});

it('pins provider SQL to the native D1 feasibility path', async () => {
  // `LV2 singleton continuation native feasibility` reads the retained marker
  // through the provider SQL path before comparing the A and B generations.
  const before = await source('./fixtures/direct-scenario-harness.ts');
  const after = before.replace(
    'return tenant?.providerRequest(request);',
    'return undefined;',
  );
  mutation('provider-sql', before, after);
  expect(after).not.toContain('return tenant?.providerRequest(request);');
  expect(before).toContain('return tenant?.providerRequest(request);');
});

it('rejects a 63-character challenge through the real Zod schema', () => {
  const input = { challenge: 'a'.repeat(63) };
  const weakened = z.object({ challenge: z.string() }).strict();
  expect(weakened.safeParse(input).success).toBe(true);
  expect(continuationInputSchema.safeParse(input).success).toBe(false);
});

it.each([
  [
    'original identity',
    (state: ReturnType<typeof completeScenario>) => {
      const identity = state.proofs.identities.a as {
        evidenceSha256: string;
      } | null;
      present(identity).evidenceSha256 = 'f'.repeat(64);
    },
  ],
  [
    'replacement version B',
    (state: ReturnType<typeof completeScenario>) => {
      present(state.proofs.continuation.versionB).identitySha256 = 'f'.repeat(
        64,
      );
    },
  ],
] as const)('rejects a tampered %s digest in the stored journal', async (_name, tamper) => {
  const { journal } = await scenarioJournal();
  const state = completeScenario();
  const before = JSON.stringify(state);
  tamper(state);
  const after = JSON.stringify(state);
  mutation('identity-digest', before, after);
  await expect(journal.recordScenario(state)).rejects.toMatchObject({
    code: 'invalid-state',
  });
});

it.each([
  [
    'worker-version and identity proofs',
    (state: ReturnType<typeof completeScenario>) => {
      for (const group of [
        state.proofs.initial,
        state.proofs.candidate,
        state.proofs.final,
        state.proofs.identities,
      ])
        [group.a, group.b] = [group.b, group.a];
    },
  ],
  [
    'export proofs and their history digests',
    (state: ReturnType<typeof completeScenario>) => {
      [state.proofs.exports.a, state.proofs.exports.b] = [
        state.proofs.exports.b,
        state.proofs.exports.a,
      ];
      const originalA = present(
        state.proofs.exportVerifications.find(
          ({ cycle, role }) => cycle === null && role === 'a',
        ),
      );
      const originalB = present(
        state.proofs.exportVerifications.find(
          ({ cycle, role }) => cycle === null && role === 'b',
        ),
      );
      [originalA.exportSha256, originalB.exportSha256] = [
        originalB.exportSha256,
        originalA.exportSha256,
      ];
    },
  ],
] as const)('rejects swapped keyed %s in the stored journal', async (_name, swap) => {
  const { journal } = await scenarioJournal();
  const state = completeScenario();
  const before = JSON.stringify(state);
  swap(state);
  const after = JSON.stringify(state);
  mutation('keyed-proof-role', before, after);
  await expect(journal.recordScenario(state)).rejects.toMatchObject({
    code: 'invalid-state',
  });
});

it('rejects a new run id in the finished continuation proof', async () => {
  const { journal } = await scenarioJournal();
  const state = completeScenario();
  const before = JSON.stringify(state.proofs.continuation.finished);
  present(state.proofs.continuation.finished).runId = 'different-run';
  const after = JSON.stringify(state.proofs.continuation.finished);
  mutation('same-run-id', before, after);
  await expect(journal.recordScenario(state)).rejects.toMatchObject({
    code: 'invalid-state',
  });
});

it('rejects an A-side continuation result', async () => {
  const { journal } = await scenarioJournal();
  const state = completeScenario();
  const before = JSON.stringify(state.proofs.continuation.finished);
  const finished = present(state.proofs.continuation.finished) as {
    release: string;
  };
  finished.release = '1';
  const after = JSON.stringify(state.proofs.continuation.finished);
  mutation('b-side-result', before, after);
  await expect(journal.recordScenario(state)).rejects.toMatchObject({
    code: 'invalid-state',
  });
});

it('rejects omitting replacement decommission proof at completion', async () => {
  const { journal } = await scenarioJournal();
  const state = completeScenario();
  const before = JSON.stringify(state.proofs.redecommission);
  state.proofs.redecommission.a = null;
  const after = JSON.stringify(state.proofs.redecommission);
  mutation('replacement-decommission', before, after);
  await expect(journal.recordScenario(state)).rejects.toMatchObject({
    code: 'invalid-state',
  });
});

it('rejects omitting the third export proof at completion', async () => {
  const { journal } = await scenarioJournal();
  const state = completeScenario();
  const before = JSON.stringify(state.proofs.reprovisionExports);
  state.proofs.reprovisionExports.a = null;
  const after = JSON.stringify(state.proofs.reprovisionExports);
  mutation('third-export', before, after);
  await expect(journal.recordScenario(state)).rejects.toMatchObject({
    code: 'invalid-state',
  });
});

it('rejects a second export-history reference for one lifecycle key', async () => {
  const { journal } = await scenarioJournal();
  const state = completeScenario();
  const before = JSON.stringify(state.proofs.exportVerifications);
  const replacement = present(
    state.proofs.exportVerifications.find(
      ({ cycle }) => cycle === 'reprovision',
    ),
  );
  state.proofs.exportVerifications.splice(1, 1);
  state.proofs.exportVerifications.push({ ...replacement });
  const after = JSON.stringify(state.proofs.exportVerifications);
  mutation('export-lifecycle-key', before, after);
  await expect(journal.recordScenario(state)).rejects.toMatchObject({
    code: 'invalid-state',
  });
});

it('rejects an under-sized maximal phase-call budget', () => {
  const calls = Object.fromEntries(
    DIRECT_SCENARIO_PHASES.map((phase) => [phase, 0]),
  ) as Record<(typeof DIRECT_SCENARIO_PHASES)[number], number>;
  const before = JSON.stringify(calls);
  calls['decommission-reprovisioned-a'] =
    DIRECT_SCENARIO_INVOCATION_BUDGET['decommission-reprovisioned-a'].ceiling;
  const after = JSON.stringify(calls);
  mutation('maximal-calls', before, after);
  expect(() =>
    checkInvocationHeadroom(
      'decommission-reprovisioned-a',
      calls,
      Number.MAX_SAFE_INTEGER,
    ),
  ).toThrowError(
    expect.objectContaining({
      code: 'budget-exhausted',
      detail: 'phase-ceiling',
    }),
  );
});
