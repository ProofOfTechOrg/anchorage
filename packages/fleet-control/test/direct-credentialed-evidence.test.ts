// SPDX-License-Identifier: Apache-2.0

import { chmod, readdir, readFile, stat, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  buildDirectEvidence,
  DIRECT_CONFORMANCE_COMMANDS,
  DIRECT_EVIDENCE_IDENTITY_PATHS,
  DIRECT_EVIDENCE_KEYS,
  DIRECT_EVIDENCE_LITERALS,
  inspectDirectEvidence,
  writeDirectEvidence,
} from '../scripts/direct-credentialed-evidence.mjs';
import { DIRECT_RESIDUAL_SURFACES } from '../scripts/direct-credentialed-reference-vocabulary.mjs';
import { DIRECT_SCENARIO_PHASES } from '../scripts/direct-credentialed-scenario-budget.mjs';
import {
  cleanupDirectRunState,
  completeScenarioJournal,
  maximalScenario,
  maximalTeardown,
  present,
} from './fixtures/direct-run-state-builder.js';

const topKeys = [
  'version',
  'contractVersion',
  'packageVersion',
  'commit',
  'mode',
  'status',
  'exitCode',
  'startedAt',
  'finishedAt',
  'resumeCount',
  'accountIdSha256Suffix',
  'zoneIdSha256Suffix',
  'resourcePrefix',
  'maxInvocations',
  'disposableAccount',
  'configSha256',
  'referenceModuleSetSha256',
  'referenceUploadBytes',
  'commands',
  'bootstrap',
  'scenario',
  'teardown',
  'teardownCall',
  'retainedIdentities',
  'cost',
];
const sentinels = {
  secrets: ['private-api-seed', 'private-invoke-seed'],
  literals: DIRECT_EVIDENCE_LITERALS,
};
afterEach(cleanupDirectRunState);
const object = (value: unknown): Record<string, unknown> => {
  expect(value).toBeTypeOf('object');
  expect(value).not.toBeNull();
  return value as Record<string, unknown>;
};
const keys = (value: unknown, expected: readonly string[]) =>
  expect(Object.keys(object(value))).toEqual(expected);
// The publication path inspects; a test that only wants the verdict reads the
// same inspection's `hit`.
const inspected = (
  evidence: object,
  sentinels: { secrets: readonly string[]; literals: readonly string[] },
) => inspectDirectEvidence(evidence, sentinels).hit;
const leafAt = (document: unknown, keyPath: string) =>
  keyPath
    .split('.')
    .reduce<unknown>(
      (value, key) => (value == null ? value : object(value)[key]),
      document,
    );

async function evidenceFixture() {
  const { f, journal } = await completeScenarioJournal();
  const scenario = maximalScenario();
  const teardown = maximalTeardown();
  const snapshot = { ...journal.snapshot(), scenario, teardown };
  const evidence = buildDirectEvidence({
    snapshot,
    prepared: f.prepared,
    mode: 'resume',
    outcome: {
      status: 'cleaned',
      exitCode: 0,
      teardownCall: { status: 'cleaned', failure: null, providerRequests: 12 },
    },
    times: { finishedAt: '2026-09-13T00:00:00.000Z' },
    commit: 'a'.repeat(40),
  });
  return { f, journal, snapshot, evidence };
}

describe.sequential('direct evidence', () => {
  it('covers every projected key with the admission vocabulary', async () => {
    const { evidence } = await evidenceFixture();
    const vocabulary = new Set(DIRECT_EVIDENCE_KEYS);
    // Array elements are addressed by index rather than by a projected name;
    // admission covers those through its digits-only rule instead.
    const walk = (value: unknown, found: Set<string>) => {
      if (Array.isArray(value)) for (const child of value) walk(child, found);
      else if (value && typeof value === 'object')
        for (const [key, child] of Object.entries(value)) {
          found.add(key);
          walk(child, found);
        }
      return found;
    };
    const projected = [...walk(evidence, new Set<string>())];
    // A projected key outside the list is a credential collision admission
    // would not have refused, so this walk is the list's completeness check.
    expect(projected.filter((key) => !vocabulary.has(key))).toEqual([]);
    expect(Object.isFrozen(DIRECT_EVIDENCE_KEYS)).toBe(true);
    expect(new Set(DIRECT_EVIDENCE_KEYS).size).toBe(
      DIRECT_EVIDENCE_KEYS.length,
    );
    expect(evidence.commands).toEqual([
      DIRECT_CONFORMANCE_COMMANDS.run,
      DIRECT_CONFORMANCE_COMMANDS.resume,
    ]);
  });

  it('pins the allowlist key set and order at every projected shape', async () => {
    const { evidence, f, snapshot } = await evidenceFixture();
    keys(evidence, topKeys);
    process.stdout.write(
      `EVIDENCE_KEYS ${JSON.stringify(Object.keys(evidence))}\n`,
    );
    keys(evidence.bootstrap, ['dispatch', 'activeVersionId']);
    keys(object(evidence.bootstrap).dispatch, ['kind', 'count']);
    keys(evidence.scenario, [
      'phase',
      'failure',
      'invocationCount',
      'sdkRequests',
      'attempts',
      'phaseCalls',
      'restart',
      'initial',
      'candidate',
      'final',
      'fence',
      'exports',
      'inventories',
      'terminalForce',
    ]);
    keys(evidence.cost, [
      'basis',
      'referenceProvider',
      'referenceMaintenance',
      'referenceApplication',
      'sdkRequests',
      'referenceInvocations',
      'teardownProvider',
      'billed',
    ]);
    expect(evidence.cost).toEqual({
      basis: 'request-counters',
      referenceProvider: snapshot.scenario.attempts.provider,
      referenceMaintenance: snapshot.scenario.attempts.maintenance,
      referenceApplication: snapshot.scenario.attempts.application,
      sdkRequests: snapshot.scenario.sdkRequests,
      referenceInvocations: snapshot.invocationCount,
      teardownProvider: snapshot.teardown.providerRequests,
      billed: null,
    });
    for (const key of [
      'referenceProvider',
      'referenceMaintenance',
      'referenceApplication',
      'sdkRequests',
      'referenceInvocations',
      'teardownProvider',
    ])
      expect(object(evidence.cost)[key]).toBeTypeOf('number');
    const scenario = object(evidence.scenario);
    keys(scenario.inventories, ['before', 'after']);
    for (const when of ['before', 'after'] as const) {
      keys(object(scenario.inventories)[when], ['routeHostnames']);
      expect(object(scenario.inventories)[when]).toEqual({
        routeHostnames:
          snapshot.scenario.proofs.inventories[when]?.routeHostnames,
      });
    }
    keys(scenario.terminalForce, ['a']);
    const forced = object(object(scenario.terminalForce).a);
    keys(forced, ['databaseId', 'scriptName', 'ordinal', 'attempts']);
    keys(forced.attempts, ['provider', 'maintenance', 'application']);
    expect(scenario.terminalForce).toEqual(
      snapshot.scenario.proofs.terminalForce,
    );
    expect(scenario.invocationCount).toBe(snapshot.invocationCount);
    keys(scenario.attempts, ['provider', 'maintenance', 'application']);
    keys(scenario.phaseCalls, DIRECT_SCENARIO_PHASES);
    keys(scenario.restart, ['lossOrdinal', 'replayOrdinal', 'resumedProcess']);
    keys(scenario.initial, ['a', 'b', 'recovery']);
    for (const role of ['a', 'b', 'recovery'])
      keys(object(scenario.initial)[role], ['versionId', 'cpuLimitMs']);
    for (const group of ['candidate', 'final']) {
      keys(scenario[group], ['a', 'b']);
      for (const role of ['a', 'b'])
        keys(object(scenario[group])[role], [
          'versionId',
          'cpuLimitMs',
          'trafficPercentage',
        ]);
    }
    keys(scenario.fence, ['drain', 'sweeps', 'reopen', 'probes']);
    const fence = object(scenario.fence);
    for (const group of Object.values(fence)) keys(group, ['a', 'b']);
    const readingKeys = [
      'state',
      'mutationEpoch',
      'requireMutationEpoch',
      'transitionRevision',
    ];
    for (const group of ['drain', 'reopen'])
      for (const role of ['a', 'b']) {
        const transition = object(object(fence[group])[role]);
        keys(transition, ['before', 'after']);
        keys(transition.before, readingKeys);
        keys(transition.after, readingKeys);
      }
    keys(scenario.exports, ['a', 'b']);
    for (const role of ['a', 'b']) {
      const sweeps = object(object(fence.sweeps)[role]);
      keys(sweeps, ['first', 'second', 'intervalMs']);
      for (const key of ['first', 'second']) {
        keys(sweeps[key], [
          'fence',
          'categoryCount',
          'workCount',
          'standingCount',
          'emptyCount',
        ]);
        keys(object(sweeps[key]).fence, readingKeys);
      }
      keys(object(fence.probes)[role], [
        'current',
        'missing',
        'stale',
        'future',
        'mutationEpoch',
      ]);
      keys(object(scenario.exports)[role], ['location', 'size', 'sha256']);
    }
    keys(evidence.teardown, [
      'failure',
      'phase',
      'providerRequests',
      'receipts',
      'residual',
    ]);
    const teardown = object(evidence.teardown);
    expect(teardown).not.toHaveProperty('status');
    keys(teardown.receipts, [
      'ingress',
      'worker',
      'fleet',
      'quota',
      'exports',
      'exportObjects',
    ]);
    const receipts = object(teardown.receipts);
    for (const key of ['ingress', 'fleet', 'quota', 'exports'])
      keys(receipts[key], ['settledByReread']);
    keys(receipts.worker, ['settledByReread', 'secretNameCount']);
    keys(receipts.exportObjects, ['count', 'settledByReread']);
    keys(teardown.residual, [
      'surfaces',
      'bucketJurisdictions',
      'dispatch',
      'versionsGone',
      'settleAttempts',
    ]);
    const residual = object(teardown.residual);
    keys(residual.surfaces, DIRECT_RESIDUAL_SURFACES);
    for (const value of Object.values(object(residual.surfaces)))
      keys(value, ['prefixCount', 'globalCount', 'exhaustive']);
    keys(residual.dispatch, ['kind', 'count', 'status', 'prefixCount']);
    expect(residual.bucketJurisdictions).toEqual(
      snapshot.teardown.residual?.bucketJurisdictions,
    );
    keys(evidence.teardownCall, ['status', 'failure', 'providerRequests']);
    const retained = buildDirectEvidence({
      snapshot,
      prepared: f.prepared,
      mode: 'resume',
      outcome: {
        status: 'retained',
        exitCode: 4,
        teardownCall: {
          status: 'retained',
          failure: { code: 'residual-present' },
          providerRequests: 12,
        },
      },
      times: { finishedAt: '2026-09-13T00:00:00.000Z' },
      commit: 'a'.repeat(40),
    });
    keys(retained.teardownCall, ['status', 'failure', 'providerRequests']);
    keys(object(retained.teardownCall).failure, ['code']);
    keys(evidence.retainedIdentities, [
      'fleetUuid',
      'quotaUuid',
      'exportBucket',
      'scriptName',
      'activeVersionId',
    ]);
    expect(Object.values(object(evidence.retainedIdentities))).toEqual([
      null,
      null,
      null,
      null,
      null,
    ]);
    expect(evidence.commands).toEqual([
      'pnpm fleet-control:credentialed:direct -- --run',
      'pnpm fleet-control:credentialed:direct -- --resume',
    ]);
  });

  it.each([
    'platform-page',
    'transport-failure',
    'non-contract-answer',
    'delivery-window-expired',
  ] as const)('admits invocation failure detail %s in evidence', async (detail) => {
    const { f, snapshot } = await evidenceFixture();
    const scenario = maximalScenario();
    scenario.failure = { code: 'outcome-unknown', ordinal: 1, detail };
    const evidence = buildDirectEvidence({
      snapshot: { ...snapshot, scenario },
      prepared: f.prepared,
      mode: 'run',
      outcome: { status: 'failed', exitCode: 1, teardownCall: null },
      times: { finishedAt: '2026-09-13T00:00:00.000Z' },
      commit: null,
    });
    expect(object(evidence.scenario).failure).toEqual(scenario.failure);
    keys(object(evidence.scenario).failure, ['code', 'ordinal', 'detail']);
    expect(
      await writeDirectEvidence({
        directory: f.runDirectory,
        evidence,
        sentinels,
      }),
    ).toEqual({ written: true });
  });

  it.each([
    'platform-page',
    'transport-failure',
    'non-contract-answer',
    'delivery-window-expired',
  ] as const)('projects pending invocation detail %s without changing the journal snapshot', async (detail) => {
    const { f, snapshot } = await evidenceFixture();
    const scenario = maximalScenario();
    scenario.failure = null;
    const pending = {
      ...snapshot,
      scenario,
      lastInvocation: {
        ordinal: snapshot.invocationCount,
        action: { kind: 'control-read' as const },
        state: 'pending' as const,
        requestSha256: 'a'.repeat(64),
      },
    };
    const before = structuredClone(pending);
    const input = {
      snapshot: pending,
      prepared: f.prepared,
      mode: 'run' as const,
      outcome: { status: 'failed' as const, exitCode: 1, teardownCall: null },
      times: { finishedAt: '2026-09-13T00:00:00.000Z' },
      commit: null,
    };
    expect(
      object(
        buildDirectEvidence({ ...input, invocationFailureDetail: detail })
          .scenario,
      ).failure,
    ).toEqual({
      code: 'outcome-unknown',
      ordinal: pending.lastInvocation.ordinal,
      detail,
    });
    expect(
      object(
        buildDirectEvidence({
          ...input,
          invocationFailureDetail: 'raw-provider-error' as typeof detail,
        }).scenario,
      ).failure,
    ).toBeNull();
    expect(pending).toEqual(before);
  });

  it('preserves null observations, absent proofs and failure detail', async () => {
    const { f, snapshot } = await evidenceFixture();
    const scenario = maximalScenario();
    scenario.failure = {
      code: 'budget-exhausted',
      ordinal: 1,
      detail: 'run-reserve',
    };
    scenario.proofs.initial = { a: null, b: null, recovery: null };
    scenario.proofs.candidate = { a: null, b: null };
    scenario.proofs.final = { a: null, b: null };
    scenario.proofs.fence = {
      drain: { a: null, b: null },
      sweeps: { a: null, b: null },
      reopen: { a: null, b: null },
      probes: { a: null, b: null },
    };
    scenario.proofs.restart = null;
    scenario.proofs.exports = { a: null, b: null };
    scenario.proofs.inventories = { before: null, after: null };
    scenario.proofs.terminalForce = { a: null };
    const input = {
      snapshot: { ...snapshot, scenario },
      prepared: f.prepared,
      mode: 'resume' as const,
      outcome: { status: 'retained' as const, exitCode: 4, teardownCall: null },
      times: { finishedAt: '2026-09-13T00:00:00.000Z' },
      commit: null,
    };
    const result = buildDirectEvidence(input);
    expect(result.scenario).toMatchObject({
      failure: { code: 'budget-exhausted', ordinal: 1, detail: 'run-reserve' },
      initial: scenario.proofs.initial,
      candidate: scenario.proofs.candidate,
      final: scenario.proofs.final,
      fence: scenario.proofs.fence,
      exports: scenario.proofs.exports,
      inventories: scenario.proofs.inventories,
      terminalForce: scenario.proofs.terminalForce,
      restart: null,
    });
    delete scenario.failure.detail;
    expect(object(buildDirectEvidence(input).scenario).failure).toEqual({
      code: 'budget-exhausted',
      ordinal: 1,
      detail: null,
    });
    const {
      scenario: _scenario,
      teardown: _teardown,
      createdAt: _createdAt,
      ...older
    } = snapshot;
    const empty = buildDirectEvidence({
      ...input,
      snapshot: { ...older, bootstrap: null },
    });
    expect(empty.cost).toEqual({
      basis: 'request-counters',
      referenceProvider: null,
      referenceMaintenance: null,
      referenceApplication: null,
      sdkRequests: null,
      referenceInvocations: snapshot.invocationCount,
      teardownProvider: null,
      billed: null,
    });
    expect(empty).toMatchObject({
      startedAt: null,
      resumeCount: 0,
      bootstrap: null,
      scenario: null,
      teardown: null,
      teardownCall: null,
    });
  });

  it('projects partial fence readings and category counts without journal bookkeeping', async () => {
    const { f, snapshot } = await evidenceFixture();
    const scenario = maximalScenario();
    present(scenario.proofs.fence.drain.a).after = null;
    const sweeps = present(scenario.proofs.fence.sweeps.a);
    sweeps.second = null;
    sweeps.intervalMs = null;
    sweeps.first.categories = [
      { category: 'work-a', class: 'work', empty: true },
      { category: 'standing-a', class: 'standing', empty: false },
    ];
    const evidence = buildDirectEvidence({
      snapshot: { ...snapshot, scenario },
      prepared: f.prepared,
      mode: 'run',
      outcome: { status: 'failed', exitCode: 1, teardownCall: null },
      times: { finishedAt: '2026-09-13T00:00:00.000Z' },
      commit: null,
    });
    const fence = object(object(evidence.scenario).fence);
    expect(object(fence.drain).a).toMatchObject({ after: null });
    expect(object(fence.sweeps).a).toMatchObject({
      first: {
        categoryCount: 2,
        workCount: 1,
        standingCount: 1,
        emptyCount: 1,
      },
      second: null,
      intervalMs: null,
    });
    for (const forbidden of [
      'ordinal',
      'observedAt',
      'categories',
      'work-a',
      'standing-a',
    ])
      expect(JSON.stringify(fence)).not.toContain(forbidden);
  });

  it('refuses a credential completed by the evidence trailing newline', async () => {
    const { f } = await evidenceFixture();
    const evidence = { version: 1 };
    const newlineSentinels = { secrets: ['}\n'], literals: [] };
    const hit = { sentinelClass: 'env-secret', keyPath: '' };
    expect(inspectDirectEvidence(evidence, newlineSentinels)).toEqual({
      hit,
      serialized: '{"version":1}\n',
    });
    expect(
      await writeDirectEvidence({
        directory: f.runDirectory,
        evidence,
        sentinels: newlineSentinels,
      }),
    ).toEqual({ written: false, ...hit });
    expect(await readdir(f.runDirectory)).toEqual(['journal.json']);
  });

  it('reports the empty path for a credential no member span carries', async () => {
    const separated = { version: 1, mode: 'run' };
    // The sentinel spans the comma between two members, so it belongs to the
    // document's structure and to no member of it.
    expect(
      inspectDirectEvidence(separated, {
        secrets: ['1,"mode"'],
        literals: [],
      }),
    ).toEqual({
      hit: { sentinelClass: 'env-secret', keyPath: '' },
      serialized: '{"version":1,"mode":"run"}\n',
    });
    // A sentinel a member does carry is named by that member instead.
    expect(
      inspected(separated, { secrets: ['"mode":"run"'], literals: [] }),
    ).toEqual({ sentinelClass: 'env-secret', keyPath: 'mode' });
  });

  it('sweeps an abandoned staging sibling and refuses a directory that is not private', async () => {
    const { f, evidence } = await evidenceFixture();
    const abandoned = join(f.runDirectory, '.evidence-abandoned.tmp');
    await writeFile(abandoned, 'interrupted\n', { mode: 0o600 });
    expect(
      await writeDirectEvidence({
        directory: f.runDirectory,
        evidence,
        sentinels,
      }),
    ).toEqual({ written: true });
    expect((await readdir(f.runDirectory)).sort()).toEqual([
      'evidence.json',
      'journal.json',
    ]);
    await chmod(f.runDirectory, 0o755);
    try {
      expect(
        await writeDirectEvidence({
          directory: f.runDirectory,
          evidence,
          sentinels,
        }),
      ).toEqual({ written: false });
    } finally {
      await chmod(f.runDirectory, 0o700);
    }
  });

  it('publishes the scanned bytes including the newline observed during read-back', async () => {
    const { f, evidence } = await evidenceFixture();
    const inspected = inspectDirectEvidence(evidence, sentinels);
    expect(inspected.hit).toBeNull();
    const scanned = Buffer.from(inspected.serialized);
    let received: Buffer | undefined;
    expect(
      await writeDirectEvidence({
        directory: f.runDirectory,
        evidence,
        sentinels,
        readBack: async (path) => {
          received = await readFile(path);
          return received;
        },
      }),
    ).toEqual({ written: true });
    // The writer swallows a throw from `readBack`, so what the callback
    // observed is asserted once the call has returned.
    expect(received).toEqual(scanned);
    expect(received).toEqual(Buffer.from(`${JSON.stringify(evidence)}\n`));
    expect(await readFile(join(f.runDirectory, 'evidence.json'))).toEqual(
      scanned,
    );
  });

  it('writes verified 0600 evidence atomically without temporary siblings', async () => {
    const { f, evidence } = await evidenceFixture();
    const result = await writeDirectEvidence({
      directory: f.runDirectory,
      evidence,
      sentinels,
    });
    expect(result).toEqual({ written: true });
    const path = join(f.runDirectory, 'evidence.json');
    expect((await stat(path)).mode & 0o777).toBe(0o600);
    expect(await readFile(path, 'utf8')).toBe(`${JSON.stringify(evidence)}\n`);
    expect(
      (await readdir(f.runDirectory)).filter((name) => name.endsWith('.tmp')),
    ).toEqual([]);
    process.stdout.write('EVIDENCE_FILE mode=0600 temporaryFiles=0\n');
  });

  it.each([
    ...DIRECT_EVIDENCE_LITERALS.map((value) => [value, 'literal']),
    ['private-api-seed', 'env-secret'],
    ['private-invoke-seed', 'env-secret'],
    ['seed"token', 'env-secret'],
    ['seed\\token', 'env-secret'],
  ])('catches sentinel %s as %s at its string-leaf path', async (value, sentinelClass) => {
    const { f, evidence } = await evidenceFixture();
    const candidate = {
      ...evidence,
      retainedIdentities: {
        ...object(evidence.retainedIdentities),
        fleetUuid: value,
      },
    };
    const result = await writeDirectEvidence({
      directory: f.runDirectory,
      evidence: candidate,
      sentinels: {
        ...sentinels,
        secrets: [...sentinels.secrets, 'seed"token', 'seed\\token'],
      },
    });
    expect(result).toEqual({
      written: false,
      sentinelClass,
      keyPath: 'retainedIdentities.fleetUuid',
    });
    expect(await readdir(f.runDirectory)).toEqual(['journal.json']);
    expect(JSON.stringify(result)).not.toContain(value as string);
    process.stdout.write(
      `SENTINEL_CAUGHT ${sentinelClass} ${result.keyPath}\n`,
    );
  });

  it('catches serialized bytes that are absent from walked string leaves', async () => {
    const { f } = await evidenceFixture();
    const evidence = {
      bootstrap: { toJSON: () => ({ dispatch: 'safe' }) },
    };
    const byteSentinels = { secrets: [], literals: ['"dispatch":'] };
    expect(inspected(evidence, byteSentinels)).toEqual({
      sentinelClass: 'literal',
      keyPath: 'bootstrap',
    });
    expect(
      await writeDirectEvidence({
        directory: f.runDirectory,
        evidence,
        sentinels: byteSentinels,
      }),
    ).toEqual({
      written: false,
      sentinelClass: 'literal',
      keyPath: 'bootstrap',
    });
    expect(await readdir(f.runDirectory)).toEqual(['journal.json']);
  });

  it.each([
    ['plain toJSON secret', 'private-api-seed'],
    ['quoted toJSON secret', 'seed"token'],
    ['backslash toJSON secret', 'seed\\token'],
  ])('refuses decoded serialization containing a %s', async (_label, secret) => {
    const { f } = await evidenceFixture();
    const evidence = { bootstrap: { toJSON: () => secret } };
    const decodedSentinels = { secrets: [secret], literals: [] };
    const hit = { sentinelClass: 'env-secret', keyPath: 'bootstrap' };
    expect(inspected(evidence, decodedSentinels)).toEqual(hit);
    expect(
      await writeDirectEvidence({
        directory: f.runDirectory,
        evidence,
        sentinels: decodedSentinels,
      }),
    ).toEqual({ written: false, ...hit });
    expect(await readdir(f.runDirectory)).toEqual(['journal.json']);
  });

  it('refuses a Unicode escape that decodes to a credential in serialized evidence', async () => {
    const { f } = await evidenceFixture();
    const rawJSON = (JSON as typeof JSON & { rawJSON(text: string): object })
      .rawJSON;
    const evidence = { bootstrap: { dispatch: rawJSON('"\\u0073eed"') } };
    const escapedSentinels = { secrets: ['seed'], literals: [] };
    expect(JSON.stringify(evidence)).toContain('\\u0073');
    const hit = { sentinelClass: 'env-secret', keyPath: 'bootstrap.dispatch' };
    expect(inspected(evidence, escapedSentinels)).toEqual(hit);
    expect(
      await writeDirectEvidence({
        directory: f.runDirectory,
        evidence,
        sentinels: escapedSentinels,
      }),
    ).toEqual({ written: false, ...hit });
    expect(await readdir(f.runDirectory)).toEqual(['journal.json']);
  });

  it('refuses a credential object key at its own path before visiting its value', async () => {
    const { f } = await evidenceFixture();
    const secret = 'seed"\\token';
    const evidence = {
      bootstrap: { toJSON: () => ({ [secret]: 'Bearer hidden' }) },
    };
    const keySentinels = {
      secrets: [secret],
      literals: DIRECT_EVIDENCE_LITERALS,
    };
    const hit = { sentinelClass: 'env-secret', keyPath: `bootstrap.${secret}` };
    expect(inspected(evidence, keySentinels)).toEqual(hit);
    expect(
      await writeDirectEvidence({
        directory: f.runDirectory,
        evidence,
        sentinels: keySentinels,
      }),
    ).toEqual({ written: false, ...hit });
    expect(await readdir(f.runDirectory)).toEqual(['journal.json']);
  });

  it('guards the identity-path leaves and excludes the prefix-derived and scenario-charset ones', async () => {
    const { evidence } = await evidenceFixture();
    expect(Object.isFrozen(DIRECT_EVIDENCE_IDENTITY_PATHS)).toBe(true);
    // Every guarded path resolves to a projected identity leaf, so a renamed
    // or moved projection key fails here rather than leaving a guard that
    // never matches again.
    expect(
      DIRECT_EVIDENCE_IDENTITY_PATHS.filter((keyPath) => {
        const leaf = leafAt(evidence, keyPath);
        return leaf !== null && typeof leaf !== 'string';
      }),
    ).toEqual([]);
    // Prefix-derived names are the rule's exclusions, not omissions.
    for (const keyPath of [
      'retainedIdentities.exportBucket',
      'retainedIdentities.scriptName',
      'scenario.terminalForce.a.scriptName',
    ])
      expect(DIRECT_EVIDENCE_IDENTITY_PATHS).not.toContain(keyPath);
    // So are the scenario paths: the journal decodes each with `scenarioId`,
    // whose charset is strictly wider than the identity shape, so a guard here
    // refuses values the journal admits.
    for (const keyPath of [
      'scenario.initial.a.versionId',
      'scenario.candidate.b.versionId',
      'scenario.final.a.versionId',
      'scenario.terminalForce.a.databaseId',
    ]) {
      expect(DIRECT_EVIDENCE_IDENTITY_PATHS).not.toContain(keyPath);
      expect(leafAt(evidence, keyPath)).toBeTypeOf('string');
    }
  });

  it.each([
    'scenario.initial.a.versionId',
    'scenario.final.b.versionId',
    'scenario.terminalForce.a.databaseId',
  ])('publishes a scenario-charset identity at %s', async (keyPath) => {
    const { f, evidence } = await evidenceFixture();
    // `a:b` is outside the identity shape and inside `scenarioId`, so the
    // journal admits it and the evidence boundary no longer refuses it.
    const carried = JSON.parse(JSON.stringify(evidence)) as Record<
      string,
      unknown
    >;
    const keys = keyPath.split('.');
    const leaf = keys.pop() as string;
    object(leafAt(carried, keys.join('.')))[leaf] = 'a:b';
    expect(inspected(carried, sentinels)).toBeNull();
    expect(
      await writeDirectEvidence({
        directory: f.runDirectory,
        evidence: carried,
        sentinels,
      }),
    ).toEqual({ written: true });
  });

  it.each(
    DIRECT_EVIDENCE_IDENTITY_PATHS,
  )('enforces identity shape without nulling malformed proof at %s', async (keyPath) => {
    const { f } = await evidenceFixture();
    for (const [value, valid] of [
      [null, true],
      ['8e7a6123-1567-4abd-9012-3456789abcde', true],
      ['version_1.2-rc', true],
      ['a'.repeat(128), true],
      ['https://example.invalid/signed?sig=opaque', false],
      ['upstream returned an unexpected response', false],
      [' padded', false],
      ['quoted"identity', false],
      ['', false],
      ['_version', false],
      ['a'.repeat(129), false],
    ] as const) {
      const evidence = object(
        keyPath
          .split('.')
          .reduceRight<unknown>((child, key) => ({ [key]: child }), value),
      );
      const before = JSON.stringify(evidence);
      const hit = { refusalClass: 'identity-shape', keyPath };
      expect(inspected(evidence, sentinels)).toEqual(valid ? null : hit);
      if (!valid) {
        expect(
          await writeDirectEvidence({
            directory: f.runDirectory,
            evidence,
            sentinels,
          }),
        ).toEqual({ written: false, ...hit });
        expect(JSON.stringify(evidence)).toBe(before);
      }
    }
    expect(await readdir(f.runDirectory)).toEqual(['journal.json']);
  });

  it('writes exactly the serialization that passed the byte scan', async () => {
    const { f } = await evidenceFixture();
    let serializations = 0;
    const evidence = {
      bootstrap: {
        toJSON: () => (++serializations === 1 ? 'safe' : 'Bearer hidden'),
      },
    };
    expect(
      await writeDirectEvidence({
        directory: f.runDirectory,
        evidence,
        sentinels,
      }),
    ).toEqual({ written: true });
    expect(serializations).toBe(1);
    expect(await readFile(join(f.runDirectory, 'evidence.json'), 'utf8')).toBe(
      '{"bootstrap":"safe"}\n',
    );
  });

  it.each([
    false,
    true,
  ])('does not publish altered read-back bytes or replace previous evidence (existing=%s)', async (existing) => {
    const { f, evidence } = await evidenceFixture();
    const path = join(f.runDirectory, 'evidence.json');
    if (existing) await writeFile(path, 'previous-evidence\n', { mode: 0o600 });
    const result = await writeDirectEvidence({
      directory: f.runDirectory,
      evidence,
      sentinels,
      readBack: async () => Buffer.from('altered bytes'),
    });
    expect(result).toEqual({ written: false });
    if (existing)
      expect(await readFile(path, 'utf8')).toBe('previous-evidence\n');
    else await expect(stat(path)).rejects.toMatchObject({ code: 'ENOENT' });
    expect(
      (await readdir(f.runDirectory)).filter((name) => name.endsWith('.tmp')),
    ).toEqual([]);
  });
});
