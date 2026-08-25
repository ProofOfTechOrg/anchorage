// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from 'vitest';
import {
  ActiveRouteAttestationError,
  attestConvergedActiveRoute,
  attestFleetRecordActiveRoute,
} from '../src/active-route.js';
import type {
  ActiveRouteAttestation,
  DeploymentSpec,
  FleetRecord,
  ProvisioningBackend,
} from '../src/types.js';

const TARGET_DIGEST = 'a'.repeat(64);
const PRIOR_DIGEST = 'b'.repeat(64);
const ATTESTED_AT = '2026-08-11T00:00:00.000Z';

const spec: DeploymentSpec = {
  tenantTag: 'acme',
  environment: 'production',
  scriptName: 'acme-production',
  databaseName: 'acme-production',
  compatibilityDate: '2026-08-10',
  mainModule: 'worker.js',
  modules: [{ name: 'worker.js', content: 'export default {}' }],
  authoredBy: 'external',
  schemaVersion: 1,
  migrations: [],
  durableObjectMigrations: [],
  durableObjectBindings: [],
  maintenanceBaseUrl: 'https://control-acme.example.test',
  routeHostname: 'acme.example.test',
};

function attestation(
  overrides: Partial<ActiveRouteAttestation> = {},
): ActiveRouteAttestation {
  return {
    specDigest: TARGET_DIGEST,
    artifactVersion: 'etag-target',
    physicalScriptName: 'acme-production-target',
    source: 'dispatch-route',
    observedAt: ATTESTED_AT,
    ...overrides,
  };
}

/**
 * One answer per attestation call, consumed in order; the last answer repeats
 * once the script runs out, which is what a permanently stuck route looks like.
 * Only `attestActiveRoute` is implemented — the helper must never reach for
 * anything else on the backend, and a thrown "unused" says so if it does.
 */
class ScriptedBackend
  implements Pick<ProvisioningBackend, 'attestActiveRoute'>
{
  readonly reads: string[] = [];
  #index = 0;
  readonly #answers: readonly (ActiveRouteAttestation | Error)[];

  constructor(answers: readonly (ActiveRouteAttestation | Error)[]) {
    if (answers.length === 0) throw new Error('scripted backend needs answers');
    this.#answers = answers;
  }

  async attestActiveRoute(
    deployment: DeploymentSpec,
  ): Promise<ActiveRouteAttestation> {
    this.reads.push(deployment.tenantTag);
    const answer =
      this.#answers[Math.min(this.#index, this.#answers.length - 1)];
    this.#index += 1;
    if (answer instanceof Error) throw answer;
    return answer as ActiveRouteAttestation;
  }
}

function backendOf(answers: readonly (ActiveRouteAttestation | Error)[]): {
  backend: ProvisioningBackend;
  scripted: ScriptedBackend;
} {
  const scripted = new ScriptedBackend(answers);
  return { backend: scripted as unknown as ProvisioningBackend, scripted };
}

/** A clock the injected sleep advances, so backoff costs no real time. */
function fakeTimeline(): {
  clock: () => number;
  sleep: (delayMs: number) => Promise<void>;
  slept: number[];
} {
  let now = 1_000_000;
  const slept: number[] = [];
  return {
    clock: () => now,
    sleep: async (delayMs: number) => {
      slept.push(delayMs);
      now += delayMs;
    },
    slept,
  };
}

function record(overrides: Partial<FleetRecord> = {}): FleetRecord {
  return {
    tenantTag: spec.tenantTag,
    backend: 'workers-for-platforms',
    environment: spec.environment,
    scriptName: spec.scriptName,
    databaseId: 'db-acme',
    databaseName: spec.databaseName,
    schemaVersion: spec.schemaVersion,
    artifactVersion: 'etag-target',
    desiredSpecDigest: TARGET_DIGEST,
    durableObjectBindings: [],
    routeHostname: spec.routeHostname,
    phase: 'ready',
    updatedAt: ATTESTED_AT,
    ...overrides,
  };
}

describe('attestConvergedActiveRoute', () => {
  it('costs one attestation when the route already matches', async () => {
    // #given a route already serving the expected release
    const { backend, scripted } = backendOf([attestation()]);
    const timeline = fakeTimeline();

    // #when it is attested
    const converged = await attestConvergedActiveRoute(
      backend,
      spec,
      { specDigest: TARGET_DIGEST, artifactVersion: 'etag-target' },
      timeline,
    );

    // #then the provider is read once and nothing is waited on
    expect(converged).toEqual(attestation());
    expect(scripted.reads).toHaveLength(1);
    expect(timeline.slept).toEqual([]);
  });

  it('waits out a stale route and returns the converged attestation', async () => {
    // #given host routing still answering with the release just replaced
    const stale = attestation({
      specDigest: PRIOR_DIGEST,
      artifactVersion: 'etag-prior',
      physicalScriptName: 'acme-production-prior',
    });
    const { backend, scripted } = backendOf([stale, stale, attestation()]);
    const timeline = fakeTimeline();

    // #when the convergence helper attests
    const converged = await attestConvergedActiveRoute(
      backend,
      spec,
      { specDigest: TARGET_DIGEST, artifactVersion: 'etag-target' },
      timeline,
    );

    // #then it returns the settled attestation after a widening backoff
    expect(converged).toEqual(attestation());
    expect(scripted.reads).toHaveLength(3);
    expect(timeline.slept).toEqual([1_000, 2_000]);
  });

  it('waits out a route mapping that has not landed yet', async () => {
    // #given a key-value route write that is not visible on first read
    const { backend, scripted } = backendOf([
      new ActiveRouteAttestationError('dispatches to no release', {}),
      attestation(),
    ]);
    const timeline = fakeTimeline();

    // #when the convergence helper attests
    const converged = await attestConvergedActiveRoute(
      backend,
      spec,
      { specDigest: TARGET_DIGEST, artifactVersion: 'etag-target' },
      timeline,
    );

    // #then the unconverged read is a wait, not a verdict
    expect(converged).toEqual(attestation());
    expect(scripted.reads).toHaveLength(2);
  });

  it('fails closed on a permanent mismatch inside a bounded budget', async () => {
    // #given a route that never moves off the prior release
    const stale = attestation({
      specDigest: PRIOR_DIGEST,
      artifactVersion: 'etag-prior',
      physicalScriptName: 'acme-production-prior',
    });
    const { backend, scripted } = backendOf([stale]);
    const timeline = fakeTimeline();

    // #when the convergence helper attests
    const failure = await attestConvergedActiveRoute(
      backend,
      spec,
      { specDigest: TARGET_DIGEST, artifactVersion: 'etag-target' },
      timeline,
    ).catch((error: unknown) => error);

    // #then it gives up rather than spinning, and hands back what it last saw
    expect(failure).toBeInstanceOf(ActiveRouteAttestationError);
    expect((failure as ActiveRouteAttestationError).attestation).toEqual(stale);
    expect((failure as ActiveRouteAttestationError).observed).toEqual({
      routedScriptName: 'acme-production-prior',
      artifactVersion: 'etag-prior',
      specDigest: PRIOR_DIGEST,
    });
    expect(scripted.reads).toHaveLength(10);
    expect(timeline.slept).toEqual([
      1_000, 2_000, 4_000, 8_000, 8_000, 8_000, 8_000, 8_000, 8_000,
    ]);
    expect(timeline.slept.reduce((total, delay) => total + delay, 0)).toBe(
      55_000,
    );
  });

  it('bounds itself even when the injected clock never advances', async () => {
    // #given a clock frozen where the elapsed-time bound can never fire
    const stale = attestation({ specDigest: PRIOR_DIGEST });
    const { backend, scripted } = backendOf([stale]);

    // #when the convergence helper attests against it
    const failure = await attestConvergedActiveRoute(
      backend,
      spec,
      { specDigest: TARGET_DIGEST, artifactVersion: 'etag-target' },
      { clock: () => 1_000_000, sleep: async () => {} },
    ).catch((error: unknown) => error);

    // #then the schedule alone still terminates it
    expect(failure).toBeInstanceOf(ActiveRouteAttestationError);
    expect(scripted.reads).toHaveLength(10);
  });

  it('fails closed carrying the last refusal when none ever succeeds', async () => {
    // #given a hostname that never routes anywhere
    const unrouted = new ActiveRouteAttestationError(
      'dispatches to no release',
      {
        routedScriptName: undefined,
      },
    );
    const { backend } = backendOf([unrouted]);
    const timeline = fakeTimeline();

    // #when the convergence helper attests
    const failure = await attestConvergedActiveRoute(
      backend,
      spec,
      { specDigest: TARGET_DIGEST, artifactVersion: 'etag-target' },
      timeline,
    ).catch((error: unknown) => error);

    // #then the refusal that stood is the cause, not something invented
    expect(failure).toBeInstanceOf(ActiveRouteAttestationError);
    expect(
      (failure as ActiveRouteAttestationError).attestation,
    ).toBeUndefined();
    expect((failure as ActiveRouteAttestationError).cause).toBe(unrouted);
    expect((failure as ActiveRouteAttestationError).message).toContain(
      'did not converge',
    );
  });

  it('never retries a failure that is not an attestation refusal', async () => {
    // #given a transport failure rather than an unconverged route
    const transport = new Error('provider connection reset');
    const { backend, scripted } = backendOf([transport]);
    const timeline = fakeTimeline();

    // #when the convergence helper attests
    const failure = await attestConvergedActiveRoute(
      backend,
      spec,
      { specDigest: TARGET_DIGEST, artifactVersion: 'etag-target' },
      timeline,
    ).catch((error: unknown) => error);

    // #then it propagates immediately instead of spending the budget on it
    expect(failure).toBe(transport);
    expect(scripted.reads).toHaveLength(1);
    expect(timeline.slept).toEqual([]);
  });

  it('matches on the digest alone while the artifact version is pending', async () => {
    // #given a first release, whose artifact version is not yet known
    const first = attestation({ artifactVersion: 'etag-first-release' });
    const { backend } = backendOf([first]);
    const timeline = fakeTimeline();

    // #when the pending sentinel stands in for an artifact expectation
    const converged = await attestConvergedActiveRoute(
      backend,
      spec,
      { specDigest: TARGET_DIGEST, artifactVersion: 'pending' },
      timeline,
    );

    // #then the digest decides the match and the attested version comes back
    expect(converged.artifactVersion).toBe('etag-first-release');
    expect(timeline.slept).toEqual([]);
  });

  it('holds the digest even when the artifact version is pending', async () => {
    // #given a route serving a release with a different specification
    const other = attestation({ specDigest: PRIOR_DIGEST });
    const { backend } = backendOf([other]);
    const timeline = fakeTimeline();

    // #when the pending sentinel stands in for an artifact expectation
    const failure = await attestConvergedActiveRoute(
      backend,
      spec,
      { specDigest: TARGET_DIGEST, artifactVersion: 'pending' },
      timeline,
    ).catch((error: unknown) => error);

    // #then the sentinel waives the version check only, never the digest
    expect(failure).toBeInstanceOf(ActiveRouteAttestationError);
  });

  it('refuses a backoff schedule it cannot honour', async () => {
    // #given a maximum delay below the delay the schedule starts at
    const { backend } = backendOf([attestation()]);

    // #when the convergence helper is configured with it
    const failure = attestConvergedActiveRoute(
      backend,
      spec,
      { specDigest: TARGET_DIGEST, artifactVersion: 'etag-target' },
      { initialRetryDelayMs: 4_000, maxRetryDelayMs: 1_000 },
    );

    // #then it refuses at configuration time rather than backing off downward
    await expect(failure).rejects.toThrow(/must not be below/);
  });
});

describe('attestFleetRecordActiveRoute', () => {
  it('reports agreement with the desired release', async () => {
    // #given a record whose desired release is the one being served
    const { backend, scripted } = backendOf([attestation()]);

    // #when the record's active route is read
    const read = await attestFleetRecordActiveRoute({
      record: record(),
      backend,
      spec,
    });

    // #then it agrees, on exactly one provider round trip
    expect(read).toEqual({ attestation: attestation(), matchesDesired: true });
    expect(scripted.reads).toHaveLength(1);
  });

  it('reports drift instead of failing on it', async () => {
    // #given a record desiring a release the route no longer serves
    const drifted = attestation({
      specDigest: PRIOR_DIGEST,
      artifactVersion: 'etag-prior',
      physicalScriptName: 'acme-production-prior',
    });
    const { backend, scripted } = backendOf([drifted, drifted]);

    // #when the record's active route is read
    const read = await attestFleetRecordActiveRoute({
      record: record(),
      backend,
      spec,
    });

    // #then the disagreement comes back as data, with no retry spent on it
    expect(read).toEqual({ attestation: drifted, matchesDesired: false });
    expect(scripted.reads).toHaveLength(1);
  });

  it('treats a pending artifact version as no version expectation', async () => {
    // #given a record persisted before its first release settled
    const first = attestation({ artifactVersion: 'etag-first-release' });
    const { backend } = backendOf([first]);

    // #when the record's active route is read
    const read = await attestFleetRecordActiveRoute({
      record: record({ artifactVersion: 'pending' }),
      backend,
      spec,
    });

    // #then the digest alone settles the comparison
    expect(read.matchesDesired).toBe(true);
  });

  it('lets an unattestable route fail rather than reporting it as drift', async () => {
    // #given a hostname that routes nowhere
    const unrouted = new ActiveRouteAttestationError(
      'dispatches to no release',
      {},
    );
    const { backend } = backendOf([unrouted]);

    // #when the record's active route is read
    const failure = await attestFleetRecordActiveRoute({
      record: record(),
      backend,
      spec,
    }).catch((error: unknown) => error);

    // #then it raises: "cannot tell" is not the same answer as "does not match"
    expect(failure).toBe(unrouted);
  });
});
